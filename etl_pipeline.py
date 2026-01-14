"""Hunter OS v3 - Zero-touch orchestration."""

import asyncio
import json
import logging
import os
import threading
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

import pandas as pd
import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from modules import cleaning, data_sources, enrichment_async, providers, scoring, storage

STATUS_RUNNING = "RUNNING"
STATUS_PAUSED = "PAUSED"
STATUS_COMPLETED = "COMPLETED"
STATUS_FAILED = "FAILED"

STAGE_PROBE = "PROBE"
STAGE_REALTIME_FETCH = "REALTIME_FETCH"
STAGE_BULK_EXPORT_REQUEST = "BULK_EXPORT_REQUEST"
STAGE_BULK_POLL = "BULK_POLL"
STAGE_BULK_DOWNLOAD = "BULK_DOWNLOAD"
STAGE_BULK_IMPORT = "BULK_IMPORT"
STAGE_LOCAL_PIPELINE = "LOCAL_PIPELINE"
STAGE_COMPLETED = "COMPLETED"
STAGE_PAUSED = "PAUSED"
STAGE_FAILED = "FAILED"

_job_registry: Dict[str, Dict[str, Any]] = {}


class JobCanceled(Exception):
    pass


def _ensure_logger() -> logging.Logger:
    logger = logging.getLogger("hunter")
    if getattr(logger, "_hunter_configured", False):
        return logger
    logger.setLevel(logging.INFO)
    os.makedirs("logs", exist_ok=True)
    handler = logging.FileHandler(os.path.join("logs", "hunter.log"))
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    logger._hunter_configured = True
    return logger


class HunterOrchestrator:
    def __init__(self) -> None:
        self.logger = _ensure_logger()

    def start_job(self, filters: Dict[str, Any]) -> str:
        run_id = storage.create_hunter_run(filters)
        cancel_event = threading.Event()
        thread = threading.Thread(
            target=self._run_job,
            args=(run_id, filters, cancel_event, False),
            daemon=True,
        )
        _job_registry[run_id] = {"thread": thread, "cancel_event": cancel_event}
        thread.start()
        return run_id

    def resume_job(self, run_id: str) -> Optional[str]:
        if self.is_running(run_id):
            return run_id
        run = storage.get_hunter_run(run_id)
        if not run:
            return None
        if run.get("status") == STATUS_COMPLETED:
            return None
        filters = json.loads(run.get("filters_json") or "{}")
        cancel_event = threading.Event()
        thread = threading.Thread(
            target=self._run_job,
            args=(run_id, filters, cancel_event, True),
            daemon=True,
        )
        _job_registry[run_id] = {"thread": thread, "cancel_event": cancel_event}
        thread.start()
        return run_id

    def cancel_job(self, run_id: str) -> None:
        job = _job_registry.get(run_id)
        if job:
            job["cancel_event"].set()
        storage.update_hunter_run(run_id, status=STATUS_PAUSED, current_stage=STAGE_PAUSED)
        storage.log_event("warning", "v3_run_paused", {"run_id": run_id})

    def is_running(self, run_id: str) -> bool:
        job = _job_registry.get(run_id)
        if not job:
            return False
        return job["thread"].is_alive()

    def _update_stage(self, run_id: str, stage: str, status: Optional[str] = None, **extra: Any) -> None:
        fields: Dict[str, Any] = {"current_stage": stage}
        if status:
            fields["status"] = status
        fields.update(extra)
        storage.update_hunter_run(run_id, **fields)
        storage.log_event("info", "v3_stage", {"run_id": run_id, "stage": stage, **extra})
        self.logger.info("run=%s stage=%s status=%s", run_id, stage, status or "")

    def _ensure_not_canceled(self, run_id: str, cancel_event: threading.Event) -> None:
        if cancel_event.is_set():
            self._update_stage(run_id, STAGE_PAUSED, status=STATUS_PAUSED)
            raise JobCanceled("Job cancelado")

    def _build_payload(self, filters: Dict[str, Any], page: int, limit: int) -> Dict[str, Any]:
        payload_override = filters.get("payload") if isinstance(filters.get("payload"), dict) else None
        if payload_override:
            payload = dict(payload_override)
            payload["pagina"] = page
            payload["limite"] = limit
            return payload
        client = data_sources.CasaDosDadosClient()
        return client.build_payload(
            uf=filters.get("uf", ""),
            municipios=filters.get("municipios", []),
            cnaes=filters.get("cnaes", []),
            excluir_mei=filters.get("excluir_mei", True),
            com_telefone=filters.get("com_telefone", False),
            com_email=filters.get("com_email", False),
            pagina=page,
            limite=limit,
        )

    @retry(
        retry=retry_if_exception_type((requests.RequestException, RuntimeError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=1, max=10),
        reraise=True,
    )
    def probe_total(self, filters: Dict[str, Any], run_id: str) -> int:
        client = data_sources.CasaDosDadosClient()
        payload = self._build_payload(filters, page=1, limit=1)
        url = f"{data_sources.CASA_DOS_DADOS_BASE_URL}?tipo_resultado=completo"
        resp = client._post(url, payload, run_id=run_id, step_name="probe_total")
        if resp.status_code != 200:
            if data_sources._is_no_balance(resp):
                raise data_sources.CasaDosDadosBalanceError("Casa dos Dados: sem saldo para a operacao.")
            raise RuntimeError(f"Probe total falhou: {resp.status_code} {resp.text[:200]}")
        data = resp.json()
        return int(data.get("total") or 0)

    def _run_job(self, run_id: str, filters: Dict[str, Any], cancel_event: threading.Event, resume: bool) -> None:
        try:
            run = storage.get_hunter_run(run_id) or {}
            strategy = run.get("strategy") or ""
            total = int(run.get("total_leads") or 0)
            if resume:
                storage.update_hunter_run(run_id, status=STATUS_RUNNING)
            if not resume or not strategy:
                self._update_stage(run_id, STAGE_PROBE, status=STATUS_RUNNING)
                total = self.probe_total(filters, run_id)
                strategy = "REALTIME" if total < 1000 else "BULK"
                storage.update_hunter_run(run_id, strategy=strategy, total_leads=total)
                storage.log_event(
                    "info",
                    "v3_probe",
                    {"run_id": run_id, "total": total, "strategy": strategy},
                )
            self._ensure_not_canceled(run_id, cancel_event)
            if strategy == "REALTIME":
                self._run_realtime(run_id, filters, total, cancel_event, resume)
            else:
                self._run_bulk(run_id, filters, total, cancel_event, resume)
            self._ensure_not_canceled(run_id, cancel_event)
            self._update_stage(run_id, STAGE_COMPLETED, status=STATUS_COMPLETED)
            storage.log_event("info", "v3_completed", {"run_id": run_id})
        except JobCanceled:
            self.logger.info("run=%s canceled", run_id)
        except Exception as exc:
            storage.update_hunter_run(run_id, status=STATUS_FAILED, current_stage=STAGE_FAILED)
            storage.log_event("error", "v3_failed", {"run_id": run_id, "error": str(exc)})
            storage.record_error(run_id, "v3_orchestrator", str(exc), traceback.format_exc())
            self.logger.exception("run=%s failed", run_id)

    def _run_realtime(
        self,
        run_id: str,
        filters: Dict[str, Any],
        total: int,
        cancel_event: threading.Event,
        resume: bool,
    ) -> None:
        self._update_stage(run_id, STAGE_REALTIME_FETCH, status=STATUS_RUNNING)
        if resume:
            cached = storage.fetch_leads_raw_by_run(run_id)
            if cached:
                storage.log_event("info", "v3_realtime_resume", {"run_id": run_id, "cached": len(cached)})
                self._run_local_pipeline(run_id, filters, cached, cancel_event)
                return
        limit = int(filters.get("limite") or 0)
        if limit <= 0:
            limit = total
        page_size = int(filters.get("page_size") or 200)
        payload = self._build_payload(filters, page=1, limit=min(page_size, 1000))
        items, telemetry = data_sources.search_v5(
            payload=payload,
            limit=limit,
            page_size=page_size,
            run_id=run_id,
        )
        normalized = [data_sources.normalize_casa_dos_dados(item) for item in items]
        storage.upsert_leads_raw(normalized, source=f"v3_realtime:{run_id}", run_id=run_id)
        storage.update_hunter_run(
            run_id,
            processed_count=len(normalized),
            total_leads=total or len(normalized),
        )
        storage.log_event(
            "info",
            "v3_realtime_fetch",
            {"run_id": run_id, "count": len(normalized), **telemetry},
        )
        self._ensure_not_canceled(run_id, cancel_event)
        self._run_local_pipeline(run_id, filters, normalized, cancel_event)

    def _ensure_export(self, run_id: str, filters: Dict[str, Any], total: int) -> str:
        job = storage.get_export_job(run_id) or {}
        export_uuid = job.get("export_uuid_cd")
        if export_uuid:
            return export_uuid
        self._update_stage(run_id, STAGE_BULK_EXPORT_REQUEST, status=STATUS_RUNNING)
        payload = self._build_payload(filters, page=1, limit=min(int(filters.get("page_size") or 200), 1000))
        total_linhas = int(filters.get("limite") or 0)
        export_info = data_sources.export_create_v5(
            payload,
            run_id=run_id,
            total_linhas=total_linhas if total_linhas > 0 else total,
        )
        export_uuid = export_info.get("arquivo_uuid") or uuid4().hex
        storage.upsert_export_job(run_id, export_uuid_cd=export_uuid)
        storage.log_event("info", "v3_export_created", {"run_id": run_id, "arquivo_uuid": export_uuid})
        return export_uuid

    def _poll_public_link(self, export_uuid: str, run_id: str) -> Optional[Dict[str, Any]]:
        client = data_sources.CasaDosDadosClient()
        url = data_sources.CASA_DOS_DADOS_EXPORT_STATUS_V4_PUBLIC_URL.format(arquivo_uuid=export_uuid)
        resp = client._get(url, run_id=run_id, step_name="v3_export_poll_public")
        if resp.status_code == 202:
            return {"status": "processing"}
        if resp.status_code == 200:
            data = resp.json()
            link = data.get("link") or data.get("url")
            return {
                "status": "ready",
                "link": link,
                "expires_at": time.time() + data_sources.CASA_EXPORT_LINK_TTL_SECONDS,
            }
        return {"status": "error", "status_code": resp.status_code, "body": resp.text}

    def _poll_export_link(self, run_id: str, export_uuid: str, cancel_event: threading.Event) -> str:
        self._update_stage(run_id, STAGE_BULK_POLL, status=STATUS_RUNNING)
        job = storage.get_export_job(run_id) or {}
        if job.get("file_url"):
            return job["file_url"]
        max_attempts = 30
        backoff = 2
        for attempt in range(max_attempts):
            self._ensure_not_canceled(run_id, cancel_event)
            try:
                result = self._poll_public_link(export_uuid, run_id)
            except Exception as exc:
                storage.log_event(
                    "warning",
                    "v3_export_poll_public_failed",
                    {"run_id": run_id, "error": str(exc)},
                )
                result = {"status": "error"}
            if result.get("status") == "ready" and result.get("link"):
                expires_at = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(result["expires_at"]))
                storage.upsert_export_job(
                    run_id,
                    export_uuid_cd=export_uuid,
                    file_url=result["link"],
                    expires_at=expires_at,
                )
                return result["link"]
            if result.get("status") == "processing":
                time.sleep(backoff)
                continue

            items = []
            try:
                items = data_sources.export_list_v4(page=1, run_id=run_id)
            except Exception as exc:
                storage.log_event("warning", "v3_export_list_failed", {"run_id": run_id, "error": str(exc)})
            for item in items:
                if item.get("arquivo_uuid") != export_uuid and item.get("arquivoUUID") != export_uuid:
                    continue
                status = str(item.get("status") or "").lower()
                link = item.get("link") or item.get("url")
                if status in {"processado", "processado_com_erro", "ready", "done"} and link:
                    storage.upsert_export_job(
                        run_id,
                        export_uuid_cd=export_uuid,
                        file_url=link,
                        expires_at=None,
                    )
                    return link
                if status in {"processado", "processado_com_erro"}:
                    break
            time.sleep(backoff)
            backoff = min(backoff + 1, 10)
        raise RuntimeError("Timeout aguardando export bulk")

    @retry(
        retry=retry_if_exception_type((requests.RequestException, RuntimeError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=1, max=15),
        reraise=True,
    )
    def _download_file(self, run_id: str, export_uuid: str, url: str) -> str:
        self._update_stage(run_id, STAGE_BULK_DOWNLOAD, status=STATUS_RUNNING)
        tmp_dir = Path("tmp")
        tmp_dir.mkdir(parents=True, exist_ok=True)
        file_path = tmp_dir / f"{export_uuid}.csv"
        with requests.get(url, stream=True, timeout=60) as resp:
            if resp.status_code != 200:
                raise RuntimeError(f"Falha download export: {resp.status_code}")
            with open(file_path, "wb") as handle:
                for chunk in resp.iter_content(chunk_size=1024 * 128):
                    if not chunk:
                        continue
                    handle.write(chunk)
        storage.upsert_export_job(run_id, export_uuid_cd=export_uuid, file_path_local=str(file_path))
        storage.log_event("info", "v3_export_downloaded", {"run_id": run_id, "file_path": str(file_path)})
        return str(file_path)

    def _import_csv(self, run_id: str, export_uuid: str, file_path: str, cancel_event: threading.Event) -> None:
        self._update_stage(run_id, STAGE_BULK_IMPORT, status=STATUS_RUNNING)
        total_imported = 0
        for chunk in pd.read_csv(file_path, chunksize=5000, dtype=str):
            self._ensure_not_canceled(run_id, cancel_event)
            try:
                rows = chunk.fillna("").to_dict(orient="records")
                normalized = [data_sources.normalize_export_row(row) for row in rows]
                storage.upsert_leads_raw(
                    normalized,
                    source=f"v3_export:{export_uuid}",
                    run_id=run_id,
                    export_uuid=export_uuid,
                )
                total_imported += len(normalized)
                storage.update_hunter_run(run_id, processed_count=total_imported, total_leads=total_imported)
            except Exception as exc:
                storage.log_event(
                    "error",
                    "v3_import_chunk_failed",
                    {"run_id": run_id, "error": str(exc)},
                )
        storage.log_event("info", "v3_import_completed", {"run_id": run_id, "rows": total_imported})

    def _run_bulk(
        self,
        run_id: str,
        filters: Dict[str, Any],
        total: int,
        cancel_event: threading.Event,
        resume: bool,
    ) -> None:
        export_uuid = self._ensure_export(run_id, filters, total)
        self._ensure_not_canceled(run_id, cancel_event)
        link = None
        job = storage.get_export_job(run_id) or {}
        if job.get("file_url"):
            link = job.get("file_url")
        if not link:
            link = self._poll_export_link(run_id, export_uuid, cancel_event)
        self._ensure_not_canceled(run_id, cancel_event)
        file_path = job.get("file_path_local") if job else None
        if not file_path or not Path(file_path).exists():
            file_path = self._download_file(run_id, export_uuid, link)
        self._ensure_not_canceled(run_id, cancel_event)
        self._import_csv(run_id, export_uuid, file_path, cancel_event)
        self._ensure_not_canceled(run_id, cancel_event)
        leads_raw = storage.fetch_leads_raw_by_run(run_id)
        self._run_local_pipeline(run_id, filters, leads_raw, cancel_event)

    def _run_local_pipeline(
        self,
        run_id: str,
        filters: Dict[str, Any],
        leads_raw: List[Dict[str, Any]],
        cancel_event: threading.Event,
    ) -> None:
        self._update_stage(run_id, STAGE_LOCAL_PIPELINE, status=STATUS_RUNNING)
        if not leads_raw:
            raise RuntimeError("Nenhum lead carregado para pipeline local")
        cleaned, stats = cleaning.clean_batch(
            leads_raw,
            exclude_mei=filters.get("excluir_mei", True),
            min_repeat=int(filters.get("telefone_repeat_threshold") or 5),
            return_stats=True,
        )
        for lead in cleaned:
            lead["score_v1"] = scoring.score_v1(lead)
        storage.upsert_leads_clean(cleaned)
        storage.update_hunter_run(run_id, processed_count=len(cleaned), total_leads=len(cleaned))
        storage.log_event("info", "v3_cleaned", {"run_id": run_id, **stats})
        self._ensure_not_canceled(run_id, cancel_event)

        enable_enrichment = bool(filters.get("enable_enrichment", True))
        enrich_top_pct = int(filters.get("enrich_top_pct") or 25)
        enrichments: List[Dict[str, Any]] = []
        if enable_enrichment and cleaned:
            sorted_clean = sorted(cleaned, key=lambda x: x.get("score_v1", 0), reverse=True)
            top_n = max(1, int(len(sorted_clean) * enrich_top_pct / 100))
            to_enrich = sorted_clean[:top_n]
            provider = providers.select_provider(filters.get("provider") or "serper")
            try:
                enrichments = asyncio.run(
                    enrichment_async.enrich_leads_hybrid(
                        to_enrich,
                        provider=provider,
                        concurrency=int(filters.get("concurrency") or 8),
                        timeout=int(filters.get("timeout") or 5),
                    )
                )
            except Exception as exc:
                storage.log_event("error", "v3_enrich_failed", {"run_id": run_id, "error": str(exc)})
            for item in enrichments:
                storage.upsert_enrichment(item.get("cnpj"), item)

        enrichment_map = {item.get("cnpj"): item for item in enrichments}
        for lead in cleaned:
            enrichment = enrichment_map.get(lead.get("cnpj"), {})
            lead["score_v2"] = scoring.score_v2(lead, enrichment)
            lead["score_label"] = scoring.label(lead["score_v2"])

        storage.upsert_leads_clean(cleaned)
        storage.log_event(
            "info",
            "v3_pipeline_completed",
            {"run_id": run_id, "total_cleaned": len(cleaned), "enriched": len(enrichments)},
        )
