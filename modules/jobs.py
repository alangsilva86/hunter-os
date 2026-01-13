"""Background job orchestration for Hunter OS."""

import asyncio
import json
import os
import threading
import time
import traceback
from typing import Any, Dict, List, Optional

from modules import cleaning, data_sources, enrichment_async, providers, scoring, storage

_job_registry: Dict[str, Dict[str, Any]] = {}


def _update_status(run_id: str, status: str, **extra: Any) -> None:
    storage.update_run(run_id, status=status, **extra)
    storage.log_event("info", "run_status", {"run_id": run_id, "status": status, **extra})


def _log_info(run_id: str, event: str, message: str, **extra: Any) -> None:
    storage.log_event("info", event, {"run_id": run_id, "message": message, **extra})


def _log_warning(run_id: str, event: str, message: str, **extra: Any) -> None:
    storage.log_event("warning", event, {"run_id": run_id, "message": message, **extra})

def _process_leads(
    run_id: str,
    params: Dict[str, Any],
    leads_raw: List[Dict[str, Any]],
    cancel_event: threading.Event,
) -> Dict[str, Any]:
    _update_status(run_id, "cleaning", total_leads=len(leads_raw))
    _log_info(
        run_id,
        "cleaning_start",
        "Iniciando limpeza e deduplicacao dos leads coletados.",
        total_leads=len(leads_raw),
    )
    step_start = time.time()
    cleaned, clean_stats = cleaning.clean_batch(
        leads_raw,
        exclude_mei=params["excluir_mei"],
        min_repeat=params["telefone_repeat_threshold"],
        return_stats=True,
    )
    storage.record_run_step(
        run_id=run_id,
        step_name="cleaning",
        status="completed",
        started_at=time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(step_start)),
        ended_at=storage._utcnow(),
        duration_ms=int((time.time() - step_start) * 1000),
        details=clean_stats,
    )
    _log_info(
        run_id,
        "cleaning_summary",
        "Limpeza concluida.",
        input_count=clean_stats.get("input_count"),
        output_count=clean_stats.get("output_count"),
        removed_mei=clean_stats.get("removed_mei"),
        removed_other=clean_stats.get("removed_other"),
    )

    for lead in cleaned:
        lead["score_v1"] = scoring.score_v1(lead)

    storage.upsert_leads_clean(cleaned)

    if cancel_event.is_set():
        _update_status(run_id, "canceled")
        return {
            "enriched_results": [],
            "enrich_stats": {
                "provider_error": None,
                "provider_error_count": 0,
                "skipped_due_to_provider_error": False,
            },
        }

    _update_status(run_id, "scoring_v1")
    storage.record_run_step(
        run_id=run_id,
        step_name="scoring_v1",
        status="completed",
        started_at=storage._utcnow(),
        ended_at=storage._utcnow(),
        duration_ms=0,
        details={"leads": len(cleaned)},
    )
    _log_info(
        run_id,
        "scoring_v1_summary",
        "Score v1 concluido.",
        leads=len(cleaned),
    )

    top_pct = params.get("enrich_top_pct", 25)
    sorted_clean = sorted(cleaned, key=lambda x: x.get("score_v1", 0), reverse=True)
    top_n = max(1, int(len(sorted_clean) * top_pct / 100)) if sorted_clean else 0
    to_enrich = sorted_clean[:top_n]

    enriched_results: List[Dict[str, Any]] = []
    enrich_stats = {
        "provider_error": None,
        "provider_error_count": 0,
        "skipped_due_to_provider_error": False,
    }
    if params.get("enable_enrichment") and to_enrich:
        _update_status(run_id, "enriching")
        _log_info(
            run_id,
            "enrichment_start",
            "Iniciando enriquecimento externo.",
            provider=params.get("provider"),
            alvo_enriquecimento=len(to_enrich),
        )
        step_start = time.time()
        provider = providers.select_provider(params["provider"])
        enricher = enrichment_async.AsyncEnricher(
            provider=provider,
            concurrency=params.get("concurrency", 10),
            timeout=params.get("timeout", 5),
            cache_ttl_hours=params.get("cache_ttl_hours", 24),
        )

        async_cancel = asyncio.Event()
        if cancel_event.is_set():
            async_cancel.set()

        enriched_results, enrich_stats = asyncio.run(enricher.enrich_batch(to_enrich, run_id, cancel_event=async_cancel))

        for item in enriched_results:
            storage.upsert_enrichment(item.get("cnpj"), item)

        storage.record_run_step(
            run_id=run_id,
            step_name="enriching",
            status="completed",
            started_at=time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(step_start)),
            ended_at=storage._utcnow(),
            duration_ms=int((time.time() - step_start) * 1000),
            details={
                "alvo_enriquecimento": len(to_enrich),
                "enriquecidos": len(enriched_results),
                "top_pct": top_pct,
                "provider_error": enrich_stats.get("provider_error"),
                "provider_error_count": enrich_stats.get("provider_error_count"),
                "skipped_due_to_provider_error": enrich_stats.get("skipped_due_to_provider_error"),
            },
        )
        _log_info(
            run_id,
            "enrichment_summary",
            "Enriquecimento concluido.",
            enriquecidos=len(enriched_results),
            alvo_enriquecimento=len(to_enrich),
            provider=params.get("provider"),
            provider_error=enrich_stats.get("provider_error"),
        )
        if enrich_stats.get("provider_error"):
            _log_warning(
                run_id,
                "enrichment_paused",
                "Enriquecimento pausado por erro do provider.",
                provider=params.get("provider"),
                error=enrich_stats.get("provider_error"),
            )

    if cancel_event.is_set():
        _update_status(run_id, "canceled")
        return {"enriched_results": enriched_results, "enrich_stats": enrich_stats}

    _update_status(run_id, "scoring_v2")
    step_start = time.time()

    enrichment_map = {item.get("cnpj"): item for item in enriched_results}
    for lead in cleaned:
        enrichment = enrichment_map.get(lead.get("cnpj"), {})
        lead["score_v2"] = scoring.score_v2(lead, enrichment)
        lead["score_label"] = scoring.label(lead["score_v2"])

    storage.upsert_leads_clean(cleaned)
    storage.record_run_step(
        run_id=run_id,
        step_name="scoring_v2",
        status="completed",
        started_at=time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(step_start)),
        ended_at=storage._utcnow(),
        duration_ms=int((time.time() - step_start) * 1000),
        details={"leads": len(cleaned)},
    )
    _log_info(
        run_id,
        "scoring_v2_summary",
        "Score v2 concluido.",
        leads=len(cleaned),
    )
    return {"enriched_results": enriched_results, "enrich_stats": enrich_stats}


def _run_pipeline(run_id: str, params: Dict[str, Any], cancel_event: threading.Event) -> None:
    try:
        _update_status(run_id, "extracting", total_leads=0, enriched_count=0, errors_count=0)
        _log_info(
            run_id,
            "run_start",
            "Run iniciado.",
            run_type=params.get("run_type"),
            mode=params.get("mode"),
            limite=params.get("limite"),
            export_all=bool(params.get("export_all")),
        )

        if params.get("export_all"):
            _log_info(
                run_id,
                "export_create_start",
                "Criando export na Casa dos Dados (sem paginar para economizar saldo).",
            )
            step_start = time.time()
            export_payload = data_sources.CasaDosDadosClient().build_payload(
                uf=params["uf"],
                municipios=params["municipios"],
                cnaes=params["cnaes"],
                excluir_mei=params["excluir_mei"],
                com_telefone=params["com_telefone"],
                com_email=params["com_email"],
                pagina=1,
                limite=min(int(params.get("page_size", 200)), 1000),
            )
            export_info = data_sources.export_create_v5(export_payload, run_id=run_id)
            storage.record_run_step(
                run_id=run_id,
                step_name="export_create_v5",
                status="completed",
                started_at=time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(step_start)),
                ended_at=storage._utcnow(),
                duration_ms=int((time.time() - step_start) * 1000),
                details={
                    "arquivo_uuid": export_info.get("arquivo_uuid"),
                    "payload_fingerprint": export_info.get("payload_fingerprint"),
                },
            )
            _log_info(
                run_id,
                "export_created",
                "Export criado. Acompanhe o status na aba Exports.",
                arquivo_uuid=export_info.get("arquivo_uuid"),
            )
            _update_status(run_id, "export_created", errors_count=0)
            return

        _log_info(
            run_id,
            "extract_start",
            "Iniciando consulta na Casa dos Dados.",
            limite=params.get("limite"),
            page_size=params.get("page_size"),
        )
        step_start = time.time()
        leads_raw, telemetry, source = data_sources.extract_leads(
            uf=params["uf"],
            municipios=params["municipios"],
            cnaes=params["cnaes"],
            excluir_mei=params["excluir_mei"],
            com_telefone=params["com_telefone"],
            com_email=params["com_email"],
            limite=params["limite"],
            mode=params.get("mode", "pagination"),
            cache_ttl_hours=params["cache_ttl_hours"],
            run_id=run_id,
            page_size=int(params.get("page_size", 200)),
        )
        storage.record_run_step(
            run_id=run_id,
            step_name="extract",
            status="completed",
            started_at=time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(step_start)),
            ended_at=storage._utcnow(),
            duration_ms=int((time.time() - step_start) * 1000),
            details={
                **telemetry,
                "limite_solicitado": params["limite"],
                "source": source,
            },
        )
        _log_info(
            run_id,
            "extract_summary",
            "Consulta concluida.",
            total_encontrado=telemetry.get("total_encontrado"),
            itens_coletados=telemetry.get("itens_coletados"),
            descartados_por_limite=telemetry.get("itens_descartados_por_limite"),
            pages_processed=telemetry.get("pages_processed"),
            source=source,
            cache_hit=telemetry.get("cache_hit"),
        )

        if cancel_event.is_set():
            _update_status(run_id, "canceled")
            _log_warning(run_id, "run_canceled", "Run cancelado pelo usuario.")
            return

        process_result = _process_leads(run_id, params, leads_raw, cancel_event)
        enriched_results = process_result.get("enriched_results", [])
        enrich_stats = process_result.get("enrich_stats", {})

        _update_status(
            run_id,
            "completed",
            enriched_count=len(enriched_results),
            errors_count=1 if enrich_stats.get("provider_error") else 0,
        )
        _log_info(
            run_id,
            "run_completed",
            "Run concluido.",
            total_leads=len(leads_raw),
            enriched_count=len(enriched_results),
            provider_error=enrich_stats.get("provider_error"),
        )
    except data_sources.CasaDosDadosBalanceError as exc:
        storage.log_event(
            "error",
            "run_failed",
            {"run_id": run_id, "error": str(exc), "error_code": "no_balance"},
        )
        storage.record_error(run_id, "extract", str(exc))
        _update_status(run_id, "failed", errors_count=1)
        return
    except Exception as exc:
        storage.log_event("error", "run_failed", {"run_id": run_id, "error": str(exc)})
        storage.record_error(run_id, "pipeline", str(exc), traceback.format_exc())
        _update_status(run_id, "failed", errors_count=1)


def start_run(params: Dict[str, Any]) -> str:
    run_id = storage.create_run(params)
    cancel_event = threading.Event()
    thread = threading.Thread(target=_run_pipeline, args=(run_id, params, cancel_event), daemon=True)
    _job_registry[run_id] = {
        "thread": thread,
        "cancel_event": cancel_event,
    }
    thread.start()
    return run_id


def _run_recovery(run_id: str, params: Dict[str, Any], arquivo_uuid: str, cancel_event: threading.Event) -> None:
    try:
        _update_status(run_id, "importing", total_leads=0, enriched_count=0, errors_count=0)
        _log_info(
            run_id,
            "recovery_start",
            "Iniciando recovery a partir do CSV exportado.",
            arquivo_uuid=arquivo_uuid,
        )
        files = storage.fetch_export_files(arquivo_uuid, limit=1)
        if not files:
            raise RuntimeError("Arquivo CSV nao encontrado para o export selecionado")
        file_path = files[0]["file_path"]
        step_start = time.time()
        leads_raw = data_sources.parse_export_csv(file_path)
        source = f"export_csv:{arquivo_uuid}"
        storage.insert_leads_raw(leads_raw, source, run_id=run_id, export_uuid=arquivo_uuid)
        storage.record_run_step(
            run_id=run_id,
            step_name="import_csv",
            status="completed",
            started_at=time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(step_start)),
            ended_at=storage._utcnow(),
            duration_ms=int((time.time() - step_start) * 1000),
            details={"arquivo_uuid": arquivo_uuid, "file_path": file_path, "rows": len(leads_raw)},
        )
        _log_info(
            run_id,
            "import_csv_summary",
            "CSV importado com sucesso.",
            arquivo_uuid=arquivo_uuid,
            file_path=file_path,
            rows=len(leads_raw),
        )

        if cancel_event.is_set():
            _update_status(run_id, "canceled")
            _log_warning(run_id, "run_canceled", "Run cancelado pelo usuario.")
            return

        process_result = _process_leads(run_id, params, leads_raw, cancel_event)
        enriched_results = process_result.get("enriched_results", [])
        enrich_stats = process_result.get("enrich_stats", {})
        _update_status(
            run_id,
            "completed",
            enriched_count=len(enriched_results),
            errors_count=1 if enrich_stats.get("provider_error") else 0,
        )
        _log_info(
            run_id,
            "run_completed",
            "Recovery concluido.",
            total_leads=len(leads_raw),
            enriched_count=len(enriched_results),
            provider_error=enrich_stats.get("provider_error"),
        )
    except Exception as exc:
        storage.log_event("error", "run_failed", {"run_id": run_id, "error": str(exc)})
        storage.record_error(run_id, "recovery", str(exc), traceback.format_exc())
        _update_status(run_id, "failed", errors_count=1)


def start_recovery(arquivo_uuid: str, params: Dict[str, Any]) -> str:
    run_id = storage.create_run(params)
    cancel_event = threading.Event()
    thread = threading.Thread(
        target=_run_recovery,
        args=(run_id, params, arquivo_uuid, cancel_event),
        daemon=True,
    )
    _job_registry[run_id] = {
        "thread": thread,
        "cancel_event": cancel_event,
    }
    thread.start()
    return run_id


def _run_excel_import(run_id: str, params: Dict[str, Any], file_path: str, cancel_event: threading.Event) -> None:
    try:
        _update_status(run_id, "importing", total_leads=0, enriched_count=0, errors_count=0)
        _log_info(
            run_id,
            "excel_import_start",
            "Iniciando importacao de Excel.",
            file_path=file_path,
            cnpj_column=params.get("cnpj_column"),
        )
        step_start = time.time()
        leads_raw = data_sources.parse_excel_file(file_path, cnpj_column=params.get("cnpj_column"))
        if not leads_raw:
            raise RuntimeError("Nenhum CNPJ valido encontrado no arquivo.")
        source = f"upload_excel:{os.path.basename(file_path)}"
        storage.insert_leads_raw(leads_raw, source, run_id=run_id)
        storage.record_run_step(
            run_id=run_id,
            step_name="import_excel",
            status="completed",
            started_at=time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(step_start)),
            ended_at=storage._utcnow(),
            duration_ms=int((time.time() - step_start) * 1000),
            details={
                "file_path": file_path,
                "rows": len(leads_raw),
                "source": source,
                "cnpj_column": params.get("cnpj_column"),
            },
        )
        _log_info(
            run_id,
            "excel_import_summary",
            "Excel importado com sucesso.",
            file_path=file_path,
            rows=len(leads_raw),
        )

        if cancel_event.is_set():
            _update_status(run_id, "canceled")
            _log_warning(run_id, "run_canceled", "Run cancelado pelo usuario.")
            return

        process_result = _process_leads(run_id, params, leads_raw, cancel_event)
        enriched_results = process_result.get("enriched_results", [])
        enrich_stats = process_result.get("enrich_stats", {})
        _update_status(
            run_id,
            "completed",
            enriched_count=len(enriched_results),
            errors_count=1 if enrich_stats.get("provider_error") else 0,
        )
        _log_info(
            run_id,
            "run_completed",
            "Importacao Excel concluida.",
            total_leads=len(leads_raw),
            enriched_count=len(enriched_results),
            provider_error=enrich_stats.get("provider_error"),
        )
    except Exception as exc:
        storage.log_event("error", "run_failed", {"run_id": run_id, "error": str(exc)})
        storage.record_error(run_id, "import_excel", str(exc), traceback.format_exc())
        _update_status(run_id, "failed", errors_count=1)


def start_excel_import(file_path: str, params: Dict[str, Any]) -> str:
    run_id = storage.create_run(params)
    cancel_event = threading.Event()
    thread = threading.Thread(
        target=_run_excel_import,
        args=(run_id, params, file_path, cancel_event),
        daemon=True,
    )
    _job_registry[run_id] = {
        "thread": thread,
        "cancel_event": cancel_event,
    }
    thread.start()
    return run_id


def resume_run(run_id: str) -> Optional[str]:
    if is_running(run_id):
        return run_id
    run = storage.get_run(run_id)
    if not run:
        return None
    params = json.loads(run.get("params_json") or "{}")
    cancel_event = threading.Event()
    thread = threading.Thread(target=_run_pipeline, args=(run_id, params, cancel_event), daemon=True)
    _job_registry[run_id] = {
        "thread": thread,
        "cancel_event": cancel_event,
    }
    thread.start()
    return run_id


def cancel_run(run_id: str) -> None:
    job = _job_registry.get(run_id)
    if job:
        job["cancel_event"].set()
        storage.update_run(run_id, status="canceled")


def is_running(run_id: str) -> bool:
    job = _job_registry.get(run_id)
    if not job:
        return False
    return job["thread"].is_alive()
