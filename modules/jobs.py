"""Background job orchestration for Hunter OS."""

import asyncio
import json
import threading
from typing import Any, Dict, List, Optional

from modules import cleaning, data_sources, enrichment_async, providers, scoring, storage

_job_registry: Dict[str, Dict[str, Any]] = {}


def _update_status(run_id: str, status: str, **extra: Any) -> None:
    storage.update_run(run_id, status=status, **extra)
    storage.log_event("info", "run_status", {"run_id": run_id, "status": status, **extra})


def _run_pipeline(run_id: str, params: Dict[str, Any], cancel_event: threading.Event) -> None:
    try:
        _update_status(run_id, "extracting", total_leads=0, enriched_count=0, errors_count=0)
        leads_raw, total, source = data_sources.extract_leads(
            uf=params["uf"],
            municipios=params["municipios"],
            cnaes=params["cnaes"],
            excluir_mei=params["excluir_mei"],
            com_telefone=params["com_telefone"],
            com_email=params["com_email"],
            limite=params["limite"],
            mode=params["mode"],
            cache_ttl_hours=params["cache_ttl_hours"],
        )

        if cancel_event.is_set():
            _update_status(run_id, "canceled")
            return

        _update_status(run_id, "cleaning", total_leads=len(leads_raw))
        cleaned = cleaning.clean_batch(
            leads_raw,
            exclude_mei=params["excluir_mei"],
            min_repeat=params["telefone_repeat_threshold"],
        )

        for lead in cleaned:
            lead["score_v1"] = scoring.score_v1(lead)

        storage.upsert_leads_clean(cleaned)

        if cancel_event.is_set():
            _update_status(run_id, "canceled")
            return

        _update_status(run_id, "scoring_v1")

        # Select top X% for enrichment
        top_pct = params.get("enrich_top_pct", 25)
        sorted_clean = sorted(cleaned, key=lambda x: x.get("score_v1", 0), reverse=True)
        top_n = max(1, int(len(sorted_clean) * top_pct / 100)) if sorted_clean else 0
        to_enrich = sorted_clean[:top_n]

        enriched_results: List[Dict[str, Any]] = []
        if params.get("enable_enrichment") and to_enrich:
            _update_status(run_id, "enriching")
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

            enriched_results = asyncio.run(enricher.enrich_batch(to_enrich, run_id, cancel_event=async_cancel))

            for item in enriched_results:
                storage.upsert_enrichment(item.get("cnpj"), item)

        if cancel_event.is_set():
            _update_status(run_id, "canceled")
            return

        _update_status(run_id, "scoring_v2")

        enrichment_map = {item.get("cnpj"): item for item in enriched_results}
        for lead in cleaned:
            enrichment = enrichment_map.get(lead.get("cnpj"), {})
            lead["score_v2"] = scoring.score_v2(lead, enrichment)
            lead["score_label"] = scoring.label(lead["score_v2"])

        storage.upsert_leads_clean(cleaned)

        _update_status(
            run_id,
            "completed",
            enriched_count=len(enriched_results),
            errors_count=0,
        )
    except Exception as exc:
        storage.log_event("error", "run_failed", {"run_id": run_id, "error": str(exc)})
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
