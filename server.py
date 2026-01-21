from __future__ import annotations

import io
import math
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from fastapi import FastAPI, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

from etl_pipeline import HunterOrchestrator
from modules import exports as exports_module
from modules import storage

BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "web" / "templates"
STATIC_DIR = BASE_DIR / "web" / "static"

app = FastAPI(title="Hunter OS", version="1.0.0")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

storage.init_db()
orchestrator = HunterOrchestrator()


def _env_int(key: str, default: int) -> int:
    raw = os.getenv(key, str(default))
    try:
        return int(raw)
    except ValueError:
        return default


def _is_checked(value: Optional[str]) -> bool:
    return str(value or "").lower() in {"on", "true", "1", "yes"}


def _parse_csv(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _parse_cnaes(value: Optional[str]) -> List[str]:
    cnaes: List[str] = []
    for item in _parse_csv(value):
        digits = re.sub(r"\D", "", item)
        if digits:
            cnaes.append(digits)
    return list(dict.fromkeys(cnaes))


def _to_int(value: Optional[str], default: int) -> int:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _to_int_optional(value: Optional[str]) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _to_float_optional(value: Optional[str]) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _build_vault_filters(
    min_score: Optional[str],
    min_tech_score: Optional[str],
    min_wealth: Optional[str],
    contact_quality: Optional[str],
    municipio: Optional[str],
    uf: Optional[str],
    has_marketing: Optional[str],
) -> Dict[str, Any]:
    return {
        "min_score": _to_int_optional(min_score),
        "min_tech_score": _to_int_optional(min_tech_score),
        "min_wealth": _to_float_optional(min_wealth),
        "contact_quality": contact_quality or None,
        "municipio": municipio or None,
        "uf": uf or None,
        "has_marketing": None if has_marketing in {None, ""} else _is_checked(has_marketing),
    }


def _stage_labels() -> Dict[str, str]:
    return {
        "PROBE": "Probe",
        "REALTIME_FETCH": "Realtime fetch",
        "BULK_EXPORT_REQUEST": "Bulk export request",
        "BULK_POLL": "Bulk poll",
        "BULK_DOWNLOAD": "Bulk download",
        "BULK_IMPORT": "Bulk import",
        "LOCAL_PIPELINE": "Local pipeline",
        "COMPLETED": "Completed",
        "FAILED": "Failed",
        "PAUSED": "Paused",
    }


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def root() -> RedirectResponse:
    return RedirectResponse(url="/mission", status_code=302)


@app.get("/mission", response_class=HTMLResponse)
def mission(request: Request, run_id: Optional[str] = Query(None)) -> HTMLResponse:
    runs = storage.list_hunter_runs(limit=10)
    active_run_id = run_id or (runs[0]["id"] if runs else None)
    active_run = storage.get_hunter_run(active_run_id) if active_run_id else None
    logs = storage.fetch_logs(limit=15, run_id=active_run_id) if active_run_id else []
    running = orchestrator.is_running(active_run_id) if active_run_id else False
    progress = 0
    if active_run:
        total = int(active_run.get("total_leads") or 0)
        processed = int(active_run.get("processed_count") or 0)
        if total > 0:
            progress = min(100, int((processed / max(total, 1)) * 100))
    defaults = {
        "uf": "PR",
        "municipios": "MARINGA",
        "cnaes": "",
        "limite": "1000",
        "page_size": "200",
        "cache_ttl_hours": str(_env_int("CACHE_TTL_HOURS", 24)),
        "telefone_repeat_threshold": "5",
        "enrich_top_pct": "25",
        "provider": os.getenv("SEARCH_PROVIDER", "serper"),
    }
    return templates.TemplateResponse(
        "mission.html",
        {
            "request": request,
            "active_run": active_run,
            "active_run_id": active_run_id,
            "runs": runs,
            "logs": logs,
            "defaults": defaults,
            "stage_labels": _stage_labels(),
            "running": running,
            "progress": progress,
        },
    )


@app.post("/mission/start")
def mission_start(
    uf: str = Form(""),
    municipios: str = Form(""),
    cnaes: str = Form(""),
    excluir_mei: Optional[str] = Form(None),
    com_telefone: Optional[str] = Form(None),
    com_email: Optional[str] = Form(None),
    limite: str = Form("0"),
    page_size: str = Form("200"),
    cache_ttl_hours: str = Form("24"),
    telefone_repeat_threshold: str = Form("5"),
    enrich_top_pct: str = Form("25"),
    enable_enrichment: Optional[str] = Form(None),
    provider: str = Form("serper"),
) -> RedirectResponse:
    filters = {
        "uf": uf.strip(),
        "municipios": _parse_csv(municipios),
        "cnaes": _parse_cnaes(cnaes),
        "excluir_mei": _is_checked(excluir_mei),
        "com_telefone": _is_checked(com_telefone),
        "com_email": _is_checked(com_email),
        "limite": _to_int(limite, 0),
        "page_size": _to_int(page_size, 200),
        "cache_ttl_hours": _to_int(cache_ttl_hours, _env_int("CACHE_TTL_HOURS", 24)),
        "telefone_repeat_threshold": _to_int(telefone_repeat_threshold, 5),
        "enrich_top_pct": _to_int(enrich_top_pct, 25),
        "enable_enrichment": _is_checked(enable_enrichment),
        "provider": provider.strip() or os.getenv("SEARCH_PROVIDER", "serper"),
        "concurrency": _env_int("CONCURRENCY", 10),
        "timeout": _env_int("TIMEOUT", 5),
    }
    run_id = orchestrator.start_job(filters)
    return RedirectResponse(url=f"/mission?run_id={run_id}", status_code=303)


@app.post("/mission/{run_id}/pause")
def mission_pause(run_id: str) -> RedirectResponse:
    orchestrator.cancel_job(run_id)
    return RedirectResponse(url=f"/mission?run_id={run_id}", status_code=303)


@app.post("/mission/{run_id}/resume")
def mission_resume(run_id: str) -> RedirectResponse:
    orchestrator.resume_job(run_id)
    return RedirectResponse(url=f"/mission?run_id={run_id}", status_code=303)


@app.get("/vault", response_class=HTMLResponse)
def vault(
    request: Request,
    status: str = Query("all"),
    min_score: Optional[str] = Query(None),
    min_tech_score: Optional[str] = Query(None),
    min_wealth: Optional[str] = Query(None),
    contact_quality: Optional[str] = Query(None),
    municipio: Optional[str] = Query(None),
    uf: Optional[str] = Query(None),
    has_marketing: Optional[str] = Query(None),
    page: int = Query(1),
    page_size: int = Query(100),
) -> HTMLResponse:
    status_filter = status if status in {"all", "enriched", "pending"} else "all"
    page_size = max(10, min(int(page_size), 500))
    page = max(1, int(page))
    filters = _build_vault_filters(
        min_score=min_score,
        min_tech_score=min_tech_score,
        min_wealth=min_wealth,
        contact_quality=contact_quality,
        municipio=municipio,
        uf=uf,
        has_marketing=has_marketing,
    )
    total = storage.count_vault_data(filters, status_filter=status_filter)
    max_page = max(1, math.ceil(total / page_size)) if page_size else 1
    if page > max_page:
        page = max_page
    rows = storage.get_vault_data(page, page_size, filters, status_filter=status_filter)
    query_string = str(request.url.query) if request.url.query else ""
    export_prefix = "/vault/export?"
    if query_string:
        export_prefix = f"/vault/export?{query_string}&"
    prev_link = None
    next_link = None
    if page > 1:
        prev_link = str(request.url.include_query_params(page=page - 1))
    if page < max_page:
        next_link = str(request.url.include_query_params(page=page + 1))
    return templates.TemplateResponse(
        "vault.html",
        {
            "request": request,
            "rows": rows,
            "total": total,
            "page": page,
            "page_size": page_size,
            "max_page": max_page,
            "status_filter": status_filter,
            "filters": filters,
            "query_string": query_string,
            "export_prefix": export_prefix,
            "prev_link": prev_link,
            "next_link": next_link,
        },
    )


@app.get("/vault/export")
def vault_export(
    status: str = Query("all"),
    min_score: Optional[str] = Query(None),
    min_tech_score: Optional[str] = Query(None),
    min_wealth: Optional[str] = Query(None),
    contact_quality: Optional[str] = Query(None),
    municipio: Optional[str] = Query(None),
    uf: Optional[str] = Query(None),
    has_marketing: Optional[str] = Query(None),
    scope: str = Query("page"),
    format: str = Query("standard"),
    page: int = Query(1),
    page_size: int = Query(100),
) -> StreamingResponse:
    status_filter = status if status in {"all", "enriched", "pending"} else "all"
    filters = _build_vault_filters(
        min_score=min_score,
        min_tech_score=min_tech_score,
        min_wealth=min_wealth,
        contact_quality=contact_quality,
        municipio=municipio,
        uf=uf,
        has_marketing=has_marketing,
    )
    max_export = _env_int("MAX_EXPORT_ROWS", 5000)
    rows: List[Dict[str, Any]] = []
    if scope == "all":
        page_size = max(50, min(int(page_size), 500))
        page = 1
        while len(rows) < max_export:
            batch = storage.get_vault_data(page, page_size, filters, status_filter=status_filter)
            if not batch:
                break
            rows.extend(batch)
            if len(batch) < page_size:
                break
            page += 1
        rows = rows[:max_export]
    else:
        rows = storage.get_vault_data(page, page_size, filters, status_filter=status_filter)

    cnpjs = [row.get("cnpj") for row in rows if row.get("cnpj")]
    socios_map = storage.fetch_socios_by_cnpjs(cnpjs)
    if format == "meta":
        df = exports_module.export_to_meta_ads(pd.DataFrame(rows), socios_map=socios_map)
        filename = "hunter_meta_ads.csv"
    else:
        df = exports_module.format_export_data(rows, socios_map=socios_map, mode="commercial")
        filename = "hunter_vault.csv"

    csv_data = df.to_csv(index=False).encode("utf-8")
    headers = {"Content-Disposition": f"attachment; filename={filename}"}
    return StreamingResponse(io.BytesIO(csv_data), media_type="text/csv", headers=headers)


@app.get("/config", response_class=HTMLResponse)
def config(request: Request, saved: Optional[str] = Query(None)) -> HTMLResponse:
    webhook_url = storage.config_get("webhook_url") or ""
    return templates.TemplateResponse(
        "config.html",
        {
            "request": request,
            "webhook_url": webhook_url,
            "saved": saved,
        },
    )


@app.post("/config/webhook")
def config_webhook(webhook_url: str = Form("")) -> RedirectResponse:
    storage.config_set("webhook_url", webhook_url.strip())
    return RedirectResponse(url="/config?saved=1", status_code=303)
