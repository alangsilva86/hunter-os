from __future__ import annotations

import io
import json
import math
import os
import re
import secrets
import uuid
from datetime import datetime
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from fastapi import Depends, FastAPI, Form, Query, Request
from fastapi import File, HTTPException, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, Response, StreamingResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

from etl_pipeline import HunterOrchestrator
from modules import data_sources, exports as exports_module, jobs, person_search, storage, validator

BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "web" / "templates"
STATIC_DIR = BASE_DIR / "web" / "static"
TMP_DIR = BASE_DIR / "tmp"

app = FastAPI(title="Hunter OS", version="1.0.0")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

storage.init_db()
orchestrator = HunterOrchestrator()
security = HTTPBasic()


def _env_int(key: str, default: int) -> int:
    raw = os.getenv(key, str(default))
    try:
        return int(raw)
    except ValueError:
        return default


def _require_basic_auth(credentials: HTTPBasicCredentials = Depends(security)) -> str:
    user = os.getenv("BASIC_AUTH_USER", "")
    password = os.getenv("BASIC_AUTH_PASS", "")
    if not user and not password:
        return ""
    if not user or not password:
        raise HTTPException(status_code=500, detail="Basic auth misconfigured")
    valid_user = secrets.compare_digest(credentials.username, user)
    valid_pass = secrets.compare_digest(credentials.password, password)
    if not (valid_user and valid_pass):
        raise HTTPException(
            status_code=401,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


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


def _add_query_param(url: str, **params: str) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query))
    query.update({k: v for k, v in params.items() if v is not None})
    new_query = urlencode(query)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))


def _coerce_list(value: Any) -> List[str]:
    if value is None or value == "":
        return []
    if isinstance(value, list):
        return [str(item) for item in value if item]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return [value]
        if isinstance(parsed, list):
            return [str(item) for item in parsed if item]
        if parsed:
            return [str(parsed)]
        return []
    return [str(value)]


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


def _now_label() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _status_class(value: Optional[str]) -> str:
    if not value:
        return "pending"
    upper = str(value).upper()
    if "RUNNING" in upper or "PROCESS" in upper:
        return "running"
    if "COMPLETE" in upper or "DONE" in upper or "SUCCESS" in upper:
        return "completed"
    if "FAIL" in upper or "ERROR" in upper:
        return "failed"
    if "PAUSE" in upper or "CANCEL" in upper or "STOP" in upper:
        return "paused"
    return "pending"


def _humanize_event(event: str) -> str:
    if not event:
        return ""
    label = re.sub(r"^v\\d+_", "", event)
    label = label.replace("_", " ").strip()
    return label.title()


def _format_log_entries(logs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    stage_labels = _stage_labels()
    priority = [
        "stage",
        "status",
        "strategy",
        "total",
        "count",
        "processed",
        "rows",
        "cached",
        "arquivo_uuid",
        "file_path",
        "provider",
        "duration_ms",
    ]
    formatted = []
    for log in logs:
        detail_raw = log.get("detail_json") or ""
        detail: Dict[str, Any] = {}
        if detail_raw:
            try:
                parsed = json.loads(detail_raw)
                if isinstance(parsed, dict):
                    detail = parsed
            except Exception:
                detail = {}
        message = ""
        if detail:
            message = str(detail.get("message") or detail.get("error") or "")
            if not message and detail.get("stage"):
                message = stage_labels.get(detail["stage"], detail["stage"])
            if not message and detail.get("status"):
                message = str(detail["status"])
        if not message and detail_raw:
            message = detail_raw if len(detail_raw) <= 180 else f"{detail_raw[:180]}..."
        meta_items = []
        if detail:
            used = set()
            for key in priority:
                if key in detail and detail[key] not in (None, ""):
                    meta_items.append({"key": key, "value": detail[key]})
                    used.add(key)
            for key in sorted(detail.keys()):
                if key in used or key in {"run_id", "message", "error"}:
                    continue
                value = detail[key]
                if value in (None, ""):
                    continue
                meta_items.append({"key": key, "value": value})
        level = str(log.get("level") or "info").lower()
        if level not in {"info", "warning", "error"}:
            level = "info"
        formatted.append(
            {
                **log,
                "detail": detail,
                "message": message,
                "event_label": _humanize_event(str(log.get("event") or "")),
                "level_class": level,
                "meta_items": meta_items,
            }
        )
    return formatted


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/_stcore/health")
def stcore_health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/_stcore/host-config")
def stcore_host_config() -> Dict[str, Any]:
    return {"host": "hunter-os", "supports_custom_theme": False}


@app.get("/favicon.ico", include_in_schema=False)
def favicon() -> Response:
    return Response(status_code=204)


@app.get("/", response_class=HTMLResponse)
def root() -> RedirectResponse:
    return RedirectResponse(url="/mission", status_code=302)


@app.get("/mission", response_class=HTMLResponse)
def mission(
    request: Request,
    run_id: Optional[str] = Query(None),
    _auth: str = Depends(_require_basic_auth),
) -> HTMLResponse:
    runs = storage.list_hunter_runs(limit=10)
    runs_view = []
    for run in runs:
        run_view = dict(run)
        run_view["status_class"] = _status_class(run.get("status"))
        run_view["stage_label"] = _stage_labels().get(run.get("current_stage"), run.get("current_stage"))
        runs_view.append(run_view)
    active_run_id = run_id or (runs_view[0]["id"] if runs_view else None)
    active_run = storage.get_hunter_run(active_run_id) if active_run_id else None
    logs_raw = storage.fetch_logs(limit=15, run_id=active_run_id) if active_run_id else []
    logs = _format_log_entries(logs_raw)
    running = orchestrator.is_running(active_run_id) if active_run_id else False
    progress = 0
    active_status_class = "pending"
    active_stage_label = ""
    active_stage_class = "pending"
    if active_run:
        total = int(active_run.get("total_leads") or 0)
        processed = int(active_run.get("processed_count") or 0)
        if total > 0:
            progress = min(100, int((processed / max(total, 1)) * 100))
        active_status_class = _status_class(active_run.get("status"))
        active_stage_label = _stage_labels().get(active_run.get("current_stage"), active_run.get("current_stage"))
        active_stage_class = _status_class(active_run.get("current_stage"))
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
            "runs": runs_view,
            "logs": logs,
            "defaults": defaults,
            "stage_labels": _stage_labels(),
            "running": running,
            "progress": progress,
            "server_time": _now_label(),
            "active_status_class": active_status_class,
            "active_stage_label": active_stage_label,
            "active_stage_class": active_stage_class,
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
    _auth: str = Depends(_require_basic_auth),
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
def mission_pause(run_id: str, _auth: str = Depends(_require_basic_auth)) -> RedirectResponse:
    orchestrator.cancel_job(run_id)
    return RedirectResponse(url=f"/mission?run_id={run_id}", status_code=303)


@app.post("/mission/{run_id}/resume")
def mission_resume(run_id: str, _auth: str = Depends(_require_basic_auth)) -> RedirectResponse:
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
    _auth: str = Depends(_require_basic_auth),
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
    _auth: str = Depends(_require_basic_auth),
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
def config(
    request: Request,
    saved: Optional[str] = Query(None),
    _auth: str = Depends(_require_basic_auth),
) -> HTMLResponse:
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
def config_webhook(
    webhook_url: str = Form(""),
    _auth: str = Depends(_require_basic_auth),
) -> RedirectResponse:
    storage.config_set("webhook_url", webhook_url.strip())
    return RedirectResponse(url="/config?saved=1", status_code=303)


@app.get("/person", response_class=HTMLResponse)
def person(
    request: Request,
    name: Optional[str] = Query(None),
    cpf: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    uf: Optional[str] = Query(None),
    source: str = Query("auto"),
    message: Optional[str] = Query(None),
    error: Optional[str] = Query(None),
    _auth: str = Depends(_require_basic_auth),
) -> HTMLResponse:
    results: List[Dict[str, Any]] = []
    query = {
        "name": name or "",
        "cpf": cpf or "",
        "city": city or "",
        "uf": uf or "",
        "source": source or "auto",
    }
    if name or cpf:
        local = []
        external = []
        if source in {"auto", "local"}:
            local = person_search.search_partners(name=name, cpf=cpf, city=city, state=uf)
        if local:
            results = [item.to_dict() for item in local]
        elif source in {"auto", "external"} and name:
            try:
                external = person_search.search_partners_external(name=name, city=city, state=uf)
                results = [item.to_dict() for item in external]
            except Exception as exc:
                error = f"External search failed: {exc}"
    return templates.TemplateResponse(
        "person.html",
        {
            "request": request,
            "results": results,
            "query": query,
            "message": message,
            "error": error,
            "batch_results": [],
            "batch_download_url": "",
        },
    )


@app.post("/person/search")
def person_search_post(
    name: str = Form(""),
    cpf: str = Form(""),
    city: str = Form(""),
    uf: str = Form(""),
    source: str = Form("auto"),
    _auth: str = Depends(_require_basic_auth),
) -> RedirectResponse:
    url = _add_query_param(
        "/person",
        name=name.strip(),
        cpf=cpf.strip(),
        city=city.strip(),
        uf=uf.strip(),
        source=source.strip() or "auto",
    )
    return RedirectResponse(url=url, status_code=303)


@app.post("/person/import")
def person_import(
    cnpj: str = Form(""),
    return_to: str = Form("/person"),
    _auth: str = Depends(_require_basic_auth),
) -> RedirectResponse:
    cnpj_digits = re.sub(r"\D", "", cnpj or "")
    if not cnpj_digits:
        return RedirectResponse(url=_add_query_param(return_to, error="Missing CNPJ"), status_code=303)
    official = validator.get_official_qsa(cnpj_digits)
    saved = person_search.import_official_company(official)
    if saved:
        return RedirectResponse(url=_add_query_param(return_to, message="Imported to Vault"), status_code=303)
    return RedirectResponse(url=_add_query_param(return_to, error="Import failed"), status_code=303)


@app.post("/person/batch", response_class=HTMLResponse)
def person_batch(
    request: Request,
    file: UploadFile = File(...),
    use_external: Optional[str] = Form(None),
    _auth: str = Depends(_require_basic_auth),
) -> HTMLResponse:
    if not file.filename:
        return templates.TemplateResponse(
            "person.html",
            {
                "request": request,
                "results": [],
                "query": {"name": "", "cpf": "", "city": "", "uf": "", "source": "auto"},
                "message": "",
                "error": "Missing file.",
                "batch_results": [],
                "batch_download_url": "",
            },
        )
    try:
        content = file.file.read()
        df = pd.read_csv(io.BytesIO(content))
    except Exception as exc:
        return templates.TemplateResponse(
            "person.html",
            {
                "request": request,
                "results": [],
                "query": {"name": "", "cpf": "", "city": "", "uf": "", "source": "auto"},
                "message": "",
                "error": f"Invalid CSV: {exc}",
                "batch_results": [],
                "batch_download_url": "",
            },
        )

    df.columns = [str(col).strip().lower() for col in df.columns]
    required = {"nome", "cidade", "uf"}
    if not required.issubset(set(df.columns)):
        return templates.TemplateResponse(
            "person.html",
            {
                "request": request,
                "results": [],
                "query": {"name": "", "cpf": "", "city": "", "uf": "", "source": "auto"},
                "message": "",
                "error": "CSV must contain columns: nome, cidade, uf.",
                "batch_results": [],
                "batch_download_url": "",
            },
        )

    limit = _env_int("PERSON_BATCH_LIMIT", 50)
    df = df.head(limit)
    results: List[Dict[str, Any]] = []
    external_enabled = _is_checked(use_external)
    for _, row in df.iterrows():
        name = str(row.get("nome") or "").strip()
        city = str(row.get("cidade") or "").strip()
        state = str(row.get("uf") or "").strip()
        cpf_value = str(row.get("cpf") or "").strip() if "cpf" in df.columns else ""
        candidates = person_search.search_partners(name=name, cpf=cpf_value, city=city, state=state)
        status = "NOT_FOUND"
        auto_resolved = False
        resolved = None
        if candidates:
            if len(candidates) == 1:
                resolved = candidates[0]
                status = "MATCH"
            else:
                resolved = person_search.choose_best_candidate(candidates)
                status = "AMBIGUOUS"
                auto_resolved = True
        elif external_enabled and name:
            try:
                ext = person_search.search_partners_external(name=name, city=city, state=state, limit=1)
                if ext:
                    resolved = ext[0]
                    status = "EXTERNAL"
            except Exception:
                status = "ERROR"
        emails = _coerce_list(resolved.emails_norm) if resolved else []
        phones = _coerce_list(resolved.telefones_norm) if resolved else []
        results.append(
            {
                "nome": name,
                "cidade": city,
                "uf": state,
                "cpf": cpf_value,
                "status": status,
                "auto_resolved": auto_resolved,
                "cnpj": resolved.cnpj if resolved else "",
                "empresa": (resolved.nome_fantasia or resolved.razao_social) if resolved else "",
                "telefone": phones[0] if phones else "",
                "email_inferido": emails[0] if emails else "",
            }
        )

    TMP_DIR.mkdir(parents=True, exist_ok=True)
    token = uuid.uuid4().hex
    out_path = TMP_DIR / f"person_batch_{token}.csv"
    pd.DataFrame(results).to_csv(out_path, index=False)
    return templates.TemplateResponse(
        "person.html",
        {
            "request": request,
            "results": [],
            "query": {"name": "", "cpf": "", "city": "", "uf": "", "source": "auto"},
            "message": "Batch processed.",
            "error": "",
            "batch_results": results[:50],
            "batch_download_url": f"/person/batch/download/{token}",
        },
    )


@app.get("/person/batch/download/{token}")
def person_batch_download(
    token: str,
    _auth: str = Depends(_require_basic_auth),
) -> StreamingResponse:
    file_path = TMP_DIR / f"person_batch_{token}.csv"
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    with open(file_path, "rb") as handle:
        data = handle.read()
    headers = {"Content-Disposition": "attachment; filename=person_batch_results.csv"}
    return StreamingResponse(io.BytesIO(data), media_type="text/csv", headers=headers)


@app.get("/exports", response_class=HTMLResponse)
def exports_view(
    request: Request,
    message: Optional[str] = Query(None),
    error: Optional[str] = Query(None),
    _auth: str = Depends(_require_basic_auth),
) -> HTMLResponse:
    exports = [{**row, "status_class": _status_class(row.get("status"))} for row in storage.list_casa_exports(limit=50)]
    files = storage.fetch_export_files(limit=20)
    return templates.TemplateResponse(
        "exports.html",
        {
            "request": request,
            "exports": exports,
            "files": files,
            "message": message,
            "error": error,
        },
    )


@app.post("/exports/refresh")
def exports_refresh(_auth: str = Depends(_require_basic_auth)) -> RedirectResponse:
    try:
        data_sources.export_list_v4(page=1, run_id=None)
        return RedirectResponse(url="/exports?message=Export list refreshed", status_code=303)
    except Exception as exc:
        return RedirectResponse(url=_add_query_param("/exports", error=str(exc)), status_code=303)


@app.post("/exports/poll")
def exports_poll(
    arquivo_uuid: str = Form(""),
    _auth: str = Depends(_require_basic_auth),
) -> RedirectResponse:
    arquivo_uuid = arquivo_uuid.strip()
    if not arquivo_uuid:
        return RedirectResponse(url="/exports?error=Missing arquivo_uuid", status_code=303)
    try:
        data_sources.export_poll_v4_public(arquivo_uuid, run_id=None, include_corpo=True)
        return RedirectResponse(url="/exports?message=Export polled", status_code=303)
    except Exception as exc:
        return RedirectResponse(url=_add_query_param("/exports", error=str(exc)), status_code=303)


@app.post("/exports/download")
def exports_download(
    arquivo_uuid: str = Form(""),
    link: str = Form(""),
    _auth: str = Depends(_require_basic_auth),
) -> RedirectResponse:
    arquivo_uuid = arquivo_uuid.strip()
    if not arquivo_uuid:
        return RedirectResponse(url="/exports?error=Missing arquivo_uuid", status_code=303)
    if not link:
        stored = storage.fetch_casa_export(arquivo_uuid)
        link = stored.get("link") if stored else ""
    if not link:
        return RedirectResponse(url="/exports?error=Missing download link", status_code=303)
    try:
        data_sources.export_download(link, arquivo_uuid, run_id=None, dest_dir="exports_files")
        return RedirectResponse(url="/exports?message=File downloaded", status_code=303)
    except Exception as exc:
        return RedirectResponse(url=_add_query_param("/exports", error=str(exc)), status_code=303)


@app.get("/recovery", response_class=HTMLResponse)
def recovery_view(
    request: Request,
    message: Optional[str] = Query(None),
    error: Optional[str] = Query(None),
    _auth: str = Depends(_require_basic_auth),
) -> HTMLResponse:
    files = storage.fetch_export_files(limit=20)
    defaults = {
        "enrich_top_pct": "25",
        "provider": os.getenv("SEARCH_PROVIDER", "serper"),
        "concurrency": str(_env_int("CONCURRENCY", 10)),
        "timeout": str(_env_int("TIMEOUT", 5)),
        "cache_ttl_hours": str(_env_int("CACHE_TTL_HOURS", 24)),
        "telefone_repeat_threshold": "5",
    }
    return templates.TemplateResponse(
        "recovery.html",
        {
            "request": request,
            "files": files,
            "defaults": defaults,
            "message": message,
            "error": error,
        },
    )


@app.post("/recovery/start")
def recovery_start(
    arquivo_uuid: str = Form(""),
    excluir_mei: Optional[str] = Form(None),
    telefone_repeat_threshold: str = Form("5"),
    enrich_top_pct: str = Form("25"),
    enable_enrichment: Optional[str] = Form(None),
    provider: str = Form("serper"),
    concurrency: str = Form("10"),
    timeout: str = Form("5"),
    cache_ttl_hours: str = Form("24"),
    _auth: str = Depends(_require_basic_auth),
) -> RedirectResponse:
    arquivo_uuid = arquivo_uuid.strip()
    if not arquivo_uuid:
        return RedirectResponse(url="/recovery?error=Missing arquivo_uuid", status_code=303)
    params = {
        "run_type": "recovery",
        "excluir_mei": _is_checked(excluir_mei),
        "telefone_repeat_threshold": _to_int(telefone_repeat_threshold, 5),
        "enrich_top_pct": _to_int(enrich_top_pct, 25),
        "enable_enrichment": _is_checked(enable_enrichment),
        "provider": provider.strip() or os.getenv("SEARCH_PROVIDER", "serper"),
        "concurrency": _to_int(concurrency, _env_int("CONCURRENCY", 10)),
        "timeout": _to_int(timeout, _env_int("TIMEOUT", 5)),
        "cache_ttl_hours": _to_int(cache_ttl_hours, _env_int("CACHE_TTL_HOURS", 24)),
        "cache_only": False,
    }
    try:
        run_id = jobs.start_recovery(arquivo_uuid, params)
    except Exception as exc:
        return RedirectResponse(url=_add_query_param("/recovery", error=str(exc)), status_code=303)
    return RedirectResponse(
        url=_add_query_param("/diagnostics", run_type="standard", run_id=run_id, message="Recovery started"),
        status_code=303,
    )


@app.get("/diagnostics", response_class=HTMLResponse)
def diagnostics(
    request: Request,
    run_type: str = Query("hunter"),
    run_id: Optional[str] = Query(None),
    message: Optional[str] = Query(None),
    error: Optional[str] = Query(None),
    _auth: str = Depends(_require_basic_auth),
) -> HTMLResponse:
    stage_labels = _stage_labels()
    hunter_runs = [
        {
            **row,
            "status_class": _status_class(row.get("status")),
            "stage_label": stage_labels.get(row.get("current_stage"), row.get("current_stage")),
        }
        for row in storage.list_hunter_runs(limit=10)
    ]
    runs = [{**row, "status_class": _status_class(row.get("status"))} for row in storage.list_runs(limit=10)]
    selected: Optional[Dict[str, Any]] = None
    steps: List[Dict[str, Any]] = []
    api_calls: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    logs: List[Dict[str, Any]] = []
    if run_id:
        if run_type == "standard":
            selected = storage.get_run(run_id)
            steps = [
                {**row, "status_class": _status_class(row.get("status"))}
                for row in storage.fetch_run_steps(run_id)
            ]
            api_calls = storage.fetch_api_calls(run_id, limit=100)
            errors = storage.fetch_errors(run_id, limit=100)
        else:
            selected = storage.get_hunter_run(run_id)
        logs = _format_log_entries(storage.fetch_logs(run_id=run_id, limit=50))
    return templates.TemplateResponse(
        "diagnostics.html",
        {
            "request": request,
            "hunter_runs": hunter_runs,
            "runs": runs,
            "run_type": run_type,
            "run_id": run_id,
            "selected": selected,
            "steps": steps,
            "api_calls": api_calls,
            "errors": errors,
            "logs": logs,
            "message": message,
            "error": error,
        },
    )
