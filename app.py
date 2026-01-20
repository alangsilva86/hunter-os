"""Hunter OS v3 - Sniper Elite Console."""

import asyncio
import hashlib
import html
import re
import json
import logging
import math
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import streamlit as st
import aiohttp

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

from etl_pipeline import HunterOrchestrator
from modules import data_sources, enrichment_async, exports as webhook_exports, providers, scoring, storage
from modules import person_intelligence, person_search
from modules.telemetry import logger as telemetry_logger


st.set_page_config(
    page_title="Hunter OS v4 - Decision Maker Intel",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="collapsed",
)

storage.init_db()

telemetry_logger.info(
    "Hunter OS Online & Ready to Hunt",
    extra={"event_type": "startup"},
)


class _SessionLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        if threading.current_thread() is not threading.main_thread():
            return
        try:
            from streamlit.runtime.scriptrunner import get_script_run_ctx
        except Exception:
            get_script_run_ctx = None
        if get_script_run_ctx and get_script_run_ctx() is None:
            return
        try:
            message = self.format(record)
        except Exception:
            message = record.getMessage()
        logs = st.session_state.setdefault("live_logs", [])
        logs.append(message)
        if len(logs) > 200:
            del logs[:-200]


def _configure_logging() -> logging.Logger:
    logger = logging.getLogger("hunter")
    logger.setLevel(logging.INFO)
    if not any(isinstance(handler, logging.FileHandler) for handler in logger.handlers):
        log_dir = Path("logs")
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_dir / "hunter.log")
        file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(file_handler)
    if not st.session_state.get("session_log_handler"):
        session_handler = _SessionLogHandler()
        session_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(session_handler)
        st.session_state["session_log_handler"] = True
    return logger


logger = _configure_logging()
orchestrator = HunterOrchestrator()


def _env(key: str, default: str = "") -> str:
    return os.getenv(key, default)


def _set_env(key: str, value: str) -> None:
    if value:
        os.environ[key] = value


def _parse_json(value: Any) -> Dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        return json.loads(value)
    except Exception:
        return {}


def _progress_label(current: int, total: int) -> str:
    if total <= 0:
        return f"{current}"
    percent = min(100, int(round((current / total) * 100)))
    return f"{current}/{total} ({percent}%)"


def _micro_label(text: str) -> None:
    st.markdown(f"<div class='micro-label'>{text}</div>", unsafe_allow_html=True)


def _estimate_cache_key(filters: Dict[str, Any]) -> str:
    payload = json.dumps(filters, sort_keys=True, ensure_ascii=False)
    return f"estimate:{hashlib.sha256(payload.encode('utf-8')).hexdigest()}"


def _estimate_targets(filters: Dict[str, Any]) -> Dict[str, Any]:
    cache_key = _estimate_cache_key(filters)
    cached = storage.cache_get(cache_key)
    if cached and isinstance(cached, dict) and "total" in cached:
        return cached
    if not _env("CASA_DOS_DADOS_API_KEY"):
        return {"total": None, "error": "missing_key"}
    try:
        total = orchestrator.probe_total(filters, run_id="preview")
    except data_sources.CasaDosDadosBalanceError:
        return {"total": None, "error": "saldo"}
    except Exception:
        return {"total": None, "error": "failed"}
    payload = {"total": total, "updated_at": datetime.now(timezone.utc).isoformat()}
    storage.cache_set(cache_key, payload, ttl_hours=1)
    return payload


def _flatten_cnaes(setores: List[str], manual: str) -> List[str]:
    cnaes: List[str] = []
    for setor in setores:
        cnaes.extend(data_sources.SETORES_CNAE.get(setor, []))
    if manual:
        for item in manual.replace("\n", ",").split(","):
            code = "".join([c for c in item.strip() if c.isdigit()])
            if code:
                cnaes.append(code)
    return list(dict.fromkeys(cnaes))


def _stack_tags(tech_stack_json: Optional[str]) -> str:
    parsed = _parse_json(tech_stack_json)
    stack: List[str] = []
    if isinstance(parsed, dict):
        stack = parsed.get("detected_stack") or parsed.get("stack") or []
    elif isinstance(parsed, list):
        stack = parsed
    if not stack:
        return ""
    tags = [f"[{item}]" for item in stack[:6]]
    return " ".join(tags)


def _parse_json_list(value: Any) -> List[Any]:
    if value is None or value == "":
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return [value]
        if parsed is None:
            return []
        if isinstance(parsed, list):
            return parsed
        return [parsed]
    return [value]


def _person_primary(value: Any) -> Dict[str, Any]:
    payload = _parse_json(value)
    primary = payload.get("primary") if isinstance(payload, dict) else {}
    return primary if isinstance(primary, dict) else {}


def _wealth_label(person_raw: Any) -> str:
    primary = _person_primary(person_raw)
    label = str(primary.get("wealth_class") or "").upper()
    if label == "A":
        return "🟪 A"
    if label == "B":
        return "🟩 B"
    if label == "C":
        return "⬜ C"
    return ""


def _format_currency(value: Any) -> str:
    try:
        amount = float(value or 0)
    except (TypeError, ValueError):
        amount = 0
    formatted = f"{amount:,.0f}".replace(",", ".")
    return f"R$ {formatted}"


def _parse_ts(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _format_duration(seconds: float) -> str:
    if seconds <= 0:
        return "--"
    minutes, sec = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {sec}s"
    return f"{sec}s"


def _person_candidate_key(candidate: Dict[str, Any]) -> str:
    seed = f"{candidate.get('cpf','')}|{candidate.get('cnpj','')}|{candidate.get('nome_socio','')}"
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:12]


def _wealth_class_from_capital(value: Any) -> str:
    try:
        amount = float(value or 0)
    except (TypeError, ValueError):
        amount = 0
    if amount >= 1_000_000:
        return "A"
    if amount >= 100_000:
        return "B"
    return "C"


def _badge_for_wealth(label: str) -> str:
    if label == "A":
        return "🟪 Classe A"
    if label == "B":
        return "🟩 Classe B"
    return "⬜ Classe C"


def _split_name(full_name: str) -> Dict[str, str]:
    parts = [part for part in re.split(r"\s+", full_name.strip()) if part]
    if not parts:
        return {"first": "", "last": ""}
    if len(parts) == 1:
        return {"first": parts[0], "last": ""}
    return {"first": parts[0], "last": " ".join(parts[1:])}


def _build_vcard(name: str, phone: str, email: str) -> str:
    names = _split_name(name)
    lines = [
        "BEGIN:VCARD",
        "VERSION:3.0",
        f"N:{names['last']};{names['first']};;;",
        f"FN:{name}",
    ]
    if phone:
        lines.append(f"TEL;TYPE=CELL:{phone}")
    if email:
        lines.append(f"EMAIL;TYPE=INTERNET:{email}")
    lines.append("END:VCARD")
    return "\n".join(lines)


async def _run_person_intel(lead: Dict[str, Any]) -> Dict[str, Any]:
    person_intel = person_intelligence.PersonIntelligence(
        evolution_base_url=_env("EVOLUTION_API_URL"),
        evolution_api_key=_env("EVOLUTION_API_KEY"),
        avatar_cache_dir=_env("AVATAR_CACHE_DIR", "uploads/avatars"),
        enable_email_finder=_env("ENABLE_EMAIL_FINDER", "0") == "1",
        enable_holehe=_env("ENABLE_HOLEHE", "0") == "1",
    )
    timeout = aiohttp.ClientTimeout(total=12)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        return await person_intel.enrich(session, lead, enrichment=None)


def _fetch_all_vault_rows(
    filters: Dict[str, Any],
    status_filter: str,
    batch_size: int = 500,
) -> List[Dict[str, Any]]:
    all_rows: List[Dict[str, Any]] = []
    page = 1
    while True:
        batch = storage.get_vault_data(
            page=page,
            page_size=batch_size,
            filters=filters,
            status_filter=status_filter,
        )
        if not batch:
            break
        all_rows.extend(batch)
        if len(batch) < batch_size:
            break
        page += 1
    return all_rows


def _status_label(score: Optional[int]) -> str:
    if pd.isna(score):
        return "🔴 FRIO"
    try:
        score_value = int(float(score))
    except (TypeError, ValueError):
        return "🔴 FRIO"
    if score_value >= 85:
        return "🟢 HOT"
    if score_value >= 70:
        return "🟡 QUALIFICADO"
    if score_value >= 55:
        return "🟠 POTENCIAL"
    return "🔴 FRIO"


def _tech_rate(total: int, enriched: int) -> str:
    if total <= 0:
        return "0%"
    pct = int(round((enriched / total) * 100))
    return f"{pct}%"


def _escape(text: str) -> str:
    return html.escape(text or "")


def _render_empty_state() -> None:
    st.info("Nenhum alvo detectado. Inicie a varredura no Mission Control.", icon="📡")


def _inject_css() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Manrope:wght@400;500;600;700&family=Sora:wght@500;600;700&display=swap');

        :root {
            --bg: #0b0d12;
            --surface: rgba(24, 24, 30, 0.86);
            --surface-strong: #15151d;
            --border: rgba(255, 255, 255, 0.08);
            --input: rgba(15, 16, 22, 0.92);
            --text-primary: #f8fafc;
            --text-secondary: #94a3b8;
            --accent: #ff7a1a;
            --accent-strong: #f25c00;
            --success: #22c55e;
            --warning: #f59e0b;
        }
        * {
            -webkit-font-smoothing: antialiased;
            -moz-osx-font-smoothing: grayscale;
        }
        html, body, [class*="css"]  {
            font-family: "Manrope", sans-serif;
        }
        .stApp {
            background-color: var(--bg);
            background-image:
                radial-gradient(circle at 20% -20%, rgba(56, 189, 248, 0.14) 0%, rgba(11, 13, 18, 0) 45%),
                radial-gradient(circle at 80% -10%, rgba(249, 115, 22, 0.12) 0%, rgba(11, 13, 18, 0) 40%);
            color: var(--text-primary);
        }
        section[data-testid="stSidebar"] {
            display: none;
        }
        .block-container {
            padding-top: 5.5rem;
            padding-left: 2rem;
            padding-right: 2rem;
            max-width: 1280px;
        }
        .app-header {
            position: sticky;
            top: 0;
            z-index: 1000;
            background: rgba(17, 18, 24, 0.72);
            border-bottom: 1px solid var(--border);
            backdrop-filter: blur(18px);
            padding: 1.25rem 2rem 1rem 2rem;
            margin: -1.5rem -2rem 2rem -2rem;
        }
        .app-title {
            font-family: "Sora", sans-serif;
            font-weight: 600;
            font-size: 1.25rem;
            letter-spacing: 0.04em;
            color: var(--text-primary);
        }
        .app-subtitle {
            color: var(--text-secondary);
            font-size: 0.9rem;
            margin-top: 0.35rem;
        }
        .micro-label {
            text-transform: none;
            letter-spacing: 0.12em;
            font-size: 0.68rem;
            color: var(--text-secondary);
            margin-bottom: 0.35rem;
        }
        .nav-wrapper div[data-testid="stHorizontalBlock"] {
            background: rgba(18, 19, 26, 0.72);
            backdrop-filter: blur(18px);
            border-radius: 14px;
            padding: 8px 10px;
            border: 1px solid var(--border);
            position: sticky;
            top: 10px;
            z-index: 999;
            margin-bottom: 30px;
        }
        h1, h2, h3 {
            font-family: "Sora", sans-serif;
            letter-spacing: -0.025em;
            color: var(--text-primary) !important;
        }
        p, span, label, small {
            font-family: "Manrope", sans-serif;
            color: var(--text-secondary) !important;
        }
        div[data-testid="stForm"], div.css-card {
            background-color: var(--surface);
            border: 1px solid var(--border);
            border-radius: 14px;
            padding: 24px;
            box-shadow: 0 14px 30px rgba(0, 0, 0, 0.2);
        }
        div[data-testid="stVerticalBlockBorderWrapper"] {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 14px;
            padding: 1.1rem;
            box-shadow: 0 18px 40px rgba(0, 0, 0, 0.18);
        }
        input, textarea, div[data-baseweb="select"] > div {
            background-color: var(--input) !important;
            border: 1px solid rgba(148, 163, 184, 0.2) !important;
            color: var(--text-primary) !important;
            border-radius: 10px !important;
            transition: border-color 0.2s, box-shadow 0.2s;
        }
        input:focus, textarea:focus, div[data-baseweb="select"] > div:focus-within {
            border-color: var(--accent) !important;
            box-shadow: 0 0 0 2px rgba(255, 122, 26, 0.3);
        }
        div.stButton > button[kind="primary"], button[kind="primary"] {
            background: var(--accent) !important;
            color: #FFFFFF !important;
            font-weight: 600 !important;
            border-radius: 999px !important;
            border: 1px solid rgba(255, 255, 255, 0.1) !important;
            box-shadow: 0 10px 24px rgba(255, 122, 26, 0.2);
            transition: all 0.15s ease;
        }
        div.stButton > button[kind="primary"]:hover, button[kind="primary"]:hover {
            background: var(--accent-strong) !important;
            transform: translateY(-1px);
        }
        div.stButton > button[kind="secondary"], button[kind="secondary"] {
            background: rgba(20, 22, 30, 0.5) !important;
            border: 1px solid rgba(148, 163, 184, 0.18) !important;
            color: #e2e8f0 !important;
            border-radius: 999px !important;
        }
        div.stButton > button[kind="secondary"]:hover, button[kind="secondary"]:hover {
            border-color: rgba(148, 163, 184, 0.4) !important;
        }
        div[data-testid="stMetric"] {
            background-color: var(--surface);
            border: 1px solid var(--border);
            padding: 20px;
            border-radius: 14px;
            box-shadow: 0 18px 40px rgba(0, 0, 0, 0.2);
        }
        div[data-testid="stMetric"] label {
            color: var(--text-secondary) !important;
            font-size: 0.8rem !important;
            text-transform: uppercase;
            letter-spacing: 0.08em;
        }
        div[data-testid="stMetricValue"] {
            color: var(--text-primary) !important;
            font-size: 2rem !important;
            font-weight: 700;
            font-variant-numeric: tabular-nums;
        }
        .kpi-card {
            background-color: var(--surface);
            border: 1px solid var(--border);
            padding: 20px;
            border-radius: 14px;
            box-shadow: 0 18px 40px rgba(0, 0, 0, 0.2);
        }
        .kpi-label {
            color: var(--text-secondary);
            font-size: 0.8rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            margin-bottom: 0.25rem;
        }
        .kpi-value {
            font-size: 2rem;
            font-weight: 700;
            font-variant-numeric: tabular-nums;
            color: var(--text-primary);
        }
        div[data-testid="stDataFrame"] {
            border: 1px solid var(--border);
            border-radius: 12px;
            overflow: hidden;
        }
        div[data-testid="stDataFrame"] th {
            background-color: rgba(30, 32, 40, 0.9) !important;
            color: #e2e8f0 !important;
            font-weight: 600;
            border-bottom: 1px solid rgba(148, 163, 184, 0.12);
        }
        div[data-testid="stDataFrame"] td {
            background-color: rgba(20, 22, 30, 0.85) !important;
            color: #cbd5f5 !important;
            border-bottom: 1px solid rgba(148, 163, 184, 0.08);
        }
        .timeline {
            display: flex;
            flex-direction: column;
            gap: 0.65rem;
            margin-top: 1rem;
        }
        .timeline-item {
            display: flex;
            align-items: center;
            gap: 0.75rem;
            padding: 0.55rem 0.75rem;
            border-radius: 12px;
            border: 1px solid rgba(148, 163, 184, 0.12);
            background: rgba(20, 22, 30, 0.8);
        }
        .timeline-item.active {
            border-color: rgba(148, 163, 184, 0.35);
            box-shadow: 0 0 0 1px rgba(148, 163, 184, 0.2);
        }
        .timeline-item.done {
            border-color: var(--success);
        }
        .timeline-item span {
            color: var(--text-secondary);
            font-size: 0.85rem;
        }
        .timeline-item strong {
            color: var(--text-primary);
            font-size: 0.95rem;
        }
        .terminal {
            background: rgba(9, 11, 16, 0.95);
            color: var(--success);
            border-radius: 14px;
            padding: 1rem;
            font-family: "JetBrains Mono", ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono",
                "Courier New", monospace;
            font-size: 0.78rem;
            line-height: 1.35rem;
            border: 1px solid rgba(148, 163, 184, 0.1);
            max-height: 360px;
            overflow: auto;
        }
        .terminal-line {
            display: block;
            color: #86efac;
        }
        .estimate-card {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 1rem;
            padding: 1rem 1.1rem;
            border-radius: 16px;
            border: 1px solid rgba(148, 163, 184, 0.12);
            background: rgba(18, 20, 28, 0.72);
            box-shadow: 0 14px 30px rgba(0, 0, 0, 0.25);
        }
        .estimate-card strong {
            color: var(--text-primary);
            font-size: 1.1rem;
        }
        .estimate-card span {
            color: var(--text-secondary);
            font-size: 0.85rem;
        }
        .pulse {
            animation: pulse 1.4s ease-in-out infinite;
        }
        .chip {
            display: inline-flex;
            align-items: center;
            gap: 0.4rem;
            padding: 0.4rem 0.7rem;
            border-radius: 999px;
            background: rgba(30, 32, 42, 0.7);
            color: var(--text-secondary);
            font-size: 0.76rem;
            border: 1px solid rgba(148, 163, 184, 0.2);
        }
        .drawer-card {
            background: rgba(17, 19, 26, 0.95);
            border: 1px solid rgba(148, 163, 184, 0.16);
            border-radius: 18px;
            padding: 1.2rem;
            box-shadow: 0 24px 60px rgba(0, 0, 0, 0.35);
            position: sticky;
            top: 110px;
        }
        .drawer-title {
            font-family: "Sora", sans-serif;
            font-size: 1rem;
            color: var(--text-primary);
            margin-bottom: 0.5rem;
        }
        .macro-status {
            display: flex;
            align-items: center;
            gap: 0.5rem;
            padding: 0.5rem 0.8rem;
            border-radius: 999px;
            border: 1px solid rgba(148, 163, 184, 0.18);
            background: rgba(30, 32, 42, 0.7);
            font-size: 0.78rem;
            color: var(--text-secondary);
        }
        .macro-dot {
            width: 8px;
            height: 8px;
            border-radius: 999px;
            background: var(--success);
            box-shadow: 0 0 0 4px rgba(34, 197, 94, 0.2);
        }
        .spotlight-card {
            background: rgba(17, 19, 26, 0.9);
            border: 1px solid rgba(148, 163, 184, 0.2);
            border-radius: 20px;
            padding: 1.5rem;
            box-shadow: 0 28px 70px rgba(0, 0, 0, 0.35);
            margin-bottom: 2rem;
        }
        .candidate-card {
            border: 1px solid rgba(148, 163, 184, 0.18);
            border-radius: 16px;
            padding: 1rem;
            background: rgba(20, 22, 30, 0.85);
            margin-bottom: 0.75rem;
        }
        .candidate-card.highlight {
            border-color: rgba(168, 85, 247, 0.6);
            box-shadow: 0 0 0 1px rgba(168, 85, 247, 0.2);
        }
        .candidate-meta {
            color: var(--text-secondary);
            font-size: 0.85rem;
        }
        .badge-wealth {
            display: inline-flex;
            align-items: center;
            gap: 0.35rem;
            padding: 0.25rem 0.6rem;
            border-radius: 999px;
            font-size: 0.72rem;
            border: 1px solid rgba(148, 163, 184, 0.25);
            background: rgba(30, 32, 42, 0.7);
        }
        @keyframes pulse {
            0% { box-shadow: 0 0 0 0 rgba(249, 115, 22, 0.25); }
            70% { box-shadow: 0 0 0 12px rgba(249, 115, 22, 0); }
            100% { box-shadow: 0 0 0 0 rgba(249, 115, 22, 0); }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_header() -> None:
    st.markdown(
        """
        <div class="app-header">
            <div class="app-title">HUNTER OS v4 • DECISION MAKER INTEL</div>
            <div class="app-subtitle">Revenue Intelligence com foco total no decisor e na conversao.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _nav_buttons() -> str:
    return st.session_state.get("active_view", "MISSION")


def _render_navigation() -> str:
    if "active_view" not in st.session_state:
        st.session_state.active_view = "MISSION"
    st.markdown("<div class='nav-wrapper'>", unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns(4, gap="small")
    with col1:
        label = "Mission Control" + (" •" if st.session_state.active_view == "MISSION" else "")
        if st.button(label, type="secondary", key="nav_mission"):
            st.session_state.active_view = "MISSION"
    with col2:
        label = "🕵️‍♂️ Person Hunter" + (" •" if st.session_state.active_view == "PERSON" else "")
        if st.button(label, type="secondary", key="nav_person"):
            st.session_state.active_view = "PERSON"
    with col3:
        label = "The Vault" + (" •" if st.session_state.active_view == "VAULT" else "")
        if st.button(label, type="secondary", key="nav_vault"):
            st.session_state.active_view = "VAULT"
    with col4:
        label = "System Core" + (" •" if st.session_state.active_view == "SYSTEM" else "")
        if st.button(label, type="secondary", key="nav_system"):
            st.session_state.active_view = "SYSTEM"
    st.markdown("</div>", unsafe_allow_html=True)
    return st.session_state.active_view


def _render_mission_control() -> None:
    st.markdown("## Mission Control")
    _micro_label("Targeting")

    col_primary, col_secondary = st.columns([2.2, 1.2], gap="large")
    with col_secondary:
        with st.container(border=True):
            st.caption("Opcoes principais")
            limite = st.number_input("Limite de leads", min_value=10, max_value=5000, value=200, step=10)
            com_telefone = st.toggle("Somente com telefone", value=False)
            com_email = st.toggle("Somente com email", value=False)
            enable_enrichment = st.toggle("Enriquecimento hibrido", value=True)
            enrich_top_pct = st.slider("Top % para enriquecer", min_value=5, max_value=100, value=25, step=5)
            with st.expander("Parametros avancados"):
                page_size = st.number_input("Itens por pagina (API)", min_value=10, max_value=1000, value=200, step=10)
                telefone_repeat_threshold = st.number_input("Telefone repetido (min N)", min_value=2, max_value=20, value=5)
                cache_ttl_hours = st.number_input(
                    "Cache TTL (horas)",
                    min_value=1,
                    max_value=168,
                    value=int(_env("CACHE_TTL_HOURS", "24")),
                )

    with col_primary:
        with st.container(border=True):
            st.caption("Perfil do alvo")
            col_geo, col_sector = st.columns([1, 1], gap="medium")
            with col_geo:
                st.caption("Geografia")
                municipios = st.multiselect("Municipios", data_sources.get_cidades_disponiveis(), default=["MARINGA"])
                uf = st.selectbox("UF", ["PR", "SP", "RJ", "MG", "SC", "RS", "BA", "GO", "DF"], index=0)
            with col_sector:
                st.caption("Setor & CNAE")
                setores = st.multiselect(
                    "Setores",
                    data_sources.get_setores_disponiveis(),
                    default=["Servicos Administrativos"],
                )
                atividade_query = st.text_input("Atividade (autocomplete)", placeholder="Ex: Software, VTEX, Logistica")
                suggestion_fn = getattr(data_sources, "search_cnae_suggestions", None)
                if callable(suggestion_fn):
                    sugestoes = suggestion_fn(atividade_query)
                else:
                    list_fn = getattr(data_sources, "list_cnae_suggestions", None)
                    sugestoes = list_fn() if callable(list_fn) else []
                sugestoes_codes = [item.get("code") for item in sugestoes if item.get("code")]
                format_fn = getattr(data_sources, "format_cnae_label", None)
                if not callable(format_fn):
                    format_fn = lambda code: str(code or "")
                cnaes_sugeridos = st.multiselect(
                    "Sugestoes CNAE",
                    options=sugestoes_codes,
                    format_func=format_fn,
                )
                cnaes_manual = st.text_area("CNAE manual (opcional)", height=70)
                excluir_mei = st.toggle("Excluir MEI", value=True)

        cnaes = list(dict.fromkeys(_flatten_cnaes(setores, cnaes_manual) + cnaes_sugeridos))
        provider = _env("SEARCH_PROVIDER", "serper")

        with st.container(border=True):
            st.caption("Estimativa & disparo")
            estimate_filters = {
                "uf": uf,
                "municipios": municipios,
                "cnaes": cnaes,
                "excluir_mei": excluir_mei,
                "com_telefone": com_telefone,
                "com_email": com_email,
            }
            estimate_key = _estimate_cache_key(estimate_filters)
            if st.session_state.get("estimate_key") != estimate_key:
                st.session_state["estimate_key"] = estimate_key
                st.session_state["estimate_payload"] = storage.cache_get(estimate_key) or {}

            estimate_payload = st.session_state.get("estimate_payload") or {}
            col_est_left, col_est_right = st.columns([3, 1], gap="medium")
            with col_est_right:
                if st.button("Atualizar estimativa", key="estimate_refresh", use_container_width=True):
                    with st.spinner("Estimando volume..."):
                        estimate_payload = _estimate_targets(estimate_filters)
                        st.session_state["estimate_payload"] = estimate_payload
            total_est = estimate_payload.get("total") if isinstance(estimate_payload, dict) else None
            status_hint = ""
            if isinstance(estimate_payload, dict):
                if estimate_payload.get("error") == "missing_key":
                    status_hint = "Configure a API key para estimar."
                elif estimate_payload.get("error") == "saldo":
                    status_hint = "Saldo insuficiente na Casa dos Dados."
                elif estimate_payload.get("error") == "failed":
                    status_hint = "Falha ao estimar. Tente novamente."
            total_label = f"{int(total_est):,}".replace(",", ".") if isinstance(total_est, (int, float)) else "--"
            with col_est_left:
                st.markdown(
                    f"""
                    <div class="estimate-card pulse">
                        <div>
                            <span>Estimativa de Alvos</span><br/>
                            <strong>{total_label} empresas</strong>
                        </div>
                        <div><span>{status_hint}</span></div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            chips = [
                f"UF {uf}" if uf else "",
                f"{len(municipios)} municipios" if municipios else "",
                f"{len(cnaes)} CNAEs" if cnaes else "Sem CNAE",
                "Com telefone" if com_telefone else "",
                "Com email" if com_email else "",
            ]
            chips = [chip for chip in chips if chip]
            if chips:
                st.markdown(" ".join([f"<span class='chip'>{chip}</span>" for chip in chips]), unsafe_allow_html=True)

            if st.button("INICIAR CAÇADA", type="primary", key="start_hunt", use_container_width=True):
                try:
                    filters = {
                        "uf": uf,
                        "municipios": municipios,
                        "cnaes": cnaes,
                        "excluir_mei": excluir_mei,
                        "com_telefone": com_telefone,
                        "com_email": com_email,
                        "limite": int(limite),
                        "page_size": int(page_size),
                        "cache_ttl_hours": int(cache_ttl_hours),
                        "telefone_repeat_threshold": int(telefone_repeat_threshold),
                        "enrich_top_pct": int(enrich_top_pct),
                        "enable_enrichment": bool(enable_enrichment),
                        "provider": provider,
                        "concurrency": int(_env("CONCURRENCY", "10")),
                        "timeout": int(_env("TIMEOUT", "5")),
                    }
                    run_id = orchestrator.start_job(filters)
                    st.session_state.current_hunter_run_id = run_id
                    st.toast("Caçada iniciada.")
                except Exception as exc:
                    st.error("O provedor de dados esta instavel. Tente novamente em instantes.")
                    logger.warning("start_hunt failed: %s", exc)

    _micro_label("Live Terminal")
    run_id = st.session_state.get("current_hunter_run_id")
    if not run_id:
        st.info("Nenhuma caçada ativa. Configure o alvo acima e dispare a missao.")
        return

    @st.fragment(run_every="2s")
    def _render_live_monitor(active_run_id: str) -> None:
        run = storage.get_hunter_run(active_run_id)
        if not run:
            st.info("Job nao encontrado.")
            return
        status = run.get("status") or "UNKNOWN"
        current_stage = run.get("current_stage") or ""
        total_leads = int(run.get("total_leads") or 0)
        processed = int(run.get("processed_count") or 0)
        running = orchestrator.is_running(active_run_id)

        if not running and status not in {"RUNNING", "PAUSED"}:
            st.info("Nenhuma caçada ativa. Consulte o cofre para resultados anteriores.")
            return

        with st.container(border=True):
            stage_label_map = {
                "PROBE": "Coleta inicial",
                "REALTIME_FETCH": "Coleta em tempo real",
                "BULK_EXPORT_REQUEST": "Exportacao bulk",
                "BULK_POLL": "Aguardando export",
                "BULK_DOWNLOAD": "Download seguro",
                "BULK_IMPORT": "Importacao",
                "LOCAL_PIPELINE": "Enriquecimento & scoring",
                "COMPLETED": "Vault pronto",
                "FAILED": "Falha",
                "PAUSED": "Pausado",
            }
            macro_map = {
                "PROBE": ("Coleta", "Estimando volume"),
                "REALTIME_FETCH": ("Coleta", "Varredura em tempo real"),
                "BULK_EXPORT_REQUEST": ("Coleta", "Gerando export"),
                "BULK_POLL": ("Coleta", "Processando export"),
                "BULK_DOWNLOAD": ("Coleta", "Baixando CSV"),
                "BULK_IMPORT": ("Coleta", "Importando dados"),
                "LOCAL_PIPELINE": ("Enriquecimento", "Identificando decisores"),
                "COMPLETED": ("Finalizado", "Resultados no Vault"),
                "FAILED": ("Falha", "Verifique os logs"),
                "PAUSED": ("Pausado", "Aguardando retomada"),
            }
            macro_label, macro_hint = macro_map.get(current_stage, ("Pipeline", "Processando"))
            status_label_map = {
                "RUNNING": "Em execucao",
                "PAUSED": "Pausado",
                "FAILED": "Falhou",
                "COMPLETED": "Concluido",
            }
            status_label = status_label_map.get(status, status)
            created_at = _parse_ts(run.get("created_at"))
            elapsed_sec = 0.0
            if created_at:
                elapsed_sec = (datetime.now(timezone.utc) - created_at).total_seconds()
            eta_sec = 0.0
            if processed > 0 and total_leads > 0:
                eta_sec = max(0.0, elapsed_sec * (total_leads / max(1, processed) - 1))
            eta_label = _format_duration(eta_sec) if eta_sec else "--"
            dot_style = "background:#22c55e;box-shadow:0 0 0 4px rgba(34,197,94,0.2);"
            if status == "PAUSED":
                dot_style = "background:#f59e0b;box-shadow:0 0 0 4px rgba(245,158,11,0.2);"
            elif status == "FAILED":
                dot_style = "background:#ef4444;box-shadow:0 0 0 4px rgba(239,68,68,0.2);"
            elif status == "COMPLETED":
                dot_style = "background:#38bdf8;box-shadow:0 0 0 4px rgba(56,189,248,0.2);"

            st.markdown(
                f"<div class='macro-status'><span class='macro-dot' style='{dot_style}'></span>"
                f"{macro_label} · {macro_hint} · ETA {eta_label}</div>",
                unsafe_allow_html=True,
            )

            col_s1, col_s2, col_s3 = st.columns(3)
            col_s1.metric("Status", status_label)
            col_s2.metric("Etapa", stage_label_map.get(current_stage, current_stage or "-"))
            lead_label = f"{processed}/{total_leads}" if total_leads else f"{processed}"
            col_s3.metric("Leads", lead_label)

            last_log = storage.fetch_logs(limit=1, run_id=active_run_id)
            if last_log:
                last_entry = last_log[0]
                detail = _parse_json(last_entry.get("detail_json") or "{}")
                message = detail.get("message") or detail.get("error") or detail.get("hint") or ""
                extra = detail.get("stage") or detail.get("status") or ""
                info = f"{last_entry.get('created_at')} | {last_entry.get('event')}"
                if extra:
                    info = f"{info} | {extra}"
                if message:
                    info = f"{info} | {message}"
                st.caption(f"Ultimo evento: {info}")

            st.progress(min(100, int((processed / max(1, total_leads)) * 100)) if total_leads else 0)
            st.caption(f"Progresso: {_progress_label(processed, total_leads)} · Tempo decorrido {_format_duration(elapsed_sec)}")

            stage_groups = {
                "PROBE": 0,
                "REALTIME_FETCH": 1,
                "BULK_EXPORT_REQUEST": 1,
                "BULK_POLL": 1,
                "BULK_DOWNLOAD": 1,
                "BULK_IMPORT": 1,
                "LOCAL_PIPELINE": 2,
                "COMPLETED": 3,
                "FAILED": 3,
                "PAUSED": 2,
            }
            current_index = stage_groups.get(current_stage, 0)
            stage_messages = {
                "PROBE": "Estimando volume na base...",
                "REALTIME_FETCH": "Coletando resultados em tempo real...",
                "BULK_EXPORT_REQUEST": "Solicitando exportacao bulk...",
                "BULK_POLL": "Aguardando processamento do export...",
                "BULK_DOWNLOAD": "Baixando CSV seguro...",
                "BULK_IMPORT": "Importando CSV...",
                "LOCAL_PIPELINE": "Limpeza, enriquecimento e scoring...",
                "COMPLETED": "Concluido. Resultados no Vault.",
                "FAILED": "Falha no processamento. Verifique os logs.",
                "PAUSED": "Job pausado pelo usuario.",
            }
            steps = [
                ("🟡", "Probe & Negotiation", "Negociando arquivo com a Receita..."),
                ("🔵", "Extraction", "Baixando CSV Seguro..."),
                ("🟣", "Refining", "Enriquecimento hibrido em andamento..."),
                ("🟢", "Storing", "Salvando no cofre..."),
            ]
            timeline_html = ["<div class='timeline'>"]
            for idx, (icon, title, caption) in enumerate(steps):
                state = "pending"
                if idx < current_index:
                    state = "done"
                elif idx == current_index:
                    state = "active"
                    caption = stage_messages.get(current_stage, caption)
                timeline_html.append(
                    f"<div class='timeline-item {state}'>"
                    f"<div>{icon}</div><div><strong>{title}</strong><br><span>{caption}</span></div></div>"
                )
            timeline_html.append("</div>")
            st.markdown("".join(timeline_html), unsafe_allow_html=True)

            col_a, col_b = st.columns(2)
            with col_a:
                if st.button("Pausar caçada", type="secondary", key="panic_button"):
                    orchestrator.cancel_job(active_run_id)
                    st.toast("Job pausado.")
            with col_b:
                if status in {"PAUSED", "FAILED"} and not running:
                    if st.button("Retomar caçada", type="secondary", key="resume_button"):
                        orchestrator.resume_job(active_run_id)
                        st.toast("Retomando caçada.")

            terminal_logs = storage.fetch_logs(limit=8, run_id=active_run_id)
            event_map = {
                "v3_probe": "🔍 Localizando CNPJs...",
                "cleaning_start": "🧹 Limpando base e deduplicando...",
                "enrichment_start": "👤 Identificando decisores...",
                "enrichment_summary": "✅ Enriquecimento concluido.",
                "v3_stage": "⚙️ Pipeline em andamento...",
                "v3_completed": "✅ Vault atualizado.",
                "enrichment_provider_error": "⚠️ Erro no provedor externo.",
                "v3_failed": "🚨 Falha no processamento.",
            }
            if terminal_logs:
                terminal_lines = []
                for log in terminal_logs[::-1]:
                    detail = _parse_json(log.get("detail_json") or "{}")
                    message = detail.get("message") or event_map.get(log.get("event")) or log.get("event") or ""
                    line = f"{log.get('created_at')} | {message}"
                    terminal_lines.append(f"<span class='terminal-line'>{_escape(line)}</span>")
                terminal_html = "<div class='terminal'>" + "".join(terminal_lines) + "</div>"
                st.markdown(terminal_html, unsafe_allow_html=True)

            with st.expander("Logs completos (ultimas 10 linhas)"):
                logs = storage.fetch_logs(limit=10, run_id=active_run_id)
                if logs:
                    for log in logs[::-1]:
                        message = _parse_json(log.get("detail_json") or "{}").get("message")
                        line = message or log.get("detail_json") or ""
                        st.caption(
                            f"{log.get('created_at')} | {log.get('level')} | {log.get('event')} | {line}"
                        )
                else:
                    st.caption("Sem logs recentes.")

    _render_live_monitor(run_id)


def _render_vault() -> None:
    st.markdown("## The Vault")

    total_leads = storage.count_leads_clean()
    total_enriched = storage.count_enrichments()
    with storage.get_conn() as conn:
        qualified_count = conn.execute("SELECT COUNT(*) AS cnt FROM leads_clean WHERE score_v2 >= 70").fetchone()["cnt"]
    col_k1, col_k2, col_k3, col_k4 = st.columns(4)
    col_k1.metric("Total Leads", total_leads)
    col_k2.metric("Enriquecidos", total_enriched)
    col_k3.metric("Taxa de Enriquecimento", _tech_rate(total_leads, total_enriched))
    hot_ratio = qualified_count / max(total_leads, 1)
    hot_color = "#10B981" if hot_ratio >= 0.2 else "#F97316"
    with col_k4:
        st.markdown(
            f"""
            <style>
            .kpi-hot .kpi-value {{
                color: {hot_color};
            }}
            </style>
            <div class="kpi-card kpi-hot">
                <div class="kpi-label">Leads Hot</div>
                <div class="kpi-value">{qualified_count}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    _micro_label("Filtros")
    with st.container(border=True):
        import inspect

        def _count_vault(filters: Dict[str, Any], status_filter: str) -> int:
            if hasattr(storage, "count_vault_data"):
                return storage.count_vault_data(filters, status_filter=status_filter)
            kwargs = {
                "min_score": filters.get("min_score"),
                "min_tech_score": filters.get("min_tech_score"),
                "min_wealth": filters.get("min_wealth"),
                "contact_quality": filters.get("contact_quality"),
                "municipio": filters.get("municipio"),
            }
            if "status_filter" in inspect.signature(storage.count_enrichment_vault).parameters:
                kwargs["status_filter"] = status_filter
            return storage.count_enrichment_vault(**kwargs)

        def _fetch_vault(
            page: int,
            page_size: int,
            filters: Dict[str, Any],
            status_filter: str,
        ) -> List[Dict[str, Any]]:
            if hasattr(storage, "get_vault_data"):
                return storage.get_vault_data(
                    page=page,
                    page_size=page_size,
                    filters=filters,
                    status_filter=status_filter,
                )
            offset = max(0, (page - 1) * page_size)
            kwargs = {
                "min_score": filters.get("min_score"),
                "min_tech_score": filters.get("min_tech_score"),
                "min_wealth": filters.get("min_wealth"),
                "contact_quality": filters.get("contact_quality"),
                "municipio": filters.get("municipio"),
                "limit": page_size,
                "offset": offset,
            }
            if "status_filter" in inspect.signature(storage.query_enrichment_vault).parameters:
                kwargs["status_filter"] = status_filter
            return storage.query_enrichment_vault(**kwargs)

        status_labels = ["Todos", "🟣 Enriquecidos", "⚪ Pendentes"]
        status_label = st.radio("Status", options=status_labels, horizontal=True, key="vault_status_filter")
        status_filter_map = {"Todos": "all", "🟣 Enriquecidos": "enriched", "⚪ Pendentes": "pending"}
        status_filter = status_filter_map[status_label]
        col_f1, col_f2 = st.columns(2, gap="medium")
        with col_f1:
            min_score = st.number_input("Score minimo", min_value=0, max_value=100, value=0)
        with col_f2:
            min_tech_score = st.number_input("Tech score minimo", min_value=0, max_value=30, value=0)
        col_w1, col_w2 = st.columns(2, gap="medium")
        with col_w1:
            wealth_preset = st.selectbox(
                "Wealth preset",
                options=["", "Classe A (>= R$ 1M)", "Classe B (>= R$ 100k)"],
            )
        with col_w2:
            min_wealth = st.number_input("Wealth minimo (R$)", min_value=0, max_value=100000000, value=0, step=50000)
        col_f3, col_f4 = st.columns([3, 1], gap="medium")
        with col_f3:
            municipio = st.text_input("Municipio")
        with col_f4:
            contact_quality = st.selectbox("Contact quality", options=["", "ok", "suspicious", "accountant_like"])
        col_f5, col_f6 = st.columns(2, gap="medium")
        with col_f5:
            page_size = st.selectbox(
                "Resultados por pagina",
                options=[50, 100, 200, 500, 1000],
                index=2,
                key="vault_page_size",
            )
        filter_min_score = min_score if min_score > 0 else None
        filter_min_tech = min_tech_score if min_tech_score > 0 else None
        preset_min_wealth = 0
        if wealth_preset == "Classe A (>= R$ 1M)":
            preset_min_wealth = 1_000_000
        elif wealth_preset == "Classe B (>= R$ 100k)":
            preset_min_wealth = 100_000
        applied_min_wealth = max(min_wealth, preset_min_wealth)
        filter_contact = contact_quality or None
        filter_municipio = municipio or None

        filters = {
            "min_score": filter_min_score,
            "min_tech_score": filter_min_tech,
            "min_wealth": applied_min_wealth if applied_min_wealth > 0 else None,
            "contact_quality": filter_contact,
            "municipio": filter_municipio,
        }

        filtered_total = _count_vault(filters, status_filter=status_filter)
        pending_total = _count_vault(filters, status_filter="pending")
        max_page = max(1, math.ceil(filtered_total / page_size)) if page_size else 1
        current_page = int(st.session_state.get("vault_page", 1))
        if current_page > max_page:
            current_page = max_page
            st.session_state["vault_page"] = current_page
        with col_f6:
            page = st.number_input(
                "Pagina",
                min_value=1,
                max_value=max_page,
                value=current_page,
                step=1,
                key="vault_page",
            )

    vault_rows = _fetch_vault(
        page=page,
        page_size=page_size,
        filters=filters,
        status_filter=status_filter,
    )

    if not vault_rows:
        _render_empty_state()
        return

    shown_count = len(vault_rows)
    if filtered_total:
        st.caption(f"Mostrando {shown_count} de {filtered_total} leads. Pagina {page} de {max_page}.")

    if pending_total and status_filter in {"all", "pending"}:
        col_info, col_btn = st.columns([3, 1])
        with col_info:
            st.caption(f"Leads aguardando enriquecimento: {pending_total}")
        with col_btn:
            batch_size = st.number_input(
                "Batch size",
                min_value=10,
                max_value=500,
                value=50,
                step=10,
                key="vault_enrich_batch_size",
            )
            if st.button(
                f"⚡ Enriquecer Lote (Proximos {int(batch_size)})",
                type="primary",
                key="vault_enrich_batch",
            ):
                pending_rows = storage.get_vault_data(
                    page=1,
                    page_size=int(batch_size),
                    filters=filters,
                    status_filter="pending",
                )
                if not pending_rows:
                    st.toast("Nao ha leads pendentes para enriquecer.", icon="✅")
                else:
                    leads: List[Dict[str, Any]] = []
                    lead_map: Dict[str, Dict[str, Any]] = {}
                    cnpjs_pending = [row.get("cnpj") for row in pending_rows if row.get("cnpj")]
                    socios_map = storage.fetch_socios_by_cnpjs(cnpjs_pending)
                    for row in pending_rows:
                        lead = {
                            "cnpj": row.get("cnpj"),
                            "razao_social": row.get("razao_social"),
                            "nome_fantasia": row.get("nome_fantasia"),
                            "municipio": row.get("municipio"),
                            "uf": row.get("uf"),
                            "porte": row.get("porte"),
                            "score_v1": row.get("score_v1"),
                            "score_v2": row.get("score_v2"),
                            "contact_quality": row.get("contact_quality"),
                            "flags": _parse_json(row.get("flags_json")),
                            "emails_norm": _parse_json_list(row.get("emails_norm")),
                        }
                        if lead.get("cnpj") and lead.get("cnpj") in socios_map:
                            lead["socios"] = socios_map.get(lead["cnpj"])
                        leads.append(lead)
                        if lead.get("cnpj"):
                            lead_map[lead["cnpj"]] = lead

                    run_id = storage.create_run(
                        {
                            "source": "vault_manual",
                            "batch_size": len(leads),
                            "filters": filters,
                        }
                    )
                    storage.update_run(run_id, status="running", total_leads=len(leads))
                    provider = providers.select_provider(_env("SEARCH_PROVIDER", "serper"))
                    enricher = enrichment_async.AsyncEnricher(
                        provider=provider,
                        concurrency=int(_env("CONCURRENCY", "10")),
                        timeout=int(_env("TIMEOUT", "5")),
                        cache_ttl_hours=int(_env("CACHE_TTL_HOURS", "24")),
                    )
                    with st.spinner("Enriquecendo leads pendentes..."):
                        try:
                            enriched_results, enrich_stats = asyncio.run(enricher.enrich_batch(leads, run_id))
                        except Exception as exc:
                            storage.update_run(run_id, status="failed")
                            st.error("Falha ao processar o lote de enriquecimento.")
                            logger.warning("vault_enrich_batch failed: %s", exc)
                            enriched_results = []
                            enrich_stats = {"errors_count": len(leads)}

                    for item in enriched_results:
                        storage.upsert_enrichment(item.get("cnpj"), item)
                        lead = lead_map.get(item.get("cnpj"))
                        if not lead:
                            continue
                        score, reasons, version = scoring.score_with_reasons(lead, item)
                        storage.update_lead_scores(lead["cnpj"], score, scoring.label(score))
                        storage.update_enrichment_scoring(lead["cnpj"], version, reasons)

                    storage.update_run(
                        run_id,
                        status="completed",
                        enriched_count=len(enriched_results),
                        errors_count=enrich_stats.get("errors_count", 0),
                    )
                    st.success("Processamento iniciado! Atualizando a lista...")
                    st.rerun()

    df_vault = pd.DataFrame(vault_rows)
    df_vault["enrichment_status"] = df_vault["enriched_at"].apply(
        lambda value: "🟣 ENRIQUECIDO" if value else "⚪ PENDENTE"
    )
    df_vault["status_label"] = df_vault["score_v2"].apply(_status_label)
    df_vault["stack_tags"] = df_vault["tech_stack_json"].apply(_stack_tags)
    df_vault["site_link"] = df_vault["site"].fillna("")
    df_vault["linkedin_link"] = df_vault["linkedin_company"].fillna("")
    df_vault["instagram_link"] = df_vault["instagram"].fillna("")
    df_vault["maps_link"] = df_vault["google_maps_url"].fillna("")
    if "avatar_url" not in df_vault.columns:
        df_vault["avatar_url"] = ""
    else:
        df_vault["avatar_url"] = df_vault["avatar_url"].fillna("")
    df_vault["wealth_label"] = df_vault["person_json"].apply(_wealth_label)

    display_cols = [
        "avatar_url",
        "cnpj",
        "razao_social",
        "enrichment_status",
        "status_label",
        "wealth_label",
        "score_v2",
        "stack_tags",
        "site_link",
        "linkedin_link",
        "instagram_link",
        "maps_link",
    ]
    df_display = df_vault[display_cols].copy()
    df_display.insert(0, "selecionar", False)

    col_table, col_drawer = st.columns([3, 1.25], gap="large")

    with col_table:
        edited = st.data_editor(
            df_display,
            hide_index=True,
            width="stretch",
            disabled=[col for col in df_display.columns if col != "selecionar"],
            column_config={
                "selecionar": st.column_config.CheckboxColumn("Selecionar"),
                "avatar_url": st.column_config.ImageColumn("Avatar", width="small"),
                "razao_social": st.column_config.TextColumn("Razao Social"),
                "enrichment_status": st.column_config.TextColumn("Status"),
                "status_label": st.column_config.TextColumn("Qualidade"),
                "wealth_label": st.column_config.TextColumn("Wealth"),
                "score_v2": st.column_config.ProgressColumn("Score", min_value=0, max_value=100),
                "stack_tags": st.column_config.TextColumn("Tech Stack"),
                "site_link": st.column_config.LinkColumn("Site", display_text="🔗"),
                "linkedin_link": st.column_config.LinkColumn("LinkedIn", display_text="🔗"),
                "instagram_link": st.column_config.LinkColumn("Instagram", display_text="🔗"),
                "maps_link": st.column_config.LinkColumn("Maps", display_text="🔗"),
            },
            key="vault_editor",
        )

    selected_idx = edited.index[edited["selecionar"]].tolist()
    selected_payload = df_vault.loc[selected_idx].to_dict(orient="records") if selected_idx else []

    with col_drawer:
        _micro_label("Inspector")
        st.markdown("<div class='drawer-card'>", unsafe_allow_html=True)
        if not selected_payload:
            st.markdown("<div class='drawer-title'>Selecione um lead</div>", unsafe_allow_html=True)
            st.caption("Clique em um lead no Vault para abrir o painel de inteligencia.")
        else:
            lead = selected_payload[0]
            person = _parse_json(lead.get("person_json"))
            primary = _person_primary(person)
            cross = person.get("cross_ownership") if isinstance(person, dict) else []
            tech_stack = _parse_json(lead.get("tech_stack_json"))
            stack_items = []
            if isinstance(tech_stack, dict):
                stack_items = tech_stack.get("detected_stack") or []
            elif isinstance(tech_stack, list):
                stack_items = tech_stack

            avatar_url = primary.get("avatar_url") or lead.get("avatar_url")
            name = primary.get("name") or lead.get("razao_social") or lead.get("nome_fantasia") or ""
            role = primary.get("role") or "Socio"
            linkedin_profile = primary.get("linkedin_profile") or ""
            if not linkedin_profile:
                linkedin_people = _parse_json_list(lead.get("linkedin_people_json"))
                linkedin_profile = str(linkedin_people[0]) if linkedin_people else ""
            wealth_estimate = primary.get("wealth_estimate") or lead.get("wealth_score") or 0
            share_pct = primary.get("share_pct") or 0
            email = primary.get("email") or ""
            email_validated = bool(primary.get("email_validated"))
            email_sources = primary.get("email_sources") or []

            st.markdown(f"<div class='drawer-title'>{name}</div>", unsafe_allow_html=True)
            st.caption(role)
            if avatar_url:
                st.image(avatar_url, width=140)
            if linkedin_profile:
                st.markdown(f"[LinkedIn pessoal]({linkedin_profile})")
            if email:
                if email_validated and email_sources:
                    sources = ", ".join([str(item) for item in email_sources])
                    st.markdown(f"Email: `{email}` • Validado ({sources})")
                elif email_validated:
                    st.markdown(f"Email: `{email}` • Validado")
                else:
                    st.markdown(f"Email: `{email}`")
            phones = _parse_json_list(lead.get("telefones_norm"))
            if phones:
                phone = str(phones[0])
                wa = f"https://wa.me/55{phone}" if not phone.startswith("55") else f"https://wa.me/{phone}"
                st.markdown(f"[WhatsApp direto]({wa})")

            st.divider()
            st.caption("Fortuna estimada")
            st.metric("Patrimonio", _format_currency(wealth_estimate))
            st.progress(min(100, int(float(share_pct or 0))))
            st.caption(f"Participacao societaria: {float(share_pct or 0):.1f}%")

            if stack_items:
                st.divider()
                st.caption("Tech Stack")
                st.write(", ".join(stack_items[:10]))
            if cross:
                st.divider()
                st.caption("Cross-Ownership")
                for item in cross[:5]:
                    line = item.get("razao_social") or item.get("nome_fantasia") or item.get("cnpj")
                    if line:
                        st.write(f"• {line}")
        st.markdown("</div>", unsafe_allow_html=True)

    _micro_label("Ações")
    with st.container(border=True):
        col_a, col_b = st.columns([2, 3], gap="medium")
        with col_a:
            csv_type = st.selectbox(
                "Tipo de CSV",
                options=["Comercial", "Debug"],
                index=0,
                key="vault_csv_type",
            )
            export_scope = st.radio(
                "Exportar CSV",
                options=["Pagina atual", "Todos filtrados", "Selecionados"],
                index=1,
                key="vault_export_scope",
            )
            if export_scope == "Selecionados" and not selected_payload:
                st.caption("Selecione ao menos um lead para exportar.")

            if export_scope == "Todos filtrados":
                export_rows = _fetch_all_vault_rows(filters, status_filter)
            elif export_scope == "Selecionados":
                export_rows = selected_payload
            else:
                export_rows = df_vault.to_dict(orient="records")

            cnpjs = [row.get("cnpj") for row in export_rows if row.get("cnpj")]
            socios_map = storage.fetch_socios_by_cnpjs(cnpjs)
            export_mode = "debug" if csv_type == "Debug" else "commercial"
            export_df = webhook_exports.format_export_data(export_rows, socios_map=socios_map, mode=export_mode)
            export_suffix = {
                "Pagina atual": "pagina",
                "Todos filtrados": "completo",
                "Selecionados": "selecionados",
            }[export_scope]
            csv_data = export_df.to_csv(index=False)
            meta_df = webhook_exports.export_to_meta_ads(pd.DataFrame(export_rows), socios_map=socios_map)
            meta_csv = meta_df.to_csv(index=False)
            export_name = f"hunter_vault_{export_suffix}.csv"
            if st.download_button(
                "Exportar CSV",
                data=csv_data,
                file_name=export_name,
                mime="text/csv",
                disabled=export_df.empty,
            ):
                st.toast(
                    f"Sucesso! Arquivo '{export_name}' preparado.",
                    icon="✅",
                )
            if not export_df.empty:
                st.caption(f"{len(export_df)} linhas no CSV.")
            meta_name = f"hunter_meta_ads_{export_suffix}.csv"
            if st.download_button(
                "📥 Baixar Lista para Meta Ads (LAL)",
                data=meta_csv,
                file_name=meta_name,
                mime="text/csv",
                disabled=meta_df.empty,
            ):
                st.toast(
                    f"Arquivo formatado para Lookalike pronto: '{meta_name}'.",
                    icon="✅",
                )
            if not meta_df.empty:
                st.caption(f"{len(meta_df)} linhas no CSV Meta Ads.")
        with col_b:
            webhook_url = storage.config_get("webhook_url") or ""
            if st.button("⚡ Disparar Webhook (CRM)", type="primary", key="vault_webhook"):
                if not selected_payload:
                    st.toast("Selecione ao menos um lead.", icon="⚠️")
                elif not webhook_url:
                    st.toast("Configure a URL do webhook no System Core.", icon="⚠️")
                else:
                    try:
                        result = webhook_exports.send_batch_to_webhook(selected_payload, webhook_url)
                        st.toast(
                            f"Webhook enviado. Sucesso: {result.get('sent', 0)} | Falhas: {result.get('failed', 0)}.",
                            icon="✅",
                        )
                    except Exception as exc:
                        st.error("Falha ao enviar webhook. Verifique a integracao.")
                        logger.warning("webhook_send failed: %s", exc)


def _render_person_hunter() -> None:
    st.markdown("## 🕵️‍♂️ Person Hunter")

    col_left, col_center, col_right = st.columns([1, 2, 1])
    with col_center:
        with st.container(border=True):
            st.caption("Busca focada em pessoas fisicas (socios)")
            tabs = st.tabs(["Busca Individual", "Upload Lote (CSV)"])

            with tabs[0]:
                col_i1, col_i2 = st.columns(2, gap="medium")
                with col_i1:
                    nome = st.text_input("Nome completo", key="person_hunter_name")
                with col_i2:
                    cpf = st.text_input("CPF (opcional)", key="person_hunter_cpf")
                col_i3, col_i4 = st.columns([2, 1], gap="medium")
                with col_i3:
                    cidade = st.text_input("Cidade", key="person_hunter_city")
                with col_i4:
                    uf = st.selectbox("UF", ["", "PR", "SP", "RJ", "MG", "SC", "RS", "BA", "GO", "DF"], index=0)
                if not cidade:
                    st.warning("Cidade vazia pode gerar homonimos. Recomendado preencher.")

                if st.button("Rastrear Alvo", type="primary", key="person_hunter_search", use_container_width=True):
                    telemetry_logger.info(
                        f"Caçada Iniciada: {nome} em {cidade}",
                        extra={
                            "event_type": "search",
                            "target": nome,
                            "location": cidade,
                            "state": uf,
                        },
                    )
                    candidates = person_search.search_partners(
                        name=nome,
                        cpf=cpf,
                        city=cidade,
                        state=uf,
                    )
                    resolver = person_search.PersonResolver(candidates)
                    result = resolver.resolve()
                    st.session_state["person_hunter_status"] = result.get("status")
                    st.session_state["person_hunter_candidates"] = [
                        item.to_dict() for item in result.get("candidates", []) or []
                    ]
                    selected = result.get("person")
                    if selected:
                        selected_dict = selected.to_dict()
                        selected_key = _person_candidate_key(selected_dict)
                        st.session_state["person_hunter_selected"] = selected_key
                        st.session_state["person_hunter_selected_candidate"] = selected_dict
                    else:
                        st.session_state["person_hunter_selected"] = None
                        st.session_state["person_hunter_selected_candidate"] = None
                    st.session_state["person_hunter_dossier"] = None

                status = st.session_state.get("person_hunter_status")
                candidates = st.session_state.get("person_hunter_candidates") or []
                if status == "NOT_FOUND":
                    st.info("Nenhuma pessoa encontrada com esses parametros.")

                if status == "AMBIGUOUS" and candidates:
                    title_name = nome or candidates[0].get("nome_socio") or "Pessoa"
                    title_city = cidade or (candidates[0].get("municipio") or "")
                    st.markdown(
                        f"Encontramos {len(candidates)} pessoas com nome **{title_name}** em **{title_city}**:"
                    )
                    for idx, candidate in enumerate(candidates):
                        capital = candidate.get("capital_social") or 0
                        wealth_class = _wealth_class_from_capital(capital)
                        highlight = "highlight" if wealth_class == "A" else ""
                        badge = _badge_for_wealth(wealth_class)
                        cnpj = candidate.get("cnpj") or ""
                        empresa = candidate.get("nome_fantasia") or candidate.get("razao_social") or cnpj
                        cidade_show = candidate.get("municipio") or ""
                        uf_show = candidate.get("uf") or ""
                        with st.container():
                            st.markdown(
                                f"<div class='candidate-card {highlight}'>"
                                f"<div><strong>{candidate.get('nome_socio') or ''}</strong> "
                                f"| Socio na <strong>{empresa}</strong></div>"
                                f"<div class='candidate-meta'>📍 {cidade_show}, {uf_show} "
                                f"| 💰 Cap. Social: {_format_currency(capital)}</div>"
                                f"<div class='badge-wealth'>{badge}</div>"
                                f"</div>",
                                unsafe_allow_html=True,
                            )
                            button_type = "primary" if wealth_class == "A" else "secondary"
                            if st.button(
                                "E este aqui",
                                type=button_type,
                                key=f"person_hunter_pick_{idx}",
                            ):
                                selected_key = _person_candidate_key(candidate)
                                st.session_state["person_hunter_selected"] = selected_key
                                st.session_state["person_hunter_selected_candidate"] = candidate
                                st.session_state["person_hunter_dossier"] = None
                                st.rerun()

                selected_candidate = st.session_state.get("person_hunter_selected_candidate")
                if selected_candidate:
                    dossier = st.session_state.get("person_hunter_dossier")
                    selected_key = st.session_state.get("person_hunter_selected")
                    if not dossier or dossier.get("candidate_key") != selected_key:
                        candidate_obj = person_search.PersonCandidate.from_row(selected_candidate)
                        lead = person_search.candidate_to_lead(candidate_obj)
                        with st.spinner("Hidratando perfil..."):
                            try:
                                person_payload = asyncio.run(_run_person_intel(lead))
                            except RuntimeError:
                                person_payload = asyncio.get_event_loop().run_until_complete(_run_person_intel(lead))
                        dossier = {
                            "candidate_key": selected_key,
                            "candidate": selected_candidate,
                            "person": person_payload,
                        }
                        st.session_state["person_hunter_dossier"] = dossier

                    if dossier:
                        candidate = dossier.get("candidate") or {}
                        person_payload = dossier.get("person") or {}
                        person = _parse_json(person_payload.get("person_json"))
                        primary = _person_primary(person)
                        avatar_url = primary.get("avatar_url") or person_payload.get("avatar_url")
                        wealth_score = primary.get("wealth_estimate") or person_payload.get("wealth_score") or 0
                        wealth_class = primary.get("wealth_class") or _wealth_class_from_capital(wealth_score)
                        badge = _badge_for_wealth(wealth_class)
                        empresas = []
                        if candidate.get("cnpj"):
                            empresas.append(candidate.get("razao_social") or candidate.get("nome_fantasia") or candidate.get("cnpj"))
                        cross = person.get("cross_ownership") if isinstance(person, dict) else []
                        for item in cross or []:
                            empresas.append(item.get("razao_social") or item.get("nome_fantasia") or item.get("cnpj"))
                        empresas = [item for item in empresas if item]
                        emails = _parse_json_list(candidate.get("emails_norm"))
                        inferred_email = primary.get("email")
                        if inferred_email and inferred_email not in emails:
                            emails.insert(0, inferred_email)
                        phones = _parse_json_list(candidate.get("telefones_norm"))

                        st.divider()
                        st.markdown("### Dossier")
                        col_d1, col_d2 = st.columns([1, 2], gap="medium")
                        with col_d1:
                            if avatar_url:
                                st.image(avatar_url, width=120)
                            st.caption(badge)
                            st.metric("Wealth Score", _format_currency(wealth_score))
                        with col_d2:
                            st.markdown(f"**{primary.get('name') or candidate.get('nome_socio') or ''}**")
                            st.caption(primary.get("role") or candidate.get("qualificacao") or "Socio")
                            if primary.get("linkedin_profile"):
                                st.markdown(f"[LinkedIn pessoal]({primary.get('linkedin_profile')})")

                        st.subheader("Empresas")
                        if empresas:
                            for empresa in list(dict.fromkeys(empresas))[:8]:
                                st.write(f"• {empresa}")
                        else:
                            st.caption("Sem empresas vinculadas.")

                        st.subheader("Contatos")
                        if emails:
                            st.write("E-mails provaveis: " + ", ".join(emails))
                        else:
                            st.caption("Sem e-mails encontrados.")
                        if phones:
                            st.write("Telefones: " + ", ".join(phones))
                        else:
                            st.caption("Sem telefones encontrados.")

                        col_a1, col_a2 = st.columns(2, gap="medium")
                        with col_a1:
                            if st.button("Salvar no Vault", type="primary", key="person_hunter_save"):
                                storage.upsert_person_enrichment(
                                    candidate.get("cnpj") or "",
                                    wealth_score,
                                    avatar_url,
                                    person_payload.get("person_json") or person,
                                )
                                st.toast("Lead salvo no Vault.", icon="✅")
                        with col_a2:
                            vcard = _build_vcard(
                                primary.get("name") or candidate.get("nome_socio") or "",
                                phones[0] if phones else "",
                                inferred_email or (emails[0] if emails else ""),
                            )
                            st.download_button(
                                "Baixar V-Card",
                                data=vcard,
                                file_name="person_hunter.vcf",
                                mime="text/vcard",
                            )

            with tabs[1]:
                st.caption("Upload CSV com colunas: nome, cidade, uf (opcional: cpf)")
                uploaded = st.file_uploader("CSV do lote", type=["csv"], key="person_hunter_csv")
                if uploaded:
                    df = pd.read_csv(uploaded)
                    df.columns = [str(col).strip().lower() for col in df.columns]
                    required = {"nome", "cidade", "uf"}
                    if not required.issubset(df.columns):
                        st.error("CSV invalido. Necessario conter colunas: nome, cidade, uf.")
                    else:
                        results = []
                        progress = st.progress(0)
                        total = len(df)
                        for idx, row in df.iterrows():
                            name = row.get("nome")
                            city = row.get("cidade")
                            state = row.get("uf")
                            cpf_value = row.get("cpf") if "cpf" in df.columns else ""
                            candidates = person_search.search_partners(
                                name=name,
                                cpf=cpf_value,
                                city=city,
                                state=state,
                            )
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
                            dossier_payload: Dict[str, Any] = {}
                            if resolved:
                                lead = person_search.candidate_to_lead(resolved)
                                try:
                                    dossier_payload = asyncio.run(_run_person_intel(lead))
                                except RuntimeError:
                                    dossier_payload = asyncio.get_event_loop().run_until_complete(_run_person_intel(lead))

                            person_payload = dossier_payload.get("person_json") if dossier_payload else {}
                            person_primary = _person_primary(person_payload)
                            emails = _parse_json_list(resolved.emails_norm) if resolved else []
                            inferred_email = person_primary.get("email") if person_primary else ""
                            telefone = ""
                            phones = _parse_json_list(resolved.telefones_norm) if resolved else []
                            if phones:
                                telefone = str(phones[0])

                            results.append(
                                {
                                    "nome": name,
                                    "cidade": city,
                                    "uf": state,
                                    "cpf": cpf_value,
                                    "status": status,
                                    "auto_resolved": auto_resolved,
                                    "cnpj": resolved.cnpj if resolved else "",
                                    "empresa": resolved.nome_fantasia or resolved.razao_social if resolved else "",
                                    "telefone": telefone,
                                    "email_inferido": inferred_email or (emails[0] if emails else ""),
                                    "wealth_score": dossier_payload.get("wealth_score") if dossier_payload else "",
                                }
                            )
                            progress.progress(min(1.0, (idx + 1) / max(1, total)))

                        result_df = pd.DataFrame(results)
                        st.dataframe(result_df, use_container_width=True)
                        csv_payload = result_df.to_csv(index=False)
                        st.download_button(
                            "Baixar CSV enriquecido",
                            data=csv_payload,
                            file_name="person_hunter_enriched.csv",
                            mime="text/csv",
                        )


def _render_system_core() -> None:
    st.markdown("## System Core")
    tabs = st.tabs(["Config & Keys", "Integracoes", "Black Box"])

    with tabs[0]:
        _micro_label("API Keys")
        col1, col2 = st.columns(2)
        with col1:
            casa_key = st.text_input("Casa dos Dados API Key", value=_env("CASA_DOS_DADOS_API_KEY"), type="password")
        with col2:
            serper_key = st.text_input("Serper.dev API Key", value=_env("SERPER_API_KEY"), type="password")
        col3, col4, col5 = st.columns(3)
        with col3:
            concurrency = st.number_input("Concorrencia", min_value=1, max_value=20, value=int(_env("CONCURRENCY", "10")))
        with col4:
            timeout = st.number_input("Timeout (s)", min_value=2, max_value=10, value=int(_env("TIMEOUT", "5")))
        with col5:
            cache_ttl = st.number_input("Cache TTL (horas)", min_value=1, max_value=168, value=int(_env("CACHE_TTL_HOURS", "24")))

        if st.button("Salvar configuracao", key="save_config"):
            _set_env("CASA_DOS_DADOS_API_KEY", casa_key)
            _set_env("SERPER_API_KEY", serper_key)
            _set_env("CONCURRENCY", str(int(concurrency)))
            _set_env("TIMEOUT", str(int(timeout)))
            _set_env("CACHE_TTL_HOURS", str(int(cache_ttl)))
            st.toast("Configuracao aplicada.", icon="✅")

    with tabs[1]:
        _micro_label("Integracoes")
        current_url = storage.config_get("webhook_url") or ""
        webhook_url = st.text_input("Webhook URL", value=current_url)
        col_a, col_b = st.columns(2)
        with col_a:
            if st.button("Salvar Webhook", key="save_webhook"):
                storage.config_set("webhook_url", webhook_url.strip())
                st.toast("Webhook salvo.", icon="✅")
        with col_b:
            if st.button("Testar Conexao", key="test_webhook"):
                if not webhook_url:
                    st.toast("Informe a URL do webhook.", icon="⚠️")
                else:
                    try:
                        resp = requests.post(
                            webhook_url,
                            json={"ping": "hunter_os", "timestamp": datetime.now(timezone.utc).isoformat()},
                            timeout=8,
                        )
                        if 200 <= resp.status_code < 300:
                            st.toast("Webhook respondeu com sucesso.", icon="✅")
                        else:
                            st.error("Webhook respondeu com erro. Verifique o endpoint.")
                    except Exception as exc:
                        st.error("Falha ao conectar no webhook.")
                        logger.warning("webhook_test failed: %s", exc)

    with tabs[2]:
        _micro_label("Black Box")
        with st.expander("Logs (debug)", expanded=False):
            logs = storage.fetch_logs(limit=200)
            if logs:
                lines = []
                for log in logs[::-1]:
                    detail = _parse_json(log.get("detail_json") or "{}")
                    message = detail.get("message") or detail.get("error") or ""
                    line = f"{log.get('created_at')} | {log.get('level')} | {log.get('event')} | {message}"
                    lines.append(line)
                st.code("\n".join(lines), language="bash")
            else:
                st.caption("Sem logs recentes.")


_inject_css()
_render_header()
active_view = _render_navigation()

if active_view == "MISSION":
    _render_mission_control()
elif active_view == "PERSON":
    _render_person_hunter()
elif active_view == "VAULT":
    _render_vault()
else:
    _render_system_core()
