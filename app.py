"""Hunter OS v3 - Sniper Elite Console."""

import asyncio
import html
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

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

from etl_pipeline import HunterOrchestrator
from modules import data_sources, enrichment_async, exports as webhook_exports, providers, scoring, storage


st.set_page_config(
    page_title="Hunter OS v3 - Sniper Elite",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="collapsed",
)

storage.init_db()


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
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=JetBrains+Mono:wght@400;700&display=swap');

        :root {
            --bg: #09090B;
            --surface: #18181B;
            --border: #27272A;
            --input: #121212;
            --text-primary: #FAFAFA;
            --text-secondary: #A1A1AA;
            --accent: #F97316;
            --accent-strong: #EA580C;
            --success: #10B981;
        }
        * {
            -webkit-font-smoothing: antialiased;
            -moz-osx-font-smoothing: grayscale;
        }
        html, body, [class*="css"]  {
            font-family: "Inter", sans-serif;
        }
        .stApp {
            background-color: var(--bg);
            background-image: radial-gradient(circle at 50% -20%, #1e1b4b 0%, var(--bg) 40%);
            color: var(--text-primary);
        }
        section[data-testid="stSidebar"] {
            display: none;
        }
        .block-container {
            padding-top: 5.5rem;
            padding-left: 2rem;
            padding-right: 2rem;
            max-width: 1200px;
        }
        .app-header {
            position: sticky;
            top: 0;
            z-index: 1000;
            background: rgba(24, 24, 27, 0.7);
            border-bottom: 1px solid rgba(255, 255, 255, 0.08);
            backdrop-filter: blur(16px);
            padding: 1.25rem 2rem 1rem 2rem;
            margin: -1.5rem -2rem 2rem -2rem;
        }
        .app-title {
            font-weight: 700;
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
            text-transform: uppercase;
            letter-spacing: 0.2em;
            font-size: 0.68rem;
            color: var(--text-secondary);
            margin-bottom: 0.35rem;
        }
        .nav-wrapper div[data-testid="stHorizontalBlock"] {
            background: rgba(24, 24, 27, 0.75);
            backdrop-filter: blur(16px);
            border-radius: 16px;
            padding: 10px 12px;
            border: 1px solid rgba(255, 255, 255, 0.08);
            position: sticky;
            top: 10px;
            z-index: 999;
            margin-bottom: 30px;
        }
        h1, h2, h3 {
            font-family: "Inter", sans-serif;
            letter-spacing: -0.025em;
            color: var(--text-primary) !important;
        }
        p, span, label, small {
            font-family: "Inter", sans-serif;
            color: var(--text-secondary) !important;
        }
        div[data-testid="stForm"], div.css-card {
            background-color: var(--surface);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 24px;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2), 0 2px 4px -1px rgba(0, 0, 0, 0.12);
        }
        div[data-testid="stVerticalBlockBorderWrapper"] {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 1.1rem;
        }
        input, textarea, div[data-baseweb="select"] > div {
            background-color: var(--input) !important;
            border: 1px solid var(--border) !important;
            color: var(--text-primary) !important;
            border-radius: 6px !important;
            transition: border-color 0.2s, box-shadow 0.2s;
        }
        input:focus, textarea:focus, div[data-baseweb="select"] > div:focus-within {
            border-color: var(--accent) !important;
            box-shadow: 0 0 0 1px var(--accent);
        }
        div.stButton > button[kind="primary"], button[kind="primary"] {
            background: var(--accent) !important;
            color: #FFFFFF !important;
            font-weight: 600 !important;
            border-radius: 8px !important;
            border: 1px solid rgba(255, 255, 255, 0.1) !important;
            box-shadow: 0 1px 2px 0 rgba(0, 0, 0, 0.05);
            transition: all 0.15s ease;
        }
        div.stButton > button[kind="primary"]:hover, button[kind="primary"]:hover {
            background: var(--accent-strong) !important;
            transform: translateY(-1px);
        }
        div.stButton > button[kind="secondary"], button[kind="secondary"] {
            background: transparent !important;
            border: 1px solid #3F3F46 !important;
            color: #E4E4E7 !important;
            border-radius: 8px !important;
        }
        div.stButton > button[kind="secondary"]:hover, button[kind="secondary"]:hover {
            border-color: rgba(255, 255, 255, 0.25) !important;
        }
        div[data-testid="stMetric"] {
            background-color: var(--surface);
            border: 1px solid var(--border);
            padding: 20px;
            border-radius: 12px;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2), 0 2px 4px -1px rgba(0, 0, 0, 0.12);
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
            border-radius: 12px;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2), 0 2px 4px -1px rgba(0, 0, 0, 0.12);
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
            border-radius: 8px;
            overflow: hidden;
        }
        div[data-testid="stDataFrame"] th {
            background-color: var(--border) !important;
            color: #E4E4E7 !important;
            font-weight: 600;
            border-bottom: 1px solid #3F3F46;
        }
        div[data-testid="stDataFrame"] td {
            background-color: var(--surface) !important;
            color: #D4D4D8 !important;
            border-bottom: 1px solid var(--border);
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
            border-radius: 10px;
            border: 1px solid var(--border);
            background: rgba(24, 24, 27, 0.8);
        }
        .timeline-item.active {
            border-color: #3f3f46;
            box-shadow: 0 0 0 1px rgba(161, 161, 170, 0.2);
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
            background: var(--bg);
            color: var(--success);
            border-radius: 12px;
            padding: 1rem;
            font-family: "JetBrains Mono", ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono",
                "Courier New", monospace;
            font-size: 0.8rem;
            line-height: 1.4rem;
            border: 1px solid #0f0f0f;
            max-height: 360px;
            overflow: auto;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_header() -> None:
    st.markdown(
        """
        <div class="app-header">
            <div class="app-title">HUNTER OS v3 • SNIPER ELITE</div>
            <div class="app-subtitle">Console de Inteligencia B2B — foco total em precisao e eficiencia.</div>
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
    col1, col2, col3 = st.columns(3, gap="small")
    with col1:
        label = "🎯 MISSION CONTROL" + (" •" if st.session_state.active_view == "MISSION" else "")
        if st.button(label, type="secondary", key="nav_mission"):
            st.session_state.active_view = "MISSION"
    with col2:
        label = "💎 THE VAULT" + (" •" if st.session_state.active_view == "VAULT" else "")
        if st.button(label, type="secondary", key="nav_vault"):
            st.session_state.active_view = "VAULT"
    with col3:
        label = "⚙️ SYSTEM CORE" + (" •" if st.session_state.active_view == "SYSTEM" else "")
        if st.button(label, type="secondary", key="nav_system"):
            st.session_state.active_view = "SYSTEM"
    st.markdown("</div>", unsafe_allow_html=True)
    return st.session_state.active_view


def _render_mission_control() -> None:
    st.markdown("## Mission Control")
    _micro_label("TARGETING")

    col_a, col_b = st.columns(2, gap="large")
    with col_a:
        with st.container(border=True):
            st.caption("Geografia")
            col_geo_long, col_geo_short = st.columns([3, 1], gap="medium")
            with col_geo_long:
                municipios = st.multiselect("Municipios", data_sources.get_cidades_disponiveis(), default=["MARINGA"])
            with col_geo_short:
                uf = st.selectbox("UF", ["PR", "SP", "RJ", "MG", "SC", "RS", "BA", "GO", "DF"], index=0)
    with col_b:
        with st.container(border=True):
            st.caption("Setor & CNAE")
            setores = st.multiselect("Setores", data_sources.get_setores_disponiveis(), default=["Servicos Administrativos"])
            col_cnae_long, col_cnae_short = st.columns([3, 1], gap="medium")
            with col_cnae_long:
                cnaes_manual = st.text_area("CNAE manual (opcional)", height=70)
            with col_cnae_short:
                excluir_mei = st.toggle("Excluir MEI", value=True)

    with st.expander("Parametros avancados"):
        col_p1, col_p2, col_p3 = st.columns(3)
        with col_p1:
            limite = st.number_input("Limite de leads", min_value=10, max_value=5000, value=200, step=10)
            page_size = st.number_input("Itens por pagina (API)", min_value=10, max_value=1000, value=200, step=10)
        with col_p2:
            com_telefone = st.toggle("Somente com telefone", value=False)
            com_email = st.toggle("Somente com email", value=False)
            telefone_repeat_threshold = st.number_input("Telefone repetido (min N)", min_value=2, max_value=20, value=5)
        with col_p3:
            enable_enrichment = st.toggle("Enriquecimento Hibrido", value=True)
            enrich_top_pct = st.slider("Top % para enriquecer", min_value=5, max_value=100, value=25, step=5)
            cache_ttl_hours = st.number_input("Cache TTL (horas)", min_value=1, max_value=168, value=int(_env("CACHE_TTL_HOURS", "24")))

    cnaes = _flatten_cnaes(setores, cnaes_manual)
    provider = _env("SEARCH_PROVIDER", "serper")

    col_left, col_center, col_right = st.columns([1, 2, 1])
    with col_center:
        if st.button("INICIAR CAÇADA AGORA", type="primary", key="start_hunt", use_container_width=True):
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

    _micro_label("LIVE MONITOR")
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
            col_s1, col_s2, col_s3, col_s4 = st.columns(4)
            col_s1.metric("Status", status)
            col_s2.metric("Etapa atual", current_stage)
            col_s3.metric("Estrategia", run.get("strategy") or "-")
            col_s4.metric("Worker", "rodando" if running else "parado")

            st.progress(min(100, int((processed / max(1, total_leads)) * 100)) if total_leads else 0)
            st.caption(f"Progresso: {_progress_label(processed, total_leads)}")

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
                timeline_html.append(
                    f"<div class='timeline-item {state}'>"
                    f"<div>{icon}</div><div><strong>{title}</strong><br><span>{caption}</span></div></div>"
                )
            timeline_html.append("</div>")
            st.markdown("".join(timeline_html), unsafe_allow_html=True)

            col_a, col_b = st.columns(2)
            with col_a:
                if st.button("Panico/Parar", key="panic_button"):
                    orchestrator.cancel_job(active_run_id)
                    st.toast("Job pausado.")
            with col_b:
                if status in {"PAUSED", "FAILED"} and not running:
                    if st.button("Retomar", key="resume_button"):
                        orchestrator.resume_job(active_run_id)
                        st.toast("Retomando caçada.")

            with st.expander("Logs vivos (ultimas 10 linhas)"):
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
    col_k1.metric("📊 Total Leads", total_leads)
    col_k2.metric("🧪 Enriquecidos", total_enriched)
    col_k3.metric("⚡ Taxa de Enriquecimento", _tech_rate(total_leads, total_enriched))
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
                <div class="kpi-label">🔥 Leads Hot</div>
                <div class="kpi-value">{qualified_count}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    _micro_label("FILTROS")
    with st.container(border=True):
        status_labels = ["Todos", "🟣 Enriquecidos", "⚪ Pendentes"]
        status_label = st.radio("Status", options=status_labels, horizontal=True, key="vault_status_filter")
        status_filter_map = {"Todos": "all", "🟣 Enriquecidos": "enriched", "⚪ Pendentes": "pending"}
        status_filter = status_filter_map[status_label]
        col_f1, col_f2 = st.columns(2, gap="medium")
        with col_f1:
            min_score = st.number_input("Score minimo", min_value=0, max_value=100, value=0)
        with col_f2:
            min_tech_score = st.number_input("Tech score minimo", min_value=0, max_value=30, value=0)
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
        filter_contact = contact_quality or None
        filter_municipio = municipio or None

        filters = {
            "min_score": filter_min_score,
            "min_tech_score": filter_min_tech,
            "contact_quality": filter_contact,
            "municipio": filter_municipio,
        }

        filtered_total = storage.count_vault_data(filters, status_filter=status_filter)
        pending_total = storage.count_vault_data(filters, status_filter="pending")
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

    vault_rows = storage.get_vault_data(
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
            if st.button("⚡ Enriquecer Lote (Proximos 50)", type="primary", key="vault_enrich_batch"):
                pending_rows = storage.get_vault_data(
                    page=1,
                    page_size=50,
                    filters=filters,
                    status_filter="pending",
                )
                if not pending_rows:
                    st.toast("Nao ha leads pendentes para enriquecer.", icon="✅")
                else:
                    leads: List[Dict[str, Any]] = []
                    lead_map: Dict[str, Dict[str, Any]] = {}
                    for row in pending_rows:
                        lead = {
                            "cnpj": row.get("cnpj"),
                            "razao_social": row.get("razao_social"),
                            "nome_fantasia": row.get("nome_fantasia"),
                            "municipio": row.get("municipio"),
                            "uf": row.get("uf"),
                            "porte": row.get("porte"),
                            "contact_quality": row.get("contact_quality"),
                            "flags": _parse_json(row.get("flags_json")),
                            "emails_norm": _parse_json_list(row.get("emails_norm")),
                        }
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

    display_cols = [
        "cnpj",
        "razao_social",
        "enrichment_status",
        "status_label",
        "score_v2",
        "stack_tags",
        "site_link",
        "linkedin_link",
        "instagram_link",
        "maps_link",
    ]
    df_display = df_vault[display_cols].copy()
    df_display.insert(0, "selecionar", False)

    edited = st.data_editor(
        df_display,
        hide_index=True,
        width="stretch",
        disabled=[col for col in df_display.columns if col != "selecionar"],
        column_config={
            "selecionar": st.column_config.CheckboxColumn("Selecionar"),
            "razao_social": st.column_config.TextColumn("Razao Social"),
            "enrichment_status": st.column_config.TextColumn("Status"),
            "status_label": st.column_config.TextColumn("Qualidade"),
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

    _micro_label("ACTIONS")
    with st.container(border=True):
        col_a, col_b = st.columns([2, 3], gap="medium")
        with col_a:
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
            export_df = webhook_exports.format_export_data(export_rows, socios_map=socios_map)
            export_suffix = {
                "Pagina atual": "pagina",
                "Todos filtrados": "completo",
                "Selecionados": "selecionados",
            }[export_scope]
            csv_data = export_df.to_csv(index=False)
            st.download_button(
                "Exportar CSV",
                data=csv_data,
                file_name=f"hunter_vault_{export_suffix}.csv",
                mime="text/csv",
                disabled=export_df.empty,
            )
            if not export_df.empty:
                st.caption(f"{len(export_df)} linhas no CSV.")
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


def _render_system_core() -> None:
    st.markdown("## System Core")
    tabs = st.tabs(["Config & Keys", "Integracoes", "Black Box"])

    with tabs[0]:
        _micro_label("API KEYS")
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
        _micro_label("INTEGRACOES")
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
        _micro_label("BLACK BOX")
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
elif active_view == "VAULT":
    _render_vault()
else:
    _render_system_core()
