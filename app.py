"""Hunter OS - B2B Prospecting (refactor v2)."""

import json
import os
import hashlib
import re
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

import pandas as pd
import streamlit as st

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

from modules import data_sources, jobs, storage


st.set_page_config(
    page_title="Hunter OS - B2B Prospecting",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
)

storage.init_db()


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


def _progress_label(current: int, total: int) -> Tuple[int, str]:
    if total <= 0:
        return 0, f"{current}"
    percent = min(100, int(round((current / total) * 100)))
    return percent, f"{current}/{total} ({percent}%)"


def _format_duration(seconds: float) -> str:
    if seconds <= 0:
        return "0s"
    if seconds < 60:
        return f"{int(seconds)}s"
    minutes = int(seconds // 60)
    remainder = int(seconds % 60)
    return f"{minutes}m {remainder}s"


def _count_fresh_cache(enrichments: Dict[str, Dict[str, Any]], ttl_hours: int) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=ttl_hours)
    fresh = 0
    for item in enrichments.values():
        enriched_at = item.get("enriched_at")
        if not enriched_at:
            continue
        try:
            ts = datetime.strptime(enriched_at, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        if ts >= cutoff:
            fresh += 1
    return fresh


def _stack_summary(tech_stack_json: Optional[str]) -> str:
    parsed = _parse_json(tech_stack_json)
    stack = []
    if isinstance(parsed, dict):
        stack = parsed.get("detected_stack") or parsed.get("stack") or []
    elif isinstance(parsed, list):
        stack = parsed
    if not stack:
        return ""
    return ", ".join([str(item) for item in stack][:6])


def _signals_summary(signals_json: Optional[str]) -> Dict[str, List[str]]:
    parsed = _parse_json(signals_json)
    if isinstance(parsed, dict):
        return {str(key): value for key, value in parsed.items() if value}
    return {}


def _log_message(detail_json: str) -> str:
    detail = _parse_json(detail_json)
    if not isinstance(detail, dict):
        return ""
    return (
        detail.get("message")
        or detail.get("error")
        or detail.get("hint")
        or ""
    )


def _render_status_pills(pills: List[str]) -> None:
    if not pills:
        return
    items = "".join(
        [
            "<span style=\"padding:4px 10px;border-radius:999px;background:#eef2f7;"
            "color:#1f2937;font-size:12px;font-weight:600;border:1px solid #d8dee8;\">"
            f"{pill}</span>"
            for pill in pills
        ]
    )
    html = (
        "<div style=\"display:flex;flex-wrap:wrap;gap:8px;margin:6px 0 4px 0;\">"
        f"{items}</div>"
    )
    st.markdown(html, unsafe_allow_html=True)


def _warning_reason_text(reason: Optional[str]) -> str:
    if not reason:
        return ""
    mapping = {
        "provider_rate_limit": "Limite do provider atingido.",
        "provider_error": "Falha no provider de busca.",
        "partial_enrichment": "Enriquecimento parcial (nem todos os leads).",
    }
    return mapping.get(reason, reason)


PIPELINE_STEPS = [
    ("extract", "Extract (Casa dos Dados)"),
    ("cleaning", "Clean/Dedup"),
    ("scoring_v1", "Score v1"),
    ("enriching", "Enrich"),
    ("scoring_v2", "Score v2"),
    ("export", "Export (manual)"),
]

STATUS_TO_STEP = {
    "extracting": "extract",
    "cleaning": "cleaning",
    "scoring_v1": "scoring_v1",
    "enriching": "enriching",
    "scoring_v2": "scoring_v2",
    "export_created": "export",
    "importing": "extract",
    "paused_provider_limit": "enriching",
}


def _module_fingerprint(module) -> Dict[str, str]:
    path = Path(getattr(module, "__file__", ""))
    if not path.exists():
        return {"path": str(path), "hash": "n/a"}
    payload = path.read_bytes()
    short_hash = hashlib.sha256(payload).hexdigest()[:8]
    return {"path": str(path), "hash": short_hash}


def _find_step(run_steps: List[Dict[str, Any]], step_name: str) -> Optional[Dict[str, Any]]:
    for step in run_steps:
        if step.get("step_name") == step_name:
            return step
    return None


def _compute_step_states(
    run_steps: List[Dict[str, Any]],
    run_status: str,
    enable_enrichment: bool,
    run_type: str,
) -> Dict[str, str]:
    states: Dict[str, str] = {}
    completed_steps = {step.get("step_name") for step in run_steps}
    step_status = {step.get("step_name"): step.get("status") for step in run_steps}
    for key, _label in PIPELINE_STEPS:
        if run_type == "export" and key != "export":
            states[key] = "skipped"
            continue
        if key == "enriching" and not enable_enrichment:
            states[key] = "skipped"
            continue
        if key == "extract" and run_type in {"recovery", "upload_excel"}:
            import_step = "import_csv" if run_type == "recovery" else "import_excel"
            if import_step in completed_steps:
                states[key] = "done"
            elif run_status == "importing":
                states[key] = "active"
            else:
                states[key] = "pending"
            continue
        if key == "export" and "export_create_v5" in completed_steps:
            states[key] = "done"
            continue
        if step_status.get(key) == "paused_provider_limit":
            states[key] = "active"
            continue
        if key in completed_steps:
            states[key] = "done"
            continue
        active_key = STATUS_TO_STEP.get(run_status, "")
        if key == active_key:
            states[key] = "active"
        else:
            states[key] = "pending"
    return states


def _format_stepper(
    run_steps: List[Dict[str, Any]],
    run_status: str,
    enable_enrichment: bool,
    run_type: str,
) -> Tuple[str, int]:
    states = _compute_step_states(run_steps, run_status, enable_enrichment, run_type)
    lines = []
    done_count = 0
    total = 0
    for key, label in PIPELINE_STEPS:
        if run_type == "recovery" and key == "extract":
            label = "Import CSV"
        elif run_type == "upload_excel" and key == "extract":
            label = "Import Excel"
        state = states.get(key, "pending")
        if state == "skipped":
            lines.append(f"[-] {label} (skip)")
            continue
        total += 1
        if state == "done":
            done_count += 1
            symbol = "[x]"
        elif state == "active":
            symbol = "[>]"
        else:
            symbol = "[ ]"
        lines.append(f"{symbol} {label}")
    percent = int(round((done_count / max(1, total)) * 100))
    return "\n".join(lines), percent


def _no_balance_message(logs: List[Dict[str, Any]]) -> str:
    for log in logs:
        if log.get("level") != "error":
            continue
        detail = _parse_json(log.get("detail_json"))
        message = str(detail.get("error") or "")
        if detail.get("error_code") == "no_balance":
            return message or "Casa dos Dados sem saldo. Recarregue creditos e tente novamente."
        if "sem saldo" in message.lower():
            return "Casa dos Dados sem saldo. Recarregue creditos e tente novamente."
    return ""


def _flatten_cnaes(setores: List[str], manual: str) -> List[str]:
    cnaes = []
    for setor in setores:
        cnaes.extend(data_sources.SETORES_CNAE.get(setor, []))
    if manual:
        for item in manual.replace("\n", ",").split(","):
            code = "".join([c for c in item.strip() if c.isdigit()])
            if code:
                cnaes.append(code)
    return list(dict.fromkeys(cnaes))


def _build_export_rows(leads: List[Dict[str, Any]], enrichments: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    for lead in leads:
        flags = _parse_json(lead.get("flags_json"))
        enrichment = enrichments.get(lead.get("cnpj"), {})
        tech_stack = _parse_json(enrichment.get("tech_stack_json"))
        rows.append(
            {
                "cnpj": lead.get("cnpj"),
                "razao_social": lead.get("razao_social"),
                "municipio": lead.get("municipio"),
                "uf": lead.get("uf"),
                "cnae": lead.get("cnae"),
                "score": lead.get("score_v2"),
                "score_label": lead.get("score_label"),
                "contact_quality": lead.get("contact_quality"),
                "site": enrichment.get("site"),
                "instagram": enrichment.get("instagram"),
                "linkedin_company": enrichment.get("linkedin_company"),
                "google_maps_url": enrichment.get("google_maps_url") or flags.get("google_maps_url"),
                "tech_score": enrichment.get("tech_score"),
                "whatsapp_probable": flags.get("whatsapp_probable"),
                "has_whatsapp_link": tech_stack.get("has_whatsapp_link"),
            }
        )
    return rows


st.title("Hunter OS - B2B Prospecting")

if "current_run_id" not in st.session_state:
    st.session_state.current_run_id = None


buscar_tab, monitor_tab, resultados_tab, exports_tab, recovery_tab, vault_tab, runs_tab, diagnostics_tab, config_tab = st.tabs(
    [
        "Buscar",
        "Monitor (Live)",
        "Resultados",
        "Exports (Casa dos Dados)",
        "Recovery",
        "Enrichment Vault",
        "Runs/Jobs",
        "Diagnostico",
        "Config",
    ]
)

with config_tab:
    st.subheader("Config")

    col1, col2 = st.columns(2)
    with col1:
        casa_key = st.text_input(
            "Casa dos Dados API Key",
            value=_env("CASA_DOS_DADOS_API_KEY"),
            type="password",
        )
        serper_key = st.text_input(
            "Serper.dev API Key",
            value=_env("SERPER_API_KEY"),
            type="password",
        )

    with col2:
        provider = st.selectbox(
            "Search Provider",
            options=["serper"],
            index=0,
        )
        cache_ttl_hours = st.number_input(
            "Cache TTL (horas)",
            min_value=1,
            max_value=168,
            value=24,
            key="config_cache_ttl_hours",
        )
        concurrency = st.number_input("Concorrencia", min_value=1, max_value=20, value=10)
        timeout = st.number_input("Timeout por request (s)", min_value=2, max_value=10, value=5)
        serper_max_rps = st.number_input(
            "Serper max RPS",
            min_value=1,
            max_value=20,
            value=int(_env("SERPER_MAX_RPS", "5")),
        )
        serper_concurrency = st.number_input(
            "Serper concurrency",
            min_value=1,
            max_value=20,
            value=int(_env("SERPER_CONCURRENCY", "5")),
        )
        backoff_base = st.number_input(
            "Provider backoff base",
            min_value=1.0,
            max_value=5.0,
            value=float(_env("PROVIDER_BACKOFF_BASE", "1.5")),
            step=0.1,
        )
        backoff_max = st.number_input(
            "Provider backoff max (s)",
            min_value=5,
            max_value=120,
            value=int(_env("PROVIDER_BACKOFF_MAX", "60")),
        )

    if st.button("Salvar configuracao"):
        _set_env("CASA_DOS_DADOS_API_KEY", casa_key)
        _set_env("SERPER_API_KEY", serper_key)
        _set_env("SEARCH_PROVIDER", provider)
        _set_env("CACHE_TTL_HOURS", str(int(cache_ttl_hours)))
        _set_env("CONCURRENCY", str(int(concurrency)))
        _set_env("TIMEOUT", str(int(timeout)))
        _set_env("SERPER_MAX_RPS", str(int(serper_max_rps)))
        _set_env("SERPER_CONCURRENCY", str(int(serper_concurrency)))
        _set_env("PROVIDER_BACKOFF_BASE", str(backoff_base))
        _set_env("PROVIDER_BACKOFF_MAX", str(int(backoff_max)))
        st.success("Configuracao aplicada para esta sessao.")

    st.markdown("#### Arquivos carregados")
    mod_data_sources = _module_fingerprint(data_sources)
    mod_jobs = _module_fingerprint(jobs)
    mod_storage = _module_fingerprint(storage)
    st.caption(f"data_sources: {mod_data_sources['path']} ({mod_data_sources['hash']})")
    st.caption(f"jobs: {mod_jobs['path']} ({mod_jobs['hash']})")
    st.caption(f"storage: {mod_storage['path']} ({mod_storage['hash']})")
    st.markdown("#### Limites do provider")
    effective_concurrency = min(int(_env("CONCURRENCY", "10")), int(_env("SERPER_CONCURRENCY", "5")))
    st.caption(f"Provider: {provider}")
    st.caption(f"max_rps={_env('SERPER_MAX_RPS', '5')} | concurrency efetiva={effective_concurrency}")
    st.warning("Acima do limite, ocorrera pausa automatica (HTTP 429).")

with buscar_tab:
    st.subheader("Buscar")

    col1, col2, col3 = st.columns(3)
    with col1:
        uf = st.selectbox("UF", ["PR", "SP", "RJ", "MG", "SC", "RS", "BA", "GO", "DF"], index=0)
        municipios = st.multiselect("Municipios", data_sources.get_cidades_disponiveis(), default=["MARINGA"])
        setores = st.multiselect("Setores", data_sources.get_setores_disponiveis(), default=["Servicos Administrativos"])
    with col2:
        cnaes_manual = st.text_area("CNAE manual (opcional)", height=100)
        excluir_mei = st.checkbox("Excluir MEI", value=True)
        com_telefone = st.checkbox("Somente com telefone", value=False)
        com_email = st.checkbox("Somente com email", value=False)
    with col3:
        limite = st.number_input("Limite de leads", min_value=10, max_value=5000, value=200, step=10)
        mode = st.selectbox("Modo", options=["bulk", "pagination"], index=1)
        enrich_top_pct = st.slider("Top % para enriquecer", min_value=5, max_value=100, value=25, step=5)
        enable_enrichment = st.checkbox("Enriquecer", value=True)
        export_all = st.checkbox("Exportar tudo (CSV)", value=False)
        if export_all:
            st.caption("Cria um arquivo na Casa dos Dados (sem paginar). Use a aba Recovery para importar o CSV.")

    st.markdown("---")
    col4, col5, col6 = st.columns(3)
    with col4:
        telefone_repeat_threshold = st.number_input("Telefone repetido (min N)", min_value=2, max_value=20, value=5)
    with col5:
        cache_ttl = int(_env("CACHE_TTL_HOURS", "24"))
        cache_ttl_hours = st.number_input(
            "Cache TTL (horas)",
            min_value=1,
            max_value=168,
            value=cache_ttl,
            key="buscar_cache_ttl_hours",
        )
    with col6:
        provider = st.selectbox("Provider", options=["serper"], index=0)
        page_size = st.number_input("Itens por pagina (API)", min_value=10, max_value=1000, value=200, step=10)

    cnaes = _flatten_cnaes(setores, cnaes_manual)

    with st.expander("Risco e custo (estimativa)"):
        safe_enrich_limit = st.selectbox(
            "Modo seguro: limitar enriquecimento",
            options=[0, 50, 100, 200, 500],
            index=0,
            format_func=lambda v: "Sem limite" if v == 0 else f"Limitar a {v}",
            key="safe_limit_search",
        )
        cache_only = st.checkbox(
            "Somente cache (sem chamadas externas)",
            value=False,
            key="cache_only_search",
        )

        est_total = int(limite)
        est_to_enrich = int(round(est_total * enrich_top_pct / 100)) if enable_enrichment else 0
        if safe_enrich_limit:
            est_to_enrich = min(est_to_enrich, int(safe_enrich_limit))
        est_requests = 0 if cache_only or not enable_enrichment else est_to_enrich
        max_rps = max(1, int(_env("SERPER_MAX_RPS", "5")))
        est_seconds = est_requests / max_rps if est_requests else 0

        col_r1, col_r2, col_r3 = st.columns(3)
        col_r1.metric("Leads estimados", est_total)
        col_r2.metric("Requisicoes externas (estim.)", est_requests)
        col_r3.metric("Tempo estimado", _format_duration(est_seconds))
        st.caption("Cache reaproveitado so pode ser estimado apos a coleta.")

    if st.button("Iniciar run"):
        params = {
            "uf": uf,
            "municipios": municipios,
            "cnaes": cnaes,
            "excluir_mei": excluir_mei,
            "com_telefone": com_telefone,
            "com_email": com_email,
            "limite": int(limite),
            "mode": mode,
            "cache_ttl_hours": int(cache_ttl_hours),
            "telefone_repeat_threshold": int(telefone_repeat_threshold),
            "enrich_top_pct": int(enrich_top_pct),
            "enable_enrichment": enable_enrichment,
            "export_all": export_all,
            "provider": provider,
            "page_size": int(page_size),
            "concurrency": int(_env("CONCURRENCY", "10")),
            "timeout": int(_env("TIMEOUT", "5")),
            "run_type": "export" if export_all else "standard",
            "safe_enrich_limit": int(safe_enrich_limit) if safe_enrich_limit else None,
            "cache_only": bool(cache_only),
        }
        run_id = jobs.start_run(params)
        st.session_state.current_run_id = run_id
        st.success(f"Run iniciado: {run_id}")

    if st.session_state.current_run_id:
        current_run_id = st.session_state.current_run_id

        @st.fragment(run_every="2s")
        def _render_run_status(run_id: str) -> None:
            run = storage.get_run(run_id)
            if not run:
                st.info("Run nao encontrado.")
                return
            st.markdown("---")
            st.subheader("Status do run")
            col_a, col_b, col_c, col_d = st.columns(4)
            col_a.metric("Status", run.get("status"))
            col_b.metric("Total leads", run.get("total_leads"))
            col_c.metric("Enriquecidos", run.get("enriched_count"))
            col_d.metric("Erros", run.get("errors_count"))
            if run.get("status") == "completed_with_warnings":
                st.warning("Concluido com avisos. Veja detalhes do provider e logs.")
            if run.get("status") == "paused_provider_limit":
                max_rps = int(_env("SERPER_MAX_RPS", "5"))
                st.warning(
                    f"Serper limitou a {max_rps} req/s. O Hunter OS pausou para evitar custo e bloqueio."
                )
            if run.get("status") == "completed" and (run.get("errors_count") or 0) > 0:
                st.warning("Concluido com avisos. Veja Diagnostico para detalhes.")
            running = jobs.is_running(run.get("run_id"))
            st.caption(f"Worker: {'rodando' if running else 'parado'}")
            warning_reason = _warning_reason_text(run.get("warning_reason"))
            if warning_reason:
                st.caption(f"Aviso: {warning_reason}")
            provider_status = run.get("provider_http_status")
            provider_message = run.get("provider_message")
            if provider_status or provider_message:
                st.caption(f"Provider feedback: HTTP {provider_status or '-'} - {provider_message or '-'}")

            logs = storage.fetch_logs(limit=20, run_id=run.get("run_id"))
            error_message = _no_balance_message(logs)
            if error_message:
                st.error(error_message)
            if run.get("status") == "export_created":
                st.info("Export criado. Acompanhe em Exports (Casa dos Dados) para buscar o link e baixar o CSV.")

            params = _parse_json(run.get("params_json"))
            limite = int(params.get("limite") or 0)
            top_pct = int(params.get("enrich_top_pct") or 25)
            enable_enrichment = bool(params.get("enable_enrichment", True))
            run_type = str(params.get("run_type") or "standard")
            run_steps = storage.fetch_run_steps(run.get("run_id"))
            stepper_lines, progress_pct = _format_stepper(
                run_steps,
                run.get("status"),
                enable_enrichment,
                run_type,
            )

            st.markdown("#### Evolucao do run")
            st.progress(progress_pct)
            st.caption(f"Progresso geral: {progress_pct}%")
            st.markdown(stepper_lines)

            extract_step = _find_step(run_steps, "extract")
            extract_details = _parse_json(extract_step.get("details_json")) if extract_step else {}
            total_encontrado = extract_details.get("total_encontrado")
            itens_coletados = extract_details.get("itens_coletados")
            descartados = extract_details.get("itens_descartados_por_limite")
            pages_processed = extract_details.get("pages_processed")
            import_details: Dict[str, Any] = {}
            if run_type in {"recovery", "upload_excel"}:
                import_step_name = "import_csv" if run_type == "recovery" else "import_excel"
                import_step = _find_step(run_steps, import_step_name)
                import_details = _parse_json(import_step.get("details_json")) if import_step else {}
            enrich_step = _find_step(run_steps, "enriching")
            enrich_details = _parse_json(enrich_step.get("details_json")) if enrich_step else {}
            provider_error = enrich_details.get("provider_error")
            total_leads = int(run.get("total_leads") or 0)
            enriched_count = int(run.get("enriched_count") or 0)

            pills: List[str] = []
            if run_type not in {"export"}:
                if run_type in {"recovery", "upload_excel"}:
                    _pct, label = _progress_label(total_leads, total_leads)
                    pills.append(f"Importacao: {label}")
                else:
                    total_found = int(total_encontrado or 0)
                    target = total_found if total_found and total_found < limite else limite
                    _pct, label = _progress_label(total_leads, int(target or 0))
                    pills.append(f"Consultas: {label}")
                if enable_enrichment:
                    planned = int(run.get("planned_to_enrich") or 0)
                    target = planned if planned else (int(round(total_leads * top_pct / 100)) if total_leads else 0)
                    _pct, label = _progress_label(enriched_count, target)
                    pills.append(f"Enriquecimento: {label}")
            _render_status_pills(pills)

            st.markdown("#### Andamento da busca e enriquecimento")
            if run_type == "export":
                st.info("Run de export criado. Acompanhe o processamento na aba Exports.")
            elif run_type in {"recovery", "upload_excel"}:
                col_p1, col_p2 = st.columns(2)
                with col_p1:
                    st.metric("Linhas importadas", import_details.get("rows") or 0)
                with col_p2:
                    st.metric("Fonte", import_details.get("source") or "upload")
                if import_details.get("file_path"):
                    st.caption(f"Arquivo: {import_details.get('file_path')}")
            else:
                col_p1, col_p2, col_p3 = st.columns(3)
                with col_p1:
                    if total_encontrado is not None:
                        st.metric("Total encontrado (API)", total_encontrado)
                    st.metric("Limite solicitado", limite)
                with col_p2:
                    st.metric("Coletados", itens_coletados or 0)
                    st.metric("Descartados por limite", descartados or 0)
                with col_p3:
                    st.metric("Paginas processadas", pages_processed or 0)
                    st.metric("Chamadas API", pages_processed or 0)

                if total_encontrado and limite and total_encontrado > limite:
                    st.info(
                        f"Encontradas {total_encontrado} empresas. "
                        f"Coletando apenas {limite} para evitar consumo desnecessario."
                    )
                if extract_details.get("cache_hit"):
                    st.caption("Resultado reaproveitado do cache local (sem nova consulta).")
            if provider_error:
                if isinstance(provider_error, dict):
                    provider_name = provider_error.get("provider") or "provider"
                    message = provider_error.get("message") or "Falha no provider de busca."
                    st.warning(f"Enriquecimento pausado: {provider_name} - {message}")
                    hint = provider_error.get("hint")
                    if hint:
                        st.caption(hint)
                else:
                    st.warning(f"Enriquecimento pausado: {provider_error}")

            if enable_enrichment:
                processed_count = enrich_details.get("processed_count")
                errors_count = enrich_details.get("errors_count")
                cache_hits = enrich_details.get("cache_hits")
                avg_fetch_ms = enrich_details.get("avg_fetch_ms")
                alvo = enrich_details.get("alvo_enriquecimento")
                if any(value is not None for value in [processed_count, errors_count, cache_hits, avg_fetch_ms, alvo]):
                    st.markdown("#### Enriquecimento (detalhes)")
                    col_e1, col_e2, col_e3, col_e4, col_e5 = st.columns(5)
                    col_e1.metric("Alvo", alvo or 0)
                    col_e2.metric("Processados", processed_count or 0)
                    col_e3.metric("Erros", errors_count or 0)
                    col_e4.metric("Cache hits", cache_hits or 0)
                    col_e5.metric("Tempo medio (ms)", avg_fetch_ms or 0)

            col_p4, col_p5 = st.columns(2)
            with col_p4:
                if run_type in {"recovery", "upload_excel"}:
                    pct, label = _progress_label(total_leads, total_leads)
                    st.metric("Importacao (arquivo)", label)
                    st.progress(pct)
                elif limite > 0:
                    total_found = int(total_encontrado or 0)
                    target = total_found if total_found and total_found < limite else limite
                    pct, label = _progress_label(total_leads, int(target or 0))
                    st.metric("Consulta (Casa dos Dados)", label)
                    st.progress(pct)
                else:
                    st.metric("Consulta (Casa dos Dados)", str(total_leads))
            with col_p5:
                if enable_enrichment:
                    planned = int(run.get("planned_to_enrich") or 0)
                    target = planned if planned else (int(round(total_leads * top_pct / 100)) if total_leads else 0)
                    pct, label = _progress_label(enriched_count, target)
                    st.metric("Enriquecimento", label)
                    st.progress(pct)
                    if target:
                        st.caption(f"Meta: top {top_pct}% (aprox.)")
                else:
                    st.metric("Enriquecimento", "off")

            col_actions = st.columns(2)
            with col_actions[0]:
                if run.get("status") in {
                    "extracting",
                    "cleaning",
                    "scoring_v1",
                    "enriching",
                    "scoring_v2",
                    "importing",
                    "paused_provider_limit",
                }:
                    if st.button("Cancelar run"):
                        jobs.cancel_run(run.get("run_id"))
                        st.warning("Cancelamento solicitado")
            with col_actions[1]:
                if run.get("status") in {"queued", "failed", "canceled", "paused_provider_limit"} and not running:
                    if st.button("Retomar run"):
                        resumed = jobs.resume_run(run.get("run_id"))
                        if resumed:
                            st.success("Run retomado")
                        else:
                            st.error("Nao foi possivel retomar este run.")

            st.markdown("#### Ultimos logs")
            if logs:
                df_logs = pd.DataFrame(logs)
                st.dataframe(
                    df_logs[["created_at", "level", "event", "detail_json"]],
                    width="stretch",
                )
            else:
                st.info("Sem logs para este run ainda.")

        _render_run_status(current_run_id)

with monitor_tab:
    st.subheader("Monitor (Live)")

    runs = storage.list_runs(limit=50)
    run_ids = [r.get("run_id") for r in runs] if runs else []
    default_run_id = st.session_state.get("current_run_id") or (run_ids[0] if run_ids else "")

    def _monitor_label(run_id: str) -> str:
        if not run_id:
            return "Selecione um run"
        run = storage.get_run(run_id) or {}
        status = run.get("status", "unknown")
        return f"{run_id[:8]}... ({status})"

    selected_run_id = st.selectbox(
        "Run para monitorar",
        options=[""] + run_ids,
        index=(run_ids.index(default_run_id) + 1) if default_run_id in run_ids else 0,
        format_func=_monitor_label,
        key="monitor_run_id",
    )

    @st.fragment(run_every="2s")
    def _render_live(run_id: str) -> None:
        if not run_id:
            st.info("Selecione um run para acompanhar.")
            return
        run = storage.get_run(run_id)
        if not run:
            st.warning("Run nao encontrado.")
            return
        params = _parse_json(run.get("params_json"))
        run_type = str(params.get("run_type") or "standard")
        enable_enrichment = bool(params.get("enable_enrichment", True))
        run_steps = storage.fetch_run_steps(run_id)
        stepper_lines, progress_pct = _format_stepper(
            run_steps,
            run.get("status"),
            enable_enrichment,
            run_type,
        )

        col_s1, col_s2, col_s3, col_s4 = st.columns(4)
        col_s1.metric("Status", run.get("status"))
        col_s2.metric("Total leads", run.get("total_leads"))
        col_s3.metric("Enriquecidos", run.get("enriched_count"))
        col_s4.metric("Erros", run.get("errors_count"))

        if run.get("status") == "paused_provider_limit":
            max_rps = int(_env("SERPER_MAX_RPS", "5"))
            st.warning(
                f"Serper limitou a {max_rps} req/s. O Hunter OS pausou para evitar custo e bloqueio."
            )
        elif run.get("status") == "completed_with_warnings":
            st.warning("Run concluido com avisos. Veja detalhes abaixo.")
        elif run.get("status") == "failed":
            st.error("Run falhou. Verifique logs e erros.")
        warning_reason = _warning_reason_text(run.get("warning_reason"))
        if warning_reason:
            st.caption(f"Aviso: {warning_reason}")

        st.progress(progress_pct)
        st.caption(f"Progresso geral: {progress_pct}%")
        st.markdown(stepper_lines)

        col_p1, col_p2, col_p3 = st.columns(3)
        col_p1.metric("Planejado", run.get("planned_to_enrich") or 0)
        col_p2.metric("Restante", run.get("remaining_to_enrich") or 0)
        col_p3.metric("Estrategia", run.get("strategy") or "default")

        provider_status = run.get("provider_http_status")
        provider_message = run.get("provider_message")
        if provider_status or provider_message:
            st.caption(f"Provider feedback: HTTP {provider_status or '-'} - {provider_message or '-'}")

        enrich_step = _find_step(run_steps, "enriching")
        enrich_details = _parse_json(enrich_step.get("details_json")) if enrich_step else {}
        if enrich_details:
            st.markdown("#### Enriquecimento (live)")
            col_e1, col_e2, col_e3, col_e4, col_e5 = st.columns(5)
            col_e1.metric("Alvo", enrich_details.get("alvo_enriquecimento") or 0)
            col_e2.metric("Processados", enrich_details.get("processed_count") or 0)
            col_e3.metric("Erros", enrich_details.get("errors_count") or 0)
            col_e4.metric("Cache hits", enrich_details.get("cache_hits") or 0)
            col_e5.metric("Tempo medio (ms)", enrich_details.get("avg_fetch_ms") or 0)

        if run.get("status") == "paused_provider_limit":
            col_a, col_b = st.columns(2)
            with col_a:
                if st.button("Retomar", key=f"monitor_resume_{run_id}"):
                    resumed = jobs.resume_run(run_id)
                    if resumed:
                        st.success("Run retomado.")
                    else:
                        st.error("Nao foi possivel retomar este run.")
            with col_b:
                current_rps = int(_env("SERPER_MAX_RPS", "5"))
                if st.button(f"Reduzir RPS (atual {current_rps})", key=f"monitor_rps_{run_id}"):
                    new_rps = max(1, current_rps - 1)
                    _set_env("SERPER_MAX_RPS", str(new_rps))
                    storage.log_event(
                        "info",
                        "provider_rps_adjusted",
                        {"run_id": run_id, "previous_rps": current_rps, "new_rps": new_rps},
                    )
                    st.success(f"Novo max_rps: {new_rps}")

        logs = storage.fetch_logs(limit=20, run_id=run_id)
        if logs:
            rows = []
            for log in logs:
                message = _log_message(log.get("detail_json", "")) or log.get("detail_json", "")
                rows.append(
                    {
                        "created_at": log.get("created_at"),
                        "level": log.get("level"),
                        "event": log.get("event"),
                        "message": message,
                    }
                )
            st.markdown("#### Logs recentes")
            st.dataframe(pd.DataFrame(rows), width="stretch")
        else:
            st.info("Sem logs ainda para este run.")

    _render_live(selected_run_id)

with resultados_tab:
    st.subheader("Resultados")

    municipio = ""
    uf_filter = ""

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        min_score = st.number_input("Score minimo", min_value=0, max_value=100, value=0)
    with col2:
        contact_quality = st.selectbox("Contact quality", options=["", "ok", "suspicious", "accountant_like"])
    with col3:
        municipio = st.text_input("Municipio")
    with col4:
        uf_filter = st.text_input("UF")

    page_size = st.selectbox("Por pagina", options=[25, 50, 100, 200], index=1)
    page = st.number_input("Pagina", min_value=1, max_value=1000, value=1)
    offset = (page - 1) * page_size

    leads = storage.query_leads_clean(
        min_score=min_score if min_score > 0 else None,
        contact_quality=contact_quality or None,
        municipio=municipio or None,
        uf=uf_filter or None,
        limit=int(page_size),
        offset=int(offset),
    )

    if leads:
        enrichments = storage.fetch_enrichments_by_cnpjs([lead.get("cnpj") for lead in leads])
        rows = []
        for lead in leads:
            cnpj = lead.get("cnpj")
            enrichment = enrichments.get(cnpj, {})
            rows.append(
                {
                    "cnpj": cnpj,
                    "razao_social": lead.get("razao_social"),
                    "municipio": lead.get("municipio"),
                    "uf": lead.get("uf"),
                    "cnae": lead.get("cnae"),
                    "score_v2": lead.get("score_v2"),
                    "score_label": lead.get("score_label"),
                    "contact_quality": lead.get("contact_quality"),
                    "tech_score": enrichment.get("tech_score"),
                    "tech_confidence": enrichment.get("tech_confidence"),
                    "stack_resumo": _stack_summary(enrichment.get("tech_stack_json")),
                    "rendered_used": enrichment.get("rendered_used"),
                }
            )
        df = pd.DataFrame(rows)
        st.dataframe(
            df[[
                "cnpj",
                "razao_social",
                "municipio",
                "uf",
                "cnae",
                "score_v2",
                "score_label",
                "contact_quality",
                "tech_score",
                "tech_confidence",
                "stack_resumo",
                "rendered_used",
            ]],
            width="stretch",
        )

        signals_by_cnpj = {
            cnpj: _signals_summary(enrichment.get("signals_json"))
            for cnpj, enrichment in enrichments.items()
            if enrichment.get("signals_json")
        }
        if signals_by_cnpj:
            st.markdown("#### Por que detectou?")
            selected_cnpj = st.selectbox(
                "CNPJ para detalhes",
                options=list(signals_by_cnpj.keys()),
            )
            signals = signals_by_cnpj.get(selected_cnpj, {})
            with st.expander("Evidencias por tecnologia"):
                if not signals:
                    st.caption("Sem evidencias registradas.")
                else:
                    for tech, evidences in signals.items():
                        st.write(f"{tech}: {', '.join(evidences)}")
    else:
        message = "Nenhum lead encontrado com os filtros."
        run_id = st.session_state.get("current_run_id")
        if run_id:
            run_steps = storage.fetch_run_steps(run_id)
            extract_step = _find_step(run_steps, "extract")
            clean_step = _find_step(run_steps, "cleaning")
            extract_details = _parse_json(extract_step.get("details_json")) if extract_step else {}
            clean_details = _parse_json(clean_step.get("details_json")) if clean_step else {}
            total_encontrado = extract_details.get("total_encontrado")
            removed_mei = clean_details.get("removed_mei")
            removed_other = clean_details.get("removed_other")
            if total_encontrado:
                message = (
                    f"Encontramos {total_encontrado} empresas, mas nenhuma passou pelos filtros atuais. "
                    f"Removidos na limpeza: MEI={removed_mei or 0}, outros={removed_other or 0}."
                )
        st.info(message)

    st.markdown("---")
    st.subheader("Exports (locais)")

    export_limit = st.number_input("Quantidade para exportar", min_value=1, max_value=5000, value=500)
    export_hot = st.checkbox("Somente Hot", value=False)
    export_hot_whatsapp = st.checkbox("Hot + WhatsApp", value=False)
    export_no_accountant = st.checkbox("Sem contador-like", value=False)
    export_with_site = st.checkbox("Com site + tech detectado", value=False)

    if st.button("Gerar CSV"):
        export_leads = storage.query_leads_clean(
            min_score=85 if (export_hot or export_hot_whatsapp) else None,
            contact_quality=None,
            municipio=municipio or None,
            uf=uf_filter or None,
            limit=int(export_limit),
            offset=0,
        )
        if export_no_accountant:
            export_leads = [
                lead for lead in export_leads
                if not _parse_json(lead.get("flags_json")).get("accountant_like")
            ]
        if export_hot_whatsapp:
            export_leads = [
                lead for lead in export_leads
                if _parse_json(lead.get("flags_json")).get("whatsapp_probable")
            ]
        enrichments = storage.fetch_enrichments_by_cnpjs([lead.get("cnpj") for lead in export_leads])
        if export_with_site:
            export_leads = [
                lead for lead in export_leads
                if enrichments.get(lead.get("cnpj"), {}).get("site")
                and (enrichments.get(lead.get("cnpj"), {}).get("tech_score") or 0) > 0
            ]
        rows = _build_export_rows(export_leads, enrichments)
        if not rows:
            st.warning("Nenhum dado para exportar.")
        else:
            os.makedirs("exports", exist_ok=True)
            filename = f"exports/hunter_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            pd.DataFrame(rows).to_csv(filename, index=False)
            storage.record_export({"filters": "custom"}, len(rows), filename)
            with open(filename, "rb") as f:
                st.download_button("Baixar CSV", f, file_name=filename.split("/")[-1])

with exports_tab:
    st.subheader("Exports (Casa dos Dados)")

    col_ex1, col_ex2 = st.columns(2)
    with col_ex1:
        exports_page = st.number_input("Pagina (listagem)", min_value=1, max_value=1000, value=1, key="exports_page")
        if st.button("Atualizar fila"):
            try:
                data_sources.export_list_v4(page=int(exports_page))
                st.success("Fila atualizada.")
            except Exception as exc:
                st.error(f"Falha ao atualizar fila: {exc}")
    with col_ex2:
        st.caption("Use a lista para acompanhar status: aguardando_processamento / processando / processado.")

    snapshots = storage.fetch_recent_export_snapshots(limit=50)
    if snapshots:
        df_snapshots = pd.DataFrame(snapshots)
        st.dataframe(
            df_snapshots[[
                "arquivo_uuid",
                "status",
                "quantidade",
                "quantidade_solicitada",
                "created_at",
            ]],
            width="stretch",
        )
        export_ids = list({s.get("arquivo_uuid") for s in snapshots if s.get("arquivo_uuid")})
    else:
        export_ids = []
        st.info("Nenhuma solicitacao listada ainda.")

    with st.expander("Exports criados pelo app"):
        exports = storage.list_casa_exports(limit=100)
        if exports:
            df_exports = pd.DataFrame(exports)
            st.dataframe(
                df_exports[[
                    "arquivo_uuid",
                    "status",
                    "total_linhas",
                    "created_at",
                    "updated_at",
                ]],
                width="stretch",
            )
        else:
            st.info("Nenhum export registrado pelo app.")

    st.markdown("#### Consultar arquivo")
    col_ea, col_eb = st.columns(2)
    with col_ea:
        selected_uuid = st.selectbox("Arquivo (uuid)", options=[""] + export_ids)
    with col_eb:
        manual_uuid = st.text_input("Ou cole arquivo_uuid manual")

    arquivo_uuid = manual_uuid.strip() or selected_uuid
    if arquivo_uuid:
        col_ec, col_ed, col_ee = st.columns(3)
        with col_ec:
            if st.button("Buscar link"):
                try:
                    result = data_sources.export_poll_v4_public(arquivo_uuid)
                    st.success("Arquivo pronto.")
                    if result.get("link"):
                        st.caption("Link expira em ~1h. Baixe agora.")
                        st.code(result.get("link"))
                except Exception as exc:
                    st.error(f"Falha ao consultar: {exc}")
        with col_ed:
            if st.button("Baixar CSV"):
                try:
                    export_record = storage.fetch_casa_export(arquivo_uuid)
                    link = (export_record or {}).get("link")
                    if not link:
                        st.warning("Link nao encontrado. Clique em Buscar link primeiro.")
                    else:
                        result = data_sources.export_download(link, arquivo_uuid)
                        st.success(f"CSV salvo em {result.get('file_path')}")
                except Exception as exc:
                    st.error(f"Falha ao baixar CSV: {exc}")
        with col_ee:
            snapshots = storage.fetch_export_snapshots(arquivo_uuid)
            if snapshots:
                df_snap = pd.DataFrame(snapshots)
                st.dataframe(
                    df_snap[["created_at", "status", "quantidade", "quantidade_solicitada"]],
                    width="stretch",
                )
            else:
                st.info("Sem snapshots ainda para este arquivo.")

with recovery_tab:
    st.subheader("Recovery")

    export_files = storage.fetch_export_files(limit=100)
    files_by_uuid = {f.get("arquivo_uuid"): f for f in export_files or []}
    if export_files:
        df_files = pd.DataFrame(export_files)
        st.dataframe(
            df_files[["arquivo_uuid", "file_path", "file_size", "downloaded_at"]],
            width="stretch",
        )
        arquivo_uuid = st.selectbox(
            "Selecionar arquivo para importar",
            options=[""] + [f.get("arquivo_uuid") for f in export_files],
        )
    else:
        arquivo_uuid = ""
        st.info("Nenhum CSV baixado ainda. Use a aba Exports para baixar.")

    with st.expander("Risco e custo (estimativa)"):
        if arquivo_uuid and arquivo_uuid in files_by_uuid:
            if st.button("Calcular estimativa", key="rec_estimate_btn"):
                file_path = files_by_uuid[arquivo_uuid].get("file_path")
                try:
                    leads_preview = data_sources.parse_export_csv(file_path)
                    cnpjs = [lead.get("cnpj") for lead in leads_preview if lead.get("cnpj")]
                    enrichments = storage.fetch_enrichments_by_cnpjs(cnpjs)
                    cache_hits = _count_fresh_cache(enrichments, int(_env("CACHE_TTL_HOURS", "24")))
                    st.session_state["recovery_estimate"] = {
                        "arquivo_uuid": arquivo_uuid,
                        "total": len(cnpjs),
                        "cache_hits": cache_hits,
                    }
                except Exception as exc:
                    st.error(f"Falha ao calcular estimativa: {exc}")
        estimate = st.session_state.get("recovery_estimate", {})
        if estimate.get("arquivo_uuid") == arquivo_uuid:
            total = int(estimate.get("total") or 0)
            cache_hits = int(estimate.get("cache_hits") or 0)
            top_pct = int(st.session_state.get("rec_top_pct", 25))
            safe_limit = int(st.session_state.get("rec_safe_limit", 0))
            cache_only = bool(st.session_state.get("rec_cache_only", False))
            planned = int(round(total * top_pct / 100)) if total else 0
            if safe_limit:
                planned = min(planned, safe_limit)
            cache_hits_est = min(cache_hits, planned)
            est_requests = 0 if cache_only else max(planned - cache_hits_est, 0)
            max_rps = max(1, int(_env("SERPER_MAX_RPS", "5")))
            est_seconds = est_requests / max_rps if est_requests else 0
            col_r1, col_r2, col_r3 = st.columns(3)
            col_r1.metric("Leads no CSV", total)
            col_r2.metric("Cache reutilizavel", cache_hits)
            col_r3.metric("Sem cache (estim.)", max(total - cache_hits, 0))
            st.caption(
                f"Requisicoes externas (estim.): {est_requests} | Tempo: {_format_duration(est_seconds)}"
            )
        else:
            st.caption("Selecione um arquivo e clique para calcular a estimativa.")

    with st.expander("Parametros de reprocessamento"):
        rec_excluir_mei = st.checkbox("Excluir MEI", value=True, key="rec_excluir_mei")
        rec_enrich = st.checkbox("Enriquecer", value=True, key="rec_enrich")
        rec_top_pct = st.slider("Top % para enriquecer", min_value=5, max_value=100, value=25, step=5, key="rec_top_pct")
        rec_provider = st.selectbox("Provider", options=["serper"], index=0, key="rec_provider")
        rec_repeat = st.number_input("Telefone repetido (min N)", min_value=2, max_value=20, value=5, key="rec_repeat")
        rec_safe_limit = st.selectbox(
            "Modo seguro: limitar enriquecimento",
            options=[0, 50, 100, 200, 500],
            index=0,
            format_func=lambda v: "Sem limite" if v == 0 else f"Limitar a {v}",
            key="rec_safe_limit",
        )
        rec_cache_only = st.checkbox(
            "Somente cache (sem chamadas externas)",
            value=False,
            key="rec_cache_only",
        )

    if arquivo_uuid and st.button("Importar e reconstruir"):
        params = {
            "excluir_mei": rec_excluir_mei,
            "telefone_repeat_threshold": int(rec_repeat),
            "enrich_top_pct": int(rec_top_pct),
            "enable_enrichment": rec_enrich,
            "provider": rec_provider,
            "concurrency": int(_env("CONCURRENCY", "10")),
            "timeout": int(_env("TIMEOUT", "5")),
            "cache_ttl_hours": int(_env("CACHE_TTL_HOURS", "24")),
            "run_type": "recovery",
            "safe_enrich_limit": int(rec_safe_limit) if rec_safe_limit else None,
            "cache_only": bool(rec_cache_only),
        }
        try:
            run_id = jobs.start_recovery(arquivo_uuid, params)
            st.success(f"Recovery iniciado: {run_id}")
            st.session_state.current_run_id = run_id
        except Exception as exc:
            st.error(f"Falha ao iniciar recovery: {exc}")

    st.markdown("---")
    st.subheader("Upload Excel (CNPJ)")
    st.caption("Suba um .xlsx com coluna de CNPJ para enriquecer sem nova chamada na Casa dos Dados.")

    uploaded_file = st.file_uploader(
        "Selecionar arquivo Excel (.xlsx)",
        type=["xlsx"],
        key="excel_upload",
    )
    cnpj_column = ""
    if uploaded_file:
        try:
            preview_df = pd.read_excel(uploaded_file, nrows=25, engine="openpyxl")
            uploaded_file.seek(0)
            if preview_df.empty:
                st.warning("Arquivo vazio ou sem linhas.")
            else:
                st.dataframe(preview_df, width="stretch")
            columns = list(preview_df.columns)
            if columns:
                def _default_cnpj_index(cols: List[Any]) -> int:
                    for idx, col in enumerate(cols):
                        if "cnpj" in str(col).lower():
                            return idx
                    return 0

                cnpj_column = st.selectbox(
                    "Coluna com CNPJ",
                    options=columns,
                    index=_default_cnpj_index(columns),
                    key="excel_cnpj_column",
                )
            else:
                st.error("Nenhuma coluna detectada no arquivo.")
        except Exception as exc:
            st.error(f"Falha ao ler o Excel: {exc}")

    with st.expander("Risco e custo (estimativa)"):
        if uploaded_file and cnpj_column:
            if st.button("Calcular estimativa", key="excel_estimate_btn"):
                try:
                    df_full = pd.read_excel(uploaded_file, dtype=str, engine="openpyxl")
                    uploaded_file.seek(0)
                    if cnpj_column not in df_full.columns:
                        raise RuntimeError("Coluna de CNPJ nao encontrada para estimativa.")
                    raw_values = df_full[cnpj_column].fillna("").astype(str)
                    cnpjs = [re.sub(r"\D", "", value) for value in raw_values]
                    cnpjs = [cnpj for cnpj in cnpjs if cnpj]
                    enrichments = storage.fetch_enrichments_by_cnpjs(cnpjs)
                    cache_hits = _count_fresh_cache(enrichments, int(_env("CACHE_TTL_HOURS", "24")))
                    st.session_state["excel_estimate"] = {
                        "file_name": uploaded_file.name,
                        "column": cnpj_column,
                        "total": len(cnpjs),
                        "cache_hits": cache_hits,
                    }
                except Exception as exc:
                    st.error(f"Falha ao calcular estimativa: {exc}")
        estimate = st.session_state.get("excel_estimate", {})
        if uploaded_file and estimate.get("file_name") == uploaded_file.name and estimate.get("column") == cnpj_column:
            total = int(estimate.get("total") or 0)
            cache_hits = int(estimate.get("cache_hits") or 0)
            top_pct = int(st.session_state.get("excel_top_pct", 25))
            safe_limit = int(st.session_state.get("excel_safe_limit", 0))
            cache_only = bool(st.session_state.get("excel_cache_only", False))
            planned = int(round(total * top_pct / 100)) if total else 0
            if safe_limit:
                planned = min(planned, safe_limit)
            cache_hits_est = min(cache_hits, planned)
            est_requests = 0 if cache_only else max(planned - cache_hits_est, 0)
            max_rps = max(1, int(_env("SERPER_MAX_RPS", "5")))
            est_seconds = est_requests / max_rps if est_requests else 0
            col_r1, col_r2, col_r3 = st.columns(3)
            col_r1.metric("Leads no Excel", total)
            col_r2.metric("Cache reutilizavel", cache_hits)
            col_r3.metric("Sem cache (estim.)", max(total - cache_hits, 0))
            st.caption(
                f"Requisicoes externas (estim.): {est_requests} | Tempo: {_format_duration(est_seconds)}"
            )
        else:
            st.caption("Selecione um arquivo e coluna para calcular a estimativa.")

    with st.expander("Parametros de importacao (Excel)"):
        excel_excluir_mei = st.checkbox("Excluir MEI", value=True, key="excel_excluir_mei")
        excel_enrich = st.checkbox("Enriquecer", value=True, key="excel_enrich")
        excel_top_pct = st.slider(
            "Top % para enriquecer",
            min_value=5,
            max_value=100,
            value=25,
            step=5,
            key="excel_top_pct",
        )
        excel_provider = st.selectbox(
            "Provider",
            options=["serper"],
            index=0,
            key="excel_provider",
        )
        excel_repeat = st.number_input(
            "Telefone repetido (min N)",
            min_value=2,
            max_value=20,
            value=5,
            key="excel_repeat",
        )
        excel_safe_limit = st.selectbox(
            "Modo seguro: limitar enriquecimento",
            options=[0, 50, 100, 200, 500],
            index=0,
            format_func=lambda v: "Sem limite" if v == 0 else f"Limitar a {v}",
            key="excel_safe_limit",
        )
        excel_cache_only = st.checkbox(
            "Somente cache (sem chamadas externas)",
            value=False,
            key="excel_cache_only",
        )

    if uploaded_file and cnpj_column and st.button("Importar e enriquecer (Excel)"):
        upload_dir = Path("uploads")
        upload_dir.mkdir(parents=True, exist_ok=True)
        suffix = Path(uploaded_file.name).suffix or ".xlsx"
        file_name = f"excel_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex}{suffix}"
        file_path = upload_dir / file_name
        with open(file_path, "wb") as handle:
            handle.write(uploaded_file.getbuffer())

        params = {
            "excluir_mei": excel_excluir_mei,
            "telefone_repeat_threshold": int(excel_repeat),
            "enrich_top_pct": int(excel_top_pct),
            "enable_enrichment": excel_enrich,
            "provider": excel_provider,
            "concurrency": int(_env("CONCURRENCY", "10")),
            "timeout": int(_env("TIMEOUT", "5")),
            "cache_ttl_hours": int(_env("CACHE_TTL_HOURS", "24")),
            "run_type": "upload_excel",
            "cnpj_column": cnpj_column,
            "upload_file_name": uploaded_file.name,
            "safe_enrich_limit": int(excel_safe_limit) if excel_safe_limit else None,
            "cache_only": bool(excel_cache_only),
        }
        try:
            run_id = jobs.start_excel_import(str(file_path), params)
            st.success(f"Importacao Excel iniciada: {run_id}")
            st.session_state.current_run_id = run_id
        except Exception as exc:
            st.error(f"Falha ao iniciar importacao: {exc}")

    st.markdown("---")
    st.subheader("Acompanhamento do recovery")

    runs = storage.list_runs(limit=50)
    recovery_runs = []
    for run in runs or []:
        params = _parse_json(run.get("params_json"))
        run_type = str(params.get("run_type") or "standard")
        if run_type in {"recovery", "upload_excel"}:
            recovery_runs.append(run)

    run_map = {r["run_id"]: r for r in recovery_runs}
    run_options = [""] + list(run_map.keys())
    default_run_id = st.session_state.get("current_run_id")
    if default_run_id and default_run_id not in run_map:
        run_options.append(default_run_id)

    def _run_label(run_id: str) -> str:
        if not run_id:
            return "Selecione um run"
        run = run_map.get(run_id) or storage.get_run(run_id) or {}
        status = run.get("status", "unknown")
        return f"{run_id[:8]}... ({status})"

    selected_run_id = st.selectbox(
        "Run para acompanhar",
        options=run_options,
        index=run_options.index(default_run_id) if default_run_id in run_options else 0,
        format_func=_run_label,
    )

    @st.fragment(run_every="2s")
    def _render_recovery_live(run_id: str) -> None:
        if not run_id:
            st.info("Selecione um run para acompanhar.")
            return
        run = storage.get_run(run_id)
        if not run:
            st.warning("Run nao encontrado.")
            return
        params = _parse_json(run.get("params_json"))
        run_type = str(params.get("run_type") or "standard")
        enable_enrichment = bool(params.get("enable_enrichment", True))
        run_steps = storage.fetch_run_steps(run_id)
        stepper_lines, progress_pct = _format_stepper(
            run_steps,
            run.get("status"),
            enable_enrichment,
            run_type,
        )
        st.markdown("#### Status")
        col_s1, col_s2, col_s3, col_s4 = st.columns(4)
        col_s1.metric("Status", run.get("status"))
        col_s2.metric("Total leads", run.get("total_leads"))
        col_s3.metric("Enriquecidos", run.get("enriched_count"))
        col_s4.metric("Erros", run.get("errors_count"))
        if run.get("status") == "completed":
            st.success("Run concluido.")
        elif run.get("status") == "completed_with_warnings":
            st.warning("Run concluido com avisos.")
        elif run.get("status") == "paused_provider_limit":
            max_rps = int(_env("SERPER_MAX_RPS", "5"))
            st.warning(
                f"Serper limitou a {max_rps} req/s. O Hunter OS pausou para evitar custo e bloqueio."
            )
        elif run.get("status") == "failed":
            st.error("Run falhou. Veja erros abaixo.")
        warning_reason = _warning_reason_text(run.get("warning_reason"))
        if warning_reason:
            st.caption(f"Aviso: {warning_reason}")
        provider_status = run.get("provider_http_status")
        provider_message = run.get("provider_message")
        if provider_status or provider_message:
            st.caption(f"Provider feedback: HTTP {provider_status or '-'} - {provider_message or '-'}")

        st.progress(progress_pct)
        st.caption(f"Progresso geral: {progress_pct}%")
        st.markdown(stepper_lines)

        enrich_step = _find_step(run_steps, "enriching")
        enrich_details = _parse_json(enrich_step.get("details_json")) if enrich_step else {}
        processed_count = enrich_details.get("processed_count")
        errors_count = enrich_details.get("errors_count")
        cache_hits = enrich_details.get("cache_hits")
        avg_fetch_ms = enrich_details.get("avg_fetch_ms")
        alvo = enrich_details.get("alvo_enriquecimento")
        if any(value is not None for value in [processed_count, errors_count, cache_hits, avg_fetch_ms, alvo]):
            st.markdown("#### Enriquecimento (detalhes)")
            col_e1, col_e2, col_e3, col_e4, col_e5 = st.columns(5)
            col_e1.metric("Alvo", alvo or 0)
            col_e2.metric("Processados", processed_count or 0)
            col_e3.metric("Erros", errors_count or 0)
            col_e4.metric("Cache hits", cache_hits or 0)
            col_e5.metric("Tempo medio (ms)", avg_fetch_ms or 0)

        logs = storage.fetch_logs(limit=30, run_id=run_id)
        if logs:
            rows = []
            for log in logs:
                message = _log_message(log.get("detail_json", "")) or log.get("detail_json", "")
                rows.append(
                    {
                        "created_at": log.get("created_at"),
                        "level": log.get("level"),
                        "event": log.get("event"),
                        "message": message,
                    }
                )
            st.markdown("#### Logs recentes")
            st.dataframe(pd.DataFrame(rows), width="stretch")
        else:
            st.info("Sem logs ainda para este run.")

        errors = storage.fetch_errors(run_id, limit=20)
        if errors:
            st.markdown("#### Erros recentes")
            df_errors = pd.DataFrame(errors)
            st.dataframe(
                df_errors[["created_at", "step_name", "error"]],
                width="stretch",
            )

    _render_recovery_live(selected_run_id)

with vault_tab:
    st.subheader("Enrichment Vault")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        min_score_vault = st.number_input("Score minimo (Vault)", min_value=0, max_value=100, value=0)
    with col2:
        min_tech_score = st.number_input("Tech score minimo", min_value=0, max_value=30, value=0)
    with col3:
        contact_quality_vault = st.selectbox("Contact quality (Vault)", options=["", "ok", "suspicious", "accountant_like"])
    with col4:
        municipio_vault = st.text_input("Municipio (Vault)")

    has_marketing_filter = st.selectbox("Has marketing", options=["", "sim", "nao"])

    page_size_vault = st.selectbox("Por pagina (Vault)", options=[25, 50, 100], index=1)
    page_vault = st.number_input("Pagina (Vault)", min_value=1, max_value=1000, value=1)
    offset_vault = (page_vault - 1) * page_size_vault

    vault_rows = storage.query_enrichment_vault(
        min_score=min_score_vault if min_score_vault > 0 else None,
        min_tech_score=min_tech_score if min_tech_score > 0 else None,
        contact_quality=contact_quality_vault or None,
        municipio=municipio_vault or None,
        has_marketing=True if has_marketing_filter == "sim" else False if has_marketing_filter == "nao" else None,
        limit=int(page_size_vault),
        offset=int(offset_vault),
    )

    if vault_rows:
        df_vault = pd.DataFrame(vault_rows)
        df_vault["stack_resumo"] = df_vault["tech_stack_json"].apply(_stack_summary)
        st.dataframe(
            df_vault[[
                "cnpj",
                "razao_social",
                "municipio",
                "uf",
                "site",
                "instagram",
                "linkedin_company",
                "tech_score",
                "tech_confidence",
                "stack_resumo",
                "has_marketing",
                "has_analytics",
                "has_ecommerce",
                "has_chat",
                "rendered_used",
                "score_v2",
                "score_label",
                "contact_quality",
            ]],
            width="stretch",
        )
        csv_data = df_vault.to_csv(index=False)
        st.download_button(
            "Exportar CSV do vault",
            data=csv_data,
            file_name="enrichment_vault.csv",
            mime="text/csv",
        )
    else:
        st.info("Nenhum enrichment encontrado.")

with runs_tab:
    st.subheader("Runs/Jobs")
    runs = storage.list_runs(limit=50)
    if runs:
        df_runs = pd.DataFrame(runs)
        st.dataframe(df_runs, width="stretch")
        run_ids = [r["run_id"] for r in runs]
        selected = st.selectbox("Reprocessar run", options=[""] + run_ids)
        if selected and st.button("Reprocessar"):
            run = storage.get_run(selected)
            params = json.loads(run.get("params_json") or "{}")
            new_run = jobs.start_run(params)
            st.success(f"Novo run iniciado: {new_run}")
        st.markdown("#### Logs recentes")
        logs = storage.fetch_logs(limit=30)
        if logs:
            df_logs = pd.DataFrame(logs)
            st.dataframe(
                df_logs[["created_at", "level", "event", "detail_json"]],
                width="stretch",
            )
        else:
            st.info("Nenhum log ainda.")
    else:
        st.info("Nenhum run encontrado.")

with diagnostics_tab:
    st.subheader("Diagnostico")
    st.markdown("#### Checagem de leads_raw por periodo")
    col_d1, col_d2 = st.columns(2)
    with col_d1:
        start_ts = st.text_input("Inicio (YYYY-MM-DD HH:MM:SS)", value="")
    with col_d2:
        end_ts = st.text_input("Fim (YYYY-MM-DD HH:MM:SS)", value="")
    if start_ts and end_ts:
        try:
            count = storage.count_leads_raw_between(start_ts, end_ts)
            st.metric("Total leads_raw no periodo", count)
            sources = storage.list_leads_raw_sources_between(start_ts, end_ts)
            if sources:
                df_sources = pd.DataFrame(sources)
                st.dataframe(df_sources, width="stretch")
        except Exception as exc:
            st.error(f"Falha na checagem: {exc}")

    runs = storage.list_runs(limit=50)
    run_ids = [r["run_id"] for r in runs] if runs else []
    selected_run = st.selectbox("Run para diagnostico", options=[""] + run_ids)

    if selected_run:
        st.markdown("#### Etapas")
        steps = storage.fetch_run_steps(selected_run)
        if steps:
            df_steps = pd.DataFrame(steps)
            st.dataframe(
                df_steps[["step_name", "status", "started_at", "ended_at", "duration_ms", "details_json"]],
                width="stretch",
            )
        else:
            st.info("Sem etapas registradas.")

        st.markdown("#### Chamadas API")
        calls = storage.fetch_api_calls(selected_run, limit=50)
        if calls:
            df_calls = pd.DataFrame(calls)
            st.dataframe(
                df_calls[["created_at", "method", "url", "status_code", "duration_ms", "request_id"]],
                width="stretch",
            )
        else:
            st.info("Sem chamadas registradas.")

        st.markdown("#### Erros")
        errors = storage.fetch_errors(selected_run, limit=50)
        if errors:
            df_errors = pd.DataFrame(errors)
            st.dataframe(
                df_errors[["created_at", "step_name", "error", "lead_id"]],
                width="stretch",
            )
        else:
            st.info("Sem erros registrados.")
