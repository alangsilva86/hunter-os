"""Hunter OS - B2B Prospecting (refactor v2)."""

import json
import os
import hashlib
from pathlib import Path
from datetime import datetime
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


buscar_tab, resultados_tab, exports_tab, recovery_tab, vault_tab, runs_tab, diagnostics_tab, config_tab = st.tabs(
    ["Buscar", "Resultados", "Exports (Casa dos Dados)", "Recovery", "Enrichment Vault", "Runs/Jobs", "Diagnostico", "Config"]
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
        builtwith_key = st.text_input(
            "BuiltWith API Key",
            value=_env("BUILTWITH_API_KEY"),
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

    if st.button("Salvar configuracao"):
        _set_env("CASA_DOS_DADOS_API_KEY", casa_key)
        _set_env("SERPER_API_KEY", serper_key)
        _set_env("BUILTWITH_API_KEY", builtwith_key)
        _set_env("SEARCH_PROVIDER", provider)
        _set_env("CACHE_TTL_HOURS", str(int(cache_ttl_hours)))
        _set_env("CONCURRENCY", str(int(concurrency)))
        _set_env("TIMEOUT", str(int(timeout)))
        st.success("Configuracao aplicada para esta sessao.")

    st.markdown("#### Arquivos carregados")
    mod_data_sources = _module_fingerprint(data_sources)
    mod_jobs = _module_fingerprint(jobs)
    mod_storage = _module_fingerprint(storage)
    st.caption(f"data_sources: {mod_data_sources['path']} ({mod_data_sources['hash']})")
    st.caption(f"jobs: {mod_jobs['path']} ({mod_jobs['hash']})")
    st.caption(f"storage: {mod_storage['path']} ({mod_storage['hash']})")

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
        }
        run_id = jobs.start_run(params)
        st.session_state.current_run_id = run_id
        st.success(f"Run iniciado: {run_id}")

    if st.session_state.current_run_id:
        run = storage.get_run(st.session_state.current_run_id)
        if run:
            st.markdown("---")
            st.subheader("Status do run")
            col_a, col_b, col_c, col_d = st.columns(4)
            col_a.metric("Status", run.get("status"))
            col_b.metric("Total leads", run.get("total_leads"))
            col_c.metric("Enriquecidos", run.get("enriched_count"))
            col_d.metric("Erros", run.get("errors_count"))
            if run.get("status") == "completed" and (run.get("errors_count") or 0) > 0:
                st.warning("Concluido com avisos. Veja Diagnostico para detalhes.")
            running = jobs.is_running(run.get("run_id"))
            st.caption(f"Worker: {'rodando' if running else 'parado'}")

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
                    target = int(round(total_leads * top_pct / 100)) if total_leads else 0
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
                    target = int(round(total_leads * top_pct / 100)) if total_leads else 0
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
                }:
                    if st.button("Cancelar run"):
                        jobs.cancel_run(run.get("run_id"))
                        st.warning("Cancelamento solicitado")
            with col_actions[1]:
                if run.get("status") in {"queued", "failed", "canceled"} and not running:
                    if st.button("Retomar run"):
                        jobs.resume_run(run.get("run_id"))
                        st.success("Run retomado")

            st.markdown("#### Ultimos logs")
            if logs:
                df_logs = pd.DataFrame(logs)
                st.dataframe(
                    df_logs[["created_at", "level", "event", "detail_json"]],
                    width="stretch",
                )
            else:
                st.info("Sem logs para este run ainda.")

with resultados_tab:
    st.subheader("Resultados")

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
        df = pd.DataFrame(leads)
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
            ]],
            width="stretch",
        )
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

    with st.expander("Parametros de reprocessamento"):
        rec_excluir_mei = st.checkbox("Excluir MEI", value=True, key="rec_excluir_mei")
        rec_enrich = st.checkbox("Enriquecer", value=True, key="rec_enrich")
        rec_top_pct = st.slider("Top % para enriquecer", min_value=5, max_value=100, value=25, step=5, key="rec_top_pct")
        rec_provider = st.selectbox("Provider", options=["serper"], index=0, key="rec_provider")
        rec_repeat = st.number_input("Telefone repetido (min N)", min_value=2, max_value=20, value=5, key="rec_repeat")

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

    if uploaded_file and cnpj_column and st.button("Importar e enriquecer (Excel)"):
        upload_dir = Path("uploads")
        upload_dir.mkdir(parents=True, exist_ok=True)
        suffix = Path(uploaded_file.name).suffix or ".xlsx"
        file_name = f"excel_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex}{suffix}"
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
        }
        try:
            run_id = jobs.start_excel_import(str(file_path), params)
            st.success(f"Importacao Excel iniciada: {run_id}")
            st.session_state.current_run_id = run_id
        except Exception as exc:
            st.error(f"Falha ao iniciar importacao: {exc}")

with vault_tab:
    st.subheader("Enrichment Vault")

    col1, col2, col3 = st.columns(3)
    with col1:
        min_score_vault = st.number_input("Score minimo (Vault)", min_value=0, max_value=100, value=0)
    with col2:
        min_tech_score = st.number_input("Tech score minimo", min_value=0, max_value=30, value=0)
    with col3:
        contact_quality_vault = st.selectbox("Contact quality (Vault)", options=["", "ok", "suspicious", "accountant_like"])

    page_size_vault = st.selectbox("Por pagina (Vault)", options=[25, 50, 100], index=1)
    page_vault = st.number_input("Pagina (Vault)", min_value=1, max_value=1000, value=1)
    offset_vault = (page_vault - 1) * page_size_vault

    vault_rows = storage.query_enrichment_vault(
        min_score=min_score_vault if min_score_vault > 0 else None,
        min_tech_score=min_tech_score if min_tech_score > 0 else None,
        contact_quality=contact_quality_vault or None,
        limit=int(page_size_vault),
        offset=int(offset_vault),
    )

    if vault_rows:
        df_vault = pd.DataFrame(vault_rows)
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
                "score_v2",
                "score_label",
                "contact_quality",
            ]],
            width="stretch",
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
