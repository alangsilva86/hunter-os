"""Hunter OS - B2B Prospecting (refactor v2)."""

import json
import os
from datetime import datetime
from typing import Any, Dict, List

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


buscar_tab, resultados_tab, vault_tab, runs_tab, config_tab = st.tabs(
    ["Buscar", "Resultados", "Enrichment Vault", "Runs/Jobs", "Config"]
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
        serp_key = st.text_input(
            "Serp.dev API Key",
            value=_env("SERPDEV_API_KEY"),
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
            options=["serpdev", "serpapi", "bing"],
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
        _set_env("SERPDEV_API_KEY", serp_key)
        _set_env("BUILTWITH_API_KEY", builtwith_key)
        _set_env("SEARCH_PROVIDER", provider)
        _set_env("CACHE_TTL_HOURS", str(int(cache_ttl_hours)))
        _set_env("CONCURRENCY", str(int(concurrency)))
        _set_env("TIMEOUT", str(int(timeout)))
        st.success("Configuracao aplicada para esta sessao.")

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
        provider = st.selectbox("Provider", options=["serpdev", "serpapi", "bing"], index=0)

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
            "provider": provider,
            "concurrency": int(_env("CONCURRENCY", "10")),
            "timeout": int(_env("TIMEOUT", "5")),
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
            running = jobs.is_running(run.get("run_id"))
            st.caption(f"Worker: {'rodando' if running else 'parado'}")

            col_actions = st.columns(2)
            with col_actions[0]:
                if run.get("status") in {"extracting", "cleaning", "scoring_v1", "enriching", "scoring_v2"}:
                    if st.button("Cancelar run"):
                        jobs.cancel_run(run.get("run_id"))
                        st.warning("Cancelamento solicitado")
            with col_actions[1]:
                if run.get("status") in {"queued", "failed", "canceled"} and not running:
                    if st.button("Retomar run"):
                        jobs.resume_run(run.get("run_id"))
                        st.success("Run retomado")

            st.markdown("#### Ultimos logs")
            logs = storage.fetch_logs(limit=20, run_id=run.get("run_id"))
            if logs:
                df_logs = pd.DataFrame(logs)
                st.dataframe(
                    df_logs[["created_at", "level", "event", "detail_json"]],
                    use_container_width=True,
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
            use_container_width=True,
        )
    else:
        st.info("Nenhum lead encontrado com os filtros.")

    st.markdown("---")
    st.subheader("Exports")

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
            use_container_width=True,
        )
    else:
        st.info("Nenhum enrichment encontrado.")

with runs_tab:
    st.subheader("Runs/Jobs")
    runs = storage.list_runs(limit=50)
    if runs:
        df_runs = pd.DataFrame(runs)
        st.dataframe(df_runs, use_container_width=True)
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
                use_container_width=True,
            )
        else:
            st.info("Nenhum log ainda.")
    else:
        st.info("Nenhum run encontrado.")
