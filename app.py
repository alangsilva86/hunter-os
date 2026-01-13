"""
Hunter OS - B2B Prospecting
Aplicação principal Streamlit para prospecção de leads B2B
Versão 2.0 - UX/UI Aprimorada
"""

import streamlit as st
import pandas as pd
from datetime import datetime
import time
import os

# Importa módulos locais
from utils import (
    CacheManager, DataExtractor, DataTransformer, DataEnricher, ICPScorer,
    CIDADES_ALVO, CNAES_PRIORITARIOS, gerar_dados_exemplo,
    exportar_csv_crm, gerar_relatorio_inteligencia
)

# ============================================================================
# CONFIGURAÇÃO DA PÁGINA
# ============================================================================

st.set_page_config(
    page_title="Hunter OS - B2B Prospecting",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS Customizado Aprimorado
st.markdown("""
<style>
    /* Reset e Base */
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1E3A5F;
        text-align: center;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
        text-align: center;
        margin-bottom: 2rem;
    }
    
    /* Cards de Status */
    .status-card {
        background: linear-gradient(135deg, #1E3A5F 0%, #2E5A8F 100%);
        padding: 1.5rem;
        border-radius: 15px;
        color: white;
        text-align: center;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
        margin-bottom: 1rem;
    }
    
    .status-card h3 {
        margin: 0;
        font-size: 0.9rem;
        opacity: 0.9;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    
    .status-card .value {
        font-size: 2.5rem;
        font-weight: bold;
        margin: 0.5rem 0;
    }
    
    .status-card .detail {
        font-size: 0.85rem;
        opacity: 0.8;
    }
    
    /* Progress Container */
    .progress-container {
        background: #f8f9fa;
        border-radius: 15px;
        padding: 2rem;
        margin: 1rem 0;
        border: 2px solid #e9ecef;
    }
    
    .progress-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 1rem;
    }
    
    .progress-title {
        font-size: 1.2rem;
        font-weight: 600;
        color: #1E3A5F;
    }
    
    .progress-percentage {
        font-size: 1.5rem;
        font-weight: bold;
        color: #28a745;
    }
    
    /* Source Badge */
    .source-badge {
        display: inline-block;
        background: #e7f3ff;
        color: #0066cc;
        padding: 0.3rem 0.8rem;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 500;
        margin: 0.2rem;
    }
    
    /* Metric Cards */
    .metric-grid {
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        gap: 1rem;
        margin: 1rem 0;
    }
    
    .metric-card {
        background: white;
        border-radius: 12px;
        padding: 1.2rem;
        box-shadow: 0 2px 10px rgba(0,0,0,0.05);
        border-left: 4px solid #667eea;
    }
    
    .metric-card.hot {
        border-left-color: #ff4b4b;
    }
    
    .metric-card.qualified {
        border-left-color: #ffa500;
    }
    
    .metric-card.contact {
        border-left-color: #28a745;
    }
    
    .metric-label {
        font-size: 0.85rem;
        color: #666;
        margin-bottom: 0.3rem;
    }
    
    .metric-value {
        font-size: 1.8rem;
        font-weight: bold;
        color: #1E3A5F;
    }
    
    .metric-delta {
        font-size: 0.8rem;
        color: #28a745;
    }
    
    /* Pipeline Funnel */
    .funnel-container {
        background: white;
        border-radius: 15px;
        padding: 1.5rem;
        box-shadow: 0 2px 10px rgba(0,0,0,0.05);
        margin: 1rem 0;
    }
    
    .funnel-step {
        display: flex;
        align-items: center;
        padding: 0.8rem 1rem;
        margin: 0.5rem 0;
        border-radius: 8px;
        transition: all 0.3s ease;
    }
    
    .funnel-step.active {
        background: #e7f3ff;
    }
    
    .funnel-step .icon {
        font-size: 1.5rem;
        margin-right: 1rem;
    }
    
    .funnel-step .info {
        flex: 1;
    }
    
    .funnel-step .count {
        font-size: 1.2rem;
        font-weight: bold;
        color: #1E3A5F;
    }
    
    /* Lead Status Tags */
    .hot-lead {
        background: linear-gradient(135deg, #ff4b4b 0%, #ff6b6b 100%);
        color: white;
        padding: 0.3rem 0.8rem;
        border-radius: 20px;
        font-weight: bold;
        font-size: 0.8rem;
    }
    
    .qualified-lead {
        background: linear-gradient(135deg, #ffa500 0%, #ffb833 100%);
        color: white;
        padding: 0.3rem 0.8rem;
        border-radius: 20px;
        font-weight: 500;
        font-size: 0.8rem;
    }
    
    .cold-lead {
        background: #e9ecef;
        color: #666;
        padding: 0.3rem 0.8rem;
        border-radius: 20px;
        font-size: 0.8rem;
    }
    
    /* Extraction Controls */
    .extraction-controls {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 15px;
        padding: 1.5rem;
        color: white;
        margin: 1rem 0;
    }
    
    .extraction-controls h4 {
        margin-top: 0;
        font-size: 1.1rem;
    }
    
    /* Animation */
    @keyframes pulse {
        0% { opacity: 1; }
        50% { opacity: 0.5; }
        100% { opacity: 1; }
    }
    
    .searching {
        animation: pulse 1.5s infinite;
    }
    
    /* Info Box */
    .info-box {
        background: #f0f7ff;
        border: 1px solid #cce5ff;
        border-radius: 10px;
        padding: 1rem;
        margin: 1rem 0;
    }
    
    .info-box.warning {
        background: #fff3cd;
        border-color: #ffc107;
    }
    
    .info-box.success {
        background: #d4edda;
        border-color: #28a745;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# INICIALIZAÇÃO
# ============================================================================

@st.cache_resource
def init_cache():
    """Inicializa o gerenciador de cache"""
    return CacheManager()

@st.cache_resource
def init_components(_cache):
    """Inicializa componentes do ETL"""
    extractor = DataExtractor(_cache)
    transformer = DataTransformer()
    enricher = DataEnricher(_cache)
    return extractor, transformer, enricher

# ============================================================================
# FUNÇÕES AUXILIARES
# ============================================================================

def carregar_dados_iniciais():
    """Carrega dados iniciais (exemplo ou cache)"""
    cache = init_cache()
    
    # Tenta carregar do cache
    empresas_cache = cache.get_all_empresas()
    
    if empresas_cache:
        return empresas_cache
    
    # Se não houver cache, usa dados de exemplo
    return gerar_dados_exemplo()

def processar_pipeline_etl(dados_brutos, status_container, progress_bar, status_text, detail_text):
    """Executa o pipeline ETL completo com feedback detalhado"""
    cache = init_cache()
    _, transformer, enricher = init_components(cache)
    
    leads_processados = []
    total = len(dados_brutos)
    
    # Estatísticas de processamento
    stats = {
        'total_encontrados': total,
        'processados': 0,
        'transformados': 0,
        'erros': 0,
        'fonte': 'BrasilAPI / CNPJ.ws'
    }
    
    for i, empresa in enumerate(dados_brutos):
        try:
            # Atualiza status
            empresa_nome = empresa.get('nome_fantasia', empresa.get('razao_social', f'Empresa {i+1}'))
            status_text.markdown(f"**🔍 Processando:** {empresa_nome[:50]}...")
            
            # Calcula progresso
            progresso = (i + 1) / total
            progress_bar.progress(progresso)
            
            # Detalhes do progresso
            detail_text.markdown(f"""
            <div style="display: flex; justify-content: space-between; font-size: 0.9rem; color: #666;">
                <span>📊 {i+1} de {total} empresas</span>
                <span>✅ {stats['transformados']} válidas</span>
                <span>❌ {stats['erros']} erros</span>
            </div>
            """, unsafe_allow_html=True)
            
            # Transform
            empresa_transformada = transformer.transformar_empresa(empresa)
            
            if empresa_transformada:
                # Calcula score ICP
                empresa_transformada['icp_score'] = ICPScorer.calcular_score(empresa_transformada)
                empresa_transformada['classificacao'] = ICPScorer.classificar_lead(empresa_transformada['icp_score'])
                
                leads_processados.append(empresa_transformada)
                stats['transformados'] += 1
            
            stats['processados'] += 1
            
        except Exception as e:
            stats['erros'] += 1
            continue
        
        # Pequena pausa para visualização
        time.sleep(0.05)
    
    return leads_processados, stats

def filtrar_dataframe(df, cidades, cnaes, porte_min_score):
    """Aplica filtros ao DataFrame"""
    df_filtrado = df.copy()
    
    # Filtro de cidade
    if cidades and len(cidades) > 0:
        df_filtrado = df_filtrado[df_filtrado['municipio'].isin(cidades)]
    
    # Filtro de CNAE
    if cnaes and len(cnaes) > 0:
        # Expande CNAEs selecionados
        cnaes_expandidos = []
        for categoria in cnaes:
            if categoria in CNAES_PRIORITARIOS:
                cnaes_expandidos.extend(CNAES_PRIORITARIOS[categoria])
        
        if cnaes_expandidos:
            df_filtrado = df_filtrado[df_filtrado['cnae_principal'].str[:4].isin(cnaes_expandidos)]
    
    # Filtro de score mínimo
    df_filtrado = df_filtrado[df_filtrado['icp_score'] >= porte_min_score]
    
    return df_filtrado

def render_funnel(total_base, total_filtrado, hot_leads, qualified_leads):
    """Renderiza o funil de conversão"""
    st.markdown("""
    <div class="funnel-container">
        <h4 style="margin-top:0; color:#1E3A5F;">📊 Funil de Prospecção</h4>
    """, unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f"""
        <div style="text-align:center; padding:1rem; background:#f8f9fa; border-radius:10px;">
            <div style="font-size:2rem;">🏢</div>
            <div style="font-size:1.5rem; font-weight:bold; color:#1E3A5F;">{total_base}</div>
            <div style="font-size:0.8rem; color:#666;">Base Total</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        pct_filtrado = (total_filtrado / total_base * 100) if total_base > 0 else 0
        st.markdown(f"""
        <div style="text-align:center; padding:1rem; background:#e7f3ff; border-radius:10px;">
            <div style="font-size:2rem;">🎯</div>
            <div style="font-size:1.5rem; font-weight:bold; color:#0066cc;">{total_filtrado}</div>
            <div style="font-size:0.8rem; color:#666;">Filtrados ({pct_filtrado:.1f}%)</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        pct_qualified = (qualified_leads / total_filtrado * 100) if total_filtrado > 0 else 0
        st.markdown(f"""
        <div style="text-align:center; padding:1rem; background:#fff3cd; border-radius:10px;">
            <div style="font-size:2rem;">⭐</div>
            <div style="font-size:1.5rem; font-weight:bold; color:#856404;">{qualified_leads}</div>
            <div style="font-size:0.8rem; color:#666;">Qualificados ({pct_qualified:.1f}%)</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        pct_hot = (hot_leads / total_filtrado * 100) if total_filtrado > 0 else 0
        st.markdown(f"""
        <div style="text-align:center; padding:1rem; background:#f8d7da; border-radius:10px;">
            <div style="font-size:2rem;">🔥</div>
            <div style="font-size:1.5rem; font-weight:bold; color:#721c24;">{hot_leads}</div>
            <div style="font-size:0.8rem; color:#666;">Hot Leads ({pct_hot:.1f}%)</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("</div>", unsafe_allow_html=True)

# ============================================================================
# INTERFACE PRINCIPAL
# ============================================================================

def main():
    # Header
    st.markdown('<h1 class="main-header">🎯 Hunter OS</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">B2B Prospecting - Maringá e Região</p>', unsafe_allow_html=True)
    
    # ========================================================================
    # SIDEBAR - FILTROS E CONTROLES
    # ========================================================================
    
    with st.sidebar:
        st.header("⚙️ Configurações")
        
        # Status da Pesquisa
        st.markdown("""
        <div class="info-box">
            <strong>📡 Fonte de Dados:</strong><br>
            <span class="source-badge">BrasilAPI</span>
            <span class="source-badge">CNPJ.ws</span>
            <span class="source-badge">ReceitaWS</span>
        </div>
        """, unsafe_allow_html=True)
        
        st.divider()
        
        # Controle de Extração
        st.subheader("📥 Controle de Extração")
        
        quantidade_extrair = st.number_input(
            "Quantidade de leads a extrair",
            min_value=10,
            max_value=1000,
            value=50,
            step=10,
            help="Defina quantos leads deseja extrair nesta pesquisa"
        )
        
        st.divider()
        
        # Seletor de cidades
        st.subheader("🏙️ Cidades")
        cidades_selecionadas = st.multiselect(
            "Selecione as cidades",
            options=list(CIDADES_ALVO.keys()),
            default=list(CIDADES_ALVO.keys()),
            help="Cidades alvo para prospecção"
        )
        
        # Seletor de CNAEs
        st.subheader("🏢 Setores (CNAE)")
        cnaes_selecionados = st.multiselect(
            "Selecione os setores",
            options=list(CNAES_PRIORITARIOS.keys()),
            default=list(CNAES_PRIORITARIOS.keys()),
            help="Setores prioritários para prospecção"
        )
        
        # Score mínimo
        st.subheader("🎯 Score ICP")
        score_minimo = st.slider(
            "Score mínimo",
            min_value=0,
            max_value=100,
            value=50,
            step=5,
            help="Filtrar leads com score acima deste valor"
        )
        
        st.divider()
        
        # Botão de Pesquisa Principal
        st.subheader("🔄 Ações")
        
        iniciar_pesquisa = st.button(
            "🚀 Iniciar Nova Pesquisa",
            use_container_width=True,
            type="primary"
        )
        
        if iniciar_pesquisa:
            st.session_state['iniciar_pesquisa'] = True
            st.session_state['reload_data'] = True
    
    # ========================================================================
    # ÁREA PRINCIPAL
    # ========================================================================
    
    # Container de Status da Pesquisa
    status_container = st.container()
    
    # Verifica se deve iniciar pesquisa
    if st.session_state.get('iniciar_pesquisa', False):
        with status_container:
            st.markdown("""
            <div class="progress-container">
                <div class="progress-header">
                    <span class="progress-title">🔍 Pesquisa em Andamento</span>
                    <span class="progress-percentage searching" id="progress-pct">0%</span>
                </div>
            """, unsafe_allow_html=True)
            
            # Informações da pesquisa
            col_info1, col_info2, col_info3 = st.columns(3)
            with col_info1:
                st.markdown("""
                <div class="info-box">
                    <strong>📍 Região:</strong> Maringá e região (50km)
                </div>
                """, unsafe_allow_html=True)
            with col_info2:
                st.markdown(f"""
                <div class="info-box">
                    <strong>🎯 Meta:</strong> {quantidade_extrair} leads
                </div>
                """, unsafe_allow_html=True)
            with col_info3:
                st.markdown("""
                <div class="info-box">
                    <strong>📡 APIs:</strong> BrasilAPI, CNPJ.ws
                </div>
                """, unsafe_allow_html=True)
            
            # Barra de progresso
            progress_bar = st.progress(0)
            status_text = st.empty()
            detail_text = st.empty()
            
            st.markdown("</div>", unsafe_allow_html=True)
            
            # Carrega e processa dados
            status_text.markdown("**🔄 Carregando base de dados...**")
            dados_brutos = carregar_dados_iniciais()
            
            # Limita à quantidade solicitada
            dados_brutos = dados_brutos[:quantidade_extrair]
            
            # Processa pipeline ETL
            leads_processados, stats = processar_pipeline_etl(
                dados_brutos, 
                status_container, 
                progress_bar, 
                status_text, 
                detail_text
            )
            
            # Finaliza
            progress_bar.progress(1.0)
            status_text.markdown("**✅ Pesquisa concluída!**")
            
            # Exibe resumo
            st.markdown(f"""
            <div class="info-box success">
                <strong>✅ Pesquisa Finalizada!</strong><br>
                📊 <strong>{stats['total_encontrados']}</strong> empresas encontradas na base<br>
                ✅ <strong>{stats['transformados']}</strong> empresas válidas processadas<br>
                ❌ <strong>{stats['erros']}</strong> registros com erro<br>
                📡 Fonte: <strong>{stats['fonte']}</strong>
            </div>
            """, unsafe_allow_html=True)
            
            # Converte para DataFrame
            st.session_state['df_leads'] = pd.DataFrame(leads_processados)
            st.session_state['stats'] = stats
            st.session_state['reload_data'] = False
            st.session_state['iniciar_pesquisa'] = False
            
            time.sleep(1)
            st.rerun()
    
    # Carrega dados se não houver pesquisa em andamento
    if 'df_leads' not in st.session_state or st.session_state.get('reload_data', True):
        with st.spinner("🔄 Carregando dados existentes..."):
            dados_brutos = carregar_dados_iniciais()
            
            # Processa sem feedback detalhado
            cache = init_cache()
            _, transformer, _ = init_components(cache)
            
            leads_processados = []
            for empresa in dados_brutos:
                try:
                    empresa_transformada = transformer.transformar_empresa(empresa)
                    if empresa_transformada:
                        empresa_transformada['icp_score'] = ICPScorer.calcular_score(empresa_transformada)
                        empresa_transformada['classificacao'] = ICPScorer.classificar_lead(empresa_transformada['icp_score'])
                        leads_processados.append(empresa_transformada)
                except:
                    continue
            
            st.session_state['df_leads'] = pd.DataFrame(leads_processados)
            st.session_state['reload_data'] = False
    
    df = st.session_state['df_leads']
    
    # Aplica filtros
    df_filtrado = filtrar_dataframe(df, cidades_selecionadas, cnaes_selecionados, score_minimo)
    
    # ========================================================================
    # FUNIL DE PROSPECÇÃO
    # ========================================================================
    
    hot_leads = len(df_filtrado[df_filtrado['icp_score'] >= 85])
    qualified_leads = len(df_filtrado[(df_filtrado['icp_score'] >= 70) & (df_filtrado['icp_score'] < 85)])
    
    render_funnel(len(df), len(df_filtrado), hot_leads, qualified_leads)
    
    # ========================================================================
    # MÉTRICAS DETALHADAS
    # ========================================================================
    
    st.markdown("### 📊 Métricas da Base Atual")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            label="🏢 Base Total",
            value=len(df),
            help="Total de empresas na base de dados"
        )
    
    with col2:
        st.metric(
            label="🎯 Após Filtros",
            value=len(df_filtrado),
            delta=f"{(len(df_filtrado)/len(df)*100):.1f}% da base" if len(df) > 0 else "0%"
        )
    
    with col3:
        st.metric(
            label="🔥 Hot Leads",
            value=hot_leads,
            delta=f"Score ≥ 85"
        )
    
    with col4:
        media_score = df_filtrado['icp_score'].mean() if len(df_filtrado) > 0 else 0
        st.metric(
            label="📈 Score Médio",
            value=f"{media_score:.1f}"
        )
    
    with col5:
        com_whatsapp = df_filtrado['whatsapp_provavel'].sum() if 'whatsapp_provavel' in df_filtrado.columns else 0
        st.metric(
            label="📱 Com WhatsApp",
            value=int(com_whatsapp)
        )
    
    st.divider()
    
    # ========================================================================
    # CONTROLE DE VISUALIZAÇÃO
    # ========================================================================
    
    st.markdown("### 📋 Lista de Leads")
    
    # Controles de visualização
    col_ctrl1, col_ctrl2, col_ctrl3 = st.columns([2, 2, 1])
    
    with col_ctrl1:
        visualizar_quantidade = st.selectbox(
            "Exibir",
            options=[10, 25, 50, 100, "Todos"],
            index=1,
            help="Quantidade de leads a exibir na tabela"
        )
    
    with col_ctrl2:
        ordenar_por = st.selectbox(
            "Ordenar por",
            options=["Score (maior)", "Score (menor)", "Nome A-Z", "Cidade"],
            index=0
        )
    
    with col_ctrl3:
        st.markdown("<br>", unsafe_allow_html=True)
        mostrar_detalhes = st.checkbox("Detalhes", value=False)
    
    # Aplica ordenação
    if ordenar_por == "Score (maior)":
        df_exibir = df_filtrado.sort_values('icp_score', ascending=False)
    elif ordenar_por == "Score (menor)":
        df_exibir = df_filtrado.sort_values('icp_score', ascending=True)
    elif ordenar_por == "Nome A-Z":
        df_exibir = df_filtrado.sort_values('nome_fantasia', ascending=True)
    else:
        df_exibir = df_filtrado.sort_values('municipio', ascending=True)
    
    # Aplica limite
    if visualizar_quantidade != "Todos":
        df_exibir = df_exibir.head(visualizar_quantidade)
    
    # Colunas para exibição
    if mostrar_detalhes:
        colunas_exibir = [
            'classificacao', 'icp_score', 'razao_social', 'nome_fantasia',
            'cnae_descricao', 'telefone', 'email', 'municipio', 'whatsapp_provavel'
        ]
    else:
        colunas_exibir = [
            'classificacao', 'icp_score', 'nome_fantasia',
            'cnae_descricao', 'telefone', 'municipio'
        ]
    
    # Filtra colunas existentes
    colunas_existentes = [c for c in colunas_exibir if c in df_exibir.columns]
    df_tabela = df_exibir[colunas_existentes].copy()
    
    # Renomeia colunas para exibição
    colunas_rename = {
        'classificacao': 'Status',
        'icp_score': 'Score',
        'razao_social': 'Razão Social',
        'nome_fantasia': 'Empresa',
        'cnae_descricao': 'Setor',
        'telefone': 'Telefone',
        'email': 'Email',
        'municipio': 'Cidade',
        'whatsapp_provavel': 'WhatsApp'
    }
    
    df_tabela = df_tabela.rename(columns=colunas_rename)
    
    # Exibe tabela interativa
    st.dataframe(
        df_tabela,
        use_container_width=True,
        height=400,
        column_config={
            "Score": st.column_config.ProgressColumn(
                "Score",
                help="Score ICP do lead",
                min_value=0,
                max_value=100,
                format="%d"
            ),
            "Status": st.column_config.TextColumn(
                "Status",
                help="Classificação do lead"
            ),
            "WhatsApp": st.column_config.CheckboxColumn(
                "WhatsApp",
                help="Possui WhatsApp provável"
            )
        }
    )
    
    # Info de paginação
    st.markdown(f"""
    <div style="text-align: center; color: #666; font-size: 0.9rem; margin-top: 0.5rem;">
        Exibindo <strong>{len(df_tabela)}</strong> de <strong>{len(df_filtrado)}</strong> leads filtrados 
        (Base total: <strong>{len(df)}</strong> empresas)
    </div>
    """, unsafe_allow_html=True)
    
    st.divider()
    
    # ========================================================================
    # EXPORTAÇÃO E RELATÓRIOS
    # ========================================================================
    
    st.markdown("### 📤 Exportação e Relatórios")
    
    col_exp1, col_exp2, col_exp3 = st.columns(3)
    
    with col_exp1:
        st.markdown("**💾 Exportar para CRM**")
        
        export_quantidade = st.number_input(
            "Quantidade a exportar",
            min_value=1,
            max_value=len(df_filtrado),
            value=min(50, len(df_filtrado)),
            help="Quantos leads exportar (ordenados por score)"
        )
        
        if st.button("📥 Gerar CSV", use_container_width=True):
            # Prepara dados para exportação
            df_export = df_filtrado.sort_values('icp_score', ascending=False).head(export_quantidade)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"leads_crm_{timestamp}.csv"
            
            # Exporta
            csv_data = df_export.to_csv(index=False, encoding='utf-8-sig')
            
            st.download_button(
                label=f"⬇️ Baixar {export_quantidade} leads",
                data=csv_data,
                file_name=filename,
                mime="text/csv"
            )
            
            st.success(f"✅ {export_quantidade} leads prontos para download!")
    
    with col_exp2:
        st.markdown("**📊 Relatório de Inteligência**")
        st.markdown(f"Análise de {len(df_filtrado)} leads")
        
        if st.button("📈 Gerar Relatório", use_container_width=True):
            relatorio = gerar_relatorio_inteligencia(df_filtrado)
            
            st.markdown(relatorio)
            
            # Oferece download do relatório
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename_rel = f"relatorio_inteligencia_{timestamp}.md"
            
            st.download_button(
                label="⬇️ Baixar Relatório",
                data=relatorio,
                file_name=filename_rel,
                mime="text/markdown"
            )
    
    with col_exp3:
        st.markdown("**📋 Resumo Rápido**")
        
        st.markdown(f"""
        <div class="info-box">
            <strong>📊 Estatísticas:</strong><br>
            • Base: {len(df)} empresas<br>
            • Filtrados: {len(df_filtrado)} ({(len(df_filtrado)/len(df)*100):.1f}%)<br>
            • Hot Leads: {hot_leads}<br>
            • Score médio: {media_score:.1f}<br>
            • Com WhatsApp: {int(com_whatsapp)}
        </div>
        """, unsafe_allow_html=True)
    
    # ========================================================================
    # DETALHES DO LEAD SELECIONADO
    # ========================================================================
    
    st.divider()
    st.markdown("### 🔍 Detalhes do Lead")
    
    if len(df_filtrado) > 0:
        # Seletor de lead
        leads_opcoes = df_filtrado['nome_fantasia'].tolist()
        lead_selecionado = st.selectbox(
            "Selecione um lead para ver detalhes",
            options=leads_opcoes
        )
        
        if lead_selecionado:
            lead_data = df_filtrado[df_filtrado['nome_fantasia'] == lead_selecionado].iloc[0]
            
            col_det1, col_det2, col_det3 = st.columns(3)
            
            with col_det1:
                st.markdown("**📋 Informações Básicas**")
                st.write(f"**Razão Social:** {lead_data.get('razao_social', 'N/A')}")
                st.write(f"**Nome Fantasia:** {lead_data.get('nome_fantasia', 'N/A')}")
                st.write(f"**CNPJ:** {lead_data.get('cnpj', 'N/A')}")
                st.write(f"**Setor:** {lead_data.get('cnae_descricao', 'N/A')}")
            
            with col_det2:
                st.markdown("**📞 Contato**")
                st.write(f"**Telefone:** {lead_data.get('telefone', 'N/A')}")
                st.write(f"**Email:** {lead_data.get('email', 'N/A')}")
                st.write(f"**WhatsApp:** {'✅ Sim' if lead_data.get('whatsapp_provavel') else '❌ Não'}")
                st.write(f"**Domínio Próprio:** {'✅ Sim' if lead_data.get('dominio_proprio') else '❌ Não'}")
            
            with col_det3:
                st.markdown("**📍 Localização**")
                st.write(f"**Cidade:** {lead_data.get('municipio', 'N/A')}")
                st.write(f"**Endereço:** {lead_data.get('endereco', 'N/A')}")
                st.write(f"**CEP:** {lead_data.get('cep', 'N/A')}")
            
            # Score breakdown
            st.markdown("---")
            st.markdown("**🎯 Análise de Score ICP**")
            
            col_score1, col_score2 = st.columns([1, 2])
            
            with col_score1:
                # Gauge visual do score
                score = lead_data.get('icp_score', 0)
                color = "#ff4b4b" if score >= 85 else "#ffa500" if score >= 70 else "#28a745" if score >= 50 else "#666"
                
                st.markdown(f"""
                <div style="text-align: center; padding: 1rem; background: #f8f9fa; border-radius: 10px;">
                    <div style="font-size: 3rem; font-weight: bold; color: {color};">{score}</div>
                    <div style="font-size: 1rem; color: #666;">{lead_data.get('classificacao', 'N/A')}</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col_score2:
                score_breakdown = []
                score_breakdown.append(f"✅ Base: 50 pontos")
                
                if lead_data.get('site') or lead_data.get('instagram'):
                    score_breakdown.append(f"✅ Site/Instagram: +20 pontos")
                else:
                    score_breakdown.append(f"❌ Site/Instagram: +0 pontos")
                
                if lead_data.get('whatsapp_provavel'):
                    score_breakdown.append(f"✅ WhatsApp provável: +15 pontos")
                else:
                    score_breakdown.append(f"❌ WhatsApp provável: +0 pontos")
                
                cnae = str(lead_data.get('cnae_principal', ''))[:4]
                if cnae in ['8211', '8219', '8220', '8291', '6910', '6920']:
                    score_breakdown.append(f"✅ CNAE Serviços: +15 pontos")
                else:
                    score_breakdown.append(f"❌ CNAE Serviços: +0 pontos")
                
                if lead_data.get('dominio_proprio'):
                    score_breakdown.append(f"✅ Domínio próprio: +10 pontos")
                else:
                    score_breakdown.append(f"❌ Domínio próprio: +0 pontos")
                
                for item in score_breakdown:
                    st.markdown(item)
    
    # ========================================================================
    # FOOTER
    # ========================================================================
    
    st.divider()
    st.markdown("""
    <div style="text-align: center; color: #666; font-size: 0.9rem;">
        <p>🎯 <strong>Hunter OS v2.0</strong> - B2B Prospecting | Desenvolvido para prospecção inteligente de leads</p>
        <p>📍 Região: Maringá, Sarandi, Marialva, Paiçandu, Mandaguari (PR)</p>
        <p>📡 Fontes: BrasilAPI, CNPJ.ws, ReceitaWS</p>
    </div>
    """, unsafe_allow_html=True)

# ============================================================================
# EXECUÇÃO
# ============================================================================

if __name__ == "__main__":
    main()
