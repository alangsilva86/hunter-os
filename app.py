"""
Hunter OS - B2B Prospecting
Aplicação principal Streamlit para prospecção de leads B2B
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

# CSS Customizado
st.markdown("""
<style>
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
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .hot-lead {
        background-color: #ff4b4b;
        color: white;
        padding: 0.2rem 0.5rem;
        border-radius: 5px;
        font-weight: bold;
    }
    .qualified-lead {
        background-color: #ffa500;
        color: white;
        padding: 0.2rem 0.5rem;
        border-radius: 5px;
    }
    .stDataFrame {
        font-size: 0.9rem;
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

def processar_pipeline_etl(dados_brutos, progress_bar=None):
    """Executa o pipeline ETL completo"""
    cache = init_cache()
    _, transformer, enricher = init_components(cache)
    
    leads_processados = []
    total = len(dados_brutos)
    
    for i, empresa in enumerate(dados_brutos):
        try:
            # Transform
            empresa_transformada = transformer.transformar_empresa(empresa)
            
            if empresa_transformada:
                # Enrich (apenas para top leads ou se habilitado)
                # Por padrão, não faz enriquecimento web para evitar rate limits
                # empresa_enriquecida = enricher.enriquecer_empresa(empresa_transformada, buscar_web=False)
                
                # Calcula score ICP
                empresa_transformada['icp_score'] = ICPScorer.calcular_score(empresa_transformada)
                empresa_transformada['classificacao'] = ICPScorer.classificar_lead(empresa_transformada['icp_score'])
                
                leads_processados.append(empresa_transformada)
        
        except Exception as e:
            st.warning(f"Erro ao processar empresa: {e}")
            continue
        
        if progress_bar:
            progress_bar.progress((i + 1) / total)
    
    return leads_processados

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
        st.header("⚙️ Filtros")
        
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
        
        # Botões de ação
        st.subheader("🔄 Ações")
        
        if st.button("🔍 Carregar/Atualizar Dados", use_container_width=True):
            st.session_state['reload_data'] = True
        
        # Opção de enriquecimento web
        st.subheader("🌐 Enriquecimento Web")
        enriquecer_web = st.checkbox(
            "Buscar sites e redes sociais",
            value=False,
            help="⚠️ Pode demorar devido a rate limits"
        )
        
        if enriquecer_web:
            st.warning("O enriquecimento web pode demorar alguns minutos.")
    
    # ========================================================================
    # ÁREA PRINCIPAL
    # ========================================================================
    
    # Carrega e processa dados
    if 'df_leads' not in st.session_state or st.session_state.get('reload_data', True):
        with st.spinner("🔄 Carregando e processando dados..."):
            progress_bar = st.progress(0)
            
            # Carrega dados brutos
            dados_brutos = carregar_dados_iniciais()
            
            # Processa pipeline ETL
            leads_processados = processar_pipeline_etl(dados_brutos, progress_bar)
            
            # Converte para DataFrame
            st.session_state['df_leads'] = pd.DataFrame(leads_processados)
            st.session_state['reload_data'] = False
            
            progress_bar.empty()
    
    df = st.session_state['df_leads']
    
    # Aplica filtros
    df_filtrado = filtrar_dataframe(df, cidades_selecionadas, cnaes_selecionados, score_minimo)
    
    # ========================================================================
    # MÉTRICAS
    # ========================================================================
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="📊 Total de Leads",
            value=len(df_filtrado),
            delta=f"de {len(df)} total"
        )
    
    with col2:
        hot_leads = len(df_filtrado[df_filtrado['icp_score'] >= 85])
        st.metric(
            label="🔥 Hot Leads",
            value=hot_leads,
            delta=f"{(hot_leads/len(df_filtrado)*100):.1f}%" if len(df_filtrado) > 0 else "0%"
        )
    
    with col3:
        media_score = df_filtrado['icp_score'].mean() if len(df_filtrado) > 0 else 0
        st.metric(
            label="🎯 Score Médio",
            value=f"{media_score:.1f}"
        )
    
    with col4:
        com_whatsapp = df_filtrado['whatsapp_provavel'].sum() if 'whatsapp_provavel' in df_filtrado.columns else 0
        st.metric(
            label="📱 Com WhatsApp",
            value=com_whatsapp
        )
    
    st.divider()
    
    # ========================================================================
    # TABELA DE DADOS
    # ========================================================================
    
    st.subheader("📋 Lista de Leads")
    
    # Colunas para exibição
    colunas_exibir = [
        'classificacao', 'icp_score', 'razao_social', 'nome_fantasia',
        'cnae_descricao', 'telefone', 'email', 'municipio'
    ]
    
    # Filtra colunas existentes
    colunas_existentes = [c for c in colunas_exibir if c in df_filtrado.columns]
    
    # Ordena por score
    df_exibir = df_filtrado[colunas_existentes].sort_values('icp_score', ascending=False)
    
    # Renomeia colunas para exibição
    colunas_rename = {
        'classificacao': 'Status',
        'icp_score': 'Score',
        'razao_social': 'Razão Social',
        'nome_fantasia': 'Nome Fantasia',
        'cnae_descricao': 'Setor',
        'telefone': 'Telefone',
        'email': 'Email',
        'municipio': 'Cidade'
    }
    
    df_exibir = df_exibir.rename(columns=colunas_rename)
    
    # Exibe tabela interativa
    st.dataframe(
        df_exibir,
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
            )
        }
    )
    
    st.divider()
    
    # ========================================================================
    # EXPORTAÇÃO E RELATÓRIOS
    # ========================================================================
    
    col_exp1, col_exp2 = st.columns(2)
    
    with col_exp1:
        st.subheader("📤 Exportar Dados")
        
        if st.button("💾 Exportar CSV para CRM", use_container_width=True):
            # Prepara dados para exportação
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"leads_crm_{timestamp}.csv"
            
            # Exporta
            df_filtrado.to_csv(filename, index=False, encoding='utf-8-sig')
            
            # Oferece download
            with open(filename, 'rb') as f:
                st.download_button(
                    label="⬇️ Baixar CSV",
                    data=f,
                    file_name=filename,
                    mime="text/csv"
                )
            
            st.success(f"✅ Arquivo exportado: {filename}")
    
    with col_exp2:
        st.subheader("📊 Relatório de Inteligência")
        
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
    
    # ========================================================================
    # DETALHES DO LEAD SELECIONADO
    # ========================================================================
    
    st.divider()
    st.subheader("🔍 Detalhes do Lead")
    
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
            
            score_breakdown = []
            score_breakdown.append(f"- Base: 50 pontos")
            
            if lead_data.get('site') or lead_data.get('instagram'):
                score_breakdown.append(f"- Site/Instagram: +20 pontos ✅")
            else:
                score_breakdown.append(f"- Site/Instagram: +0 pontos ❌")
            
            if lead_data.get('whatsapp_provavel'):
                score_breakdown.append(f"- WhatsApp provável: +15 pontos ✅")
            else:
                score_breakdown.append(f"- WhatsApp provável: +0 pontos ❌")
            
            cnae = str(lead_data.get('cnae_principal', ''))[:4]
            if cnae in ['8211', '8219', '8220', '8291', '6910', '6920']:
                score_breakdown.append(f"- CNAE Serviços: +15 pontos ✅")
            else:
                score_breakdown.append(f"- CNAE Serviços: +0 pontos ❌")
            
            if lead_data.get('dominio_proprio'):
                score_breakdown.append(f"- Domínio próprio: +10 pontos ✅")
            else:
                score_breakdown.append(f"- Domínio próprio: +0 pontos ❌")
            
            score_breakdown.append(f"\n**Total: {lead_data.get('icp_score', 0)} pontos - {lead_data.get('classificacao', 'N/A')}**")
            
            st.markdown('\n'.join(score_breakdown))
    
    # ========================================================================
    # FOOTER
    # ========================================================================
    
    st.divider()
    st.markdown("""
    <div style="text-align: center; color: #666; font-size: 0.9rem;">
        <p>🎯 Hunter OS - B2B Prospecting | Desenvolvido para prospecção inteligente de leads</p>
        <p>Região: Maringá, Sarandi, Marialva, Paiçandu, Mandaguari (PR)</p>
    </div>
    """, unsafe_allow_html=True)

# ============================================================================
# EXECUÇÃO
# ============================================================================

if __name__ == "__main__":
    main()
