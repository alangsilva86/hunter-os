"""
Hunter OS - B2B Prospecting Tool
Interface Streamlit com integração à API Casa dos Dados
Versão 3.0
"""

import streamlit as st
import pandas as pd
import time
import re
from datetime import datetime
from data_sources import (
    criar_searcher, 
    get_setores_disponiveis, 
    get_cidades_disponiveis,
    SETORES_CNAE
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

# CSS customizado
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        text-align: center;
        padding: 1rem 0;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .status-badge {
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: bold;
    }
    .status-active {
        background-color: #10B981;
        color: white;
    }
    .status-searching {
        background-color: #F59E0B;
        color: white;
    }
    .funnel-step {
        padding: 0.5rem 1rem;
        margin: 0.25rem 0;
        border-radius: 5px;
        text-align: center;
    }
    .progress-container {
        background: #f0f0f0;
        border-radius: 10px;
        padding: 1rem;
        margin: 1rem 0;
    }
    .empresa-card {
        border: 1px solid #e0e0e0;
        border-radius: 10px;
        padding: 1rem;
        margin: 0.5rem 0;
        background: white;
    }
    .score-high { color: #10B981; font-weight: bold; }
    .score-medium { color: #F59E0B; font-weight: bold; }
    .score-low { color: #EF4444; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# FUNÇÕES AUXILIARES
# ============================================================================

def calcular_score_icp(empresa: dict) -> int:
    """Calcula score ICP (Ideal Customer Profile) de 0-100"""
    score = 0
    
    # Tem telefone (+25)
    if empresa.get('ddd_telefone_1'):
        score += 25
        # É celular (+10 extra)
        tel = str(empresa.get('ddd_telefone_1', ''))
        if len(tel) >= 10 and tel[2] in ['9', '8', '7']:
            score += 10
    
    # Tem email (+20)
    if empresa.get('email'):
        score += 20
        # Email próprio (não é gmail/hotmail) (+5 extra)
        email = empresa.get('email', '').lower()
        if email and '@' in email:
            dominio = email.split('@')[1]
            if dominio not in ['gmail.com', 'hotmail.com', 'outlook.com', 'yahoo.com']:
                score += 5
    
    # Porte da empresa
    porte = str(empresa.get('porte', '')).upper()
    if 'GRANDE' in porte:
        score += 20
    elif 'MEDIO' in porte or 'MÉDIO' in porte:
        score += 15
    elif 'PEQUENO' in porte:
        score += 10
    
    # Capital social
    capital = empresa.get('capital_social', 0)
    if capital:
        if capital >= 1000000:
            score += 15
        elif capital >= 100000:
            score += 10
        elif capital >= 10000:
            score += 5
    
    # Tem quadro societário (+5)
    if empresa.get('quadro_societario'):
        score += 5
    
    return min(score, 100)

def formatar_telefone(telefone: str) -> str:
    """Formata telefone para exibição"""
    if not telefone:
        return "-"
    tel = re.sub(r'\D', '', str(telefone))
    if len(tel) == 11:
        return f"({tel[:2]}) {tel[2:7]}-{tel[7:]}"
    elif len(tel) == 10:
        return f"({tel[:2]}) {tel[2:6]}-{tel[6:]}"
    return telefone

def formatar_cnpj(cnpj: str) -> str:
    """Formata CNPJ para exibição"""
    if not cnpj:
        return "-"
    cnpj = re.sub(r'\D', '', str(cnpj))
    if len(cnpj) == 14:
        return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"
    return cnpj

def formatar_capital(valor: float) -> str:
    """Formata capital social"""
    if not valor:
        return "-"
    if valor >= 1000000:
        return f"R$ {valor/1000000:.1f}M"
    elif valor >= 1000:
        return f"R$ {valor/1000:.1f}K"
    return f"R$ {valor:.2f}"

def get_score_class(score: int) -> str:
    """Retorna classe CSS baseada no score"""
    if score >= 70:
        return "score-high"
    elif score >= 40:
        return "score-medium"
    return "score-low"

# ============================================================================
# INICIALIZAÇÃO DO ESTADO
# ============================================================================

if 'empresas' not in st.session_state:
    st.session_state.empresas = []
if 'stats' not in st.session_state:
    st.session_state.stats = {}
if 'buscando' not in st.session_state:
    st.session_state.buscando = False
if 'progresso' not in st.session_state:
    st.session_state.progresso = {}
if 'searcher' not in st.session_state:
    st.session_state.searcher = criar_searcher()

# ============================================================================
# SIDEBAR - CONFIGURAÇÕES
# ============================================================================

with st.sidebar:
    st.markdown("## ⚙️ Configurações de Busca")
    
    # Status da API
    st.markdown("### 📡 Status da API")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<span class="status-badge status-active">● Casa dos Dados</span>', unsafe_allow_html=True)
    with col2:
        cache_count = st.session_state.searcher.get_cache_count()
        st.caption(f"💾 Cache: {cache_count} empresas")
    
    st.markdown("---")
    
    # Quantidade de leads
    st.markdown("### 📥 Quantidade de Leads")
    limite = st.number_input(
        "Quantas empresas buscar?",
        min_value=10,
        max_value=1000,
        value=50,
        step=10,
        help="Quantidade de empresas a serem buscadas (máx: 1000)"
    )
    
    st.markdown("---")
    
    # Seleção de cidades
    st.markdown("### 🏙️ Cidades")
    cidades_disponiveis = get_cidades_disponiveis()
    cidades_selecionadas = st.multiselect(
        "Selecione as cidades",
        options=cidades_disponiveis,
        default=["MARINGA"],
        help="Selecione uma ou mais cidades para buscar"
    )
    
    # Estado
    uf = st.selectbox(
        "Estado",
        options=["PR", "SP", "RJ", "MG", "SC", "RS", "BA", "GO", "DF"],
        index=0
    )
    
    st.markdown("---")
    
    # Seleção de setores
    st.markdown("### 🏢 Setores (CNAE)")
    setores_disponiveis = get_setores_disponiveis()
    setores_selecionados = st.multiselect(
        "Selecione os setores",
        options=setores_disponiveis,
        default=["Serviços Administrativos"],
        help="Selecione um ou mais setores de atividade"
    )
    
    # Mostra CNAEs selecionados
    if setores_selecionados:
        with st.expander("📋 CNAEs incluídos"):
            for setor in setores_selecionados:
                cnaes = SETORES_CNAE.get(setor, [])
                st.caption(f"**{setor}:** {', '.join(cnaes)}")
    
    st.markdown("---")
    
    # Filtros avançados
    st.markdown("### 🎯 Filtros Avançados")
    
    excluir_mei = st.checkbox("Excluir MEI", value=True)
    com_telefone = st.checkbox("Apenas com telefone", value=False)
    com_email = st.checkbox("Apenas com e-mail", value=False)
    
    score_minimo = st.slider(
        "Score ICP mínimo",
        min_value=0,
        max_value=100,
        value=0,
        help="Filtrar empresas com score acima deste valor"
    )
    
    st.markdown("---")
    
    # Botões de ação
    st.markdown("### 🚀 Ações")
    
    col1, col2 = st.columns(2)
    with col1:
        btn_buscar = st.button(
            "🔍 Buscar",
            type="primary",
            use_container_width=True,
            disabled=st.session_state.buscando
        )
    with col2:
        btn_limpar = st.button(
            "🗑️ Limpar",
            use_container_width=True
        )

# ============================================================================
# ÁREA PRINCIPAL
# ============================================================================

# Header
st.markdown('<h1 class="main-header">🎯 Hunter OS</h1>', unsafe_allow_html=True)
st.markdown('<p style="text-align: center; color: #666;">B2B Prospecting - Dados Reais da Receita Federal via Casa dos Dados</p>', unsafe_allow_html=True)

# Ação de limpar
if btn_limpar:
    st.session_state.empresas = []
    st.session_state.stats = {}
    st.session_state.progresso = {}
    st.rerun()

# Ação de buscar
if btn_buscar:
    if not cidades_selecionadas:
        st.error("⚠️ Selecione pelo menos uma cidade!")
    elif not setores_selecionados:
        st.error("⚠️ Selecione pelo menos um setor!")
    else:
        st.session_state.buscando = True
        st.session_state.empresas = []
        st.session_state.stats = {}
        
        # Container de progresso
        progress_container = st.container()
        
        with progress_container:
            st.markdown("### 🔄 Pesquisa em Andamento")
            
            # Informações da busca
            col1, col2, col3 = st.columns(3)
            with col1:
                st.info(f"📍 **Região:** {', '.join(cidades_selecionadas)}")
            with col2:
                st.info(f"🎯 **Meta:** {limite} leads")
            with col3:
                st.info(f"📡 **Fonte:** Casa dos Dados API")
            
            # Barra de progresso
            progress_bar = st.progress(0)
            status_text = st.empty()
            metrics_placeholder = st.empty()
            
            # Callback de progresso
            def atualizar_progresso(fase, pagina, encontrados, meta, mensagem, **kwargs):
                progresso = min(encontrados / meta, 1.0) if meta > 0 else 0
                progress_bar.progress(progresso)
                status_text.markdown(f"**{mensagem}**")
                
                total_base = kwargs.get('total_base', 0)
                with metrics_placeholder.container():
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("📊 Total na Base", f"{total_base:,}" if total_base else "...")
                    c2.metric("📥 Extraídos", f"{encontrados:,}")
                    c3.metric("📄 Páginas", pagina)
                    c4.metric("⏱️ Status", fase.upper())
            
            # Executa busca
            try:
                empresas, stats = st.session_state.searcher.buscar_empresas(
                    cidades=cidades_selecionadas,
                    setores=setores_selecionados,
                    limite=limite,
                    callback_progresso=atualizar_progresso
                )
                
                # Aplica score ICP
                for emp in empresas:
                    emp['score_icp'] = calcular_score_icp(emp)
                
                # Filtra por score mínimo
                if score_minimo > 0:
                    empresas = [e for e in empresas if e.get('score_icp', 0) >= score_minimo]
                
                st.session_state.empresas = empresas
                st.session_state.stats = stats
                
                progress_bar.progress(1.0)
                
                if stats.get('erros'):
                    for erro in stats['erros']:
                        st.error(f"⚠️ {erro}")
                else:
                    status_text.success(f"✅ Busca concluída! {len(empresas)} empresas encontradas.")
                
            except Exception as e:
                st.error(f"❌ Erro na busca: {str(e)}")
            
            finally:
                st.session_state.buscando = False
                time.sleep(1)
                st.rerun()

# ============================================================================
# EXIBIÇÃO DOS RESULTADOS
# ============================================================================

if st.session_state.empresas:
    empresas = st.session_state.empresas
    stats = st.session_state.stats
    
    st.markdown("---")
    
    # Funil de prospecção
    st.markdown("### 📊 Funil de Prospecção")
    
    total_base = stats.get('total_na_base', 0)
    total_extraido = len(empresas)
    hot_leads = len([e for e in empresas if e.get('score_icp', 0) >= 70])
    com_telefone = len([e for e in empresas if e.get('ddd_telefone_1')])
    com_email = len([e for e in empresas if e.get('email')])
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            "🏢 Total na Base",
            f"{total_base:,}",
            help="Total de empresas encontradas com os filtros"
        )
    
    with col2:
        pct = (total_extraido / total_base * 100) if total_base > 0 else 0
        st.metric(
            "📥 Extraídos",
            f"{total_extraido:,}",
            f"{pct:.1f}%"
        )
    
    with col3:
        pct = (hot_leads / total_extraido * 100) if total_extraido > 0 else 0
        st.metric(
            "🔥 Hot Leads",
            f"{hot_leads:,}",
            f"{pct:.1f}%"
        )
    
    with col4:
        pct = (com_telefone / total_extraido * 100) if total_extraido > 0 else 0
        st.metric(
            "📞 Com Telefone",
            f"{com_telefone:,}",
            f"{pct:.1f}%"
        )
    
    with col5:
        pct = (com_email / total_extraido * 100) if total_extraido > 0 else 0
        st.metric(
            "📧 Com E-mail",
            f"{com_email:,}",
            f"{pct:.1f}%"
        )
    
    st.markdown("---")
    
    # Controles de visualização
    st.markdown("### 📋 Lista de Empresas")
    
    col1, col2, col3 = st.columns([2, 2, 1])
    
    with col1:
        visualizar_qtd = st.selectbox(
            "Exibir",
            options=[10, 25, 50, 100, len(empresas)],
            index=1,
            format_func=lambda x: f"{x} empresas" if x < len(empresas) else f"Todas ({len(empresas)})"
        )
    
    with col2:
        ordenar_por = st.selectbox(
            "Ordenar por",
            options=["Score ICP (maior)", "Score ICP (menor)", "Razão Social (A-Z)", "Capital Social"],
            index=0
        )
    
    with col3:
        mostrar_detalhes = st.checkbox("Detalhes", value=True)
    
    # Ordena empresas
    if ordenar_por == "Score ICP (maior)":
        empresas_ordenadas = sorted(empresas, key=lambda x: x.get('score_icp', 0), reverse=True)
    elif ordenar_por == "Score ICP (menor)":
        empresas_ordenadas = sorted(empresas, key=lambda x: x.get('score_icp', 0))
    elif ordenar_por == "Razão Social (A-Z)":
        empresas_ordenadas = sorted(empresas, key=lambda x: x.get('razao_social', ''))
    else:
        empresas_ordenadas = sorted(empresas, key=lambda x: x.get('capital_social', 0) or 0, reverse=True)
    
    # Exibe empresas
    for i, emp in enumerate(empresas_ordenadas[:visualizar_qtd]):
        score = emp.get('score_icp', 0)
        score_class = get_score_class(score)
        
        with st.container():
            col1, col2, col3, col4 = st.columns([3, 2, 1, 1])
            
            with col1:
                nome = emp.get('nome_fantasia') or emp.get('razao_social', 'N/A')
                st.markdown(f"**{i+1}. {nome[:50]}**")
                if mostrar_detalhes:
                    st.caption(f"📍 {emp.get('municipio', 'N/A')}/{emp.get('uf', '')} | CNPJ: {formatar_cnpj(emp.get('cnpj'))}")
            
            with col2:
                telefone = formatar_telefone(emp.get('ddd_telefone_1'))
                email = emp.get('email', '-') or '-'
                st.markdown(f"📞 {telefone}")
                if mostrar_detalhes:
                    st.caption(f"📧 {email[:30]}..." if len(email) > 30 else f"📧 {email}")
            
            with col3:
                st.markdown(f"💰 {formatar_capital(emp.get('capital_social'))}")
                if mostrar_detalhes:
                    st.caption(f"🏢 {emp.get('porte', 'N/A')[:15]}")
            
            with col4:
                st.markdown(f'<span class="{score_class}">Score: {score}</span>', unsafe_allow_html=True)
                if score >= 70:
                    st.caption("🔥 Hot Lead")
            
            st.markdown("---")
    
    # Exportação
    st.markdown("### 📤 Exportar Dados")
    
    col1, col2 = st.columns(2)
    
    with col1:
        qtd_exportar = st.number_input(
            "Quantidade a exportar",
            min_value=1,
            max_value=len(empresas),
            value=min(50, len(empresas))
        )
    
    with col2:
        # Prepara DataFrame para exportação
        df_export = pd.DataFrame([
            {
                'CNPJ': formatar_cnpj(e.get('cnpj')),
                'Razão Social': e.get('razao_social'),
                'Nome Fantasia': e.get('nome_fantasia'),
                'Telefone': formatar_telefone(e.get('ddd_telefone_1')),
                'Email': e.get('email'),
                'Cidade': e.get('municipio'),
                'UF': e.get('uf'),
                'Bairro': e.get('bairro'),
                'Endereço': f"{e.get('logradouro', '')}, {e.get('numero', '')}",
                'CEP': e.get('cep'),
                'CNAE': e.get('cnae_fiscal'),
                'Atividade': e.get('cnae_fiscal_descricao'),
                'Porte': e.get('porte'),
                'Capital Social': e.get('capital_social'),
                'Score ICP': e.get('score_icp'),
                'Situação': e.get('situacao_cadastral')
            }
            for e in empresas_ordenadas[:qtd_exportar]
        ])
        
        csv = df_export.to_csv(index=False, encoding='utf-8-sig')
        
        st.download_button(
            label="📥 Baixar CSV",
            data=csv,
            file_name=f"hunter_os_leads_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True
        )

else:
    # Tela inicial
    st.markdown("---")
    
    st.info("""
    ### 👋 Bem-vindo ao Hunter OS!
    
    Para começar sua prospecção B2B:
    
    1. **Configure os filtros** na barra lateral:
       - Selecione as **cidades** de interesse
       - Escolha os **setores** (CNAE) desejados
       - Defina a **quantidade** de leads
       - Ajuste os **filtros avançados**
    
    2. **Clique em "🔍 Buscar"** para iniciar
    
    3. **Acompanhe o progresso** em tempo real:
       - Total de empresas na base
       - Quantidade extraída
       - Status da busca
    
    4. **Exporte os resultados** em CSV para seu CRM
    
    ---
    
    **📡 Fonte de Dados:** API Casa dos Dados (Receita Federal)  
    **🔄 Atualização:** Dados atualizados diariamente  
    **📊 Base:** 69+ milhões de empresas brasileiras
    """)
    
    # Estatísticas do cache
    cache_count = st.session_state.searcher.get_cache_count()
    if cache_count > 0:
        st.success(f"💾 **Cache local:** {cache_count} empresas armazenadas de buscas anteriores")

# Footer
st.markdown("---")
st.markdown(
    '<p style="text-align: center; color: #888; font-size: 0.8rem;">'
    'Hunter OS v3.0 | Powered by Casa dos Dados API | '
    '© 2024 Momentum OS'
    '</p>',
    unsafe_allow_html=True
)
