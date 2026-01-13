# 🎯 Hunter OS - B2B Prospecting

Sistema de prospecção inteligente de leads B2B focado em empresas de Maringá-PR e região.

## 📋 Descrição

O Hunter OS é uma aplicação de ETL (Extract, Transform, Load) para prospecção de leads B2B. A ferramenta realiza extração, filtragem, enriquecimento e exportação de leads baseados em geografia e CNAE, com foco em encontrar empresas com "dores operacionais" em Maringá-PR e região.

## 🎯 ICP (Ideal Customer Profile)

### Geografia
- Maringá, Sarandi, Marialva, Paiçandu, Mandaguari (Raio 50km)

### Porte
- Pequena (EPP) e Média (DEMAIS)
- Exclui MEI (foco em empresas com funcionários)

### Setores Prioritários (CNAEs)
1. **Serviços Administrativos** (82.11, 82.19, 82.20, 82.91)
2. **Atividades Jurídicas e Contábeis** (69.10, 69.20)
3. **Logística e Transporte** (49.30, 52.11, 52.50)
4. **Saúde e Clínicas** (86.10, 86.30, 86.50)
5. **Construção e Incorporação** (41.10, 41.20)

## 🚀 Instalação e Execução

### Pré-requisitos
- Python 3.8+
- pip

### Instalação

```bash
# Clone ou baixe o projeto
cd hunter-os

# Instale as dependências
pip install -r requirements.txt

# Execute a aplicação
streamlit run app.py
```

A aplicação estará disponível em `http://localhost:8501`

## 📁 Estrutura do Projeto

```
hunter-os/
├── app.py              # Aplicação principal Streamlit
├── utils.py            # Módulo de utilitários ETL
├── requirements.txt    # Dependências Python
├── cache.db           # Cache SQLite (gerado automaticamente)
└── README.md          # Este arquivo
```

## 🔧 Funcionalidades

### 1. EXTRACT (Extração)
- Integração com APIs públicas (BrasilAPI, CNPJ.ws)
- Cache local SQLite para evitar requisições repetidas
- Rate limiting e backoff exponencial

### 2. TRANSFORM (Transformação)
- Filtro de porte (exclui MEI)
- Normalização de nomes (Title Case)
- Formatação de telefones (XX) XXXXX-XXXX
- Limpeza de empresas inativas

### 3. ENRICH (Enriquecimento)
- Busca de site oficial via Google
- Identificação de redes sociais (Instagram/LinkedIn)
- Validação de tipo de telefone (fixo/celular)
- Detecção de formulário de contato

### 4. LOAD (Interface e Exportação)
- Dashboard interativo com métricas
- Filtros por cidade, CNAE e score
- Tabela de leads ordenável
- Exportação CSV para CRM
- Relatório de inteligência

## 🎯 Score ICP (0-100)

O algoritmo de scoring prioriza leads com maior potencial:

| Critério | Pontos |
|----------|--------|
| Base | 50 |
| Site/Instagram validado | +20 |
| Telefone celular (WhatsApp provável) | +15 |
| CNAE de Serviços (dor operacional alta) | +15 |
| Email com domínio próprio | +10 |

### Classificação
- 🔥 **Hot Lead**: 85+ pontos
- ⭐ **Qualificado**: 70-84 pontos
- 📊 **Potencial**: 55-69 pontos
- ❄️ **Frio**: < 55 pontos

## 📊 Interface

### Sidebar (Filtros)
- Seletor de cidades
- Seletor de setores (CNAE)
- Slider de score mínimo
- Opção de enriquecimento web

### Área Principal
- Métricas resumidas (Total, Hot Leads, Score Médio, WhatsApp)
- Tabela interativa de leads
- Detalhes do lead selecionado
- Exportação CSV e Relatório

## 📤 Exportação

### CSV para CRM
Gera arquivo compatível com:
- RD Station
- Pipedrive
- Waapi
- Outros CRMs

### Relatório de Inteligência
Inclui:
- Distribuição por cidade
- Distribuição por setor
- Análise de score ICP
- Canais de contato disponíveis

## ⚙️ Configurações Técnicas

- **Rate Limiting**: Implementado com `time.sleep` e `backoff`
- **Cache**: SQLite local com TTL de 24 horas
- **Tratamento de Erros**: Log e skip para não quebrar o pipeline
- **Código**: Modular, limpo e documentado

## 📝 Licença

Desenvolvido para prospecção inteligente de leads B2B.

---

**Hunter OS** - Encontre os melhores leads para seu negócio 🎯
