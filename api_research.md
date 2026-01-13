# Pesquisa de APIs para Hunter OS

## 1. CNPJ.ws - API Pública

**Endpoint:** `https://publica.cnpj.ws/cnpj/{cnpj}`
**Método:** GET
**Limite:** 3 consultas por minuto
**Formato:** JSON

### Campos retornados:
- cnpj_raiz
- razao_social
- capital_social
- responsavel_federativo
- atualizado_em
- porte
- natureza_juridica
- qualificacao_do_responsavel
- socios
- simples
- estabelecimento (dados do estabelecimento)

### Limitações:
- Apenas consulta por CNPJ específico
- 3 requisições/minuto
- Não permite busca por CNAE ou cidade

## 2. BrasilAPI

**Endpoint:** `https://brasilapi.com.br/api/cnpj/v1/{cnpj}`
**Limite:** Rate limit não especificado
**Formato:** JSON

## 3. ReceitaWS

**Endpoint:** `https://receitaws.com.br/v1/cnpj/{cnpj}`
**Limite:** 3 consultas/minuto (gratuito)

## Solução para busca por região/CNAE:

Como nenhuma API gratuita permite busca por CNAE/cidade, precisamos:

1. **Base de dados da Receita Federal** - Dados públicos disponíveis em:
   - https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj

2. **Alternativa:** Usar lista de CNPJs conhecidos da região e enriquecer via APIs


## 4. Casa dos Dados - Pesquisa Avançada

**URL:** https://casadosdados.com.br/solucao/cnpj/pesquisa-avancada
**Base:** 69.211.712 empresas identificadas

### Filtros disponíveis:
- Razão Social ou Nome Fantasia
- Atividade Principal (CNAE)
- Incluir Atividade Secundária
- Natureza Jurídica
- Situação Cadastral (ATIVA, BAIXADA, INAPTA, SUSPENSA, NULA)
- Estado (UF)
- Município
- Bairro
- CEP
- DDD
- Data de Abertura
- Capital Social
- Somente MEI / Excluir MEI
- Somente matriz / filial
- Com contato de telefone / Somente fixo / Somente celular
- Com e-mail

### Limitações:
- Limitado a 20 resultados na versão gratuita
- API disponível na plataforma paga

### Dados por estado (PR - Paraná):
- 4.700.627 empresas

## Estratégia de Implementação:

1. **Web Scraping da Casa dos Dados** - Buscar empresas por CNAE + Município
2. **Enriquecimento via APIs** - CNPJ.ws, BrasilAPI para dados detalhados
3. **Cache local** - Armazenar resultados para evitar requisições repetidas
