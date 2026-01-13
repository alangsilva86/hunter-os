# API Casa dos Dados - Documentação

## Endpoint de Pesquisa Avançada

**URL:** `https://api.casadosdados.com.br/v5/cnpj/pesquisa`
**Método:** POST

## Autenticação

**Header:** `api-key: <sua-api-key>`

A API requer uma chave de API (api-key) no header da requisição.

## Parâmetros de Query

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| tipo_resultado | enum | `simples` ou `completo`. Simples retorna cnpj, razão social, nome fantasia e situação cadastral. |

## Parâmetros do Body (JSON)

| Campo | Tipo | Descrição |
|-------|------|-----------|
| cnpj | array[string] | Lista de CNPJs específicos |
| codigo_atividade_principal | array[string] | Códigos CNAE (ex: ["7112000","9602501"]) |
| incluir_atividade_secundaria | boolean | Incluir atividades secundárias na busca |
| situacao_cadastral | array[string] | ATIVA, BAIXADA, INAPTA, NULA, SUSPENSA |
| uf | array[string] | Estados (ex: ["sp","rj","mg"]) |
| municipio | array[string] | Municípios (ex: ["sao paulo","guarulhos"]) |
| mei.excluir_optante | boolean | Excluir MEI |
| mais_filtros.com_telefone | boolean | Apenas com telefone |
| mais_filtros.com_email | boolean | Apenas com email |
| limite | integer | 1-1000 |
| pagina | integer | >= 1 |

## Exemplo de Requisição

```bash
curl --location --request POST 'https://api.casadosdados.com.br/v5/cnpj/pesquisa?tipo_resultado=completo' \
--header 'api-key: <api-key>' \
--header 'Content-Type: application/json' \
--data-raw '{
    "codigo_atividade_principal": ["8211300"],
    "situacao_cadastral": ["ATIVA"],
    "uf": ["pr"],
    "municipio": ["maringa"],
    "mei": {
        "excluir_optante": true
    },
    "mais_filtros": {
        "com_telefone": true
    },
    "limite": 50,
    "pagina": 1
}'
```

## Resposta

```json
{
    "total": 0,
    "cnpjs": [
        {
            "cnpj": "33000167004794",
            "cnpj_raiz": "33000167",
            "razao_social": "EMPRESA EXEMPLO",
            "nome_fantasia": "NOME FANTASIA",
            "situacao_cadastral": {...},
            "endereco": {...},
            "capital_social": 0
        }
    ]
}
```

## Observação Importante

A API da Casa dos Dados é **PAGA** e requer uma chave de API válida.
O erro 403 indica que a requisição foi bloqueada por falta de autenticação ou limite excedido.

## Alternativas Gratuitas

1. **BrasilAPI** - `https://brasilapi.com.br/api/cnpj/v1/{cnpj}` - Consulta CNPJ individual
2. **CNPJ.ws** - `https://publica.cnpj.ws/cnpj/{cnpj}` - 3 req/min
3. **ReceitaWS** - `https://receitaws.com.br/v1/cnpj/{cnpj}` - 3 req/min

As APIs gratuitas só permitem consulta por CNPJ específico, não por região/CNAE.
