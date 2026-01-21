# Hunter OS - B2B Prospecting (Refactor v2)

Pipeline inteligente para extracao, limpeza, enriquecimento e scoring de leads B2B.

## Objetivo
Extrair + filtrar + enriquecer + pontuar + armazenar leads, com reuso de enriquecimentos via **Enrichment Vault**.

## Arquitetura (Waterfall B2B)
A) EXTRACT (Casa dos Dados) -> B) STAGING (SQLite) -> C) CLEAN/DEDUP/FLAGS -> D) SCORE v1 -> E) ENRICH async (top X%) -> F) TECH DETECTION -> G) SCORE v2 -> H) VAULT + EXPORTS

Fluxo Casa dos Dados (quando exportar tudo):
1) Pesquisa v5 -> 2) Gerar arquivo v5 -> 3) Listar solicitacoes v4 -> 4) Consultar link v4 public -> 5) Import/Process/Enrich/Export

## Estrutura do projeto
```
hunter-os/
├── app.py
├── modules/
│   ├── data_sources.py
│   ├── storage.py
│   ├── cleaning.py
│   ├── scoring.py
│   ├── enrichment_async.py
│   ├── providers.py
│   └── jobs.py
├── requirements.txt
├── hunter.db                # gerado automaticamente
└── README.md
```

## Setup

### Requisitos
- Python 3.9+
- pip

### Instalacao
```bash
pip install -r requirements.txt
```

### Variaveis de ambiente
Use `.env` ou export no shell:

```
CASA_DOS_DADOS_API_KEY=...
SERPER_API_KEY=...
SEARCH_PROVIDER=serper
CACHE_TTL_HOURS=24
CONCURRENCY=10
TIMEOUT=5
HUNTER_DB_PATH=hunter.db
ENABLE_PLAYWRIGHT=0
PLAYWRIGHT_TIMEOUT_MS=8000
BASIC_AUTH_USER=admin
BASIC_AUTH_PASS=change-me
MAX_EXPORT_ROWS=5000
PERSON_BATCH_LIMIT=50
AUTO_RESUME_RUNS=1
BULK_POLL_MAX_ATTEMPTS=30
BULK_POLL_BACKOFF_START=2
BULK_POLL_BACKOFF_MAX=10
CASA_EXPORT_POLL_MAX_ATTEMPTS=20
CASA_EXPORT_POLL_BACKOFF_SECONDS=2
```

### Export Casa dos Dados
O app usa os endpoints oficiais (v5/v4) por padrao. Se precisar sobrescrever:
```
CASA_DOS_DADOS_EXPORT_CREATE_URL=...
CASA_DOS_DADOS_EXPORT_LIST_URL=...
CASA_DOS_DADOS_EXPORT_STATUS_V4_PUBLIC_URL=...
```

### Executar
```bash
uvicorn server:app --host 0.0.0.0 --port 8000
```

## Como funciona a pesquisa
- **Extract**: consulta Casa dos Dados por UF/municipio/CNAE com `situacao_cadastral=ATIVA` e limite duro.
- **Cache por fingerprint**: reusa resultados ja extraidos sem novas chamadas.
- **Staging**: resultados brutos entram em `leads_raw` (idempotente por run).
- **Cleaning**: normaliza telefones/emails, remove MEI, detecta contador-like, marca telefone repetido.
- **Score v1**: define top X% para enriquecimento.
- **Enrichment async**: busca site/redes via provider e detecta tecnologias por fingerprints (HTML/headers/cookies).
- **Score v2**: usa dados de enriquecimento e flags para pontuacao final.
- **Vault**: resultados enriquecidos ficam reutilizaveis em `enrichments`.

## UI web leve
- **Mission**: inicia caçadas e monitora etapas (probe, bulk, pipeline).
- **Vault**: filtra leads enriquecidos e exporta CSV.
- **Person**: busca socios e permite importacao no Vault.
- **Exports**: monitora exports da Casa dos Dados e faz download.
- **Recovery**: reprocessa CSVs ja baixados.
- **Diagnostics**: logs, passos, chamadas de API e erros.
- **Config**: define webhook basico.

## Enrichment Vault
Tabela `enrichments` usa UPSERT por CNPJ para reaproveitar enriquecimentos antigos.

## Exportacao
Exports segmentados:
- Hot
- Hot + WhatsApp
- Sem contador-like
- Com site + tech detectado

Colunas base: cnpj, razao_social, cidade, cnae, score, contact_quality, site, instagram, linkedin, google_maps_url.

## Observabilidade
- Logs estruturados em `logs`
- Runs em `runs`
- Etapas em `run_steps`
- Chamadas em `api_calls`
- Erros em `errors`
- Snapshots de exports em `exports_status_snapshots`

## Notas
- `cache.db` e `hunter_cache.db` sao legados e podem ser ignorados.
- Provider de busca e chaves podem ser ajustados na aba **Config** do app.
- Modulos legacy no root (`data_sources.py`, `utils.py`, `lead_processing.py`) sao wrappers deprecated.
