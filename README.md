# Hunter OS - B2B Prospecting (Refactor v2)

Pipeline inteligente para extracao, limpeza, enriquecimento e scoring de leads B2B.

## Objetivo
Extrair + filtrar + enriquecer + pontuar + armazenar leads, com reuso de enriquecimentos via **Enrichment Vault**.

## Arquitetura (Waterfall B2B)
A) EXTRACT (Casa dos Dados) -> B) STAGING (SQLite) -> C) CLEAN/DEDUP/FLAGS -> D) SCORE v1 -> E) ENRICH async (top X%) -> F) TECH DETECTION -> G) SCORE v2 -> H) VAULT + EXPORTS

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
SERPDEV_API_KEY=...
BUILTWITH_API_KEY=...
SEARCH_PROVIDER=serpdev
CACHE_TTL_HOURS=24
CONCURRENCY=10
TIMEOUT=5
HUNTER_DB_PATH=hunter.db
```

### Bulk export (opcional)
Para habilitar modo **bulk**, configure os endpoints:
```
CASA_DOS_DADOS_EXPORT_CREATE_URL=...
CASA_DOS_DADOS_EXPORT_STATUS_URL=.../{job_id}
CASA_DOS_DADOS_EXPORT_DOWNLOAD_URL=.../{job_id}
```

### Executar
```bash
streamlit run app.py
```

## Como funciona a pesquisa
- **Extract**: consulta Casa dos Dados por UF/municipio/CNAE com `situacao_cadastral=ATIVA`.
- **Cache por fingerprint**: o payload e resultados ficam cacheados por TTL.
- **Staging**: resultados brutos entram em `leads_raw`.
- **Cleaning**: normaliza telefones/emails, remove MEI, detecta contador-like, marca telefone repetido.
- **Score v1**: define top X% para enriquecimento.
- **Enrichment async**: busca site/redes via provider e detecta tecnologias por assinatura.
- **Score v2**: usa dados de enriquecimento e flags para pontuacao final.
- **Vault**: resultados enriquecidos ficam reutilizaveis em `enrichments`.

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
- Runs em `enrichment_runs`
- Erros por lead sem quebrar a run

## Notas
- `cache.db` e `hunter_cache.db` sao legados e podem ser ignorados.
- Provider de busca e chaves podem ser ajustados na aba **Config** do app.
