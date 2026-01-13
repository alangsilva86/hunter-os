"""
Extraction layer for Casa dos Dados.
"""

import hashlib
import json
import os
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

import backoff
import requests

from modules import storage

CASA_DOS_DADOS_BASE_URL = os.getenv(
    "CASA_DOS_DADOS_BASE_URL",
    "https://api.casadosdados.com.br/v5/cnpj/pesquisa",
)

SETORES_CNAE = {
    "Servicos Administrativos": ["8211300", "8219999", "8220200", "8291100"],
    "Atividades Juridicas e Contabeis": ["6910701", "6910702", "6920601", "6920602"],
    "Logistica e Transporte": ["4930202", "4930201", "5211701", "5212500"],
    "Saude e Clinicas": ["8630501", "8630502", "8630503", "8650001", "8650002"],
    "Construcao e Incorporacao": ["4110700", "4120400", "4121400", "4399103"],
    "Tecnologia e Software": ["6201501", "6201502", "6202300", "6203100"],
    "Comercio Varejista": ["4711301", "4711302", "4712100", "4713001"],
    "Alimentacao e Restaurantes": ["5611201", "5611202", "5611203", "5612100"],
    "Educacao": ["8511200", "8512100", "8513900", "8520100"],
    "Industria": ["1011201", "1012101", "2211100", "2511000"],
}

CIDADES_DISPONIVEIS = [
    "MARINGA",
    "SARANDI",
    "MARIALVA",
    "PAICANDU",
    "MANDAGUARI",
    "LONDRINA",
    "CURITIBA",
    "CASCAVEL",
    "FOZ DO IGUACU",
    "PONTA GROSSA",
    "SAO PAULO",
    "RIO DE JANEIRO",
    "BELO HORIZONTE",
    "BRASILIA",
    "SALVADOR",
]


class CasaDosDadosClient:
    def __init__(self, api_key: Optional[str] = None, timeout: int = 30):
        self.api_key = api_key or os.getenv("CASA_DOS_DADOS_API_KEY")
        self.timeout = timeout
        self.session = requests.Session()
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "HunterOS/2.0",
        }
        if self.api_key:
            headers["api-key"] = self.api_key
        self.session.headers.update(headers)

        self.export_create_url = os.getenv("CASA_DOS_DADOS_EXPORT_CREATE_URL")
        self.export_status_url = os.getenv("CASA_DOS_DADOS_EXPORT_STATUS_URL")
        self.export_download_url = os.getenv("CASA_DOS_DADOS_EXPORT_DOWNLOAD_URL")

    @backoff.on_exception(
        backoff.expo,
        (requests.RequestException,),
        max_tries=3,
        jitter=backoff.full_jitter,
    )
    def _post(self, url: str, payload: Dict[str, Any]) -> requests.Response:
        resp = self.session.post(url, json=payload, timeout=self.timeout)
        if resp.status_code in {429, 500, 502, 503, 504}:
            raise requests.RequestException(f"HTTP {resp.status_code}")
        return resp

    def _get(self, url: str) -> requests.Response:
        resp = self.session.get(url, timeout=self.timeout)
        if resp.status_code in {429, 500, 502, 503, 504}:
            raise requests.RequestException(f"HTTP {resp.status_code}")
        return resp

    def build_payload(
        self,
        uf: str,
        municipios: List[str],
        cnaes: List[str],
        excluir_mei: bool,
        com_telefone: bool,
        com_email: bool,
        pagina: int,
        limite: int,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "situacao_cadastral": ["ATIVA"],
            "limite": min(limite, 1000),
            "pagina": pagina,
        }
        if uf:
            payload["uf"] = [uf.lower()]
        if municipios:
            payload["municipio"] = [m.lower().replace("_", " ") for m in municipios]
        if cnaes:
            payload["codigo_atividade_principal"] = cnaes
            payload["incluir_atividade_secundaria"] = True
        if excluir_mei:
            payload["mei"] = {"excluir_optante": True}
        if com_telefone or com_email:
            payload["mais_filtros"] = {}
            if com_telefone:
                payload["mais_filtros"]["com_telefone"] = True
            if com_email:
                payload["mais_filtros"]["com_email"] = True
        return payload

    def pagination_search(self, payload: Dict[str, Any], tipo_resultado: str = "completo") -> Tuple[List[Dict[str, Any]], int]:
        all_items: List[Dict[str, Any]] = []
        total = 0
        pagina = payload.get("pagina", 1)
        limite = payload.get("limite", 100)

        while True:
            payload["pagina"] = pagina
            resp = self._post(f"{CASA_DOS_DADOS_BASE_URL}?tipo_resultado={tipo_resultado}", payload)
            if resp.status_code != 200:
                raise RuntimeError(f"Casa dos Dados erro {resp.status_code}: {resp.text[:200]}")
            data = resp.json()
            total = data.get("total", 0)
            items = data.get("cnpjs", [])
            if not items:
                break
            all_items.extend(items)
            if len(items) < limite or len(all_items) >= total:
                break
            pagina += 1
            time.sleep(0.3)
        return all_items, total

    def bulk_export(self, payload: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], int]:
        if not (self.export_create_url and self.export_status_url and self.export_download_url):
            raise RuntimeError("Bulk export endpoints nao configurados")

        create_resp = self._post(self.export_create_url, payload)
        if create_resp.status_code not in {200, 201, 202}:
            raise RuntimeError(f"Erro ao criar export: {create_resp.status_code} {create_resp.text[:200]}")
        data = create_resp.json()
        job_id = data.get("job_id") or data.get("id") or data.get("export_id")
        if not job_id:
            raise RuntimeError("Nao foi possivel obter job_id do export")

        status_url = self.export_status_url.format(job_id=job_id)
        download_url = self.export_download_url.format(job_id=job_id)

        status = "pending"
        for _ in range(60):
            status_resp = self._get(status_url)
            if status_resp.status_code != 200:
                raise RuntimeError(f"Erro status export: {status_resp.status_code}")
            status_data = status_resp.json()
            status = str(status_data.get("status", "")).lower()
            if status in {"done", "finished", "ready", "completed"}:
                break
            if status in {"failed", "error"}:
                raise RuntimeError("Export falhou")
            time.sleep(5)

        if status not in {"done", "finished", "ready", "completed"}:
            raise RuntimeError("Timeout aguardando export")

        download_resp = self._get(download_url)
        if download_resp.status_code != 200:
            raise RuntimeError(f"Erro download export: {download_resp.status_code}")

        try:
            export_data = download_resp.json()
            items = export_data.get("cnpjs", export_data)
            if isinstance(items, dict):
                items = items.get("cnpjs", [])
        except ValueError:
            # CSV fallback
            lines = download_resp.text.splitlines()
            items = _parse_csv_to_dicts(lines)

        total = len(items)
        return items, total


def _parse_csv_to_dicts(lines: List[str]) -> List[Dict[str, Any]]:
    if not lines:
        return []
    header = [h.strip() for h in lines[0].split(",")]
    items = []
    for row in lines[1:]:
        parts = [p.strip() for p in row.split(",")]
        items.append(dict(zip(header, parts)))
    return items


def normalize_casa_dos_dados(record: Dict[str, Any]) -> Dict[str, Any]:
    endereco = record.get("endereco", {}) or {}
    situacao = record.get("situacao_cadastral", {}) or {}
    porte = record.get("porte_empresa", {}) or {}
    atividade = record.get("atividade_principal", {}) or {}

    telefones = record.get("contato_telefonico", []) or []
    emails = record.get("contato_email", []) or []

    telefone = ""
    if telefones:
        tel = telefones[0]
        telefone = f"{tel.get('ddd', '')}{tel.get('numero', '')}"

    email = ""
    if emails:
        email = emails[0].get("email", "")

    return {
        "cnpj": record.get("cnpj", ""),
        "cnpj_raiz": record.get("cnpj_raiz", ""),
        "razao_social": record.get("razao_social", ""),
        "nome_fantasia": record.get("nome_fantasia", "") or record.get("razao_social", ""),
        "cnae_fiscal": atividade.get("codigo", "") if isinstance(atividade, dict) else "",
        "cnae_fiscal_descricao": atividade.get("descricao", "") if isinstance(atividade, dict) else "",
        "ddd_telefone_1": telefone,
        "telefones": telefones,
        "email": email,
        "emails": emails,
        "logradouro": endereco.get("logradouro", ""),
        "numero": endereco.get("numero", ""),
        "complemento": endereco.get("complemento", ""),
        "bairro": endereco.get("bairro", ""),
        "municipio": endereco.get("municipio", ""),
        "uf": endereco.get("uf", ""),
        "cep": endereco.get("cep", ""),
        "porte": porte.get("descricao", "") if isinstance(porte, dict) else record.get("porte", ""),
        "natureza_juridica": record.get("descricao_natureza_juridica", ""),
        "capital_social": record.get("capital_social", 0),
        "data_inicio_atividade": record.get("data_abertura", ""),
        "situacao_cadastral": situacao.get("descricao", "") if isinstance(situacao, dict) else "ATIVA",
        "matriz_filial": record.get("matriz_filial", ""),
        "quadro_societario": record.get("quadro_societario", []),
        "fonte": "casa_dos_dados",
    }


def _fingerprint(payload: Dict[str, Any]) -> str:
    payload_str = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(payload_str.encode("utf-8")).hexdigest()


def extract_leads(
    uf: str,
    municipios: List[str],
    cnaes: List[str],
    excluir_mei: bool,
    com_telefone: bool,
    com_email: bool,
    limite: int,
    mode: str,
    cache_ttl_hours: int,
) -> Tuple[List[Dict[str, Any]], int, str]:
    client = CasaDosDadosClient()
    if not client.api_key:
        raise RuntimeError("CASA_DOS_DADOS_API_KEY nao configurada")

    payload = client.build_payload(
        uf=uf,
        municipios=municipios,
        cnaes=cnaes,
        excluir_mei=excluir_mei,
        com_telefone=com_telefone,
        com_email=com_email,
        pagina=1,
        limite=min(limite, 1000),
    )
    fingerprint = _fingerprint(payload)
    cached = storage.extract_cache_get(fingerprint)
    source = f"casa_dos_dados:{fingerprint}"

    if cached:
        cached_leads = storage.fetch_leads_raw_by_source(source)
        if cached_leads:
            return cached_leads, cached.get("result_count", len(cached_leads)), source

    if mode == "bulk":
        try:
            items, total = client.bulk_export(payload)
        except Exception:
            mode = "pagination"
            items, total = client.pagination_search(payload)
    else:
        items, total = client.pagination_search(payload)

    # trim to limite
    items = items[:limite]
    total = min(total, limite)

    normalized = [normalize_casa_dos_dados(item) for item in items]
    storage.insert_leads_raw(normalized, source)
    storage.extract_cache_set(fingerprint, payload, result_count=len(normalized), ttl_hours=cache_ttl_hours)
    return normalized, total, source


def get_setores_disponiveis() -> List[str]:
    return list(SETORES_CNAE.keys())


def get_cidades_disponiveis() -> List[str]:
    return CIDADES_DISPONIVEIS
