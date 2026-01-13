"""
Extraction layer for Casa dos Dados.
"""

import csv
import hashlib
import json
import os
import re
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

import backoff
import pandas as pd
import requests

from modules import storage

CASA_DOS_DADOS_BASE_URL = os.getenv(
    "CASA_DOS_DADOS_BASE_URL",
    "https://api.casadosdados.com.br/v5/cnpj/pesquisa",
)
CASA_DOS_DADOS_EXPORT_CREATE_URL = os.getenv(
    "CASA_DOS_DADOS_EXPORT_CREATE_URL",
    "https://api.casadosdados.com.br/v5/cnpj/pesquisa/arquivo",
)
CASA_DOS_DADOS_EXPORT_LIST_URL = os.getenv(
    "CASA_DOS_DADOS_EXPORT_LIST_URL",
    "https://api.casadosdados.com.br/v4/cnpj/pesquisa/arquivo",
)
CASA_DOS_DADOS_EXPORT_STATUS_V4_PUBLIC_URL = os.getenv(
    "CASA_DOS_DADOS_EXPORT_STATUS_V4_PUBLIC_URL",
    "https://api.casadosdados.com.br/v4/public/cnpj/pesquisa/arquivo/{arquivo_uuid}",
)
CASA_EXPORT_LINK_TTL_SECONDS = 3600

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

class CasaDosDadosBalanceError(RuntimeError):
    pass


def _request_id_from_response(resp: requests.Response) -> Optional[str]:
    headers = resp.headers or {}
    for key in (
        "x-request-id",
        "x-correlation-id",
        "x-amzn-requestid",
        "x-amz-request-id",
        "request-id",
    ):
        value = headers.get(key) or headers.get(key.title())
        if value:
            return str(value)
    return None


def _response_excerpt(resp: requests.Response, limit: int = 200) -> str:
    text = (resp.text or "").strip().replace("\n", " ")
    return text[:limit] if text else ""


def _extract_response_message(resp: requests.Response) -> str:
    try:
        payload = resp.json()
    except ValueError:
        payload = {}
    if isinstance(payload, dict):
        message = payload.get("mensagem") or payload.get("message") or payload.get("erro") or ""
        if message:
            return str(message)
    return (resp.text or "").strip()


def _is_no_balance(resp: requests.Response) -> bool:
    if resp.status_code != 403:
        return False
    message = _extract_response_message(resp).lower()
    return "sem saldo" in message or "saldo insuficiente" in message or "insufficient balance" in message


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
    def _post(
        self,
        url: str,
        payload: Dict[str, Any],
        run_id: Optional[str] = None,
        step_name: str = "extract",
        payload_fingerprint: Optional[str] = None,
    ) -> requests.Response:
        start = time.time()
        resp = self.session.post(url, json=payload, timeout=self.timeout)
        duration_ms = int((time.time() - start) * 1000)
        if run_id:
            storage.record_api_call(
                run_id=run_id,
                step_name=step_name,
                method="POST",
                url=url,
                status_code=resp.status_code,
                duration_ms=duration_ms,
                payload_fingerprint=payload_fingerprint,
                request_id=_request_id_from_response(resp),
                response_excerpt=_response_excerpt(resp),
            )
        if resp.status_code in {429, 500, 502, 503, 504}:
            raise requests.RequestException(f"HTTP {resp.status_code}")
        return resp

    def _get(
        self,
        url: str,
        run_id: Optional[str] = None,
        step_name: str = "extract",
        payload_fingerprint: Optional[str] = None,
    ) -> requests.Response:
        start = time.time()
        resp = self.session.get(url, timeout=self.timeout)
        duration_ms = int((time.time() - start) * 1000)
        if run_id:
            storage.record_api_call(
                run_id=run_id,
                step_name=step_name,
                method="GET",
                url=url,
                status_code=resp.status_code,
                duration_ms=duration_ms,
                payload_fingerprint=payload_fingerprint,
                request_id=_request_id_from_response(resp),
                response_excerpt=_response_excerpt(resp),
            )
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
                if _is_no_balance(resp):
                    raise CasaDosDadosBalanceError(
                        "Casa dos Dados: sem saldo para a operacao. Recarregue creditos e tente novamente."
                    )
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
            if _is_no_balance(create_resp):
                raise CasaDosDadosBalanceError(
                    "Casa dos Dados: sem saldo para a operacao. Recarregue creditos e tente novamente."
                )
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
                if _is_no_balance(status_resp):
                    raise CasaDosDadosBalanceError(
                        "Casa dos Dados: sem saldo para a operacao. Recarregue creditos e tente novamente."
                    )
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
            if _is_no_balance(download_resp):
                raise CasaDosDadosBalanceError(
                    "Casa dos Dados: sem saldo para a operacao. Recarregue creditos e tente novamente."
                )
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


def _normalize_key(key: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", str(key or "").strip().lower())


def _pick_value(data: Dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = data.get(key)
        if value is not None and str(value).strip() != "":
            return str(value).strip()
    return ""


def normalize_export_row(row: Dict[str, Any]) -> Dict[str, Any]:
    normalized = {_normalize_key(k): v for k, v in row.items()}
    cnpj = _pick_value(
        normalized,
        "cnpj",
        "cnpj_completo",
    )
    cnpj_basico = _pick_value(normalized, "cnpj_basico", "cnpj_base")
    cnpj_ordem = _pick_value(normalized, "cnpj_ordem")
    cnpj_dv = _pick_value(normalized, "cnpj_dv", "cnpj_digito")
    if not cnpj and cnpj_basico and cnpj_ordem and cnpj_dv:
        cnpj = f"{cnpj_basico}{cnpj_ordem}{cnpj_dv}"

    razao_social = _pick_value(normalized, "razao_social", "razao")
    nome_fantasia = _pick_value(normalized, "nome_fantasia", "fantasia")
    if not razao_social:
        razao_social = nome_fantasia or cnpj
    if not nome_fantasia:
        nome_fantasia = razao_social
    cnae = _pick_value(normalized, "cnae_fiscal", "cnae_principal", "cnae")
    cnae_desc = _pick_value(normalized, "cnae_fiscal_descricao", "cnae_descricao", "cnae_desc")

    ddd = _pick_value(normalized, "ddd", "ddd1")
    telefone_raw = _pick_value(normalized, "telefone", "telefone1", "telefone_1", "telefone_principal")
    if ddd and telefone_raw and not telefone_raw.startswith(ddd):
        ddd_telefone_1 = f"{ddd}{telefone_raw}"
    else:
        ddd_telefone_1 = telefone_raw

    telefones = []
    if telefone_raw:
        telefones.append({"ddd": ddd, "numero": telefone_raw})

    email = _pick_value(normalized, "email", "email1", "email_principal")
    emails = [{"email": email}] if email else []

    endereco = {
        "logradouro": _pick_value(normalized, "logradouro", "endereco", "rua"),
        "numero": _pick_value(normalized, "numero", "numero_endereco"),
        "complemento": _pick_value(normalized, "complemento"),
        "bairro": _pick_value(normalized, "bairro"),
        "municipio": _pick_value(normalized, "municipio", "cidade"),
        "uf": _pick_value(normalized, "uf", "estado"),
        "cep": _pick_value(normalized, "cep"),
    }

    def _to_float(value: str) -> float:
        if not value:
            return 0.0
        value = value.replace(".", "").replace(",", ".")
        try:
            return float(value)
        except ValueError:
            return 0.0

    porte = _pick_value(normalized, "porte", "porte_empresa")
    natureza_juridica = _pick_value(normalized, "natureza_juridica")
    situacao = _pick_value(normalized, "situacao_cadastral")
    matriz_filial = _pick_value(normalized, "matriz_filial")

    return {
        "cnpj": cnpj,
        "cnpj_raiz": _pick_value(normalized, "cnpj_raiz") or (cnpj[:8] if cnpj else ""),
        "razao_social": razao_social,
        "nome_fantasia": nome_fantasia,
        "cnae_fiscal": cnae,
        "cnae_fiscal_descricao": cnae_desc,
        "ddd_telefone_1": ddd_telefone_1,
        "telefones": telefones,
        "email": email,
        "emails": emails,
        "logradouro": endereco["logradouro"],
        "numero": endereco["numero"],
        "complemento": endereco["complemento"],
        "bairro": endereco["bairro"],
        "municipio": endereco["municipio"],
        "uf": endereco["uf"],
        "cep": endereco["cep"],
        "porte": porte,
        "natureza_juridica": natureza_juridica,
        "capital_social": _to_float(_pick_value(normalized, "capital_social")),
        "data_inicio_atividade": _pick_value(normalized, "data_abertura", "data_inicio_atividade"),
        "situacao_cadastral": situacao or "ATIVA",
        "matriz_filial": matriz_filial,
        "quadro_societario": [],
        "fonte": "casa_dos_dados_export",
    }


def parse_export_csv(file_path: str) -> List[Dict[str, Any]]:
    leads = []
    with open(file_path, "r", encoding="utf-8-sig", newline="") as handle:
        sample = handle.read(4096)
        handle.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample)
        except csv.Error:
            dialect = csv.excel
        reader = csv.DictReader(handle, dialect=dialect)
        for row in reader:
            leads.append(normalize_export_row(row))
    return leads


def parse_excel_file(file_path: str, cnpj_column: Optional[str] = None) -> List[Dict[str, Any]]:
    df = pd.read_excel(file_path, dtype=str, engine="openpyxl")
    if df.empty:
        return []
    if cnpj_column:
        if cnpj_column not in df.columns:
            raise RuntimeError(f"Coluna '{cnpj_column}' nao encontrada no arquivo.")
        df = df.rename(columns={cnpj_column: "cnpj"})

    rows = df.to_dict(orient="records")
    leads: List[Dict[str, Any]] = []

    def _is_blank(value: Any) -> bool:
        if value is None:
            return True
        text = str(value).strip()
        return text == "" or text.lower() in {"nan", "none"}

    for row in rows:
        normalized = normalize_export_row(row)
        cnpj_digits = re.sub(r"\D", "", str(normalized.get("cnpj") or ""))
        if not cnpj_digits:
            continue
        normalized["cnpj"] = cnpj_digits
        if _is_blank(normalized.get("razao_social")):
            normalized["razao_social"] = cnpj_digits
        if _is_blank(normalized.get("nome_fantasia")):
            normalized["nome_fantasia"] = normalized["razao_social"]
        leads.append(normalized)
    return leads


def _fingerprint(payload: Dict[str, Any]) -> str:
    payload_str = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(payload_str.encode("utf-8")).hexdigest()


def search_v5(
    payload: Dict[str, Any],
    limit: int,
    page_size: int = 200,
    run_id: Optional[str] = None,
    tipo_resultado: str = "completo",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    client = CasaDosDadosClient()
    items: List[Dict[str, Any]] = []
    request_ids: List[str] = []
    timings_ms: List[int] = []
    pages_processed = 0
    total_encontrado = 0
    descartados_por_limite = 0

    page_size = max(1, min(int(page_size), 1000, int(limit)))
    payload = dict(payload)
    payload["limite"] = page_size

    fingerprint = _fingerprint(payload)

    pagina = int(payload.get("pagina") or 1)
    while len(items) < limit:
        payload["pagina"] = pagina
        start = time.time()
        resp = client._post(
            f"{CASA_DOS_DADOS_BASE_URL}?tipo_resultado={tipo_resultado}",
            payload,
            run_id=run_id,
            step_name="search_v5",
            payload_fingerprint=fingerprint,
        )
        duration_ms = int((time.time() - start) * 1000)
        timings_ms.append(duration_ms)

        if resp.status_code != 200:
            if _is_no_balance(resp):
                raise CasaDosDadosBalanceError(
                    "Casa dos Dados: sem saldo para a operacao. Recarregue creditos e tente novamente."
                )
            raise RuntimeError(f"Casa dos Dados erro {resp.status_code}: {resp.text[:200]}")

        request_id = _request_id_from_response(resp)
        if request_id:
            request_ids.append(request_id)

        data = resp.json()
        total_encontrado = total_encontrado or int(data.get("total") or 0)
        page_items = data.get("cnpjs", []) or []
        pages_processed += 1

        if not page_items:
            break

        remaining = limit - len(items)
        if remaining <= 0:
            break

        if len(page_items) > remaining:
            items.extend(page_items[:remaining])
            descartados_por_limite += len(page_items) - remaining
        else:
            items.extend(page_items)

        if len(page_items) < page_size:
            break
        if len(items) >= limit:
            break
        if total_encontrado and len(items) >= total_encontrado:
            break

        pagina += 1
        time.sleep(0.3)

    if total_encontrado:
        descartados_por_limite = max(descartados_por_limite, total_encontrado - len(items))

    telemetry = {
        "total_encontrado": total_encontrado,
        "pages_processed": pages_processed,
        "itens_coletados": len(items),
        "itens_descartados_por_limite": descartados_por_limite,
        "page_size": page_size,
        "request_ids": request_ids,
        "durations_ms": timings_ms,
        "payload_fingerprint": fingerprint,
    }
    return items, telemetry


def export_create_v5(
    payload: Dict[str, Any],
    run_id: Optional[str] = None,
    total_linhas: int = 0,
    nome: Optional[str] = None,
    enviar_para: Optional[List[str]] = None,
) -> Dict[str, Any]:
    client = CasaDosDadosClient()
    export_payload = {
        "total_linhas": total_linhas,
        "nome": nome or f"hunter_os_{int(time.time())}",
        "tipo": "csv",
        "enviar_para": enviar_para or [],
        "pesquisa": payload,
    }
    fingerprint = _fingerprint(payload)
    resp = client._post(
        CASA_DOS_DADOS_EXPORT_CREATE_URL,
        export_payload,
        run_id=run_id,
        step_name="export_create_v5",
        payload_fingerprint=fingerprint,
    )
    if resp.status_code not in {200, 201, 202}:
        if _is_no_balance(resp):
            raise CasaDosDadosBalanceError(
                "Casa dos Dados: sem saldo para a operacao. Recarregue creditos e tente novamente."
            )
        raise RuntimeError(f"Erro ao criar export: {resp.status_code} {resp.text[:200]}")

    data = resp.json()
    arquivo_uuid = data.get("arquivo_uuid") or data.get("arquivoUUID") or data.get("id") or ""
    if not arquivo_uuid:
        raise RuntimeError("Nao foi possivel obter arquivo_uuid do export")

    storage.create_casa_export(
        run_id=run_id or "",
        arquivo_uuid=arquivo_uuid,
        payload_fingerprint=fingerprint,
        status="created",
        total_linhas=total_linhas,
    )
    return {
        "arquivo_uuid": arquivo_uuid,
        "mensagem": data.get("mensagem"),
        "status_code": resp.status_code,
        "payload_fingerprint": fingerprint,
    }


def export_list_v4(page: int = 1, run_id: Optional[str] = None) -> List[Dict[str, Any]]:
    client = CasaDosDadosClient()
    url = f"{CASA_DOS_DADOS_EXPORT_LIST_URL}?pagina={page}"
    resp = client._get(url, run_id=run_id, step_name="export_list_v4")
    if resp.status_code != 200:
        if _is_no_balance(resp):
            raise CasaDosDadosBalanceError(
                "Casa dos Dados: sem saldo para a operacao. Recarregue creditos e tente novamente."
            )
        raise RuntimeError(f"Erro ao listar exports: {resp.status_code} {resp.text[:200]}")
    data = resp.json()
    items = data if isinstance(data, list) else data.get("data", data.get("items", [])) or []
    for item in items:
        arquivo_uuid = item.get("arquivo_uuid") or item.get("arquivoUUID") or ""
        status = str(item.get("status") or "").lower()
        storage.record_export_snapshot(
            run_id=run_id,
            arquivo_uuid=arquivo_uuid,
            status=status,
            quantidade=item.get("quantidade"),
            quantidade_solicitada=item.get("quantidade_solicitada"),
            raw=item,
        )
        if arquivo_uuid:
            storage.update_casa_export(
                arquivo_uuid,
                status=status,
                updated_at=storage._utcnow(),
            )
    return items


def export_poll_v4_public(
    arquivo_uuid: str,
    run_id: Optional[str] = None,
    max_attempts: int = 20,
    backoff_seconds: int = 2,
    include_corpo: bool = True,
) -> Dict[str, Any]:
    client = CasaDosDadosClient()
    url = CASA_DOS_DADOS_EXPORT_STATUS_V4_PUBLIC_URL.format(arquivo_uuid=arquivo_uuid)
    if include_corpo:
        url = f"{url}?corpo"

    for attempt in range(max_attempts):
        resp = client._get(url, run_id=run_id, step_name="export_poll_v4_public")
        if resp.status_code == 202:
            storage.update_casa_export(arquivo_uuid, status="processando", updated_at=storage._utcnow())
            storage.record_export_snapshot(
                run_id=run_id,
                arquivo_uuid=arquivo_uuid,
                status="processando",
                quantidade=None,
                quantidade_solicitada=None,
                raw={"status_code": 202},
            )
            time.sleep(backoff_seconds * (attempt + 1))
            continue
        if resp.status_code == 200:
            data = resp.json()
            link = data.get("link") or data.get("url") or ""
            expires_at = time.time() + CASA_EXPORT_LINK_TTL_SECONDS
            storage.update_casa_export(
                arquivo_uuid,
                status="processado",
                link=link,
                expires_at=time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(expires_at)),
                updated_at=storage._utcnow(),
            )
            storage.record_export_snapshot(
                run_id=run_id,
                arquivo_uuid=arquivo_uuid,
                status="processado",
                quantidade=None,
                quantidade_solicitada=None,
                raw=data,
            )
            return {"status": "processado", "link": link, "expires_at": expires_at}
        if resp.status_code == 404:
            storage.update_casa_export(arquivo_uuid, status="nao_encontrado", updated_at=storage._utcnow())
            raise RuntimeError("Arquivo nao encontrado na Casa dos Dados")
        if _is_no_balance(resp):
            raise CasaDosDadosBalanceError(
                "Casa dos Dados: sem saldo para a operacao. Recarregue creditos e tente novamente."
            )
        raise RuntimeError(f"Erro ao consultar export: {resp.status_code} {resp.text[:200]}")

    storage.update_casa_export(arquivo_uuid, status="timeout", updated_at=storage._utcnow())
    raise RuntimeError("Timeout aguardando processamento do export")


def export_download(
    link: str,
    arquivo_uuid: str,
    run_id: Optional[str] = None,
    dest_dir: str = "exports_files",
) -> Dict[str, Any]:
    if not link:
        raise RuntimeError("Link de download invalido")

    os.makedirs(dest_dir, exist_ok=True)
    file_path = os.path.join(dest_dir, f"{arquivo_uuid}.csv")

    start = time.time()
    resp = requests.get(link, stream=True, timeout=60)
    duration_ms = int((time.time() - start) * 1000)
    if run_id:
        storage.record_api_call(
            run_id=run_id,
            step_name="export_download",
            method="GET",
            url=link,
            status_code=resp.status_code,
            duration_ms=duration_ms,
            payload_fingerprint=None,
            request_id=_request_id_from_response(resp),
            response_excerpt=_response_excerpt(resp),
        )
    if resp.status_code != 200:
        raise RuntimeError(f"Erro download export: {resp.status_code}")

    hasher = hashlib.sha256()
    size = 0
    with open(file_path, "wb") as handle:
        for chunk in resp.iter_content(chunk_size=1024 * 128):
            if not chunk:
                continue
            handle.write(chunk)
            hasher.update(chunk)
            size += len(chunk)

    file_hash = hasher.hexdigest()
    storage.record_export_file(
        arquivo_uuid=arquivo_uuid,
        run_id=run_id,
        file_path=file_path,
        file_size=size,
        file_hash=file_hash,
        link=link,
        expires_at=None,
    )
    return {"file_path": file_path, "file_size": size, "file_hash": file_hash}

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
    run_id: Optional[str] = None,
    page_size: int = 200,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], str]:
    client = CasaDosDadosClient()
    if not client.api_key:
        raise RuntimeError("CASA_DOS_DADOS_API_KEY nao configurada")

    page_size = min(int(page_size), int(limite)) if limite else int(page_size)
    payload = client.build_payload(
        uf=uf,
        municipios=municipios,
        cnaes=cnaes,
        excluir_mei=excluir_mei,
        com_telefone=com_telefone,
        com_email=com_email,
        pagina=1,
        limite=min(page_size, 1000),
    )
    fingerprint = _fingerprint(payload)
    source = f"casa_dos_dados:{fingerprint}"

    cached_leads = storage.fetch_leads_raw_by_source(source)
    cached_meta = storage.extract_cache_get(fingerprint)
    if cached_leads:
        trimmed = cached_leads[:limite]
        telemetry = {
            "total_encontrado": cached_meta.get("result_count") if cached_meta else len(cached_leads),
            "pages_processed": 0,
            "itens_coletados": len(trimmed),
            "itens_descartados_por_limite": max(0, len(cached_leads) - len(trimmed)),
            "page_size": page_size,
            "request_ids": [],
            "durations_ms": [],
            "payload_fingerprint": fingerprint,
            "cache_hit": True,
        }
        return trimmed, telemetry, source

    items, telemetry = search_v5(
        payload=payload,
        limit=limite,
        page_size=page_size,
        run_id=run_id,
    )

    normalized = [normalize_casa_dos_dados(item) for item in items]
    storage.insert_leads_raw(normalized, source, run_id=run_id)
    storage.extract_cache_set(fingerprint, payload, result_count=len(normalized), ttl_hours=cache_ttl_hours)
    return normalized, telemetry, source


def get_setores_disponiveis() -> List[str]:
    return list(SETORES_CNAE.keys())


def get_cidades_disponiveis() -> List[str]:
    return CIDADES_DISPONIVEIS
