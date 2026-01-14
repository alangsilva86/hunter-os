"""Webhook export helpers for Hunter OS."""

import json
import logging
import re
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
from uuid import uuid4

import requests

from modules import storage

logger = logging.getLogger("hunter")


def _chunked(items: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    for idx in range(0, len(items), size):
        yield items[idx: idx + size]


def send_batch_to_webhook(
    leads: List[Dict[str, Any]],
    url: str,
    batch_size: int = 75,
    timeout: int = 15,
) -> Dict[str, Any]:
    if not url:
        raise RuntimeError("Webhook URL nao configurada")
    if not leads:
        return {"sent": 0, "failed": 0, "batches": 0}

    run_id = next((lead.get("run_id") for lead in leads if lead.get("run_id")), None)
    results = {"sent": 0, "failed": 0, "batches": 0}

    for batch in _chunked(leads, batch_size):
        payload = {
            "batch_id": uuid4().hex,
            "sent_at": storage._utcnow(),
            "count": len(batch),
            "run_id": run_id,
            "leads": batch,
        }
        try:
            body = json.dumps(payload, ensure_ascii=False, default=str)
            resp = requests.post(
                url,
                data=body,
                headers={"Content-Type": "application/json"},
                timeout=timeout,
            )
            ok = 200 <= resp.status_code < 300
            status = "success" if ok else "error"
            response_code = resp.status_code
        except Exception as exc:
            logger.warning("Webhook batch failed: %s", exc)
            status = "error"
            response_code = None

        for lead in batch:
            storage.record_webhook_delivery(
                run_id=run_id,
                lead_cnpj=lead.get("cnpj"),
                status=status,
                response_code=response_code,
            )

        results["batches"] += 1
        if status == "success":
            results["sent"] += len(batch)
        else:
            results["failed"] += len(batch)

    return results


def _coerce_list(value: Any) -> List[Any]:
    if value is None or value == "":
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return [value]
        if parsed is None:
            return []
        if isinstance(parsed, list):
            return parsed
        return [parsed]
    return [value]


def _digits_only(value: Any) -> str:
    return re.sub(r"\D", "", str(value or ""))


def _stack_summary(value: Any) -> str:
    if not value:
        return ""
    if isinstance(value, dict):
        stack = value.get("detected_stack") or value.get("stack") or []
    else:
        try:
            parsed = json.loads(value)
        except Exception:
            parsed = value
        if isinstance(parsed, dict):
            stack = parsed.get("detected_stack") or parsed.get("stack") or []
        elif isinstance(parsed, list):
            stack = parsed
        else:
            stack = []
    return ", ".join(stack[:10])


def _format_links(site: str, instagram: str, linkedin: str) -> str:
    links = [item for item in [site, instagram, linkedin] if item]
    return " | ".join(links)


def _format_city_uf(municipio: str, uf: str) -> str:
    if municipio and uf:
        return f"{municipio}/{uf}"
    return municipio or uf or ""


def _format_socios(value: Any) -> str:
    socios = _coerce_list(value)
    if not socios:
        return ""
    formatted = []
    for socio in socios:
        if isinstance(socio, dict):
            nome = socio.get("nome_socio") or socio.get("nome") or ""
            qual = socio.get("qualificacao") or socio.get("qual") or socio.get("qualificacao_socio") or ""
        else:
            nome = str(socio)
            qual = ""
        nome = nome.title().strip()
        qual = qual.strip()
        if nome and qual:
            formatted.append(f"{nome} ({qual})")
        elif nome:
            formatted.append(nome)
    return ", ".join(formatted)


def format_export_data(
    rows: List[Dict[str, Any]],
    socios_map: Optional[Dict[str, List[Dict[str, Any]]]] = None,
) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.fillna("")
    for col in [
        "cnpj",
        "razao_social",
        "nome_fantasia",
        "municipio",
        "uf",
        "telefones_norm",
        "emails_norm",
        "socios_json",
        "tech_stack_json",
        "site",
        "instagram",
        "linkedin_company",
    ]:
        if col not in df.columns:
            df[col] = ""

    def make_wa(phones: Any) -> str:
        items = _coerce_list(phones)
        if not items:
            return ""
        num = _digits_only(items[0])
        if not num:
            return ""
        if num.startswith("55"):
            return f"https://wa.me/{num}"
        return f"https://wa.me/55{num}"

    def make_phones(phones: Any) -> str:
        items = _coerce_list(phones)
        cleaned = [_digits_only(item) for item in items]
        cleaned = [item for item in cleaned if item]
        return ", ".join(cleaned)

    def make_email(emails: Any) -> str:
        items = _coerce_list(emails)
        if not items:
            return ""
        return str(items[0])

    def make_socios(cnpj: str, socios_raw: Any) -> str:
        if socios_map and cnpj in socios_map:
            return _format_socios(socios_map[cnpj])
        return _format_socios(socios_raw)

    df["Link WhatsApp"] = df["telefones_norm"].apply(make_wa)
    df["Telefones"] = df["telefones_norm"].apply(make_phones)
    df["E-mail"] = df["emails_norm"].apply(make_email)
    df["S\u00f3cios"] = df.apply(
        lambda row: make_socios(row.get("cnpj", ""), row.get("socios_json", "")),
        axis=1,
    )
    df["Empresa"] = df.apply(
        lambda row: row.get("razao_social") or row.get("nome_fantasia") or "",
        axis=1,
    )
    df["Cidade/UF"] = df.apply(
        lambda row: _format_city_uf(row.get("municipio", ""), row.get("uf", "")),
        axis=1,
    )
    df["Stack Tecnol\u00f3gico"] = df["tech_stack_json"].apply(_stack_summary)
    df["Links"] = df.apply(
        lambda row: _format_links(row.get("site", ""), row.get("instagram", ""), row.get("linkedin_company", "")),
        axis=1,
    )

    export_columns = [
        "Empresa",
        "Link WhatsApp",
        "Telefones",
        "S\u00f3cios",
        "E-mail",
        "Cidade/UF",
        "Stack Tecnol\u00f3gico",
        "Links",
    ]
    return df[export_columns]
