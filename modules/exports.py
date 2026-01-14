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


def _format_list(value: Any) -> str:
    if value is None or value == "":
        return ""
    if isinstance(value, list):
        return ", ".join([str(item) for item in value if item])
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return value
        if isinstance(parsed, list):
            return ", ".join([str(item) for item in parsed if item])
        return json.dumps(parsed, ensure_ascii=False)
    return str(value)


def _parse_json(value: Any) -> Dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _flatten_flags(value: Any) -> str:
    flags = _parse_json(value)
    if not flags:
        return ""
    enabled = [key for key, val in flags.items() if val]
    return ", ".join(sorted(enabled))


def _flag_value(value: Any, key: str) -> str:
    flags = _parse_json(value)
    return "true" if flags.get(key) else ""


def _format_emails(value: Any) -> str:
    items = _coerce_list(value)
    return ", ".join([str(item) for item in items if item])


def _format_cpfs(value: Any) -> str:
    socios = _coerce_list(value)
    if not socios:
        return ""
    cpfs = []
    for socio in socios:
        if isinstance(socio, dict):
            cpf = socio.get("cpf") or ""
            if cpf:
                cpfs.append(cpf)
    return ", ".join(cpfs)

def format_export_data(
    rows: List[Dict[str, Any]],
    socios_map: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    mode: str = "commercial",
) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.fillna("")
    for col in [
        "cnpj",
        "razao_social",
        "nome_fantasia",
        "cnae_desc",
        "porte",
        "endereco_norm",
        "municipio",
        "uf",
        "telefones_norm",
        "emails_norm",
        "socios_json",
        "tech_stack_json",
        "site",
        "instagram",
        "linkedin_company",
        "search_term_used",
        "discovery_method",
        "website_confidence",
        "website_match_reasons",
        "candidates_considered",
        "excluded_candidates_count",
        "golden_techs_found",
        "tech_sources",
        "score_version",
        "score_reasons",
        "flags_json",
        "score_label",
        "score_v2",
        "google_maps_url",
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
    df["E-mails"] = df["emails_norm"].apply(_format_emails)
    df["socios"] = df.apply(
        lambda row: make_socios(row.get("cnpj", ""), row.get("socios_json", "")),
        axis=1,
    )
    df["cpf"] = df.apply(
        lambda row: _format_cpfs(socios_map.get(row.get("cnpj"), [])) if socios_map else "",
        axis=1,
    )
    df["cidade"] = df["municipio"]
    df["Stack Tecnol\u00f3gico"] = df["tech_stack_json"].apply(_stack_summary)
    df["whatsapp_probable"] = df["flags_json"].apply(lambda value: _flag_value(value, "whatsapp_probable"))
    df["flags achatadas"] = df["flags_json"].apply(_flatten_flags)
    df["score"] = df["score_v2"]

    export_columns = [
        "cnpj",
        "nome_fantasia",
        "razao_social",
        "cnae_desc",
        "porte",
        "cidade",
        "uf",
        "endereco_norm",
        "whatsapp_probable",
        "Link WhatsApp",
        "Telefones",
        "E-mails",
        "socios",
        "cpf",
        "site",
        "instagram",
        "linkedin_company",
        "google_maps_url",
        "score_label",
        "score_version",
        "score",
        "score_reasons",
        "flags achatadas",
        "Stack Tecnol\u00f3gico",
        "golden_techs_found",
    ]
    debug_columns = [
        "search_term_used",
        "discovery_method",
        "website_confidence",
        "website_match_reasons",
        "candidates_considered",
        "excluded_candidates_count",
        "tech_sources",
    ]

    for col in [
        "website_match_reasons",
        "golden_techs_found",
        "tech_sources",
        "score_reasons",
    ]:
        df[col] = df[col].apply(_format_list)

    columns = export_columns + (debug_columns if mode == "debug" else [])
    return df[columns]
