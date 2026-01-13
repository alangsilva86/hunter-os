"""
Cleaning and dedup flags for Hunter OS.
"""

import json
import re
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote_plus

CNAE_PRIORITARIOS = {
    "8211", "8219", "8220", "8291",
    "6910", "6920",
    "4930", "5211", "5250",
    "8610", "8630", "8650",
    "4110", "4120",
}

ACCOUNTANT_REGEX = re.compile(r"contabil|contabilidade|escritorio|assessoria|bpo", re.IGNORECASE)


def _digits(value: str) -> str:
    return re.sub(r"\D", "", str(value or ""))


def normalize_phone(phone: str) -> Optional[str]:
    digits = _digits(phone)
    if digits.startswith("55") and len(digits) > 11:
        digits = digits[2:]
    if len(digits) in {10, 11}:
        return digits
    return None


def is_mobile(phone: str) -> bool:
    digits = normalize_phone(phone)
    if not digits:
        return False
    return len(digits) == 11 and digits[2] == "9"


def normalize_email(email: str) -> Optional[str]:
    email = (email or "").strip().lower()
    if "@" not in email:
        return None
    return email


def email_domain_own(email: str) -> bool:
    domain = (email or "").split("@")[-1].lower()
    if not domain:
        return False
    generic = {
        "gmail.com",
        "hotmail.com",
        "outlook.com",
        "yahoo.com",
        "bol.com.br",
        "uol.com.br",
        "icloud.com",
        "live.com",
    }
    return domain not in generic


def cnae_prefix(cnae: str, size: int = 4) -> str:
    digits = _digits(cnae)
    return digits[:size]


def google_maps_url(razao_social: str, municipio: str, uf: str) -> str:
    query = quote_plus(f"{razao_social} {municipio} {uf}")
    return f"https://www.google.com/maps/search/?api=1&query={query}"


def is_mei(raw: Dict[str, Any]) -> bool:
    natureza = str(raw.get("natureza_juridica", "")).upper()
    porte = str(raw.get("porte", "")).upper()
    if "MEI" in natureza or "MICROEMPREENDEDOR" in natureza:
        return True
    if "MEI" in porte or "MICROEMPREENDEDOR" in porte:
        return True
    return False


def extract_phones(raw: Dict[str, Any]) -> List[str]:
    phones = []
    if raw.get("ddd_telefone_1"):
        phones.append(raw.get("ddd_telefone_1"))
    for tel in raw.get("telefones", []) or []:
        ddd = tel.get("ddd", "")
        numero = tel.get("numero", "")
        phones.append(f"{ddd}{numero}")
    normalized = []
    for phone in phones:
        norm = normalize_phone(phone)
        if norm:
            normalized.append(norm)
    return list(dict.fromkeys(normalized))


def extract_emails(raw: Dict[str, Any]) -> List[str]:
    emails = []
    if raw.get("email"):
        emails.append(raw.get("email"))
    for item in raw.get("emails", []) or []:
        if isinstance(item, dict):
            emails.append(item.get("email"))
        else:
            emails.append(item)
    normalized = []
    for email in emails:
        norm = normalize_email(email)
        if norm:
            normalized.append(norm)
    return list(dict.fromkeys(normalized))


def accountant_like(raw: Dict[str, Any], emails: List[str]) -> bool:
    text = f"{raw.get('razao_social', '')} {raw.get('nome_fantasia', '')}".lower()
    if ACCOUNTANT_REGEX.search(text):
        return True
    for email in emails:
        if ACCOUNTANT_REGEX.search(email):
            return True
    return False


def clean_lead(raw: Dict[str, Any], exclude_mei: bool = True) -> Optional[Dict[str, Any]]:
    if exclude_mei and is_mei(raw):
        return None

    phones = extract_phones(raw)
    emails = extract_emails(raw)
    flags = {
        "accountant_like": accountant_like(raw, emails),
        "telefone_repetido": False,
        "cnae_priority": cnae_prefix(raw.get("cnae_fiscal", "")) in CNAE_PRIORITARIOS,
        "email_domain_own": any(email_domain_own(e) for e in emails),
        "whatsapp_probable": any(is_mobile(p) for p in phones),
        "google_maps_url": google_maps_url(
            raw.get("razao_social", ""),
            raw.get("municipio", ""),
            raw.get("uf", ""),
        ),
    }

    endereco_parts = [
        raw.get("logradouro", ""),
        raw.get("numero", ""),
        raw.get("complemento", ""),
        raw.get("bairro", ""),
    ]
    endereco = ", ".join([p for p in endereco_parts if p])

    return {
        "cnpj": _digits(raw.get("cnpj", "")),
        "razao_social": raw.get("razao_social", ""),
        "nome_fantasia": raw.get("nome_fantasia", ""),
        "cnae": raw.get("cnae_fiscal", ""),
        "cnae_desc": raw.get("cnae_fiscal_descricao", ""),
        "porte": raw.get("porte", ""),
        "natureza_juridica": raw.get("natureza_juridica", ""),
        "capital_social": raw.get("capital_social", 0),
        "municipio": raw.get("municipio", ""),
        "uf": raw.get("uf", ""),
        "endereco_norm": endereco,
        "telefones_norm": phones,
        "emails_norm": emails,
        "flags": flags,
    }


def apply_repeated_phone_flags(cleaned: List[Dict[str, Any]], min_count: int = 5) -> None:
    counter: Counter = Counter()
    for lead in cleaned:
        for phone in lead.get("telefones_norm", []):
            counter[phone] += 1

    repeated = {phone for phone, cnt in counter.items() if cnt >= min_count}
    for lead in cleaned:
        lead["flags"]["telefone_repetido"] = any(
            phone in repeated for phone in lead.get("telefones_norm", [])
        )


def contact_quality(flags: Dict[str, Any]) -> str:
    if flags.get("accountant_like"):
        return "accountant_like"
    if flags.get("telefone_repetido"):
        return "suspicious"
    return "ok"


def clean_batch(
    raw_leads: List[Dict[str, Any]],
    exclude_mei: bool = True,
    min_repeat: int = 5,
    return_stats: bool = False,
) -> Any:
    cleaned = []
    removed_mei = 0
    removed_other = 0
    for raw in raw_leads:
        lead = clean_lead(raw, exclude_mei=exclude_mei)
        if lead:
            cleaned.append(lead)
        else:
            if exclude_mei and is_mei(raw):
                removed_mei += 1
            else:
                removed_other += 1
    apply_repeated_phone_flags(cleaned, min_count=min_repeat)
    for lead in cleaned:
        lead["contact_quality"] = contact_quality(lead["flags"])

    if return_stats:
        stats = {
            "input_count": len(raw_leads),
            "output_count": len(cleaned),
            "removed_mei": removed_mei,
            "removed_other": removed_other,
        }
        return cleaned, stats
    return cleaned
