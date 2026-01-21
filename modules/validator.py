"""Validation helpers for Deep Hunt (BrasilAPI + fuzzy match)."""

from __future__ import annotations

import logging
import re
from typing import Any, Dict

import requests
from thefuzz import fuzz


logger = logging.getLogger("hunter")

_CNPJ_PATTERN = re.compile(r"\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}")


def extract_cnpj_from_text(text: str) -> str:
    """Extract the first valid CNPJ pattern found in a text snippet."""
    if not text:
        return ""
    match = _CNPJ_PATTERN.search(text)
    if not match:
        return ""
    return re.sub(r"\D", "", match.group(0))


def get_official_qsa(cnpj: str) -> Dict[str, Any]:
    """Fetch official CNPJ data (including QSA) from BrasilAPI."""
    cnpj_digits = re.sub(r"\D", "", str(cnpj or ""))
    if not cnpj_digits:
        return {}
    url = f"https://brasilapi.com.br/api/cnpj/v1/{cnpj_digits}"
    try:
        resp = requests.get(url, timeout=5)
        if resp.status_code == 200:
            return resp.json()
    except requests.RequestException as exc:
        logger.warning("BrasilAPI request failed: %s", exc)
    except Exception as exc:
        logger.warning("BrasilAPI error: %s", exc)
    return {}


def validate_partner(target_name: str, official_data: Dict[str, Any]) -> Dict[str, Any]:
    """Check if target_name is present in the official QSA list."""
    qsa = official_data.get("qsa") or []
    target = (target_name or "").strip().upper()
    best_score = 0
    best_match = None

    for partner in qsa:
        if not isinstance(partner, dict):
            continue
        p_name = (
            partner.get("nome_socio_razao_social")
            or partner.get("nome_socio")
            or partner.get("nome")
            or ""
        )
        score = fuzz.token_set_ratio(target, str(p_name).upper())
        if score > best_score:
            best_score = score
            best_match = partner

    if best_score >= 85 and best_match:
        return {
            "is_match": True,
            "official_name": best_match.get("nome_socio_razao_social")
            or best_match.get("nome_socio")
            or best_match.get("nome")
            or "",
            "role": best_match.get("qualificacao_socio")
            or best_match.get("qualificacao")
            or "Socio",
            "confidence": int(best_score),
            "partner_document": best_match.get("cnpj_cpf_do_socio")
            or best_match.get("cpf_socio")
            or "",
        }

    return {"is_match": False, "confidence": int(best_score), "official_name": None, "partner_document": ""}
