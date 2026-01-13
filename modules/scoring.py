"""Scoring v1 and v2 for Hunter OS."""

import re
from typing import Any, Dict

from modules.cleaning import CNAE_PRIORITARIOS


def score_v1(lead: Dict[str, Any]) -> int:
    score = 50
    if lead.get("flags", {}).get("cnae_priority"):
        score += 15
    if lead.get("telefones_norm"):
        score += 10
    if lead.get("flags", {}).get("email_domain_own"):
        score += 10
    if (lead.get("capital_social") or 0) >= 100000:
        score += 5
    return min(score, 100)


def _is_pme(porte: Any) -> bool:
    text = str(porte or "").lower()
    return bool(re.search(r"\\b(me|epp|mei|micro|pequeno)\\b", text))


def score_v2(lead: Dict[str, Any], enrichment: Dict[str, Any]) -> int:
    score = 50
    flags = lead.get("flags", {})
    tech_score = int(enrichment.get("tech_score") or 0)
    tech_confidence = enrichment.get("tech_confidence")
    if tech_confidence is None:
        tech_confidence = 100 if tech_score else 0
    tech_confidence = int(tech_confidence or 0)

    has_whatsapp_link = False
    tech_stack = enrichment.get("tech_stack", {}) or {}
    if isinstance(tech_stack, dict):
        has_whatsapp_link = bool(tech_stack.get("has_whatsapp_link"))

    if tech_confidence >= 30 and tech_score > 0:
        score += int(round((tech_score / 30) * 20))
        if enrichment.get("has_marketing") and _is_pme(lead.get("porte")):
            score += 5

    if flags.get("whatsapp_probable") and has_whatsapp_link:
        score += 15
    if flags.get("cnae_priority"):
        score += 15
    if flags.get("email_domain_own"):
        score += 10
    if flags.get("accountant_like"):
        score -= 30
    if flags.get("telefone_repetido"):
        score -= 15

    return max(0, min(score, 100))


def label(score: int) -> str:
    if score >= 85:
        return "Hot"
    if score >= 70:
        return "Qualificado"
    if score >= 55:
        return "Potencial"
    return "Frio"
