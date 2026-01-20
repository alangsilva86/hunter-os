"""Scoring v1 and v2 for Hunter OS."""

import json
import re
import unicodedata
from typing import Any, Dict, List, Tuple

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


def _as_list(value: Any) -> List[Any]:
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


def _person_primary(value: Any) -> Dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, dict):
        payload = value
    elif isinstance(value, str):
        try:
            payload = json.loads(value)
        except Exception:
            return {}
    else:
        return {}
    primary = payload.get("primary") if isinstance(payload, dict) else {}
    return primary if isinstance(primary, dict) else {}


def _normalize_token(text: str) -> str:
    decomposed = unicodedata.normalize("NFD", text or "")
    cleaned = "".join(ch for ch in decomposed if unicodedata.category(ch) != "Mn")
    return re.sub(r"[^a-z0-9]", "", cleaned.lower())


def _socios_names(value: Any) -> List[str]:
    socios = _as_list(value)
    names: List[str] = []
    for socio in socios:
        if isinstance(socio, dict):
            name = (
                socio.get("nome_socio")
                or socio.get("nome")
                or socio.get("socio")
                or socio.get("name")
                or socio.get("representante")
                or ""
            )
        else:
            name = str(socio)
        name = name.strip()
        if name:
            names.append(name)
    return names


def partner_email_match(emails: Any, socios: Any) -> Tuple[bool, Any]:
    email_list = [str(item).strip().lower() for item in _as_list(emails) if item]
    socio_names = _socios_names(socios)
    for email in email_list:
        local = email.split("@")[0]
        local_norm = _normalize_token(local)
        if not local_norm:
            continue
        for name in socio_names:
            parts = [part for part in re.split(r"\s+", name) if part]
            if len(parts) < 2:
                continue
            first = _normalize_token(parts[0])
            last = _normalize_token(parts[-1])
            if len(first) < 3 or len(last) < 3:
                continue
            if first in local_norm and last in local_norm:
                return True, email
    return False, None


def _determine_profile(lead: Dict[str, Any], enrichment: Dict[str, Any]) -> str:
    cnae = str(lead.get("cnae") or "")
    instagram = enrichment.get("instagram")
    linkedin = enrichment.get("linkedin_company")
    has_ecommerce = bool(enrichment.get("has_ecommerce"))
    tech_stack = enrichment.get("tech_stack", {}) or {}
    stack_list = _as_list(tech_stack.get("detected_stack")) if isinstance(tech_stack, dict) else _as_list(tech_stack)

    if has_ecommerce or any(item in {"vtex", "shopify", "magento"} for item in stack_list):
        return "ECOMMERCE"
    if instagram and not linkedin:
        return "LOCAL_RETAIL"
    if any(code in CNAE_PRIORITARIOS for code in [cnae]):
        return "B2B_SERVICES"
    return "B2B_SERVICES" if linkedin else "LOCAL_RETAIL"


def score_with_reasons(lead: Dict[str, Any], enrichment: Dict[str, Any]) -> Tuple[int, List[str], str]:
    score = 50
    reasons: List[str] = []
    flags = lead.setdefault("flags", {})

    website_confidence = int(enrichment.get("website_confidence") or 0)
    tech_score = int(enrichment.get("tech_score") or 0)
    tech_confidence = enrichment.get("tech_confidence")
    if tech_confidence is None:
        tech_confidence = 100 if tech_score else 0
    tech_confidence = int(tech_confidence or 0)

    tech_stack = enrichment.get("tech_stack", {}) or {}
    if isinstance(tech_stack, dict):
        has_whatsapp_link = bool(tech_stack.get("has_whatsapp_link"))
        detected_stack = _as_list(tech_stack.get("detected_stack"))
    else:
        has_whatsapp_link = False
        detected_stack = _as_list(tech_stack)

    golden_techs = _as_list(enrichment.get("golden_techs_found"))
    if golden_techs and tech_confidence >= 60:
        score = 80
        reasons.append("golden_tech_gate")

    person_primary = _person_primary(enrichment.get("person_json"))
    emails_source = lead.get("emails_norm") or lead.get("emails") or lead.get("email")
    email_candidates = _as_list(emails_source)
    person_email = person_primary.get("email") if isinstance(person_primary, dict) else None
    if person_email:
        email_candidates.append(person_email)
    partner_match, _matched_email = partner_email_match(
        email_candidates,
        lead.get("socios") or lead.get("socios_json") or lead.get("quadro_societario"),
    )
    if not partner_match and person_primary.get("decision_maker_match"):
        partner_match = True
    if partner_match:
        score += 20
        reasons.append("decision_maker_email")
    flags["is_decision_maker_email"] = bool(partner_match)

    if enrichment.get("linkedin_company"):
        score += 10
        reasons.append("linkedin_found")
    if enrichment.get("instagram"):
        score += 5
        reasons.append("instagram_found")
    if enrichment.get("google_maps_url"):
        score += 5
        reasons.append("maps_found")

    if flags.get("whatsapp_probable") and has_whatsapp_link:
        score += 15
        reasons.append("whatsapp_verified")
    if enrichment.get("has_form") or enrichment.get("has_contact_page"):
        score += 5
        reasons.append("contact_available")

    if tech_score >= 20:
        score += 10
        reasons.append("tech_stack_strong")
    elif tech_score >= 10:
        score += 5
        reasons.append("tech_stack_ok")

    if flags.get("cnae_priority"):
        score += 15
        reasons.append("cnae_priority")
    if flags.get("email_domain_own"):
        score += 10
        reasons.append("email_domain_own")

    profile = _determine_profile(lead, enrichment)
    if profile == "LOCAL_RETAIL":
        if enrichment.get("instagram"):
            score += 5
            reasons.append("profile_local_retail")
        if has_whatsapp_link:
            score += 5
            reasons.append("profile_whatsapp")
    elif profile == "B2B_SERVICES":
        if enrichment.get("linkedin_company"):
            score += 5
            reasons.append("profile_b2b_linkedin")
        if website_confidence >= 70:
            score += 5
            reasons.append("profile_b2b_site")
    elif profile == "ECOMMERCE":
        if enrichment.get("has_ecommerce") or any(item in {"vtex", "shopify", "magento"} for item in detected_stack):
            score += 8
            reasons.append("profile_ecommerce")

    if flags.get("accountant_like"):
        score -= 30
        reasons.append("accountant_like")
    if flags.get("telefone_repetido"):
        score -= 15
        reasons.append("telefone_repetido")

    if website_confidence < 50:
        score = min(score, 69)
        reasons.append("website_confidence_gate")

    score = max(0, min(score, 100))
    score_version = "v5"
    return score, reasons[:5], score_version


def score_v2(lead: Dict[str, Any], enrichment: Dict[str, Any]) -> int:
    score, _, _ = score_with_reasons(lead, enrichment)
    return score


def label(score: int) -> str:
    if score >= 85:
        return "Hot"
    if score >= 70:
        return "Qualificado"
    if score >= 55:
        return "Potencial"
    return "Frio"
