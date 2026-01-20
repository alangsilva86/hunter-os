"""Person intelligence helpers for Hunter OS."""

import hashlib
import json
import logging
import time
import os
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import aiohttp

from modules import email_finder, storage

logger = logging.getLogger("hunter")

_ROLE_KEYWORDS = (
    "administrador",
    "diretor",
    "presidente",
    "owner",
    "socio administrador",
    "socio-administrador",
    "ceo",
)

_WEALTH_TIERS = [
    (1_000_000, "A"),
    (100_000, "B"),
    (0, "C"),
]


def _safe_float(value: Any) -> float:
    if value is None or value == "":
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    try:
        text = str(value).strip().replace(".", "").replace(",", ".")
        return float(text)
    except Exception:
        return 0.0


def _parse_percent(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        pct = float(value)
    else:
        try:
            text = str(value).strip().replace("%", "").replace(",", ".")
            pct = float(text)
        except Exception:
            return None
    if pct <= 1:
        pct *= 100
    if pct < 0:
        return None
    return pct


def _normalize_domain(value: str) -> str:
    if not value:
        return ""
    parsed = urlparse(value if "://" in value else f"https://{value}")
    host = parsed.netloc or parsed.path
    host = host.lower().strip().strip("/")
    if host.startswith("www."):
        host = host[4:]
    return host


def _normalize_phone_e164(phone: Any) -> str:
    digits = re.sub(r"\D", "", str(phone or ""))
    if not digits:
        return ""
    if digits.startswith("55"):
        return f"+{digits}"
    if len(digits) in {10, 11}:
        return f"+55{digits}"
    return f"+{digits}"


def _extract_socios(lead: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw = lead.get("socios") or lead.get("socios_json") or lead.get("quadro_societario") or []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return []
    if isinstance(raw, dict):
        return [raw]
    if isinstance(raw, list):
        return [item for item in raw if item]
    return []


def _resolve_shares(socios: List[Dict[str, Any]]) -> List[float]:
    shares: List[Optional[float]] = []
    for socio in socios:
        share = None
        if isinstance(socio, dict):
            for key in (
                "percentual_capital",
                "percentual",
                "participacao",
                "participacao_capital",
                "quota",
                "participacao_societaria",
            ):
                if key in socio and socio.get(key) not in (None, ""):
                    share = _parse_percent(socio.get(key))
                    if share is not None:
                        break
        shares.append(share)

    if not socios:
        return []

    if any(share is not None for share in shares):
        known = sum([share for share in shares if share is not None])
        missing = sum(1 for share in shares if share is None)
        if known > 100:
            shares = [((share or 0) / known) * 100 for share in shares]
        elif missing:
            remaining = max(0.0, 100 - known)
            fill = remaining / missing
            shares = [(share if share is not None else fill) for share in shares]
        else:
            shares = [(share or 0) for share in shares]
    else:
        equal = 100 / len(socios)
        shares = [equal for _ in socios]

    return [float(share or 0) for share in shares]


def _socio_role_weight(qualificacao: str) -> int:
    qual = str(qualificacao or "").lower()
    return 2 if any(token in qual for token in _ROLE_KEYWORDS) else 1


def _pick_primary_index(socios: List[Dict[str, Any]], shares: List[float]) -> Optional[int]:
    if not socios:
        return None
    best_idx = 0
    best_score = -1.0
    for idx, socio in enumerate(socios):
        qual = ""
        if isinstance(socio, dict):
            qual = (
                socio.get("qualificacao")
                or socio.get("qual")
                or socio.get("qualificacao_socio")
                or ""
            )
        score = shares[idx] + (_socio_role_weight(qual) * 5)
        if score > best_score:
            best_score = score
            best_idx = idx
    return best_idx


def _wealth_class(value: float) -> str:
    for threshold, label in _WEALTH_TIERS:
        if value >= threshold:
            return label
    return "C"


async def _fetch_json(
    session: aiohttp.ClientSession,
    url: str,
    payload: Dict[str, Any],
    headers: Dict[str, str],
    timeout: int = 10,
) -> Dict[str, Any]:
    try:
        async with session.post(url, json=payload, headers=headers, timeout=timeout) as resp:
            if resp.status >= 400:
                return {}
            return await resp.json(content_type=None)
    except Exception:
        return {}


async def _download_avatar(
    session: aiohttp.ClientSession,
    url: str,
    dest_path: str,
    headers: Dict[str, str],
    timeout: int = 10,
) -> bool:
    try:
        async with session.get(url, headers=headers, timeout=timeout) as resp:
            if resp.status >= 400:
                return False
            content = await resp.read()
        with open(dest_path, "wb") as handle:
            handle.write(content)
        return True
    except Exception:
        return False


class PersonIntelligence:
    def __init__(
        self,
        evolution_base_url: Optional[str] = None,
        evolution_api_key: Optional[str] = None,
        avatar_cache_dir: str = "uploads/avatars",
        enable_email_finder: bool = False,
        enable_holehe: bool = False,
    ) -> None:
        self.evolution_base_url = (evolution_base_url or "").rstrip("/")
        self.evolution_api_key = evolution_api_key or ""
        self.avatar_cache_dir = avatar_cache_dir
        self.enable_email_finder = enable_email_finder
        self.enable_holehe = enable_holehe

        if self.avatar_cache_dir:
            os.makedirs(self.avatar_cache_dir, exist_ok=True)

    async def fetch_avatar(
        self,
        session: aiohttp.ClientSession,
        phone_e164: str,
    ) -> Optional[str]:
        if not self.evolution_base_url or not phone_e164:
            return None

        # 1. Configuration & Setup
        instance_name = os.getenv("WA_INSTANCE_NAME", "91acessus")
        phone_key = phone_e164.replace("+", "")

        # Cache check
        file_name = hashlib.sha256(phone_key.encode("utf-8")).hexdigest()[:16] + ".jpg"
        cached_path = os.path.join(self.avatar_cache_dir, file_name)
        if os.path.exists(cached_path):
            return cached_path

        # 2. Auth Headers (Robust for Render/Gateway)
        headers = {
            "Content-Type": "application/json",
            "apikey": self.evolution_api_key,
            "x-api-key": self.evolution_api_key,
            "Authorization": f"Bearer {self.evolution_api_key}",
        }

        # 3. Construct Instance URL
        # Logic: Handle if user put full path in .env or just base
        base_url = self.evolution_base_url.rstrip("/")
        if "/instances" not in base_url:
            base_instance_url = f"{base_url}/instances/{instance_name}"
        else:
            base_instance_url = base_url

        try:
            # 4. Check Number Existence
            # Custom API usually expects GET with query params
            check_url = f"{base_instance_url}/check-number"
            async with session.get(check_url, params={"number": phone_key}, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    exists = (
                        data.get("exists")
                        or data.get("valid")
                        or (data.get("result") or {}).get("exists")
                    )
                    if not exists:
                        return None
                else:
                    # Silent fail if endpoint unreachable
                    return None

            # 5. Fetch Profile Picture URL
            pic_url_endpoint = f"{base_instance_url}/profile-pic"
            remote_url = None

            async with session.get(pic_url_endpoint, params={"number": phone_key}, headers=headers) as resp_pic:
                if resp_pic.status == 200:
                    pic_data = await resp_pic.json()
                    remote_url = (
                        pic_data.get("profilePicUrl")
                        or pic_data.get("url")
                        or pic_data.get("imgUrl")
                    )

            if not remote_url:
                return None

            # 6. Download Binary Image
            if await _download_avatar(session, remote_url, cached_path, headers):
                return cached_path

        except Exception as e:
            logger.warning(f"Avatar fetch failed for {phone_e164} on instance {instance_name}: {e}")
            try:
                from modules.telemetry import logger as telemetry_logger

                telemetry_logger.error(
                    f"Falha no avatar: {phone_e164}",
                    extra={"event_type": "error", "trace": str(e)},
                )
            except Exception:
                pass
            return None

        return None

    def _select_phone(self, lead: Dict[str, Any]) -> str:
        phones = lead.get("telefones_norm") or []
        if isinstance(phones, str):
            try:
                phones = json.loads(phones)
            except Exception:
                phones = [phones]
        for phone in phones:
            normalized = _normalize_phone_e164(phone)
            if normalized:
                return normalized
        return ""

    def _link_from_enrichment(self, enrichment: Optional[Dict[str, Any]]) -> str:
        if not enrichment:
            return ""
        people = enrichment.get("linkedin_people") or enrichment.get("linkedin_people_json") or []
        if isinstance(people, str):
            try:
                people = json.loads(people)
            except Exception:
                people = [people]
        if isinstance(people, list) and people:
            return str(people[0])
        return ""

    def _email_from_domain(self, name: str, domain: str, socios: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not self.enable_email_finder or not name or not domain:
            return {}
        return email_finder.find_best_email(
            full_name=name,
            domain=domain,
            socios=socios,
            enable_validation=self.enable_holehe,
        )

    def _build_person_payload(
        self,
        lead: Dict[str, Any],
        enrichment: Optional[Dict[str, Any]],
        socios: List[Dict[str, Any]],
        shares: List[float],
    ) -> Dict[str, Any]:
        if not socios:
            return {}
        primary_idx = _pick_primary_index(socios, shares)
        if primary_idx is None:
            return {}

        primary = socios[primary_idx]
        share_pct = shares[primary_idx] if primary_idx < len(shares) else 0.0
        capital_social = _safe_float(lead.get("capital_social"))
        wealth_estimate = capital_social * (share_pct / 100.0)
        wealth_class = _wealth_class(wealth_estimate)

        name = ""
        cpf = ""
        qualificacao = ""
        if isinstance(primary, dict):
            name = (
                primary.get("nome_socio")
                or primary.get("nome")
                or primary.get("socio")
                or primary.get("name")
                or ""
            )
            cpf = primary.get("cpf") or primary.get("documento") or ""
            qualificacao = primary.get("qualificacao") or primary.get("qual") or ""
        else:
            name = str(primary)

        linkedin_profile = self._link_from_enrichment(enrichment)
        domain = _normalize_domain(enrichment.get("site") if enrichment else "")
        if not domain:
            emails = lead.get("emails_norm") or lead.get("email") or []
            if isinstance(emails, str):
                try:
                    emails = json.loads(emails)
                except Exception:
                    emails = [emails]
            if isinstance(emails, list) and emails:
                first_email = str(emails[0])
                if "@" in first_email:
                    domain = first_email.split("@")[-1].strip().lower()
        email_payload = self._email_from_domain(name, domain, socios)

        cross = storage.find_cross_ownership(
            cpf=cpf,
            name=name,
            exclude_cnpj=lead.get("cnpj"),
            limit=5,
        )

        payload = {
            "primary": {
                "name": name,
                "cpf": cpf,
                "role": qualificacao,
                "share_pct": round(share_pct, 2),
                "wealth_estimate": round(wealth_estimate, 2),
                "wealth_class": wealth_class,
                "linkedin_profile": linkedin_profile,
            },
            "cross_ownership": cross,
        }

        if wealth_class == "A":
            try:
                from modules.telemetry import logger as telemetry_logger

                telemetry_logger.info(
                    f"ALVO CLASSE A DETECTADO: R$ {wealth_estimate:,.2f}",
                    extra={
                        "event_type": "wealth",
                        "amount": wealth_estimate,
                        "role": qualificacao,
                    },
                )
            except Exception:
                pass

        if email_payload:
            payload["primary"]["email"] = email_payload.get("email") or ""
            payload["primary"]["email_validated"] = bool(email_payload.get("validated"))
            payload["primary"]["email_sources"] = email_payload.get("validation_sources") or []
            payload["primary"]["decision_maker_match"] = bool(email_payload.get("decision_maker_match"))

        return payload

    async def enrich(
        self,
        session: aiohttp.ClientSession,
        lead: Dict[str, Any],
        enrichment: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        socios = _extract_socios(lead)
        if not socios:
            return {"wealth_score": 0, "avatar_url": None, "person_json": {}}

        shares = _resolve_shares(socios)
        person_payload = self._build_person_payload(lead, enrichment, socios, shares)

        wealth_score = 0.0
        primary = person_payload.get("primary") if isinstance(person_payload, dict) else {}
        if isinstance(primary, dict):
            wealth_score = _safe_float(primary.get("wealth_estimate"))

        avatar_url = None
        phone_e164 = self._select_phone(lead)
        if phone_e164:
            start = time.time()
            avatar_url = await self.fetch_avatar(session, phone_e164)
            duration_ms = round((time.time() - start) * 1000, 2)
            if avatar_url:
                try:
                    from modules.telemetry import logger as telemetry_logger

                    telemetry_logger.info(
                        f"Avatar baixado em {duration_ms}ms",
                        extra={
                            "event_type": "api",
                            "latency_ms": duration_ms,
                            "provider": "Evolution/Baileys",
                        },
                    )
                except Exception:
                    pass
        if avatar_url:
            person_payload.setdefault("primary", {})["avatar_url"] = avatar_url

        return {
            "wealth_score": wealth_score,
            "avatar_url": avatar_url,
            "person_json": person_payload,
        }
