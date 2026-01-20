"""Email discovery helpers for Hunter OS."""

import logging
import os
import re
import shutil
import subprocess
import sys
import unicodedata
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

try:
    from email_validator import EmailNotValidError, validate_email
    _EMAIL_VALIDATOR_AVAILABLE = True
except Exception:  # pragma: no cover - fallback when optional deps are missing
    EmailNotValidError = Exception
    _EMAIL_VALIDATOR_AVAILABLE = False

    def validate_email(email: str, check_deliverability: bool = False) -> Dict[str, Any]:
        return {"email": email}

from modules import scoring

logger = logging.getLogger("hunter")

_EMAIL_PATTERNS = [
    "{first}.{last}",
    "{first}{last}",
    "{f}{last}",
    "{first}{l}",
    "{first}_{last}",
    "{first}-{last}",
    "{last}.{first}",
    "{last}{first}",
    "{first}",
]


def _strip_accents(value: str) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", value)
    return "".join([ch for ch in normalized if not unicodedata.combining(ch)])


def _slugify(value: str) -> str:
    text = _strip_accents(value or "").lower()
    text = re.sub(r"[^a-z0-9]+", " ", text).strip()
    return re.sub(r"\s+", " ", text)


def _normalize_domain(domain: str) -> str:
    if not domain:
        return ""
    parsed = urlparse(domain if "://" in domain else f"https://{domain}")
    host = parsed.netloc or parsed.path
    host = host.lower().strip().strip("/")
    if host.startswith("www."):
        host = host[4:]
    return host


def _split_name(full_name: str) -> List[str]:
    cleaned = _slugify(full_name)
    return [part for part in cleaned.split(" ") if part]


def _valid_syntax(email: str) -> bool:
    if not _EMAIL_VALIDATOR_AVAILABLE:
        return bool(re.match(r"^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", email or ""))
    try:
        validate_email(email, check_deliverability=False)
    except EmailNotValidError:
        return False
    return True


def generate_permutations(full_name: str, domain: str, limit: int = 20) -> List[str]:
    domain = _normalize_domain(domain)
    if not domain:
        return []

    parts = _split_name(full_name)
    if not parts:
        return []

    first = parts[0]
    last = parts[-1] if len(parts) > 1 else ""
    middle = parts[1] if len(parts) > 2 else ""
    f = first[0] if first else ""
    l = last[0] if last else ""

    patterns = list(_EMAIL_PATTERNS)
    if middle:
        patterns.extend(
            [
                "{first}.{middle}.{last}",
                "{first}{middle}{last}",
                "{first}.{m}.{last}",
                "{f}.{middle}.{last}",
            ]
        )

    candidates: List[str] = []
    for pattern in patterns:
        try:
            local = pattern.format(
                first=first,
                last=last,
                middle=middle,
                f=f,
                l=l,
                m=middle[0] if middle else "",
            )
        except Exception:
            continue
        local = local.strip(".-_")
        if not local:
            continue
        email = f"{local}@{domain}"
        if _valid_syntax(email):
            candidates.append(email)

    return list(dict.fromkeys(candidates))[:limit]


def _holehe_cmd() -> Optional[List[str]]:
    bin_name = os.getenv("HOLEHE_BIN")
    if bin_name:
        return [bin_name]
    if shutil.which("holehe"):
        return ["holehe"]
    return [sys.executable, "-m", "holehe"]


def _parse_holehe_output(output: str) -> List[str]:
    providers: List[str] = []
    for line in output.splitlines():
        low = line.lower()
        if "linkedin" in low and any(token in low for token in ("found", "true", "yes", "live")):
            providers.append("linkedin")
        if any(token in low for token in ("microsoft", "outlook", "office")) and any(
            token in low for token in ("found", "true", "yes", "live")
        ):
            providers.append("microsoft")
    return list(dict.fromkeys(providers))


def validate_with_holehe(email: str, timeout: int = 20) -> Dict[str, Any]:
    cmd = _holehe_cmd()
    if not cmd:
        return {"valid": False, "providers": []}

    try:
        result = subprocess.run(
            cmd + [email],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except Exception as exc:
        logger.warning("holehe check failed: %s", exc)
        return {"valid": False, "providers": []}

    output = (result.stdout or "") + "\n" + (result.stderr or "")
    providers = _parse_holehe_output(output)
    return {"valid": bool(providers), "providers": providers}


def find_best_email(
    full_name: str,
    domain: str,
    socios: Any = None,
    enable_validation: bool = False,
) -> Dict[str, Any]:
    candidates = generate_permutations(full_name, domain)
    if not candidates:
        return {
            "email": "",
            "candidates": [],
            "validated": False,
            "validation_sources": [],
            "decision_maker_match": False,
            "matched_email": None,
        }

    validated_email = ""
    validation_sources: List[str] = []
    if enable_validation:
        for email in candidates:
            check = validate_with_holehe(email)
            if check.get("valid"):
                validated_email = email
                validation_sources = check.get("providers", [])
                break

    selected = validated_email or candidates[0]
    partner_match, matched_email = scoring.partner_email_match([selected], socios)

    return {
        "email": selected,
        "candidates": candidates,
        "validated": bool(validated_email),
        "validation_sources": validation_sources,
        "decision_maker_match": partner_match,
        "matched_email": matched_email,
    }
