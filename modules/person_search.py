"""Person search utilities for Hunter OS (PF discovery + disambiguation)."""

import asyncio
from dataclasses import dataclass
import json
import os
import re
import unicodedata
from typing import Any, Dict, List, Optional

import aiohttp

from modules import cleaning, providers, scoring, storage, validator


_NAME_STOPWORDS = {"de", "da", "do", "dos", "das", "e"}


def _digits(value: Any) -> str:
    return re.sub(r"\D", "", str(value or ""))


def _strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    return "".join([char for char in normalized if not unicodedata.combining(char)])


def _normalize_name(value: str) -> str:
    text = _strip_accents(str(value or "")).upper()
    parts = [part for part in re.split(r"\s+", text) if part]
    parts = [part for part in parts if part.lower() not in _NAME_STOPWORDS]
    return " ".join(parts)


def _normalize_city(value: str) -> str:
    return _strip_accents(str(value or "")).upper().strip()


def _like_pattern(name: str) -> str:
    tokens = [token for token in re.split(r"\s+", name) if token]
    if not tokens:
        return "%"
    return "%" + "%".join(tokens) + "%"


def _coerce_json_list(value: Any) -> List[str]:
    if value is None or value == "":
        return []
    if isinstance(value, list):
        return [str(item) for item in value if item]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return [value]
        if isinstance(parsed, list):
            return [str(item) for item in parsed if item]
        if parsed:
            return [str(parsed)]
        return []
    return [str(value)]


@dataclass
class PersonCandidate:
    nome_socio: str
    cpf: str
    qualificacao: str
    razao_social: str
    nome_fantasia: str
    cnpj: str
    municipio: str
    uf: str
    capital_social: float
    telefones_norm: Any
    emails_norm: Any
    socios_json: Any
    is_external: bool = False
    is_verified: bool = False
    verification_score: int = 0
    found_cnpj: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "nome_socio": self.nome_socio,
            "cpf": self.cpf,
            "qualificacao": self.qualificacao,
            "razao_social": self.razao_social,
            "nome_fantasia": self.nome_fantasia,
            "cnpj": self.cnpj,
            "municipio": self.municipio,
            "uf": self.uf,
            "capital_social": self.capital_social,
            "telefones_norm": self.telefones_norm,
            "emails_norm": self.emails_norm,
            "socios_json": self.socios_json,
            "is_external": self.is_external,
            "is_verified": self.is_verified,
            "verification_score": self.verification_score,
            "found_cnpj": self.found_cnpj,
        }

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "PersonCandidate":
        return cls(
            nome_socio=row.get("nome_socio") or "",
            cpf=row.get("cpf") or "",
            qualificacao=row.get("qualificacao") or "",
            razao_social=row.get("razao_social") or "",
            nome_fantasia=row.get("nome_fantasia") or "",
            cnpj=row.get("cnpj") or "",
            municipio=row.get("municipio") or "",
            uf=row.get("uf") or "",
            capital_social=float(row.get("capital_social") or 0),
            telefones_norm=row.get("telefones_norm"),
            emails_norm=row.get("emails_norm"),
            socios_json=row.get("socios_json"),
            is_external=bool(row.get("is_external") or False),
            is_verified=bool(row.get("is_verified") or False),
            verification_score=int(row.get("verification_score") or 0),
            found_cnpj=row.get("found_cnpj") or "",
        )


class PersonResolver:
    def __init__(self, candidates: List[PersonCandidate]) -> None:
        self.candidates = candidates

    def resolve(self) -> Dict[str, Any]:
        if not self.candidates:
            return {"status": "NOT_FOUND", "person": None, "candidates": []}
        if len(self.candidates) == 1:
            return {"status": "MATCH", "person": self.candidates[0], "candidates": []}
        return {"status": "AMBIGUOUS", "person": None, "candidates": self.candidates}


def choose_best_candidate(candidates: List[PersonCandidate]) -> Optional[PersonCandidate]:
    if not candidates:
        return None
    return max(candidates, key=lambda item: float(item.capital_social or 0))


def candidate_to_lead(candidate: PersonCandidate) -> Dict[str, Any]:
    return {
        "cnpj": candidate.cnpj,
        "razao_social": candidate.razao_social,
        "nome_fantasia": candidate.nome_fantasia,
        "municipio": candidate.municipio,
        "uf": candidate.uf,
        "capital_social": candidate.capital_social,
        "telefones_norm": _coerce_json_list(candidate.telefones_norm),
        "emails_norm": _coerce_json_list(candidate.emails_norm),
        "socios": [
            {
                "nome_socio": candidate.nome_socio,
                "cpf": candidate.cpf,
                "qualificacao": candidate.qualificacao,
            }
        ],
    }


def _run_async(coro: Any) -> Any:
    try:
        return asyncio.run(coro)
    except RuntimeError:
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(coro)


def _to_float(value: Any) -> float:
    if value is None:
        return 0.0
    text = str(value).strip()
    if not text:
        return 0.0
    text = text.replace(".", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return 0.0


def _brasilapi_to_raw(data: Dict[str, Any]) -> Dict[str, Any]:
    cnpj = _digits(data.get("cnpj") or "")
    telefone_raw = _digits(data.get("telefone") or "")
    ddd = telefone_raw[:2] if len(telefone_raw) >= 10 else ""
    numero = telefone_raw[2:] if len(telefone_raw) >= 10 else telefone_raw
    telefones = []
    if telefone_raw:
        telefones.append({"ddd": ddd, "numero": numero})
    email = str(data.get("email") or "").strip()
    emails = [{"email": email}] if email else []
    socios = []
    for partner in data.get("qsa", []) or []:
        if not isinstance(partner, dict):
            continue
        socios.append(
            {
                "nome_socio": partner.get("nome_socio_razao_social")
                or partner.get("nome_socio")
                or partner.get("nome")
                or "",
                "qualificacao": partner.get("qualificacao_socio")
                or partner.get("qualificacao")
                or "",
                "cpf": partner.get("cnpj_cpf_do_socio") or partner.get("cpf_socio") or "",
                "fonte": "brasilapi",
            }
        )
    return {
        "cnpj": cnpj,
        "razao_social": data.get("razao_social") or "",
        "nome_fantasia": data.get("nome_fantasia") or data.get("razao_social") or "",
        "cnae_fiscal": data.get("cnae_fiscal") or "",
        "cnae_fiscal_descricao": data.get("cnae_fiscal_descricao") or "",
        "ddd_telefone_1": telefone_raw or "",
        "telefones": telefones,
        "email": email,
        "emails": emails,
        "logradouro": data.get("logradouro") or "",
        "numero": data.get("numero") or "",
        "complemento": data.get("complemento") or "",
        "bairro": data.get("bairro") or "",
        "municipio": data.get("municipio") or "",
        "uf": data.get("uf") or "",
        "cep": data.get("cep") or "",
        "porte": data.get("porte") or "",
        "natureza_juridica": data.get("natureza_juridica") or "",
        "capital_social": _to_float(data.get("capital_social")),
        "data_inicio_atividade": data.get("data_inicio_atividade") or "",
        "situacao_cadastral": data.get("situacao_cadastral") or "ATIVA",
        "matriz_filial": data.get("matriz_filial") or "",
        "quadro_societario": socios,
        "fonte": "brasilapi",
    }


def import_official_company(official_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not official_data:
        return None
    raw = _brasilapi_to_raw(official_data)
    if not raw.get("cnpj"):
        return None
    storage.upsert_leads_raw([raw], source="deep_hunt")
    lead = cleaning.clean_lead(raw, exclude_mei=False)
    if not lead:
        return None
    lead["contact_quality"] = cleaning.contact_quality(lead.get("flags", {}))
    lead["score_v1"] = scoring.score_v1(lead)
    lead["score_v2"] = lead["score_v1"]
    lead["score_label"] = scoring.label(lead["score_v2"])
    storage.upsert_socios_from_leads([lead])
    storage.upsert_leads_clean([lead])
    return lead


def _build_external_candidate(
    name: str,
    cnpj: str,
    official_data: Dict[str, Any],
    match: Dict[str, Any],
    fallback_city: str,
    fallback_state: str,
) -> PersonCandidate:
    municipio = official_data.get("municipio") or fallback_city or ""
    uf = official_data.get("uf") or fallback_state or ""
    telefones = []
    telefone_raw = _digits(official_data.get("telefone") or "")
    if telefone_raw:
        telefones.append(telefone_raw)
    emails = []
    if official_data.get("email"):
        emails.append(official_data.get("email"))
    socios_json = official_data.get("qsa") or []
    return PersonCandidate(
        nome_socio=match.get("official_name") or name or "",
        cpf=match.get("partner_document") or "",
        qualificacao=match.get("role") or "Socio",
        razao_social=official_data.get("razao_social") or "",
        nome_fantasia=official_data.get("nome_fantasia") or official_data.get("razao_social") or "",
        cnpj=cnpj,
        municipio=municipio,
        uf=uf,
        capital_social=_to_float(official_data.get("capital_social")),
        telefones_norm=telefones,
        emails_norm=emails,
        socios_json=socios_json,
        is_external=True,
        is_verified=True,
        verification_score=int(match.get("confidence") or 0),
        found_cnpj=cnpj,
    )


def search_partners_external(
    name: str,
    city: Optional[str] = None,
    state: Optional[str] = None,
    limit: int = 5,
) -> List[PersonCandidate]:
    if not name:
        return []
    query_parts = [f'"{name}"']
    if city:
        query_parts.append(f'"{city}"')
    if state:
        query_parts.append(f'"{state}"')
    query = f"site:casadosdados.com.br {' '.join(query_parts)}"
    provider_name = os.getenv("SEARCH_PROVIDER", "serper")
    provider = providers.select_provider(provider_name)

    async def _search() -> Dict[str, Any]:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            return await provider.search(session, query)

    results = _run_async(_search())
    candidates = results.get("candidates") or []
    found: List[PersonCandidate] = []
    seen_cnpjs = set()
    city_norm = city or ""
    state_norm = state or ""

    for item in candidates:
        if len(found) >= limit:
            break
        snippet = f"{item.get('title') or ''} {item.get('snippet') or ''}"
        possible_cnpj = validator.extract_cnpj_from_text(snippet)
        if not possible_cnpj or possible_cnpj in seen_cnpjs:
            continue
        seen_cnpjs.add(possible_cnpj)
        official = validator.get_official_qsa(possible_cnpj)
        if not official:
            continue
        match = validator.validate_partner(name, official)
        if not match.get("is_match"):
            continue
        found.append(_build_external_candidate(name, possible_cnpj, official, match, city_norm, state_norm))

    try:
        from modules.telemetry import logger as telemetry_logger

        telemetry_logger.info(
            "Busca PF externa executada",
            extra={
                "event_type": "search",
                "query_type": "external",
                "target": _normalize_name(name),
                "city": _normalize_city(city_norm),
                "state": state_norm,
                "result_count": len(found),
            },
        )
    except Exception:
        pass

    return found


def search_partners_hybrid(
    name: Optional[str] = None,
    cpf: Optional[str] = None,
    city: Optional[str] = None,
    state: Optional[str] = None,
    limit: int = 25,
) -> Dict[str, Any]:
    local_results = search_partners(name=name, cpf=cpf, city=city, state=state, limit=limit)
    if local_results:
        return {"source": "local", "results": local_results}
    try:
        external_results = search_partners_external(name or "", city=city, state=state, limit=min(5, limit))
        return {"source": "external", "results": external_results}
    except Exception as exc:
        return {"source": "error", "results": [], "error": str(exc)}


def search_partners(
    name: Optional[str] = None,
    cpf: Optional[str] = None,
    city: Optional[str] = None,
    state: Optional[str] = None,
    limit: int = 25,
) -> List[PersonCandidate]:
    cleaned_cpf = _digits(cpf)
    cleaned_name = _normalize_name(name or "")
    city_norm = _normalize_city(city or "")
    state_norm = str(state or "").strip().upper()

    if not cleaned_cpf and not cleaned_name:
        return []

    params: List[Any] = []
    where_clauses: List[str] = []
    if cleaned_cpf:
        where_clauses.append("s.cpf = ?")
        params.append(cleaned_cpf)
    else:
        where_clauses.append("UPPER(s.nome_socio) LIKE ?")
        params.append(_like_pattern(cleaned_name))

    if city_norm:
        where_clauses.append("UPPER(l.municipio) = ?")
        params.append(city_norm)
    if state_norm:
        where_clauses.append("UPPER(l.uf) = ?")
        params.append(state_norm)

    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
    sql = (
        "SELECT s.nome_socio, s.cpf, s.qualificacao, "
        "l.razao_social, l.nome_fantasia, l.cnpj, l.municipio, l.uf, l.capital_social, "
        "l.telefones_norm, l.emails_norm, l.socios_json "
        "FROM socios s "
        "JOIN leads_clean l ON s.cnpj = l.cnpj "
        f"WHERE {where_sql} "
        "ORDER BY COALESCE(l.capital_social, 0) DESC "
        "LIMIT ?"
    )
    params.append(int(limit))

    with storage.get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    results = [PersonCandidate.from_row(dict(row)) for row in rows]

    try:
        from modules.telemetry import logger as telemetry_logger

        masked_cpf = ""
        if cleaned_cpf and len(cleaned_cpf) >= 4:
            masked_cpf = f"***{cleaned_cpf[-4:]}"
        telemetry_logger.info(
            "Busca PF executada",
            extra={
                "event_type": "search",
                "query_type": "cpf" if cleaned_cpf else "name",
                "target": cleaned_name or "",
                "cpf": masked_cpf,
                "city": city_norm,
                "state": state_norm,
                "result_count": len(results),
            },
        )
    except Exception:
        pass

    return results
