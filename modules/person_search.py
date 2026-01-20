"""Person search utilities for Hunter OS (PF discovery + disambiguation)."""

from dataclasses import dataclass
import json
import re
import unicodedata
from typing import Any, Dict, List, Optional

from modules import storage


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
