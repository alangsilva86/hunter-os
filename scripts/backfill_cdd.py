#!/usr/bin/env python3
import argparse
import json
import time
from typing import Any, Dict, Iterable, List

from modules import cleaning, data_sources, scoring, storage


def _parse_json(value: Any, default: Any) -> Any:
    if value is None or value == "":
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return default


def _as_list(value: Any) -> List[Any]:
    parsed = _parse_json(value, [])
    return parsed if isinstance(parsed, list) else []


def _as_dict(value: Any) -> Dict[str, Any]:
    parsed = _parse_json(value, {})
    return parsed if isinstance(parsed, dict) else {}


def _merge_scalar(existing: Any, fresh: Any, placeholders: Iterable[str] = ()) -> Any:
    if existing is None or str(existing).strip() == "" or str(existing).strip() in placeholders:
        return fresh if fresh is not None else existing
    return existing


def _build_enrichment(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "instagram": row.get("instagram"),
        "linkedin_company": row.get("linkedin_company"),
        "google_maps_url": row.get("google_maps_url"),
        "has_contact_page": bool(row.get("has_contact_page")),
        "has_form": bool(row.get("has_form")),
        "tech_stack": _parse_json(row.get("tech_stack_json"), {}),
        "tech_score": row.get("tech_score"),
        "tech_confidence": row.get("tech_confidence"),
        "has_marketing": bool(row.get("has_marketing")),
        "has_analytics": bool(row.get("has_analytics")),
        "has_ecommerce": bool(row.get("has_ecommerce")),
        "has_chat": bool(row.get("has_chat")),
        "signals": _parse_json(row.get("signals_json"), {}),
        "website_confidence": row.get("website_confidence"),
        "golden_techs_found": _parse_json(row.get("golden_techs_found"), []),
        "tech_sources": _parse_json(row.get("tech_sources"), {}),
    }


def _fetch_targets(limit: int) -> List[str]:
    with storage.get_conn() as conn:
        rows = conn.execute(
            """
            SELECT cnpj
            FROM leads_clean
            WHERE cnpj IS NOT NULL
              AND (
                emails_norm IS NULL OR emails_norm = '' OR emails_norm = '[]'
                OR municipio IS NULL OR municipio = ''
                OR socios_json IS NULL OR socios_json = '' OR socios_json = '[]'
              )
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [row["cnpj"] for row in rows if row["cnpj"]]


def _fetch_existing(cnpjs: List[str]) -> Dict[str, Dict[str, Any]]:
    if not cnpjs:
        return {}
    placeholders = ",".join(["?"] * len(cnpjs))
    with storage.get_conn() as conn:
        rows = conn.execute(
            f"""
            SELECT
                cnpj, razao_social, nome_fantasia, cnae, cnae_desc, porte,
                natureza_juridica, capital_social, municipio, uf, endereco_norm,
                telefones_norm, emails_norm, socios_json, flags_json,
                score_v1, score_v2, score_label, contact_quality
            FROM leads_clean
            WHERE cnpj IN ({placeholders})
            """,
            cnpjs,
        ).fetchall()
    return {row["cnpj"]: dict(row) for row in rows}


def _chunk(items: List[str], size: int) -> Iterable[List[str]]:
    for idx in range(0, len(items), size):
        yield items[idx: idx + size]


def _merge_lead(existing: Dict[str, Any], fresh: Dict[str, Any]) -> Dict[str, Any]:
    cnpj = existing.get("cnpj") or fresh.get("cnpj")
    existing_emails = _as_list(existing.get("emails_norm"))
    existing_phones = _as_list(existing.get("telefones_norm"))
    existing_socios = _as_list(existing.get("socios_json"))
    existing_flags = _as_dict(existing.get("flags_json"))
    fresh_flags = fresh.get("flags") or {}

    merged_flags = dict(existing_flags)
    for key, value in fresh_flags.items():
        if key not in merged_flags or merged_flags[key] in (None, "", False):
            merged_flags[key] = value

    merged = {
        "cnpj": cnpj,
        "razao_social": _merge_scalar(existing.get("razao_social"), fresh.get("razao_social"), {cnpj}),
        "nome_fantasia": _merge_scalar(existing.get("nome_fantasia"), fresh.get("nome_fantasia"), {cnpj}),
        "cnae": _merge_scalar(existing.get("cnae"), fresh.get("cnae")),
        "cnae_desc": _merge_scalar(existing.get("cnae_desc"), fresh.get("cnae_desc")),
        "porte": _merge_scalar(existing.get("porte"), fresh.get("porte")),
        "natureza_juridica": _merge_scalar(existing.get("natureza_juridica"), fresh.get("natureza_juridica")),
        "capital_social": (
            existing.get("capital_social")
            if (existing.get("capital_social") or 0) > 0
            else fresh.get("capital_social")
        ),
        "municipio": _merge_scalar(existing.get("municipio"), fresh.get("municipio")),
        "uf": _merge_scalar(existing.get("uf"), fresh.get("uf")),
        "endereco_norm": _merge_scalar(existing.get("endereco_norm"), fresh.get("endereco_norm")),
        "telefones_norm": existing_phones or (fresh.get("telefones_norm") or []),
        "emails_norm": existing_emails or (fresh.get("emails_norm") or []),
        "socios": existing_socios or (fresh.get("socios") or []),
        "flags": merged_flags,
        "score_v1": existing.get("score_v1"),
        "score_v2": existing.get("score_v2"),
        "score_label": existing.get("score_label"),
        "contact_quality": existing.get("contact_quality"),
    }

    merged["score_v1"] = scoring.score_v1(merged)
    merged["contact_quality"] = cleaning.contact_quality(merged["flags"])
    return merged


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill leads_clean with Casa dos Dados data.")
    parser.add_argument("--limit", type=int, default=500, help="Max leads to backfill.")
    parser.add_argument("--batch-size", type=int, default=50, help="Batch size for CNPJ requests.")
    parser.add_argument("--sleep", type=float, default=1.0, help="Seconds to sleep between batches.")
    parser.add_argument("--cnpjs-file", type=str, default="", help="Optional file with one CNPJ per line.")
    parser.add_argument("--recompute-scores", action="store_true", help="Recompute score_v2 if enrichment exists.")
    parser.add_argument("--dry-run", action="store_true", help="Do not write updates.")
    args = parser.parse_args()

    if args.cnpjs_file:
        with open(args.cnpjs_file, "r", encoding="utf-8") as handle:
            targets = [line.strip() for line in handle if line.strip()]
    else:
        targets = _fetch_targets(args.limit)

    if not targets:
        print("No targets found.")
        return

    total_updated = 0
    for batch in _chunk(targets, args.batch_size):
        items, telemetry = data_sources.fetch_cnpjs_v5(batch, run_id="backfill_cdd")
        normalized = [data_sources.normalize_casa_dos_dados(item) for item in items]
        cleaned = []
        for item in normalized:
            lead = cleaning.clean_lead(item, exclude_mei=False)
            if lead:
                cleaned.append(lead)

        existing_map = _fetch_existing([lead.get("cnpj") for lead in cleaned if lead.get("cnpj")])
        merged = []
        for lead in cleaned:
            existing = existing_map.get(lead.get("cnpj"))
            if not existing:
                continue
            merged.append(_merge_lead(existing, lead))

        if args.recompute_scores and merged:
            enrich_map = storage.fetch_enrichments_by_cnpjs([lead.get("cnpj") for lead in merged])
            for lead in merged:
                enrichment_row = enrich_map.get(lead.get("cnpj"))
                if not enrichment_row:
                    continue
                enrichment = _build_enrichment(enrichment_row)
                score, reasons, version = scoring.score_with_reasons(lead, enrichment)
                lead["score_v2"] = score
                lead["score_label"] = scoring.label(score)
                storage.update_enrichment_scoring(lead.get("cnpj"), version, reasons)

        if not args.dry_run and merged:
            storage.upsert_socios_from_leads(merged)
            storage.upsert_leads_clean(merged)
            total_updated += len(merged)

        print(
            f"batch={len(batch)} fetched={len(items)} merged={len(merged)} "
            f"updated={total_updated} request_ids={telemetry.get('request_ids')}"
        )
        time.sleep(max(0.0, args.sleep))

    print(f"Done. updated={total_updated}")


if __name__ == "__main__":
    main()
