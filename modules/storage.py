"""
SQLite storage layer for Hunter OS.
"""

import json
import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple
from uuid import uuid4

DEFAULT_DB_PATH = os.getenv("HUNTER_DB_PATH", "hunter.db")
_SCHEMA_READY = False
logger = logging.getLogger("hunter")


def _utcnow() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def get_db_path() -> str:
    env_path = os.getenv("HUNTER_DB_PATH")
    if env_path:
        return env_path
    if os.path.isdir("/data") and os.access("/data", os.W_OK):
        return os.path.join("/data", "hunter.db")
    return DEFAULT_DB_PATH


def _ensure_schema() -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    init_db()
    _SCHEMA_READY = True

def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None

def _ensure_column(conn: sqlite3.Connection, table: str, column: str, col_type: str) -> None:
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
    except sqlite3.OperationalError:
        return


@contextmanager
def get_conn():
    conn = sqlite3.connect(get_db_path(), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS leads_raw (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cnpj TEXT,
                payload_json TEXT,
                fetched_at TIMESTAMP,
                source TEXT,
                run_id TEXT,
                export_uuid TEXT
            )
            """
        )
        _ensure_column(conn, "leads_raw", "run_id", "TEXT")
        _ensure_column(conn, "leads_raw", "export_uuid", "TEXT")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_leads_raw_cnpj ON leads_raw(cnpj)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_leads_raw_source ON leads_raw(source)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_leads_raw_run_id ON leads_raw(run_id)")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS leads_clean (
                cnpj TEXT PRIMARY KEY,
                razao_social TEXT,
                nome_fantasia TEXT,
                cnae TEXT,
                cnae_desc TEXT,
                porte TEXT,
                natureza_juridica TEXT,
                capital_social REAL,
                municipio TEXT,
                uf TEXT,
                endereco_norm TEXT,
                telefones_norm TEXT,
                emails_norm TEXT,
                socios_json TEXT,
                flags_json TEXT,
                score_v1 REAL,
                score_v2 REAL,
                score_label TEXT,
                contact_quality TEXT,
                updated_at TIMESTAMP
            )
            """
        )
        _ensure_column(conn, "leads_clean", "socios_json", "TEXT")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_leads_clean_score ON leads_clean(score_v2)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_leads_clean_city ON leads_clean(municipio, uf)")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS socios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cnpj TEXT,
                nome_socio TEXT,
                cpf TEXT,
                idade INTEGER,
                qualificacao TEXT,
                fonte TEXT,
                created_at TIMESTAMP
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_socios_cnpj ON socios(cnpj)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_socios_nome ON socios(nome_socio)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_socios_cpf ON socios(cpf)")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS enrichment_runs (
                run_id TEXT PRIMARY KEY,
                created_at TIMESTAMP,
                params_json TEXT,
                status TEXT,
                total_leads INTEGER,
                enriched_count INTEGER,
                errors_count INTEGER
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                created_at TIMESTAMP,
                params_json TEXT,
                status TEXT,
                total_leads INTEGER,
                enriched_count INTEGER,
                errors_count INTEGER,
                planned_to_enrich INTEGER,
                remaining_to_enrich INTEGER,
                warning_reason TEXT,
                provider_http_status INTEGER,
                provider_message TEXT,
                strategy TEXT
            )
            """
        )
        _ensure_column(conn, "runs", "planned_to_enrich", "INTEGER")
        _ensure_column(conn, "runs", "remaining_to_enrich", "INTEGER")
        _ensure_column(conn, "runs", "warning_reason", "TEXT")
        _ensure_column(conn, "runs", "provider_http_status", "INTEGER")
        _ensure_column(conn, "runs", "provider_message", "TEXT")
        _ensure_column(conn, "runs", "strategy", "TEXT")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS run_steps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                step_name TEXT,
                status TEXT,
                started_at TIMESTAMP,
                ended_at TIMESTAMP,
                duration_ms INTEGER,
                details_json TEXT
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_run_steps_run ON run_steps(run_id)")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS api_calls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                step_name TEXT,
                method TEXT,
                url TEXT,
                status_code INTEGER,
                duration_ms INTEGER,
                payload_fingerprint TEXT,
                request_id TEXT,
                response_excerpt TEXT,
                created_at TIMESTAMP
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_api_calls_run ON api_calls(run_id)")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS enrichments (
                cnpj TEXT PRIMARY KEY,
                run_id TEXT,
                site TEXT,
                instagram TEXT,
                linkedin_company TEXT,
                linkedin_people_json TEXT,
                google_maps_url TEXT,
                has_contact_page INTEGER,
                has_form INTEGER,
                tech_stack_json TEXT,
                tech_score INTEGER,
                tech_confidence INTEGER,
                has_marketing INTEGER,
                has_analytics INTEGER,
                has_ecommerce INTEGER,
                has_chat INTEGER,
                signals_json TEXT,
                fetched_url TEXT,
                fetch_status INTEGER,
                fetch_ms INTEGER,
                rendered_used INTEGER,
                contact_quality TEXT,
                notes TEXT,
                enriched_at TIMESTAMP
            )
            """
        )
        _ensure_column(conn, "enrichments", "run_id", "TEXT")
        _ensure_column(conn, "enrichments", "site", "TEXT")
        _ensure_column(conn, "enrichments", "instagram", "TEXT")
        _ensure_column(conn, "enrichments", "linkedin_company", "TEXT")
        _ensure_column(conn, "enrichments", "linkedin_people_json", "TEXT")
        _ensure_column(conn, "enrichments", "google_maps_url", "TEXT")
        _ensure_column(conn, "enrichments", "has_contact_page", "INTEGER")
        _ensure_column(conn, "enrichments", "has_form", "INTEGER")
        _ensure_column(conn, "enrichments", "tech_stack_json", "TEXT")
        _ensure_column(conn, "enrichments", "tech_score", "INTEGER")
        _ensure_column(conn, "enrichments", "contact_quality", "TEXT")
        _ensure_column(conn, "enrichments", "notes", "TEXT")
        _ensure_column(conn, "enrichments", "enriched_at", "TIMESTAMP")
        _ensure_column(conn, "enrichments", "tech_confidence", "INTEGER")
        _ensure_column(conn, "enrichments", "has_marketing", "INTEGER")
        _ensure_column(conn, "enrichments", "has_analytics", "INTEGER")
        _ensure_column(conn, "enrichments", "has_ecommerce", "INTEGER")
        _ensure_column(conn, "enrichments", "has_chat", "INTEGER")
        _ensure_column(conn, "enrichments", "signals_json", "TEXT")
        _ensure_column(conn, "enrichments", "fetched_url", "TEXT")
        _ensure_column(conn, "enrichments", "fetch_status", "INTEGER")
        _ensure_column(conn, "enrichments", "fetch_ms", "INTEGER")
        _ensure_column(conn, "enrichments", "rendered_used", "INTEGER")
        _ensure_column(conn, "enrichments", "website_confidence", "INTEGER")
        _ensure_column(conn, "enrichments", "discovery_method", "TEXT")
        _ensure_column(conn, "enrichments", "search_term_used", "TEXT")
        _ensure_column(conn, "enrichments", "candidates_considered", "INTEGER")
        _ensure_column(conn, "enrichments", "website_match_reasons", "TEXT")
        _ensure_column(conn, "enrichments", "excluded_candidates_count", "INTEGER")
        _ensure_column(conn, "enrichments", "golden_techs_found", "TEXT")
        _ensure_column(conn, "enrichments", "tech_sources", "TEXT")
        _ensure_column(conn, "enrichments", "score_version", "TEXT")
        _ensure_column(conn, "enrichments", "score_reasons", "TEXT")
        _ensure_column(conn, "enrichments", "wealth_score", "REAL")
        _ensure_column(conn, "enrichments", "avatar_url", "TEXT")
        _ensure_column(conn, "enrichments", "person_json", "TEXT")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_enrichments_run ON enrichments(run_id)")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS exports (
                export_id TEXT PRIMARY KEY,
                created_at TIMESTAMP,
                filters_json TEXT,
                row_count INTEGER,
                file_path TEXT,
                run_id TEXT,
                arquivo_uuid TEXT,
                payload_fingerprint TEXT,
                status TEXT,
                kind TEXT,
                link TEXT,
                expires_at TIMESTAMP,
                updated_at TIMESTAMP,
                total_linhas INTEGER
            )
            """
        )
        _ensure_column(conn, "exports", "run_id", "TEXT")
        _ensure_column(conn, "exports", "arquivo_uuid", "TEXT")
        _ensure_column(conn, "exports", "payload_fingerprint", "TEXT")
        _ensure_column(conn, "exports", "status", "TEXT")
        _ensure_column(conn, "exports", "kind", "TEXT")
        _ensure_column(conn, "exports", "link", "TEXT")
        _ensure_column(conn, "exports", "expires_at", "TIMESTAMP")
        _ensure_column(conn, "exports", "updated_at", "TIMESTAMP")
        _ensure_column(conn, "exports", "total_linhas", "INTEGER")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_exports_run ON exports(run_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_exports_uuid ON exports(arquivo_uuid)")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TIMESTAMP,
                level TEXT,
                event TEXT,
                detail_json TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS cache (
                key TEXT PRIMARY KEY,
                data TEXT,
                created_at TIMESTAMP,
                expires_at TIMESTAMP
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_cache_expires ON cache(expires_at)")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS extract_cache (
                fingerprint TEXT PRIMARY KEY,
                payload_json TEXT,
                created_at TIMESTAMP,
                expires_at TIMESTAMP,
                result_count INTEGER
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS exports_status_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                arquivo_uuid TEXT,
                status TEXT,
                quantidade INTEGER,
                quantidade_solicitada INTEGER,
                created_at TIMESTAMP,
                raw_json TEXT
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_export_snapshots_uuid ON exports_status_snapshots(arquivo_uuid)")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS exports_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                arquivo_uuid TEXT,
                run_id TEXT,
                file_path TEXT,
                file_size INTEGER,
                file_hash TEXT,
                link TEXT,
                expires_at TIMESTAMP,
                downloaded_at TIMESTAMP
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_exports_files_uuid ON exports_files(arquivo_uuid)")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS hunter_runs (
                id TEXT PRIMARY KEY,
                filters_json TEXT,
                strategy TEXT,
                current_stage TEXT,
                status TEXT,
                total_leads INTEGER,
                processed_count INTEGER,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
            """
        )
        _ensure_column(conn, "hunter_runs", "filters_json", "TEXT")
        _ensure_column(conn, "hunter_runs", "strategy", "TEXT")
        _ensure_column(conn, "hunter_runs", "current_stage", "TEXT")
        _ensure_column(conn, "hunter_runs", "status", "TEXT")
        _ensure_column(conn, "hunter_runs", "total_leads", "INTEGER")
        _ensure_column(conn, "hunter_runs", "processed_count", "INTEGER")
        _ensure_column(conn, "hunter_runs", "created_at", "TIMESTAMP")
        _ensure_column(conn, "hunter_runs", "updated_at", "TIMESTAMP")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS exports_jobs (
                run_id TEXT PRIMARY KEY,
                export_uuid_cd TEXT,
                file_url TEXT,
                expires_at TIMESTAMP,
                file_path_local TEXT
            )
            """
        )
        _ensure_column(conn, "exports_jobs", "export_uuid_cd", "TEXT")
        _ensure_column(conn, "exports_jobs", "file_url", "TEXT")
        _ensure_column(conn, "exports_jobs", "expires_at", "TIMESTAMP")
        _ensure_column(conn, "exports_jobs", "file_path_local", "TEXT")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP
            )
            """
        )
        _ensure_column(conn, "config", "value", "TEXT")
        _ensure_column(conn, "config", "updated_at", "TIMESTAMP")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS webhook_deliveries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                lead_cnpj TEXT,
                status TEXT,
                response_code INTEGER,
                timestamp TIMESTAMP
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_run ON webhook_deliveries(run_id)")
        _ensure_column(conn, "webhook_deliveries", "lead_cnpj", "TEXT")
        _ensure_column(conn, "webhook_deliveries", "status", "TEXT")
        _ensure_column(conn, "webhook_deliveries", "response_code", "INTEGER")
        _ensure_column(conn, "webhook_deliveries", "timestamp", "TIMESTAMP")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS errors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                step_name TEXT,
                lead_id TEXT,
                error TEXT,
                traceback TEXT,
                created_at TIMESTAMP
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_errors_run ON errors(run_id)")

        # Ensure legacy tables have new columns if they already existed.
        # Migrate legacy enrichment_runs into runs once.
        if _table_exists(conn, "enrichment_runs"):
            has_runs = conn.execute("SELECT COUNT(*) AS cnt FROM runs").fetchone()["cnt"]
            if has_runs == 0:
                conn.execute(
                    """
                    INSERT INTO runs (run_id, created_at, params_json, status, total_leads, enriched_count, errors_count)
                    SELECT run_id, created_at, params_json, status, total_leads, enriched_count, errors_count
                    FROM enrichment_runs
                    """
                )
    global _SCHEMA_READY
    _SCHEMA_READY = True


def log_event(level: str, event: str, detail: Optional[Dict[str, Any]] = None) -> None:
    detail_json = json.dumps(detail or {}, ensure_ascii=False)
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO logs (created_at, level, event, detail_json) VALUES (?, ?, ?, ?)",
            (_utcnow(), level, event, detail_json),
        )


def fetch_logs(limit: int = 50, run_id: Optional[str] = None) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        if run_id:
            like = f'%\"run_id\": \"{run_id}\"%'
            rows = conn.execute(
                "SELECT * FROM logs WHERE detail_json LIKE ? ORDER BY created_at DESC LIMIT ?",
                (like, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM logs ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
    return [dict(row) for row in rows]


def cache_get(key: str) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT data FROM cache WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)",
            (key, _utcnow()),
        ).fetchone()
        if not row:
            return None
        return json.loads(row["data"])


def cache_set(key: str, data: Dict[str, Any], ttl_hours: Optional[int] = 24) -> None:
    expires_at = None
    if ttl_hours:
        expires_at = (datetime.utcnow() + timedelta(hours=ttl_hours)).strftime("%Y-%m-%d %H:%M:%S")
    with get_conn() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO cache (key, data, created_at, expires_at) VALUES (?, ?, ?, ?)",
            (key, json.dumps(data, ensure_ascii=False), _utcnow(), expires_at),
        )


def extract_cache_get(fingerprint: str) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT payload_json, result_count, created_at, expires_at
            FROM extract_cache
            WHERE fingerprint = ? AND (expires_at IS NULL OR expires_at > ?)
            """,
            (fingerprint, _utcnow()),
        ).fetchone()
        if not row:
            return None
        return {
            "payload": json.loads(row["payload_json"]),
            "result_count": row["result_count"],
            "created_at": row["created_at"],
        }


def extract_cache_set(fingerprint: str, payload: Dict[str, Any], result_count: int, ttl_hours: int = 24) -> None:
    expires_at = (datetime.utcnow() + timedelta(hours=ttl_hours)).strftime("%Y-%m-%d %H:%M:%S")
    with get_conn() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO extract_cache
            (fingerprint, payload_json, created_at, expires_at, result_count)
            VALUES (?, ?, ?, ?, ?)
            """,
            (fingerprint, json.dumps(payload, ensure_ascii=False), _utcnow(), expires_at, result_count),
        )


def insert_leads_raw(
    leads: List[Dict[str, Any]],
    source: str,
    run_id: Optional[str] = None,
    export_uuid: Optional[str] = None,
) -> None:
    if not leads:
        return
    rows = []
    for lead in leads:
        rows.append((
            lead.get("cnpj"),
            json.dumps(lead, ensure_ascii=False),
            _utcnow(),
            source,
            run_id,
            export_uuid,
        ))
    with get_conn() as conn:
        conn.executemany(
            """
            INSERT OR IGNORE INTO leads_raw
            (cnpj, payload_json, fetched_at, source, run_id, export_uuid)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )


def upsert_leads_raw(
    leads: List[Dict[str, Any]],
    source: str,
    run_id: Optional[str] = None,
    export_uuid: Optional[str] = None,
) -> None:
    if not leads:
        return
    cnpjs = [lead.get("cnpj") for lead in leads if lead.get("cnpj")]
    if not cnpjs:
        return
    placeholders = ",".join(["?"] * len(cnpjs))
    with get_conn() as conn:
        if run_id:
            rows = conn.execute(
                f"SELECT cnpj FROM leads_raw WHERE run_id = ? AND cnpj IN ({placeholders})",
                [run_id, *cnpjs],
            ).fetchall()
        else:
            rows = conn.execute(
                f"SELECT cnpj FROM leads_raw WHERE cnpj IN ({placeholders})",
                cnpjs,
            ).fetchall()
    existing = {row["cnpj"] for row in rows}
    to_insert: List[Tuple[Any, ...]] = []
    to_update: List[Tuple[Any, ...]] = []
    now = _utcnow()
    for lead in leads:
        cnpj = lead.get("cnpj")
        payload = json.dumps(lead, ensure_ascii=False)
        if cnpj in existing:
            if run_id:
                to_update.append((payload, now, source, export_uuid, run_id, cnpj))
            else:
                to_update.append((payload, now, source, export_uuid, cnpj))
        else:
            to_insert.append((cnpj, payload, now, source, run_id, export_uuid))
    with get_conn() as conn:
        if to_insert:
            conn.executemany(
                """
                INSERT INTO leads_raw
                (cnpj, payload_json, fetched_at, source, run_id, export_uuid)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                to_insert,
            )
        if to_update:
            if run_id:
                conn.executemany(
                    """
                    UPDATE leads_raw
                    SET payload_json = ?, fetched_at = ?, source = ?, export_uuid = ?
                    WHERE run_id = ? AND cnpj = ?
                    """,
                    to_update,
                )
            else:
                conn.executemany(
                    """
                    UPDATE leads_raw
                    SET payload_json = ?, fetched_at = ?, source = ?, export_uuid = ?
                    WHERE cnpj = ?
                    """,
                    to_update,
                )


def fetch_leads_raw_by_source(source: str) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT payload_json FROM leads_raw WHERE source = ?",
            (source,),
        ).fetchall()
    return [json.loads(r["payload_json"]) for r in rows]


def fetch_leads_raw_by_run(run_id: str) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT payload_json FROM leads_raw WHERE run_id = ?",
            (run_id,),
        ).fetchall()
    return [json.loads(r["payload_json"]) for r in rows]


def count_leads_raw_between(start_ts: str, end_ts: str) -> int:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM leads_raw WHERE fetched_at BETWEEN ? AND ?",
            (start_ts, end_ts),
        ).fetchone()
    return int(row["cnt"] or 0)


def list_leads_raw_sources_between(start_ts: str, end_ts: str) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT source, COUNT(*) AS cnt
            FROM leads_raw
            WHERE fetched_at BETWEEN ? AND ?
            GROUP BY source
            ORDER BY cnt DESC
            """,
            (start_ts, end_ts),
        ).fetchall()
    return [dict(row) for row in rows]


def upsert_socios_from_leads(leads: List[Dict[str, Any]]) -> None:
    if not leads:
        return
    _ensure_schema()
    rows: List[Tuple[Any, ...]] = []
    cnpjs: set = set()

    def _parse_socios(raw: Any) -> List[Dict[str, Any]]:
        if not raw:
            return []
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
            except Exception:
                return []
            raw = parsed
        if isinstance(raw, dict):
            return [raw]
        if isinstance(raw, list):
            return [item for item in raw if item]
        return []

    for lead in leads:
        cnpj = lead.get("cnpj")
        socios_raw = lead.get("socios") or lead.get("socios_json") or []
        socios = _parse_socios(socios_raw)
        if not cnpj or not socios:
            continue
        cnpjs.add(cnpj)
        for socio in socios:
            if isinstance(socio, dict):
                nome = socio.get("nome_socio") or socio.get("nome") or socio.get("socio") or socio.get("name") or ""
                qualificacao = socio.get("qualificacao") or socio.get("qual") or socio.get("qualificacao_socio") or ""
                cpf = socio.get("cpf") or socio.get("documento") or ""
                idade = socio.get("idade")
                fonte = socio.get("fonte") or lead.get("fonte") or lead.get("source")
            else:
                nome = str(socio)
                qualificacao = ""
                cpf = ""
                idade = None
                fonte = lead.get("fonte") or lead.get("source")
            nome = (nome or "").strip()
            qualificacao = (qualificacao or "").strip()
            if not nome:
                continue
            rows.append((cnpj, nome, cpf, idade, qualificacao, fonte, _utcnow()))

    if not rows or not cnpjs:
        return

    placeholders = ",".join(["?"] * len(cnpjs))
    with get_conn() as conn:
        conn.execute(f"DELETE FROM socios WHERE cnpj IN ({placeholders})", list(cnpjs))
        conn.executemany(
            """
            INSERT INTO socios (cnpj, nome_socio, cpf, idade, qualificacao, fonte, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )


def upsert_leads_clean(leads: List[Dict[str, Any]]) -> None:
    if not leads:
        return
    _ensure_schema()
    rows = []
    for lead in leads:
        socios = lead.get("socios")
        if socios is None:
            socios = lead.get("socios_json", [])
        socios_json = socios if isinstance(socios, str) else json.dumps(socios, ensure_ascii=False)
        rows.append(
            (
                lead.get("cnpj"),
                lead.get("razao_social"),
                lead.get("nome_fantasia"),
                lead.get("cnae"),
                lead.get("cnae_desc"),
                lead.get("porte"),
                lead.get("natureza_juridica"),
                lead.get("capital_social"),
                lead.get("municipio"),
                lead.get("uf"),
                lead.get("endereco_norm"),
                json.dumps(lead.get("telefones_norm", []), ensure_ascii=False),
                json.dumps(lead.get("emails_norm", []), ensure_ascii=False),
                socios_json,
                json.dumps(lead.get("flags", {}), ensure_ascii=False),
                lead.get("score_v1"),
                lead.get("score_v2"),
                lead.get("score_label"),
                lead.get("contact_quality"),
                _utcnow(),
            )
        )
    with get_conn() as conn:
        conn.executemany(
            """
            INSERT INTO leads_clean (
                cnpj, razao_social, nome_fantasia, cnae, cnae_desc, porte,
                natureza_juridica, capital_social, municipio, uf, endereco_norm,
                telefones_norm, emails_norm, socios_json, flags_json, score_v1, score_v2,
                score_label, contact_quality, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(cnpj) DO UPDATE SET
                razao_social=excluded.razao_social,
                nome_fantasia=excluded.nome_fantasia,
                cnae=excluded.cnae,
                cnae_desc=excluded.cnae_desc,
                porte=excluded.porte,
                natureza_juridica=excluded.natureza_juridica,
                capital_social=excluded.capital_social,
                municipio=excluded.municipio,
                uf=excluded.uf,
                endereco_norm=excluded.endereco_norm,
                telefones_norm=excluded.telefones_norm,
                emails_norm=excluded.emails_norm,
                socios_json=excluded.socios_json,
                flags_json=excluded.flags_json,
                score_v1=excluded.score_v1,
                score_v2=excluded.score_v2,
                score_label=excluded.score_label,
                contact_quality=excluded.contact_quality,
                updated_at=excluded.updated_at
            """,
            rows,
        )


def upsert_enrichment(cnpj: str, data: Dict[str, Any]) -> None:
    _ensure_schema()
    with get_conn() as conn:
        try:
            conn.execute(
                """
                INSERT INTO enrichments (
                    cnpj, run_id, site, instagram, linkedin_company,
                    linkedin_people_json, google_maps_url, has_contact_page,
                    has_form, tech_stack_json, tech_score, tech_confidence,
                    has_marketing, has_analytics, has_ecommerce, has_chat,
                    signals_json, fetched_url, fetch_status, fetch_ms,
                    rendered_used, contact_quality, notes, enriched_at,
                    website_confidence, discovery_method, search_term_used,
                    candidates_considered, website_match_reasons, excluded_candidates_count,
                    golden_techs_found, tech_sources, score_version, score_reasons,
                    wealth_score, avatar_url, person_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(cnpj) DO UPDATE SET
                    run_id=excluded.run_id,
                    site=excluded.site,
                    instagram=excluded.instagram,
                    linkedin_company=excluded.linkedin_company,
                    linkedin_people_json=excluded.linkedin_people_json,
                    google_maps_url=excluded.google_maps_url,
                    has_contact_page=excluded.has_contact_page,
                    has_form=excluded.has_form,
                    tech_stack_json=excluded.tech_stack_json,
                    tech_score=excluded.tech_score,
                    tech_confidence=excluded.tech_confidence,
                    has_marketing=excluded.has_marketing,
                    has_analytics=excluded.has_analytics,
                    has_ecommerce=excluded.has_ecommerce,
                    has_chat=excluded.has_chat,
                    signals_json=excluded.signals_json,
                    fetched_url=excluded.fetched_url,
                    fetch_status=excluded.fetch_status,
                    fetch_ms=excluded.fetch_ms,
                    rendered_used=excluded.rendered_used,
                    contact_quality=excluded.contact_quality,
                    notes=excluded.notes,
                    enriched_at=excluded.enriched_at,
                    website_confidence=excluded.website_confidence,
                    discovery_method=excluded.discovery_method,
                    search_term_used=excluded.search_term_used,
                    candidates_considered=excluded.candidates_considered,
                    website_match_reasons=excluded.website_match_reasons,
                    excluded_candidates_count=excluded.excluded_candidates_count,
                    golden_techs_found=excluded.golden_techs_found,
                    tech_sources=excluded.tech_sources,
                    score_version=excluded.score_version,
                    score_reasons=excluded.score_reasons,
                    wealth_score=excluded.wealth_score,
                    avatar_url=excluded.avatar_url,
                    person_json=excluded.person_json
                """,
                (
                    cnpj,
                    data.get("run_id"),
                    data.get("site"),
                    data.get("instagram"),
                    data.get("linkedin_company"),
                    json.dumps(data.get("linkedin_people", []), ensure_ascii=False),
                    data.get("google_maps_url"),
                    int(bool(data.get("has_contact_page"))),
                    int(bool(data.get("has_form"))),
                    json.dumps(data.get("tech_stack", {}), ensure_ascii=False),
                    data.get("tech_score"),
                    data.get("tech_confidence"),
                    int(bool(data.get("has_marketing"))),
                    int(bool(data.get("has_analytics"))),
                    int(bool(data.get("has_ecommerce"))),
                    int(bool(data.get("has_chat"))),
                    json.dumps(data.get("signals", {}), ensure_ascii=False),
                    data.get("fetched_url"),
                    data.get("fetch_status"),
                    data.get("fetch_ms"),
                    int(bool(data.get("rendered_used"))),
                    data.get("contact_quality"),
                    data.get("notes"),
                    data.get("enriched_at") or _utcnow(),
                    data.get("website_confidence"),
                    data.get("discovery_method"),
                    data.get("search_term_used"),
                    data.get("candidates_considered"),
                    json.dumps(data.get("website_match_reasons", []), ensure_ascii=False),
                    data.get("excluded_candidates_count"),
                    json.dumps(data.get("golden_techs_found", []), ensure_ascii=False),
                    json.dumps(data.get("tech_sources", {}), ensure_ascii=False),
                    data.get("score_version"),
                    json.dumps(data.get("score_reasons", []), ensure_ascii=False),
                    data.get("wealth_score"),
                    data.get("avatar_url"),
                    data.get("person_json")
                    if isinstance(data.get("person_json"), str)
                    else json.dumps(data.get("person_json", {}), ensure_ascii=False),
                ),
            )
        except sqlite3.OperationalError as exc:
            logger.exception("upsert_enrichment failed (cnpj=%s): %s", cnpj, exc)
            raise


def upsert_person_enrichment(
    cnpj: str,
    wealth_score: Any,
    avatar_url: Optional[str],
    person_json: Any,
) -> None:
    if not cnpj:
        return
    _ensure_schema()
    payload = person_json if isinstance(person_json, str) else json.dumps(person_json or {}, ensure_ascii=False)
    now = _utcnow()
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO enrichments (cnpj, wealth_score, avatar_url, person_json, enriched_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(cnpj) DO UPDATE SET
                wealth_score=excluded.wealth_score,
                avatar_url=excluded.avatar_url,
                person_json=excluded.person_json,
                enriched_at=COALESCE(enrichments.enriched_at, excluded.enriched_at)
            """,
            (cnpj, wealth_score, avatar_url, payload, now),
        )


def create_run(params: Dict[str, Any]) -> str:
    run_id = str(uuid4())
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO runs
            (run_id, created_at, params_json, status, total_leads, enriched_count, errors_count)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                _utcnow(),
                json.dumps(params, ensure_ascii=False),
                "queued",
                0,
                0,
                0,
            ),
        )
    return run_id


def update_run(run_id: str, **fields: Any) -> None:
    if not fields:
        return
    keys = []
    values = []
    for key, value in fields.items():
        keys.append(f"{key}=?")
        values.append(value)
    values.append(run_id)
    with get_conn() as conn:
        conn.execute(
            f"UPDATE runs SET {', '.join(keys)} WHERE run_id = ?",
            values,
        )


def list_runs(limit: int = 50) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM runs ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def get_run(run_id: str) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
    return dict(row) if row else None


def create_hunter_run(
    filters: Dict[str, Any],
    strategy: Optional[str] = None,
    status: str = "RUNNING",
    current_stage: str = "PROBE",
) -> str:
    run_id = str(uuid4())
    now = _utcnow()
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO hunter_runs
            (id, filters_json, strategy, current_stage, status, total_leads, processed_count, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                json.dumps(filters, ensure_ascii=False),
                strategy,
                current_stage,
                status,
                0,
                0,
                now,
                now,
            ),
        )
    return run_id


def update_hunter_run(run_id: str, **fields: Any) -> None:
    if not fields:
        return
    if "updated_at" not in fields:
        fields["updated_at"] = _utcnow()
    keys = []
    values = []
    for key, value in fields.items():
        keys.append(f"{key}=?")
        values.append(value)
    values.append(run_id)
    with get_conn() as conn:
        conn.execute(
            f"UPDATE hunter_runs SET {', '.join(keys)} WHERE id = ?",
            values,
        )


def get_hunter_run(run_id: str) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM hunter_runs WHERE id = ?",
            (run_id,),
        ).fetchone()
    return dict(row) if row else None


def list_hunter_runs(limit: int = 50) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM hunter_runs ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def list_hunter_runs_by_status(statuses: Iterable[str], limit: int = 50) -> List[Dict[str, Any]]:
    status_list = [status for status in statuses if status]
    if not status_list:
        return []
    placeholders = ", ".join("?" for _ in status_list)
    query = f"SELECT * FROM hunter_runs WHERE status IN ({placeholders}) ORDER BY created_at DESC LIMIT ?"
    with get_conn() as conn:
        rows = conn.execute(query, (*status_list, limit)).fetchall()
    return [dict(row) for row in rows]


def upsert_export_job(
    run_id: str,
    export_uuid_cd: Optional[str] = None,
    file_url: Optional[str] = None,
    expires_at: Optional[str] = None,
    file_path_local: Optional[str] = None,
) -> None:
    with get_conn() as conn:
        cur = conn.execute(
            """
            UPDATE exports_jobs
            SET export_uuid_cd = ?, file_url = ?, expires_at = ?, file_path_local = ?
            WHERE run_id = ?
            """,
            (export_uuid_cd, file_url, expires_at, file_path_local, run_id),
        )
        if cur.rowcount == 0:
            conn.execute(
                """
                INSERT INTO exports_jobs
                (run_id, export_uuid_cd, file_url, expires_at, file_path_local)
                VALUES (?, ?, ?, ?, ?)
                """,
                (run_id, export_uuid_cd, file_url, expires_at, file_path_local),
            )


def get_export_job(run_id: str) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM exports_jobs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
    return dict(row) if row else None


def list_export_jobs(limit: int = 50) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM exports_jobs ORDER BY rowid DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def config_get(key: str) -> Optional[str]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT value FROM config WHERE key = ?",
            (key,),
        ).fetchone()
    return row["value"] if row else None


def config_set(key: str, value: str) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO config (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value=excluded.value,
                updated_at=excluded.updated_at
            """,
            (key, value, _utcnow()),
        )


def record_webhook_delivery(
    run_id: Optional[str],
    lead_cnpj: Optional[str],
    status: str,
    response_code: Optional[int],
) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO webhook_deliveries
            (run_id, lead_cnpj, status, response_code, timestamp)
            VALUES (?, ?, ?, ?, ?)
            """,
            (run_id, lead_cnpj, status, response_code, _utcnow()),
        )


def fetch_webhook_deliveries(run_id: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        if run_id:
            rows = conn.execute(
                """
                SELECT * FROM webhook_deliveries
                WHERE run_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (run_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM webhook_deliveries ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
    return [dict(row) for row in rows]


def fetch_leads_clean(limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM leads_clean ORDER BY score_v2 DESC NULLS LAST LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()
    return [dict(row) for row in rows]


def query_leads_clean(
    min_score: Optional[int] = None,
    contact_quality: Optional[str] = None,
    municipio: Optional[str] = None,
    uf: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    order_by: str = "score_v2 DESC",
) -> List[Dict[str, Any]]:
    clauses = []
    params: List[Any] = []
    if min_score is not None:
        clauses.append("score_v2 >= ?")
        params.append(min_score)
    if contact_quality:
        clauses.append("contact_quality = ?")
        params.append(contact_quality)
    if municipio:
        clauses.append("municipio = ?")
        params.append(municipio)
    if uf:
        clauses.append("uf = ?")
        params.append(uf)

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    sql = f"SELECT * FROM leads_clean {where_sql} ORDER BY {order_by} LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(row) for row in rows]


def fetch_enrichment_vault(limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT e.*, c.razao_social, c.nome_fantasia, c.cnae, c.municipio, c.uf,
                   c.score_v2, c.score_label, c.contact_quality, c.telefones_norm,
                   c.emails_norm, c.socios_json, c.flags_json, c.porte, c.endereco_norm
            FROM enrichments e
            LEFT JOIN leads_clean c ON c.cnpj = e.cnpj
            ORDER BY e.enriched_at DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        ).fetchall()
    return [dict(row) for row in rows]


def _build_vault_filters(
    filters: Dict[str, Any],
    status_filter: str = "all",
) -> Tuple[str, List[Any]]:
    clauses = []
    params: List[Any] = []
    min_score = filters.get("min_score")
    min_tech_score = filters.get("min_tech_score")
    min_wealth = filters.get("min_wealth")
    contact_quality = filters.get("contact_quality")
    municipio = filters.get("municipio")
    uf = filters.get("uf")
    has_marketing = filters.get("has_marketing")

    if min_score is not None:
        clauses.append("lc.score_v2 >= ?")
        params.append(min_score)
    if min_tech_score is not None:
        clauses.append("e.tech_score >= ?")
        params.append(min_tech_score)
    if min_wealth is not None:
        clauses.append("e.wealth_score >= ?")
        params.append(min_wealth)
    if contact_quality:
        clauses.append("lc.contact_quality = ?")
        params.append(contact_quality)
    if municipio:
        clauses.append("lc.municipio = ?")
        params.append(municipio)
    if uf:
        clauses.append("lc.uf = ?")
        params.append(uf)
    if has_marketing is not None:
        clauses.append("e.has_marketing = ?")
        params.append(1 if has_marketing else 0)
    if status_filter == "enriched":
        clauses.append("e.enriched_at IS NOT NULL")
    elif status_filter == "pending":
        clauses.append("e.enriched_at IS NULL")

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return where_sql, params


def _vault_select_sql() -> str:
    return (
        "SELECT "
        "lc.cnpj, lc.razao_social, lc.nome_fantasia, lc.cnae, lc.cnae_desc, lc.porte, "
        "lc.natureza_juridica, lc.capital_social, lc.municipio, lc.uf, lc.endereco_norm, "
        "lc.telefones_norm, lc.emails_norm, lc.socios_json, lc.flags_json, lc.score_v1, lc.score_v2, "
        "lc.score_label, lc.contact_quality, lc.updated_at, "
        "e.run_id, e.site, e.instagram, e.linkedin_company, e.linkedin_people_json, "
        "e.google_maps_url, e.has_contact_page, e.has_form, e.tech_stack_json, "
        "e.tech_score, e.tech_confidence, e.has_marketing, e.has_analytics, "
        "e.has_ecommerce, e.has_chat, e.signals_json, e.fetched_url, e.fetch_status, "
        "e.fetch_ms, e.rendered_used, e.notes, e.enriched_at, "
        "e.website_confidence, e.discovery_method, e.search_term_used, "
        "e.candidates_considered, e.website_match_reasons, e.excluded_candidates_count, "
        "e.golden_techs_found, e.tech_sources, e.score_version, e.score_reasons, "
        "e.wealth_score, e.avatar_url, e.person_json "
        "FROM leads_clean lc LEFT JOIN enrichments e ON lc.cnpj = e.cnpj "
    )


def get_vault_data(
    page: int,
    page_size: int,
    filters: Dict[str, Any],
    status_filter: str = "all",
) -> List[Dict[str, Any]]:
    where_sql, params = _build_vault_filters(filters, status_filter)
    order_sql = (
        "ORDER BY (e.enriched_at IS NULL) ASC, "
        "COALESCE(e.wealth_score, 0) DESC, "
        "e.enriched_at DESC, lc.score_v2 DESC"
    )
    offset = max(0, (page - 1) * page_size)
    sql = f"{_vault_select_sql()} {where_sql} {order_sql} LIMIT ? OFFSET ?"
    params.extend([page_size, offset])
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(row) for row in rows]


def query_enrichment_vault(
    min_score: Optional[int] = None,
    min_tech_score: Optional[int] = None,
    min_wealth: Optional[float] = None,
    contact_quality: Optional[str] = None,
    municipio: Optional[str] = None,
    has_marketing: Optional[bool] = None,
    limit: int = 100,
    offset: int = 0,
    status_filter: str = "enriched",
) -> List[Dict[str, Any]]:
    filters = {
        "min_score": min_score,
        "min_tech_score": min_tech_score,
        "min_wealth": min_wealth,
        "contact_quality": contact_quality,
        "municipio": municipio,
        "has_marketing": has_marketing,
    }
    where_sql, params = _build_vault_filters(filters, status_filter)
    order_sql = (
        "ORDER BY (e.enriched_at IS NULL) ASC, "
        "COALESCE(e.wealth_score, 0) DESC, "
        "e.enriched_at DESC, lc.score_v2 DESC"
    )
    sql = f"{_vault_select_sql()} {where_sql} {order_sql} LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(row) for row in rows]


def count_enrichment_vault(
    min_score: Optional[int] = None,
    min_tech_score: Optional[int] = None,
    min_wealth: Optional[float] = None,
    contact_quality: Optional[str] = None,
    municipio: Optional[str] = None,
    has_marketing: Optional[bool] = None,
    status_filter: str = "enriched",
) -> int:
    filters = {
        "min_score": min_score,
        "min_tech_score": min_tech_score,
        "min_wealth": min_wealth,
        "contact_quality": contact_quality,
        "municipio": municipio,
        "has_marketing": has_marketing,
    }
    return count_vault_data(filters, status_filter=status_filter)


def count_vault_data(
    filters: Dict[str, Any],
    status_filter: str = "all",
) -> int:
    where_sql, params = _build_vault_filters(filters, status_filter)
    sql = f"SELECT COUNT(*) AS cnt FROM leads_clean lc LEFT JOIN enrichments e ON lc.cnpj = e.cnpj {where_sql}"
    with get_conn() as conn:
        row = conn.execute(sql, params).fetchone()
    return int(row["cnt"])


def count_leads_clean() -> int:
    with get_conn() as conn:
        row = conn.execute("SELECT COUNT(*) AS cnt FROM leads_clean").fetchone()
    return int(row["cnt"])


def count_enrichments() -> int:
    with get_conn() as conn:
        row = conn.execute("SELECT COUNT(*) AS cnt FROM enrichments").fetchone()
    return int(row["cnt"])


def fetch_enrichments_by_cnpjs(cnpjs: List[str]) -> Dict[str, Dict[str, Any]]:
    if not cnpjs:
        return {}
    placeholders = ",".join(["?"] * len(cnpjs))
    with get_conn() as conn:
        rows = conn.execute(
            f"SELECT * FROM enrichments WHERE cnpj IN ({placeholders})",
            cnpjs,
        ).fetchall()
    result: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        result[row["cnpj"]] = dict(row)
    return result


def fetch_socios_by_cnpjs(cnpjs: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    if not cnpjs:
        return {}
    placeholders = ",".join(["?"] * len(cnpjs))
    with get_conn() as conn:
        rows = conn.execute(
            f"SELECT cnpj, nome_socio, qualificacao, cpf FROM socios WHERE cnpj IN ({placeholders})",
            cnpjs,
        ).fetchall()
    result: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        result.setdefault(row["cnpj"], []).append(
            {
                "nome_socio": row["nome_socio"],
                "qualificacao": row["qualificacao"],
                "cpf": row["cpf"],
            }
        )
    return result


def find_cross_ownership(
    cpf: Optional[str],
    name: Optional[str],
    exclude_cnpj: Optional[str] = None,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    if not cpf and not name:
        return []
    _ensure_schema()
    params: List[Any] = []
    clauses: List[str] = []
    if cpf:
        clauses.append("s.cpf = ?")
        params.append(cpf)
    if name:
        clauses.append("LOWER(s.nome_socio) = ?")
        params.append(str(name).strip().lower())
    where_sql = " OR ".join(clauses) if clauses else "1=0"
    if exclude_cnpj:
        where_sql = f"({where_sql}) AND s.cnpj != ?"
        params.append(exclude_cnpj)

    sql = (
        "SELECT s.cnpj, s.nome_socio, s.qualificacao, lc.razao_social, lc.nome_fantasia "
        "FROM socios s LEFT JOIN leads_clean lc ON lc.cnpj = s.cnpj "
        f"WHERE {where_sql} "
        "ORDER BY lc.razao_social ASC LIMIT ?"
    )
    params.append(limit)
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(row) for row in rows]


def update_lead_scores(cnpj: str, score_v2: int, score_label: str) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE leads_clean
            SET score_v2 = ?, score_label = ?, updated_at = ?
            WHERE cnpj = ?
            """,
            (score_v2, score_label, _utcnow(), cnpj),
        )


def update_enrichment_scoring(
    cnpj: str,
    score_version: str,
    score_reasons: List[str],
) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE enrichments
            SET score_version = ?, score_reasons = ?
            WHERE cnpj = ?
            """,
            (score_version, json.dumps(score_reasons, ensure_ascii=False), cnpj),
        )


def record_export(filters: Dict[str, Any], row_count: int, file_path: str) -> str:
    export_id = str(uuid4())
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO exports
            (export_id, created_at, filters_json, row_count, file_path, kind, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                export_id,
                _utcnow(),
                json.dumps(filters, ensure_ascii=False),
                row_count,
                file_path,
                "local_export",
                _utcnow(),
            ),
        )
    return export_id


def create_casa_export(
    run_id: str,
    arquivo_uuid: str,
    payload_fingerprint: str,
    status: str = "created",
    total_linhas: Optional[int] = None,
) -> str:
    export_id = str(uuid4())
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO exports (
                export_id, created_at, run_id, arquivo_uuid, payload_fingerprint,
                status, kind, total_linhas, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                export_id,
                _utcnow(),
                run_id,
                arquivo_uuid,
                payload_fingerprint,
                status,
                "casa_export",
                total_linhas,
                _utcnow(),
            ),
        )
    return export_id


def update_casa_export(
    arquivo_uuid: str,
    **fields: Any,
) -> None:
    if not fields:
        return
    keys = []
    values = []
    for key, value in fields.items():
        keys.append(f"{key}=?")
        values.append(value)
    values.append(arquivo_uuid)
    with get_conn() as conn:
        conn.execute(
            f"UPDATE exports SET {', '.join(keys)} WHERE arquivo_uuid = ?",
            values,
        )


def list_casa_exports(limit: int = 50) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT * FROM exports
            WHERE kind = 'casa_export'
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def fetch_casa_export(arquivo_uuid: str) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM exports WHERE arquivo_uuid = ? AND kind = 'casa_export'",
            (arquivo_uuid,),
        ).fetchone()
    return dict(row) if row else None


def record_run_step(
    run_id: str,
    step_name: str,
    status: str,
    started_at: Optional[str] = None,
    ended_at: Optional[str] = None,
    duration_ms: Optional[int] = None,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO run_steps
            (run_id, step_name, status, started_at, ended_at, duration_ms, details_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                step_name,
                status,
                started_at,
                ended_at,
                duration_ms,
                json.dumps(details or {}, ensure_ascii=False),
            ),
        )


def fetch_run_steps(run_id: str) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM run_steps WHERE run_id = ? ORDER BY id ASC",
            (run_id,),
        ).fetchall()
    return [dict(row) for row in rows]


def record_api_call(
    run_id: str,
    step_name: str,
    method: str,
    url: str,
    status_code: int,
    duration_ms: int,
    payload_fingerprint: Optional[str] = None,
    request_id: Optional[str] = None,
    response_excerpt: Optional[str] = None,
) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO api_calls (
                run_id, step_name, method, url, status_code, duration_ms,
                payload_fingerprint, request_id, response_excerpt, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                step_name,
                method,
                url,
                status_code,
                duration_ms,
                payload_fingerprint,
                request_id,
                response_excerpt,
                _utcnow(),
            ),
        )


def fetch_api_calls(run_id: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        if run_id:
            rows = conn.execute(
                "SELECT * FROM api_calls WHERE run_id = ? ORDER BY id DESC LIMIT ?",
                (run_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM api_calls ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
    return [dict(row) for row in rows]


def record_error(
    run_id: str,
    step_name: str,
    error: str,
    traceback_text: Optional[str] = None,
    lead_id: Optional[str] = None,
) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO errors (run_id, step_name, lead_id, error, traceback, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (run_id, step_name, lead_id, error, traceback_text, _utcnow()),
        )


def fetch_errors(run_id: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        if run_id:
            rows = conn.execute(
                "SELECT * FROM errors WHERE run_id = ? ORDER BY id DESC LIMIT ?",
                (run_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM errors ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
    return [dict(row) for row in rows]


def record_export_snapshot(
    run_id: Optional[str],
    arquivo_uuid: str,
    status: str,
    quantidade: Optional[int],
    quantidade_solicitada: Optional[int],
    raw: Dict[str, Any],
) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO exports_status_snapshots
            (run_id, arquivo_uuid, status, quantidade, quantidade_solicitada, created_at, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                arquivo_uuid,
                status,
                quantidade,
                quantidade_solicitada,
                _utcnow(),
                json.dumps(raw, ensure_ascii=False),
            ),
        )


def fetch_export_snapshots(arquivo_uuid: str, limit: int = 20) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT * FROM exports_status_snapshots
            WHERE arquivo_uuid = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (arquivo_uuid, limit),
        ).fetchall()
    return [dict(row) for row in rows]


def fetch_recent_export_snapshots(limit: int = 50) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM exports_status_snapshots ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def record_export_file(
    arquivo_uuid: str,
    run_id: Optional[str],
    file_path: str,
    file_size: int,
    file_hash: str,
    link: Optional[str],
    expires_at: Optional[str],
) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO exports_files
            (arquivo_uuid, run_id, file_path, file_size, file_hash, link, expires_at, downloaded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                arquivo_uuid,
                run_id,
                file_path,
                file_size,
                file_hash,
                link,
                expires_at,
                _utcnow(),
            ),
        )


def fetch_export_files(arquivo_uuid: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        if arquivo_uuid:
            rows = conn.execute(
                "SELECT * FROM exports_files WHERE arquivo_uuid = ? ORDER BY id DESC LIMIT ?",
                (arquivo_uuid, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM exports_files ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
    return [dict(row) for row in rows]
