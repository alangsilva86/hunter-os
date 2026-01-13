"""
SQLite storage layer for Hunter OS.
"""

import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple
from uuid import uuid4

DEFAULT_DB_PATH = os.getenv("HUNTER_DB_PATH", "hunter.db")


def _utcnow() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def get_db_path() -> str:
    return os.getenv("HUNTER_DB_PATH", DEFAULT_DB_PATH)

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
                flags_json TEXT,
                score_v1 REAL,
                score_v2 REAL,
                score_label TEXT,
                contact_quality TEXT,
                updated_at TIMESTAMP
            )
            """
        )
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


def upsert_leads_clean(leads: List[Dict[str, Any]]) -> None:
    if not leads:
        return
    rows = []
    for lead in leads:
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
                telefones_norm, emails_norm, flags_json, score_v1, score_v2,
                score_label, contact_quality, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO enrichments (
                cnpj, run_id, site, instagram, linkedin_company,
                linkedin_people_json, google_maps_url, has_contact_page,
                has_form, tech_stack_json, tech_score, tech_confidence,
                has_marketing, has_analytics, has_ecommerce, has_chat,
                signals_json, fetched_url, fetch_status, fetch_ms,
                rendered_used, contact_quality, notes, enriched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                enriched_at=excluded.enriched_at
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
            ),
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
                   c.score_v2, c.score_label, c.contact_quality
            FROM enrichments e
            LEFT JOIN leads_clean c ON c.cnpj = e.cnpj
            ORDER BY e.enriched_at DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        ).fetchall()
    return [dict(row) for row in rows]


def query_enrichment_vault(
    min_score: Optional[int] = None,
    min_tech_score: Optional[int] = None,
    contact_quality: Optional[str] = None,
    municipio: Optional[str] = None,
    has_marketing: Optional[bool] = None,
    limit: int = 100,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    clauses = []
    params: List[Any] = []
    if min_score is not None:
        clauses.append("c.score_v2 >= ?")
        params.append(min_score)
    if min_tech_score is not None:
        clauses.append("e.tech_score >= ?")
        params.append(min_tech_score)
    if contact_quality:
        clauses.append("c.contact_quality = ?")
        params.append(contact_quality)
    if municipio:
        clauses.append("c.municipio = ?")
        params.append(municipio)
    if has_marketing is not None:
        clauses.append("e.has_marketing = ?")
        params.append(1 if has_marketing else 0)

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    sql = (
        "SELECT e.*, c.razao_social, c.nome_fantasia, c.cnae, c.municipio, c.uf, "
        "c.score_v2, c.score_label, c.contact_quality "
        "FROM enrichments e LEFT JOIN leads_clean c ON c.cnpj = e.cnpj "
        f"{where_sql} ORDER BY e.enriched_at DESC LIMIT ? OFFSET ?"
    )
    params.extend([limit, offset])
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(row) for row in rows]


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
