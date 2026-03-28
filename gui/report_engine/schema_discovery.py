"""
gui/report_engine/schema_discovery.py
─────────────────────────────────────────────────────────────────────────────
Discovers the live database schema at startup.
Stores column map in schema_discovery table.
Validates all queries before execution — tells you exactly what's wrong.

Public API:
    SchemaDiscovery.get()           → singleton
    .discover(conn)                 → SchemaMap
    .validate_sql(sql)              → ValidationResult
    .has_column(table, col)         → bool
    .safe_columns(table, wanted)    → list of cols that actually exist
    .get_map()                      → SchemaMap
─────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations
import sqlite3, json, logging, re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

# ── DB location ────────────────────────────────────────────────────────────────
_DB_PATH = Path(__file__).parent.parent.parent / "data" / "experiments.db"


# ── Data structures ────────────────────────────────────────────────────────────

@dataclass
class ColumnInfo:
    name: str
    col_type: str           # TEXT | INTEGER | REAL | BLOB | NUMERIC
    notnull: bool = False
    pk: bool = False
    default_value: str | None = None


@dataclass
class TableInfo:
    name: str
    kind: str               # 'table' | 'view'
    columns: dict[str, ColumnInfo] = field(default_factory=dict)

    def has(self, col: str) -> bool:
        return col in self.columns

    def col_names(self) -> list[str]:
        return list(self.columns.keys())


@dataclass
class SchemaMap:
    tables: dict[str, TableInfo] = field(default_factory=dict)
    discovered_at: datetime = field(default_factory=datetime.utcnow)

    def has_table(self, name: str) -> bool:
        return name in self.tables

    def has_column(self, table: str, col: str) -> bool:
        t = self.tables.get(table)
        return t is not None and t.has(col)

    def safe_columns(self, table: str, wanted: list[str]) -> list[str]:
        """Return only the columns from wanted[] that actually exist."""
        t = self.tables.get(table)
        if t is None:
            return []
        return [c for c in wanted if t.has(c)]

    def suggest(self, col: str) -> list[str]:
        """Find tables that DO have this column — for helpful error messages."""
        hits = []
        for tname, tinfo in self.tables.items():
            if tinfo.has(col):
                hits.append(f"{tname}.{col}")
        return hits


@dataclass
class ValidationResult:
    valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def ok(self) -> bool:
        return self.valid and not self.errors


# ── Discovery logic ────────────────────────────────────────────────────────────

def _discover_schema(conn: sqlite3.Connection) -> SchemaMap:
    schema = SchemaMap()

    # Get all tables and views
    rows = conn.execute("""
        SELECT name, type FROM sqlite_master
        WHERE type IN ('table', 'view')
          AND name NOT LIKE 'sqlite_%'
        ORDER BY type, name
    """).fetchall()

    for name, kind in rows:
        try:
            cols_raw = conn.execute(f"PRAGMA table_info({name})").fetchall()
        except Exception as e:
            log.warning(f"Could not introspect {name}: {e}")
            continue

        tinfo = TableInfo(name=name, kind=kind)
        for row in cols_raw:
            # row: (cid, name, type, notnull, dflt_value, pk)
            tinfo.columns[row[1]] = ColumnInfo(
                name=row[1],
                col_type=row[2] or "TEXT",
                notnull=bool(row[3]),
                pk=bool(row[5]),
                default_value=row[4],
            )
        schema.tables[name] = tinfo
        log.debug(f"Discovered {kind} '{name}': {len(tinfo.columns)} columns")

    log.info(
        f"Schema discovery complete: {len(schema.tables)} tables/views, "
        f"{sum(len(t.columns) for t in schema.tables.values())} total columns"
    )
    return schema


# ── SQL reference extractor (lightweight, no parser dependency) ─────────────

_TABLE_ALIAS_RE = re.compile(
    r"\bFROM\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?|"
    r"\bJOIN\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?",
    re.IGNORECASE,
)
_COL_REF_RE = re.compile(r"\b(\w+)\.(\w+)\b")


def _extract_refs(sql: str) -> tuple[dict[str, str], list[tuple[str, str]]]:
    """
    Extract (alias → table_name) map and (alias, column) references from SQL.
    Returns: (alias_map, col_refs)
    """
    alias_map: dict[str, str] = {}
    for m in _TABLE_ALIAS_RE.finditer(sql):
        table = m.group(1) or m.group(3)
        alias = m.group(2) or m.group(4) or table
        if table:
            alias_map[alias] = table
            alias_map[table] = table   # self-alias always works

    col_refs: list[tuple[str, str]] = []
    for m in _COL_REF_RE.finditer(sql):
        alias, col = m.group(1), m.group(2)
        col_refs.append((alias, col))

    return alias_map, col_refs


def _validate_sql(sql: str, schema: SchemaMap) -> ValidationResult:
    result = ValidationResult(valid=True)
    clean = re.sub(r"--[^\n]*", " ", sql)   # strip comments
    clean = re.sub(r"/\*.*?\*/", " ", clean, flags=re.DOTALL)

    alias_map, col_refs = _extract_refs(clean)

    for alias, col in col_refs:
        table_name = alias_map.get(alias)
        if table_name is None:
            result.warnings.append(
                f"Alias '{alias}' not resolved — cannot validate column '{col}'"
            )
            continue

        tinfo = schema.tables.get(table_name)
        if tinfo is None:
            result.errors.append(
                f"Table '{table_name}' does not exist in the database."
            )
            result.valid = False
            continue

        if not tinfo.has(col):
            suggestions = schema.suggest(col)
            msg = f"Column '{alias}.{col}' does not exist in '{table_name}'."
            if suggestions:
                msg += f" Did you mean: {', '.join(suggestions[:3])}?"
            else:
                msg += f" Available columns: {', '.join(tinfo.col_names()[:8])}…"
            result.errors.append(msg)
            result.valid = False

    return result


# ── Persistence ────────────────────────────────────────────────────────────────

_DDL = """
CREATE TABLE IF NOT EXISTS schema_discovery (
    table_name    TEXT NOT NULL,
    column_name   TEXT NOT NULL,
    column_type   TEXT,
    is_pk         INTEGER DEFAULT 0,
    is_notnull    INTEGER DEFAULT 0,
    table_kind    TEXT DEFAULT 'table',
    discovered_at TEXT NOT NULL,
    PRIMARY KEY (table_name, column_name)
);
"""


def _persist(schema: SchemaMap, conn: sqlite3.Connection) -> None:
    conn.execute(_DDL)
    conn.execute("DELETE FROM schema_discovery")
    ts = schema.discovered_at.isoformat()
    rows = []
    for tname, tinfo in schema.tables.items():
        for cname, cinfo in tinfo.columns.items():
            rows.append((
                tname, cname, cinfo.col_type,
                int(cinfo.pk), int(cinfo.notnull),
                tinfo.kind, ts,
            ))
    conn.executemany(
        "INSERT INTO schema_discovery VALUES (?,?,?,?,?,?,?)", rows
    )
    conn.commit()
    log.info(f"Schema persisted: {len(rows)} column records")


def _load_from_db(conn: sqlite3.Connection) -> SchemaMap | None:
    try:
        conn.execute(_DDL)
        rows = conn.execute(
            "SELECT table_name, column_name, column_type, is_pk, is_notnull, "
            "table_kind, discovered_at FROM schema_discovery"
        ).fetchall()
        if not rows:
            return None
        schema = SchemaMap(
            discovered_at=datetime.fromisoformat(rows[0][6])
        )
        for tname, cname, ctype, pk, nn, kind, ts in rows:
            if tname not in schema.tables:
                schema.tables[tname] = TableInfo(name=tname, kind=kind)
            schema.tables[tname].columns[cname] = ColumnInfo(
                name=cname, col_type=ctype or "TEXT",
                pk=bool(pk), notnull=bool(nn)
            )
        return schema
    except Exception as e:
        log.debug(f"Could not load schema from DB: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
# SINGLETON
# ══════════════════════════════════════════════════════════════════════════════

class SchemaDiscovery:
    """
    Singleton. Call SchemaDiscovery.get() everywhere.
    Discovers schema once at startup, then serves from cache.
    """

    _instance: Optional[SchemaDiscovery] = None
    _schema: SchemaMap | None = None

    @classmethod
    def get(cls) -> SchemaDiscovery:
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def ensure_discovered(self, db_path: str | Path | None = None) -> SchemaMap:
        """Discover schema if not already done. Idempotent."""
        if self._schema is not None:
            return self._schema

        path = Path(db_path) if db_path else _DB_PATH
        if not path.exists():
            log.warning(f"DB not found at {path} — returning empty schema")
            self._schema = SchemaMap()
            return self._schema

        conn = sqlite3.connect(str(path))
        try:
            # Try loading cached schema first
            cached = _load_from_db(conn)
            if cached and len(cached.tables) > 0:
                self._schema = cached
                log.info(f"Schema loaded from DB cache: {len(cached.tables)} tables")
                return self._schema

            # Fresh discovery
            self._schema = _discover_schema(conn)
            _persist(self._schema, conn)
        finally:
            conn.close()

        return self._schema

    def refresh(self, db_path: str | Path | None = None) -> SchemaMap:
        """Force a fresh discovery, bypassing cache."""
        self._schema = None
        path = Path(db_path) if db_path else _DB_PATH
        conn = sqlite3.connect(str(path))
        try:
            self._schema = _discover_schema(conn)
            _persist(self._schema, conn)
        finally:
            conn.close()
        return self._schema

    def get_map(self) -> SchemaMap:
        return self._schema or self.ensure_discovered()

    def has_column(self, table: str, col: str) -> bool:
        return self.get_map().has_column(table, col)

    def safe_columns(self, table: str, wanted: list[str]) -> list[str]:
        return self.get_map().safe_columns(table, wanted)

    def validate_sql(self, sql: str) -> ValidationResult:
        return _validate_sql(sql, self.get_map())

    def table_names(self) -> list[str]:
        return list(self.get_map().tables.keys())

    def column_names(self, table: str) -> list[str]:
        t = self.get_map().tables.get(table)
        return t.col_names() if t else []

    def schema_summary(self) -> dict[str, list[str]]:
        """Returns {table_name: [col_names...]} for UI display."""
        return {
            name: tinfo.col_names()
            for name, tinfo in self.get_map().tables.items()
        }
