"""
gui/report_engine/query_registry.py
─────────────────────────────────────────────────────────────────────────────
Query Registry — named, versioned, schema-validated SQL queries.

You write the SQL. The engine executes it, introspects the result
columns automatically, routes output to the correct report section.

File layout:
    gui/report_engine/queries/agentic_summary.yaml
    gui/report_engine/queries/ooi_by_provider.yaml
    ...

Each YAML file defines one query. Goal configs reference them by query_id.
─────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations
import sqlite3, yaml, json, logging, hashlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from .schema_discovery import SchemaDiscovery

log = logging.getLogger(__name__)

_QUERIES_DIR = Path(__file__).parent / "queries"
_DB_PATH     = Path(__file__).parent.parent.parent / "data" / "experiments.db"


# ══════════════════════════════════════════════════════════════════════════════
# DATA MODELS
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class OutputColumn:
    column:    str
    col_type:  str = "text"      # text | integer | float | boolean
    label:     str = ""          # display name
    unit:      str = ""          # µJ | ms | % | etc.
    precision: int = 2


@dataclass
class QueryDef:
    query_id:       str
    name:           str
    description:    str
    sql:            str
    output_columns: list[OutputColumn] = field(default_factory=list)
    cacheable:      bool = True
    cache_ttl_min:  int  = 60
    tags:           list[str] = field(default_factory=list)
    version:        str  = "1.0.0"

    def cache_key(self, params: dict | None = None) -> str:
        payload = self.query_id + (json.dumps(params or {}, sort_keys=True))
        return hashlib.md5(payload.encode()).hexdigest()[:12]


@dataclass
class QueryResult:
    query_id:    str
    df:          pd.DataFrame
    columns:     list[OutputColumn]    # resolved column metadata
    cached:      bool = False
    executed_at: datetime = field(default_factory=datetime.utcnow)
    row_count:   int = 0
    error:       str | None = None

    def ok(self) -> bool:
        return self.error is None and not self.df.empty


# ══════════════════════════════════════════════════════════════════════════════
# YAML LOADER
# ══════════════════════════════════════════════════════════════════════════════

def _parse_col(d: dict) -> OutputColumn:
    return OutputColumn(
        column=d["column"],
        col_type=d.get("type", "text"),
        label=d.get("label", d["column"]),
        unit=d.get("unit", ""),
        precision=d.get("precision", 2),
    )


def load_query_from_yaml(path: Path) -> QueryDef:
    with open(path) as f:
        data = yaml.safe_load(f)
    q = data["query"]
    return QueryDef(
        query_id=q["query_id"],
        name=q["name"],
        description=q.get("description", ""),
        sql=q["sql"],
        output_columns=[_parse_col(c) for c in q.get("output_schema", [])],
        cacheable=q.get("cacheable", True),
        cache_ttl_min=q.get("cache_ttl_minutes", 60),
        tags=q.get("tags", []),
        version=q.get("version", "1.0.0"),
    )


# ══════════════════════════════════════════════════════════════════════════════
# IN-MEMORY CACHE
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class _CacheEntry:
    result: QueryResult
    expires_at: datetime


class _QueryCache:
    def __init__(self):
        self._store: dict[str, _CacheEntry] = {}

    def get(self, key: str) -> QueryResult | None:
        entry = self._store.get(key)
        if entry and datetime.utcnow() < entry.expires_at:
            return entry.result
        return None

    def set(self, key: str, result: QueryResult, ttl_min: int) -> None:
        self._store[key] = _CacheEntry(
            result=result,
            expires_at=datetime.utcnow() + timedelta(minutes=ttl_min),
        )

    def invalidate(self, query_id: str) -> None:
        self._store = {
            k: v for k, v in self._store.items()
            if not k.startswith(query_id)
        }

    def clear(self) -> None:
        self._store.clear()


# ══════════════════════════════════════════════════════════════════════════════
# AUTO COLUMN RESOLUTION
# ══════════════════════════════════════════════════════════════════════════════

def _infer_columns(df: pd.DataFrame, declared: list[OutputColumn]) -> list[OutputColumn]:
    """
    If output_schema was declared in YAML, use it.
    Otherwise, infer column metadata from the DataFrame dtypes.
    """
    if declared:
        # Fill in any columns not declared but present in df
        declared_names = {c.column for c in declared}
        extra = []
        for col in df.columns:
            if col not in declared_names:
                dtype = str(df[col].dtype)
                col_type = (
                    "float"   if "float" in dtype else
                    "integer" if "int"   in dtype else
                    "text"
                )
                extra.append(OutputColumn(column=col, col_type=col_type, label=col))
        return declared + extra

    # Pure inference
    inferred = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        col_type = (
            "float"   if "float" in dtype else
            "integer" if "int"   in dtype else
            "text"
        )
        inferred.append(OutputColumn(
            column=col, col_type=col_type,
            label=col.replace("_", " ").title(),
        ))
    return inferred


# ══════════════════════════════════════════════════════════════════════════════
# EXECUTOR
# ══════════════════════════════════════════════════════════════════════════════

def _execute(
    qdef: QueryDef,
    db_path: Path,
    params: dict | None = None,
) -> QueryResult:
    """Execute a query and return a typed QueryResult."""
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        # Substitute {param} placeholders if any
        sql = qdef.sql
        if params:
            for k, v in params.items():
                sql = sql.replace(f"{{{k}}}", str(v))

        df = pd.read_sql_query(sql, conn)
        conn.close()

        cols = _infer_columns(df, qdef.output_columns)
        return QueryResult(
            query_id=qdef.query_id,
            df=df,
            columns=cols,
            row_count=len(df),
        )
    except Exception as e:
        log.error(f"Query '{qdef.query_id}' failed: {e}")
        return QueryResult(
            query_id=qdef.query_id,
            df=pd.DataFrame(),
            columns=[],
            error=str(e),
        )


# ══════════════════════════════════════════════════════════════════════════════
# SINGLETON REGISTRY
# ══════════════════════════════════════════════════════════════════════════════

class QueryRegistry:
    """
    Singleton. Call QueryRegistry.get() everywhere.
    Loads all YAML queries from queries/ dir at startup.
    Validates SQL against live schema before execution.
    """

    _instance: Optional[QueryRegistry] = None
    _queries: dict[str, QueryDef] = {}
    _cache:   _QueryCache = _QueryCache()
    _loaded:  bool = False

    @classmethod
    def get(cls) -> QueryRegistry:
        if cls._instance is None:
            cls._instance = cls()
        if not cls._loaded:
            cls._instance.load_all()
        return cls._instance

    def load_all(
        self,
        extra_dirs: list[Path] | None = None,
    ) -> dict[str, list[str]]:
        loaded, errors = [], []
        dirs = [_QUERIES_DIR] + (extra_dirs or [])
        for d in dirs:
            if not d.exists():
                continue
            for f in sorted(d.glob("*.yaml")):
                try:
                    qdef = load_query_from_yaml(f)
                    self._queries[qdef.query_id] = qdef
                    loaded.append(qdef.query_id)
                except Exception as e:
                    errors.append(f"{f.name}: {e}")
        self._loaded = True
        log.info(f"QueryRegistry: {len(loaded)} queries loaded, {len(errors)} errors")
        return {"loaded": loaded, "errors": errors}

    def register(self, qdef: QueryDef) -> None:
        self._queries[qdef.query_id] = qdef

    def register_from_sql(
        self,
        query_id: str,
        name: str,
        sql: str,
        description: str = "",
        tags: list[str] | None = None,
    ) -> QueryDef:
        """Register an ad-hoc query (e.g. pinned from SQL Query page)."""
        qdef = QueryDef(
            query_id=query_id,
            name=name,
            description=description,
            sql=sql,
            tags=tags or ["ad-hoc"],
            cacheable=False,
        )
        self._queries[query_id] = qdef
        return qdef

    def get_query(self, query_id: str) -> QueryDef | None:
        return self._queries.get(query_id)

    def list_queries(self) -> list[QueryDef]:
        return list(self._queries.values())

    def get_query_ids(self) -> list[str]:
        return list(self._queries.keys())

    def validate(self, query_id: str) -> dict:
        """Validate a query's SQL against the live schema."""
        qdef = self._queries.get(query_id)
        if qdef is None:
            return {"valid": False, "errors": [f"Unknown query_id: {query_id}"]}
        result = SchemaDiscovery.get().validate_sql(qdef.sql)
        return {
            "valid":    result.valid,
            "errors":   result.errors,
            "warnings": result.warnings,
        }

    def validate_sql_string(self, sql: str) -> dict:
        result = SchemaDiscovery.get().validate_sql(sql)
        return {
            "valid":    result.valid,
            "errors":   result.errors,
            "warnings": result.warnings,
        }

    def execute(
        self,
        query_id: str,
        db_path: str | Path | None = None,
        params: dict | None = None,
        force_refresh: bool = False,
    ) -> QueryResult:
        """
        Execute a named query. Returns cached result if available.
        Validates SQL against schema before running.
        """
        qdef = self._queries.get(query_id)
        if qdef is None:
            return QueryResult(
                query_id=query_id,
                df=pd.DataFrame(), columns=[],
                error=f"Unknown query_id: '{query_id}'. "
                      f"Available: {self.get_query_ids()}",
            )

        # Schema validation
        val = SchemaDiscovery.get().validate_sql(qdef.sql)
        if not val.valid:
            log.warning(
                f"Query '{query_id}' has schema errors: {val.errors}. "
                f"Attempting execution anyway."
            )

        # Cache check
        cache_key = qdef.cache_key(params)
        if qdef.cacheable and not force_refresh:
            cached = self._cache.get(cache_key)
            if cached:
                cached.cached = True
                return cached

        # Execute
        path = Path(db_path) if db_path else _DB_PATH
        result = _execute(qdef, path, params)

        if result.ok() and qdef.cacheable:
            self._cache.set(cache_key, result, qdef.cache_ttl_min)

        return result

    def execute_sql(
        self,
        sql: str,
        db_path: str | Path | None = None,
        query_id: str = "ad_hoc",
    ) -> QueryResult:
        """Execute a raw SQL string — used for ad-hoc / pinned queries."""
        adhoc = QueryDef(
            query_id=query_id,
            name="Ad-hoc query",
            description="",
            sql=sql,
            cacheable=False,
        )
        path = Path(db_path) if db_path else _DB_PATH
        return _execute(adhoc, path)

    def invalidate_cache(self, query_id: str | None = None) -> None:
        if query_id:
            self._cache.invalidate(query_id)
        else:
            self._cache.clear()
