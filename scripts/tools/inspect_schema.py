"""
A-LEMS Schema Inspector
Run this on any machine that has the SQLite DB:

    python inspect_schema.py
    python inspect_schema.py --db /path/to/experiments.db

Prints full schema: tables, columns, types, PKs, FKs, indexes.
Paste the output back to Claude.
"""

import sqlite3
import argparse
import os
import sys
from pathlib import Path

def find_db():
    candidates = [
        Path("data/experiments.db"),
        Path("~/mydrive/a-lems/data/experiments.db").expanduser(),
        Path("experiments.db"),
    ]
    for p in candidates:
        if p.exists():
            return p
    return None

def inspect(db_path: Path):
    print(f"\n{'='*60}")
    print(f"  A-LEMS Schema Inspector")
    print(f"  DB: {db_path.resolve()}")
    print(f"  Size: {db_path.stat().st_size / 1024:.1f} KB")
    print(f"{'='*60}\n")

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # All tables
    cur.execute("SELECT name, type FROM sqlite_master WHERE type IN ('table','view') ORDER BY type, name")
    objects = cur.fetchall()

    tables = [r["name"] for r in objects if r["type"] == "table"]
    views  = [r["name"] for r in objects if r["type"] == "view"]

    print(f"TABLES ({len(tables)}): {', '.join(tables)}")
    if views:
        print(f"VIEWS  ({len(views)}): {', '.join(views)}")
    print()

    for tbl in tables:
        print(f"┌─ TABLE: {tbl} {'─'*(50-len(tbl))}")

        # Columns
        cur.execute(f"PRAGMA table_info('{tbl}')")
        cols = cur.fetchall()
        for c in cols:
            pk    = " [PK]"  if c["pk"] else ""
            notnull = " NOT NULL" if c["notnull"] else ""
            dflt  = f" DEFAULT {c['dflt_value']}" if c["dflt_value"] is not None else ""
            print(f"│  {c['cid']:>2}  {c['name']:<35} {c['type']:<15}{pk}{notnull}{dflt}")

        # Foreign keys
        cur.execute(f"PRAGMA foreign_key_list('{tbl}')")
        fks = cur.fetchall()
        if fks:
            print("│  FK:")
            for fk in fks:
                print(f"│      {fk['from']} → {fk['table']}.{fk['to']}  on_delete={fk['on_delete']}")

        # Indexes
        cur.execute(f"PRAGMA index_list('{tbl}')")
        idxs = cur.fetchall()
        if idxs:
            print("│  INDEXES:")
            for idx in idxs:
                cur.execute(f"PRAGMA index_info('{idx['name']}')")
                icols = [r["name"] for r in cur.fetchall()]
                unique = " UNIQUE" if idx["unique"] else ""
                print(f"│      {idx['name']}{unique}: ({', '.join(icols)})")

        # Row count
        cur.execute(f"SELECT COUNT(*) as n FROM '{tbl}'")
        n = cur.fetchone()["n"]
        print(f"│  ROWS: {n}")
        print("│")

        # Sample first row (column names only, no values — privacy safe)
        if n > 0:
            cur.execute(f"SELECT * FROM '{tbl}' LIMIT 1")
            row = cur.fetchone()
            print(f"│  SAMPLE COLUMNS CONFIRMED: {list(row.keys())}")
        print(f"└{'─'*58}\n")

    # Full CREATE statements (source of truth)
    print("\n" + "="*60)
    print("  CREATE STATEMENTS (exact)")
    print("="*60)
    cur.execute("SELECT name, sql FROM sqlite_master WHERE type='table' AND sql IS NOT NULL ORDER BY name")
    for row in cur.fetchall():
        print(f"\n-- {row['name']}")
        print(row["sql"])
        print()

    con.close()
    print("\n" + "="*60)
    print("  Paste everything above this line back to Claude.")
    print("="*60 + "\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=str, default=None, help="Path to experiments.db")
    args = parser.parse_args()

    if args.db:
        db_path = Path(args.db)
    else:
        db_path = find_db()

    if not db_path or not db_path.exists():
        print("ERROR: Could not find experiments.db")
        print("Run with: python inspect_schema.py --db /path/to/experiments.db")
        sys.exit(1)

    inspect(db_path)
