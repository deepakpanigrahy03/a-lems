#!/usr/bin/env python3
"""
Database Verification Script - Dynamically adapts to schema
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from core.config_loader import ConfigLoader
from core.database.manager import DatabaseManager


def print_section(title: str):
    print(f"\n📊 {title}")
    print("=" * 50)


def get_column_names(conn, table):
    """Return list of column names for a table."""
    cursor = conn.execute(f"PRAGMA table_info({table})")
    return [row[1] for row in cursor.fetchall()]


def main():
    print("🔍 A-LEMS DATABASE VERIFICATION")
    print("=" * 60)

    config = ConfigLoader().get_db_config()
    db_path = Path("data/experiments.db")
    if not db_path.exists():
        print(f"\n⚠️ Database file not found at: {db_path.absolute()}")
        return 1

    print(f"📁 Database file: {db_path} ({db_path.stat().st_size / 1024:.1f} KB)")

    db = DatabaseManager(config)

    try:
        with db:
            adapter = db.db
            conn = adapter.conn

            # Get all tables
            cursor = conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
            """)
            tables = [row[0] for row in cursor.fetchall()]

            print(f"\n📋 Found {len(tables)} tables:")
            for table in tables:
                print(f"   • {table}")

            # Show row counts
            print_section("TABLE ROW COUNTS")
            for table in tables:
                cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"{table:25} : {count:6} rows")

            # --- Experiments table details ---
            if "experiments" in tables:
                exp_cols = get_column_names(conn, "experiments")
                print_section("EXPERIMENTS COLUMNS")
                print(f"Columns: {', '.join(exp_cols)}")

                # Determine likely ID column
                id_col = next((c for c in exp_cols if "id" in c.lower()), exp_cols[0])
                name_col = next(
                    (c for c in exp_cols if "name" in c.lower()),
                    exp_cols[1] if len(exp_cols) > 1 else exp_cols[0],
                )
                time_col = next(
                    (c for c in exp_cols if "time" in c.lower()),
                    "timestamp" if "timestamp" in exp_cols else exp_cols[-1],
                )

                cursor = conn.execute(f"""
                    SELECT {id_col}, {name_col}, {time_col}
                    FROM experiments
                    ORDER BY {time_col} DESC
                    LIMIT 3
                """)
                rows = cursor.fetchall()
                print_section("RECENT EXPERIMENTS")
                if rows:
                    for row in rows:
                        print(f"ID: {row[0]} | Name: {row[1]} | Time: {row[2]}")
                else:
                    print("No experiments found")

            # --- Runs table details ---
            if "runs" in tables:
                run_cols = get_column_names(conn, "runs")
                print_section("RUNS COLUMNS")
                print(f"Columns: {', '.join(run_cols)}")

                # Select common columns
                id_col = next(
                    (c for c in run_cols if "run_id" in c.lower() or "id" in c.lower()),
                    run_cols[0],
                )
                exp_id_col = next(
                    (
                        c
                        for c in run_cols
                        if "experiment" in c.lower() and "id" in c.lower()
                    ),
                    None,
                )
                run_num_col = next(
                    (
                        c
                        for c in run_cols
                        if "run_number" in c.lower() or "num" in c.lower()
                    ),
                    None,
                )
                wf_col = next((c for c in run_cols if "workflow" in c.lower()), None)
                energy_col = next(
                    (
                        c
                        for c in run_cols
                        if "energy" in c.lower() and "joules" in c.lower()
                    ),
                    None,
                )
                success_col = next(
                    (c for c in run_cols if "success" in c.lower()), None
                )

                select_cols = [id_col]
                if exp_id_col:
                    select_cols.append(exp_id_col)
                if run_num_col:
                    select_cols.append(run_num_col)
                if wf_col:
                    select_cols.append(wf_col)
                if energy_col:
                    select_cols.append(energy_col)
                if success_col:
                    select_cols.append(success_col)

                query = f"SELECT {', '.join(select_cols)} FROM runs ORDER BY {id_col} DESC LIMIT 5"
                cursor = conn.execute(query)
                rows = cursor.fetchall()

                print_section("RECENT RUNS")
                if rows:
                    for row in rows:
                        parts = []
                        for i, col in enumerate(select_cols):
                            val = row[i]
                            if col == id_col:
                                parts.append(f"Run {val}")
                            elif col == exp_id_col:
                                parts.append(f"Exp={val}")
                            elif col == run_num_col:
                                parts.append(f"#{val}")
                            elif col == wf_col:
                                parts.append(f"{val:8}")
                            elif col == energy_col and val is not None:
                                parts.append(f"{val:.2f} J")
                            elif col == success_col:
                                parts.append("✓" if val else "✗")
                        print(" | ".join(parts))
                else:
                    print("No runs found")

            # --- Integrity checks ---
            print_section("INTEGRITY CHECKS")
            # Only check if we have the needed columns
            if "runs" in tables and "experiments" in tables:
                exp_id_col = next(
                    (
                        c
                        for c in get_column_names(conn, "experiments")
                        if "id" in c.lower()
                    ),
                    None,
                )
                run_exp_col = next(
                    (
                        c
                        for c in get_column_names(conn, "runs")
                        if "experiment" in c.lower()
                    ),
                    None,
                )
                if exp_id_col and run_exp_col:
                    cursor = conn.execute(f"""
                        SELECT COUNT(*) FROM runs r
                        LEFT JOIN experiments e ON r.{run_exp_col} = e.{exp_id_col}
                        WHERE e.{exp_id_col} IS NULL
                    """)
                    orphaned = cursor.fetchone()[0]
                    print(f"Orphaned runs: {orphaned}")

            # Summary
            print_section("SUMMARY")
            exp_count = (
                conn.execute("SELECT COUNT(*) FROM experiments").fetchone()[0]
                if "experiments" in tables
                else 0
            )
            run_count = (
                conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
                if "runs" in tables
                else 0
            )

            if exp_count > 0 and run_count > 0:
                print("✅ Database looks healthy!")
                print(f"   • {exp_count} experiments")
                print(f"   • {run_count} runs")
                print(f"   • {len(tables)} tables")
            else:
                print("⚠️ Database exists but contains minimal data")
                print("   Run experiments with --save-db to populate it")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
