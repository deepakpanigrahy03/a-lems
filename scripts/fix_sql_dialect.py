#!/usr/bin/env python3
"""
scripts/fix_sql_dialect.py
────────────────────────────────────────────────────────────────────────────
One-time script to make all page SQL compatible with both SQLite and PostgreSQL.

Changes made (all backward-compatible with SQLite 3.38+):
  CAST(x AS REAL)           → CAST(x AS DOUBLE PRECISION)
  ROUND(expr, n)            → ROUND(CAST(expr AS NUMERIC), n)
  datetime('now', ...)      → left as-is (only in overview.py, handled by _adapt_sql)
  AS INTEGER                → left as-is (compatible)

Run once:
  python scripts/fix_sql_dialect.py

Run in dry-run mode (no changes):
  python scripts/fix_sql_dialect.py --dry-run

After running, verify SQLite still works:
  python -m alems.tests.test_e2e --sqlite-only
────────────────────────────────────────────────────────────────────────────
"""

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
PAGES_DIR = ROOT / "gui" / "pages"
DB_FILE   = ROOT / "gui" / "db.py"

# Files to fix (all Python files in gui/pages/ + gui/db.py)
TARGET_FILES = sorted(PAGES_DIR.glob("*.py")) + [DB_FILE]


def fix_round(sql: str) -> str:
    """
    ROUND(expr, n) → ROUND(CAST(expr AS NUMERIC), n)
    Uses parenthesis counting to handle nested expressions.
    Works on both SQLite 3.38+ and PostgreSQL.
    """
    result = []
    i = 0
    upper = sql.upper()
    while i < len(sql):
        if upper[i:i+6] == 'ROUND(':
            result.append('ROUND(CAST(')
            i += 6
            depth = 1
            inner = []
            while i < len(sql) and depth > 0:
                c = sql[i]
                if c == '(':
                    depth += 1
                elif c == ')':
                    depth -= 1
                    if depth == 0:
                        break
                inner.append(c)
                i += 1
            i += 1  # skip closing )

            inner_str = ''.join(inner)

            # Find last comma at depth 0
            d, last_comma = 0, -1
            for j, c in enumerate(inner_str):
                if c == '(':
                    d += 1
                elif c == ')':
                    d -= 1
                elif c == ',' and d == 0:
                    last_comma = j

            if last_comma >= 0:
                expr      = inner_str[:last_comma].strip()
                precision = inner_str[last_comma + 1:].strip()
                # Don't double-wrap if already CAST(...AS NUMERIC)
                if 'AS NUMERIC' in expr.upper():
                    result.append(f'{expr}, {precision})')
                else:
                    result.append(f'{expr} AS NUMERIC), {precision})')
            else:
                result.append(f'{inner_str} AS NUMERIC))')
        else:
            result.append(sql[i])
            i += 1

    return ''.join(result)


def fix_cast_real(content: str) -> str:
    """CAST(x AS REAL) → CAST(x AS DOUBLE PRECISION)"""
    return re.sub(
        r'\bAS\s+REAL\b',
        'AS DOUBLE PRECISION',
        content,
        flags=re.IGNORECASE,
    )


def fix_file(path: Path, dry_run: bool = False) -> tuple[bool, list[str]]:
    """
    Apply dialect fixes to a single file.
    Returns (changed, list_of_changes).
    """
    original = path.read_text(encoding='utf-8')
    content  = original

    changes = []

    # Fix CAST AS REAL
    fixed_cast = fix_cast_real(content)
    if fixed_cast != content:
        n = len(re.findall(r'\bAS\s+REAL\b', content, re.IGNORECASE))
        changes.append(f"  {n}x CAST(AS REAL) → CAST(AS DOUBLE PRECISION)")
        content = fixed_cast

    # Fix ROUND — only inside SQL strings (between triple quotes or regular quotes)
    # We fix the whole file content since ROUND only appears in SQL contexts
    fixed_round = fix_round(content)
    if fixed_round != content:
        n = content.upper().count('ROUND(')
        changes.append(f"  {n}x ROUND(expr,n) → ROUND(CAST(expr AS NUMERIC),n)")
        content = fixed_round

    changed = content != original

    if changed and not dry_run:
        # Backup original
        backup = path.with_suffix('.py.sql_bak')
        if not backup.exists():
            backup.write_text(original, encoding='utf-8')
        path.write_text(content, encoding='utf-8')

    return changed, changes


def main():
    parser = argparse.ArgumentParser(description="Fix SQL dialect issues")
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would change without modifying files')
    args = parser.parse_args()

    if args.dry_run:
        print("DRY RUN — no files will be modified\n")

    total_files  = 0
    changed_files = 0

    for path in TARGET_FILES:
        if not path.exists():
            continue
        if path.name.startswith('__'):
            continue

        changed, changes = fix_file(path, dry_run=args.dry_run)
        total_files += 1

        if changed:
            changed_files += 1
            status = "WOULD FIX" if args.dry_run else "FIXED"
            print(f"{status}: {path.relative_to(ROOT)}")
            for c in changes:
                print(c)

    print(f"\n{'Would fix' if args.dry_run else 'Fixed'} "
          f"{changed_files}/{total_files} files")

    if not args.dry_run and changed_files > 0:
        print("\nBackups saved as .py.sql_bak")
        print("Run tests to verify:")
        print("  python -m alems.tests.test_e2e --sqlite-only")


if __name__ == '__main__':
    main()
