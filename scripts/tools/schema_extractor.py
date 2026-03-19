#!/usr/bin/env python3
"""
Schema Extractor for A-LEMS
ULTRA SIMPLE - Guaranteed to work.
"""

import argparse
import sqlite3
import subprocess
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))
from path_loader import config


class SchemaExtractor:
    def __init__(self):
        self.db_path = config.DB_PATH
        self.conn = None
        self.tables = {}
        self.fks = []
        
    def connect(self):
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        
    def extract(self):
        cursor = self.conn.cursor()
        
        # Get tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        for (name,) in cursor.fetchall():
            if name.startswith('sqlite_'):
                continue
            
            # Get columns
            cursor.execute(f"PRAGMA table_info({name})")
            cols = []
            for c in cursor.fetchall():
                cols.append(f"{c[1]}:{c[2]}")
            self.tables[name] = cols
            
            # Get FKs
            cursor.execute(f"PRAGMA foreign_key_list({name})")
            for fk in cursor.fetchall():
                self.fks.append((name, fk[2], fk[3]))
    
    def generate_dot(self):
        lines = ['digraph {']
        lines.append('  rankdir=LR;')
        lines.append('  node [shape=record];')
        lines.append('')
        
        # Tables
        for table, cols in self.tables.items():
            label = f"{table}|" + "|".join(cols[:8])
            if len(cols) > 8:
                label += f"|... +{len(cols)-8}"
            lines.append(f'  "{table}" [label="{label}"];')
        
        lines.append('')
        
        # Relationships
        for from_t, to_t, col in self.fks:
            lines.append(f'  "{from_t}" -> "{to_t}" [label="{col}"];')
        
        lines.append('}')
        return '\n'.join(lines)
    
    def save_svg(self, path):
        dot = self.generate_dot()
        result = subprocess.run(
            ['dot', '-Tsvg', '-o', str(path)],
            input=dot,
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            print(f"✅ Saved: {path}")
        else:
            print(f"❌ Error: {result.stderr}")


def main():
    out = config.DIAGRAMS_OUTPUT / "schema.svg"
    out.parent.mkdir(exist_ok=True)
    
    ex = SchemaExtractor()
    ex.connect()
    ex.extract()
    print(f"📊 {len(ex.tables)} tables, {len(ex.fks)} relationships")
    ex.save_svg(out)


if __name__ == "__main__":
    main()