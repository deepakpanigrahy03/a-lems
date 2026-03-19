#!/usr/bin/env python3
"""
Column Flow Analyzer for A-LEMS
Trace how a database column gets its data
Includes heuristics for dictionary keys and object attributes
"""

import argparse
import ast
import sys
import yaml
import sqlite3
from pathlib import Path

def find_repo_root():
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / '.git').exists():
            return current
        current = current.parent
    return Path.cwd()

REPO_ROOT = find_repo_root()

def load_config():
    path = REPO_ROOT / 'config' / 'app_settings.yaml'
    with open(path) as f:
        return yaml.safe_load(f).get('database', {})

def get_db():
    config = load_config()
    db_path = REPO_ROOT / config['sqlite']['path']
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    print(f"✅ DB: {db_path}")
    return conn

def get_attr_root(node):
    """Extract root object name from nested attributes"""
    while isinstance(node, ast.Attribute):
        node = node.value
    if isinstance(node, ast.Name):
        return node.id
    return "?"

def safe_unparse(node):
    """Safe version of ast.unparse with fallback"""
    if hasattr(ast, 'unparse'):
        try:
            return ast.unparse(node)
        except:
            return "<complex expression>"
    return "<assignment>"

def find_inserts(table):
    """Find files that insert into the given table"""
    files = []
    search_path = REPO_ROOT / "core" / "database"
    for py in search_path.rglob("*.py"):
        try:
            with open(py, encoding='utf-8') as f:
                if f"INSERT INTO {table}" in f.read():
                    files.append(py.relative_to(REPO_ROOT))
        except Exception as e:
            print(f"⚠️  Error reading {py}: {e}", file=sys.stderr)
    return files

def find_dict_key_assignments(dict_name, key_name):
    """Heuristic 1: Find where key is assigned to dictionary"""
    results = []
    search_path = REPO_ROOT / "core"
    for py_file in search_path.rglob("*.py"):
        try:
            with open(py_file, encoding='utf-8') as f:
                tree = ast.parse(f.read())
            
            for node in ast.walk(tree):
                # Pattern 1: dict_name['key_name'] = value
                if isinstance(node, ast.Assign):
                    for target in node.targets:
                        if (isinstance(target, ast.Subscript) and
                            isinstance(target.value, ast.Name) and
                            target.value.id == dict_name and
                            isinstance(target.slice, ast.Constant) and
                            target.slice.value == key_name):
                            
                            deps = []
                            for n in ast.walk(node.value):
                                if isinstance(n, ast.Name):
                                    deps.append(n.id)
                            
                            results.append({
                                'file': py_file.relative_to(REPO_ROOT),
                                'line': node.lineno,
                                'code': safe_unparse(node),
                                'deps': deps,
                                'type': 'dict_assign'
                            })
                
                # Pattern 2: dict_name.update({'key_name': value})
                if (isinstance(node, ast.Expr) and
                    isinstance(node.value, ast.Call) and
                    isinstance(node.value.func, ast.Attribute) and
                    isinstance(node.value.func.value, ast.Name) and
                    node.value.func.value.id == dict_name and
                    node.value.func.attr == 'update'):
                    
                    for arg in node.value.args:
                        if isinstance(arg, ast.Dict):
                            for k, v in zip(arg.keys, arg.values):
                                if isinstance(k, ast.Constant) and k.value == key_name:
                                    deps = []
                                    for n in ast.walk(v):
                                        if isinstance(n, ast.Name):
                                            deps.append(n.id)
                                    results.append({
                                        'file': py_file.relative_to(REPO_ROOT),
                                        'line': node.lineno,
                                        'code': safe_unparse(node),
                                        'deps': deps,
                                        'type': 'dict_update'
                                    })
        except Exception as e:
            print(f"⚠️  Skipping {py_file}: {e}", file=sys.stderr)
    return results

def find_attribute_assignments(attr_name):
    """Heuristic 2: Find where object.attribute gets a value"""
    results = []
    search_path = REPO_ROOT / "core"
    for py_file in search_path.rglob("*.py"):
        try:
            with open(py_file, encoding='utf-8') as f:
                tree = ast.parse(f.read())
            
            for node in ast.walk(tree):
                if not isinstance(node, ast.Assign):
                    continue
                
                for target in node.targets:
                    if (isinstance(target, ast.Attribute) and
                        target.attr == attr_name):
                        
                        deps = []
                        for n in ast.walk(node.value):
                            if isinstance(n, ast.Name):
                                deps.append(n.id)
                        
                        results.append({
                            'file': py_file.relative_to(REPO_ROOT),
                            'line': node.lineno,
                            'code': safe_unparse(node),
                            'deps': deps,
                            'type': 'attr_assign',
                            'obj': get_attr_root(target.value)
                        })
        except Exception as e:
            print(f"⚠️  Skipping {py_file}: {e}", file=sys.stderr)
    return results

def find_assignments(var_name):
    """Basic variable assignment tracking"""
    results = []
    search_path = REPO_ROOT / "core"
    for py_file in search_path.rglob("*.py"):
        try:
            with open(py_file, encoding='utf-8') as f:
                content = f.read()
            if var_name not in content:
                continue
                
            tree = ast.parse(content)
            for node in ast.walk(tree):
                if not isinstance(node, ast.Assign):
                    continue
                for target in node.targets:
                    if not isinstance(target, ast.Name):
                        continue
                    if target.id != var_name:
                        continue
                    
                    deps = []
                    for n in ast.walk(node.value):
                        if isinstance(n, ast.Name):
                            deps.append(n.id)
                    
                    results.append({
                        'file': py_file.relative_to(REPO_ROOT),
                        'line': node.lineno,
                        'code': safe_unparse(node),
                        'deps': deps,
                        'type': 'direct_assign'
                    })
        except Exception as e:
            print(f"⚠️  Skipping {py_file}: {e}", file=sys.stderr)
    return results

def extract_dict_key_from_call(call_node):
    """Extract key name from ml.get('key') pattern"""
    if (isinstance(call_node.func, ast.Attribute) and
        call_node.func.attr == 'get' and
        len(call_node.args) > 0 and
        isinstance(call_node.args[0], ast.Constant)):
        return call_node.args[0].value
    return None

def trace(column, filter_text=None):
    """Trace column with optional text filtering"""
    seen = set()
    to_process = [(column, None, None)]
    all_results = []
    
    while to_process:
        current, dict_name, key_name = to_process.pop(0)
        if current in seen:
            continue
        seen.add(current)
        
        # Basic assignments
        found = find_assignments(current)
        for f in found:
            all_results.append(f)
            for dep in f['deps']:
                if dep not in seen:
                    to_process.append((dep, None, None))
        
        # Dictionary key lookups
        if dict_name and key_name:
            dict_results = find_dict_key_assignments(dict_name, key_name)
            for d in dict_results:
                all_results.append(d)
                for dep in d['deps']:
                    if dep not in seen:
                        to_process.append((dep, None, None))
        
        # Attribute access
        if '.' in current and not current.startswith('_'):
            parts = current.split('.')
            if len(parts) == 2:
                obj, attr = parts
                attr_results = find_attribute_assignments(attr)
                for a in attr_results:
                    all_results.append(a)
                    for dep in a['deps']:
                        if dep not in seen:
                            to_process.append((dep, None, None))
    
    # Apply filter if specified
    if filter_text:
        filtered = []
        filter_lower = filter_text.lower()
        for result in all_results:
            code_lower = result['code'].lower()
            if filter_lower in code_lower:
                filtered.append(result)
        return filtered
    
    return all_results
def build_dependency_chain(sources, start_column):
    """Build a readable dependency chain"""
    chain = [f"🔍 {start_column}"]
    
    # Simple topological sort - group by file for now
    # This can be enhanced later
    return chain

def main():
    parser = argparse.ArgumentParser(description='Trace column data flow through codebase')
    parser.add_argument("--table", required=True, help="Database table name")
    parser.add_argument("--column", required=True, help="Column name to trace")
    parser.add_argument("--filter", help="Additional filter text (combines with column name)")
    args = parser.parse_args()
    
    db = get_db()
    
    # Safe table info query
    cursor = db.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    if args.table not in tables:
        print(f"❌ Table '{args.table}' not found")
        return
    
    cols = db.execute(f"PRAGMA table_info({args.table})").fetchall()
    if args.column not in [c[1] for c in cols]:
        print(f"❌ Column '{args.column}' not found")
        return
    
    # ALWAYS filter by column name at minimum
    filter_text = args.column
    if args.filter:
        filter_text = f"{args.column} {args.filter}"  # Combine with custom filter
    
    print(f"\n🔍 {args.table}.{args.column}")
    print(f"📋 Filter: '{filter_text}'")
    print("=" * 60)
    
    inserts = find_inserts(args.table)
    if inserts:
        print("\n📦 INSERT LOCATIONS:")
        for f in inserts:
            print(f"   📄 {f}")
    
    print("\n📊 DATA SOURCES:")
    sources = trace(args.column, filter_text)
    
    by_file = {}
    for s in sources:
        by_file.setdefault(s['file'], []).append(s)
    
    for f, srcs in by_file.items():
        print(f"\n   📄 {f}")
        for s in sorted(srcs, key=lambda x: x['line']):
            type_icon = {
                'direct_assign': '📝',
                'dict_assign': '📚',
                'dict_update': '📚',
                'attr_assign': '🔧'
            }.get(s['type'], '📄')
            print(f"      {type_icon} Line {s['line']:4d}: {s['code']}")
    
    print(f"\n📈 Total matches: {len(sources)}")
    print("=" * 60)

if __name__ == "__main__":
    main()