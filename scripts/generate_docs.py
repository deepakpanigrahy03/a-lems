#!/usr/bin/env python3
"""
A-LEMS Documentation Generator
Reads actual code and generates accurate markdown documentation
"""

import ast
import importlib.util
import inspect
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


class ALEMSDocGenerator:
    def __init__(self, project_root: str):
        self.root = Path(project_root)
        self.docs_dir = self.root / "docs"
        self.docs_dir.mkdir(exist_ok=True)

    def generate_all(self):
        """Generate all documentation files"""
        self.generate_overview()
        self.generate_architecture()
        self.generate_database_schema()
        self.generate_hardware_detection()
        self.generate_environment_tracking()
        self.generate_execution_flow()
        self.generate_api_reference()
        self.generate_troubleshooting()

    def generate_overview(self):
        """Generate overview from actual code comments"""
        content = f"""# A-LEMS: Project Overview

> Generated from codebase on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 🎯 Core Purpose

{self._extract_docstring('core/energy_engine.py')}

## 📦 Modules

"""
        # List all core modules
        modules = self._get_core_modules()
        for module in modules:
            doc = self._extract_docstring(module)
            content += f"### `{module}`\n{doc}\n\n"

        self._write_file("01_OVERVIEW.md", content)

    def generate_architecture(self):
        """Generate architecture documentation"""
        content = f"""# System Architecture

> Generated from codebase on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 🏗️ High-Level Architecture
┌─────────────────────────────────────────────────────────────┐
│ A-LEMS System │
├─────────────────────────────────────────────────────────────┤
│ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ │
│ │ Module 0 │ │ Module 1 │ │ Module 2 │ │
│ │ Config │───▶│ Energy │───▶│ Sustain- │ │
│ │ Layer │ │ Engine │ │ ability │ │
│ └─────────────┘ └─────────────┘ └─────────────┘ │
│ │ │ │ │
│ ▼ ▼ ▼ │
│ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ │
│ │ Module 3 │ │ Module 4 │ │ Module 5 │ │
│ │ Execution │───▶│ Database │───▶│ UI │ │
│ │ Layer │ │ Layer │ │ Layer │ │
│ └─────────────┘ └─────────────┘ └─────────────┘ │
└─────────────────────────────────────────────────────────────┘

text

## 📦 Core Modules

"""
        # List core modules with their purposes
        modules = [
            (
                "ConfigLoader",
                "core/config_loader.py",
                "Centralized configuration management",
            ),
            (
                "EnergyEngine",
                "core/energy_engine.py",
                "Hardware measurement orchestration",
            ),
            ("ExperimentHarness", "core/execution/harness.py", "Experiment controller"),
            ("DatabaseManager", "core/database/manager.py", "Database interface"),
            (
                "SQLiteAdapter",
                "core/database/sqlite_adapter.py",
                "SQLite implementation",
            ),
            ("RAPLReader", "core/readers/rapl_reader.py", "Intel RAPL energy reading"),
            (
                "MSRReader",
                "core/readers/msr_reader.py",
                "Model-specific register access",
            ),
            (
                "SchedulerMonitor",
                "core/readers/scheduler_monitor.py",
                "System scheduler metrics",
            ),
        ]

        for name, path, purpose in modules:
            content += f"### `{name}`\n"
            content += f"**File:** `{path}`\n"
            content += f"**Purpose:** {purpose}\n\n"

        self._write_file("02_ARCHITECTURE.md", content)

    def generate_database_schema(self):
        """Generate database schema documentation from schema.py"""
        schema_path = self.root / "core" / "database" / "schema.py"
        content = f"""# A-LEMS Database Schema

> Generated from `core/database/schema.py` on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 📊 Tables

"""
        # Parse schema.py and extract CREATE TABLE statements
        tables = self._parse_schema_tables(schema_path)

        for table_name, table_sql in tables.items():
            content += f"### `{table_name}`\n\n```sql\n{table_sql}\n```\n\n"

            # Add column descriptions
            columns = self._parse_columns(table_sql)
            if columns:
                content += "| Column | Type | Description |\n"
                content += "|--------|------|-------------|\n"
                for col in columns:
                    content += (
                        f"| `{col['name']}` | {col['type']} | {col.get('desc', '')} |\n"
                    )
                content += "\n"

        self._write_file("08_DATABASE_SCHEMA.md", content)

    def generate_hardware_detection(self):
        """Generate hardware detection docs from actual code"""
        hw_path = self.root / "scripts" / "detect_hardware.py"
        content = f"""# Hardware Detection System

> Generated from `scripts/detect_hardware.py` on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 🔍 Detection Functions

"""
        functions = self._extract_functions(hw_path)
        for func in functions:
            content += f"### `{func['name']}()`\n\n"
            content += f"{func['docstring']}\n\n"
            if func["returns"]:
                content += f"**Returns:** {func['returns']}\n\n"

        # Add hardware_config table structure
        content += "\n## 📊 Hardware Config Table\n\n"
        content += self._get_table_schema("hardware_config")

        self._write_file("05_HARDWARE_DETECTION.md", content)

    def generate_environment_tracking(self):
        """Generate environment tracking docs"""
        env_path = self.root / "scripts" / "detect_environment.py"
        content = f"""# Environment Tracking System

> Generated from `scripts/detect_environment.py` on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 🔍 Detection Functions

"""
        if env_path.exists():
            functions = self._extract_functions(env_path)
            for func in functions:
                content += f"### `{func['name']}()`\n\n"
                content += f"{func['docstring']}\n\n"

        content += "\n## 📊 Environment Config Table\n\n"
        content += self._get_table_schema("environment_config")

        self._write_file("06_ENVIRONMENT_TRACKING.md", content)

    def generate_execution_flow(self):
        """Generate execution flow documentation"""
        content = f"""# Experiment Execution Flow

> Generated from codebase on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 🚀 Entry Points

"""
        # Document test_harness.py
        harness_path = self.root / "core" / "execution" / "tests" / "test_harness.py"
        if harness_path.exists():
            content += "### `test_harness.py`\n\n"
            content += f"{self._extract_docstring(harness_path)}\n\n"

            # Extract main function
            main_func = self._extract_function(harness_path, "main")
            if main_func:
                content += "**Main flow:**\n```python\n"
                content += main_func["source"][:500] + "...\n```\n\n"

        # Document run_experiment.py
        run_path = self.root / "core" / "execution" / "tests" / "run_experiment.py"
        if run_path.exists():
            content += "### `run_experiment.py`\n\n"
            content += f"{self._extract_docstring(run_path)}\n\n"

        self._write_file("07_EXECUTION.md", content)

    def generate_api_reference(self):
        """Generate API reference from code"""
        content = f"""# API Reference

> Generated from codebase on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 📦 Core Classes

"""
        # Document key classes
        classes = [
            ("core/energy_engine.py", "EnergyEngine"),
            ("core/execution/harness.py", "ExperimentHarness"),
            ("core/execution/experiment_runner.py", "ExperimentRunner"),
            ("core/execution/linear.py", "LinearExecutor"),
            ("core/execution/agentic.py", "AgenticExecutor"),
            ("core/database/manager.py", "DatabaseManager"),
            ("core/database/sqlite_adapter.py", "SQLiteAdapter"),
            ("core/readers/rapl_reader.py", "RAPLReader"),
            ("core/readers/msr_reader.py", "MSRReader"),
            ("core/readers/scheduler_monitor.py", "SchedulerMonitor"),
        ]

        for file_path, class_name in classes:
            full_path = self.root / file_path
            if full_path.exists():
                class_doc = self._extract_class_doc(full_path, class_name)
                if class_doc:
                    content += f"### `{class_name}`\n\n"
                    content += f"{class_doc['docstring']}\n\n"

                    # List methods
                    content += "**Methods:**\n\n"
                    for method in class_doc["methods"]:
                        content += f"- `{method['name']}({method['args']})`"
                        if method["docstring"]:
                            short_doc = method["docstring"].split("\n")[0]
                            content += f": {short_doc}"
                        content += "\n"
                    content += "\n"

        self._write_file("10_API_REFERENCE.md", content)

    def generate_troubleshooting(self):
        """Generate troubleshooting guide from open issues"""
        content = f"""# Troubleshooting Guide

> Generated from open issues on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 🔴 Critical Issues

"""
        # Add known issues from our list
        issues = [
            ("S1", "Sample count bug", "Fixed with list() copy in harness.py"),
            ("E1", "Energy display bug", "Fixed - using derived.workload_energy_j"),
            ("B1", "Baseline correction", "Fixed - using mean-2σ baseline"),
            ("D2", "orchestration_events empty", "Fixed - events now captured"),
            ("T1", "Thermal zone instability", "Fixed - dynamic mapping by type"),
            ("D4", "Interrupt rate threshold", "Fixed - baseline-relative comparison"),
            ("O1", "Governor fallback", "Fixed - fallback chain in optimizer"),
            (
                "L3",
                "Local model context window",
                "Fixed - increased max_tokens in config",
            ),
        ]

        for issue_id, desc, status in issues:
            content += f"### {issue_id}: {desc}\n"
            content += f"**Status:** {status}\n\n"

        content += "\n## 🔧 Common Fixes\n\n"

        # Add common fixes based on actual code
        content += "### Database Locked\n```bash\n"
        content += "# Find processes using the database\n"
        content += "lsof data/experiments.db\n\n"
        content += "# Kill the process\n"
        content += "kill -9 <PID>\n\n"
        content += "# Remove lock files (if no processes are running)\n"
        content += "rm -f data/experiments.db-journal\n"
        content += "rm -f data/experiments.db-wal\n"
        content += "rm -f data/experiments.db-shm\n```\n\n"

        content += "### Missing Tables\n```bash\n"
        content += "# Run migrations\n"
        content += "python scripts/migrate_db.py\n\n"
        content += "# Or create fresh database\n"
        content += "python scripts/setup_fresh_db.py\n```\n\n"

        content += "### Permission Denied\n```bash\n"
        content += "# Fix ownership\n"
        content += "sudo chown -R $USER:$USER data/\n"
        content += "sudo chown -R $USER:$USER config/\n```\n\n"

        self._write_file("90_TROUBLESHOOTING.md", content)

    # ====================================================================
    # Helper Methods
    # ====================================================================

    def _extract_docstring(self, file_path: str) -> str:
        """Extract module docstring from file"""
        full_path = self.root / file_path
        if not full_path.exists():
            return ""

        with open(full_path) as f:
            content = f.read()

        try:
            module = ast.parse(content)
            return ast.get_docstring(module) or ""
        except:
            return ""

    def _extract_functions(self, file_path: Path) -> List[Dict]:
        """Extract all functions and their docstrings"""
        functions = []
        if not file_path.exists():
            return functions

        with open(file_path) as f:
            content = f.read()

        try:
            tree = ast.parse(content)
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    docstring = ast.get_docstring(node) or ""
                    returns = self._get_return_annotation(node)
                    functions.append(
                        {
                            "name": node.name,
                            "docstring": docstring,
                            "returns": returns,
                            "line": node.lineno,
                        }
                    )
        except Exception as e:
            print(f"Error parsing {file_path}: {e}")

        return functions

    def _extract_function(self, file_path: Path, func_name: str) -> Dict:
        """Extract a specific function"""
        if not file_path.exists():
            return None

        with open(file_path) as f:
            content = f.read()

        try:
            tree = ast.parse(content)
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef) and node.name == func_name:
                    return {
                        "name": node.name,
                        "source": ast.unparse(node),
                        "docstring": ast.get_docstring(node) or "",
                        "line": node.lineno,
                    }
        except:
            pass
        return None

    def _extract_class_doc(self, file_path: Path, class_name: str) -> Dict:
        """Extract class and method documentation"""
        if not file_path.exists():
            return None

        with open(file_path) as f:
            content = f.read()

        try:
            tree = ast.parse(content)
            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef) and node.name == class_name:
                    methods = []
                    for item in node.body:
                        if isinstance(item, ast.FunctionDef):
                            args = [arg.arg for arg in item.args.args]
                            methods.append(
                                {
                                    "name": item.name,
                                    "args": ", ".join(args),
                                    "docstring": ast.get_docstring(item) or "",
                                }
                            )

                    return {
                        "name": class_name,
                        "docstring": ast.get_docstring(node) or "",
                        "methods": methods,
                        "line": node.lineno,
                    }
        except:
            pass
        return None

    def _get_return_annotation(self, node: ast.FunctionDef) -> str:
        """Extract return type annotation"""
        if node.returns:
            return ast.unparse(node.returns)
        return ""

    def _get_core_modules(self) -> List[str]:
        """Get list of core Python modules"""
        modules = []
        core_dir = self.root / "core"
        for py_file in core_dir.rglob("*.py"):
            if "__pycache__" not in str(py_file):
                modules.append(str(py_file.relative_to(self.root)))
        return sorted(modules)

    def _parse_schema_tables(self, schema_path: Path) -> Dict[str, str]:
        """Parse CREATE TABLE statements from schema.py"""
        tables = {}
        if not schema_path.exists():
            return tables

        with open(schema_path) as f:
            content = f.read()

        # Find all CREATE TABLE assignments
        import re

        pattern = r'(\w+)\s*=\s*"""\s*(CREATE TABLE.*?)"""'
        matches = re.findall(pattern, content, re.DOTALL | re.IGNORECASE)

        for var_name, table_sql in matches:
            if "CREATE TABLE" in table_sql.upper():
                # Extract table name
                table_match = re.search(
                    r"CREATE TABLE\s+IF NOT EXISTS\s+(\w+)", table_sql, re.IGNORECASE
                )
                if table_match:
                    table_name = table_match.group(1)
                    tables[table_name] = table_sql.strip()

        return tables

    def _parse_columns(self, table_sql: str) -> List[Dict]:
        """Parse column definitions from CREATE TABLE statement"""
        columns = []
        lines = table_sql.split("\n")

        for line in lines:
            line = line.strip()
            if (
                line
                and not line.upper().startswith("CREATE")
                and not line.upper().startswith("FOREIGN")
                and not line.upper().startswith("PRIMARY")
            ):
                # Simple column parsing
                parts = line.split()
                if len(parts) >= 2 and not parts[0].startswith("--"):
                    col_name = parts[0].strip(",")
                    col_type = parts[1].strip(",")

                    # Look for comment
                    comment = ""
                    if "--" in line:
                        comment = line.split("--")[1].strip()

                    columns.append(
                        {"name": col_name, "type": col_type, "desc": comment}
                    )

        return columns

    def _get_table_schema(self, table_name: str) -> str:
        """Get table schema from actual database"""
        db_path = self.root / "data" / "experiments.db"
        if not db_path.exists():
            return "Database not found. Run an experiment first."

        import sqlite3

        try:
            conn = sqlite3.connect(str(db_path))
            cursor = conn.execute(
                f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'"
            )
            result = cursor.fetchone()
            conn.close()
            if result:
                return f"```sql\n{result[0]}\n```"
            return f"Table '{table_name}' not found."
        except Exception as e:
            return f"Error: {e}"

    def _write_file(self, filename: str, content: str):
        """Write content to markdown file"""
        file_path = self.docs_dir / filename
        file_path.write_text(content)
        print(f"✅ Generated: {filename}")


if __name__ == "__main__":
    project_root = Path(__file__).parent.parent
    generator = ALEMSDocGenerator(project_root)
    generator.generate_all()
    print(f"\n📚 Documentation generated in {generator.docs_dir}")
    print("\n📖 To view:")
    print("   - In VS Code: code docs/")
    print("   - In browser: pip install grip && grip docs/README.md")
