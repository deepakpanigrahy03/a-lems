#!/usr/bin/env python3
"""
LLM Context Generator for A-LEMS
Goal 1: Generate complete context for LLM code changes
Run: python scripts/tools/llm_context.py --change "your request"
"""

import argparse
import ast
import glob
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


class LLMContextGenerator:
    def __init__(self, change_request: str):
        self.request = change_request
        self.repo_root = self._find_repo_root()
        self.context = {
            "timestamp": datetime.now().isoformat(),
            "request": change_request,
            "files": {},
            "schema": {},
            "dependencies": [],
            "questions": [],
            "related_files": [],
            "data_flow": [],
        }

    def _find_repo_root(self) -> Path:
        """Find repository root (where .git is)"""
        current = Path(__file__).resolve().parent
        while current != current.parent:
            if (current / ".git").exists():
                return current
            current = current.parent
        return Path.cwd()

    def generate(self) -> str:
        """Generate complete context"""

        # Parse request to find tables/columns
        self._parse_request()

        # Extract schema
        self._extract_schema()

        # Find related files
        self._find_related_files()

        # Trace data flow
        self._trace_data_flow()

        # Get dependencies
        self._get_dependencies()

        return self._format_output()

    def _parse_request(self):
        """Extract table and column names from request"""
        # Look for table names
        table_match = re.search(r"(\w+)\s+table", self.request.lower())
        if table_match:
            self.context["target_table"] = table_match.group(1)

        # Look for column names
        column_matches = re.findall(r"(\w+)\s+column", self.request.lower())
        if column_matches:
            self.context["target_columns"] = column_matches

        # Generate questions
        self.context["questions"] = [
            f"Which table needs to be modified?",
            f"Where does this data come from?",
            f"Should we add indexes?",
            f"Any existing columns with similar data?",
        ]

    def _extract_schema(self):
        """Extract database schema from schema.py"""
        schema_path = self.repo_root / "core" / "database" / "schema.py"
        if schema_path.exists():
            with open(schema_path) as f:
                content = f.read()

            # Find CREATE TABLE statements
            tables = re.findall(r"CREATE TABLE (\w+) \(([^;]+)\)", content, re.DOTALL)
            for table, definition in tables:
                if table == self.context.get("target_table"):
                    self.context["schema"][table] = definition.strip()

    def _find_related_files(self):
        """Find all files that might be related to the change"""
        target = self.context.get("target_table", "runs")

        # Search for files mentioning the table
        for root, _, files in os.walk(self.repo_root / "core"):
            for file in files:
                if file.endswith(".py"):
                    path = Path(root) / file
                    with open(path) as f:
                        content = f.read()
                        if target in content:
                            rel_path = str(path.relative_to(self.repo_root))
                            self.context["related_files"].append(rel_path)

                            # Extract relevant snippets
                            self._extract_snippets(path, content, target)

    def _extract_snippets(self, path: Path, content: str, target: str):
        """Extract relevant code snippets"""
        lines = content.split("\n")
        snippets = []

        # Look for functions that might be relevant
        for i, line in enumerate(lines):
            if target in line.lower():
                start = max(0, i - 5)
                end = min(len(lines), i + 5)
                snippet = "\n".join(lines[start:end])
                snippets.append({"line": i + 1, "code": snippet})

        if snippets:
            self.context["files"][str(path)] = snippets[:3]  # First 3 snippets

    def _trace_data_flow(self):
        """Trace where data comes from and where it goes"""
        target = self.context.get("target_table", "runs")

        # Look for INSERT statements
        for file_path in self.context["related_files"]:
            full_path = self.repo_root / file_path
            with open(full_path) as f:
                content = f.read()
                if f"INSERT INTO {target}" in content:
                    self.context["data_flow"].append(
                        {"file": file_path, "type": "insert", "location": "found"}
                    )

    def _get_dependencies(self):
        """Get module dependencies using pydeps"""
        try:
            result = subprocess.run(
                ["pydeps", "core/database/", "--show-deps", "--noshow"],
                capture_output=True,
                text=True,
                timeout=5,
                cwd=self.repo_root,
            )
            self.context["dependencies"] = result.stdout.split("\n")[:10]
        except:
            self.context["dependencies"] = ["pydeps not available"]

    def _format_output(self) -> str:
        """Format everything for LLM consumption"""
        output = []
        output.append("📋 A-LEMS CHANGE CONTEXT")
        output.append("=" * 60)
        output.append(f"Generated: {self.context['timestamp']}")
        output.append(f"Request: {self.context['request']}")
        output.append("")

        # Target info
        if "target_table" in self.context:
            output.append(f"🎯 Target Table: {self.context['target_table']}")
        if "target_columns" in self.context:
            output.append(
                f"📊 Target Columns: {', '.join(self.context['target_columns'])}"
            )
        output.append("")

        # Current schema
        if self.context["schema"]:
            output.append("📊 CURRENT SCHEMA:")
            for table, definition in self.context["schema"].items():
                output.append(f"\n{table}:")
                output.append(
                    definition[:500] + "..." if len(definition) > 500 else definition
                )
        output.append("")

        # Related files
        if self.context["related_files"]:
            output.append("📁 RELATED FILES:")
            for f in sorted(set(self.context["related_files"])):
                output.append(f"  - {f}")
        output.append("")

        # Code snippets
        if self.context["files"]:
            output.append("🔍 RELEVANT CODE SNIPPETS:")
            for filepath, snippets in list(self.context["files"].items())[:5]:
                output.append(f"\n{filepath}:")
                for i, snippet in enumerate(snippets[:2]):
                    output.append(f"\n  Lines {snippet['line']}-{snippet['line']+5}:")
                    output.append("  ```python")
                    for line in snippet["code"].split("\n"):
                        output.append(f"  {line}")
                    output.append("  ```")
        output.append("")

        # Data flow
        if self.context["data_flow"]:
            output.append("🔄 DATA FLOW:")
            for flow in self.context["data_flow"]:
                output.append(f"  • {flow['file']} ({flow['type']})")
        output.append("")

        # Questions
        if self.context["questions"]:
            output.append("🤔 QUESTIONS TO ANSWER:")
            for q in self.context["questions"]:
                output.append(f"  • {q}")
        output.append("")

        # Verification
        if "target_table" in self.context:
            output.append("✅ VERIFICATION QUERIES:")
            output.append("  ```sql")
            output.append(f"  SELECT * FROM {self.context['target_table']} LIMIT 5;")
            if "target_columns" in self.context:
                cols = ", ".join(self.context["target_columns"])
                output.append(
                    f"  SELECT {cols} FROM {self.context['target_table']} WHERE {cols[0]} IS NOT NULL LIMIT 5;"
                )
            output.append("  ```")

        return "\n".join(output)


def main():
    parser = argparse.ArgumentParser(
        description="Generate context for LLM code changes"
    )
    parser.add_argument("--change", required=True, help="Description of change needed")
    parser.add_argument("--output", "-o", help="Output file (default: print to stdout)")
    args = parser.parse_args()

    generator = LLMContextGenerator(args.change)
    output = generator.generate()

    if args.output:
        with open(args.output, "w") as f:
            f.write(output)
        print(f"✅ Context saved to {args.output}")
    else:
        print(output)


if __name__ == "__main__":
    main()
