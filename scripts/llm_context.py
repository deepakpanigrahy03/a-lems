#!/usr/bin/env python3
"""
scripts/llm_context.py - Generate complete context for LLM code generation
"""

import argparse
import ast
import glob
import json
import re
import subprocess
from datetime import datetime
from pathlib import Path


class LLMContextGenerator:
    def __init__(self, change_request):
        self.request = change_request
        self.context = {
            "timestamp": datetime.now().isoformat(),
            "request": change_request,
            "files": {},
            "schema": {},
            "dependencies": [],
            "questions": [],
        }

    def generate(self):
        """Generate complete context for LLM"""

        # 1. Parse what's being requested
        if "page_faults" in self.request.lower():
            self._handle_page_faults()
        elif "network" in self.request.lower():
            self._handle_network_metrics()
        elif "llm" in self.request.lower():
            self._handle_llm_changes()
        else:
            self._generic_analysis()

        # 2. Get current schema
        self._extract_schema()

        # 3. Get relevant code
        self._extract_relevant_code()

        # 4. Get dependencies
        self._get_dependencies()

        # 5. Generate output
        return self._format_output()

    def _handle_page_faults(self):
        """Specific handler for page faults request"""
        self.context["tables"] = ["runs"]
        self.context["new_columns"] = [
            "minor_page_faults INTEGER",
            "major_page_faults INTEGER",
            "page_fault_rate REAL",
        ]
        self.context["data_source"] = "perf_reader.py"
        self.context["files_to_update"] = [
            "core/database/schema.py",
            "core/database/migrations/",
            "core/database/repositories/runs.py",
            "core/execution/harness.py",
            "core/readers/perf_reader.py",
            "tests/test_database.py",
        ]
        self.context["questions"] = [
            "Does perf_event_open provide minor/major separately?",
            "Should page_fault_rate be faults/second or faults/instruction?",
            "Do we need to track both minor and major or just total?",
        ]

    def _handle_network_metrics(self):
        """Specific handler for network metrics request"""
        self.context["tables"] = ["runs"]
        self.context["new_columns"] = [
            "bytes_sent INTEGER",
            "bytes_recv INTEGER",
            "tcp_retransmits INTEGER",
            "network_rtt_ms REAL",
        ]
        self.context["data_source"] = "linear.py, agentic.py"
        self.context["files_to_update"] = [
            "core/database/schema.py",
            "core/database/migrations/",
            "core/database/repositories/runs.py",
            "core/execution/linear.py",
            "core/execution/agentic.py",
            "core/execution/harness.py",
            "tests/test_database.py",
        ]
        self.context["questions"] = [
            "Which network metrics are available from each provider?",
            "Should we capture at start/end or during entire run?",
            "Do we need separate columns for each provider?",
        ]

    def _handle_llm_changes(self):
        """Specific handler for LLM interaction fixes"""
        self.context["tables"] = ["llm_interactions"]
        self.context["files_to_update"] = [
            "core/execution/linear.py",
            "core/execution/agentic.py",
            "core/execution/experiment_runner.py",
            "core/database/repositories/runs.py",
        ]
        self.context["questions"] = [
            "Why are pending_interactions empty in result dict?",
            "Is _call_llm() being called at all?",
            "Is the list being cleared too early?",
        ]

    def _generic_analysis(self):
        """Generic handler for unknown requests"""
        self.context["tables"] = ["runs"]
        self.context["files_to_update"] = [
            "core/database/schema.py",
            "core/database/migrations/",
            "core/database/repositories/runs.py",
            "core/execution/harness.py",
        ]
        self.context["questions"] = [
            f"What specific change is needed for: {self.request}?",
            "Which data source provides this metric?",
            "Should it be added to runs table or a new table?",
        ]

    def _extract_schema(self):
        """Get current table schema"""
        schema_file = "core/database/schema.py"
        if Path(schema_file).exists():
            with open(schema_file) as f:
                content = f.read()
                # Extract CREATE_TABLE statements
                tables = re.findall(
                    r"CREATE TABLE (\w+) \(([^;]+)\)", content, re.DOTALL
                )
                for table, definition in tables:
                    self.context["schema"][table] = definition.strip()

    def _extract_relevant_code(self):
        """Extract code snippets relevant to the change"""
        for file_pattern in self.context.get("files_to_update", []):
            if "*" in file_pattern:
                for f in glob.glob(file_pattern):
                    self._extract_file_snippets(f)
            else:
                if Path(file_pattern).exists():
                    self._extract_file_snippets(file_pattern)

    def _extract_file_snippets(self, filepath):
        """Extract relevant snippets from a file"""
        if not Path(filepath).exists():
            return

        with open(filepath) as f:
            content = f.read()

        # Find relevant functions based on request
        snippets = []

        if "runs.py" in filepath and "extract_from_ml_features" in content:
            # Extract the extract_from_ml_features method
            lines = content.split("\n")
            for i, line in enumerate(lines):
                if "def _extract_from_ml_features" in line:
                    start = i
                    # Find the function body (until next def or end)
                    end = len(lines)
                    for j in range(i + 1, len(lines)):
                        if lines[j].strip().startswith("def "):
                            end = j
                            break
                    snippet = "\n".join(lines[start : min(end, start + 50)])
                    snippets.append(("_extract_from_ml_features", snippet))
                    break

        if "harness.py" in filepath and "ml_features = {" in content:
            # Extract ml_features dict creation
            lines = content.split("\n")
            for i, line in enumerate(lines):
                if "ml_features = {" in line:
                    start = i
                    # Find closing brace
                    brace_count = 1
                    for j in range(i + 1, min(i + 100, len(lines))):
                        brace_count += lines[j].count("{") - lines[j].count("}")
                        if brace_count == 0:
                            end = j
                            snippet = "\n".join(lines[start : end + 1])
                            snippets.append(("ml_features dict", snippet))
                            break

        if "perf_reader.py" in filepath and "get_counters" in content:
            # Extract get_counters method
            lines = content.split("\n")
            for i, line in enumerate(lines):
                if "def get_counters" in line:
                    start = i
                    end = min(i + 30, len(lines))
                    snippet = "\n".join(lines[start:end])
                    snippets.append(("get_counters", snippet))
                    break

        if snippets:
            self.context["files"][filepath] = snippets

    def _get_dependencies(self):
        """Get module dependencies using pydeps if available"""
        try:
            result = subprocess.run(
                ["pydeps", "core/database/", "--show-deps", "--noshow"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            self.context["dependencies"] = result.stdout.split("\n")[:10]
        except:
            self.context["dependencies"] = [
                "pydeps not available - install with: pip install pydeps"
            ]

    def _format_output(self):
        """Format everything for LLM consumption"""
        output = []
        output.append("📋 A-LEMS CHANGE CONTEXT")
        output.append("=" * 60)
        output.append(f"Generated: {self.context['timestamp']}")
        output.append(f"Request: {self.context['request']}")
        output.append("")

        # Project structure
        output.append("📁 PROJECT STRUCTURE:")
        output.append(self._get_tree())
        output.append("")

        # Current schema
        output.append("📊 CURRENT SCHEMA:")
        for table in self.context.get("tables", []):
            if table in self.context["schema"]:
                output.append(f"\n{table}:")
                output.append(self.context["schema"][table][:500] + "...")
        output.append("")

        # Files to update
        output.append("📝 FILES TO UPDATE:")
        for f in self.context.get("files_to_update", []):
            output.append(f"  - {f}")
        output.append("")

        # Relevant code snippets
        output.append("🔍 RELEVANT CODE:")
        for filepath, snippets in self.context.get("files", {}).items():
            output.append(f"\n{filepath}:")
            for name, snippet in snippets:
                output.append(f"\n  # {name}:")
                output.append("  ```python")
                for line in snippet.split("\n")[:15]:
                    output.append(f"  {line}")
                output.append("  ```")
        output.append("")

        # Questions for LLM
        if self.context.get("questions"):
            output.append("🤔 QUESTIONS TO ANSWER:")
            for q in self.context["questions"]:
                output.append(f"  - {q}")
            output.append("")

        # Verification
        output.append("✅ VERIFICATION QUERIES:")
        if self.context.get("new_columns"):
            cols = [c.split()[0] for c in self.context["new_columns"]]
            output.append("  ```sql")
            output.append(f"  SELECT {', '.join(cols)} FROM runs LIMIT 5;")
            output.append("  ```")
        else:
            output.append("  Run test experiment and check database")

        return "\n".join(output)

    def _get_tree(self, max_depth=2):
        """Get project tree structure"""
        try:
            result = subprocess.run(
                [
                    "tree",
                    "-L",
                    str(max_depth),
                    "-I",
                    "__pycache__|*.pyc|*.egg-info|venv",
                ],
                capture_output=True,
                text=True,
                timeout=5,
            )
            return (
                result.stdout[:500] + "..."
                if len(result.stdout) > 500
                else result.stdout
            )
        except:
            return "core/\n  database/\n  execution/\n  readers/"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate context for LLM code generation"
    )
    parser.add_argument("--change", required=True, help="Description of change needed")
    args = parser.parse_args()

    generator = LLMContextGenerator(args.change)
    print(generator.generate())
