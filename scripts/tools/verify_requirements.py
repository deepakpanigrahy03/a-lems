#!/usr/bin/env python3
"""
Requirements Verifier for A-LEMS
Goal 7: Compare specifications against actual code implementation
Run: python scripts/tools/verify_requirements.py
"""

import argparse
import ast
import glob
import json
import re
from datetime import datetime
from pathlib import Path

import yaml


class RequirementsVerifier:
    def __init__(self, spec_file=None):
        self.repo_root = self._find_repo_root()
        self.spec_file = spec_file
        self.specs = self._load_specs()
        self.results = {
            "timestamp": datetime.now().isoformat(),
            "implemented": [],
            "partial": [],
            "missing": [],
            "extra": [],
        }

    def _find_repo_root(self) -> Path:
        current = Path(__file__).resolve().parent
        while current != current.parent:
            if (current / ".git").exists():
                return current
            current = current.parent
        return Path.cwd()

    def _load_specs(self):
        """Load requirements from YAML or create default"""
        specs = {"requirements": []}

        if self.spec_file:
            path = Path(self.spec_file)
            if path.exists():
                with open(path) as f:
                    if path.suffix in [".yaml", ".yml"]:
                        specs = yaml.safe_load(f)
                    elif path.suffix == ".json":
                        specs = json.load(f)

        # If no specs, create default based on A-LEMS known features
        if not specs.get("requirements"):
            specs["requirements"] = [
                {
                    "id": "RAPL-001",
                    "title": "RAPL Energy Reading",
                    "description": "Read package energy from RAPL interface",
                    "files": ["core/readers/rapl_reader.py"],
                    "functions": ["read_energy_uj"],
                },
                {
                    "id": "MSR-001",
                    "title": "MSR C-State Counters",
                    "description": "Read C-state counters from MSR registers",
                    "files": ["core/readers/msr_reader.py"],
                    "functions": ["snapshot_cstate_counters"],
                },
                {
                    "id": "PERF-001",
                    "title": "Performance Counters",
                    "description": "Read instructions, cycles, cache misses",
                    "files": ["core/readers/perf_reader.py"],
                    "functions": ["get_counters"],
                },
                {
                    "id": "EXEC-001",
                    "title": "Linear Executor",
                    "description": "Execute single LLM call",
                    "files": ["core/execution/linear.py"],
                    "classes": ["LinearExecutor"],
                },
                {
                    "id": "EXEC-002",
                    "title": "Agentic Executor",
                    "description": "Execute multi-step agentic workflow",
                    "files": ["core/execution/agentic.py"],
                    "classes": ["AgenticExecutor"],
                },
            ]

        return specs

    def check_file_exists(self, filepath):
        """Check if a file exists in the codebase"""
        full_path = self.repo_root / filepath
        return full_path.exists()

    def check_function_exists(self, filepath, function_name):
        """Check if a function exists in a file"""
        full_path = self.repo_root / filepath
        if not full_path.exists():
            return False

        with open(full_path) as f:
            content = f.read()
            return f"def {function_name}" in content

    def check_class_exists(self, filepath, class_name):
        """Check if a class exists in a file"""
        full_path = self.repo_root / filepath
        if not full_path.exists():
            return False

        with open(full_path) as f:
            content = f.read()
            return f"class {class_name}" in content

    def find_extra_features(self):
        """Find features in code not in specs"""
        # Look for TODO or REQ comments without matching specs
        for py_file in glob.glob(str(self.repo_root / "core/**/*.py"), recursive=True):
            with open(py_file) as f:
                content = f.read()

            # Find REQ comments
            req_matches = re.findall(r"#\s*REQ-(\w+)", content)
            for req in req_matches:
                found = False
                for spec in self.specs.get("requirements", []):
                    if spec.get("id") == f"REQ-{req}":
                        found = True
                        break
                if not found:
                    self.results["extra"].append(
                        {
                            "id": f"REQ-{req}",
                            "file": py_file,
                            "note": "Found in code but not in specs",
                        }
                    )

    def verify_all(self):
        """Verify all requirements against code"""

        for spec in self.specs.get("requirements", []):
            req_id = spec.get("id", "UNKNOWN")
            files = spec.get("files", [])
            functions = spec.get("functions", [])
            classes = spec.get("classes", [])

            missing_items = []

            # Check files
            for file in files:
                if not self.check_file_exists(file):
                    missing_items.append(f"Missing file: {file}")

            # Check functions
            for file, func in zip(files * len(functions), functions):
                if not self.check_function_exists(file, func):
                    missing_items.append(f"Missing function: {func} in {file}")

            # Check classes
            for file, cls in zip(files * len(classes), classes):
                if not self.check_class_exists(file, cls):
                    missing_items.append(f"Missing class: {cls} in {file}")

            # Categorize
            if not missing_items:
                self.results["implemented"].append(
                    {
                        "id": req_id,
                        "title": spec.get("title"),
                        "status": "✅ FULLY IMPLEMENTED",
                    }
                )
            elif len(missing_items) < len(files) + len(functions) + len(classes):
                self.results["partial"].append(
                    {
                        "id": req_id,
                        "title": spec.get("title"),
                        "status": "⚠️ PARTIALLY IMPLEMENTED",
                        "missing": missing_items,
                    }
                )
            else:
                self.results["missing"].append(
                    {
                        "id": req_id,
                        "title": spec.get("title"),
                        "status": "❌ NOT IMPLEMENTED",
                        "missing": missing_items,
                    }
                )

        # Find extra features
        self.find_extra_features()

        return self.results

    def print_report(self):
        """Print formatted report"""
        print("\n" + "=" * 60)
        print("📋 REQUIREMENTS VERIFICATION REPORT")
        print("=" * 60)
        print(f"Generated: {self.results['timestamp']}")
        print(f"Spec file: {self.spec_file or 'default'}")
        print()

        # Summary
        total = len(self.specs.get("requirements", []))
        implemented = len(self.results["implemented"])
        partial = len(self.results["partial"])
        missing = len(self.results["missing"])
        extra = len(self.results["extra"])

        print(f"📊 SUMMARY")
        print(f"  Total requirements: {total}")
        print(f"  ✅ Fully implemented: {implemented}")
        print(f"  ⚠️  Partially implemented: {partial}")
        print(f"  ❌ Not implemented: {missing}")
        print(f"  🆕 Extra features found: {extra}")
        print()

        # Implemented
        if self.results["implemented"]:
            print("✅ FULLY IMPLEMENTED")
            for item in self.results["implemented"][:10]:
                print(f"  • {item['id']}: {item['title']}")
            if len(self.results["implemented"]) > 10:
                print(f"    ... and {len(self.results['implemented'])-10} more")
            print()

        # Partial
        if self.results["partial"]:
            print("⚠️  PARTIALLY IMPLEMENTED")
            for item in self.results["partial"]:
                print(f"  • {item['id']}: {item['title']}")
                for m in item.get("missing", [])[:3]:
                    print(f"    - {m}")
                if len(item.get("missing", [])) > 3:
                    print(f"    - ... and {len(item.get('missing', []))-3} more")
            print()

        # Missing
        if self.results["missing"]:
            print("❌ NOT IMPLEMENTED")
            for item in self.results["missing"]:
                print(f"  • {item['id']}: {item['title']}")
                for m in item.get("missing", [])[:2]:
                    print(f"    - {m}")
            print()

        # Extra
        if self.results["extra"]:
            print("🆕 EXTRA FEATURES (in code but not in specs)")
            for item in self.results["extra"][:5]:
                print(f"  • {item['id']} in {item['file']}")
            if len(self.results["extra"]) > 5:
                print(f"    ... and {len(self.results['extra'])-5} more")
            print()

        print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Verify requirements against code")
    parser.add_argument("--spec", "-s", help="Spec file (YAML or JSON)")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--save", help="Save results to file")

    args = parser.parse_args()

    verifier = RequirementsVerifier(spec_file=args.spec)
    results = verifier.verify_all()

    if args.json:
        print(json.dumps(results, indent=2, default=str))
    else:
        verifier.print_report()

    if args.save:
        with open(args.save, "w") as f:
            if args.save.endswith(".json"):
                json.dump(results, f, indent=2, default=str)
            else:
                # Save as text
                import io
                from contextlib import redirect_stdout

                buffer = io.StringIO()
                with redirect_stdout(buffer):
                    verifier.print_report()

                with open(args.save, "w") as f:
                    f.write(buffer.getvalue())

        print(f"\n✅ Results saved to {args.save}")


if __name__ == "__main__":
    main()
