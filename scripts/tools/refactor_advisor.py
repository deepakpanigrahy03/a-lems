#!/usr/bin/env python3
"""
Refactoring Advisor for A-LEMS
Goal 4: Suggest code structure improvements
Run: python scripts/tools/refactor_advisor.py --target core/execution/harness.py
"""

import argparse
import ast
import glob
import subprocess
import sys
from collections import defaultdict
from pathlib import Path


class RefactoringAdvisor:
    def __init__(self, target_path):
        self.repo_root = self._find_repo_root()
        self.target = Path(target_path)
        if not self.target.is_absolute():
            self.target = self.repo_root / self.target

        self.analysis = {
            "file_too_large": [],
            "duplicate_code": [],
            "high_complexity": [],
            "circular_deps": [],
            "suggestions": [],
        }

    def _find_repo_root(self) -> Path:
        current = Path(__file__).resolve().parent
        while current != current.parent:
            if (current / ".git").exists():
                return current
            current = current.parent
        return Path.cwd()

    def analyze_file_size(self):
        """Check if file is too large"""
        if not self.target.exists():
            return

        with open(self.target) as f:
            lines = f.readlines()

        line_count = len(lines)

        if line_count > 1000:
            self.analysis["file_too_large"].append(
                {
                    "file": str(self.target),
                    "lines": line_count,
                    "suggestion": f"Split into multiple files (<500 lines each)",
                }
            )
        elif line_count > 500:
            self.analysis["file_too_large"].append(
                {
                    "file": str(self.target),
                    "lines": line_count,
                    "suggestion": f"Consider splitting (optimal <500 lines)",
                }
            )

    def analyze_complexity(self):
        """Analyze function complexity using radon"""
        try:
            result = subprocess.run(
                ["radon", "cc", str(self.target), "-s", "-j"],
                capture_output=True,
                text=True,
            )

            if result.stdout:
                import json

                complexity = json.loads(result.stdout)

                for file, funcs in complexity.items():
                    for func in funcs:
                        if func.get("complexity", 0) > 15:
                            self.analysis["high_complexity"].append(
                                {
                                    "file": file,
                                    "function": func["name"],
                                    "complexity": func["complexity"],
                                    "line": func.get("lineno", "?"),
                                    "suggestion": f"Break into smaller functions (complexity >15)",
                                }
                            )
                        elif func.get("complexity", 0) > 10:
                            self.analysis["high_complexity"].append(
                                {
                                    "file": file,
                                    "function": func["name"],
                                    "complexity": func["complexity"],
                                    "line": func.get("lineno", "?"),
                                    "suggestion": f"Consider simplifying (complexity >10)",
                                }
                            )
        except:
            pass

    def find_duplicates(self):
        """Find duplicate code blocks using pydeps or simple analysis"""
        if not self.target.exists():
            return

        with open(self.target) as f:
            content = f.read()

        # Simple duplicate line detection
        lines = content.split("\n")
        line_patterns = defaultdict(list)

        for i, line in enumerate(lines):
            line = line.strip()
            if (
                len(line) > 30
                and not line.startswith("#")
                and not line.startswith('"""')
            ):
                line_patterns[line].append(i + 1)

        for line, positions in line_patterns.items():
            if len(positions) > 2:
                self.analysis["duplicate_code"].append(
                    {
                        "file": str(self.target),
                        "line": positions[0],
                        "duplicates": len(positions),
                        "code": line[:50] + "...",
                        "suggestion": f"Extract repeated code into a function",
                    }
                )

    def analyze_imports(self):
        """Analyze imports for circular dependencies"""
        try:
            result = subprocess.run(
                ["pydeps", str(self.target), "--show-deps", "--noshow"],
                capture_output=True,
                text=True,
            )

            if "circular" in result.stdout.lower():
                self.analysis["circular_deps"].append(
                    {
                        "file": str(self.target),
                        "details": "Circular dependency detected",
                        "suggestion": "Use dependency inversion or move common code to a new module",
                    }
                )
        except:
            pass

    def suggest_refactoring(self):
        """Generate refactoring suggestions"""
        if self.analysis["file_too_large"]:
            for item in self.analysis["file_too_large"]:
                self.analysis["suggestions"].append(
                    {
                        "type": "split_file",
                        "target": item["file"],
                        "action": item["suggestion"],
                    }
                )

        if self.analysis["high_complexity"]:
            for item in self.analysis["high_complexity"]:
                self.analysis["suggestions"].append(
                    {
                        "type": "simplify_function",
                        "target": f"{item['function']} (line {item['line']})",
                        "action": item["suggestion"],
                    }
                )

        if self.analysis["duplicate_code"]:
            for item in self.analysis["duplicate_code"][:3]:  # Limit suggestions
                self.analysis["suggestions"].append(
                    {
                        "type": "extract_code",
                        "target": f"{item['file']} line {item['line']}",
                        "action": item["suggestion"],
                    }
                )

    def run(self):
        """Run all analyses"""
        print(f"🔍 Analyzing {self.target}...")

        self.analyze_file_size()
        self.analyze_complexity()
        self.find_duplicates()
        self.analyze_imports()
        self.suggest_refactoring()

        return self.analysis

    def print_report(self):
        """Print formatted report"""
        print("\n" + "=" * 60)
        print("🔄 REFACTORING ADVISOR REPORT")
        print("=" * 60)
        print(f"Target: {self.target}")
        print()

        if self.analysis["file_too_large"]:
            print("📏 FILE SIZE ISSUES:")
            for item in self.analysis["file_too_large"]:
                print(f"  • {item['file']}: {item['lines']} lines")
                print(f"    → {item['suggestion']}")
            print()

        if self.analysis["high_complexity"]:
            print("🧠 HIGH COMPLEXITY FUNCTIONS:")
            for item in self.analysis["high_complexity"][:5]:  # Top 5
                print(
                    f"  • {item['function']} (line {item['line']}): complexity {item['complexity']}"
                )
                print(f"    → {item['suggestion']}")
            if len(self.analysis["high_complexity"]) > 5:
                print(f"    ... and {len(self.analysis['high_complexity'])-5} more")
            print()

        if self.analysis["duplicate_code"]:
            print("🔄 DUPLICATE CODE:")
            for item in self.analysis["duplicate_code"][:3]:  # Top 3
                print(f"  • Line {item['line']} appears {item['duplicates']} times")
                print(f"    Code: {item['code']}")
                print(f"    → {item['suggestion']}")
            if len(self.analysis["duplicate_code"]) > 3:
                print(f"    ... and {len(self.analysis['duplicate_code'])-3} more")
            print()

        if self.analysis["circular_deps"]:
            print("🔄 CIRCULAR DEPENDENCIES:")
            for item in self.analysis["circular_deps"]:
                print(f"  • {item['file']}")
                print(f"    → {item['suggestion']}")
            print()

        if self.analysis["suggestions"]:
            print("💡 REFACTORING SUGGESTIONS:")
            for i, suggestion in enumerate(self.analysis["suggestions"], 1):
                print(f"\n  {i}. {suggestion['type'].upper()}")
                print(f"     Target: {suggestion['target']}")
                print(f"     Action: {suggestion['action']}")

        if not any(
            [
                self.analysis["file_too_large"],
                self.analysis["high_complexity"],
                self.analysis["duplicate_code"],
                self.analysis["circular_deps"],
            ]
        ):
            print("✅ No refactoring needed - code looks good!")

        print("\n" + "=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Suggest refactoring improvements")
    parser.add_argument("--target", required=True, help="File or directory to analyze")
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Show detailed output"
    )

    args = parser.parse_args()

    advisor = RefactoringAdvisor(args.target)
    advisor.run()
    advisor.print_report()


if __name__ == "__main__":
    main()
