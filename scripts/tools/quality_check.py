#!/usr/bin/env python3
"""
Code Quality Guardian for A-LEMS
Goal 3: Run comprehensive quality checks on codebase
Run: python scripts/tools/quality_check.py
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path


class QualityChecker:
    def __init__(self, target_dir="core", verbose=False):
        self.repo_root = self._find_repo_root()
        self.target = self.repo_root / target_dir
        self.verbose = verbose
        self.results = {
            "timestamp": datetime.now().isoformat(),
            "target": str(self.target),
            "checks": {},
        }

    def _find_repo_root(self) -> Path:
        current = Path(__file__).resolve().parent
        while current != current.parent:
            if (current / ".git").exists():
                return current
            current = current.parent
        return Path.cwd()

    def run_pylint(self):
        """Run pylint for code quality"""
        print("🔍 Running pylint...")
        try:
            result = subprocess.run(
                ["pylint", str(self.target), "--output-format=json"],
                capture_output=True,
                text=True,
            )

            if result.stdout:
                issues = json.loads(result.stdout)
                self.results["checks"]["pylint"] = {
                    "total_issues": len(issues),
                    "issues": issues[:10] if self.verbose else len(issues),
                }
            else:
                self.results["checks"]["pylint"] = {"status": "clean"}

        except FileNotFoundError:
            self.results["checks"]["pylint"] = {"error": "pylint not installed"}

    def run_mypy(self):
        """Run mypy for type checking"""
        print("🔍 Running mypy...")
        try:
            result = subprocess.run(
                ["mypy", str(self.target)], capture_output=True, text=True
            )

            errors = [line for line in result.stdout.split("\n") if "error" in line]
            self.results["checks"]["mypy"] = {
                "total_errors": len(errors),
                "errors": errors[:10] if self.verbose else len(errors),
            }
        except FileNotFoundError:
            self.results["checks"]["mypy"] = {"error": "mypy not installed"}

    def run_radon(self):
        """Run radon for complexity analysis"""
        print("🔍 Running radon complexity analysis...")
        try:
            result = subprocess.run(
                ["radon", "cc", str(self.target), "-s", "-j"],
                capture_output=True,
                text=True,
            )

            if not result.stdout:
                return

            complexity_data = json.loads(result.stdout)
            high_complexity = []

            for file_path, functions in complexity_data.items():
                for func in functions:
                    if not isinstance(func, dict):
                        continue
                    comp_value = func.get("complexity", 0)
                    if comp_value > 10:
                        high_complexity.append(
                            {
                                "file": file_path,
                                "function": func.get("name", "unknown"),
                                "complexity": comp_value,
                            }
                        )

            count = len(high_complexity)
            display = high_complexity[:10] if self.verbose else count
            self.results["checks"]["radon"] = {"high_complexity": display}

        except FileNotFoundError:
            self.results["checks"]["radon"] = {"error": "radon not installed"}
        except Exception as e:
            self.results["checks"]["radon"] = {"error": str(e)}

    def run_bandit(self):
        """Run bandit for security issues"""
        print("🔍 Running bandit security check...")
        try:
            result = subprocess.run(
                ["bandit", "-r", str(self.target), "-f", "json"],
                capture_output=True,
                text=True,
            )

            if result.stdout and result.stdout.strip():
                try:
                    security = json.loads(result.stdout)
                    results_list = security.get("results", [])
                    high_count = len(
                        [r for r in results_list if r.get("issue_severity") == "HIGH"]
                    )

                    self.results["checks"]["bandit"] = {
                        "total_issues": len(results_list),
                        "high_severity": high_count,
                    }
                except json.JSONDecodeError:
                    self.results["checks"]["bandit"] = {
                        "error": "bandit output not JSON",
                        "output": result.stdout[:200],
                    }
            else:
                self.results["checks"]["bandit"] = {
                    "total_issues": 0,
                    "high_severity": 0,
                }

        except FileNotFoundError:
            self.results["checks"]["bandit"] = {"error": "bandit not installed"}
        except Exception as e:
            self.results["checks"]["bandit"] = {"error": str(e)}

    def run_vulture(self):
        """Run vulture for dead code detection"""
        print("🔍 Running vulture dead code check...")
        try:
            result = subprocess.run(
                ["vulture", str(self.target)], capture_output=True, text=True
            )

            dead_code = [line for line in result.stdout.split("\n") if line.strip()]
            count = len(dead_code)
            display = dead_code[:10] if self.verbose else count

            self.results["checks"]["vulture"] = {"total_dead": count, "items": display}
        except FileNotFoundError:
            self.results["checks"]["vulture"] = {"error": "vulture not installed"}

    def run_black_check(self):
        """Check formatting with black"""
        print("🔍 Checking formatting with black...")
        try:
            result = subprocess.run(
                ["black", "--check", str(self.target)], capture_output=True, text=True
            )

            self.results["checks"]["black"] = {"would_reformat": result.returncode != 0}
        except FileNotFoundError:
            self.results["checks"]["black"] = {"error": "black not installed"}

    def run_isort_check(self):
        """Check import sorting with isort"""
        print("🔍 Checking import sorting with isort...")
        try:
            result = subprocess.run(
                ["isort", "--check-only", str(self.target)],
                capture_output=True,
                text=True,
            )

            self.results["checks"]["isort"] = {"needs_sorting": result.returncode != 0}
        except FileNotFoundError:
            self.results["checks"]["isort"] = {"error": "isort not installed"}

    def run_all(self):
        """Run all quality checks"""
        self.run_pylint()
        self.run_mypy()
        self.run_radon()
        self.run_bandit()
        self.run_vulture()
        self.run_black_check()
        self.run_isort_check()
        return self.results

    def print_report(self):
        """Print formatted report"""
        print("\n" + "=" * 60)
        print("📊 CODE QUALITY REPORT")
        print("=" * 60)
        print(f"Generated: {self.results['timestamp']}")
        print(f"Target: {self.results['target']}")
        print()

        checks = self.results["checks"]

        if "pylint" in checks:
            pylint = checks["pylint"]
            if "error" in pylint:
                print(f"⚠️  Pylint: {pylint['error']}")
            else:
                count = pylint.get("total_issues", 0)
                status = "✅" if count == 0 else "❌"
                print(f"{status} Pylint issues: {count}")

        if "mypy" in checks:
            mypy = checks["mypy"]
            if "error" in mypy:
                print(f"⚠️  Mypy: {mypy['error']}")
            else:
                count = mypy.get("total_errors", 0)
                status = "✅" if count == 0 else "❌"
                print(f"{status} Type errors: {count}")

        if "radon" in checks:
            radon = checks["radon"]
            if "error" in radon:
                print(f"⚠️  Radon: {radon['error']}")
            else:
                high = radon.get("high_complexity", [])
                if isinstance(high, int):
                    count = high
                else:
                    count = len(high)
                status = "✅" if count == 0 else "⚠️"
                print(f"{status} High complexity functions: {count}")

        if "bandit" in checks:
            bandit = checks["bandit"]
            if "error" in bandit:
                print(f"⚠️  Bandit: {bandit['error']}")
            else:
                high = bandit.get("high_severity", 0)
                total = bandit.get("total_issues", 0)
                status = "✅" if high == 0 else "🔴"
                print(f"{status} Security issues: {total} (High: {high})")

        if "vulture" in checks:
            vulture = checks["vulture"]
            if "error" in vulture:
                print(f"⚠️  Vulture: {vulture['error']}")
            else:
                count = vulture.get("total_dead", 0)
                status = "✅" if count == 0 else "⚠️"
                print(f"{status} Dead code candidates: {count}")

        if "black" in checks:
            black = checks["black"]
            if "error" in black:
                print(f"⚠️  Black: {black['error']}")
            else:
                status = "✅" if not black.get("would_reformat") else "⚠️"
                print(
                    f"{status} Black formatting: {'Clean' if not black.get('would_reformat') else 'Needs formatting'}"
                )

        if "isort" in checks:
            isort = checks["isort"]
            if "error" in isort:
                print(f"⚠️  Isort: {isort['error']}")
            else:
                status = "✅" if not isort.get("needs_sorting") else "⚠️"
                print(
                    f"{status} Import sorting: {'Clean' if not isort.get('needs_sorting') else 'Needs sorting'}"
                )

        print("\n" + "=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Run quality checks on A-LEMS codebase"
    )
    parser.add_argument(
        "--target", default="core", help="Target directory (default: core)"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Show detailed output"
    )
    parser.add_argument("--save", "-s", help="Save results to file")
    parser.add_argument("--json", action="store_true", help="Output as JSON")

    args = parser.parse_args()

    checker = QualityChecker(target_dir=args.target, verbose=args.verbose)
    results = checker.run_all()

    if args.json:
        print(json.dumps(results, indent=2))
    else:
        checker.print_report()

    if args.save:
        with open(args.save, "w") as f:
            if args.save.endswith(".json"):
                json.dump(results, f, indent=2)
            else:
                import io
                from contextlib import redirect_stdout

                buffer = io.StringIO()
                with redirect_stdout(buffer):
                    checker.print_report()

                with open(args.save, "w") as f:
                    f.write(buffer.getvalue())

        print(f"\n✅ Results saved to {args.save}")


if __name__ == "__main__":
    main()
