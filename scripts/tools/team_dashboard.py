#!/usr/bin/env python3
"""
Collaboration Dashboard for A-LEMS
Goal 5: Show team activity, ownership, and knowledge gaps
Run: python scripts/tools/team_dashboard.py
"""

import argparse
import json
import subprocess
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path


class TeamDashboard:
    def __init__(self, days=30):
        self.repo_root = self._find_repo_root()
        self.days = days
        self.since_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

        self.stats = {
            "commits": [],
            "authors": set(),
            "files": defaultdict(lambda: {"commits": 0, "authors": set()}),
            "extensions": Counter(),
            "activity_by_day": defaultdict(int),
            "bus_factors": {},
        }

    def _find_repo_root(self) -> Path:
        current = Path(__file__).resolve().parent
        while current != current.parent:
            if (current / ".git").exists():
                return current
            current = current.parent
        return Path.cwd()

    def get_git_log(self):
        """Get git log for the specified time period"""
        try:
            result = subprocess.run(
                [
                    "git",
                    "log",
                    f"--since={self.since_date}",
                    "--pretty=format:%H|%an|%ae|%ad|%s",
                    "--name-only",
                ],
                capture_output=True,
                text=True,
                cwd=self.repo_root,
            )

            return result.stdout.split("\n")
        except:
            return []

    def analyze_commits(self):
        """Analyze commit history"""
        log_lines = self.get_git_log()

        current_commit = {}
        files = []

        for line in log_lines:
            if not line.strip():
                continue

            if "|" in line and len(line.split("|")) >= 5:
                # Save previous commit
                if current_commit and files:
                    self._process_commit(current_commit, files)

                # Start new commit
                parts = line.split("|")
                current_commit = {
                    "hash": parts[0][:8],
                    "author": parts[1],
                    "email": parts[2],
                    "date": parts[3],
                    "message": parts[4],
                }
                files = []
            elif current_commit and line.strip():
                files.append(line.strip())

        # Process last commit
        if current_commit and files:
            self._process_commit(current_commit, files)

    def _process_commit(self, commit, files):
        """Process a single commit"""
        self.stats["authors"].add(commit["author"])
        self.stats["commits"].append(commit)

        # Count by day
        date = commit["date"].split()[0]
        self.stats["activity_by_day"][date] += 1

        # Process files
        for file in files:
            if file.startswith("core/") or file.startswith("scripts/"):
                self.stats["files"][file]["commits"] += 1
                self.stats["files"][file]["authors"].add(commit["author"])

                # Count extensions
                ext = Path(file).suffix
                if ext:
                    self.stats["extensions"][ext] += 1

    def calculate_bus_factor(self):
        """Calculate bus factor for each file"""
        for file, data in self.stats["files"].items():
            authors = data["authors"]
            if len(authors) == 1:
                author = list(authors)[0]
                self.stats["bus_factors"][file] = {
                    "risk": "HIGH",
                    "sole_author": author,
                    "commits": data["commits"],
                }
            elif len(authors) == 2:
                self.stats["bus_factors"][file] = {
                    "risk": "MEDIUM",
                    "authors": len(authors),
                    "commits": data["commits"],
                }

    def get_hotspots(self):
        """Find files with most commits"""
        hotspots = []
        for file, data in sorted(
            self.stats["files"].items(), key=lambda x: x[1]["commits"], reverse=True
        )[:10]:
            hotspots.append(
                {
                    "file": file,
                    "commits": data["commits"],
                    "authors": len(data["authors"]),
                }
            )
        return hotspots

    def print_report(self):
        """Print formatted dashboard"""
        print("\n" + "=" * 60)
        print("👥 TEAM COLLABORATION DASHBOARD")
        print("=" * 60)
        print(f"Period: Last {self.days} days (since {self.since_date})")
        print()

        # Summary stats
        print("📊 SUMMARY")
        print(f"  • Total commits: {len(self.stats['commits'])}")
        print(f"  • Active authors: {len(self.stats['authors'])}")
        print(f"  • Files changed: {len(self.stats['files'])}")
        print()

        # Active authors
        if self.stats["authors"]:
            print("👨‍💻 ACTIVE AUTHORS")
            for author in sorted(self.stats["authors"]):
                # Count commits by this author
                author_commits = sum(
                    1 for c in self.stats["commits"] if c["author"] == author
                )
                print(f"  • {author}: {author_commits} commits")
            print()

        # Hotspots
        hotspots = self.get_hotspots()
        if hotspots:
            print("🔥 HOTSPOTS (Most Changed Files)")
            for i, hotspot in enumerate(hotspots, 1):
                risk = (
                    "🔴"
                    if hotspot["authors"] == 1
                    else "🟡" if hotspot["authors"] == 2 else "🟢"
                )
                print(f"  {risk} {hotspot['file']}")
                print(
                    f"     {hotspot['commits']} commits, {hotspot['authors']} authors"
                )
            print()

        # Bus factor (high risk files)
        high_risk = {
            f: d for f, d in self.stats["bus_factors"].items() if d["risk"] == "HIGH"
        }
        if high_risk:
            print("⚠️  BUS FACTOR RISKS (Single Author Files)")
            for file, data in list(high_risk.items())[:10]:
                print(f"  • {file}")
                print(
                    f"     Sole author: {data['sole_author']}, {data['commits']} commits"
                )
            if len(high_risk) > 10:
                print(f"     ... and {len(high_risk)-10} more")
            print()

        # Weekly activity
        if self.stats["activity_by_day"]:
            print("📅 ACTIVITY BY DAY")
            # Show last 14 days
            sorted_days = sorted(self.stats["activity_by_day"].items(), reverse=True)[
                :14
            ]
            for date, count in sorted(sorted_days):
                bar = "█" * min(count, 20)
                print(f"  {date}: {bar} {count}")
            print()

        # Recommendations
        print("💡 RECOMMENDATIONS")
        if high_risk:
            print("  • Review high-risk files and distribute knowledge")
        if hotspots:
            print("  • Consider refactoring hotspots to reduce complexity")
        if len(self.stats["authors"]) < 2:
            print("  • Consider pair programming to increase bus factor")
        else:
            print("  • Team is healthy - keep up the good work!")

        print("\n" + "=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Generate team collaboration dashboard"
    )
    parser.add_argument(
        "--days", type=int, default=30, help="Number of days to analyze"
    )
    parser.add_argument("--json", action="store_true", help="Output as JSON")

    args = parser.parse_args()

    dashboard = TeamDashboard(days=args.days)
    dashboard.analyze_commits()
    dashboard.calculate_bus_factor()

    if args.json:
        # Convert for JSON serialization
        output = {
            "period_days": args.days,
            "total_commits": len(dashboard.stats["commits"]),
            "active_authors": list(dashboard.stats["authors"]),
            "files_changed": len(dashboard.stats["files"]),
            "hotspots": dashboard.get_hotspots(),
            "bus_factors": dashboard.stats["bus_factors"],
        }
        print(json.dumps(output, indent=2, default=str))
    else:
        dashboard.print_report()


if __name__ == "__main__":
    main()
