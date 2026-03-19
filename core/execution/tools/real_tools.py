#!/usr/bin/env python3
"""
================================================================================
REAL TOOLS – Actual tool implementations for realistic workloads
================================================================================

Purpose:
    Replace simulated tools with real ones that perform actual work.
    This ensures orchestration tax measurements reflect real-world overhead.

Author: Deepak Panigrahy
================================================================================
"""

import hashlib
import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List

import requests


class DatabaseQueryTool:
    """Real tool that queries a local database."""

    def __init__(self, db_path: str = "data/experiments.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Initialize a small test database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_data (
                id INTEGER PRIMARY KEY,
                key TEXT,
                value TEXT
            )
        """)
        # Add some sample data
        sample = [("test1", "value1"), ("test2", "value2")]
        cursor.executemany(
            "INSERT OR IGNORE INTO test_data (key, value) VALUES (?, ?)", sample
        )
        conn.commit()
        conn.close()

    def execute(self, query: str) -> List[Dict]:
        """Execute SQL query."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(query)
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return results


class FileProcessorTool:
    """Real tool that processes files."""

    def __init__(self, data_dir: str = "data/test_files"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._create_test_files()

    def _create_test_files(self):
        """Create some test files for processing."""
        for i in range(10):
            file_path = self.data_dir / f"test_{i}.txt"
            if not file_path.exists():
                with open(file_path, "w") as f:
                    f.write(f"Test data {i}\n" * 100)

    def execute(self, operation: str, filename: str) -> Dict:
        """Perform file operations."""
        file_path = self.data_dir / filename

        if operation == "read":
            with open(file_path, "r") as f:
                content = f.read()
            return {"size": len(content), "lines": len(content.split("\n"))}

        elif operation == "hash":
            with open(file_path, "rb") as f:
                content = f.read()
            return {"md5": hashlib.md5(content).hexdigest()}

        elif operation == "count":
            with open(file_path, "r") as f:
                words = f.read().split()
            return {"word_count": len(words)}

        return {"error": "Unknown operation"}


class APIQueryTool:
    """Real tool that queries public APIs."""

    def execute(self, endpoint: str, params: Dict = None) -> Dict:
        """Make real API call."""
        # Use safe, free APIs
        if endpoint == "numbers":
            # Numbers API for facts
            url = f"http://numbersapi.com/{params.get('number', 42)}"
            response = requests.get(url, timeout=5)
            return {"fact": response.text}

        elif endpoint == "joke":
            # Free joke API
            response = requests.get(
                "https://v2.jokeapi.dev/joke/Any?type=single", timeout=5
            )
            return response.json()

        elif endpoint == "time":
            # World time API
            response = requests.get(
                "http://worldtimeapi.org/api/timezone/Etc/UTC", timeout=5
            )
            return response.json()

        return {"error": "Unknown endpoint"}
