#!/usr/bin/env python3
"""
Task loader – reads predefined tasks from YAML configuration.
"""

import sys
from pathlib import Path

# ============================================================================
# Add project root to Python path
# ============================================================================
project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


def load_tasks(config_path: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Load tasks from YAML file.

    Args:
        config_path: Path to tasks.yaml. If None, uses default 'config/tasks.yaml'.

    Returns:
        List of task dictionaries, each containing id, name, description,
        level, tool_calls, prompt, tags.

    Raises:
        FileNotFoundError: If config file does not exist.
    """
    if config_path is None:
        # Assume we're running from project root
        config_path = Path(__file__).parent.parent.parent / "config" / "tasks.yaml"
    else:
        config_path = Path(config_path)

    if not config_path.exists():
        raise FileNotFoundError(f"Task config not found: {config_path}")

    with open(config_path, "r") as f:
        data = yaml.safe_load(f)

    return data.get("tasks", [])


def get_task_by_id(task_id: str, tasks: Optional[List[Dict]] = None) -> Optional[Dict]:
    """Find a task by its id."""
    if tasks is None:
        tasks = load_tasks()
    for t in tasks:
        if t["id"] == task_id:
            return t
    return None


def get_tasks_by_level(level: int, tasks: Optional[List[Dict]] = None) -> List[Dict]:
    """Filter tasks by complexity level."""
    if tasks is None:
        tasks = load_tasks()
    return [t for t in tasks if t["level"] == level]


def list_task_summary(tasks: Optional[List[Dict]] = None) -> None:
    """
    Print a formatted summary of all tasks.

    Args:
        tasks: Optional list of tasks. If None, loads fresh.
    """
    if tasks is None:
        tasks = load_tasks()

    print("\n📋 Available Tasks:")
    print("-" * 70)
    print(f"{'ID':<15} {'Name':<25} {'Level':<8} {'Tools':<8}")
    print("-" * 70)
    for t in tasks:
        print(
            f"{t['id']:<15} {t['name'][:24]:<25} {t['level']:<8} {t['tool_calls']:<8}"
        )
    print("-" * 70)
