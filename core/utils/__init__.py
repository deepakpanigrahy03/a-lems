#!/usr/bin/env python3
"""
================================================================================
TASK LOADER – Load predefined tasks from YAML configuration
================================================================================

Purpose:
    Provides a central interface to load task definitions from config/tasks.yaml.
    Used by both experiment runner and GUI to ensure consistency.

Author: Deepak Panigrahy
================================================================================
"""

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
        yaml.YAMLError: If YAML parsing fails.
    """
    # Determine config path
    if config_path is None:
        # Assume we're running from project root
        config_path = Path(__file__).parent.parent.parent / "config" / "tasks.yaml"
    else:
        config_path = Path(config_path)

    # Check if file exists
    if not config_path.exists():
        raise FileNotFoundError(f"Task config not found: {config_path}")

    # Load and parse YAML
    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    return data.get("tasks", [])


def get_task_by_id(task_id: str, tasks: Optional[List[Dict]] = None) -> Optional[Dict]:
    """
    Find a task by its ID.

    Args:
        task_id: The task identifier (e.g., 'simple', 'capital')
        tasks: Optional list of tasks. If None, loads fresh.

    Returns:
        Task dictionary or None if not found.
    """
    if tasks is None:
        tasks = load_tasks()

    for task in tasks:
        if task["id"] == task_id:
            return task
    return None


def get_tasks_by_level(level: int, tasks: Optional[List[Dict]] = None) -> List[Dict]:
    """
    Filter tasks by complexity level.

    Args:
        level: 1, 2, or 3
        tasks: Optional list of tasks. If None, loads fresh.

    Returns:
        List of tasks matching the level.
    """
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
