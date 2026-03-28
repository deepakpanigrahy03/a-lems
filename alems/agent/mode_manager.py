"""
alems/agent/mode_manager.py
────────────────────────────────────────────────────────────────────────────
Reads and writes ~/.alems/agent.conf (TOML format).
The agent polls this file every loop iteration — mode changes take effect
within one poll cycle (≤10s) without restarting the agent.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

try:
    import tomllib          # Python 3.11+
except ImportError:
    try:
        import tomli as tomllib  # pip install tomli for older Python
    except ImportError:
        tomllib = None

CONF_DIR  = Path.home() / ".alems"
CONF_PATH = CONF_DIR / "agent.conf"

_DEFAULTS: dict[str, Any] = {
    "agent": {
        "mode":          "local",
        "server_url":    "http://129.153.71.47:8000",
        "api_key":       "",
        "hw_id_local":   1,
        "hw_id_server":  0,
    },
    "sync": {
        "interval_seconds": 60,
        "batch_size":       100,
        "retry_max":        3,
        "retry_backoff_s":  30,
    },
    "execution": {
        "poll_interval_s":  10,
        "heartbeat_s":      30,
        "heartbeat_run_s":  5,
    },
}


def _read_raw() -> dict:
    """Read conf file. Returns defaults if file missing or unreadable."""
    if not CONF_PATH.exists():
        return _DEFAULTS.copy()
    try:
        if tomllib:
            with open(CONF_PATH, "rb") as f:
                data = tomllib.load(f)
        else:
            # Fallback: very simple key=value parser for agent section only
            data = _simple_parse(CONF_PATH)
        # Merge with defaults so missing keys always have values
        merged = _DEFAULTS.copy()
        for section, vals in data.items():
            if section in merged:
                merged[section].update(vals)
        return merged
    except Exception as e:
        print(f"[mode_manager] Warning: could not read agent.conf: {e}")
        return _DEFAULTS.copy()


def _simple_parse(path: Path) -> dict:
    """Minimal TOML-like parser — handles [section] and key = value."""
    data: dict[str, dict] = {}
    section = None
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1].strip()
            data[section] = {}
        elif "=" in line and section:
            k, _, v = line.partition("=")
            v = v.strip().strip('"').strip("'")
            data[section][k.strip()] = v
    return data


def _write_conf(conf: dict) -> None:
    CONF_DIR.mkdir(parents=True, exist_ok=True)
    lines = []
    for section, vals in conf.items():
        lines.append(f"\n[{section}]")
        for k, v in vals.items():
            if isinstance(v, str):
                lines.append(f'{k} = "{v}"')
            else:
                lines.append(f"{k} = {v}")
    CONF_PATH.write_text("\n".join(lines) + "\n")


# ── Public API ────────────────────────────────────────────────────────────────

def get_mode() -> str:
    """Returns 'local' or 'connected'."""
    return _read_raw()["agent"].get("mode", "local")


def get_server_url() -> str:
    return _read_raw()["agent"].get("server_url", "http://129.153.71.47:8000")


def get_api_key() -> str:
    return _read_raw()["agent"].get("api_key", "")


def get_local_hw_id() -> int:
    return int(_read_raw()["agent"].get("hw_id_local", 1))


def get_server_hw_id() -> int:
    return int(_read_raw()["agent"].get("hw_id_server", 0))


def get_sync_config() -> dict:
    return _read_raw()["sync"]


def get_execution_config() -> dict:
    return _read_raw()["execution"]


def set_mode(mode: str) -> None:
    """Switch between 'local' and 'connected'."""
    assert mode in ("local", "connected"), f"Invalid mode: {mode}"
    conf = _read_raw()
    conf["agent"]["mode"] = mode
    _write_conf(conf)
    print(f"[mode_manager] Mode set to: {mode}")


def save_registration(api_key: str, server_hw_id: int) -> None:
    """Called after successful POST /register."""
    conf = _read_raw()
    conf["agent"]["api_key"] = api_key
    conf["agent"]["hw_id_server"] = server_hw_id
    _write_conf(conf)
    print(f"[mode_manager] Registration saved: server_hw_id={server_hw_id}")


def ensure_conf_exists() -> None:
    """Create default conf file if it doesn't exist."""
    if not CONF_PATH.exists():
        _write_conf(_DEFAULTS)
        print(f"[mode_manager] Created default config: {CONF_PATH}")


def is_registered() -> bool:
    return bool(get_api_key())
