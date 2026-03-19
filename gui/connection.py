"""
gui/connection.py
Manages live connection from HF Space to the laptop server via Cloudflare tunnel.

st.session_state["conn"]:
    url       : permanent Cloudflare tunnel URL
    token     : shared access token
    verified  : bool - reached /api/online successfully
    harness   : bool - measurement harness loaded on laptop
    mode      : "offline" | "online"
    error     : last error message
"""

import streamlit as st

try:
    import requests as _req

    _REQUESTS_OK = True
except ImportError:
    _REQUESTS_OK = False

_DEFAULT = dict(
    url="", token="", verified=False, harness=False, mode="offline", error=""
)


def get_conn() -> dict:
    if "conn" not in st.session_state:
        st.session_state["conn"] = dict(_DEFAULT)
    return st.session_state["conn"]


def is_online() -> bool:
    return get_conn().get("verified", False)


def get_token() -> str:
    return get_conn().get("token", "")


def verify_connection(url: str, token: str) -> tuple:
    """Returns (success: bool, message: str, harness: bool)"""
    if not _REQUESTS_OK:
        return False, "pip install requests", False
    try:
        r = _req.get(f"{url.rstrip('/')}/api/online", timeout=6)
        if r.status_code == 200:
            d = r.json()
            if d.get("auth_required") and not token:
                return False, "Lab requires a token.", False
            return True, "Connected", d.get("harness", False)
        elif r.status_code == 403:
            return False, "Wrong token.", False
        else:
            return False, f"Server returned HTTP {r.status_code}", False
    except Exception as e:
        return False, f"Cannot reach server: {e}", False


def api_post(path: str, payload: dict, timeout: int = 30):
    """POST to live server. Returns (data, error)."""
    conn = get_conn()
    if not conn["verified"]:
        return None, "Not connected to live lab."
    try:
        r = _req.post(
            f"{conn['url'].rstrip('/')}{path}",
            json={**payload, "token": conn["token"]},
            timeout=timeout,
        )
        r.raise_for_status()
        return r.json(), None
    except Exception as e:
        return None, str(e)


def api_get(path: str, timeout: int = 10):
    """GET from live server. Returns (data, error)."""
    conn = get_conn()
    if not conn["verified"]:
        return None, "Not connected."
    try:
        r = _req.get(f"{conn['url'].rstrip('/')}{path}", timeout=timeout)
        r.raise_for_status()
        return r.json(), None
    except Exception as e:
        return None, str(e)


def disconnect():
    st.session_state["conn"] = dict(_DEFAULT)
