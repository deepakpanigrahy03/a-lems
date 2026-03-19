#!/usr/bin/env python3
"""
A-LEMS Tunnel Agent  —  localhost.run + auto-update HF Space
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
How it works:
  1. Starts FastAPI server on port 8765
  2. Opens localhost.run tunnel → gets a URL
  3. Pushes the URL to your HF Space repo (live_url.json)
  4. HF Space sidebar reads live_url.json and shows it automatically
  5. Researchers just open the HF Space — current URL is always there
  6. Auto-restarts tunnel if it drops, pushes new URL each time

USAGE:
  cd ~/mydrive/a-lems
  source venv/bin/activate
  python tunnel_agent.py

  # Background:
  nohup python tunnel_agent.py > logs/tunnel.log 2>&1 &
  tail -f logs/tunnel.log

FIRST TIME SETUP:
  Set hf_space_repo in config/tunnel.yaml
  Make sure you have git configured with HF credentials
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

try:
    import yaml as _yaml

    _YAML_OK = True
except ImportError:
    _YAML_OK = False

ROOT = Path(__file__).parent
CFG_FILE = ROOT / "config" / "tunnel.yaml"
STATE_FILE = ROOT / ".tunnel_state.json"
LOG_DIR = ROOT / "logs"
PORT = int(os.environ.get("ALEMS_PORT", 8765))
SSH_KEY = str(Path.home() / ".ssh" / "id_rsa_tunnel")

BANNER = """
╔══════════════════════════════════════════════════════════════╗
║       ⚡  A-LEMS Tunnel Agent  (auto-update HF Space)       ║
╚══════════════════════════════════════════════════════════════╝"""


# ── Config ────────────────────────────────────────────────────────────────────


def _load_config() -> dict:
    if not CFG_FILE.exists():
        print(f"  ❌  config/tunnel.yaml not found")
        sys.exit(1)
    if not _YAML_OK:
        print("  ❌  pip install pyyaml")
        sys.exit(1)
    with open(CFG_FILE) as f:
        cfg = _yaml.safe_load(f)
    token = cfg.get("token", "")
    if not token or "choose-a-passphrase" in token:
        print("  ❌  Set token in config/tunnel.yaml")
        print(
            "     Generate: python -c \"import secrets; print('alems-'+secrets.token_urlsafe(12))\""
        )
        sys.exit(1)
    return cfg


# ── Push URL to HF Space repo ─────────────────────────────────────────────────


def _push_url_to_hf(url: str, token: str, hf_repo_path: str):
    """
    Write live_url.json into the HF Space repo and git push.
    HF Space sidebar reads this file to show the current tunnel URL.
    """
    hf_path = Path(hf_repo_path).expanduser()
    if not hf_path.exists():
        print(f"  ⚠️   HF Space repo not found at: {hf_path}")
        print(f"      Set hf_space_repo in config/tunnel.yaml")
        return False

    live_file = hf_path / "live_url.json"
    live_file.write_text(
        json.dumps(
            {
                "url": url,
                "token": token,
                "online": True,
                "updated": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
            },
            indent=2,
        )
    )

    try:
        subprocess.run(
            ["git", "add", "live_url.json"],
            cwd=str(hf_path),
            check=True,
            capture_output=True,
        )
        subprocess.run(
            [
                "git",
                "commit",
                "-m",
                f"tunnel: update live URL {time.strftime('%H:%M')}",
            ],
            cwd=str(hf_path),
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["git", "push"], cwd=str(hf_path), check=True, capture_output=True
        )
        print(f"  ✅  URL pushed to HF Space → researchers see it automatically")
        return True
    except subprocess.CalledProcessError as e:
        # Nothing to commit is fine
        if b"nothing to commit" in (e.stdout or b"") + (e.stderr or b""):
            return True
        print(f"  ⚠️   Git push failed: {e.stderr.decode() if e.stderr else e}")
        return False


def _push_offline_to_hf(hf_repo_path: str):
    """Mark the lab as offline in HF Space."""
    hf_path = Path(hf_repo_path).expanduser()
    if not hf_path.exists():
        return
    live_file = hf_path / "live_url.json"
    live_file.write_text(
        json.dumps(
            {
                "url": "",
                "token": "",
                "online": False,
                "updated": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
            },
            indent=2,
        )
    )
    try:
        subprocess.run(
            ["git", "add", "live_url.json"],
            cwd=str(hf_path),
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["git", "commit", "-m", "tunnel: lab offline"],
            cwd=str(hf_path),
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["git", "push"], cwd=str(hf_path), check=True, capture_output=True
        )
    except Exception:
        pass


# ── State file ────────────────────────────────────────────────────────────────


def _write_state(url: str, token: str, online: bool):
    LOG_DIR.mkdir(exist_ok=True)
    STATE_FILE.write_text(
        json.dumps(
            {
                "online": online,
                "url": url,
                "token": token,
                "ts": time.time(),
            }
        )
    )


def _clear_state():
    STATE_FILE.write_text(json.dumps({"online": False, "url": "", "token": ""}))


# ── FastAPI server ────────────────────────────────────────────────────────────


def _start_server(token: str) -> subprocess.Popen:
    env = {**os.environ, "ALEMS_TOKEN": token, "ALEMS_LIVE_MODE": "1"}
    cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "server:app",
        "--host",
        "127.0.0.1",
        "--port",
        str(PORT),
        "--log-level",
        "warning",
    ]
    print(f"  Starting FastAPI server on port {PORT}...")
    proc = subprocess.Popen(
        cmd, cwd=str(ROOT), env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    threading.Thread(
        target=lambda p: [_ for _ in p.stdout], args=(proc,), daemon=True
    ).start()
    time.sleep(2)
    if proc.poll() is not None:
        print(f"  ❌  Server failed to start")
        sys.exit(1)
    print(f"  ✅  Server running (pid {proc.pid})")
    return proc


# ── Cloudflare quick tunnel ───────────────────────────────────────────────────


def _start_tunnel() -> tuple:
    """Returns (process, url). Uses cloudflared quick tunnel — no login needed."""
    # Check cloudflared is installed
    import shutil

    if not shutil.which("cloudflared"):
        print("  ❌  cloudflared not found. Install it:")
        print(
            "      wget https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64.deb"
        )
        print("      sudo dpkg -i cloudflared-linux-amd64.deb")
        return None, None

    cmd = [
        "cloudflared",
        "tunnel",
        "--url",
        f"http://localhost:{PORT}",
        "--no-autoupdate",
    ]
    print(f"  Starting Cloudflare tunnel...")
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )

    url = None
    output_lines = []

    # cloudflared prints the URL to stderr/stdout within ~5s
    import select as _sel

    deadline = time.time() + 35
    while time.time() < deadline:
        ready = _sel.select([proc.stdout], [], [], 2.0)[0]
        if not ready:
            if proc.poll() is not None:
                break
            continue
        line = proc.stdout.readline()
        if not line:
            break
        line = line.rstrip()
        output_lines.append(line)
        # cloudflared prints: "https://xxxx-xxxx-xxxx.trycloudflare.com"
        m = re.search(r"https://[a-z0-9\-]+\.trycloudflare\.com", line)
        if m:
            url = m.group(0)
            break

    # Drain in background
    threading.Thread(
        target=lambda p: [_ for _ in p.stdout], args=(proc,), daemon=True
    ).start()

    if proc.poll() is not None:
        print(f"  ❌  Tunnel exited immediately")
        for l in output_lines[-5:]:
            print(f"     {l}")
        return proc, None

    if url:
        print(f"  ✅  Tunnel online → {url}")
    else:
        print(f"  ⚠️   Tunnel started but could not extract URL")
        print(f"      Last output: {output_lines[-3:] if output_lines else '(none)'}")

    return proc, url


# ── Health check ──────────────────────────────────────────────────────────────


def _server_alive() -> bool:
    try:
        import urllib.request

        r = urllib.request.urlopen(f"http://127.0.0.1:{PORT}/health", timeout=3)
        return r.status == 200
    except Exception:
        return False


# ── Main ──────────────────────────────────────────────────────────────────────


def main():
    print(BANNER)
    LOG_DIR.mkdir(exist_ok=True)

    cfg = _load_config()
    token = cfg["token"]
    hf_repo = cfg.get("hf_space_repo", "~/mydrive/hf-space")

    print(f"\n  Token      : {token}")
    print(f"  HF repo    : {hf_repo}")
    print(f"  Port       : {PORT}\n")

    srv = _start_server(token)
    tun, url = _start_tunnel()

    # Retry tunnel up to 5 times if connection reset
    for _attempt in range(5):
        if url or (tun.poll() is None):
            break
        print(f"  Retry {_attempt+1}/5 in 20s...")
        time.sleep(20)
        tun, url = _start_tunnel()

    if url:
        _write_state(url, token, True)
        _push_url_to_hf(url, token, hf_repo)
    else:
        url = "connecting..."
        _write_state(url, token, False)

    print(f"""
╔══════════════════════════════════════════════════════════════╗
║  🟢  A-LEMS is LIVE                                         ║
║                                                              ║
║  Current URL  →  {url:<44}║
║  Token        →  {token:<44}║
║                                                              ║
║  URL pushed to HF Space — researchers see it automatically. ║
║  They just open the Space — no URL sharing needed!          ║
║                                                              ║
║  HF Space: huggingface.co/spaces/a-lems/Energy              ║
║                                                              ║
║  Ctrl+C to go offline.                                      ║
╚══════════════════════════════════════════════════════════════╝
""")

    # Graceful shutdown
    def _shutdown(sig, frame):
        print("\n  Shutting down A-LEMS...")
        _clear_state()
        _push_offline_to_hf(hf_repo)
        try:
            tun.terminate()
        except:
            pass
        try:
            srv.terminate()
        except:
            pass
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # ── Heartbeat — auto-restart if anything dies ─────────────────────────────
    srv_ref = [srv]
    tun_ref = [tun]
    url_ref = [url]

    while True:
        time.sleep(15)
        ts = time.strftime("%H:%M:%S")

        # Check server
        if srv_ref[0].poll() is not None or not _server_alive():
            print(f"  [{ts}]  ⚠️  Server died — restarting...")
            try:
                srv_ref[0].terminate()
            except:
                pass
            time.sleep(1)
            srv_ref[0] = _start_server(token)

        # Check tunnel
        if tun_ref[0].poll() is not None:
            print(f"  [{ts}]  ⚠️  Tunnel died — restarting in 15s...")
            time.sleep(15)
            new_tun, new_url = _start_tunnel()
            tun_ref[0] = new_tun
            if new_url and new_url != url_ref[0]:
                url_ref[0] = new_url
                print(f"  [{ts}]  📡  New URL: {new_url}")
                _write_state(new_url, token, True)
                _push_url_to_hf(new_url, token, hf_repo)
            elif new_url:
                _write_state(url_ref[0], token, True)
            else:
                print(f"  [{ts}]  ⚠️  Tunnel still not connected — will retry in 15s")

        print(f"  [{ts}]  🟢  {url_ref[0]}")


if __name__ == "__main__":
    main()
