#!/usr/bin/env python3
"""
MkDocs Documentation Generator for A-LEMS
Integrated with Goal 6 Web Controller
"""

import argparse
import os
import signal
import subprocess
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))
from path_loader import config

ROOT = Path(__file__).resolve().parents[2]
PID_FILE = ROOT / "scripts/tools/.mkdocs.pid"
import shutil

def check_config():
    """Verify mkdocs.yml exists"""
    if not config.MKDOCS_CONFIG.exists():
        print(f"❌ mkdocs.yml not found at {config.MKDOCS_CONFIG}")
        print("   Run with --init first to create it.")
        return False
    return True


def build():
    """Build documentation and copy diagrams"""
    if not check_config():
        return
    
    # Copy diagrams to source FIRST (so MkDocs finds them during build)
    print("📊 Copying diagrams to source...")
    source_diagrams = config.MKDOCS_SOURCE / "assets" / "diagrams"
    source_diagrams.mkdir(parents=True, exist_ok=True)
    
    for svg in config.DIAGRAMS_OUTPUT.glob("*.svg"):
        dest = source_diagrams / svg.name
        shutil.copy2(svg, dest)
        print(f"   📄 {svg.name}")
    
    print("📚 Building documentation...")
    
    # Build MkDocs
    subprocess.run([
        "mkdocs", "build",
        "-f", str(config.MKDOCS_CONFIG),
        "-d", str(config.MKDOCS_OUTPUT)
    ])
    
    print(f"✅ Build complete. Site in {config.MKDOCS_OUTPUT}")


def serve_background():
    """Start MkDocs server in background"""
    if not check_config():
        return

    print("🌐 Starting MkDocs server in background...")

    if PID_FILE.exists():
        with open(PID_FILE) as f:
            old_pid = f.read().strip()
        print(f"⚠️  Server already running with PID {old_pid}")
        return

    process = subprocess.Popen(
        ["mkdocs", "serve", "-f", str(config.MKDOCS_CONFIG), "-a", "0.0.0.0:8000"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    with open(PID_FILE, "w") as f:
        f.write(str(process.pid))

    print(f"✅ Server started on http://localhost:8000 (PID: {process.pid})")


def stop_server():
    """Stop MkDocs server"""
    if not PID_FILE.exists():
        print("❌ No server running")
        return

    with open(PID_FILE) as f:
        pid = int(f.read().strip())

    try:
        os.kill(pid, signal.SIGTERM)
        PID_FILE.unlink()
        print(f"✅ Server stopped (PID: {pid})")
    except ProcessLookupError:
        print(f"⚠️  Process {pid} not found, removing PID file")
        PID_FILE.unlink()


def serve_foreground():
    """Serve in foreground with automatic port fallback."""
    import socket
    
    if not check_config():
        return
    
    # Try ports from 8000 to 8010
    for port in range(8000, 8011):
        try:
            # Test if port is available
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('0.0.0.0', port))
            
            # Port is available - serve on it
            print(f"🌐 Serving docs at http://127.0.0.1:{port}")
            subprocess.run(["mkdocs", "serve", "-f", str(config.MKDOCS_CONFIG), "-a", f"0.0.0.0:{port}"])
            return
            
        except OSError:
            # Port in use, try next
            continue
    
    print("❌ No available ports in range 8000-8010")
def init_config():
    """Create default mkdocs.yml using config values"""
    if config.MKDOCS_CONFIG.exists():
        print(f"✅ mkdocs.yml already exists at {config.MKDOCS_CONFIG}")
        return
    
    print(f"📝 Creating default mkdocs.yml at {config.MKDOCS_CONFIG}")
    config.MKDOCS_CONFIG.parent.mkdir(parents=True, exist_ok=True)
    
    template = f"""site_name: {config.PROJECT_NAME} Documentation
site_description: {config.DESCRIPTION}
site_author: {config.AUTHOR}
repo_url: {config.REPO_URL}

theme:
  name: material
  features:
    - navigation.tabs
    - navigation.sections
    - toc.integrate

nav:
  - Home: index.md
  - Getting Started:
    - Installation: getting-started/01-installation.md
    - Database Setup: getting-started/02-database-setup.md
    - Model Config: getting-started/03-model-config.md
    - Quick Start: getting-started/04-quick-start.md
    - Troubleshooting: getting-started/05-troubleshooting.md

markdown_extensions:
  - pymdownx.highlight
  - pymdownx.superfences
  - admonition
  - footnotes
"""
    config.MKDOCS_CONFIG.write_text(template)
    print(f"✅ Created {config.MKDOCS_CONFIG}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", action="store_true", help="Build documentation")
    parser.add_argument("--serve", action="store_true", help="Serve in foreground")
    parser.add_argument("--serve-bg", action="store_true", help="Serve in background")
    parser.add_argument("--stop", action="store_true", help="Stop background server")
    parser.add_argument("--init", action="store_true", help="Create default mkdocs.yml")

    args = parser.parse_args()

    if args.init:
        init_config()
    if args.build:
        build()
    elif args.serve:
        serve_foreground()
    elif args.serve_bg:
        serve_background()
    elif args.stop:
        stop_server()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()