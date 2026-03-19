#!/usr/bin/env python3
"""
Build complete A-LEMS documentation
Run: python scripts/tools/build_docs.py
"""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DOCS_DIR = ROOT / "docs"


def main():
    print("📚 Building A-LEMS Documentation")
    print("=" * 50)

    # Generate diagrams
    print("\n1. Generating diagrams...")
    subprocess.run([sys.executable, "scripts/tools/generate_diagrams.py"])

    # Build docs using mkdocs.yml in docs folder
    print("\n2. Building MkDocs site...")
    result = subprocess.run(
        ["mkdocs", "build", "-f", str(DOCS_DIR / "mkdocs.yml")],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )

    if result.returncode == 0:
        print("\n✅ Documentation built successfully!")
        print(f"   Open: {ROOT / 'site' / 'index.html'}")
    else:
        print("\n❌ Build failed:")
        print(result.stderr)


if __name__ == "__main__":
    main()
