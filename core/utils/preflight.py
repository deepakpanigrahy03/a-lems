#!/usr/bin/env python3
"""Pre-flight validation for A-LEMS experiments."""

import os
import sys
from pathlib import Path


def get_env(key):
    """Get env var from environment or .env file."""
    val = os.getenv(key)
    if val:
        return val
    env_file = Path("core/.env")
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                if line.startswith(f"{key}="):
                    return line.strip().split("=", 1)[1]
    return None


def check_msr():
    """Check MSR helper."""
    msr = Path("core/msr_helper/msr_read")
    if not msr.exists():
        sys.exit("❌ MSR helper not found. Run: sudo ./scripts/fix_permissions.sh")
    if not os.access(msr, os.X_OK):
        sys.exit("❌ MSR helper not executable. Run: sudo chmod +x core/msr_helper/msr_read")
    print("✅ MSR helper: OK")


def check_configs():
    """Check essential config files exist."""
    required = [
        "config/models.json",
        "config/hw_config.json",
        "config/app_settings.yaml",
    ]
    for f in required:
        if not Path(f).exists():
            sys.exit(f"❌ Config file missing: {f}")
    print("✅ Config files: OK")


def check_cloud(config):
    """Check cloud API."""
    key = get_env(config.get("api_key_env", "GROQ_API_KEY"))
    if not key:
        sys.exit("❌ API key not found in environment or core/.env")
    
    import requests
    try:
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {key}"},
            json={"model": "llama-3.3-70b-versatile", "messages": [{"role": "user", "content": "hi"}], "max_tokens": 1},
            timeout=3
        )
        if r.status_code != 200:
            sys.exit(f"❌ API key invalid: {r.status_code}")
    except Exception as e:
        sys.exit(f"❌ API test failed: {e}")
    print("✅ Cloud API: OK")


def check_local(config):
    """Check local LLM."""
    try:
        import llama_cpp
    except ImportError:
        sys.exit("❌ llama-cpp-python not installed. Run: pip install llama-cpp-python")
    print("✅ llama-cpp-python: OK")
    
    model = config.get("model_path")
    if model and not Path(model).exists():
        sys.exit(f"❌ Model not found: {model}")
    print(f"✅ Model file: {model}")


def preflight(executor, provider):
    """Run checks."""
    print("\n🔍 Pre-flight checks:\n")
    check_msr()
    check_configs()
    if provider == "cloud":
        check_cloud(executor.config)
    else:
        check_local(executor.config)
    print("\n✅ All checks passed! Ready to run experiments.\n")


if __name__ == "__main__":
    import argparse
    from core.config_loader import ConfigLoader
    
    p = argparse.ArgumentParser()
    p.add_argument("--provider", choices=["cloud", "local"], required=True)
    args = p.parse_args()
    
    cfg = ConfigLoader().get_model_config(args.provider, "linear")
    preflight(type('obj', (object,), {'config': cfg})(), args.provider)