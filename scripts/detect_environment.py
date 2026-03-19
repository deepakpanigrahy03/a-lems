#!/usr/bin/env python3
"""Detect environment and save to config/environment.json"""

import hashlib
import json
import platform
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


def get_git_info():
    """Get git information"""
    info = {"commit": None, "branch": None, "dirty": None}
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip()[:16]
        branch = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"], text=True
        ).strip()
        status = subprocess.check_output(
            ["git", "status", "--porcelain"], text=True
        ).strip()
        info = {"commit": commit, "branch": branch, "dirty": bool(status)}
    except:
        pass
    return info


def get_dependency_versions():
    """Get key dependency versions"""
    deps = {"numpy": None, "torch": None, "transformers": None}
    try:
        import numpy

        deps["numpy"] = numpy.__version__
    except:
        pass
    try:
        import torch

        deps["torch"] = torch.__version__
    except:
        pass
    try:
        import transformers

        deps["transformers"] = transformers.__version__
    except:
        pass
    return deps


def generate_env_hash(hash_input):
    """Generate environment fingerprint from flat input"""
    hash_str = json.dumps(hash_input, sort_keys=True)
    return hashlib.sha256(hash_str.encode()).hexdigest()[:16]


def save_with_merge(path, new_data):
    """Merge with existing JSON"""
    if path.exists():
        existing = json.loads(path.read_text())
        existing.update(new_data)
        path.write_text(json.dumps(existing, indent=2))
    else:
        path.write_text(json.dumps(new_data, indent=2))


def main():
    config_path = Path("config/environment.json")

    git_info = get_git_info()
    deps = get_dependency_versions()

    # FLAT structure - no nested dicts!
    env_info = {
        "python_version": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "os_name": platform.system(),
        "os_version": platform.version(),
        "kernel_version": platform.release(),
        "git_commit": git_info["commit"],
        "git_branch": git_info["branch"],
        "git_dirty": git_info["dirty"],
        "numpy_version": deps["numpy"],
        "torch_version": deps["torch"],
        "transformers_version": deps["transformers"],
        "container_runtime": None,
        "container_image": None,
    }

    # Generate hash from flat fields
    hash_input = {
        "python_version": env_info["python_version"],
        "os_name": env_info["os_name"],
        "git_commit": env_info["git_commit"],
        "numpy_version": env_info["numpy_version"],
    }
    env_info["env_hash"] = generate_env_hash(hash_input)

    save_with_merge(config_path, env_info)
    print(f"✅ Environment detected: {env_info['env_hash']}")


if __name__ == "__main__":
    main()
