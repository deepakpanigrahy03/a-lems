#!/usr/bin/env python3
"""
================================================================================
LLM SETUP TEST – Verify all model configurations work
================================================================================
"""

import argparse
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# ============================================================================
# Add project root to Python path (works from anywhere)
# ============================================================================
project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Load environment variables
env_path = Path(__file__).parent.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)
print(f"📁 Loading .env from: {env_path}")

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from core.config_loader import ConfigLoader
from core.execution.agentic import AgenticExecutor
from core.execution.linear import LinearExecutor
from core.utils.debug import is_debug_enabled


class LLMSetupTester:
    def __init__(self):
        self.config = ConfigLoader()
        self.test_prompt = "What is 2+2? Answer in one word."

    def test_cloud_model(self, mode: str = "linear") -> dict:
        print(f"\n☁️ Testing CLOUD {mode.upper()} model...")

        try:
            model_config = self.config.get_model_config("cloud", mode)
            if not model_config:
                return {"success": False, "error": "Cloud model not configured"}

            print(f"   Provider: {model_config.get('provider', 'unknown')}")
            print(f"   Model: {model_config.get('name', 'unknown')}")
            print(f"   Model ID: {model_config.get('model_id', 'unknown')}")

            api_key_env = model_config.get("api_key_env")
            if not api_key_env:
                return {"success": False, "error": "No api_key_env in model config"}

            api_key = os.getenv(api_key_env)
            if not api_key:
                return {"success": False, "error": f"API key not found: {api_key_env}"}

            print(f"   ✓ API key found")

            if mode == "linear":
                executor = LinearExecutor(model_config)
            else:
                executor = AgenticExecutor(model_config)

            print(f"   Calling API...")
            result = executor.execute(self.test_prompt)

            # Debug output if enabled
            if is_debug_enabled():
                print(f"\n📝 Result: {json.dumps(result, indent=2)}")

            return {
                "success": True,
                "provider": model_config.get("provider", "unknown"),
                "mode": mode,
                "model": model_config.get("name", "unknown"),
                "model_id": model_config.get("model_id", "unknown"),
                "response": result.get("response", ""),
                "tokens": result.get("tokens", {}),
            }

        except Exception as e:
            return {"success": False, "error": str(e)}

    # ========================================================================
    # NEW: Test local models (GGUF files)
    # ========================================================================
    def test_local_model(self, mode: str = "linear") -> dict:
        print(f"\n💻 Testing LOCAL {mode.upper()} model...")

        try:
            model_config = self.config.get_model_config("local", mode)
            if not model_config:
                return {"success": False, "error": "Local model not configured"}

            print(f"   Model: {model_config.get('name', 'unknown')}")
            print(f"   Model path: {model_config.get('model_path', 'unknown')}")

            # Check if model file exists
            model_path = model_config.get("model_path")
            if not model_path:
                return {"success": False, "error": "No model_path in config"}

            if not os.path.exists(model_path):
                return {
                    "success": False,
                    "error": f"Model file not found: {model_path}",
                }

            print(f"   ✓ Model file found ({os.path.getsize(model_path) / 1e9:.2f} GB)")

            if mode == "linear":
                executor = LinearExecutor(model_config)
            else:
                executor = AgenticExecutor(model_config)

            print(f"   Loading model and running inference...")
            result = executor.execute(self.test_prompt)

            # Debug output if enabled
            if is_debug_enabled():
                print(f"\n📝 Result: {json.dumps(result, indent=2)}")

            return {
                "success": True,
                "provider": "local",
                "mode": mode,
                "model": model_config.get("name", "unknown"),
                "model_path": model_config.get("model_path"),
                "response": result.get("response", ""),
                "tokens": result.get("tokens", {}),
            }

        except Exception as e:
            return {"success": False, "error": str(e)}

    def run_all_tests(self, provider="all"):
        results = {}

        if provider in ["cloud", "all"]:
            if self.config.get_model_config("cloud", "linear"):
                results["cloud_linear"] = self.test_cloud_model("linear")
            if self.config.get_model_config("cloud", "agentic"):
                results["cloud_agentic"] = self.test_cloud_model("agentic")

        if provider in ["local", "all"]:
            if self.config.get_model_config("local", "linear"):
                results["local_linear"] = self.test_local_model("linear")
            if self.config.get_model_config("local", "agentic"):
                results["local_agentic"] = self.test_local_model("agentic")

        return results


def print_results(results):
    print("\n" + "=" * 70)
    print("LLM SETUP TEST RESULTS")
    print("=" * 70)

    all_success = True

    for test_name, result in results.items():
        status = "✅" if result.get("success") else "❌"
        print(f"\n{status} {test_name.upper()}")

        if result.get("success"):
            if "provider" in result:
                print(f"   Provider: {result.get('provider', 'N/A')}")
            print(f"   Model: {result.get('model', 'N/A')}")
            if "model_id" in result:
                print(f"   Model ID: {result.get('model_id', 'N/A')}")
            if "model_path" in result:
                print(f"   Model path: {result.get('model_path', 'N/A')}")
            print(f"   Response: {result.get('response', 'N/A')}")
            if "tokens" in result and result["tokens"]:
                print(f"   Tokens: {result['tokens']}")
        else:
            print(f"   Error: {result.get('error', 'Unknown error')}")
            all_success = False

    print("\n" + "=" * 70)
    if all_success:
        print("✅ ALL TESTS PASSED – LLM Setup is ready!")
    else:
        print("⚠️ Some tests failed – Check configuration above")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider", choices=["cloud", "local", "all"], default="all")
    parser.add_argument("--mode", choices=["linear", "agentic", "all"], default="all")
    args = parser.parse_args()

    tester = LLMSetupTester()

    if args.provider != "all" and args.mode != "all":
        # Test specific provider and mode
        if args.provider == "cloud":
            result = tester.test_cloud_model(args.mode)
            print_results({f"cloud_{args.mode}": result})
        elif args.provider == "local":
            result = tester.test_local_model(args.mode)
            print_results({f"local_{args.mode}": result})
    else:
        # Test all requested providers
        results = tester.run_all_tests(args.provider)
        print_results(results)


if __name__ == "__main__":
    main()
