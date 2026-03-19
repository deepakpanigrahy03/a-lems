#!/usr/bin/env python3
"""
================================================================================
OPTIMIZER WRAPPER - Runs agentic code with real-time system optimization
================================================================================

This wrapper:
1. Launches the real agentic.py as a subprocess
2. Detects phases via environment variables or signals
3. Applies system optimizations based on YOUR A-LEMS data
4. Passes through all results unchanged

NO CHANGES to agentic.py required!
================================================================================
"""

import glob
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import psutil

# Add project root to path
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from core.execution.agentic import AgenticExecutor


class SystemOptimizer:
    """Real-time system optimizer based on phase detection"""

    def __init__(self, pid):
        self.pid = pid
        self.process = psutil.Process(pid)
        self.current_phase = "unknown"
        self.changes_made = []  # Track all changes

    def _apply_setting(self, path, value):
        """Apply system setting using sudo (will prompt for password once)"""
        try:
            # Try direct write first (if running as root)
            with open(path, "w") as f:
                f.write(value)
            print(f"   ✅ Set {path} to {value}")
            return True
        except PermissionError:
            # Fall back to sudo
            import subprocess

            try:
                # Ensure value is string
                if isinstance(value, bytes):
                    value = value.decode()

                result = subprocess.run(
                    ["sudo", "tee", path], input=value, capture_output=True, text=True
                )
                if result.returncode == 0:
                    print(f"   ✅ Set {path} to {value} (via sudo)")
                    return True
                else:
                    print(f"   ⚠️ Failed to set {path}: {result.stderr}")
                    return False
            except Exception as e:
                print(f"   ⚠️ Sudo failed: {e}")
                return False
        except Exception as e:
            print(f"   ⚠️ Error: {e}")
            return False

    def _read_governor(self):
        """Read current CPU governor"""
        try:
            path = "/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"
            with open(path, "r") as f:
                return f.read().strip()
        except:
            return "unknown"

    def _set_governor(self, governor):
        """Set CPU governor and track change"""
        print(f"🔍 DEBUG - _set_governor called with: {governor}")
        old = self._read_governor()
        print(f"🔍 DEBUG - Current governor: {old}")

        if old != governor:
            print(f"🔍 DEBUG - Governor needs to change from {old} to {governor}")
            path = "/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"
            if self._apply_setting(path, governor):
                self.changes_made.append(
                    {
                        "time": time.time(),
                        "phase": self.current_phase,
                        "setting": "governor",
                        "from": old,
                        "to": governor,
                    }
                )
                print(f"   ⚡ Governor: {old} → {governor}")
            else:
                print(f"🔍 DEBUG - Failed to set governor to {governor}")
        else:
            print(f"🔍 DEBUG - Governor already {governor}, no change needed")

    def _get_cstate_number(self, target_name):
        """Find state number for a given C-state name"""
        try:
            for state_dir in glob.glob("/sys/devices/system/cpu/cpu0/cpuidle/state*"):
                name_path = f"{state_dir}/name"
                if os.path.exists(name_path):
                    with open(name_path, "r") as f:
                        name = f.read().strip()
                        if name == target_name:
                            return state_dir.split("state")[-1]
        except:
            pass
        return None

    def _read_cstate(self, state_num):
        """Check if C-state is enabled by number"""
        try:
            path = f"/sys/devices/system/cpu/cpu0/cpuidle/state{state_num}/disable"
            with open(path, "r") as f:
                return "disabled" if f.read().strip() == "1" else "enabled"
        except:
            return "unknown"

    def _set_cstate(self, state_num, enable=True):
        """Enable/disable C-state by number and track change"""
        old = self._read_cstate(state_num)
        new = "enabled" if enable else "disabled"
        if old != new:
            path = f"/sys/devices/system/cpu/cpu0/cpuidle/state{state_num}/disable"
            value = "0" if enable else "1"
            if self._apply_setting(path, value):
                self.changes_made.append(
                    {
                        "time": time.time(),
                        "phase": self.current_phase,
                        "setting": f"state{state_num}",
                        "from": old,
                        "to": new,
                    }
                )
                print(f"   ⚡ State{state_num}: {old} → {new}")

    def _enable_cstates(self, deep=True):
        """Enable/disable deep C-states (C2_ACPI and C3_ACPI)"""
        # Find which state numbers correspond to deep idle
        c2_num = self._get_cstate_number("C2_ACPI")
        c3_num = self._get_cstate_number("C3_ACPI")

        if deep:
            # Enable deeper states (allow them)
            if c2_num:
                self._set_cstate(c2_num, enable=True)
            if c3_num:
                self._set_cstate(c3_num, enable=True)
        else:
            # Disable deeper states during active phases
            if c2_num:
                self._set_cstate(c2_num, enable=False)
            if c3_num:
                self._set_cstate(c3_num, enable=False)

    def _read_coalescing(self):
        """Read interrupt coalescing setting"""
        try:
            for eth in os.listdir("/sys/class/net/"):
                if eth.startswith("eth") or eth.startswith("enp"):
                    path = f"/sys/class/net/{eth}/device/interrupt_coalescing"
                    if os.path.exists(path):
                        with open(path, "r") as f:
                            return f.read().strip()
            return "unknown"
        except:
            return "unknown"

    def _set_coalescing(self, enable):
        """Set interrupt coalescing and track change"""
        old = self._read_coalescing()
        new = "100" if enable else "0"
        if old != new and old != "unknown":
            for eth in os.listdir("/sys/class/net/"):
                if eth.startswith("eth") or eth.startswith("enp"):
                    path = f"/sys/class/net/{eth}/device/interrupt_coalescing"
                    if os.path.exists(path):
                        if self._apply_setting(path, new):
                            self.changes_made.append(
                                {
                                    "time": time.time(),
                                    "phase": self.current_phase,
                                    "setting": "coalescing",
                                    "from": old,
                                    "to": new,
                                }
                            )
                            print(f"   ⚡ Coalescing: {old} → {new}")
                            break

    def set_phase(self, phase):
        """Called when phase changes - applies optimizations"""
        if phase == self.current_phase:
            return

        print(f"\n⚡ OPTIMIZER: Phase {self.current_phase} → {phase}")
        self.current_phase = phase

        # ====================================================================
        # RULES BASED ON YOUR A-LEMS DATA
        # ====================================================================
        if phase == "planning":
            # Your data: planning takes 12-22s, mostly LLM wait
            print("   📝 Planning: Setting powersave governor, enabling deep C-states")

            self._set_governor("powersave")
            self._enable_cstates(deep=True)
            self._set_coalescing(True)

        elif phase == "llm_wait":
            # Your data: 10-20s network waits
            print("   ⏳ LLM Wait: Setting powersave governor, enabling deep C-states")
            self._set_governor("powersave")
            self._enable_cstates(deep=True)
            self._set_coalescing(True)

        elif phase == "tool_exec":
            # Your data: CPU bursts need performance
            print(f"🔍 DEBUG - ENTERING tool_exec phase")
            self._set_governor("performance")
            self._enable_cstates(deep=False)
            self._set_coalescing(False)

        elif phase == "between_steps":
            # Your data: short idle periods
            # Use powersave if ondemand not available
            available = self._get_available_governors()
            if "ondemand" in available:
                self._set_governor("ondemand")
            else:
                self._set_governor("powersave")  # fallback
            self._enable_cstates(deep=False)
            self._set_coalescing(False)
        elif phase == "synthesis":
            # Similar to planning - LLM wait
            self._set_governor("powersave")
            self._enable_cstates(deep=True)
            self._set_coalescing(True)

    def run(self, stop_event):
        """Monitor process and detect phases until stop_event is set"""
        start_time = time.time()

        while not stop_event.is_set():  # Check stop event instead of process
            now = time.time()
            elapsed = now - start_time

            # Simple phase detection (replace with real detection later)
            if elapsed < 2:
                self.set_phase("planning")
            elif 2 <= elapsed < 5:
                self.set_phase("llm_wait")
            elif 5 <= elapsed < 7:
                print(f"🔍 DEBUG - Should trigger tool_exec at {elapsed:.1f}s")
                self.set_phase("tool_exec")
            elif 7 <= elapsed < 8:
                self.set_phase("between_steps")
            elif 8 <= elapsed < 10:
                self.set_phase("tool_exec")
            elif 10 <= elapsed < 12:
                self.set_phase("synthesis")
            elif elapsed >= 12:
                # After all phases, just stay in last phase
                pass

            time.sleep(0.1)

        # Print summary at end
        self.print_summary()

    def print_summary(self):
        """Print optimizer summary"""
        print("\n" + "=" * 60)
        print("📊 OPTIMIZER SUMMARY")
        print("=" * 60)
        print(f"Total changes made: {len(self.changes_made)}")
        for i, change in enumerate(self.changes_made[-10:]):  # Last 10 changes
            print(
                f"  {i+1}. {change['time']:.1f}s | Phase {change['phase']}: "
                f"{change['setting']} {change['from']} → {change['to']}"
            )
        print("=" * 60)

    def _get_available_governors(self):
        """Get list of available CPU governors"""
        try:
            path = "/sys/devices/system/cpu/cpu0/cpufreq/scaling_available_governors"
            with open(path, "r") as f:
                return f.read().strip().split()
        except:
            return ["powersave", "performance"]  # fallback

    def _set_best_governor(self, preferred):
        """Set the best available governor"""
        available = self._get_available_governors()

        if preferred in available:
            self._set_governor(preferred)
        elif "powersave" in available:
            self._set_governor("powersave")
        elif "conservative" in available:
            self._set_governor("conservative")
        else:
            self._set_governor(available[0])  # first available


class OptimizedExecutorWrapper:
    """
    Wraps ANY executor with real-time system optimization.
    Compatible with both LinearExecutor and AgenticExecutor.
    """

    def __init__(self, executor_or_config, workflow_type="agentic"):
        """
        Args:
            executor_or_config: Either an executor instance OR config dict
            workflow_type: 'linear' or 'agentic' - determines phase detection
        """
        self.workflow_type = workflow_type

        # Handle both cases: config dict or existing executor
        if isinstance(executor_or_config, dict):
            # Create appropriate executor from config
            if workflow_type == "agentic":
                self.original = AgenticExecutor(executor_or_config)
            else:
                from core.execution.linear import LinearExecutor

                self.original = LinearExecutor(executor_or_config)
        elif executor_or_config is None:
            # For standalone testing - create default executor
            if workflow_type == "agentic":
                self.original = AgenticExecutor({})
            else:
                from core.execution.linear import LinearExecutor

                self.original = LinearExecutor({})
        else:
            # Use existing executor
            self.original = executor_or_config

        self.optimizer = None

    def execute_comparison(self, task):
        """For agentic workflow - matches original interface"""
        return self._execute_with_optimizer(task, "execute_comparison")

    def execute(self, task):
        """For linear workflow"""
        return self._execute_with_optimizer(task, "execute")

    def _execute_with_optimizer(self, task, method_name):
        """Common optimization logic"""
        pid = os.getpid()

        import threading

        self.optimizer_stop = threading.Event()
        self.optimizer = SystemOptimizer(pid)

        def run_optimizer():
            self.optimizer.run(self.optimizer_stop)

        optimizer_thread = threading.Thread(target=run_optimizer)
        optimizer_thread.daemon = True
        optimizer_thread.start()

        # Call appropriate method on original executor
        executor_method = getattr(self.original, method_name)
        result = executor_method(task)

        self.optimizer_stop.set()
        optimizer_thread.join(timeout=2)

        return result

    def __getattr__(self, name):
        """Pass through any other attributes to the original executor"""
        return getattr(self.original, name)


# Keep the command-line test section at the bottom (unchanged)
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--task", type=str, default="What is 2+2?")
    parser.add_argument(
        "--type", type=str, default="agentic", choices=["agentic", "linear"]
    )
    args = parser.parse_args()

    if args.type == "agentic":
        wrapper = OptimizedExecutorWrapper(None, "agentic")
        result = wrapper.execute_comparison(args.task)
    else:
        wrapper = OptimizedExecutorWrapper(None, "linear")
        result = wrapper.execute(args.task)
    print(f"Result: {result['response']}")
