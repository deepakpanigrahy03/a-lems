#!/usr/bin/env python3
"""
================================================================================
PERFORMANCE COUNTERS DATA MODEL
================================================================================

This module defines the PerformanceCounters data class used throughout Module 1
to store hardware performance counter readings.

The class provides:
- Type-safe fields for all counter types
- Derived metrics (IPC, cache miss rate)
- Serialization methods

Author: Deepak Panigrahy
================================================================================
"""

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Optional


@dataclass
class PerformanceCounters:
    """
    Hardware performance counter readings from perf_events.

    These counters provide insight into what the CPU was actually doing
    during the measurement interval. They help distinguish between:
    - Actual computation (instructions retired)
    - Memory bottlenecks (cache misses)
    - OS scheduler overhead (context switches)

    Req 1.5: Instruction density (IPC)
    Req 1.6: Cache behavior
    Req 1.10: Page faults
    Req 1.43: Thread migrations
    """

    # ===== Req 1.5: Instruction density =====
    instructions_retired: int = 0
    """Number of instructions retired (actual work done)."""

    cpu_cycles: int = 0
    """Number of CPU cycles elapsed."""

    # ===== Req 1.6: Cache behavior =====
    cache_references: int = 0
    """Number of cache references (LLC accesses)."""

    cache_misses: int = 0
    """Number of cache misses (LLC misses)."""

    # ===== Req 1.10: Memory pressure =====
    major_page_faults: int = 0
    """Major page faults (needed disk I/O)."""

    minor_page_faults: int = 0
    """Minor page faults (resolved in memory)."""

    # ===== Req 1.12, 1.43: Scheduler activity =====
    context_switches_voluntary: int = 0
    """Voluntary context switches (process yielded CPU)."""

    context_switches_involuntary: int = 0
    """Involuntary context switches (scheduler preempted)."""

    thread_migrations: int = 0
    """Thread migrations between CPU cores."""

    # ===== Additional perf data =====
    branches: int = 0
    """Branch instructions executed."""

    branch_misses: int = 0
    """Mispredicted branches."""

    cpu_clock_ms: float = 0.0
    """CPU time consumed in milliseconds."""

    task_clock_ms: float = 0.0
    """Task elapsed time in milliseconds."""

    # ===== Metadata =====
    duration_ms: float = 0.0
    """Duration over which counters were measured."""

    def instructions_per_cycle(self) -> float:
        """
        Calculate Instructions Per Cycle (IPC).

        IPC tells us how efficiently the CPU was running:
        - High IPC (>1) means CPU was doing useful work
        - Low IPC (<0.5) suggests stalls (waiting for memory)

        Req 1.5: Instruction density

        Returns:
            float: Instructions per cycle, or 0.0 if no cycles
        """
        if self.cpu_cycles > 0:
            return self.instructions_retired / self.cpu_cycles
        return 0.0

    def cache_miss_rate(self) -> float:
        """
        Calculate cache miss rate.

        Higher miss rates indicate memory bandwidth pressure,
        which can increase energy consumption significantly.

        Req 1.6: Cache behavior

        Returns:
            float: Miss rate between 0.0 and 1.0
        """
        if self.cache_references > 0:
            return self.cache_misses / self.cache_references
        return 0.0

    def total_context_switches(self) -> int:
        """
        Total context switches (voluntary + involuntary).

        Req 1.12: Kernel context switches

        Returns:
            int: Total number of context switches
        """
        return self.context_switches_voluntary + self.context_switches_involuntary

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for serialization.

        Returns:
            Dict with all fields and derived metrics
        """
        result = asdict(self)
        result["ipc"] = self.instructions_per_cycle()
        result["cache_miss_rate"] = self.cache_miss_rate()
        result["total_context_switches"] = self.total_context_switches()
        return result

    def to_json(self) -> str:
        """
        Convert to JSON string.

        Returns:
            JSON string with pretty formatting
        """
        return json.dumps(self.to_dict(), indent=2)


# ============================================================================
# USAGE EXAMPLE
# ============================================================================

if __name__ == "__main__":
    """
    Example: Creating and using a PerformanceCounters object.
    """
    print("\n🔧 Testing PerformanceCounters...")

    # Create a sample counters object
    perf = PerformanceCounters(
        instructions_retired=1234567,
        cpu_cycles=987654,
        cache_references=50000,
        cache_misses=5000,
        context_switches_voluntary=842,
        context_switches_involuntary=156,
        thread_migrations=42,
        duration_ms=100,
    )

    print(f"✅ IPC: {perf.instructions_per_cycle():.2f}")
    print(f"✅ Cache miss rate: {perf.cache_miss_rate():.2%}")
    print(f"✅ Total context switches: {perf.total_context_switches()}")
    print(f"✅ JSON output: {perf.to_json()[:100]}...")

    print("\n✅ PerformanceCounters working!")
