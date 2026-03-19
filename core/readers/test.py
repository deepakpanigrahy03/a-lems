#!/usr/bin/env python3
"""
Simple CPU-intensive workload to generate perf events.
"""

import math
import time


def busy_work(duration_seconds=1.0):
    """Do CPU-intensive work for specified duration."""
    end_time = time.time() + duration_seconds
    result = 0
    iterations = 0

    while time.time() < end_time:
        # Busy work: calculate primes, factorials, etc.
        for i in range(1000):
            result += math.sqrt(i * math.pi)
            result -= math.sin(result)
        iterations += 1

    return result, iterations


if __name__ == "__main__":
    print("🚀 Starting CPU workload...")
    start = time.time()
    result, iterations = busy_work(2.0)
    elapsed = time.time() - start
    print(f"✅ Completed {iterations} iterations in {elapsed:.2f}s")
    print(f"   Result: {result}")
