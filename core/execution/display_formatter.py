import logging
import socket
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np
from scipy import stats as scipy_stats

from core.analysis.energy_analyzer import EnergyAnalyzer
from core.config_loader import ConfigLoader
from core.database.manager import DatabaseManager
from core.energy_engine import EnergyEngine
from core.execution.base import calc_stats
from core.sustainability.calculator import SustainabilityCalculator
from core.utils.baseline_manager import BaselineManager
from core.utils.debug import dprint


def display_hardware(runs, label):
    """Display ALL hardware parameters from DerivedEnergyMeasurement (Layer 3)."""
    for idx, run in enumerate(runs):
        print(f"\n📊 {label} Run {idx+1}:")

        # Use layer3_derived – this contains all computed metrics
        derived = run["layer3_derived"]

        # --------------------------------------------------------------------
        # Req 1.1, 1.3: RAPL Energy Domains & Uncore Waste
        # --------------------------------------------------------------------
        energy_uj = derived.get("energy_uj", {})
        print("   ⚡ RAPL Energy (Req 1.1):")
        print(f"      Package: {energy_uj.get('package', 0)/1e6:.3f} J")
        print(f"      Core:    {energy_uj.get('core', 0)/1e6:.3f} J")

        uncore = energy_uj.get("uncore", 0)
        if uncore > 0:
            print(
                f"      Uncore:  {uncore/1e6:.3f} J (includes GPU if no separate GPU domain)"
            )

        dram = energy_uj.get("dram")
        if dram:
            print(f"      DRAM:    {dram/1e6:.3f} J")

        # --------------------------------------------------------------------
        # Req 1.5, 1.6, 1.10, 1.12, 1.43: Performance Counters & Scheduler
        # --------------------------------------------------------------------
        perf = derived.get("performance", {})
        print("   📈 Performance Counters:")
        print(f"      Instructions:      {perf.get('instructions', 0):,}")
        print(f"      Cycles:            {perf.get('cycles', 0):,}")
        print(f"      IPC:               {perf.get('ipc', 0):.2f}")
        print(f"      Cache References:  {perf.get('cache_references', 0):,}")
        print(f"      Cache Misses:      {perf.get('cache_misses', 0):,}")
        cache_refs = perf.get("cache_references", 1)
        miss_rate = (perf.get("cache_misses", 0) / cache_refs) if cache_refs > 0 else 0
        print(f"      Cache Miss Rate:   {miss_rate:.2%}")
        print(
            f"      Page Faults:       {perf.get('page_faults', 0):,} "
            f"(major: {perf.get('major_page_faults', 0)}, "
            f"minor: {perf.get('minor_page_faults', 0)})"
        )

        scheduler = derived.get("scheduler", {})
        # print(f"      Voluntary Ctx Sw:  {scheduler.get('context_switches_voluntary', 0):,}")
        # print(f"      Involuntary Ctx Sw:{scheduler.get('context_switches_involuntary', 0):,}")
        # print(f"      Thread Migrations: {scheduler.get('thread_migrations', 0):,}")

        sched = derived.get("scheduler", {})
        print(f"🔍 DEBUG scheduler keys: {list(sched.keys())}")
        power = derived.get("power_states", {})
        print(f"🔍 DEBUG power_states keys: {list(power.keys())}")
        print(f"🔍 DEBUG frequency_mhz: {power.get('frequency_mhz', 'MISSING')}")

        # --------------------------------------------------------------------
        # Req 1.9: Thermal + Derived Thermal Metrics
        # --------------------------------------------------------------------
        thermal = derived.get("thermal", {})
        print("   🌡️ Thermal (Req 1.9):")

        pkg_temp = thermal.get("package_temp_celsius")

        if pkg_temp and pkg_temp > -100:
            print(f"      Package Temp: {pkg_temp:.1f}°C")
        else:
            print(f"      Package Temp: N/A")

        core_temps = thermal.get("core_temps_celsius", [])
        valid_temps = [t for t in core_temps if t > 10]
        if valid_temps:
            temps = ", ".join([f"{t:.1f}°C" for t in valid_temps])
            print(f"      Core Temps:   [{temps}]")
        # ====================================================================
        # NEW: Display Heat Flux if available
        # ====================================================================
        if "ml_features" in run:
            heat_flux = run["ml_features"].get("heat_flux")
            if heat_flux is not None:
                # Color code based on severity
                if heat_flux > 3.0:
                    flux_indicator = "🔴 HIGH"
                elif heat_flux > 1.5:
                    flux_indicator = "🟡 MODERATE"
                else:
                    flux_indicator = "🟢 LOW"

                print(f"      Heat Flux: {heat_flux:.2f}°C/s {flux_indicator}")

                # Add interpretation
                if heat_flux > 3.0:
                    print(f"         ⚠️  Rapid heating - thermal event risk")
                elif heat_flux < 0:
                    print(f"         ❄️  System cooling down")
        # ====================================================================
        # NEW: Display derived thermal metrics from the 'thermal' section
        # ====================================================================
        # Check if we have the new thermal section in derived
        exp_start = derived.get("exp_start_time", "N/A")
        exp_end = derived.get("exp_end_time", "N/A")
        during = derived.get("thermal_during_experiment", 0)
        now_active = derived.get("thermal_now_active", 0)
        since_boot = derived.get("thermal_since_boot", 0)
        # Experiment validity: No throttling during experiment AND not throttling now
        experiment_valid = during == 0 and now_active == 0

        print(f"\n   📋 Experiment Timeline:")
        print(f"      Start: {exp_start}")
        print(f"      End:   {exp_end}")
        print(f"\n   ✅ Thermal Summary:")
        print(f"      Since Boot: {'YES' if since_boot else 'NO'}")
        print(f"      During Experiment: {'YES' if during else 'NO'}")
        print(f"      Active at End: {'YES' if now_active else 'NO'}")
        print(f"\n   🔬 Experiment Valid: {'YES' if experiment_valid else 'NO'}")
        # ====================================================================
        # NEW: System State Metrics (M3-1 through M3-6)
        # ====================================================================
        if "ml_features" in run:
            ml = run["ml_features"]
            print("\n   ⚙️ System State:")

            # M3-1: Governor/Turbo
            governor = ml.get("governor", "unknown")
            turbo = ml.get("turbo_enabled", 0)
            turbo_status = "ENABLED" if turbo else "DISABLED"
            print(f"      CPU Governor: {governor}")
            print(f"      Turbo Boost: {turbo_status}")

            # M3-2: Interrupt Rate
            intr_rate = ml.get("interrupt_rate", 0)
            baseline_intr = ml.get("baseline_interrupt_rate", 2000)
            if baseline_intr > 0:
                ratio = intr_rate / baseline_intr
            else:
                ratio = 1.0

            if ratio > 2.0:
                intr_indicator = "🔴 HIGH"
            elif ratio > 1.2:
                intr_indicator = "🟡 MODERATE"
            else:
                intr_indicator = "🟢 LOW"
            print(f"      Interrupt Rate: {intr_rate:.0f}/sec {intr_indicator}")

            # M3-3: Temperature Tracking
            start_temp = ml.get("start_temp_c", 0)
            max_temp = ml.get("max_temp_c", 0)
            if start_temp > 0 and max_temp > 0:
                temp_rise = max_temp - start_temp
                print(
                    f"      Temperature: {start_temp:.1f}°C → {max_temp:.1f}°C (Δ{temp_rise:+.1f}°C)"
                )

            # M3-4: Cold Start Flag
            cold_start = ml.get("is_cold_start", 0)
            if cold_start:
                print(f"      Cold Start: YES (first run)")

            # M3-5: Background Noise
            bg_cpu = ml.get("background_cpu_percent", 0)
            proc_count = ml.get("process_count", 0)
            if bg_cpu > 0 or proc_count > 0:
                print(f"      Background CPU: {bg_cpu:.1f}%")
                print(f"      Running Processes: {proc_count}")

            # M3-6: Memory Metrics
            rss = ml.get("rss_memory_mb", 0)
            vms = ml.get("vms_memory_mb", 0)
            if rss > 0 or vms > 0:
                print(f"      Process Memory:")
                print(f"         RSS: {rss:.1f} MB")
                print(f"         VMS: {vms:.1f} MB")

            # Heat Flux (already in your code)
            heat_flux = ml.get("heat_flux")
            if heat_flux is not None:
                if heat_flux > 3.0:
                    flux_indicator = "🔴 HIGH"
                elif heat_flux > 1.5:
                    flux_indicator = "🟡 MODERATE"
                else:
                    flux_indicator = "🟢 LOW"
                print(f"      Heat Flux: {heat_flux:.2f}°C/s {flux_indicator}")

        # --------------------------------------------------------------------
        # Req 1.7, 1.41, 1.8, 1.4: Power States (C-states, frequencies, GPU)
        # --------------------------------------------------------------------
        power = derived.get("power_states", {})
        print("   💤 Power States (Req 1.7, 1.41, 1.8, 1.4):")
        cstates = power.get("c_state_residencies", {})
        if cstates:
            # Show only C‑states with positive residency
            states = [f"{k}: {v:.1f}%" for k, v in cstates.items() if v > 0]
            if states:
                print(f"      C-state residency (per-core avg): {', '.join(states)}")
                print(
                    f"      (Note: Values can sum to >100% as each core reports independently)"
                )
        print(f"      CPU Frequency:  {power.get('frequency_mhz', 0):.0f} MHz")
        gpu_freq = power.get("gpu_frequency_mhz", 0)
        if gpu_freq > 0:
            print(f"      GPU Frequency:  {gpu_freq:.0f} MHz")
        gpu_rc6 = power.get("gpu_rc6_percent", 0)
        if gpu_rc6 > 0:
            print(f"      GPU RC6:        {gpu_rc6:.1f}%")

        # --------------------------------------------------------------------
        # Req 1.23, 1.36: Scheduler Metrics (additional)
        # --------------------------------------------------------------------
        scheduler = derived.get("scheduler", {})
        print("   🔄 Scheduler Metrics:")
        print(
            f"      Voluntary Ctx Sw:  {scheduler.get('context_switches_voluntary', 0):,}"
        )
        print(
            f"      Involuntary Ctx Sw: {scheduler.get('context_switches_involuntary', 0):,}"
        )
        print(f"      Thread Migrations: {scheduler.get('thread_migrations', 0):,}")
        print(f"      Run Queue Length: {scheduler.get('run_queue_length', 0):.2f}")
        print(f"      Kernel Time:      {scheduler.get('kernel_time_ms', 0):.2f} ms")
        print(f"      User Time:        {scheduler.get('user_time_ms', 0):.2f} ms")
        # ====================================================================
        # DEBUG: See what's in derived
        # ====================================================================
        # print("\n   🔍 DEBUG: derived keys =", list(derived.keys()))
        if "msr" in derived:
            # print("   🔍 DEBUG: msr keys =", list(derived['msr'].keys()))
            if derived["msr"] and isinstance(derived["msr"], dict):
                msr_preview = str(derived["msr"])[:200]
                print(f"   🔍 DEBUG: msr content = {msr_preview}...")

        # ====================================================================
        # DEBUG: Print raw msr_data structure
        # ====================================================================
        msr_data = derived.get("msr", {})
        # if msr_data:
        # dprint("\n 🔍 DEBUG: msr_data top-level keys =", list(msr_data.keys()))
        # dprint("   🔍 DEBUG: wakeup_latency_us =", msr_data.get('wakeup_latency_us'))
        # dprint("   🔍 DEBUG: thermal_throttle =", msr_data.get('thermal_throttle'))
        # dprint("   🔍 DEBUG: baseline keys =", list(msr_data.get('baseline', {}).keys()))
        # dprint("   🔍 DEBUG: dynamic keys =", list(msr_data.get('dynamic', {}).keys()))
        # ====================================================================
        # MSR Metrics Display (Fixed for actual structure)
        # ====================================================================
        msr_data = derived.get("msr", {})
        if msr_data:
            print("\n   🔧 MSR Metrics:")

            # Ring bus frequency
            ring_bus = msr_data.get("ring_bus", {})
            if ring_bus and ring_bus.get("current_mhz"):
                print(f"      Ring Bus Frequency: {ring_bus['current_mhz']:.1f} MHz")

            # Wake-up latency - directly from top level
            wake_lat = msr_data.get("wakeup_latency_us")
            if wake_lat:
                print(f"      Wake-up Latency: {wake_lat:.2f} µs")

            # Thermal throttle - directly from top level
            throttle = msr_data.get("thermal_throttle")
            if throttle is not None:
                status = "DETECTED" if throttle else "NOT DETECTED"
                print(f"      Thermal Throttle Flag: {throttle} ({status})")

            # C-state times - from c_states
            c_states = msr_data.get("c_states", {})
            if c_states:
                print("      C-State Times (since boot):")
                for state, state_data in c_states.items():
                    seconds = state_data.get("seconds", 0)
                    if seconds > 0:
                        if seconds < 60:
                            time_str = f"{seconds:.2f} seconds"
                        elif seconds < 3600:
                            time_str = f"{seconds/60:.2f} minutes"
                        else:
                            time_str = f"{seconds/3600:.2f} hours"
                        print(f"         {state.upper()}: {time_str}")

            # TSC frequency for reference
            tsc_freq = msr_data.get("tsc_frequency_hz", 0)
            if tsc_freq:
                print(f"      TSC Frequency: {tsc_freq/1e6:.0f} MHz")


def display_ipc_analysis(all_linear, all_agentic):
    """
    Display IPC efficiency analysis comparing linear and agentic runs.

    Args:
        all_linear: List of linear run results
        all_agentic: List of agentic run results
    """

    # ====================================================================
    # IPC Efficiency Analysis
    # ====================================================================
    if len(all_linear) > 0 and len(all_agentic) > 0:
        # Get average IPC from each run
        linear_ipcs = []
        agentic_ipcs = []

        for run in all_linear:
            perf = run["layer3_derived"].get("performance", {})
            ipc = perf.get("ipc", 0)
            if ipc > 0:
                linear_ipcs.append(ipc)

        for run in all_agentic:
            perf = run["layer3_derived"].get("performance", {})
            ipc = perf.get("ipc", 0)
            if ipc > 0:
                agentic_ipcs.append(ipc)

        if linear_ipcs and agentic_ipcs:
            avg_linear_ipc = sum(linear_ipcs) / len(linear_ipcs)
            avg_agentic_ipc = sum(agentic_ipcs) / len(agentic_ipcs)
            ipc_ratio = avg_agentic_ipc / avg_linear_ipc if avg_linear_ipc > 0 else 0

            print(f"\n{'='*70}")
            print("📊 IPC EFFICIENCY ANALYSIS")
            print("=" * 70)
            print(f"   Linear IPC:  {avg_linear_ipc:.2f}")
            print(f"   Agentic IPC: {avg_agentic_ipc:.2f}")
            print(f"   Efficiency Ratio: {ipc_ratio:.2f}x")

            if ipc_ratio > 1:
                print(
                    f"   → Agentic workflow keeps CPU {((ipc_ratio-1)*100):.1f}% busier"
                )
                print(f"   → Higher instruction density during orchestration")
            elif ipc_ratio < 1:
                print(
                    f"   → Agentic workflow is {((1-ipc_ratio)*100):.1f}% less CPU efficient"
                )
                print(f"   → More pipeline stalls or cache misses")
            else:
                print(f"   → No IPC difference between workflows")

            print("=" * 70)


def display_sustainability_header(all_linear):
    """Display grid information and sources."""
    print("\n" + "=" * 70)
    print("🌍 SUSTAINABILITY IMPACT")
    print("=" * 70)
    if all_linear and all_linear[0].get("sustainability"):
        sus = all_linear[0]["sustainability"]
        country = all_linear[0].get("country_code", "US")
        print(f"\n   📍 Grid Region: {country}")
        print(f"   " + "-" * 50)

        if sus and "carbon" in sus:
            c = sus["carbon"]
            print(f"   Carbon Intensity: {c.get('source', 'Unknown')}")
            print(f"      Factor: {c.get('grams_per_kwh', 0):.1f} g/kWh")
            print(f"      Uncertainty: ±{c.get('uncertainty_percent', 0)}% [Req 2.16]")

        if sus and "water" in sus:
            w = sus["water"]
            print(f"\n   Water Intensity: {w.get('source', 'Unknown')}")

        if sus and "methane" in sus:
            m = sus["methane"]
            print(f"\n   Methane Leakage: {m.get('source', 'Unknown')}")


def display_workflow_comparison(linear_stats, agentic_stats):
    """Display workflow comparison table."""
    if linear_stats and agentic_stats:
        print(f"\n   📊 WORKFLOW COMPARISON")
        print(f"   " + "=" * 50)
        print(f"   {'Metric':<20} {'LINEAR':>15} {'AGENTIC':>15} {'RATIO':>10}")
        print(f"   " + "-" * 60)

        # Energy
        energy_ratio = (
            (agentic_stats["energy"] / linear_stats["energy"])
            if linear_stats["energy"]
            else float("nan")
        )
        print(
            f"   {'Energy (J)':<20} {linear_stats['energy']:>15.6f} {agentic_stats['energy']:>15.6f} "
            f"{energy_ratio:>10.2f}x"
        )

        # Carbon
        carbon_ratio = (
            (agentic_stats["carbon"] / linear_stats["carbon"])
            if linear_stats["carbon"]
            else float("nan")
        )
        print(
            f"   {'Carbon (mg)':<20} {linear_stats['carbon']*1000:>15.6f} {agentic_stats['carbon']*1000:>15.6f} "
            f"{carbon_ratio:>10.2f}x"
        )

        # Water
        water_ratio = (
            (agentic_stats["water"] / linear_stats["water"])
            if linear_stats["water"]
            else float("nan")
        )
        print(
            f"   {'Water (µl)':<20} {linear_stats['water']*1000:>15.6f} {agentic_stats['water']*1000:>15.6f} "
            f"{water_ratio:>10.2f}x"
        )

        # Methane
        methane_ratio = (
            (agentic_stats["methane"] / linear_stats["methane"])
            if linear_stats["methane"]
            else float("nan")
        )
        print(
            f"   {'Methane (mg)':<20} {linear_stats['methane']*1000:>15.6f} {agentic_stats['methane']*1000:>15.6f} "
            f"{methane_ratio:>10.2f}x"
        )

        print(f"   " + "-" * 60)


def display_pair_hardware(linear_results, agentic_results, title=None):
    """Display hardware parameters for each pair."""
    if title:
        print("\n" + "=" * 70)
        print(f"🔧 {title}")
        print("=" * 70)

    for i in range(len(linear_results)):
        print(f"\n{'─'*50}")
        print(f"📊 PAIR {i+1}: Linear + Agentic")
        print(f"{'─'*50}")
        display_hardware([linear_results[i]], f"LINEAR Run {i+1}")
        if i < len(agentic_results):
            display_hardware([agentic_results[i]], f"AGENTIC Run {i+1}")

    display_ipc_analysis(linear_results, agentic_results)
