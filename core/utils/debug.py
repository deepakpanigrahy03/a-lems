#!/usr/bin/env python3
"""
================================================================================
DEBUG UTILITY - Unified debugging across all A-LEMS modules
================================================================================

This module provides a consistent debugging interface. Debug output can be
enabled globally or per-module via environment variables.

Usage in your code:
    from core.utils.debug import dprint, set_debug

    dprint("Reading MSR...", msr=0x60d, value=12345)  # Only prints if debug on

Environment variables:
    export A_LEMS_DEBUG=1           # Enable ALL debug output
    export A_LEMS_DEBUG_MODULES=msr_reader,rapl_reader  # Specific modules only
    export A_LEMS_DEBUG_FILE=/tmp/a-lems-debug.log  # Log to file (optional)

Author: Deepak Panigrahy
================================================================================
"""

import inspect
import os
import sys
from datetime import datetime
from typing import Any, Dict, Optional, Set

# ============================================================================
# GLOBAL STATE
# ============================================================================

_DEBUG_ENABLED: bool = False
_DEBUG_MODULES: Set[str] = set()
_DEBUG_FILE: Optional[str] = None
_DEBUG_COLORS: bool = True


# ANSI color codes for pretty output
class Colors:
    BLUE = "\033[94m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    MAGENTA = "\033[95m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RESET = "\033[0m"


# ============================================================================
# INITIALIZATION
# ============================================================================


def init_debug_from_env() -> None:
    """
    Initialize debug settings from environment variables.
    Call this once at program start.
    """
    global _DEBUG_ENABLED, _DEBUG_MODULES, _DEBUG_FILE, _DEBUG_COLORS

    # Check global debug flag
    debug_env = os.getenv("A_LEMS_DEBUG", "").lower()
    if debug_env in ["1", "true", "yes", "on"]:
        _DEBUG_ENABLED = True
        _DEBUG_MODULES = set()  # Empty set means ALL modules

    # Check module-specific debug
    modules_env = os.getenv("A_LEMS_DEBUG_MODULES", "")
    if modules_env:
        _DEBUG_MODULES = set(m.strip() for m in modules_env.split(","))
        _DEBUG_ENABLED = True  # Enable debug for these modules

    # Check debug file
    debug_file = os.getenv("A_LEMS_DEBUG_FILE", "")
    if debug_file:
        _DEBUG_FILE = debug_file

    # Check if we should disable colors (for log files)
    if os.getenv("A_LEMS_DEBUG_NOCOLOR") in ["1", "true"]:
        _DEBUG_COLORS = False

    if _DEBUG_ENABLED:
        dprint_raw(f"🐛 Debug initialized - Modules: {_DEBUG_MODULES or 'ALL'}")


# ============================================================================
# CORE FUNCTIONS
# ============================================================================


def set_debug(enabled: bool = True, module: Optional[str] = None) -> None:
    """
    Enable/disable debug for current module programmatically.

    Args:
        enabled: True to enable, False to disable
        module: Module name (auto-detected if None)
    """
    global _DEBUG_ENABLED, _DEBUG_MODULES

    if module is None:
        # Auto-detect calling module
        frame = inspect.currentframe().f_back
        module = frame.f_globals.get("__name__", "unknown").split(".")[-1]

    if enabled:
        _DEBUG_MODULES.add(module)
        _DEBUG_ENABLED = True
    else:
        _DEBUG_MODULES.discard(module)

    dprint_raw(f"🐛 Debug {'enabled' if enabled else 'disabled'} for {module}")


def is_debug_enabled(module_name: Optional[str] = None) -> bool:
    """
    Check if debug is enabled for given module.

    Args:
        module_name: Module name (auto-detected if None)

    Returns:
        True if debug should be printed
    """
    if not _DEBUG_ENABLED:
        return False

    if not _DEBUG_MODULES:
        # Empty set means ALL modules
        return True

    if module_name is None:
        # Auto-detect calling module
        frame = inspect.currentframe().f_back
        module_name = frame.f_globals.get("__name__", "unknown").split(".")[-1]

    return module_name in _DEBUG_MODULES


def dprint(*args, **kwargs) -> None:
    """
    Debug print – only prints if debug is enabled for current module.

    Usage:
        dprint("Reading MSR...")  # Simple message
        dprint("Value:", 12345)   # Multiple args
        dprint("Counters:", c2=100, c3=200)  # Keyword args become key=value

    Examples:
        dprint("MSR read failed", msr=0x60d, error="Timeout")
        dprint("C-state counters:", **counters)
    """
    # Auto-detect calling module
    frame = inspect.currentframe().f_back
    module = frame.f_globals.get("__name__", "unknown").split(".")[-1]

    if not is_debug_enabled(module):
        return

    dprint_raw(module, *args, **kwargs)


def dprint_raw(module: str, *args, **kwargs) -> None:
    """
    Raw debug print – bypasses module check. Use for internal debug messages.
    """
    # Format timestamp
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]

    # Build message
    parts = []

    # Add timestamp and module with color
    if _DEBUG_COLORS:
        parts.append(f"{Colors.DIM}{timestamp}{Colors.RESET}")
        parts.append(f"{Colors.CYAN}[{module}]{Colors.RESET}")
    else:
        parts.append(f"{timestamp} [{module}]")

    # Add regular args
    for arg in args:
        parts.append(str(arg))

    # Add kwargs as key=value
    if kwargs:
        kv_pairs = []
        for k, v in kwargs.items():
            if _DEBUG_COLORS:
                kv_pairs.append(
                    f"{Colors.YELLOW}{k}{Colors.RESET}={Colors.GREEN}{v}{Colors.RESET}"
                )
            else:
                kv_pairs.append(f"{k}={v}")
        parts.append(" ".join(kv_pairs))

    message = " ".join(parts)

    # Print to stderr (so it doesn't interfere with normal output)
    print(message, file=sys.stderr)

    # Also write to debug file if specified
    if _DEBUG_FILE:
        try:
            with open(_DEBUG_FILE, "a") as f:
                # Strip colors for log file
                plain_message = message
                for color in [
                    Colors.BLUE,
                    Colors.GREEN,
                    Colors.YELLOW,
                    Colors.RED,
                    Colors.MAGENTA,
                    Colors.CYAN,
                    Colors.BOLD,
                    Colors.DIM,
                    Colors.RESET,
                ]:
                    plain_message = plain_message.replace(color, "")
                f.write(plain_message + "\n")
        except:
            pass


# ============================================================================
# CONVENIENCE DECORATOR
# ============================================================================


def trace(func):
    """
    Decorator to trace function calls with debug output.

    Usage:
        @trace
        def my_function(x, y):
            return x + y
    """

    def wrapper(*args, **kwargs):
        module = func.__module__.split(".")[-1]
        if is_debug_enabled(module):
            dprint(f"→ {func.__name__}(", args=args, kwargs=kwargs)
            result = func(*args, **kwargs)
            dprint(f"← {func.__name__} =", result)
            return result
        return func(*args, **kwargs)

    return wrapper


# ============================================================================
# AUTO-INIT
# ============================================================================

# Initialize when module is imported
init_debug_from_env()
