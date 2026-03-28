"""
alems/shared/uuid_gen.py
────────────────────────────────────────────────────────────────────────────
UUID generation for A-LEMS distributed identity.

Two modes:
  1. Deterministic (backfill) — uuid5 from hw_id + local integer id.
     Same inputs → always same UUID. Safe to rerun backfill multiple times.

  2. Random (new rows) — uuid4. Used for any row created AFTER migration 007.

The stable namespace UUID below is fixed forever — do not change it.
────────────────────────────────────────────────────────────────────────────
"""

import uuid

# Fixed namespace for all A-LEMS UUIDs — generated once, never changes
_NS = uuid.UUID("a1e05000-0000-4000-8000-000000000001")


def run_uuid(hw_id: int, run_id: int) -> str:
    """
    Deterministic UUID for an existing run.
    Used by backfill only — never for new rows.
    """
    return str(uuid.uuid5(_NS, f"run:{hw_id}:{run_id}"))


def exp_uuid(hw_id: int, exp_id: int) -> str:
    """
    Deterministic UUID for an existing experiment.
    Used by backfill only — never for new rows.
    """
    return str(uuid.uuid5(_NS, f"exp:{hw_id}:{exp_id}"))


def new_run_uuid() -> str:
    """Random UUID for a new run (created after migration 007)."""
    return str(uuid.uuid4())


def new_exp_uuid() -> str:
    """Random UUID for a new experiment (created after migration 007)."""
    return str(uuid.uuid4())


def child_uuid(global_run_id: str, table: str, local_id: int) -> str:
    """
    Deterministic UUID for a child row (energy_samples etc.) during backfill.
    Not stored — used only to generate stable sync keys.
    """
    return str(uuid.uuid5(_NS, f"{table}:{global_run_id}:{local_id}"))
