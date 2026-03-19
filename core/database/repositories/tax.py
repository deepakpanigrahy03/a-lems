#!/usr/bin/env python3
"""
================================================================================
TAX REPOSITORY – Handles orchestration tax computation and storage
================================================================================

PURPOSE:
    Contains logic for computing and storing orchestration tax summaries.
    Pairs linear and agentic runs and calculates tax = agentic - linear.

WHY THIS EXISTS:
    - Separates tax logic from other database operations
    - Tax is a research concept that may evolve
    - Part of splitting the god object manager.py

AUTHOR: Deepak Panigrahy
================================================================================
"""

from typing import Any, Dict

from ..base import DatabaseInterface


class TaxRepository:
    """
    Repository for orchestration tax operations.

    Handles pairing of linear/agentic runs and tax computation.
    """

    def __init__(self, db: DatabaseInterface):
        """
        Initialize with database adapter.

        Args:
            db: DatabaseInterface instance
        """
        self.db = db

    def create_tax_summaries(self, exp_id: int) -> None:
        """
        Create tax summary entries for all agentic runs in an experiment.

        This method:
        1. Finds all runs in the experiment
        2. Pairs linear and agentic runs by run_number
        3. Computes tax = agentic_dynamic_uj - linear_dynamic_uj
        4. Stores results in orchestration_tax_summary table

        Args:
            exp_id: Experiment ID
        """
        # Get all runs with their run_number and workflow_type
        runs_info = self.db.execute(
            """
            SELECT r.run_id, r.run_number, e.workflow_type
            FROM runs r
            JOIN experiments e ON r.exp_id = e.exp_id
            WHERE r.exp_id = ?
        """,
            (exp_id,),
        )

        # Build mapping: run_number -> {linear: run_id, agentic: run_id}
        pairs = {}
        for r in runs_info:
            num = r["run_number"]
            if num not in pairs:
                pairs[num] = {}
            pairs[num][r["workflow_type"]] = r["run_id"]

        # For each pair that has both linear and agentic, compute tax
        # No transaction wrapper - caller manages transactions
        for num, pair in pairs.items():
            linear_id = pair.get("linear")
            agentic_id = pair.get("agentic")
            if linear_id and agentic_id:
                # Get dynamic energies
                energies = self.db.execute(
                    "SELECT run_id, dynamic_energy_uj FROM runs WHERE run_id IN (?, ?)",
                    (linear_id, agentic_id),
                )
                energy_dict = {e["run_id"]: e["dynamic_energy_uj"] for e in energies}

                linear_uj = energy_dict.get(linear_id, 0)
                agentic_uj = energy_dict.get(agentic_id, 0)
                tax_uj = agentic_uj - linear_uj
                tax_percent = (tax_uj / agentic_uj * 100) if agentic_uj > 0 else 0

                self.db.conn.execute(
                    """
                    INSERT INTO orchestration_tax_summary
                    (linear_run_id, agentic_run_id, linear_dynamic_uj, agentic_dynamic_uj,
                     orchestration_tax_uj, tax_percent)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (linear_id, agentic_id, linear_uj, agentic_uj, tax_uj, tax_percent),
                )

    def create_tax_summary_for_pair(
        self,
        linear_id: int,
        agentic_id: int,
        linear_uj: int,
        agentic_uj: int,
        linear_orchestration_uj: int = 0,
        agentic_orchestration_uj: int = 0,
    ) -> None:
        """
        Create tax summary for ONE specific pair.

        Args:
            linear_id: Linear run ID
            agentic_id: Agentic run ID
            linear_uj: Linear dynamic energy in microjoules
            agentic_uj: Agentic dynamic energy in microjoules
            linear_orchestration_uj: Linear orchestration overhead
            agentic_orchestration_uj: Agentic orchestration overhead
        """
        tax_uj = agentic_uj - linear_uj
        tax_percent = (tax_uj / agentic_uj * 100) if agentic_uj > 0 else 0

        self.db.conn.execute(
            """
            INSERT INTO orchestration_tax_summary
            (linear_run_id, agentic_run_id, linear_dynamic_uj, agentic_dynamic_uj,
             orchestration_tax_uj, tax_percent,
             linear_orchestration_uj, agentic_orchestration_uj)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                linear_id,
                agentic_id,
                linear_uj,
                agentic_uj,
                tax_uj,
                tax_percent,
                linear_orchestration_uj,
                agentic_orchestration_uj,
            ),
        )
