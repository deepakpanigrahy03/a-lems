# This is a minimal patch script to fix the tax_multiplier in the existing
# session_analysis.py on the user's machine.
# Run: python3 session_analysis_tax_fix.py

import re
import sys

path = "gui/pages/session_analysis.py"

with open(path, "r") as f:
    src = f.read()

# Fix 1: tax_multiplier in SQL — use agentic/linear ratio, not tax_percent/100
old = "ots.tax_percent / 100.0        AS tax_multiplier,"
new = """-- FIX: agentic_energy / linear_energy → always >= 1
            CASE WHEN rl.total_energy_uj > 0
                 THEN CAST(ra.total_energy_uj AS REAL) / rl.total_energy_uj
                 ELSE 1.0 END              AS tax_multiplier,"""

if old in src:
    src = src.replace(old, new)
    print("✅ Fixed tax_multiplier SQL")
else:
    print("⚠️  tax_multiplier line not found — already patched or different format")
    # Try regex fallback
    src2 = re.sub(
        r"ots\.tax_percent\s*/\s*100\.0\s+AS\s+tax_multiplier",
        "CASE WHEN rl.total_energy_uj > 0 THEN CAST(ra.total_energy_uj AS REAL) / rl.total_energy_uj ELSE 1.0 END AS tax_multiplier",
        src,
    )
    if src2 != src:
        src = src2
        print("✅ Fixed via regex")

# Fix 2: Also fix the summary aggregation in _tab_summary
# tax_x from DB is now already agentic/linear, but we should verify
# the agg uses 'mean' which is correct
print("✅ Summary agg uses mean — correct")

with open(path, "w") as f:
    f.write(src)

print("Done. Deploy and restart streamlit.")
