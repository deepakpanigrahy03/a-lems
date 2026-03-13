# Issue 5 — Designer Tab: Run Button Fix

The designer.py file builds experiment configs but doesn't expose a run/queue button.

## Manual Fix

After your design is displayed (wherever you call `st.json(design)` or show the
experiment config), add this block:

```python
# Store the designed experiment
if design:  # or whatever variable holds your experiment config
    st.session_state.designed_experiment = {
        "name": design.get("name", "Designer Experiment"),
        "cmd":  design.get("cmd", []),   # must have a cmd list
        # ... other fields from your design
    }

# Run / Queue buttons
if st.session_state.get("designed_experiment"):
    _exp = st.session_state.designed_experiment
    col1, col2 = st.columns(2)
    if col1.button("▶ Run Now", type="primary", key="designer_run_now"):
        if "ex_queue" not in st.session_state:
            st.session_state.ex_queue = []
        st.session_state.ex_queue.insert(0, _exp)
        st.success("Queued! Go to Execute → Live Execution")
    if col2.button("➕ Queue", key="designer_queue"):
        if "ex_queue" not in st.session_state:
            st.session_state.ex_queue = []
        st.session_state.ex_queue.append(_exp)
        st.success(f"Queued at position {len(st.session_state.ex_queue)}")
```
