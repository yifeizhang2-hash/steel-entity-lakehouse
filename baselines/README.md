# Baselines

Reference values the Dagster drift checks compare each run against.

These are **tracked on purpose**. `lake/` is gitignored build output that `make clean`
deletes; a baseline stored there would vanish with one clean, and the next run would
silently re-establish it from whatever the pipeline happened to produce — including a
degraded one — and report success. A reference that the thing it measures can erase is
not a reference.

`check_baseline.json` is updated by `orchestration/checks.py` when a run improves on the
recorded value, so a change to it should appear in a diff and be looked at.
