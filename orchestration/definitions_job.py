"""The job the sensor targets.

Split into its own module so that `sensors.py` and `definitions.py` can both import it
without a cycle.
"""

from __future__ import annotations

from dagster import define_asset_job

build_everything = define_asset_job(name="build_everything", selection="*")
