"""The Dagster code location."""

from __future__ import annotations

from dagster import Definitions, load_assets_from_modules

from orchestration import assets as asset_module
from orchestration import checks as check_module
from orchestration.assets import dbt_resource
from orchestration.definitions_job import build_everything
from orchestration.resources import LakeConfig
from orchestration.sensors import new_source_vintage_sensor

all_assets = load_assets_from_modules([asset_module])
all_checks = [
    check_module.bridge_reconciles,
    check_module.furnace_reconciles,
    check_module.geo_coverage_has_not_regressed,
    check_module.ownership_noise_is_bounded,
    check_module.precision_against_labels,
]

defs = Definitions(
    assets=all_assets,
    asset_checks=all_checks,
    jobs=[build_everything],
    # Defined, not enabled. Starting a directory-watching daemon should be a deliberate
    # act by whoever operates the repo, not a side effect of importing a module.
    sensors=[new_source_vintage_sensor],
    resources={"lake": LakeConfig(), "dbt": dbt_resource()},
)
