"""A sensor that fires when a new source vintage lands.

[no DataFrame library] Nothing here imports Polars or pandas.

WHY THERE IS NO SCHEDULE
------------------------
The obvious thing to add at this point is a daily or weekly cron. It would be theatre.

Every source in this project is an annual file drop: GSPT publishes its tracker once a
year, and the furnace census arrives as a recompiled vintage. Running the pipeline on a
timer would re-read the same four files on 364 days out of 365, produce byte-identical
output, and fill the run history with green ticks that mean nothing. Worse, it would
make a genuinely stale pipeline look healthy -- "it ran this morning" is not "it has
current data", and a schedule is very good at blurring those two.

What actually matters is *a new vintage appearing*. That is an event, so it gets a
sensor: `data/raw/source_version=*` is watched for a directory nobody has processed yet,
and the pipeline runs when one shows up. Between vintages nothing runs, which is the
correct amount of work to do when nothing has changed.

The sensor is defined but **not enabled by default**. Turning it on starts a daemon that
watches a directory, and that should be a deliberate act by whoever operates the
repository rather than a side effect of importing a module.
"""

from __future__ import annotations

import json
from pathlib import Path

from dagster import (
    DefaultSensorStatus,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    sensor,
)

from ingest.config import RAW_ROOT
from orchestration.definitions_job import build_everything

SOURCE_DIRECTORY_PREFIX = "source_version="


def discovered_vintages(raw_root: Path | None = None) -> list[str]:
    """Vintage names present on disk, from the `source_version=` directory layout."""
    root = Path(raw_root or RAW_ROOT)
    if not root.is_dir():
        return []
    return sorted(
        path.name[len(SOURCE_DIRECTORY_PREFIX) :]
        for path in root.iterdir()
        if path.is_dir() and path.name.startswith(SOURCE_DIRECTORY_PREFIX)
    )


@sensor(
    job=build_everything,
    minimum_interval_seconds=3600,
    default_status=DefaultSensorStatus.STOPPED,
    description=(
        "Runs the pipeline when a new source vintage directory appears under "
        "data/raw/. There is deliberately no schedule: the sources are annual file "
        "drops, and a timer would re-read identical files daily and make a stale "
        "pipeline look healthy."
    ),
)
def new_source_vintage_sensor(context: SensorEvaluationContext):
    """Fire once per vintage that has not been seen before.

    The cursor holds the vintages already processed, so a restart does not replay the
    whole history and an unchanged directory does not trigger anything.
    """
    seen = set(json.loads(context.cursor)) if context.cursor else set()
    present = discovered_vintages()
    fresh = [vintage for vintage in present if vintage not in seen]

    if not fresh:
        return SkipReason(
            f"no new source vintage; {len(present)} already processed "
            f"({', '.join(sorted(present)) or 'none'})"
        )

    return SensorResult(
        run_requests=[
            RunRequest(run_key=f"vintage-{vintage}", tags={"source_version": vintage})
            for vintage in fresh
        ],
        cursor=json.dumps(sorted(set(present) | seen)),
    )
