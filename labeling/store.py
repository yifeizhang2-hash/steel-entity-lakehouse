"""Append-only storage for human decisions.

[no DataFrame library in the write path] Nothing here imports Polars or `ingest`.

Two properties matter and both are structural rather than conventional:

* **Append only.** A decision is never edited or deleted. Changing your mind writes a
  new line; the old one stays. The file is therefore a record of what was decided *and
  when*, which is what makes a disagreement or a drift in judgement visible at all.
* **The probability shown is stored with the verdict.** Without it there is no way to
  check afterwards whether labellers were anchored by the model's own opinion, and
  a labelling loop that cannot be audited for anchoring is a loop that launders the
  model's mistakes into ground truth.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from lakehouse.paths import REPO_ROOT

DECISIONS_PATH = Path(os.environ.get("STEEL_DECISIONS", REPO_ROOT / "labeling" / "decisions.jsonl"))

VERDICTS = ("match", "no_match", "unsure")

# Labels written by `labeling.synthetic`. Never a real person, and the evaluation
# refuses to present results from these as accuracy without saying so.
SYNTHETIC_ANNOTATOR = "SYNTHETIC"


@dataclass(frozen=True)
class Decision:
    pair_id: str
    unique_id_l: str
    unique_id_r: str
    verdict: str
    annotator: str
    # The model's opinion as it was displayed at decision time. Stored so anchoring can
    # be measured later; never used to decide anything.
    probability_shown: float | None = None
    stratum: str | None = None
    queue_seed: int | None = None
    notes: str = ""
    decided_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    synthetic: bool = False

    def __post_init__(self) -> None:
        if self.verdict not in VERDICTS:
            raise ValueError(f"verdict must be one of {VERDICTS}, got {self.verdict!r}")
        if not self.annotator.strip():
            raise ValueError("every decision must record who made it")

    def as_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, sort_keys=True)


def append(decision: Decision, path: Path | None = None) -> Path:
    """Append one decision. Never rewrites an existing line."""
    target = Path(path or DECISIONS_PATH)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(decision.as_json() + "\n")
    return target


def read_all(path: Path | None = None) -> list[dict[str, Any]]:
    """Every decision ever written, in the order it was written."""
    target = Path(path or DECISIONS_PATH)
    if not target.exists():
        return []
    return list(_iter_records(target))


def _iter_records(target: Path) -> Iterator[dict[str, Any]]:
    with target.open(encoding="utf-8") as handle:
        for number, line in enumerate(handle, start=1):
            text = line.strip()
            if not text:
                continue
            try:
                yield json.loads(text)
            except json.JSONDecodeError as error:
                raise ValueError(f"{target}:{number} is not valid JSON: {error}") from error


def current(path: Path | None = None) -> dict[str, dict[str, Any]]:
    """The latest decision per pair.

    Later lines supersede earlier ones for the purpose of evaluation, but nothing is
    removed from the file: the superseded line is still there to be compared against.
    """
    latest: dict[str, dict[str, Any]] = {}
    for record in read_all(path):
        latest[record["pair_id"]] = record
    return latest


def summary(path: Path | None = None) -> dict[str, Any]:
    """Counts a caller can print without having to reason about supersession."""
    records = read_all(path)
    latest = current(path)
    verdicts: dict[str, int] = {}
    for record in latest.values():
        verdicts[record["verdict"]] = verdicts.get(record["verdict"], 0) + 1
    annotators = sorted({record["annotator"] for record in records})
    return {
        "lines_written": len(records),
        "pairs_decided": len(latest),
        "superseded": len(records) - len(latest),
        "verdicts": verdicts,
        "annotators": annotators,
        "synthetic_only": bool(records) and all(r.get("synthetic") for r in records),
        "has_real_labels": any(not r.get("synthetic") for r in records),
    }
