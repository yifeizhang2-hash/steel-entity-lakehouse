"""Evaluate the linker against human labels, and retrain its m values from them.

[pandas] Nothing here imports Polars or `ingest`.

WHAT THIS CAN AND CANNOT SAY
----------------------------
Precision is computable from labels drawn anywhere. **Recall is not.** It needs labels
from the low-probability end too, which is why the queue samples there and why a caller
that only labelled the top of the ranking gets a recall of "not computable" rather than
a flattering number.

Everything here is reported *within the labelled sample*. The strata are not equally
sampled -- three are exhaustive, two are random draws -- so these are not estimates of
population precision and recall, and the report says so on every run.

If the labels are synthetic, every metric is printed with that stated. A synthetic
number presented as accuracy would be worse than no number.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from labeling import store
from labeling.sampling import DEFAULT_SEED, build_queue
from resolution.model import TrainedModel, train
from resolution.report import ComparatorReport, comparator_report

THRESHOLDS = (0.01, 0.1, 0.5, 0.9, 0.95, 0.99, 0.999)


@dataclass
class LabelledSet:
    frame: pd.DataFrame
    synthetic: bool
    annotators: list[str]

    @property
    def decisive(self) -> pd.DataFrame:
        """Labels with an actual verdict. `unsure` is excluded from every metric.

        Counting "unsure" as either class would invent a judgement the labeller
        explicitly declined to make.
        """
        return self.frame[self.frame["verdict"].isin(("match", "no_match"))]

    @property
    def positives(self) -> int:
        return int((self.decisive["verdict"] == "match").sum())


def load_labels(
    predictions: pd.DataFrame, path: Path | None = None, queue: pd.DataFrame | None = None
) -> LabelledSet:
    """Join the latest decision per pair onto the model's predictions."""
    latest = store.current(path)
    if not latest:
        return LabelledSet(frame=pd.DataFrame(), synthetic=False, annotators=[])

    labels = pd.DataFrame(list(latest.values()))
    scored = predictions.copy()
    scored["pair_id"] = [
        f"{left}|{right}"
        for left, right in zip(scored["unique_id_l"], scored["unique_id_r"], strict=True)
    ]
    merged = labels.merge(
        scored.drop(columns=[c for c in ("stratum",) if c in scored.columns]),
        on="pair_id",
        how="inner",
        suffixes=("", "_pred"),
    )
    if queue is not None and "stratum" in queue.columns:
        merged = merged.drop(columns=["stratum"], errors="ignore").merge(
            queue[["pair_id", "stratum"]], on="pair_id", how="left"
        )
    return LabelledSet(
        frame=merged,
        synthetic=bool(labels["synthetic"].all()) if "synthetic" in labels else False,
        annotators=sorted(labels["annotator"].unique().tolist()),
    )


def confusion_at(labelled: pd.DataFrame, threshold: float) -> dict[str, Any]:
    """Confusion counts treating `match_probability >= threshold` as the prediction."""
    predicted = labelled["match_probability"] >= threshold
    actual = labelled["verdict"] == "match"
    tp = int((predicted & actual).sum())
    fp = int((predicted & ~actual).sum())
    fn = int((~predicted & actual).sum())
    tn = int((~predicted & ~actual).sum())
    precision = tp / (tp + fp) if (tp + fp) else None
    recall = tp / (tp + fn) if (tp + fn) else None
    f1 = (
        2 * precision * recall / (precision + recall)
        if precision and recall and (precision + recall)
        else None
    )
    return {
        "threshold": threshold,
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


def pr_curve(labelled: pd.DataFrame, thresholds: tuple[float, ...] = THRESHOLDS) -> pd.DataFrame:
    return pd.DataFrame([confusion_at(labelled, t) for t in thresholds])


def stratified_confusion(labelled: pd.DataFrame, threshold: float = 0.99) -> pd.DataFrame:
    """Confusion counts per stratum. **Never pool these.**

    The strata are sampled at completely different rates -- three exhaustively, two as
    30-pair random draws -- so a single pooled precision is an average over a sample
    whose composition was chosen by the sampler, not by the world. It would also hide
    the finding that matters most: the model scores well and assigns badly, and those
    two failures live in different strata.
    """
    rows = []
    for stratum, group in labelled.groupby("stratum"):
        counts = confusion_at(group, threshold)
        counts["stratum"] = stratum
        counts["labelled"] = int(len(group))
        rows.append(counts)
    frame = pd.DataFrame(rows)
    columns = ["stratum", "labelled", "tp", "fp", "fn", "tn", "precision", "recall", "f1"]
    return frame[columns].sort_values("labelled", ascending=False).reset_index(drop=True)


def contested_split(labelled: pd.DataFrame, threshold: float = 0.99) -> pd.DataFrame:
    """Precision where one record has several confident claims, against everywhere else.

    This is the split that explains the model. `contested` is *defined* as the model
    giving one plant several candidates above the confidence bar, so at most one can be
    right -- the failure there is not scoring, it is choosing. Separating it shows a
    scoring model that works next to an assignment problem that is unsolved.
    """
    tagged = labelled.assign(
        group=labelled["stratum"].map(
            lambda s: "contested" if s == "contested" else "not contested"
        )
    )
    rows = []
    for group, frame in tagged.groupby("group"):
        counts = confusion_at(frame, threshold)
        counts["group"] = group
        counts["predicted_positive"] = counts["tp"] + counts["fp"]
        rows.append(counts)
    return pd.DataFrame(rows)[
        ["group", "labelled" if False else "predicted_positive", "tp", "fp", "precision"]
    ]


def by_stratum(labelled: pd.DataFrame) -> pd.DataFrame:
    """Where the labels came from and what they said.

    Reported per stratum because the strata are not sampled at the same rate; a single
    pooled number would mix an exhaustive census with a 30-pair random draw.
    """
    if "stratum" not in labelled.columns:
        return pd.DataFrame()
    return (
        labelled.assign(is_match=(labelled["verdict"] == "match").astype(int))
        .groupby("stratum")
        .agg(
            labelled=("verdict", "size"),
            matches=("is_match", "sum"),
            median_probability=("match_probability", "median"),
        )
        .reset_index()
    )


def anchoring_check(labelled: pd.DataFrame) -> dict[str, Any]:
    """Did labellers see the model's opinion, and did it move them?

    This cannot prove independence. What it can do is say whether the question is even
    answerable -- if `probability_shown` is null everywhere, the model's opinion was
    hidden, and that is the strongest evidence available that the labels are its own.
    """
    if "probability_shown" not in labelled.columns or labelled.empty:
        return {"labels": 0, "model_opinion_shown": 0, "agreement_when_shown": None}
    shown = labelled[labelled["probability_shown"].notna()]
    hidden = labelled[labelled["probability_shown"].isna()]

    def agreement(frame: pd.DataFrame) -> float | None:
        decisive = frame[frame["verdict"].isin(("match", "no_match"))]
        if decisive.empty:
            return None
        model = decisive["match_probability"] >= 0.5
        human = decisive["verdict"] == "match"
        return float((model == human).mean())

    return {
        "labels": int(len(labelled)),
        "model_opinion_shown": int(len(shown)),
        "model_opinion_hidden": int(len(hidden)),
        "agreement_when_shown": agreement(shown),
        "agreement_when_hidden": agreement(hidden),
    }


def retrain_from_labels(
    model: TrainedModel, labelled: pd.DataFrame
) -> tuple[ComparatorReport, str | None]:
    """Re-estimate m from the labels and return the resulting comparator report.

    This is the only mechanism in the project that can correct the known conditional-
    independence violation between ZIP, street and county. EM cannot see that those
    three agree together for a common reason; labelled pairs can.

    Two properties of `estimate_m_from_pairwise_labels` matter and are easy to get
    wrong:

    * **It ignores `clerical_match_score` entirely** and treats *every row of the table
      as a confirmed match.* Passing the `no_match` rows as well -- the obvious reading
      of "train from the labels" -- would assert that every rejected pair is a match and
      silently corrupt every m value. Only the positives are passed.
    * The table must be registered first, and the registered handle passed back; a bare
      name that was never registered fails with an opaque SQL parser error.

    Returns `(report, error)`. A failure is reported, not swallowed and not patched.
    """
    positives = labelled[labelled["verdict"] == "match"]
    if positives.empty:
        return comparator_report(model), "no positive labels: m cannot be estimated from labels"

    frame = pd.DataFrame(
        {
            "source_dataset_l": "gspt",
            "unique_id_l": positives["unique_id_l"].astype(str),
            "source_dataset_r": "plantgen",
            "unique_id_r": positives["unique_id_r"].astype(str),
        }
    )
    try:
        registered = model.linker.table_management.register_labels_table(frame, overwrite=True)
        model.linker.training.estimate_m_from_pairwise_labels(registered)
    except Exception as error:  # noqa: BLE001 - surfaced in the report, never silenced
        return comparator_report(model), f"{type(error).__name__}: {error}"
    return comparator_report(model), None


def weight_comparison(before: ComparatorReport, after: ComparatorReport) -> pd.DataFrame:
    """Side-by-side match weights, EM-only against label-informed."""
    left = before.as_frame().set_index(["comparison", "level"])["match_weight"]
    right = after.as_frame().set_index(["comparison", "level"])["match_weight"]
    joined = pd.concat([left.rename("em_only"), right.rename("with_labels")], axis=1)
    joined["change"] = joined["with_labels"] - joined["em_only"]
    return joined.reset_index()


@dataclass
class EvaluationReport:
    labels_path: Path
    synthetic: bool
    annotators: list[str]
    label_counts: dict[str, Any]
    curve: pd.DataFrame
    strata: pd.DataFrame
    confusion_by_stratum: pd.DataFrame
    contested_vs_rest: pd.DataFrame
    anchoring: dict[str, Any]
    weights: pd.DataFrame
    retrain_error: str | None = None
    notes: list[str] = field(default_factory=list)

    def banner(self) -> str:
        if not self.label_counts.get("pairs_decided"):
            return (
                "NO LABELS. The real label set is empty, so there is no precision or "
                "recall number of any kind. Run `make label` and have a human decide."
            )
        if self.synthetic:
            return (
                "SYNTHETIC LABELS ONLY. Every number below is computed from machine-"
                "generated verdicts written by `labeling.synthetic`. They exercise the "
                "pipeline. They are NOT accuracy, and the real label set is still empty."
            )
        return "Labels are human-generated."

    def render(self) -> str:
        lines = ["=" * 72, self.banner(), "=" * 72, ""]
        lines += [
            f"labels file        {self.labels_path}",
            f"annotators         {', '.join(self.annotators) or '(none)'}",
            f"lines written      {self.label_counts.get('lines_written', 0)}",
            f"pairs decided      {self.label_counts.get('pairs_decided', 0)}",
            f"superseded lines   {self.label_counts.get('superseded', 0)}",
            f"verdicts           {self.label_counts.get('verdicts', {})}",
            "",
        ]
        if not self.label_counts.get("pairs_decided"):
            return "\n".join(lines)

        lines += [
            "Labels by stratum (sampling rates differ, so do not pool these)",
            "-" * 72,
            self.strata.to_string(index=False) if not self.strata.empty else "  (no strata)",
            "",
            "Confusion by stratum at p >= 0.99 -- DO NOT POOL THESE",
            "-" * 72,
            self.confusion_by_stratum.to_string(index=False),
            "",
            "  The strata are sampled at different rates, so a pooled precision would",
            "  average over a sample the sampler composed. It would also hide this:",
            "",
            self.contested_vs_rest.to_string(index=False),
            "",
            "Precision / recall within the labelled sample, pooled across strata",
            "(shown for the shape of the curve, not as an accuracy figure)",
            "-" * 72,
            self.curve.to_string(index=False),
            "",
            "Anchoring check",
            "-" * 72,
            "  " + ", ".join(f"{k}={v}" for k, v in self.anchoring.items()),
            "",
            "Comparator weights: EM-only vs re-estimated from labels",
            "-" * 72,
        ]
        if self.retrain_error:
            lines.append(f"  label-informed retraining FAILED: {self.retrain_error}")
        else:
            movers = self.weights.dropna(subset=["change"]).reindex(
                self.weights["change"].abs().sort_values(ascending=False).index
            )
            lines.append(movers.head(15).to_string(index=False))
        lines += ["", *self.notes]
        return "\n".join(lines)


def build(
    labels_path: Path | None = None,
    seed: int = DEFAULT_SEED,
    db_path: Path | None = None,
    warehouse: Path | None = None,
    model: TrainedModel | None = None,
) -> EvaluationReport:
    """Run the whole evaluation. Safe to call with an empty label file."""
    trained = model if model is not None else train(db_path, warehouse)
    path = Path(labels_path or store.DECISIONS_PATH)
    counts = store.summary(path)
    queue = build_queue(db_path=db_path, warehouse=warehouse, seed=seed)
    labelled = load_labels(trained.predictions, path=path, queue=queue)

    notes = [
        "Strata are sampled at different rates -- three exhaustively, two randomly --",
        "so these are within-sample figures, not population precision and recall.",
        "`unsure` verdicts are excluded from every metric rather than assigned a class.",
    ]

    if labelled.frame.empty:
        return EvaluationReport(
            labels_path=path,
            synthetic=False,
            annotators=[],
            label_counts=counts,
            curve=pd.DataFrame(),
            strata=pd.DataFrame(),
            confusion_by_stratum=pd.DataFrame(),
            contested_vs_rest=pd.DataFrame(),
            anchoring={},
            weights=pd.DataFrame(),
            notes=notes,
        )

    before = comparator_report(trained)
    after, error = retrain_from_labels(trained, labelled.decisive)
    return EvaluationReport(
        labels_path=path,
        synthetic=labelled.synthetic,
        annotators=labelled.annotators,
        label_counts=counts,
        curve=pr_curve(labelled.decisive),
        strata=by_stratum(labelled.decisive),
        confusion_by_stratum=stratified_confusion(labelled.decisive),
        contested_vs_rest=contested_split(labelled.decisive),
        anchoring=anchoring_check(labelled.frame),
        weights=weight_comparison(before, after),
        retrain_error=error,
        notes=notes,
    )


def main(argv: list[str] | None = None) -> int:
    import argparse
    import logging

    from labeling.synthetic import SYNTHETIC_PATH

    parser = argparse.ArgumentParser(description="Evaluate the linker against labels.")
    parser.add_argument(
        "--synthetic",
        action="store_true",
        help="Evaluate the synthetic label file instead of the real one.",
    )
    parser.add_argument("--labels", type=Path, default=None)
    args = parser.parse_args(argv)
    logging.getLogger("splink").setLevel(logging.ERROR)

    path = args.labels or (SYNTHETIC_PATH if args.synthetic else store.DECISIONS_PATH)
    print(build(labels_path=path).render())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
