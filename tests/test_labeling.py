"""Sampling, the decision log, and the evaluation pipeline.

None of these tests create real labels. The real decision log must be written by a
human; there is an explicit test asserting this repository ships it empty.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from labeling import store
from labeling.evaluate import (
    anchoring_check,
    confusion_at,
    load_labels,
    pr_curve,
    retrain_from_labels,
)
from labeling.evaluate import (
    build as build_evaluation,
)
from labeling.sampling import (
    DEFAULT_SEED,
    STRATUM_ORDER,
    Quotas,
    assign_strata,
    build_queue,
    composition,
    contested_groups,
    contested_pairs,
    pair_id,
)
from labeling.synthetic import generate as generate_synthetic


def _candidates() -> pd.DataFrame:
    """A small hand-built candidate set, so the sampler can be tested without a lake."""
    rows = [
        # left, right, probability
        ("G1", "P1", 0.999),  # high, and contested with G1/P2
        ("G1", "P2", 0.995),
        ("G2", "P3", 0.5),  # ambiguous
        ("G2", "P4", 0.2),  # ambiguous
        ("G3", "COOK1", 0.0001),  # cook county
        ("G3", "P5", 0.0001),  # low
        ("G4", "P6", 0.9995),  # high
        ("G5", "P7", 0.000001),  # low
    ]
    return pd.DataFrame(
        [
            {"unique_id_l": left, "unique_id_r": right, "match_probability": probability}
            for left, right, probability in rows
        ]
    )


class TestStrata:
    def test_every_pair_gets_exactly_one_stratum(self) -> None:
        candidates = _candidates()
        strata = assign_strata(candidates, cook_ids={"COOK1"})
        assert len(strata) == len(candidates)
        assert set(strata) <= set(STRATUM_ORDER)

    def test_contested_detects_competing_confident_claims(self) -> None:
        """Both of G1's candidates are confident, so neither can be taken on its own."""
        candidates = _candidates()
        contested = contested_pairs(candidates)
        assert contested.sum() == 2
        assert set(candidates.loc[contested, "unique_id_r"]) == {"P1", "P2"}

    def test_a_single_confident_claim_is_not_contested(self) -> None:
        candidates = _candidates()
        contested = contested_pairs(candidates)
        assert not contested[candidates["unique_id_r"] == "P6"].any()

    def test_cook_county_beats_the_low_band(self) -> None:
        """A near-zero Cook pair is sampled as Cook, not lost into the random low draw."""
        candidates = _candidates()
        strata = assign_strata(candidates, cook_ids={"COOK1"})
        assert strata[candidates["unique_id_r"] == "COOK1"].tolist() == ["cook_county"]


class TestQueue:
    def test_is_reproducible(self) -> None:
        candidates = _candidates()
        first = build_queue(candidates=candidates, cook_ids={"COOK1"}, seed=7)
        second = build_queue(candidates=candidates, cook_ids={"COOK1"}, seed=7)
        assert first["pair_id"].tolist() == second["pair_id"].tolist()

    def test_a_different_seed_can_draw_differently(self) -> None:
        """Only the randomly-drawn strata move; the exhaustive ones cannot."""
        candidates = _candidates()
        quotas = Quotas(high_confidence=1, low_confidence=1)
        a = build_queue(candidates=candidates, cook_ids={"COOK1"}, seed=1, quotas=quotas)
        b = build_queue(candidates=candidates, cook_ids={"COOK1"}, seed=2, quotas=quotas)
        exhaustive = {"ambiguous", "contested", "cook_county"}
        assert set(a[a["stratum"].isin(exhaustive)]["pair_id"]) == set(
            b[b["stratum"].isin(exhaustive)]["pair_id"]
        )

    def test_no_duplicate_pairs(self) -> None:
        queue = build_queue(candidates=_candidates(), cook_ids={"COOK1"})
        assert not queue["pair_id"].duplicated().any()

    def test_quotas_are_respected(self) -> None:
        queue = build_queue(
            candidates=_candidates(),
            cook_ids={"COOK1"},
            quotas=Quotas(high_confidence=1, low_confidence=1),
        )
        assert (queue["stratum"] == "high_confidence").sum() == 1
        assert (queue["stratum"] == "low_confidence").sum() == 1

    def test_composition_adds_up(self) -> None:
        candidates = _candidates()
        candidates["stratum"] = assign_strata(candidates, {"COOK1"})
        queue = build_queue(candidates=candidates.drop(columns="stratum"), cook_ids={"COOK1"})
        table = composition(queue, candidates)
        total = table[table["stratum"] == "TOTAL"]["in_queue"].iloc[0]
        assert total == len(queue)
        assert table[table["stratum"] != "TOTAL"]["in_queue"].sum() == total

    def test_contested_groups_keep_candidates_together(self) -> None:
        """The UI needs a record's candidates as a group, not as separate questions."""
        queue = build_queue(candidates=_candidates(), cook_ids={"COOK1"})
        groups = contested_groups(queue)
        assert "G1" in groups
        assert len(groups["G1"]) == 2

    def test_pair_id_is_stable(self) -> None:
        assert pair_id("A", "B") == "A|B"


class TestDecisionLog:
    def test_a_decision_round_trips(self, tmp_path: Path) -> None:
        path = tmp_path / "decisions.jsonl"
        store.append(
            store.Decision(
                pair_id="A|B",
                unique_id_l="A",
                unique_id_r="B",
                verdict="match",
                annotator="tester",
                probability_shown=0.42,
                stratum="ambiguous",
            ),
            path=path,
        )
        records = store.read_all(path)
        assert len(records) == 1
        assert records[0]["verdict"] == "match"
        assert records[0]["probability_shown"] == 0.42
        assert records[0]["decided_at"].endswith("+00:00")

    def test_nothing_is_ever_overwritten(self, tmp_path: Path) -> None:
        """Changing your mind adds a line; the old judgement stays on the record."""
        path = tmp_path / "decisions.jsonl"
        for verdict in ("match", "no_match"):
            store.append(
                store.Decision("A|B", "A", "B", verdict, "tester"),
                path=path,
            )
        assert len(store.read_all(path)) == 2
        assert store.current(path)["A|B"]["verdict"] == "no_match"
        assert store.summary(path)["superseded"] == 1

    def test_an_unknown_verdict_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="verdict must be one of"):
            store.Decision("A|B", "A", "B", "probably", "tester")

    def test_an_anonymous_decision_is_rejected(self) -> None:
        """A label with no author cannot be audited or disputed."""
        with pytest.raises(ValueError, match="who made it"):
            store.Decision("A|B", "A", "B", "match", "   ")

    def test_the_probability_shown_is_recorded(self, tmp_path: Path) -> None:
        """Without this, anchoring can only be assumed absent, never checked."""
        path = tmp_path / "decisions.jsonl"
        store.append(store.Decision("A|B", "A", "B", "match", "t", probability_shown=None), path)
        store.append(store.Decision("C|D", "C", "D", "match", "t", probability_shown=0.9), path)
        shown = [r["probability_shown"] for r in store.read_all(path)]
        assert shown == [None, 0.9]

    def test_a_corrupt_line_is_reported_with_its_number(self, tmp_path: Path) -> None:
        path = tmp_path / "decisions.jsonl"
        path.write_text('{"pair_id": "A|B"}\nnot json\n')
        with pytest.raises(ValueError, match=":2"):
            store.read_all(path)


class TestTheLabelsAreHuman:
    """The decision log now has 192 human labels in it. These pin what makes them usable.

    A committed log with machine-written rows in it would be indefensible: nothing
    downstream could tell them from human judgement. So the provenance is asserted, not
    assumed.
    """

    def test_the_log_holds_real_human_labels(self) -> None:
        counts = store.summary()
        assert counts["has_real_labels"] is True
        assert counts["synthetic_only"] is False
        assert counts["pairs_decided"] == 192

    def test_no_synthetic_row_leaked_into_the_real_log(self) -> None:
        records = store.read_all()
        assert records
        assert not any(record["synthetic"] for record in records)
        assert store.SYNTHETIC_ANNOTATOR not in {r["annotator"] for r in records}

    def test_nothing_was_superseded(self) -> None:
        counts = store.summary()
        assert counts["lines_written"] == counts["pairs_decided"] == 192
        assert counts["superseded"] == 0

    def test_every_label_was_made_without_seeing_the_model_score(self) -> None:
        """The single most important property of this label set.

        A label produced while looking at the model's answer cannot be used to evaluate
        that model. `probability_shown` records what was on screen at decision time, so
        this is measured rather than assumed.
        """
        records = store.read_all()
        shown = [r for r in records if r["probability_shown"] is not None]
        assert shown == [], f"{len(shown)} labels were made with the model score visible"

    def test_the_verdict_mix_is_what_was_labelled(self) -> None:
        counts = store.summary()
        assert counts["verdicts"] == {"match": 37, "no_match": 144, "unsure": 11}

    def test_unsure_is_a_real_outcome_and_is_recorded(self) -> None:
        """11 declined calls, 6 of them in the hardest stratum. Not silently dropped."""
        records = store.read_all()
        unsure = [r for r in records if r["verdict"] == "unsure"]
        assert len(unsure) == 11
        by_stratum: dict[str, int] = {}
        for record in unsure:
            by_stratum[record["stratum"]] = by_stratum.get(record["stratum"], 0) + 1
        assert by_stratum["contested"] == 6


class TestSyntheticLabels:
    @pytest.fixture
    def synthetic_path(self, tmp_path: Path) -> Path:
        queue = build_queue(candidates=_candidates(), cook_ids={"COOK1"})
        return generate_synthetic(queue=queue, path=tmp_path / "synthetic.jsonl", seed=3)

    def test_every_synthetic_record_is_marked(self, synthetic_path: Path) -> None:
        records = store.read_all(synthetic_path)
        assert records
        for record in records:
            assert record["synthetic"] is True
            assert record["annotator"] == store.SYNTHETIC_ANNOTATOR
            assert "not ground truth" in record["notes"]

    def test_they_do_not_simply_echo_the_model(self, synthetic_path: Path) -> None:
        """If they did, precision would be 1.0 by construction and prove nothing."""
        queue = build_queue(candidates=_candidates(), cook_ids={"COOK1"})
        labels = {r["pair_id"]: r["verdict"] for r in store.read_all(synthetic_path)}
        disagreements = sum(
            1
            for _, row in queue.iterrows()
            if labels[row["pair_id"]]
            != ("match" if row["match_probability"] >= 0.5 else "no_match")
        )
        assert disagreements > 0

    def test_generation_is_reproducible(self, tmp_path: Path) -> None:
        queue = build_queue(candidates=_candidates(), cook_ids={"COOK1"})
        first = generate_synthetic(queue=queue, path=tmp_path / "a.jsonl", seed=11)
        second = generate_synthetic(queue=queue, path=tmp_path / "b.jsonl", seed=11)
        verdicts = [[r["verdict"] for r in store.read_all(p)] for p in (first, second)]
        assert verdicts[0] == verdicts[1]


class TestMetrics:
    @staticmethod
    def _labelled() -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"match_probability": 0.99, "verdict": "match"},
                {"match_probability": 0.97, "verdict": "no_match"},
                {"match_probability": 0.30, "verdict": "match"},
                {"match_probability": 0.001, "verdict": "no_match"},
                {"match_probability": 0.001, "verdict": "match"},
            ]
        )

    def test_confusion_counts(self) -> None:
        result = confusion_at(self._labelled(), 0.5)
        assert (result["tp"], result["fp"], result["fn"], result["tn"]) == (1, 1, 2, 1)
        assert result["precision"] == 0.5
        assert result["recall"] == pytest.approx(1 / 3)

    def test_recall_needs_the_low_end(self) -> None:
        """Label only the confident pairs and recall becomes meaningless, not merely high.

        Here every labelled pair is above the threshold, so there are no false negatives
        to find and recall reads 1.0 -- a number that says nothing about the model.
        """
        top_only = self._labelled().head(2)
        result = confusion_at(top_only, 0.5)
        assert result["fn"] == 0
        assert result["recall"] == 1.0
        full = confusion_at(self._labelled(), 0.5)
        assert full["recall"] < 1.0

    def test_pr_curve_covers_every_threshold(self) -> None:
        curve = pr_curve(self._labelled())
        assert len(curve) == 7
        assert curve["threshold"].is_monotonic_increasing

    def test_unsure_is_excluded_not_reassigned(self, tmp_path: Path) -> None:
        predictions = pd.DataFrame(
            {"unique_id_l": ["A"], "unique_id_r": ["B"], "match_probability": [0.8]}
        )
        path = tmp_path / "d.jsonl"
        store.append(store.Decision("A|B", "A", "B", "unsure", "tester"), path)
        labelled = load_labels(predictions, path=path)
        assert len(labelled.frame) == 1
        assert labelled.decisive.empty

    def test_anchoring_check_separates_shown_from_hidden(self) -> None:
        labelled = pd.DataFrame(
            [
                {"match_probability": 0.99, "verdict": "match", "probability_shown": 0.99},
                {"match_probability": 0.99, "verdict": "no_match", "probability_shown": None},
            ]
        )
        result = anchoring_check(labelled)
        assert result["model_opinion_shown"] == 1
        assert result["model_opinion_hidden"] == 1
        assert result["agreement_when_shown"] == 1.0
        assert result["agreement_when_hidden"] == 0.0


@pytest.mark.slow
class TestEvaluationPipeline:
    def test_no_labels_produces_no_metrics(self, tmp_path: Path) -> None:
        """The empty case must say so, not print a zero."""
        report = build_evaluation(labels_path=tmp_path / "absent.jsonl")
        assert "NO LABELS" in report.banner()
        assert report.curve.empty
        assert "no precision or recall" in report.render()

    def test_synthetic_labels_are_labelled_as_such(self, tmp_path: Path) -> None:
        path = tmp_path / "synthetic.jsonl"
        generate_synthetic(path=path)
        report = build_evaluation(labels_path=path)
        assert report.synthetic is True
        assert "SYNTHETIC LABELS ONLY" in report.banner()
        assert "NOT accuracy" in report.banner()
        assert not report.curve.empty

    def test_retraining_from_labels_moves_weights(self, tmp_path: Path) -> None:
        path = tmp_path / "synthetic.jsonl"
        generate_synthetic(path=path)
        report = build_evaluation(labels_path=path)
        assert report.retrain_error is None
        changed = report.weights.dropna(subset=["change"])
        assert not changed.empty
        assert changed["change"].abs().max() > 0

    def test_only_positive_labels_are_used_for_m_estimation(self) -> None:
        """`estimate_m_from_pairwise_labels` treats every row it is given as a match.

        Passing the rejections too would assert they are matches and corrupt every m.
        """
        import inspect

        source = inspect.getsource(retrain_from_labels)
        assert 'verdict"] == "match"' in source
        assert "clerical_match_score" not in source.split('"""')[2]

    def test_weight_comparison_is_side_by_side(self, tmp_path: Path) -> None:
        path = tmp_path / "synthetic.jsonl"
        generate_synthetic(path=path)
        report = build_evaluation(labels_path=path)
        assert set(report.weights.columns) >= {
            "comparison",
            "level",
            "em_only",
            "with_labels",
            "change",
        }


@pytest.mark.slow
class TestRealQueue:
    def test_the_queue_covers_every_stratum(self) -> None:
        queue = build_queue(seed=DEFAULT_SEED)
        assert set(queue["stratum"]) == set(STRATUM_ORDER)
        assert len(queue) == 192

    def test_cook_county_is_exhaustive(self) -> None:
        """The whole point: the county where county and ZIP both fail is not sampled."""
        from labeling.sampling import _cook_county_ids, load_candidates

        candidates = load_candidates()
        candidates["stratum"] = assign_strata(candidates, _cook_county_ids(None, None))
        queue = build_queue(seed=DEFAULT_SEED)
        available = int((candidates["stratum"] == "cook_county").sum())
        assert int((queue["stratum"] == "cook_county").sum()) == available

    def test_indiana_harbor_is_queued_as_contested(self) -> None:
        """Same county, same ZIP, same city: no rule can separate these, so a human must."""
        queue = build_queue(seed=DEFAULT_SEED)
        harbor = queue[queue["unique_id_l"] == "P100000120926"]
        assert set(harbor["unique_id_r"]) >= {"97", "142"}
        assert set(harbor[harbor["unique_id_r"].isin(["97", "142"])]["stratum"]) == {"contested"}

    def test_the_queue_carries_what_a_human_needs_to_judge(self) -> None:
        queue = build_queue(seed=DEFAULT_SEED)
        for column in ("gspt_name", "gspt_address", "plantgen_name", "plantgen_zip"):
            assert column in queue.columns

    def test_queue_json_is_writable(self, tmp_path: Path) -> None:
        queue = build_queue(seed=DEFAULT_SEED).head(3)
        path = tmp_path / "queue.jsonl"
        path.write_text(
            "\n".join(json.dumps({"pair_id": p}) for p in queue["pair_id"]), encoding="utf-8"
        )
        assert len(path.read_text().splitlines()) == 3


def _require_real_data() -> None:
    """Skip when the pipeline's inputs are absent.

    Everything below trains the real linker against the built lake. The source data is
    not redistributed, so on a fresh clone -- and in CI -- these skip rather than fail.
    """
    from ingest.config import PLANTGEN_CSV
    from lakehouse.paths import LAKE_ROOT

    if not PLANTGEN_CSV.exists():
        pytest.skip(f"raw data not present: {PLANTGEN_CSV}")
    if not (LAKE_ROOT / "catalog.db").exists():
        pytest.skip("no lake; run `make ingest geo resolve` first")


@pytest.fixture(scope="module")
def report():
    _require_real_data()
    from labeling.evaluate import build as build_evaluation

    return build_evaluation()


@pytest.mark.slow
class TestStratifiedResults:
    """The measured findings, pinned. If the model regresses, these fail."""

    def test_the_confusion_is_reported_per_stratum(self, report) -> None:
        frame = report.confusion_by_stratum.set_index("stratum")
        assert set(frame.index) == {
            "cook_county",
            "low_confidence",
            "high_confidence",
            "contested",
            "ambiguous",
        }
        assert int(frame.loc["high_confidence", "tp"]) == 25
        assert int(frame.loc["high_confidence", "fp"]) == 2
        assert int(frame.loc["contested", "tp"]) == 7
        assert int(frame.loc["contested", "fp"]) == 7

    def test_scoring_works_and_assignment_does_not(self, report) -> None:
        """The headline finding: 0.93 where one candidate is confident, 0.50 where several are."""
        split = report.contested_vs_rest.set_index("group")
        assert round(float(split.loc["not contested", "precision"]), 3) == 0.929
        assert float(split.loc["contested", "precision"]) == 0.5

    def test_no_true_match_hides_below_the_low_threshold(self, report) -> None:
        """30 pairs from below p=0.01 contained zero matches. Only a stratified queue
        can produce this number; labelling the top of the ranking cannot."""
        frame = report.confusion_by_stratum.set_index("stratum")
        assert int(frame.loc["low_confidence", "tp"]) == 0
        assert int(frame.loc["low_confidence", "fn"]) == 0
        assert int(frame.loc["low_confidence", "tn"]) == 30

    def test_cook_county_was_exhaustive_and_perfect(self, report) -> None:
        """The hardest subgroup: county and ZIP fail together. 94 labels bought 1 positive."""
        frame = report.confusion_by_stratum.set_index("stratum")
        assert int(frame.loc["cook_county", "labelled"]) == 94
        assert int(frame.loc["cook_county", "tp"]) == 1
        assert int(frame.loc["cook_county", "fp"]) == 0
        assert int(frame.loc["cook_county", "fn"]) == 0

    def test_the_labels_were_not_anchored(self, report) -> None:
        assert report.anchoring["model_opinion_shown"] == 0
        assert report.anchoring["model_opinion_hidden"] == 192

    def test_the_labels_confirm_rather_than_restructure_the_em_fit(self, report) -> None:
        """Largest weight change 0.31; most under 0.05. Independent corroboration of an
        unsupervised fit -- and why 37 positives cannot fix the correlated comparators."""
        assert report.retrain_error is None
        change = report.weights["change"].abs()
        assert change.max() < 0.35
        assert change.median() < 0.05

    def test_it_is_not_reported_as_synthetic(self, report) -> None:
        assert report.synthetic is False
        assert "SYNTHETIC" not in report.banner()
        assert report.annotators == ["fei"]


def _contested_by_facility() -> pd.DataFrame:
    """Each contested GSPT plant, its candidate gap, and what taking the top scorer does."""
    from labeling.evaluate import load_labels
    from labeling.sampling import build_queue
    from resolution.model import train

    model = train()
    labelled = load_labels(model.predictions, queue=build_queue()).frame
    contested = labelled[labelled["stratum"] == "contested"]
    rows = []
    for plant, group in contested.groupby("unique_id_l"):
        ordered = group.sort_values("match_probability", ascending=False)
        truth = ordered[ordered["verdict"] == "match"]
        gap = (
            float(ordered.iloc[0]["match_probability"] - ordered.iloc[1]["match_probability"])
            if len(ordered) > 1
            else float("nan")
        )
        rows.append(
            {
                "plant": plant,
                "candidates": len(ordered),
                "gap": gap,
                "distinct_scores": ordered["match_probability"].nunique(),
                "outcome": (
                    "none_of_these"
                    if truth.empty
                    else (
                        "correct"
                        if truth.iloc[0]["unique_id_r"] == ordered.iloc[0]["unique_id_r"]
                        else "wrong"
                    )
                ),
            }
        )
    return pd.DataFrame(rows)


@pytest.fixture(scope="module")
def facilities() -> pd.DataFrame:
    _require_real_data()
    return _contested_by_facility()


@pytest.mark.slow
class TestContestedAssignmentIsUnresolved:
    """Evidence for ADR-009, pinned so a change cannot quietly invalidate it."""

    def test_taking_the_top_score_is_a_coin_flip_at_facility_grain(
        self, facilities: pd.DataFrame
    ) -> None:
        """Same 50% as the pair-level count, reached by a different route."""
        tally = facilities["outcome"].value_counts().to_dict()
        assert tally == {"correct": 5, "none_of_these": 3, "wrong": 2}
        assert len(facilities) == 10
        assert tally["correct"] / len(facilities) == 0.5

    def test_some_contested_groups_have_a_probability_gap_of_exactly_zero(
        self, facilities: pd.DataFrame
    ) -> None:
        """The load-bearing fact in ADR-009.

        Two facilities' candidates carry bit-identical probabilities, so their ordering
        is decided by row order rather than by evidence.

        If a future change makes every gap non-zero, the overwhelmingly likely cause is
        that an artificial tiebreak has been introduced somewhere -- not that the model
        learned to separate records whose every compared field is identical. That is why
        this asserts the ties still EXIST rather than asserting they are gone.
        """
        tied = facilities[facilities["gap"] == 0.0]
        assert len(tied) == 2, (
            "expected 2 contested facilities with an exact tie; if this dropped to 0, "
            "check whether a tiebreak was added rather than assuming the model improved"
        )
        assert (tied["candidates"] >= 2).all()

    def test_a_tied_group_has_one_single_distinct_score_across_all_candidates(
        self, facilities: pd.DataFrame
    ) -> None:
        """Cleveland-Cliffs Cleveland: three candidates, one probability between them."""
        assert (facilities["distinct_scores"] < facilities["candidates"]).sum() >= 2

    def test_the_correct_answer_is_sometimes_none_of_the_candidates(
        self, facilities: pd.DataFrame
    ) -> None:
        """The root objection to assignment: a matcher cannot express rejection.

        On 3 of 10 facilities every candidate is wrong, so any algorithm whose only
        operation is *selecting an element* must emit a confident error.
        """
        none_of_these = facilities[facilities["outcome"] == "none_of_these"]
        assert len(none_of_these) == 3
        assert none_of_these["candidates"].max() == 4

    def test_most_correct_picks_rest_on_a_gap_under_one_thousandth(
        self, facilities: pd.DataFrame
    ) -> None:
        """A coin landing the right way up is not evidence that flipping it is a method."""
        correct = facilities[facilities["outcome"] == "correct"]
        assert (correct["gap"] < 0.001).sum() >= 4
