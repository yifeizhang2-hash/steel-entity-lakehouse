"""Streamlit review UI for the labelling queue.

[pandas] Run with `make label`. Nothing here imports Polars or `ingest`.

TWO DESIGN RULES, BOTH ABOUT NOT CORRUPTING THE LABELS
------------------------------------------------------
1. **The model's opinion is not visually dominant.** The match probability and the
   evidence vector are available, but folded away behind an expander and never rendered
   near the verdict buttons. A labeller who sees "99.7%" in large type before reading
   the records is being asked to agree, not to judge, and the resulting labels would
   mostly measure the model they are supposed to test. The probability that *was*
   displayed is recorded with each decision so the anchoring can be checked afterwards
   rather than assumed absent.

2. **Every candidate for a record is shown at once.** Presenting candidates one at a
   time asks "is this the one?", which is a different and easier question than "which of
   these, if any, is the one?". Cleveland-Cliffs Indiana Harbor has two candidates that
   share its county, its ZIP and its city; shown separately, both look convincing.
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import streamlit as st

from labeling import store
from labeling.sampling import DEFAULT_SEED, STRATUM_PURPOSE, build_queue

GAMMA_LABELS = {
    "gamma_zip5": "ZIP",
    "gamma_street": "street",
    "gamma_place": "place name",
    "gamma_alias_place": "secondary names",
    "gamma_start_year": "start year",
    "gamma_county": "county",
}

# The GSPT record is identical across a group's candidates, so it is rendered once at
# full width rather than repeated in every column. Repeating it halved the space
# available for the records that actually differ.
GSPT_FIELDS = (
    ("name", "gspt_name"),
    ("other names", "gspt_other_names"),
    ("city", "gspt_city"),
    ("state", "gspt_state"),
    ("address", "gspt_address"),
    ("owner", "gspt_owner"),
    ("start", "gspt_start"),
    ("country", "gspt_country"),
)

PLANTGEN_FIELDS = (
    ("name", "plantgen_name"),
    ("city", "plantgen_city"),
    ("county", "plantgen_county"),
    ("state", "plantgen_state"),
    ("ZIP", "plantgen_zip"),
    ("address", "plantgen_address"),
    ("start", "plantgen_start"),
)


@st.cache_data(show_spinner="Loading the labelling queue...")
def _queue(seed: int) -> pd.DataFrame:
    return build_queue(seed=seed)


def _value(row: pd.Series, column: str | None) -> str:
    if column is None or column not in row.index:
        return "—"
    value = row[column]
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "—"
    text = str(value).strip()
    return text if text and text.lower() not in {"nan", "unknown", "n/a"} else "—"


def _record_table(row: pd.Series, fields: tuple[tuple[str, str], ...], title: str) -> pd.DataFrame:
    return pd.DataFrame([{"field": label, title: _value(row, column)} for label, column in fields])


def _evidence_table(row: pd.Series) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"signal": label, "comparison level": int(row[column])}
            for column, label in GAMMA_LABELS.items()
            if column in row.index and pd.notna(row[column])
        ]
    )


def _decide(row: pd.Series, verdict: str, annotator: str, notes: str, path: Path | None) -> None:
    store.append(
        store.Decision(
            pair_id=row["pair_id"],
            unique_id_l=str(row["unique_id_l"]),
            unique_id_r=str(row["unique_id_r"]),
            verdict=verdict,
            annotator=annotator,
            probability_shown=(
                float(row["match_probability"])
                if st.session_state.get("reveal_model", False)
                else None
            ),
            stratum=row.get("stratum"),
            queue_seed=int(row.get("queue_seed", DEFAULT_SEED)),
            notes=notes,
        ),
        path=path,
    )


def main() -> None:
    st.set_page_config(page_title="Plant match review", layout="wide")
    st.title("Plant match review")

    decisions_path = Path(os.environ.get("STEEL_DECISIONS", store.DECISIONS_PATH))
    seed = int(os.environ.get("STEEL_QUEUE_SEED", DEFAULT_SEED))
    queue = _queue(seed)

    with st.sidebar:
        st.header("Session")
        annotator = st.text_input("Your name or initials", key="annotator")
        st.caption("Recorded with every decision. Required.")
        st.divider()
        st.checkbox(
            "Show the model's probability",
            key="reveal_model",
            value=False,
            help=(
                "Off by default. Whether it was on is stored with each decision, so "
                "anchoring can be measured rather than assumed away."
            ),
        )
        st.divider()
        counts = store.summary(decisions_path)
        st.metric("Pairs decided", counts["pairs_decided"])
        st.caption(f"{counts['lines_written']} lines written, {counts['superseded']} superseded")
        if counts["synthetic_only"] and counts["lines_written"]:
            st.warning("This file contains synthetic labels only.")

    decided = set(store.current(decisions_path))
    remaining = queue[~queue["pair_id"].isin(decided)]
    st.progress(
        (len(queue) - len(remaining)) / len(queue) if len(queue) else 0.0,
        text=f"{len(queue) - len(remaining)} of {len(queue)} pairs decided",
    )

    if remaining.empty:
        st.success("Every pair in the queue has been decided.")
        return

    # Work one GSPT record at a time, showing all of its queued candidates together.
    current_left = remaining.iloc[0]["unique_id_l"]
    group = remaining[remaining["unique_id_l"] == current_left]

    st.subheader(f"GSPT record {current_left}")
    st.caption(
        f"{len(group)} candidate(s) in the queue for this record  ·  "
        f"stratum: {', '.join(sorted(set(group['stratum'])))}"
    )
    for stratum in sorted(set(group["stratum"])):
        st.caption(STRATUM_PURPOSE.get(stratum, ""))

    if len(group) > 1:
        st.info(
            "This record has several candidates. They are shown side by side on purpose: "
            "choose the one that is the same plant, or mark them all as not a match."
        )

    if not annotator.strip():
        st.warning("Enter your name in the sidebar before deciding anything.")

    st.dataframe(
        _record_table(group.iloc[0], GSPT_FIELDS, "GSPT record"),
        hide_index=True,
        use_container_width=True,
    )

    notes = st.text_area("Notes (optional, stored with the decision)", key="notes")

    st.markdown("**Candidates in PLANTGEN**")
    columns = st.columns(len(group))
    for column, (_, row) in zip(columns, group.iterrows(), strict=True):
        with column:
            st.markdown(f"**PLANTGEN {row['unique_id_r']}**")
            st.dataframe(
                _record_table(row, PLANTGEN_FIELDS, "value"),
                hide_index=True,
                use_container_width=True,
            )

            # Deliberately below the records and collapsed: available, not prominent.
            with st.expander("What the model thinks"):
                if st.session_state.get("reveal_model", False):
                    st.write(f"match probability: {row['match_probability']:.4f}")
                    st.write(f"match weight: {row['match_weight']:.2f}")
                else:
                    st.caption("Hidden. Turn on in the sidebar if you want to see it.")
                evidence = _evidence_table(row)
                if not evidence.empty:
                    st.dataframe(evidence, hide_index=True, use_container_width=True)

            disabled = not annotator.strip()
            if st.button(
                "Same plant", key=f"match-{row['pair_id']}", disabled=disabled, type="primary"
            ):
                # Choosing one candidate rejects the others for this record.
                for _, other in group.iterrows():
                    verdict = "match" if other["pair_id"] == row["pair_id"] else "no_match"
                    _decide(other, verdict, annotator, notes, decisions_path)
                st.rerun()
            if st.button("Not a match", key=f"no-{row['pair_id']}", disabled=disabled):
                _decide(row, "no_match", annotator, notes, decisions_path)
                st.rerun()
            if st.button("Unsure", key=f"unsure-{row['pair_id']}", disabled=disabled):
                _decide(row, "unsure", annotator, notes, decisions_path)
                st.rerun()

    st.divider()
    if st.button("None of these is a match", disabled=not annotator.strip()):
        for _, row in group.iterrows():
            _decide(row, "no_match", annotator, notes, decisions_path)
        st.rerun()


if __name__ == "__main__":
    main()
