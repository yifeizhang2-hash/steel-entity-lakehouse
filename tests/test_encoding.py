"""Encoding-degradation detection and cross-vintage alignment.

See docs/adr/ADR-003-encoding-degradation.md. The rule under test is that damage is
*detected and aligned around*, never repaired.
"""

from __future__ import annotations

import polars as pl
import pytest

from ingest import encoding as enc
from ingest.sources import CLEAN_NAME_VERSION, read_furnace_years

# The two real failure modes, taken verbatim from the sources.
REVERSIBLE = (
    "Nucor Steel–South Carolina Darlington, S.C.",
    "Nucor Steel?South Carolina Darlington, S.C.",
)
LOSSY = ("North American Höganäs Co. Hollsopple, Pa.", "North American H?an? Co. Hollsopple, Pa.")


class TestDetection:
    def test_reversible_substitution_is_degraded_but_not_lossy(self) -> None:
        old, new = REVERSIBLE
        assert len(old) == len(new)
        assert enc.is_degraded(old, new)
        assert not enc.is_lossy(old, new)

    def test_swallowed_characters_are_degraded_and_lossy(self) -> None:
        """42 characters become 40: two characters are gone and unrecoverable."""
        old, new = LOSSY
        assert (len(old), len(new)) == (42, 40)
        assert enc.is_degraded(old, new)
        assert enc.is_lossy(old, new)

    def test_a_legitimate_question_mark_is_not_degradation(self) -> None:
        assert not enc.is_degraded("Who? Steel Co.", "Who? Steel Co.")
        assert not enc.is_degraded("Plain ASCII Steel", "Plain ASCII Steel?")

    def test_identical_strings_are_not_degradation(self) -> None:
        assert not enc.is_degraded(REVERSIBLE[0], REVERSIBLE[0])

    def test_missing_counterpart_is_not_a_verdict(self) -> None:
        assert not enc.is_degraded(None, "Nucor Steel?Texas")
        assert not enc.is_degraded(REVERSIBLE[0], None)

    @pytest.mark.parametrize(
        ("text", "expected"),
        [("Höganäs", True), ("Steel–Co", True), ("plain", False), ("", False), (None, False)],
    )
    def test_has_non_ascii(self, text: str | None, expected: bool) -> None:
        assert enc.has_non_ascii(text) is expected

    def test_alias_lists_of_different_length_are_not_compared(self) -> None:
        """Different lengths mean the lists are not the same record; no verdict."""
        assert enc._compare_lists([REVERSIBLE[0]], [REVERSIBLE[1], "extra"]) == (False, False)


class TestNoRepair:
    def test_the_module_exposes_no_repair_function(self) -> None:
        """A guard against someone adding one later. See ADR-003."""
        banned = [n for n in dir(enc) if any(w in n.lower() for w in ("fix", "repair", "restore"))]
        assert banned == []

    def test_degraded_values_reach_the_table_untouched(self, furnace_files) -> None:
        frame = read_furnace_years(furnace_files)
        damaged = frame.filter(pl.col("encoding_lossy")).head(1)
        assert damaged["corrected_cl"][0].to_list() == [LOSSY[1]]
        assert damaged["corrected_cl_clean"][0].to_list() == [LOSSY[0]]


@pytest.fixture(scope="module")
def furnaces(furnace_files) -> pl.DataFrame:
    return read_furnace_years(furnace_files)


@pytest.fixture(scope="module")
def report(furnaces: pl.DataFrame) -> enc.EncodingReport:
    return enc.audit(furnaces)


class TestAlignment:
    def test_alignment_does_not_change_the_row_count(self, furnaces: pl.DataFrame) -> None:
        """A fan-out here would silently duplicate furnace-years."""
        assert furnaces.height == 4352

    def test_every_keyed_row_aligns(self, furnaces: pl.DataFrame) -> None:
        counts = dict(furnaces.group_by("alignment_status").len().iter_rows())  # type: ignore[arg-type]
        assert counts == {"aligned": 4330, "unaligned_no_key": 22}
        assert "unaligned_no_match" not in counts

    def test_only_the_keyless_south_american_rows_fail(self, furnaces: pl.DataFrame) -> None:
        unaligned = furnaces.filter(pl.col("alignment_status") == "unaligned_no_key")
        assert unaligned["align_key"].null_count() == unaligned.height
        # Two distinct facilities, not a scattering of rows across many.
        assert unaligned["owner"].n_unique() == 2
        assert set(unaligned["source_file"].to_list()) == {"eaf_owner_filled.csv"}

    def test_clean_names_come_from_the_intact_vintage(self, furnaces: pl.DataFrame) -> None:
        by_version = {
            (row["source_version"], row["corrected_cl_name_version"]): row["len"]
            for row in furnaces.group_by("source_version", "corrected_cl_name_version")
            .len()
            .to_dicts()
        }
        assert by_version[("owner_filled_2025", CLEAN_NAME_VERSION)] == 2177
        assert by_version[("owner_filled_2025", "owner_filled_2025")] == 22
        assert by_version[("gspt_2024", CLEAN_NAME_VERSION)] == 2153

    def test_owner_comes_only_from_the_damaged_vintage(self, furnaces: pl.DataFrame) -> None:
        """This is why the damaged vintage cannot simply be dropped."""
        clean = furnaces.filter(pl.col("source_version") == CLEAN_NAME_VERSION)
        filled = furnaces.filter(pl.col("source_version") == "owner_filled_2025")
        assert clean["owner"].null_count() == clean.height
        assert filled["owner"].null_count() == 0

    def test_one_row_shows_the_whole_combination(self, furnaces: pl.DataFrame) -> None:
        row = furnaces.filter(
            (pl.col("source_version") == "owner_filled_2025") & pl.col("encoding_lossy")
        ).head(1)
        assert row["owner"][0] == "North American H?an?"
        assert row["corrected_cl"][0].to_list() == [LOSSY[1]]
        assert row["corrected_cl_clean"][0].to_list() == [LOSSY[0]]
        assert row["corrected_cl_name_version"][0] == CLEAN_NAME_VERSION
        assert row["alignment_status"][0] == "aligned"


class TestAudit:
    def test_counts_match_the_measured_damage(self, report: enc.EncodingReport) -> None:
        assert report.as_dict() == {
            "rows_checked": 4352,
            "degraded_rows": 143,
            "lossy_rows": 12,
            "degraded_facilities": 12,
            "owner_replacement_rows": 48,
        }
        assert not report.clean

    def test_it_warns_rather_than_halting(self, furnace_files, caplog) -> None:
        """Halting would be wrong: the damaged vintage is the only source of owners."""
        caplog.clear()
        with caplog.at_level("WARNING", logger="ingest.encoding"):
            frame = read_furnace_years(furnace_files)
        assert frame.height == 4352
        assert any("encoding degradation detected" in r.message for r in caplog.records)
        assert any("NOT repaired" in r.getMessage() for r in caplog.records)

    def test_a_clean_frame_produces_no_warning(self, furnaces: pl.DataFrame, caplog) -> None:
        frame = furnaces.with_columns(
            pl.lit(False).alias("encoding_degraded"),
            pl.lit(False).alias("encoding_lossy"),
            pl.lit(False).alias("owner_has_replacement_char"),
        )
        caplog.clear()
        with caplog.at_level("WARNING", logger="ingest.encoding"):
            result = enc.audit(frame)
        assert result.clean
        assert not [r for r in caplog.records if "encoding degradation" in r.message]
