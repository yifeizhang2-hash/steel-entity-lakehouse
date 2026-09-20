"""Unit tests for the value- and name-normalisation helpers."""

from __future__ import annotations

import polars as pl
import pytest

from ingest import normalize as nz


class TestZipCode:
    """ZIP codes are identifiers, not numbers.

    All three inputs below occur in practice: `"601.0"` is what survives a float
    round-trip (pandas infers PLANTGEN's ZipCode as float64), `"6607"` is what the
    PLANTGEN file itself already stores for Bridgeport CT, and `"16003"` is intact.
    """

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("601.0", "00601"),  # the float round-trip case
            ("601", "00601"),
            ("6607", "06607"),  # the case that is really in PLANTGEN.csv
            ("6607.0", "06607"),
            ("16003", "16003"),
            ("16003.0", "16003"),
            ("16003-1234", "16003"),  # ZIP+4 truncates to the 5-digit prefix
            ("  8077 ", "08077"),
            ("", None),
            (None, None),
            ("N/A", None),  # not digits -> null, never a wrong code
        ],
    )
    def test_zip5(self, raw: str | None, expected: str | None) -> None:
        frame = pl.DataFrame({"zip_code": [raw]}, schema={"zip_code": pl.Utf8})
        assert frame.with_columns(nz.zip5("zip_code"))["zip_code"][0] == expected

    def test_zip5_result_is_text(self) -> None:
        frame = pl.DataFrame({"zip_code": ["601.0"]}, schema={"zip_code": pl.Utf8})
        assert frame.with_columns(nz.zip5("zip_code")).schema["zip_code"] == pl.Utf8


class TestFips:
    @pytest.mark.parametrize(
        ("raw", "width", "expected"),
        [
            ("9001", 5, "09001"),
            ("09001", 5, "09001"),
            ("9001.0", 5, "09001"),
            ("9", 2, "09"),
            ("19", 3, "019"),
            (None, 5, None),
        ],
    )
    def test_pad_fips(self, raw: str | None, width: int, expected: str | None) -> None:
        frame = pl.DataFrame({"f": [raw]}, schema={"f": pl.Utf8})
        assert frame.with_columns(nz.pad_fips("f", width))["f"][0] == expected


class TestColumnCanonicalisation:
    """The two column-name spellings in the furnace CSVs must collapse to one name."""

    @pytest.mark.parametrize(
        ("r_style", "original"),
        [
            ("Corrected.C.L", "Corrected C/L"),
            ("No..of.furnaces", "No. of furnaces"),
            ("Year.List", "Year List"),
            ("Power..kWh..metric.ton.", "Power (kWh/ metric ton)"),
        ],
    )
    def test_spellings_collapse(self, r_style: str, original: str) -> None:
        assert nz.canonical_column_name(r_style) == nz.canonical_column_name(original)

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("Corrected C/L", "corrected_c_l"),
            ("Power (kWh/ metric ton)", "power_kwh_metric_ton"),
            ("Plant ID", "plant_id"),
            ("Country/Area", "country_area"),
            ("Subnational unit (province/state)", "subnational_unit_province_state"),
            ("Nominal crude steel capacity (ttpa)", "nominal_crude_steel_capacity_ttpa"),
        ],
    )
    def test_canonical_names(self, raw: str, expected: str) -> None:
        assert nz.canonical_column_name(raw) == expected

    def test_collision_is_an_error(self) -> None:
        """Two source columns folding onto one name would silently drop data."""
        frame = pl.DataFrame({"Year List": [1], "Year.List": [2]})
        with pytest.raises(ValueError, match="canonicalise"):
            nz.canonicalize_columns(frame)


class TestAliasList:
    def test_parses_python_list_literal(self) -> None:
        raw = "['Kyoei Steel AltaSteel Ltd. Edmonton, Alta.', 'Moly-Cop Group AltaSteel Ltd. Edmonton, Alta.']"
        assert nz.parse_alias_list(raw) == [
            "Kyoei Steel AltaSteel Ltd. Edmonton, Alta.",
            "Moly-Cop Group AltaSteel Ltd. Edmonton, Alta.",
        ]

    def test_empty_and_null(self) -> None:
        assert nz.parse_alias_list(None) == []
        assert nz.parse_alias_list("") == []
        assert nz.parse_alias_list("[]") == []

    def test_unparseable_cell_is_kept_as_one_alias(self) -> None:
        """A parse failure must not lose the alias."""
        assert nz.parse_alias_list("Nucor Steel Berkeley, S.C.") == ["Nucor Steel Berkeley, S.C."]

    def test_does_not_execute_code(self) -> None:
        """`ast.literal_eval` cannot evaluate a call, so the cell falls through to text."""
        payload = "__import__('os').getcwd()"
        assert nz.parse_alias_list(payload) == [payload]


class TestCoordinates:
    def test_split(self) -> None:
        frame = pl.DataFrame(
            {"coordinates": ["36.747413, 36.217330", "-17.397866, 15.891022", "unknown", None]},
            schema={"coordinates": pl.Utf8},
        ).with_columns(nz.split_coordinates("coordinates"))
        assert frame["latitude"].to_list() == [36.747413, -17.397866, None, None]
        assert frame["longitude"].to_list() == [36.21733, 15.891022, None, None]


class TestAnnotatedFurnaceCount:
    @pytest.mark.parametrize(
        ("raw", "count", "not_melting"),
        [
            ("1", 1, False),
            ("1 (#5)", 1, False),
            ("1  (#8)", 1, False),
            ("1*", 1, False),
            ("3 (not melting)", 3, True),
            ("Not melting", None, True),
            (None, None, False),
        ],
    )
    def test_leading_int_and_flag(
        self, raw: str | None, count: int | None, not_melting: bool
    ) -> None:
        frame = pl.DataFrame({"n": [raw]}, schema={"n": pl.Utf8}).with_columns(
            nz.leading_int("n", "count"), nz.not_melting_flag("n", "not_melting")
        )
        assert frame["count"][0] == count
        assert frame["not_melting"][0] is not_melting
