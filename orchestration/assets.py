"""The pipeline as Dagster assets.

[no DataFrame library at module level] The asset bodies call into the layers that own
their DataFrames; nothing here imports Polars or pandas itself.

The shape mirrors the phases: bronze ingestion -> geographic enrichment -> entity
resolution -> the DuckDB bootstrap -> dbt. `bootstrap` is a real asset rather than a
resource because it does real work and can really fail: Iceberg metadata paths move on
every load, so the views must be rebuilt, and when that breaks it should surface as a
failed node with a position in the lineage instead of an exception during resource
setup that nobody can locate.
"""

# NOTE: no `from __future__ import annotations` in this module. Dagster inspects the
# `context` parameter's annotation at decoration time; with PEP 563 the annotation is a
# string and Dagster rejects it with "Cannot annotate `context` parameter with type
# AssetExecutionContext". Every other module in the project uses the future import.

from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetOut,
    MetadataValue,
    Output,
    StaticPartitionsDefinition,
    asset,
    multi_asset,
)
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets

from orchestration.resources import (
    DBT_PROFILES_DIR,
    DBT_PROJECT_DIR,
    SOURCE_VERSIONS,
    LakeConfig,
)

# Only the furnace census actually ships in two vintages, so it is the only asset with a
# meaningful partition. Partitioning the others by a dimension they do not have would be
# decoration.
source_version_partitions = StaticPartitionsDefinition(list(SOURCE_VERSIONS))

BRONZE_GROUP = "bronze"


def _lake_kwargs(config: LakeConfig) -> "dict[str, Path]":
    return {"catalog_db": Path(config.catalog_db), "warehouse": Path(config.warehouse)}


# --------------------------------------------------------------------------------------
# bronze
# --------------------------------------------------------------------------------------


@asset(group_name=BRONZE_GROUP, compute_kind="dlt", key=AssetKey(["raw", "plantgen"]))
def plantgen(context: AssetExecutionContext, lake: LakeConfig) -> None:
    from ingest.pipeline import run

    result = run(tables=("plantgen",), **_lake_kwargs(lake))[0]
    context.add_output_metadata(
        {"rows": result.rows, "metadata_location": MetadataValue.path(result.metadata_location)}
    )


@asset(group_name=BRONZE_GROUP, compute_kind="dlt", key=AssetKey(["raw", "gspt_plants"]))
def gspt_plants(context: AssetExecutionContext, lake: LakeConfig) -> None:
    from ingest.pipeline import run

    result = run(tables=("gspt_plants",), **_lake_kwargs(lake))[0]
    context.add_output_metadata({"rows": result.rows})


@asset(group_name=BRONZE_GROUP, compute_kind="dlt", key=AssetKey(["raw", "gspt_production"]))
def gspt_production(context: AssetExecutionContext, lake: LakeConfig) -> None:
    from ingest.pipeline import run

    result = run(tables=("gspt_production",), **_lake_kwargs(lake))[0]
    context.add_output_metadata({"rows": result.rows})


@asset(
    group_name=BRONZE_GROUP,
    compute_kind="dlt",
    key=AssetKey(["raw", "furnace_years"]),
    partitions_def=source_version_partitions,
)
def furnace_years(context: AssetExecutionContext, lake: LakeConfig) -> None:
    """Both vintages of the furnace census, one partition each.

    The loader writes both in one pass -- they have to be read together, because the
    intact facility names in `gspt_2024` are joined onto the owner-bearing rows of
    `owner_filled_2025` (ADR-003). The partition therefore records which vintage a run
    is *about*, and the metadata reports that vintage's own row count.
    """
    from ingest.pipeline import run

    result = run(tables=("furnace_years",), **_lake_kwargs(lake))[0]

    from lakehouse.duck import connect

    con = connect(Path(lake.catalog_db), Path(lake.warehouse))
    try:
        partition = context.partition_key
        rows = con.execute(
            "select count(*) from raw.furnace_years where source_version = ?", [partition]
        ).fetchone()[0]
    finally:
        con.close()
    context.add_output_metadata(
        {"rows_total": result.rows, "rows_in_partition": rows, "source_version": partition}
    )


# --------------------------------------------------------------------------------------
# enrichment and resolution
# --------------------------------------------------------------------------------------


@asset(
    group_name="geo",
    compute_kind="geopandas",
    key=AssetKey(["geo", "gspt_plant_county"]),
    deps=[AssetKey(["raw", "gspt_plants"]), AssetKey(["raw", "plantgen"])],
)
def gspt_plant_county(context: AssetExecutionContext, lake: LakeConfig) -> None:
    from geo.enrich import build

    _, report = build(db_path=Path(lake.catalog_db), warehouse=Path(lake.warehouse), write=True)
    context.add_output_metadata(
        {
            "plants": report.plants_total,
            "with_coordinates": report.plants_with_coordinates,
            "county_assigned": report.county_assigned,
            "matched_plantgen_county": report.matched_plantgen_county,
            "report": MetadataValue.md(f"```\n{report.render()}\n```"),
        }
    )


@asset(
    group_name="resolution",
    compute_kind="splink",
    key=AssetKey(["resolution", "gspt_plantgen_candidates"]),
    deps=[AssetKey(["geo", "gspt_plant_county"]), AssetKey(["raw", "plantgen"])],
)
def gspt_plantgen_candidates(context: AssetExecutionContext, lake: LakeConfig) -> None:
    import logging

    logging.getLogger("splink").setLevel(logging.ERROR)
    from resolution.report import build

    _, report = build(db_path=Path(lake.catalog_db), warehouse=Path(lake.warehouse), write=True)
    context.add_output_metadata(
        {
            "candidate_pairs": report.blocking.blocked_pairs,
            "fallback_pairs": report.blocking.fallback_pairs,
            "reduction_ratio": report.blocking.reduction_ratio,
            "unestimated_levels": len(report.comparators.unestimated),
            "report": MetadataValue.md(f"```\n{report.render()}\n```"),
        }
    )


# --------------------------------------------------------------------------------------
# dbt
# --------------------------------------------------------------------------------------


# The lake tables dbt reads, and the view key each one is exposed under. Declared
# rather than discovered so that the asset graph is static and a table that silently
# stopped being loaded shows up as a failed asset instead of a missing node.
LAKE_TABLES = (
    ("raw", "plantgen"),
    ("raw", "gspt_plants"),
    ("raw", "gspt_production"),
    ("raw", "furnace_years"),
    ("geo", "gspt_plant_county"),
    ("resolution", "gspt_plantgen_candidates"),
)


def view_key(namespace: str, table: str) -> AssetKey:
    return AssetKey(["views", namespace, table])


@multi_asset(
    name="duckdb_lake_views",
    group_name="transform",
    compute_kind="duckdb",
    outs={
        f"{namespace}_{table}": AssetOut(key=view_key(namespace, table), is_required=False)
        for namespace, table in LAKE_TABLES
    },
    can_subset=False,
    deps=[
        AssetKey(["raw", "plantgen"]),
        AssetKey(["raw", "gspt_plants"]),
        AssetKey(["raw", "gspt_production"]),
        AssetKey(["raw", "furnace_years"]),
        AssetKey(["geo", "gspt_plant_county"]),
        AssetKey(["resolution", "gspt_plantgen_candidates"]),
    ],
)
def duckdb_lake_views(context: AssetExecutionContext, lake: LakeConfig):
    """Rebuild the DuckDB views dbt reads the Iceberg lake through.

    An asset, not a resource. Iceberg metadata locations change on every load, so this
    does real work every run, and when it fails -- an empty catalog, a table that was
    never loaded -- the failure belongs somewhere visible in the graph rather than in a
    resource initialiser nobody can locate.

    One output per view, because dbt's sources map onto them one for one: that keeps the
    lineage `bronze -> view -> dbt model` instead of pretending dbt reads Iceberg
    directly, which it does not.
    """
    from transform.bootstrap import bootstrap

    created = bootstrap(catalog_db=Path(lake.catalog_db), warehouse=Path(lake.warehouse))
    for namespace, table in LAKE_TABLES:
        qualified = f"{namespace}.{table}"
        if qualified not in created:
            raise RuntimeError(f"{qualified} is not in the catalog; was it ever loaded?")
        yield Output(
            value=None,
            output_name=f"{namespace}_{table}",
            metadata={"metadata_location": MetadataValue.path(created[qualified])},
        )


class ViewBackedTranslator(DagsterDbtTranslator):
    """Point every dbt source at the bootstrap asset instead of at the Iceberg table.

    By default dagster-dbt maps a dbt source `raw.plantgen` to the asset key
    `["raw", "plantgen"]`, which happens to be exactly the bronze asset's key -- so the
    lineage would read "bronze -> dbt" and dbt would be free to run alongside the
    bootstrap that creates the views it needs.

    That lineage is also untrue. dbt does not read the Iceberg tables; it reads DuckDB
    views over them, which `duckdb_lake_views` rebuilds on every run because Iceberg
    metadata locations move. Mapping each source onto its own view asset gives
    `bronze -> view -> dbt model`, which both orders the run correctly and says what
    actually happens.
    """

    def get_asset_key(self, dbt_resource_props):
        if dbt_resource_props["resource_type"] == "source":
            return view_key(dbt_resource_props["source_name"], dbt_resource_props["name"])
        return super().get_asset_key(dbt_resource_props)


@dbt_assets(
    manifest=DBT_PROJECT_DIR / "target" / "manifest.json",
    dagster_dbt_translator=ViewBackedTranslator(),
)
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """Every dbt model and every dbt test, surfaced as Dagster assets and checks.

    The 48 dbt assertions are NOT re-implemented as Dagster asset checks. Two copies of
    the same assertion drift, and then nobody knows which one is authoritative. Dagster
    runs dbt and reports what dbt found; the Dagster-native checks below cover only the
    invariants dbt cannot see, because they span assets dbt does not own.
    """
    yield from dbt.cli(["build"], context=context).stream()


def dbt_resource() -> DbtCliResource:
    return DbtCliResource(
        project_dir=str(DBT_PROJECT_DIR),
        profiles_dir=str(DBT_PROFILES_DIR),
        target="dev",
    )
