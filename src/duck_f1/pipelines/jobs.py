from dagster import AssetSelection, define_asset_job
from dagster_dbt import build_dbt_asset_selection

from .assets.duckdb.dbt import duckdb_dbt_assets

dbt_build_job = define_asset_job(
    name="dbt_build",
    description="Run dbt build command",
    selection=build_dbt_asset_selection([duckdb_dbt_assets]).downstream(),
)

ergast_job = define_asset_job(
    name="ergast",
    selection=AssetSelection.key_prefixes(["duckdb", "ingress", "ergast"]).upstream(),
)

live_timing_job = define_asset_job(
    name="live_timing",
    selection=AssetSelection.key_prefixes(
        ["duckdb", "ingress", "live_timing"]
    ).upstream(),
)
