from dagster import load_assets_from_package_module

from . import duckdb, ergast, live_timing

all_assets = [
    *load_assets_from_package_module(ergast),
    *load_assets_from_package_module(duckdb),
    *live_timing.live_timing_assets,
    *duckdb.ergast.ergast_tables(),
    *duckdb.live_timing.living_timing_tables(),
]
