from dagster import load_assets_from_package_module

from . import duckdb, live_timing
from .ergast import ErgastAssetsManager

ergast_assets_manager = ErgastAssetsManager()

all_assets = [
    *ergast_assets_manager.get_parquet_assets(),
    *ergast_assets_manager.get_duckdb_assets(),
    *load_assets_from_package_module(duckdb),
    *live_timing.live_timing_assets,
    *duckdb.live_timing.living_timing_tables(),
]
