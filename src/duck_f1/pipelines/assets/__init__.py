from dagster import load_assets_from_package_module

from . import duckdb
from .ergast import ErgastAssetsManager
from .live_timing import LiveTimingAssetManager

ergast_assets_manager = ErgastAssetsManager()
live_timing_asset_manager = LiveTimingAssetManager()

all_assets = [
    *ergast_assets_manager.create_parquet_assets(),
    *ergast_assets_manager.create_duckdb_assets(),
    *live_timing_asset_manager.create_parquet_assets(),
    *live_timing_asset_manager.create_duckdb_assets(),
    *load_assets_from_package_module(duckdb),
]
