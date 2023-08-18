from dagster import load_assets_from_package_module

from . import duckdb, ergast

all_assets = [
    *load_assets_from_package_module(ergast),
    *load_assets_from_package_module(duckdb),
    *duckdb.duckdb.ergast_tables(),
]
