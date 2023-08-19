import os

from dagster import OpExecutionContext, SourceAsset, asset
from dagster_duckdb import DuckDBResource

from ..live_timing import config_manager


@asset(
    group_name="duckdb",
    compute_kind="duckdb",
)
def living_timing_schema(duckdb: DuckDBResource) -> None:
    with duckdb.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS live_timing;")


def living_timing_tables():
    def table_factory(table):
        @asset(
            name=table,
            group_name="duckdb",
            deps=[SourceAsset(["live_timing", table]), living_timing_schema],
            key_prefix=["duck-db", "live_timing"],
            compute_kind="duckdb",
        )
        def duck_db_asset(context: OpExecutionContext, duckdb: DuckDBResource) -> None:
            OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./data")

            with duckdb.get_connection() as conn:
                conn.execute(
                    f"""
                    CREATE OR REPLACE TABLE live_timing.{table} AS
                    SELECT * FROM read_parquet('{OUTPUT_DIR}/live_timing/{table}/**/*.parquet')
                    """
                )

        return duck_db_asset

    out = []
    for t in config_manager.datasets:
        out.append(table_factory(t.table))

    return out
