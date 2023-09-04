import os

from dagster import OpExecutionContext, SourceAsset, asset
from dagster_duckdb import DuckDBResource

from ..live_timing.processors import LiveTimingProcessorBuilder


@asset(
    group_name="duckdb",
    compute_kind="duckdb",
    key_prefix=["duckdb", "live_timing"],
)
def schema(duckdb: DuckDBResource) -> None:
    with duckdb.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS live_timing;")


def living_timing_tables():
    processor_builder = LiveTimingProcessorBuilder()

    def table_factory(table):
        @asset(
            name=table,
            group_name="duckdb",
            deps=[SourceAsset(["live_timing", table]), schema],
            key_prefix=["duckdb", "live_timing"],
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

    for i in processor_builder.assets:
        out.append(table_factory(i))

    return out
