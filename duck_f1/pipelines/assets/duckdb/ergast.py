import os

import yaml
from dagster import OpExecutionContext, SourceAsset, asset
from dagster_duckdb import DuckDBResource

with open("./config/ergast.yaml", "r", encoding="UTF-8") as stream:
    CONFIG = yaml.safe_load(stream)


@asset(
    group_name="duckdb",
    compute_kind="duckdb",
    key_prefix=["duckdb", "ergast"],
)
def schema(duckdb: DuckDBResource) -> None:
    with duckdb.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS ergast;")


def ergast_tables():
    def table_factory(table):
        @asset(
            name=table,
            group_name="duckdb",
            deps=[SourceAsset(["ergast", table]), schema],
            key_prefix=["duckdb", "ergast"],
            compute_kind="duckdb",
        )
        def duck_db_asset(context: OpExecutionContext, duckdb: DuckDBResource) -> None:
            OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./data")

            with duckdb.get_connection() as conn:
                conn.execute(
                    f"""
                    CREATE OR REPLACE TABLE ergast.{table} AS
                    SELECT * FROM read_parquet('{OUTPUT_DIR}/ergast/{table}.parquet')
                    """
                )

        return duck_db_asset

    out = []
    for t in CONFIG["tables"]:
        out.append(table_factory(t["table"]))

    return out
