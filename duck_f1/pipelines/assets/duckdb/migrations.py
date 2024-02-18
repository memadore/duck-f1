from dagster import asset
from dagster_duckdb import DuckDBResource


@asset(
    group_name="duckdb",
    compute_kind="duckdb",
    key_prefix=["duckdb"],
)
def migrations(duckdb: DuckDBResource) -> None:
    with duckdb.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS ingress;")
