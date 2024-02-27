import yaml
from dagster import AssetExecutionContext, SourceAsset, asset
from dagster_duckdb import DuckDBResource

from ...resources import FileSystemResource

with open("./config/ergast.yaml", "r", encoding="UTF-8") as stream:
    CONFIG = yaml.safe_load(stream)


def ergast_tables():
    def table_factory(table):
        @asset(
            name=table,
            group_name="duckdb",
            deps=[SourceAsset(["ergast", table]), SourceAsset(["duckdb", "migrations"])],
            key_prefix=["duckdb", "ingress", "ergast"],
            compute_kind="duckdb",
        )
        def duck_db_asset(
            context: AssetExecutionContext, duckdb: DuckDBResource, fs_config: FileSystemResource
        ) -> None:

            with duckdb.get_connection() as conn:
                conn.execute(
                    f"""
                    CREATE OR REPLACE TABLE ingress.ergast__{table} AS
                    SELECT * FROM read_parquet('{fs_config.output_path}/ergast/{table}.parquet');
                    """
                )

        return duck_db_asset

    out = []
    for t in CONFIG["tables"]:
        out.append(table_factory(t["table"]))

    return out
