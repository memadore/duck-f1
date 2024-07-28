from dagster import AssetExecutionContext, SourceAsset, asset
from dagster_duckdb import DuckDBResource

from ...resources import FileSystemResource
from .config import ErgastAsset


def duckdb_parquet_asset_factory(ergast_asset: ErgastAsset):
    @asset(
        name=ergast_asset.asset_name,
        group_name="duckdb",
        deps=[
            SourceAsset(["ergast", ergast_asset.asset_name]),
            SourceAsset(["duckdb", "migrations"]),
        ],
        key_prefix=["duckdb", "ingress", "ergast"],
        compute_kind="duckdb",
    )
    def _duck_db_asset(
        context: AssetExecutionContext,
        duckdb: DuckDBResource,
        fs_config: FileSystemResource,
    ) -> None:
        table = ergast_asset.asset_name
        base_path = fs_config.output_path

        with duckdb.get_connection() as conn:
            conn.execute(
                f"""
                    CREATE OR REPLACE TABLE ingress.ergast__{table} AS
                    SELECT * FROM read_parquet('{base_path}/ergast/{table}.parquet');
                    """
            )

    return _duck_db_asset
