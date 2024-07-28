from typing import List

import pyarrow as pa
from dagster import AssetExecutionContext, PartitionsDefinition, SourceAsset, asset

from ...resources import DuckDBResource, FileSystemResource
from .config import LiveTimingEvent


def duckdb_parquet_asset_factory(
    asset_name: str,
    session_partitions: PartitionsDefinition,
):
    @asset(
        name=asset_name,
        group_name="duckdb",
        deps=[
            SourceAsset(["live_timing", asset_name]),
            SourceAsset(["duckdb", "migrations"]),
        ],
        key_prefix=["duckdb", "ingress", "live_timing"],
        compute_kind="duckdb",
        partitions_def=session_partitions,
    )
    def _duck_db_asset(
        context: AssetExecutionContext,
        duckdb: DuckDBResource,
        fs_config: FileSystemResource,
    ) -> None:
        session = context.partition_key
        filename = f"{fs_config.output_path}/live_timing/{asset_name}/{session}.parquet"

        with duckdb.get_connection() as conn:
            db_tables = conn.sql("USE ingress; SHOW TABLES;")
            if db_tables is None:
                db_tables = []
            else:
                db_tables = ["".join(i) for i in db_tables.fetchall()]

            if f"live_timing__{asset_name}" in db_tables:
                context.log.info("Table exists. Appending new data.")
                conn.execute(
                    f"""
                        DELETE FROM ingress.live_timing__{asset_name}
                            WHERE filename='{filename}';
                        INSERT INTO ingress.live_timing__{asset_name}
                        SELECT * FROM read_parquet('{filename}', filename=true);
                        """
                )
            else:
                context.log.info(
                    "Table not found. Creating new table and inserting data."
                )
                conn.execute(
                    f"""
                        CREATE TABLE ingress.live_timing__{asset_name} AS
                        SELECT * FROM read_parquet('{filename}', filename=true);
                        """
                )

    return _duck_db_asset


def duckdb_live_timing_sessions(events: List[LiveTimingEvent]):
    @asset(
        name="sessions",
        group_name="duckdb",
        compute_kind="duckdb",
        deps=[SourceAsset(["duckdb", "migrations"])],
        key_prefix=["duckdb", "ingress", "live_timing"],
    )
    def _sessions(duckdb: DuckDBResource) -> None:
        out = []
        for event in events:
            for session in event.sessions:
                event_data = {f"event_{k}": v for k, v in event.model_dump().items()}
                session_data = {
                    f"session_{k}": v for k, v in session.model_dump().items()
                }
                out.append({**event_data, **session_data})

        table = pa.Table.from_pylist(out)

        with duckdb.get_connection() as conn:
            conn.register("tmp_table", table)
            conn.execute(
                """
                        CREATE OR REPLACE TABLE ingress.live_timing__sessions AS
                        SELECT * FROM tmp_table;
                """
            )

    return _sessions
