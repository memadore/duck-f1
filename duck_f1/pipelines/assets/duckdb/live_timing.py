import os

import pyarrow as pa
from dagster import OpExecutionContext, SourceAsset, asset
from dagster_duckdb import DuckDBResource

from ..live_timing import partition_manager
from ..live_timing.partitions import LiveTimingConfigManager
from ..live_timing.processors import LiveTimingProcessorBuilder


@asset(
    group_name="duckdb",
    compute_kind="duckdb",
    key_prefix=["duckdb", "live_timing"],
)
def schema(duckdb: DuckDBResource) -> None:
    with duckdb.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS live_timing;")


@asset(
    group_name="duckdb",
    compute_kind="duckdb",
    deps=[schema],
    key_prefix=["duckdb", "live_timing"],
)
def sessions(duckdb: DuckDBResource) -> None:
    config_manager = LiveTimingConfigManager("./config/live_timing.yaml")
    events = config_manager.events
    out = []
    for event in events:
        for session in event.sessions:
            event_data = {f"event_{k}": v for k, v in event.dict().items()}
            session_data = {f"session_{k}": v for k, v in session.dict().items()}
            out.append({**event_data, **session_data})

    table = pa.Table.from_pylist(out)

    with duckdb.get_connection() as conn:
        conn.register("tmp_table", table)
        conn.execute(
            """
                    CREATE OR REPLACE TABLE live_timing.sessions AS
                    SELECT * FROM tmp_table
            """
        )


def living_timing_tables():
    processor_builder = LiveTimingProcessorBuilder()

    def table_factory(table):
        @asset(
            name=table,
            group_name="duckdb",
            deps=[SourceAsset(["live_timing", table]), schema],
            key_prefix=["duckdb", "live_timing"],
            compute_kind="duckdb",
            partitions_def=partition_manager.dagster_partitions,
            op_tags={"backend": "duckdb"},
        )
        def duck_db_asset(context: OpExecutionContext, duckdb: DuckDBResource) -> None:
            OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./data")
            pk = context.partition_key
            filename = f"{OUTPUT_DIR}/live_timing/{table}/{pk}.parquet"

            with duckdb.get_connection() as conn:
                db_tables = conn.sql("USE live_timing; SHOW TABLES;")
                if db_tables is None:
                    db_tables = []
                else:
                    db_tables = ["".join(i) for i in db_tables.fetchall()]

                context.log.info(db_tables)

                if f"{table}" in db_tables:
                    context.log.info("Table exists. Appending new data.")
                    conn.execute(
                        f"""
                        DELETE FROM live_timing.{table} WHERE filename='{filename}';
                        INSERT INTO live_timing.{table}
                        SELECT * FROM read_parquet('{filename}', filename=true)
                        """
                    )
                else:
                    context.log.info("Table not found. Creating new table and inserting data.")
                    conn.execute(
                        f"""
                        CREATE TABLE live_timing.{table} AS
                        SELECT * FROM read_parquet('{filename}', filename=true)
                        """
                    )

        return duck_db_asset

    out = []

    for i in processor_builder.assets:
        out.append(table_factory(i))

    return out
