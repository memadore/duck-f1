import pyarrow as pa
from dagster import AssetExecutionContext, SourceAsset, asset
from dagster_duckdb import DuckDBResource

from ...resources import FileSystemResource
from ..live_timing.live_timing import LiveTimingConfig
from ..live_timing.processors import LiveTimingProcessorBuilder
from ..live_timing.sessions import LiveTimingConfigManager


@asset(
    group_name="duckdb",
    compute_kind="duckdb",
    deps=[SourceAsset(["duckdb", "migrations"])],
    key_prefix=["duckdb", "ingress", "live_timing"],
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
                    CREATE OR REPLACE TABLE ingress.live_timing__sessions AS
                    SELECT * FROM tmp_table;
            """
        )


def living_timing_tables():
    processor_builder = LiveTimingProcessorBuilder()

    def table_factory(table):
        @asset(
            name=table,
            group_name="duckdb",
            deps=[SourceAsset(["live_timing", table]), SourceAsset(["duckdb", "migrations"])],
            key_prefix=["duckdb", "ingress", "live_timing"],
            compute_kind="duckdb",
        )
        def duck_db_asset(
            context: AssetExecutionContext,
            duckdb: DuckDBResource,
            fs_config: FileSystemResource,
            config: LiveTimingConfig,
        ) -> None:
            pk = config.session_key
            filename = f"{fs_config.output_path}/live_timing/{table}/{pk}.parquet"

            with duckdb.get_connection() as conn:
                db_tables = conn.sql("USE ingress; SHOW TABLES;")
                if db_tables is None:
                    db_tables = []
                else:
                    db_tables = ["".join(i) for i in db_tables.fetchall()]

                if f"live_timing__{table}" in db_tables:
                    context.log.info("Table exists. Appending new data.")
                    conn.execute(
                        f"""
                        DELETE FROM ingress.live_timing__{table}
                            WHERE filename='{filename}';
                        INSERT INTO ingress.live_timing__{table}
                        SELECT * FROM read_parquet('{filename}', filename=true);
                        """
                    )
                else:
                    context.log.info("Table not found. Creating new table and inserting data.")
                    conn.execute(
                        f"""
                        CREATE TABLE ingress.live_timing__{table} AS
                        SELECT * FROM read_parquet('{filename}', filename=true);
                        """
                    )

        return duck_db_asset

    out = []

    for i in processor_builder.assets:
        out.append(table_factory(i))

    return out
