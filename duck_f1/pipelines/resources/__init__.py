import os

from dagster_duckdb import DuckDBResource
from upath import UPath

from ..resources.file_system_io_manager import ArrowParquetIOManager

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./data")

RESOURCES = {
    "pyarrow_parquet_io_manager": ArrowParquetIOManager(base_path=UPath(OUTPUT_DIR)),
    "duckdb": DuckDBResource(database=f"{OUTPUT_DIR}/f1.duckdb"),
}
