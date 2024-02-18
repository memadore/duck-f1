import os

from dagster_dbt import DbtCliResource
from dagster_duckdb import DuckDBResource
from upath import UPath

from ..constants import DBT_PROJECT_DIR
from ..resources.file_system_io_manager import ArrowParquetIOManager

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./data")

RESOURCES = {
    "dbt": DbtCliResource(project_dir=os.fspath(DBT_PROJECT_DIR)),
    "duckdb": DuckDBResource(database=f"{OUTPUT_DIR}/f1.duckdb"),
    "pyarrow_parquet_io_manager": ArrowParquetIOManager(base_path=UPath(OUTPUT_DIR)),
}
