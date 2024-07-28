import os
from pathlib import Path

from dagster import ConfigurableResource
from dagster_dbt import DbtCliResource
from dagster_duckdb import DuckDBResource
from upath import UPath

from ..constants import DBT_PROJECT_DIR
from ..resources.file_system_io_manager import ArrowParquetIOManager


class FileSystemResource(ConfigurableResource):
    output_dir: str = "data"
    db_name: str = "f1"

    @property
    def output_path(self) -> str:
        return str(Path(self.output_dir).resolve())

    @property
    def db_path(self) -> str:
        return str((Path(self.output_dir) / (self.db_name + ".duckdb")).resolve())


def init_resources(fs_config: FileSystemResource) -> dict:
    return {
        "dbt": DbtCliResource(
            project_dir=os.fspath(DBT_PROJECT_DIR),
            global_config_flags=["--quiet"],
        ),
        "duckdb": DuckDBResource(database=fs_config.db_path),
        "fs_config": fs_config,
        "pyarrow_parquet_io_manager": ArrowParquetIOManager(
            base_path=UPath(fs_config.output_path)
        ),
    }
