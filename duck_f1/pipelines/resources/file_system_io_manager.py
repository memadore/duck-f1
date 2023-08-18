import pyarrow as pa
import pyarrow.parquet as pq
from dagster import InputContext, OutputContext, UPathIOManager
from upath import UPath


class ArrowParquetIOManager(UPathIOManager):
    extension: str = ".parquet"

    def dump_to_path(self, context: OutputContext, obj: pa.Table, path: UPath):
        with path.open("wb") as file:
            pq.write_table(obj, file)

    def load_from_path(self, context: InputContext, path: UPath) -> pa.Table:
        with path.open("rb") as file:
            return pq.read_table(file)
