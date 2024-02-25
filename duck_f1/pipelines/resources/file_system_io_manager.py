import os
from typing import Union

import pyarrow as pa
import pyarrow.parquet as pq
from dagster import InputContext, MetadataValue, OutputContext, UPathIOManager
from upath import UPath


class ArrowParquetIOManager(UPathIOManager):
    extension: str = ".parquet"

    def get_asset_relative_path(self, context: Union[InputContext, OutputContext]) -> "UPath":

        if context._step_context is None:
            return UPath(*context.asset_key.path)

        step_config = context.step_context.op_config
        if "session_key" in step_config.keys():
            return UPath(*context.asset_key.path) / step_config["session_key"]
        else:
            return UPath(*context.asset_key.path)

    def dump_to_path(self, context: OutputContext, obj: pa.Table, path: UPath):

        with path.open("wb") as file:
            pq.write_table(obj, file)

        output_metadata = {
            "table_size": obj.nbytes,
            "table_size_label": self._compute_size_label(obj.nbytes),
            "col_count": obj.num_columns,
            "row_count": obj.num_rows,
        }

        if os.getenv("ENV", "prod") == "dev":
            output_metadata["preview"] = MetadataValue.md(
                obj.slice(length=5).to_pandas().to_markdown(index=False)
            )

        context.add_output_metadata(output_metadata)

    def load_from_path(self, context: InputContext, path: UPath) -> pa.Table:
        with path.open("rb") as file:
            return pq.read_table(file)

    @staticmethod
    def _compute_size_label(length: int, decimals: int = 2) -> float:
        scales = {
            "B": 1,
            "KB": 1e-3,
            "MB": 1e-6,
            "GB": 1e-9,
            "TB": 1e-12,
        }
        for label, scale in scales.items():
            if length * scale < 1000:
                value = round(length * scale, decimals)
                return f"{value} {label}"
