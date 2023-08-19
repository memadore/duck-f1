import io
from abc import ABC, abstractmethod
from datetime import datetime

import pyarrow as pa
from dagster import OpExecutionContext

from .partitions import LiveTimingPartitionMetadata


class AbstractLiveTimingProcessor(ABC):
    def __init__(self, context: OpExecutionContext, metadata: LiveTimingPartitionMetadata) -> None:
        self.context = context
        self.metadata = metadata

    @abstractmethod
    def _processor(self, data: io.BytesIO) -> pa.Table:
        pass

    def _add_metadata(self, table: pa.Table) -> pa.Table:
        columns = self.metadata.dict()
        self.context.log.info("Metadata: %s", columns)
        table_len = table.num_rows
        for col, value in columns.items():
            match value:
                case int():
                    column_type = pa.int32()
                    column_value = value
                case datetime():
                    column_type = pa.timestamp("s")
                    column_value = value
                case _:
                    column_type = pa.string()
                    column_value = str(value)

            table = table.append_column(col, pa.array([column_value] * table_len, column_type))

        return table

    def run(self, data: io.BytesIO) -> pa.Table:
        table = self._processor(data)
        table = self._add_metadata(table)
        return table


class WeatherDataProcessor(AbstractLiveTimingProcessor):
    def _processor(self, data: dict) -> pa.Table:
        schema = pa.schema(
            [
                ("ts", pa.string()),
                ("AirTemp", pa.decimal128(5, 2)),
                ("Humidity", pa.decimal128(5, 2)),
                ("Pressure", pa.decimal128(6, 2)),
                ("Rainfall", pa.decimal128(5, 2)),
                ("TrackTemp", pa.decimal128(5, 2)),
                ("WindDirection", pa.int16()),
                ("WindSpeed", pa.decimal128(5, 2)),
            ]
        )

        table = pa.Table.from_pylist(data).cast(schema)
        return table


class LiveTimingProcessorBuilder:
    def __init__(self, context: OpExecutionContext):
        self.context = context

    def build(
        self, table: str, metadata: LiveTimingPartitionMetadata
    ) -> AbstractLiveTimingProcessor:
        processors = {"weather_data": WeatherDataProcessor}
        processor = processors.get(table, None)
        return processor(self.context, metadata)
