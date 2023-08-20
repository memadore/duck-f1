import base64
import io
import json
import urllib.request
import zlib
from functools import partial
from typing import Callable, List
from urllib.error import HTTPError

import pyarrow as pa
from dagster import OpExecutionContext, asset

from .partitions import LiveTimingDataset, LiveTimingPartitionManager
from .processors import LiveTimingProcessorBuilder


class LiveTimingApi:
    BASE_URL = "https://livetiming.formula1.com"

    def __init__(self, context: OpExecutionContext) -> None:
        self.context = context

    @staticmethod
    def _json_processor(buffer: io.BytesIO) -> List[dict]:
        raw_txt = buffer.read().decode("utf-8-sig")
        out = "[" + raw_txt + "]"
        return json.loads(out)

    @staticmethod
    def _zlib_decompress(data: str) -> str:
        data = data[12:].strip('"')
        data = base64.b64decode(data)
        data = zlib.decompress(data, -zlib.MAX_WBITS)
        return data.decode("utf-8-sig")[1:]  # remove the first {

    @staticmethod
    def _json_stream_processor(
        buffer: io.BytesIO, data_post_process: Callable = None
    ) -> List[dict]:
        raw_txt = buffer.read().decode("utf-8-sig")
        txt = []
        for line in raw_txt.splitlines():
            ts = f'"ts":"{line[:12]}"'  # 00.00.00:000
            if not data_post_process:
                data = line[13:]  # remove 00.00.00:000{
            else:
                data = data_post_process(line)
            record = f"{{{ts}, {data}"
            txt.append(record)

        out = "[" + ",".join(txt) + "]"
        return json.loads(out)

    def _file_processor_builder(self, file: str) -> callable:
        processor = {
            "json": self._json_processor,
            "jsonStream": self._json_stream_processor,
            "z.jsonStream": partial(
                self._json_stream_processor, data_post_process=self._zlib_decompress
            ),
        }
        file_parts = file.split(".")
        file_encoding = ".".join(file_parts[1:])
        return processor[file_encoding]

    def _api_request(self, path: str) -> io.BytesIO:
        url = "/".join([self.BASE_URL, path])

        self.context.log.info("Making request to: %s", url)
        try:
            with urllib.request.urlopen(url) as response:
                stream = io.BytesIO(response.read())
                return stream
        except HTTPError:
            self.context.log.warn("File not found")
            return None

    def get_dataset(self, event_key: str, dataset: str) -> dict:
        path = "/".join(["static", event_key, dataset])
        response = self._api_request(path)
        file_processor = self._file_processor_builder(dataset)
        return file_processor(response)


def live_timing_files(
    partition_manager: LiveTimingPartitionManager, datasets: List[LiveTimingDataset]
):
    def parquet_file_factory(dataset: LiveTimingDataset):
        @asset(
            name=dataset.table,
            group_name="live_timing",
            key_prefix=["live_timing"],
            compute_kind="python",
            io_manager_key="pyarrow_parquet_io_manager",
            partitions_def=partition_manager.dagster_partitions,
        )
        def live_timing_asset(context: OpExecutionContext) -> pa.Table:
            processor_builder = LiveTimingProcessorBuilder(context)
            api_client = LiveTimingApi(context)

            partition = partition_manager.get_partition(context.partition_key)
            processor = processor_builder.build(dataset.table, partition.metadata)
            data = api_client.get_dataset(partition.event_key, dataset.file)

            return processor.run(data)

        return live_timing_asset

    out = []
    for dataset in datasets:
        if dataset.table == "weather_data":
            out.append(parquet_file_factory(dataset))

    return out