import base64
import io
import json
import zlib
from functools import partial
from typing import Callable, List, Union

import pyarrow as pa
import requests
from dagster import OpExecutionContext, multi_asset

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
        out = out.replace('"ts"', '"_StreamTimestamp"')
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
        out = out.replace('"ts"', '"_StreamTimestamp"')
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

        response = requests.get(url, timeout=10)

        if response.status_code != 200:
            self.context.log.warn("File not found")
            return None

        stream = io.BytesIO(response.content)
        return stream

    def get_dataset(self, event_path: str, dataset: str) -> Union[dict, None]:
        path = "/".join(["static", event_path, dataset])
        file_processor = self._file_processor_builder(dataset)
        response = self._api_request(path)

        if response is None:
            return

        return file_processor(response)


def live_timing_files(
    partition_manager: LiveTimingPartitionManager, datasets: List[LiveTimingDataset]
):
    processor_builder = LiveTimingProcessorBuilder()

    def parquet_file_factory(dataset: LiveTimingDataset, output_assets):
        @multi_asset(
            outs=output_assets,
            name=dataset.table,
            group_name="live_timing",
            compute_kind="python",
            can_subset=True,
            partitions_def=partition_manager.dagster_partitions,
        )
        def live_timing_asset(context: OpExecutionContext) -> pa.Table:
            api_client = LiveTimingApi(context)

            partition = partition_manager.get_partition(context.partition_key)
            processor = processor_builder.build(dataset.table, partition.metadata, context)

            data = api_client.get_dataset(partition.event_path, dataset.file)
            assets = processor.run(data)

            for i in assets:
                yield i

        return live_timing_asset

    out = []
    for dataset in datasets:
        if dataset.table in processor_builder.processors:
            asset_config = {
                "key_prefix": ["live_timing"],
                "is_required": False,
                "io_manager_key": "pyarrow_parquet_io_manager",
            }

            output_assets = processor_builder.assets_definition(dataset.table, **asset_config)
            out.append(parquet_file_factory(dataset, output_assets))

    return out
