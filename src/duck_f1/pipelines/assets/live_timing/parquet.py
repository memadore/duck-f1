import json
import os
from typing import Dict

from dagster import AssetExecutionContext, AssetOut, PartitionsDefinition, multi_asset
from pyarrow import Table

from .api import LiveTimingApi
from .config import LiveTimingDataset
from .processors import LiveTimingProcessorBuilder


def parquet_asset_factory(
    multi_asset_outs: Dict[str, AssetOut],
    dataset: LiveTimingDataset,
    processor_builder: LiveTimingProcessorBuilder,
    session_partitions: PartitionsDefinition,
    session_mapper: callable,
):
    @multi_asset(
        outs=multi_asset_outs,
        name=dataset.table,
        group_name="live_timing",
        compute_kind="python",
        can_subset=True,
        partitions_def=session_partitions,
    )
    def live_timing_asset(context: AssetExecutionContext) -> Table:
        api_client = LiveTimingApi(context)
        session = session_mapper(context.partition_key)
        processor = processor_builder.build(dataset.table, session.metadata, context)
        data = api_client.get_dataset(session.event_path, dataset.file)

        try:
            assets = processor.run(data)
        except Exception as e:
            context.log.error(e)

            if os.getenv("dev", "prod") != "dev":
                path = f"logs/{context.partition_key}"
                os.makedirs(path, exist_ok=True)
                file_path = f"{path}/{dataset.table}.json"
                with open(file_path, "w") as f:
                    f.write(json.dumps(data))

        for i in assets:
            yield i

    return live_timing_asset
