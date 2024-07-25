import json
from unittest.mock import MagicMock

from duck_f1.pipelines.assets.live_timing import config_manager, partition_manager
from duck_f1.pipelines.assets.live_timing.live_timing import LiveTimingApi

api_client = LiveTimingApi(MagicMock())
partitions = partition_manager.partitions
partition = partition_manager.get_partition("2020/11/29/race")  # Baraihn

for dataset in config_manager.datasets:
    print(partition)
    data = api_client.get_dataset(partition.event_path, dataset.file)

    if data is None:
        continue

    with open(f"./dist/json/live_timing_{dataset.table}_full.json", "w", encoding="utf-8") as file:
        # index = min(len(data), 5)
        content = json.dumps(data, indent=2)
        file.write(content)

    with open(f"./dist/json/live_timing_{dataset.table}.json", "w", encoding="utf-8") as file:
        index = min(len(data), 50)
        content = json.dumps(data[:index], indent=2)
        file.write(content)
