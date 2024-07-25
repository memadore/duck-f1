from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from unittest.mock import MagicMock

from duck_f1.pipelines.assets.live_timing import config_manager, partition_manager
from duck_f1.pipelines.assets.live_timing.live_timing import LiveTimingApi

api_client = LiveTimingApi(MagicMock())
partitions = partition_manager.partitions

keys = []

for dataset in config_manager.datasets:
    for partition in partitions:
        keys.append(
            {
                "event_key": partition.event_key,
                "event_name": partition.metadata.event_name,
                "event_date": partition.metadata.event_date,
                "season_year": partition.metadata.event_date.year,
                "session_type": partition.metadata.session_type,
                "session_date": partition.metadata.session_date,
                "dataset": dataset.file,
            }
        )

OUT = []


def task(keys):
    print(keys)
    data = api_client.get_dataset(keys["event_key"], keys["dataset"])
    OUT.append(
        {
            **keys,
            "availability": "🔴" if data is None else "🟢",
        }
    )


with ThreadPoolExecutor(max_workers=16) as executor:
    executor.map(task, keys)


df = pd.DataFrame(OUT)
df = df.sort_values(by=["season_year", "event_date", "session_date", "dataset"])
with open("./dist/doc/live_timing_datasets.md", "w", encoding="utf-8") as file:
    file.write("# Live timing datasets availability\n\n")
    seasons = list(df["season_year"].unique())

    for season in seasons:
        season_df = df[df["season_year"] == season]
        events = list(season_df["event_name"].unique())
        file.write(f"## {season} season\n\n")

        for event in events:
            event_df = season_df[season_df["event_name"] == event]
            event_date = list(event_df["event_date"].unique())[0]
            file.write(f"### {event} ({event_date.strftime('%Y-%m-%d')})\n\n")

            event_df = event_df.pivot(
                index="dataset", columns="session_type", values="availability"
            )

            event_df.reset_index(inplace=True)

            cols = list(event_df.columns)
            renamed_cols = dict([(i, i.replace("_", " ").lower().title()) for i in cols])
            event_df.rename(columns=renamed_cols, inplace=True)

            file.write(event_df.to_markdown(index=False))
            file.write("\n\n")
