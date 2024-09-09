import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock

import click
import pandas as pd
from rich.progress import track

from duck_f1.pipelines.assets.live_timing import LiveTimingAssetManager
from duck_f1.pipelines.assets.live_timing.api import LiveTimingApi


@dataclass
class CliConfig:
    csv_output_path: Path = Path("src/duck_f1/transform/seeds/session_availability.csv")
    markdown_output_path: Path = Path("docs/live_timing_datasets.md")


def get_dataset_availability(
    start_date: datetime = None,
    end_date: datetime = None,
) -> pd.DataFrame:
    api_client = LiveTimingApi(MagicMock())
    live_timing_asset_manager = LiveTimingAssetManager()

    datasets = {}

    for dataset in live_timing_asset_manager.datasets:
        datasets[dataset.table] = []
        for session in live_timing_asset_manager.session_manager.sessions:
            if start_date is not None and session.metadata.session_date < start_date:
                continue

            if end_date is not None and session.metadata.session_date > end_date:
                continue

            datasets[dataset.table].append(
                {
                    "season_year": session.metadata.event_date.year,
                    "event_sha": session.metadata.event_sha,
                    "event_path": session.event_path,
                    "event_name": session.metadata.event_name,
                    "event_date": session.metadata.event_date,
                    "session_sha": session.metadata.session_sha,
                    "session_type": session.metadata.session_type,
                    "session_date": session.metadata.session_date,
                    "dataset": dataset.file,
                }
            )

    out = []

    max_padding = max([len(i) for i in datasets.keys()]) + max(
        [len(str(len(i))) for i in datasets.values()]
    )

    for k, v in datasets.items():
        for i in track(
            v,
            f"Checking {k} availability for {len(v)} sessions ".ljust(
                36 + max_padding, "."
            ),
        ):
            exists, content_length = api_client.check_if_dataset_exists(
                i["event_path"], i["dataset"]
            )
            out.append(
                {
                    **i,
                    "available": exists,
                    "content_length": 0 if content_length is None else content_length,
                }
            )

    df = pd.DataFrame(out)
    df = df.sort_values(by=["season_year", "event_date", "session_date", "dataset"])

    return df


def save_markdown_file(data: pd.DataFrame, output_file: Path) -> None:
    def availability_format(row):
        return "🟢" if row["available"] else "🔴"

    data["availability"] = data.apply(availability_format, axis=1)

    with open(output_file, "w", encoding="utf-8") as file:
        file.write("# Live timing datasets availability\n\n")
        seasons = list(data["season_year"].unique())

        for season in seasons:
            season_df = data[data["season_year"] == season]
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
                renamed_cols = dict(
                    [(i, i.replace("_", " ").lower().title()) for i in cols]
                )
                event_df.rename(columns=renamed_cols, inplace=True)

                file.write(event_df.to_markdown(index=False))
                file.write("\n\n")


def save_csv_file(data: pd.DataFrame, output_file: Path):
    data.to_csv(output_file, date_format="%Y-%m-%dT%H:%M:%S", header=True, index=False)


def get_seed_data(input_file: Path) -> Optional[pd.DataFrame]:
    if not input_file.is_file():
        logging.warn(
            "Seed file not found with path %s", input_file.resolve().as_posix()
        )
        return

    df = pd.read_csv(input_file)

    df["content_length"].fillna(0, inplace=True)
    df["content_length"] = pd.to_numeric(df["content_length"], downcast="integer")

    df["event_date"] = pd.to_datetime(df["event_date"], format="%Y-%m-%dT%H:%M:%S")
    df["session_date"] = pd.to_datetime(df["session_date"], format="%Y-%m-%dT%H:%M:%S")

    return df


def get_most_recent_session(input_file: Path) -> Optional[datetime]:
    df = get_seed_data(input_file)
    if df is None:
        return

    return df["session_date"].max()


@click.command()
@click.option(
    "--full-refresh",
    is_flag=True,
    default=False,
    required=False,
    help="Refresh all datasets",
)
@click.option(
    "--start-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    required=False,
    help="Start date filter for the session (format: YYYY-MM-DD)",
)
@click.option(
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=datetime.now(),
    required=False,
    help="End date filter for the session (format: YYYY-MM-DD)",
)
def main(
    full_refresh: bool, start_date: Optional[datetime], end_date: Optional[datetime]
):
    """
    Retrieve and save dataset availability information.
    """

    config = CliConfig()

    if not full_refresh and start_date is None:
        start_date = get_most_recent_session(config.csv_output_path)
    elif full_refresh:
        start_date = None

    df = get_seed_data(config.csv_output_path)
    df = pd.DataFrame() if df is None else df

    diff = get_dataset_availability(start_date, end_date)

    data = pd.concat([df, diff], ignore_index=True)
    data.drop_duplicates(
        subset=["event_sha", "session_sha", "dataset"], inplace=True, ignore_index=True
    )

    save_markdown_file(data.copy(), config.markdown_output_path)
    save_csv_file(data.copy(), config.csv_output_path)


if __name__ == "__main__":
    main()
