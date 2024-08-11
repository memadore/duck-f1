from pathlib import Path
from typing import List

import yaml
from dagster import AssetsDefinition, StaticPartitionsDefinition

from .config import (
    LiveTimingConfig,
    LiveTimingDataset,
    LiveTimingEvent,
    LiveTimingSession,
    LiveTimingSessionMetadata,
)
from .duckdb import duckdb_live_timing_sessions, duckdb_parquet_asset_factory
from .parquet import parquet_asset_factory
from .processors import LiveTimingProcessorBuilder


class LiveTimingSessionManager:
    def __init__(self, events: List[LiveTimingEvent]):
        self._sessions = self._create_sessions(events)

    def _create_sessions(
        self, events: List[LiveTimingEvent]
    ) -> List[LiveTimingSession]:
        out = []
        for event in events:
            for session in event.sessions:
                out.append(
                    LiveTimingSession(
                        session_key=self._create_session_key(event, session),
                        event_path=self._create_event_path(event, session),
                        metadata=LiveTimingSessionMetadata(
                            event_round_number=event.round_number,
                            event_sha=event.sha,
                            event_country=event.country,
                            event_location=event.location,
                            event_date=event.date,
                            event_name=event.name,
                            session_sha=session.sha,
                            session_type=session.type,
                            session_date=session.date,
                        ),
                    )
                )

        return out

    @property
    def sessions(self) -> List[LiveTimingSession]:
        return self._sessions

    @property
    def session_keys(self) -> List[str]:
        out = [i.session_key for i in self._sessions]
        return out

    @property
    def dagster_partitions(self) -> StaticPartitionsDefinition:
        return StaticPartitionsDefinition(self.session_keys)

    def get_session(self, session_key: str) -> LiveTimingSession:
        session = next((i for i in self._sessions if i.session_key == session_key))
        return session

    def filter_sessions(
        self,
        season: List[int] = None,
        event_sha: List[str] = None,
        # event_date: Union[datetime, DateRange] = None,
        event_location: List[str] = None,
        event_country: List[str] = None,
        # round_number: Union[int, RoundRange] = None,
        session_sha: List[str] = None,
        # session_date: Union[datetime, DateRange] = None,
        session_type: List[str] = None,
    ) -> List[LiveTimingSession]:
        _subset = self._sessions
        _filters = {
            "season": season,
            "event_sha": event_sha,
            # "event_date": event_date,
            "event_location": event_location,
            "event_country": event_country,
            # "round_number": round_number,
            "session_sha": session_sha,
            # "session_date": session_date,
            "session_type": session_type,
        }
        for k, v in _filters.items():
            if v is None or len(v) == 0:
                continue

            if k == "season":
                _subset = self._filter_by_season(_subset, season)
            else:
                v = [i.strip().lower() for i in v]
                _subset = [
                    i for i in _subset if getattr(i.metadata, k).strip().lower() in v
                ]

        return _subset

    @staticmethod
    def _filter_by_season(
        sessions: List[LiveTimingSession], seasons: List[int]
    ) -> List[LiveTimingSession]:
        _subset = []
        for i in seasons:
            _subset.extend([j for j in sessions if j.metadata.event_date.year == i])

        return _subset

    @staticmethod
    def _create_session_key(event: LiveTimingEvent, session: LiveTimingSession) -> str:
        year = str(event.date.year)
        month = f"{event.date.month:02d}"
        day = f"{event.date.day:02d}"
        session = session.type.lower()
        key = "/".join([year, month, day, session])
        fmt_key = key.replace(" ", "_")
        return fmt_key

    @staticmethod
    def _create_event_path(event: LiveTimingEvent, session: LiveTimingSession) -> str:
        year = str(event.date.year)
        event = f"{event.date.strftime('%Y-%m-%d')}_{event.name}"
        session = f"{session.date.strftime('%Y-%m-%d')}_{session.name}"
        key = "/".join([year, event, session])
        fmt_key = key.replace(" ", "_")
        return fmt_key


class LiveTimingAssetManager:
    default_config_path = (Path(__file__).parent / "values.yaml").resolve()

    def __init__(self, config_path: Path = None):
        config_path = self.default_config_path if config_path is None else config_path
        self._config = self._load_yaml_config(config_path)
        self.session_manager = LiveTimingSessionManager(self.events)
        self.processor_builder = LiveTimingProcessorBuilder()

    def _load_yaml_config(self, path: Path) -> LiveTimingConfig:
        with path.open("r", encoding="utf-8") as file:
            return LiveTimingConfig(**yaml.safe_load(file))

    @property
    def asset_names(self) -> List[str]:
        return self.processor_builder.assets

    @property
    def config(self) -> LiveTimingConfig:
        return self._config

    @property
    def datasets(self) -> List[LiveTimingDataset]:
        return self._config.datasets

    @property
    def events(self) -> List[LiveTimingEvent]:
        return self._config.events

    def create_parquet_assets(self) -> List[AssetsDefinition]:
        out = []

        for dataset in self.datasets:
            if dataset.table in self.processor_builder.processors:
                asset_config = {
                    "key_prefix": ["live_timing"],
                    "is_required": False,
                    "io_manager_key": "pyarrow_parquet_io_manager",
                }

                multi_asset_outs = self.processor_builder.assets_definition(
                    dataset.table, **asset_config
                )

                out.append(
                    parquet_asset_factory(
                        multi_asset_outs=multi_asset_outs,
                        dataset=dataset,
                        processor_builder=self.processor_builder,
                        session_partitions=self.session_manager.dagster_partitions,
                        session_mapper=self.session_manager.get_session,
                    )
                )

        return out

    def create_duckdb_assets(self) -> List[AssetsDefinition]:
        out = []
        for i in self.asset_names:
            out.append(
                duckdb_parquet_asset_factory(
                    asset_name=i,
                    session_partitions=self.session_manager.dagster_partitions,
                )
            )

        out.append(duckdb_live_timing_sessions(self.events))

        return out
