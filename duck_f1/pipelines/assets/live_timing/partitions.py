from datetime import datetime
from typing import List

import yaml
from dagster import StaticPartitionsDefinition
from pydantic import BaseModel


class LiveTimingDataset(BaseModel):
    file: str
    table: str


class LiveTimingSession(BaseModel):
    date: datetime
    name: str
    type: str


class LiveTimingEvent(BaseModel):
    country: str
    date: datetime
    gmt_offset: str
    location: str
    name: str
    official_event_name: str
    round_number: int
    sessions: List[LiveTimingSession]


class LiveTimingConfig(BaseModel):
    datasets: List[LiveTimingDataset]
    events: List[LiveTimingEvent]


class LiveTimingPartitionMetadata(BaseModel):
    season_round: int
    event_country: str
    event_date: datetime
    event_name: str
    session_type: str
    session_date: datetime


class LiveTimingPartition(BaseModel):
    partition_key: str
    event_key: str
    metadata: LiveTimingPartitionMetadata


class LiveTimingConfigManager:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self._config = self._read_config(self.config_path)

    @staticmethod
    def _read_config(path: str) -> LiveTimingConfig:
        with open(path, "r", encoding="UTF-8") as stream:
            data = yaml.safe_load(stream)
            return LiveTimingConfig(**data)

    @property
    def config(self) -> LiveTimingConfig:
        return self._config

    @property
    def datasets(self) -> List[LiveTimingDataset]:
        return self._config.datasets

    @property
    def events(self) -> List[LiveTimingEvent]:
        return self._config.events


class LiveTimingPartitionManager:
    def __init__(self, events: List[LiveTimingEvent]):
        self._partitions = self._create_partitions(events)

    def _create_partitions(self, events: List[LiveTimingEvent]) -> List[LiveTimingPartition]:
        out = []
        for event in events:
            for session in event.sessions:
                out.append(
                    LiveTimingPartition(
                        partition_key=self._create_partitions_key(event, session),
                        event_key=self._create_event_key(event, session),
                        metadata=LiveTimingPartitionMetadata(
                            season_round=event.round_number,
                            event_country=event.country,
                            event_date=event.date,
                            event_name=event.name,
                            session_type=session.type,
                            session_date=session.date,
                        ),
                    )
                )

        return out

    @property
    def partitions(self) -> List[LiveTimingPartition]:
        return self._partitions

    @property
    def partition_keys(self) -> List[str]:
        out = [i.partition_key for i in self._partitions]
        return out

    @property
    def dagster_partitions(self) -> StaticPartitionsDefinition:
        return StaticPartitionsDefinition(self.partition_keys)

    def get_partition(self, partition_key: str) -> LiveTimingPartition:
        partition = next((i for i in self._partitions if i.partition_key == partition_key))
        return partition

    @staticmethod
    def _create_partitions_key(event: LiveTimingEvent, session: LiveTimingSession) -> str:
        year = str(event.date.year)
        month = f"{event.date.month:02d}"
        day = f"{event.date.day:02d}"
        session = session.type.lower()
        key = "/".join([year, month, day, session])
        fmt_key = key.replace(" ", "_")
        return fmt_key

    @staticmethod
    def _create_event_key(event: LiveTimingEvent, session: LiveTimingSession) -> str:
        year = str(event.date.year)
        event = f"{event.date.strftime('%Y-%m-%d')}_{event.name}"
        session = f"{session.date.strftime('%Y-%m-%d')}_{session.name}"
        key = "/".join([year, event, session])
        fmt_key = key.replace(" ", "_")
        return fmt_key
