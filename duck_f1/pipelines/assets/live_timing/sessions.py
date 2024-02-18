from datetime import datetime
from typing import List

import yaml
from pydantic import BaseModel


class LiveTimingDataset(BaseModel):
    file: str
    table: str


class LiveTimingSessionDetail(BaseModel):
    sha: str
    date: datetime
    name: str
    type: str


class LiveTimingEvent(BaseModel):
    country: str
    date: datetime
    sha: str
    gmt_offset: str
    location: str
    name: str
    official_event_name: str
    round_number: int
    sessions: List[LiveTimingSessionDetail]


class LiveTimingConfig(BaseModel):
    datasets: List[LiveTimingDataset]
    events: List[LiveTimingEvent]


class LiveTimingSessionMetadata(BaseModel):
    season_round: int
    event_sha: str
    event_country: str
    event_date: datetime
    event_name: str
    session_sha: str
    session_type: str
    session_date: datetime


class LiveTimingSession(BaseModel):
    session_key: str
    event_path: str
    metadata: LiveTimingSessionMetadata


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


class LiveTimingSessionManager:
    def __init__(self, events: List[LiveTimingEvent]):
        self._sessions = self._create_sessions(events)

    def _create_sessions(self, events: List[LiveTimingEvent]) -> List[LiveTimingSession]:
        out = []
        for event in events:
            for session in event.sessions:
                out.append(
                    LiveTimingSession(
                        session_key=self._create_session_key(event, session),
                        event_path=self._create_event_path(event, session),
                        metadata=LiveTimingSessionMetadata(
                            season_round=event.round_number,
                            event_sha=event.sha,
                            event_country=event.country,
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

    def get_session(self, session_key: str) -> LiveTimingSession:
        session = next((i for i in self._sessions if i.session_key == session_key))
        return session

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
