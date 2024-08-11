from datetime import datetime
from typing import List

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
    event_round_number: int
    event_sha: str
    event_country: str
    event_location: str
    event_date: datetime
    event_name: str
    session_sha: str
    session_type: str
    session_date: datetime


class LiveTimingSession(BaseModel):
    session_key: str
    event_path: str
    metadata: LiveTimingSessionMetadata
