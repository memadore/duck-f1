import base64
import io
import json
from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime
from typing import Dict, List

import pyarrow as pa
from dagster import AssetOut, OpExecutionContext, Output
from pydantic import BaseModel

from .config import LiveTimingSessionMetadata


class LiveTimingAsset(BaseModel):
    key: str
    output: pa.Table

    class Config:
        arbitrary_types_allowed = True


class AbstractLiveTimingProcessor(ABC):
    source_asset: str
    materializing_assets: List[str]

    def __init__(
        self, context: OpExecutionContext, metadata: LiveTimingSessionMetadata
    ) -> None:
        self.context = context
        self.metadata = metadata

    @property
    def materializing_assets(self) -> List[str]:
        return self.materializing_assets

    @property
    def source_asset(self) -> str:
        return self.source_asset

    @abstractmethod
    def _processor(self, data: io.BytesIO) -> List[LiveTimingAsset]:
        pass

    def _add_metadata(self, table: pa.Table) -> pa.Table:
        columns = self.metadata.dict()
        # self.context.log.info("Metadata: %s", columns)
        table_len = table.num_rows
        for col, value in columns.items():
            if type(value) is int:
                column_type = pa.int32()
                column_value = value
            elif type(value) is datetime:
                column_type = pa.timestamp("s")
                column_value = value
            else:
                column_type = pa.string()
                column_value = str(value)

            table = table.append_column(
                col, pa.array([column_value] * table_len, column_type)
            )

        return table

    def run(self, data: io.BytesIO) -> List[Output]:
        out = []

        if data is None:
            return out

        assets = self._processor(data)
        for i in assets:
            i.output = self._add_metadata(i.output)
            out.append(Output(value=i.output, output_name=i.key))

        return out


class ArchiveStatusProcessor(AbstractLiveTimingProcessor):
    source_asset = "archive_status"
    materializing_assets = ["archive_status"]

    def _processor(self, data: dict) -> List[LiveTimingAsset]:
        schema = pa.schema([("Status", pa.string())])

        table = pa.Table.from_pylist(data).cast(schema)
        return [LiveTimingAsset(key="archive_status", output=table)]


class AudioStreamsProcessor(AbstractLiveTimingProcessor):
    source_asset = "audio_streams"
    materializing_assets = ["audio_streams"]

    @staticmethod
    def _row_processor(stream_ts: str, streams: List[dict]) -> List[dict]:
        out = []
        for i in streams:
            out.append(
                {
                    "Name": i.get("Name", None),
                    "Language": i.get("Language", None),
                    "Uri": i.get("Uri", None),
                    "Path": i.get("Path", None),
                    "Utc": i.get("Utc", None),
                }
            )

        out = list(map(lambda item: dict(item, _StreamTimestamp=stream_ts), out))
        return out

    def _processor(self, data: dict) -> List[LiveTimingAsset]:
        schema = pa.schema(
            [
                ("Name", pa.string()),
                ("Language", pa.string()),
                ("Uri", pa.string()),
                ("Path", pa.string()),
                ("Utc", pa.string()),
                ("_StreamTimestamp", pa.string()),
            ]
        )

        processed_data = []

        for i in data:
            processed_data.extend(
                AudioStreamsProcessor._row_processor(
                    stream_ts=i["_StreamTimestamp"], streams=i["Streams"]
                )
            )

        table = pa.Table.from_pylist(processed_data).cast(schema)
        return [LiveTimingAsset(key="audio_streams", output=table)]


class CarDataProcessor(AbstractLiveTimingProcessor):
    source_asset = "car_data"
    materializing_assets = ["car_data"]

    @staticmethod
    def _explode(capture_ts: str, car_number: str, channel_data: dict):
        out = [
            {
                "CaptureTimestamp": capture_ts,
                "CarNumber": car_number,
                "EngineRpm": channel_data.get("0", None),
                "CarSpeed": channel_data.get("2", None),
                "EngineGear": channel_data.get("3", None),
                "ThrottlePosition": channel_data.get("4", None),
                "BrakePosition": channel_data.get("5", None),
                "DrsStatus": channel_data.get("45", None),
            }
        ]
        return out

    @staticmethod
    def _entry_transformer(entry: dict) -> List[dict]:
        out = []
        capture_ts = entry["Utc"]
        for car_number, car_data in entry["Cars"].items():
            records = CarDataProcessor._explode(
                capture_ts=capture_ts,
                car_number=car_number,
                channel_data=car_data["Channels"],
            )
            out.extend(records)

        return out

    @staticmethod
    def _row_processor(stream_ts: str, entries: List[dict]) -> List[dict]:
        out = []
        for entry in entries:
            out.extend(CarDataProcessor._entry_transformer(entry))

        out = list(map(lambda item: dict(item, _StreamTimestamp=stream_ts), out))
        return out

    def _processor(self, data: List[dict]) -> pa.Table:
        schema = pa.schema(
            [
                ("CaptureTimestamp", pa.string()),
                ("CarNumber", pa.int16()),
                ("EngineRpm", pa.int16()),
                ("CarSpeed", pa.int16()),
                ("EngineGear", pa.int16()),
                ("ThrottlePosition", pa.int16()),
                ("BrakePosition", pa.int16()),
                ("DrsStatus", pa.int16()),
                ("_StreamTimestamp", pa.string()),
            ]
        )

        processed_data = []

        for i in data:
            processed_data.extend(
                CarDataProcessor._row_processor(
                    stream_ts=i["_StreamTimestamp"], entries=i["Entries"]
                )
            )

        table = pa.Table.from_pylist(processed_data).cast(schema)
        return [LiveTimingAsset(key="car_data", output=table)]


class ChampionshipPredictionProcessor(AbstractLiveTimingProcessor):
    source_asset = "championship_prediction"
    materializing_assets = ["championship_prediction"]

    @staticmethod
    def _explode(entity: str, identifier: str, metrics: dict) -> List[dict]:
        out = []
        wanted_metrics = [
            "CurrentPosition",
            "PredictedPosition",
            "CurrentPoints",
            "PredictedPoints",
        ]
        for key, value in metrics.items():
            if key not in wanted_metrics:
                continue

            out.append(
                {
                    "Entity": entity,
                    "Identifier": identifier,
                    "Metric": key,
                    "Value": value,
                }
            )

        return out

    @staticmethod
    def _row_processor(stream_ts: str, entity: str, data: dict) -> List[dict]:
        out = []
        for key, value in data.items():
            if len(key) == 0:
                continue
            out.extend(ChampionshipPredictionProcessor._explode(entity, key, value))

        out = list(map(lambda item: dict(item, _StreamTimestamp=stream_ts), out))
        return out

    def _processor(self, data: dict) -> List[LiveTimingAsset]:
        schema = pa.schema(
            [
                ("Entity", pa.string()),
                ("Identifier", pa.string()),
                ("Metric", pa.string()),
                ("Value", pa.decimal128(5, 2)),
                ("_StreamTimestamp", pa.string()),
            ]
        )

        processed_data = []

        if len(data) == 1:
            # first row is empty
            table = schema.empty_table()

        else:
            for i in data:
                if "Drivers" in i:
                    processed_data.extend(
                        ChampionshipPredictionProcessor._row_processor(
                            stream_ts=i["_StreamTimestamp"],
                            entity="driver",
                            data=i["Drivers"],
                        )
                    )

                if "Teams" in i:
                    processed_data.extend(
                        ChampionshipPredictionProcessor._row_processor(
                            stream_ts=i["_StreamTimestamp"],
                            entity="team",
                            data=i["Teams"],
                        )
                    )

            table = pa.Table.from_pylist(processed_data).cast(schema)

        return [LiveTimingAsset(key="championship_prediction", output=table)]


class CurrentTyresProcessor(AbstractLiveTimingProcessor):
    source_asset = "current_tyres"
    materializing_assets = ["current_tyres"]

    @staticmethod
    def _row_processor(stream_ts: str, tyres: List[dict]) -> List[dict]:
        out = []
        for driver, data in tyres.items():
            if len(driver) == 0 or len(driver) > 2:
                continue

            if driver == "_deleted":
                continue

            out.append(
                {
                    "Driver": driver,
                    "Compound": data.get("Compound", "UNKNOWN"),
                    "New": data.get("New", None),
                }
            )

        out = list(map(lambda item: dict(item, _StreamTimestamp=stream_ts), out))
        return out

    def _processor(self, data: dict) -> List[LiveTimingAsset]:
        schema = pa.schema(
            [
                ("Driver", pa.string()),
                ("Compound", pa.string()),
                ("New", pa.bool_()),
                ("_StreamTimestamp", pa.string()),
            ]
        )

        processed_data = []

        for i in data:
            processed_data.extend(
                CurrentTyresProcessor._row_processor(
                    stream_ts=i["_StreamTimestamp"], tyres=i["Tyres"]
                )
            )

        table = pa.Table.from_pylist(processed_data).cast(schema)
        return [LiveTimingAsset(key="current_tyres", output=table)]


class DriverListProcessor(AbstractLiveTimingProcessor):
    source_asset = "driver_list"
    materializing_assets = ["driver_list"]

    @staticmethod
    def _row_processor(data: dict) -> List[dict]:
        out = []
        stream_ts = data.pop("_StreamTimestamp")
        for driver, driver_info in data.items():
            out.append(
                {
                    "_StreamTimestamp": stream_ts,
                    "RacingNumber": driver_info.get("RacingNumber", None),
                    "BroadcastName": driver_info.get("BroadcastName", None),
                    "FullName": driver_info.get("FullName", None),
                    "Tla": driver_info.get("Tla", None),
                    "Line": driver_info.get("Line", None),
                    "TeamName": driver_info.get("TeamName", None),
                    "TeamColour": driver_info.get("TeamColour", None),
                    "FirstName": driver_info.get("FirstName", None),
                    "LastName": driver_info.get("LastName", None),
                    "Reference": driver_info.get("Reference", None),
                    "HeadshotUrl": driver_info.get("HeadshotUrl", None),
                }
            )

        return out

    def _processor(self, data: dict) -> List[LiveTimingAsset]:
        schema = pa.schema(
            [
                ("_StreamTimestamp", pa.string()),
                ("RacingNumber", pa.string()),
                ("BroadcastName", pa.string()),
                ("FullName", pa.string()),
                ("Tla", pa.string()),
                ("Line", pa.int16()),
                ("TeamName", pa.string()),
                ("TeamColour", pa.string()),
                ("FirstName", pa.string()),
                ("LastName", pa.string()),
                ("Reference", pa.string()),
                ("HeadshotUrl", pa.string()),
            ]
        )

        processed_data = DriverListProcessor._row_processor(data[0])

        table = pa.Table.from_pylist(processed_data).cast(schema)
        return [LiveTimingAsset(key="driver_list", output=table)]


class DriverRaceInfoProcessor(AbstractLiveTimingProcessor):
    source_asset = "driver_race_info"
    materializing_assets = ["driver_race_info"]

    @staticmethod
    def _row_processor(data: dict) -> List[dict]:
        out = []
        ts = data.pop("_StreamTimestamp")
        for key, value in data.items():
            if len(key) == 0:
                continue

            out.append(
                {
                    "Driver": key,
                    "Position": value.get("Position", None),
                    "Gap": value.get("Gap", None),
                    "Interval": value.get("Interval", None),
                    "PitStops": value.get("PitStops", None),
                    "Catching": value.get("Catching", None),
                    "OvertakeState": value.get("OvertakeState", None),
                    "IsOut": value.get("IsOut", None),
                    "_StreamTimestamp": ts,
                }
            )

        return out

    def _processor(self, data: dict) -> List[LiveTimingAsset]:
        schema = pa.schema(
            [
                ("Driver", pa.int16()),
                ("Position", pa.int16()),
                ("Gap", pa.string()),
                ("Interval", pa.string()),
                ("PitStops", pa.int16()),
                ("Catching", pa.int16()),
                ("OvertakeState", pa.int16()),
                ("IsOut", pa.bool_()),
                ("_StreamTimestamp", pa.string()),
            ]
        )

        processed_data = []

        for i in data:
            processed_data.extend(DriverRaceInfoProcessor._row_processor(i))

        table = pa.Table.from_pylist(processed_data).cast(schema)
        return [LiveTimingAsset(key="driver_race_info", output=table)]


class ExtrapolatedClockProcessor(AbstractLiveTimingProcessor):
    source_asset = "extrapolated_clock"
    materializing_assets = ["extrapolated_clock"]

    def _processor(self, data: dict) -> List[LiveTimingAsset]:
        schema = pa.schema(
            [
                ("_StreamTimestamp", pa.string()),
                ("Utc", pa.string()),
                ("Remaining", pa.string()),
                ("Extrapolating", pa.bool_()),
            ]
        )

        table = pa.Table.from_pylist(data).cast(schema)
        return [LiveTimingAsset(key="extrapolated_clock", output=table)]


class HeartbeatProcessor(AbstractLiveTimingProcessor):
    source_asset = "heartbeat"
    materializing_assets = ["heartbeat"]

    def _processor(self, data: dict) -> List[LiveTimingAsset]:
        schema = pa.schema([("_StreamTimestamp", pa.string()), ("Utc", pa.string())])

        table = pa.Table.from_pylist(data).cast(schema)
        return [LiveTimingAsset(key="heartbeat", output=table)]


class IndexProcessor(AbstractLiveTimingProcessor):
    source_asset = "index"
    materializing_assets = ["index"]

    @staticmethod
    def _feed_processor(feeds: dict) -> List[dict]:
        out = []
        for _, value in feeds.items():
            out.append(value)

        return out

    def _processor(self, data: dict) -> List[LiveTimingAsset]:
        schema = pa.schema([("KeyFramePath", pa.string()), ("StreamPath", pa.string())])

        processed_data = []

        for i in data:
            processed_data.extend(IndexProcessor._feed_processor(feeds=i["Feeds"]))

        table = pa.Table.from_pylist(processed_data).cast(schema)
        return [LiveTimingAsset(key="index", output=table)]


class LapCountProcessor(AbstractLiveTimingProcessor):
    source_asset = "lap_count"
    materializing_assets = ["lap_count"]

    @staticmethod
    def _row_processor(data: dict) -> List[dict]:
        out = []
        stream_ts = data.pop("_StreamTimestamp")
        for key, value in data.items():
            if len(key) == 0:
                continue

            out.append({"Metric": key, "Value": value})

        out = list(map(lambda item: dict(item, _StreamTimestamp=stream_ts), out))
        return out

    def _processor(self, data: dict) -> List[LiveTimingAsset]:
        schema = pa.schema(
            [
                ("Metric", pa.string()),
                ("Value", pa.int16()),
                ("_StreamTimestamp", pa.string()),
            ]
        )

        processed_data = []

        for i in data:
            processed_data.extend(LapCountProcessor._row_processor(i))

        table = pa.Table.from_pylist(processed_data).cast(schema)
        return [LiveTimingAsset(key="lap_count", output=table)]


class LapSeriesProcessor(AbstractLiveTimingProcessor):
    source_asset = "lap_series"
    materializing_assets = ["lap_series"]

    @staticmethod
    def _explode(driver_number: int, position_data) -> List[dict]:
        out = []
        if type(position_data) is list:
            out.append(
                {
                    "DriverNumber": int(driver_number),
                    "LapNumber": 0,
                    "LapPosition": int(position_data[0]),
                }
            )
        else:
            for lap, position in position_data.items():
                out.append(
                    {
                        "DriverNumber": int(driver_number),
                        "LapNumber": int(lap),
                        "LapPosition": int(position),
                    }
                )

        return out

    @staticmethod
    def _row_processor(data: dict) -> List[dict]:
        out = []

        stream_ts = data.pop("_StreamTimestamp")
        for driver, value in data.items():
            if len(driver) == 0 or len(driver) > 2:
                continue

            out.extend(LapSeriesProcessor._explode(driver, value["LapPosition"]))

        out = list(map(lambda item: dict(item, _StreamTimestamp=stream_ts), out))
        return out

    def _processor(self, data: List[dict]) -> pa.Table:
        schema = pa.schema(
            [
                ("DriverNumber", pa.int16()),
                ("LapNumber", pa.int16()),
                ("LapPosition", pa.int16()),
                ("_StreamTimestamp", pa.string()),
            ]
        )

        processed_data = []

        for i in data:
            processed_data.extend(LapSeriesProcessor._row_processor(i))

        table = pa.Table.from_pylist(processed_data).cast(schema)
        return [LiveTimingAsset(key="lap_series", output=table)]


class PitLaneTimeCollectionProcessor(AbstractLiveTimingProcessor):
    source_asset = "pit_lane_time_collection"
    materializing_assets = ["pit_lane_time_collection"]

    @staticmethod
    def _row_processor(stream_ts: str, pit_times: dict) -> List[dict]:
        out = []
        for driver, data in pit_times.items():
            if driver == "_deleted":
                continue

            out.append(
                {
                    "Driver": driver,
                    "Duration": data.get("Duration"),
                    "Lap": data.get("Lap") if len(data.get("Lap", "")) > 0 else None,
                }
            )

        out = list(map(lambda item: dict(item, _StreamTimestamp=stream_ts), out))
        return out

    def _processor(self, data: List[dict]) -> pa.Table:
        schema = pa.schema(
            [
                ("Driver", pa.string()),
                ("Duration", pa.string()),
                ("Lap", pa.int16()),
                ("_StreamTimestamp", pa.string()),
            ]
        )

        processed_data = []

        for i in data:
            processed_data.extend(
                PitLaneTimeCollectionProcessor._row_processor(
                    stream_ts=i["_StreamTimestamp"], pit_times=i["PitTimes"]
                )
            )

        table = pa.Table.from_pylist(processed_data).cast(schema)
        return [LiveTimingAsset(key="pit_lane_time_collection", output=table)]


class PositionProcessor(AbstractLiveTimingProcessor):
    source_asset = "position"
    materializing_assets = ["position"]

    @staticmethod
    def _entry_transformer(entry: dict) -> List[dict]:
        out = []
        capture_ts = entry["Timestamp"]
        for driver_number, position_data in entry["Entries"].items():
            out.append(
                {"Timestamp": capture_ts, "Driver": driver_number, **position_data}
            )

        return out

    @staticmethod
    def _row_processor(stream_ts: str, positions: List[dict]) -> List[dict]:
        out = []
        for i in positions:
            out.extend(PositionProcessor._entry_transformer(i))

        out = list(map(lambda item: dict(item, _StreamTimestamp=stream_ts), out))
        return out

    def _processor(self, data: List[dict]) -> pa.Table:
        schema = pa.schema(
            [
                ("Timestamp", pa.string()),
                ("Driver", pa.string()),
                ("Status", pa.string()),
                ("X", pa.int32()),
                ("Y", pa.int32()),
                ("Z", pa.int32()),
                ("_StreamTimestamp", pa.string()),
            ]
        )

        processed_data = []

        for i in data:
            processed_data.extend(
                PositionProcessor._row_processor(
                    stream_ts=i["_StreamTimestamp"], positions=i["Position"]
                )
            )

        table = pa.Table.from_pylist(processed_data).cast(schema)
        return [LiveTimingAsset(key="position", output=table)]


class RaceControlMessagesProcessor(AbstractLiveTimingProcessor):
    source_asset = "race_control_messages"
    materializing_assets = ["race_control_messages"]

    @staticmethod
    def _entry_transformer(message_id: int, message: dict) -> List[dict]:
        header = {
            "MessageId": int(message_id) if message_id is not None else None,
            "Utc": message.pop("Utc"),
            "Lap": message.pop("Lap") if "Lap" in message else None,
            "Category": message.pop("Category"),
        }

        data = base64.urlsafe_b64encode(json.dumps(message).encode()).decode()

        return {**header, "MessageData": data}

    @staticmethod
    def _row_processor(stream_ts: str, messages) -> List[dict]:
        out = []

        if type(messages) is list:
            for i in messages:
                out.append(RaceControlMessagesProcessor._entry_transformer(None, i))
        elif type(messages) is dict:
            for message_id, data in messages.items():
                out.append(
                    RaceControlMessagesProcessor._entry_transformer(message_id, data)
                )

        out = list(map(lambda item: dict(item, _StreamTimestamp=stream_ts), out))

        return out

    def _processor(self, data: List[dict]) -> pa.Table:
        schema = pa.schema(
            [
                ("MessageId", pa.int16()),
                ("Utc", pa.string()),
                ("Lap", pa.int16()),
                ("Category", pa.string()),
                ("MessageData", pa.string()),
                ("_StreamTimestamp", pa.string()),
            ]
        )

        processed_data = []

        for i in data:
            processed_data.extend(
                RaceControlMessagesProcessor._row_processor(
                    stream_ts=i["_StreamTimestamp"], messages=i["Messages"]
                )
            )

        table = pa.Table.from_pylist(processed_data).cast(schema)
        return [LiveTimingAsset(key="race_control_messages", output=table)]


class SessionDataProcessor(AbstractLiveTimingProcessor):
    source_asset = "session_data"
    materializing_assets = ["session_data"]

    @staticmethod
    def _row_processor(key: str, data: List[dict]) -> List[dict]:
        out = []
        for i in data:
            measure = {"Key": key, "Utc": i.pop("Utc")}

            for metric_name, metric_value in i.items():
                measure.update(
                    {"MetricName": metric_name, "MetricValue": str(metric_value)}
                )

            out.append(measure)

        return out

    def _processor(self, data: dict) -> List[LiveTimingAsset]:
        schema = pa.schema(
            [
                ("Key", pa.string()),
                ("Utc", pa.string()),
                ("MetricName", pa.string()),
                ("MetricValue", pa.string()),
            ]
        )

        processed_data = []

        for i in data:
            processed_data.extend(
                SessionDataProcessor._row_processor(key="Series", data=i["Series"])
            )

            processed_data.extend(
                SessionDataProcessor._row_processor(
                    key="StatusSeries", data=i["StatusSeries"]
                )
            )

        table = pa.Table.from_pylist(processed_data).cast(schema)
        return [LiveTimingAsset(key="session_data", output=table)]


class SessionInfoProcessor(AbstractLiveTimingProcessor):
    source_asset = "session_info"
    materializing_assets = ["session_info"]

    def _row_processor(data: dict, prefix: str = None) -> dict:
        out = {
            "MeetingKey": None,
            "MeetingName": None,
            "MeetingOfficialName": None,
            "MeetingLocation": None,
            "MeetingCountryKey": None,
            "MeetingCountryCode": None,
            "MeetingCountryName": None,
            "MeetingCircuitKey": None,
            "MeetingCircuitShortName": None,
            "ArchiveStatusStatus": None,
            "Key": None,
            "Type": None,
            "Name": None,
            "StartDate": None,
            "EndDate": None,
            "GmtOffset": None,
            "Path": None,
            "Number": None,
        }

        for key, value in data.items():
            fmt_key = key if prefix is None else f"{prefix}{key}"
            if isinstance(value, dict):
                out.update(SessionInfoProcessor._row_processor(value, fmt_key))
            else:
                out.update({fmt_key: value})

        return out

    def _processor(self, data: dict) -> List[LiveTimingAsset]:
        schema = pa.schema(
            [
                ("MeetingKey", pa.string()),
                ("MeetingName", pa.string()),
                ("MeetingOfficialName", pa.string()),
                ("MeetingLocation", pa.string()),
                ("MeetingCountryKey", pa.string()),
                ("MeetingCountryCode", pa.string()),
                ("MeetingCountryName", pa.string()),
                ("MeetingCircuitKey", pa.string()),
                ("MeetingCircuitShortName", pa.string()),
                ("ArchiveStatusStatus", pa.string()),
                ("Key", pa.string()),
                ("Type", pa.string()),
                ("Name", pa.string()),
                ("StartDate", pa.string()),
                ("EndDate", pa.string()),
                ("GmtOffset", pa.string()),
                ("Path", pa.string()),
                ("Number", pa.string()),
            ]
        )

        processed_data = SessionInfoProcessor._row_processor(data[0])

        table = pa.Table.from_pylist([processed_data]).cast(schema)
        return [LiveTimingAsset(key="session_info", output=table)]


class SessionStatusProcessor(AbstractLiveTimingProcessor):
    source_asset = "session_status"
    materializing_assets = ["session_status"]

    def _processor(self, data: dict) -> List[LiveTimingAsset]:
        schema = pa.schema([("_StreamTimestamp", pa.string()), ("Status", pa.string())])

        table = pa.Table.from_pylist(data).cast(schema)
        return [LiveTimingAsset(key="session_status", output=table)]


class TimingDataProcessor(AbstractLiveTimingProcessor):
    source_asset = "timing_data"
    materializing_assets = [
        "timing_data_best_lap",
        "timing_data_interval",
        "timing_data_last_lap",
        "timing_data_sector_segments",
        "timing_data_sectors",
        "timing_data_speeds",
        "timing_data_status",
    ]

    @staticmethod
    def _stack_dicts(data: List[dict]) -> dict:
        out = defaultdict(list)
        for i in data:
            for k, v in i.items():
                out[k].extend(v)

        return out

    @staticmethod
    def _best_lap_transformer(data: dict) -> dict:
        return {
            "Value": data.get("Value", None),
            "Lap": data.get("Lap", None),
        }

    @staticmethod
    def _interval_transformer(data: dict) -> dict:
        return {
            "Value": data.get("Value", None),
            "Catching": data.get("Catching", None),
        }

    @staticmethod
    def _last_lap_transformer(data: dict) -> dict:
        return {
            "Value": data.get("Value", None),
            "Status": data.get("Status", None),
            "OverallFastest": data.get("OverallFastest", None),
            "PersonalFastest": data.get("PersonalFastest", None),
        }

    @staticmethod
    def _sector_segments_transformer(sector_key: str, segments: dict) -> List[dict]:
        out = []
        for segment_key, segment_data in segments.items():
            out.append(
                {
                    "SectorKey": sector_key,
                    "SegmentKey": segment_key,
                    "Status": segment_data.get("Status", None),
                }
            )

        return out

    @staticmethod
    def _sectors_transformer(data: dict) -> dict:
        out = {"sectors": [], "sector_segments": []}

        if isinstance(data, list):  # skip if list
            return out

        for sector_key, sector_data in data.items():
            if "Segments" in sector_data:
                segments = sector_data.pop("Segments")
                if not isinstance(segments, list):  # skip if list
                    out["sector_segments"].extend(
                        TimingDataProcessor._sector_segments_transformer(
                            sector_key, segments
                        )
                    )

            out["sectors"].append(
                {
                    "SectorKey": sector_key,
                    "Stopped": sector_data.get("Stopped", None),
                    "Value": sector_data.get("Value", None),
                    "PreviousValue": sector_data.get("PreviousValue", None),
                    "Status": sector_data.get("Status", None),
                    "OverallFastest": sector_data.get("OverallFastest", None),
                    "PersonalFastest": sector_data.get("PersonalFastest", None),
                }
            )

        return out

    @staticmethod
    def _speeds_transformer(data: dict) -> dict:
        out = []

        for speed_key, speed_data in data.items():
            out.append(
                {
                    "SpeedKey": speed_key,
                    "Value": speed_data.get("Value", None),
                    "Status": speed_data.get("Status", None),
                    "OverallFastest": speed_data.get("OverallFastest", None),
                    "PersonalFastest": speed_data.get("PersonalFastest", None),
                }
            )

        return out

    @staticmethod
    def _status_transformer(metric_name: str, metric_value: str) -> List[dict]:
        return {
            "MetricName": metric_name,
            "MetricValue": str(metric_value),
        }

    @staticmethod
    def _entry_transformer(driver: int, metrics: dict) -> dict:
        out = defaultdict(list)

        for metric, data in metrics.items():
            if metric == "IntervalToPositionAhead":
                out["timing_data_interval"].append(
                    TimingDataProcessor._interval_transformer(data)
                )

            elif metric == "BestLapTime":
                out["timing_data_best_lap"].append(
                    TimingDataProcessor._best_lap_transformer(data)
                )

            elif metric == "LastLapTime":
                out["timing_data_last_lap"].append(
                    TimingDataProcessor._last_lap_transformer(data)
                )

            elif metric == "Sectors":
                transformer_output = TimingDataProcessor._sectors_transformer(data)
                out["timing_data_sectors"].extend(transformer_output["sectors"])
                out["timing_data_sector_segments"].extend(
                    transformer_output["sector_segments"]
                )

            elif metric == "Speeds":
                out["timing_data_speeds"].extend(
                    TimingDataProcessor._speeds_transformer(data)
                )

            else:
                out["timing_data_status"].append(
                    TimingDataProcessor._status_transformer(metric, data)
                )

        for k, v in out.items():  # add driver key
            out[k] = list(map(lambda item: dict(item, Driver=driver), v))

        return out

    @staticmethod
    def _row_processor(stream_ts: str, lines: dict) -> List[dict]:
        output = []

        for driver, metrics in lines.items():
            if len(driver) == 0 or len(driver) > 2:
                continue

            output.append(TimingDataProcessor._entry_transformer(int(driver), metrics))

        out = TimingDataProcessor._stack_dicts(output)

        for k, v in out.items():  # add ts key
            out[k] = list(map(lambda item: dict(item, _StreamTimestamp=stream_ts), v))

        return out

    def _processor(self, data: List[dict]) -> pa.Table:
        schemas = {
            "timing_data_best_lap": pa.schema(
                [
                    ("Value", pa.string()),
                    ("Lap", pa.string()),
                    ("Driver", pa.int16()),
                    ("_StreamTimestamp", pa.string()),
                ]
            ),
            "timing_data_interval": pa.schema(
                [
                    ("Value", pa.string()),
                    ("Catching", pa.string()),
                    ("Driver", pa.int16()),
                    ("_StreamTimestamp", pa.string()),
                ]
            ),
            "timing_data_last_lap": pa.schema(
                [
                    ("Value", pa.string()),
                    ("Status", pa.int16()),
                    ("OverallFastest", pa.bool_()),
                    ("PersonalFastest", pa.bool_()),
                    ("Driver", pa.int16()),
                    ("_StreamTimestamp", pa.string()),
                ]
            ),
            "timing_data_sector_segments": pa.schema(
                [
                    ("SectorKey", pa.string()),
                    ("SegmentKey", pa.string()),
                    ("Status", pa.int16()),
                    ("Driver", pa.int16()),
                    ("_StreamTimestamp", pa.string()),
                ]
            ),
            "timing_data_sectors": pa.schema(
                [
                    ("SectorKey", pa.string()),
                    ("Stopped", pa.string()),
                    ("Value", pa.string()),
                    ("PreviousValue", pa.string()),
                    ("Status", pa.int16()),
                    ("OverallFastest", pa.bool_()),
                    ("PersonalFastest", pa.bool_()),
                    ("Driver", pa.int16()),
                    ("_StreamTimestamp", pa.string()),
                ]
            ),
            "timing_data_speeds": pa.schema(
                [
                    ("SpeedKey", pa.string()),
                    ("Value", pa.string()),
                    ("Status", pa.int16()),
                    ("OverallFastest", pa.bool_()),
                    ("PersonalFastest", pa.bool_()),
                    ("Driver", pa.int16()),
                    ("_StreamTimestamp", pa.string()),
                ]
            ),
            "timing_data_status": pa.schema(
                [
                    ("MetricName", pa.string()),
                    ("MetricValue", pa.string()),
                    ("Driver", pa.int16()),
                    ("_StreamTimestamp", pa.string()),
                ]
            ),
        }

        processed_data = []

        for i in data:
            if "Withheld" in i:  # first empty element
                continue

            if "Lines" not in i:
                continue

            processed_data.append(
                TimingDataProcessor._row_processor(
                    stream_ts=i["_StreamTimestamp"], lines=i["Lines"]
                )
            )

        tables = TimingDataProcessor._stack_dicts(processed_data)

        out = []
        for k, v in tables.items():
            if len(v) == 0:
                continue

            table = pa.Table.from_pylist(v).cast(schemas[k])
            out.append(LiveTimingAsset(key=k, output=table))

        return out


class TimingStatsProcessor(AbstractLiveTimingProcessor):
    source_asset = "timing_stats"
    materializing_assets = [
        "timing_stats_lap_times",
        "timing_stats_sectors",
        "timing_stats_speeds",
    ]

    @staticmethod
    def _stack_dicts(data: List[dict]) -> dict:
        out = defaultdict(list)
        for i in data:
            for k, v in i.items():
                out[k].extend(v)

        return out

    @staticmethod
    def _lap_times_transformer(data: dict) -> dict:
        return {
            "Value": data.get("Value", None),
            "Lap": data.get("Lap", None),
            "Position": data.get("Position", None),
        }

    @staticmethod
    def _sectors_transformer(data: dict) -> dict:
        out = []

        for sector_key, sector_data in data.items():
            out.append(
                {
                    "SectorKey": sector_key,
                    "Value": sector_data.get("Value", None),
                    "Position": sector_data.get("Position", None),
                }
            )

        return out

    @staticmethod
    def _speeds_transformer(data: dict) -> dict:
        out = []

        for speed_key, speed_data in data.items():
            out.append(
                {
                    "SpeedTrapKey": speed_key,
                    "Value": speed_data.get("Value", None),
                    "Position": speed_data.get("Position", None),
                }
            )

        return out

    @staticmethod
    def _entry_transformer(driver: int, metrics: dict) -> dict:
        out = defaultdict(list)

        for metric, data in metrics.items():
            if metric == "PersonalBestLapTime":
                out["timing_stats_lap_times"].append(
                    TimingStatsProcessor._lap_times_transformer(data)
                )

            elif (metric == "BestSectors") and (type(data) is dict):
                out["timing_stats_sectors"].extend(
                    TimingStatsProcessor._sectors_transformer(data)
                )

            elif metric == "BestSpeeds":
                out["timing_stats_speeds"].extend(
                    TimingStatsProcessor._speeds_transformer(data)
                )

        for k, v in out.items():  # add driver key
            out[k] = list(map(lambda item: dict(item, Driver=driver), v))

        return out

    @staticmethod
    def _row_processor(stream_ts: str, lines: dict) -> List[dict]:
        output = []

        for driver, metrics in lines.items():
            if len(driver) == 0 or len(driver) > 2:
                continue

            output.append(TimingStatsProcessor._entry_transformer(int(driver), metrics))

        out = TimingStatsProcessor._stack_dicts(output)

        for k, v in out.items():  # add ts key
            out[k] = list(map(lambda item: dict(item, _StreamTimestamp=stream_ts), v))

        return out

    def _processor(self, data: List[dict]) -> pa.Table:
        schemas = {
            "timing_stats_lap_times": pa.schema(
                [
                    ("Value", pa.string()),
                    ("Lap", pa.int16()),
                    ("Position", pa.int16()),
                    ("Driver", pa.int16()),
                    ("_StreamTimestamp", pa.string()),
                ]
            ),
            "timing_stats_sectors": pa.schema(
                [
                    ("SectorKey", pa.string()),
                    ("Value", pa.string()),
                    ("Position", pa.int16()),
                    ("Driver", pa.int16()),
                    ("_StreamTimestamp", pa.string()),
                ]
            ),
            "timing_stats_speeds": pa.schema(
                [
                    ("SpeedTrapKey", pa.string()),
                    ("Value", pa.string()),
                    ("Position", pa.int16()),
                    ("Driver", pa.int16()),
                    ("_StreamTimestamp", pa.string()),
                ]
            ),
        }

        processed_data = []

        for i in data:
            if "Withheld" in i:  # first empty element
                continue

            processed_data.append(
                TimingStatsProcessor._row_processor(
                    stream_ts=i["_StreamTimestamp"], lines=i["Lines"]
                )
            )

        tables = TimingStatsProcessor._stack_dicts(processed_data)

        out = []
        for k, v in tables.items():
            if len(v) == 0:
                continue

            table = pa.Table.from_pylist(v).cast(schemas[k])
            out.append(LiveTimingAsset(key=k, output=table))

        return out


class TlaRcmProcessor(AbstractLiveTimingProcessor):
    source_asset = "tla_rcm"
    materializing_assets = ["tla_rcm"]

    def _processor(self, data: dict) -> List[LiveTimingAsset]:
        schema = pa.schema(
            [
                ("_StreamTimestamp", pa.string()),
                ("Timestamp", pa.string()),
                ("Message", pa.string()),
            ]
        )

        table = pa.Table.from_pylist(data).cast(schema)
        return [LiveTimingAsset(key="tla_rcm", output=table)]


class TrackStatusProcessor(AbstractLiveTimingProcessor):
    source_asset = "track_status"
    materializing_assets = ["track_status"]

    def _processor(self, data: dict) -> List[LiveTimingAsset]:
        schema = pa.schema(
            [
                ("_StreamTimestamp", pa.string()),
                ("Status", pa.string()),
                ("Message", pa.string()),
            ]
        )

        table = pa.Table.from_pylist(data).cast(schema)
        return [LiveTimingAsset(key="track_status", output=table)]


class TyreStintSeriesProcessor(AbstractLiveTimingProcessor):
    source_asset = "tyre_stint_series"
    materializing_assets = ["tyre_stint_series"]

    @staticmethod
    def _entry_transformer(driver: int, driver_stints: dict) -> List[dict]:
        out = []
        for stint, data in driver_stints.items():
            new = data["New"].lower() in ("true") if "New" in data else None
            tyres_not_changed = (
                bool(int(data["TyresNotChanged"]))
                if "TyresNotChanged" in data
                else None
            )

            out.append(
                {
                    "Driver": driver,
                    "Stint": stint,
                    "Compound": data.get("Compound", None),
                    "New": new,
                    "TyresNotChanged": tyres_not_changed,
                    "TotalLaps": data.get("TotalLaps", None),
                    "StartLaps": data.get("StartLaps", None),
                }
            )

        return out

    @staticmethod
    def _row_processor(stream_ts: str, stints: dict) -> List[dict]:
        out = []

        for driver, stint in stints.items():
            if isinstance(stint, list):
                # empty list at the start
                continue

            out.extend(TyreStintSeriesProcessor._entry_transformer(driver, stint))

        out = list(map(lambda item: dict(item, _StreamTimestamp=stream_ts), out))

        return out

    def _processor(self, data: List[dict]) -> pa.Table:
        schema = pa.schema(
            [
                ("Driver", pa.int16()),
                ("Stint", pa.int16()),
                ("Compound", pa.string()),
                ("New", pa.bool_()),
                ("TyresNotChanged", pa.bool_()),
                ("TotalLaps", pa.int16()),
                ("StartLaps", pa.int16()),
                ("_StreamTimestamp", pa.string()),
            ]
        )

        processed_data = []

        for i in data:
            processed_data.extend(
                TyreStintSeriesProcessor._row_processor(
                    stream_ts=i["_StreamTimestamp"], stints=i["Stints"]
                )
            )

        if len(processed_data) == 0:
            return [
                LiveTimingAsset(key="tyre_stint_series", output=schema.empty_table())
            ]

        table = pa.Table.from_pylist(processed_data).cast(schema)
        return [LiveTimingAsset(key="tyre_stint_series", output=table)]


class WeatherDataProcessor(AbstractLiveTimingProcessor):
    source_asset = "weather_data"
    materializing_assets = ["weather_data"]

    def _processor(self, data: dict) -> List[LiveTimingAsset]:
        schema = pa.schema(
            [
                ("_StreamTimestamp", pa.string()),
                ("AirTemp", pa.decimal128(5, 2)),
                ("Humidity", pa.decimal128(5, 2)),
                ("Pressure", pa.decimal128(6, 2)),
                ("Rainfall", pa.decimal128(5, 2)),
                ("TrackTemp", pa.decimal128(5, 2)),
                ("WindDirection", pa.int16()),
                ("WindSpeed", pa.decimal128(5, 2)),
            ]
        )

        table = pa.Table.from_pylist(data).cast(schema)
        return [LiveTimingAsset(key="weather_data", output=table)]


class LiveTimingProcessorBuilder:
    def __init__(self):
        self._processors: Dict[str, AbstractLiveTimingProcessor] = {
            "archive_status": ArchiveStatusProcessor,
            "audio_streams": AudioStreamsProcessor,
            "car_data": CarDataProcessor,
            "championship_prediction": ChampionshipPredictionProcessor,
            "current_tyres": CurrentTyresProcessor,
            "driver_list": DriverListProcessor,
            "driver_race_info": DriverRaceInfoProcessor,
            "extrapolated_clock": ExtrapolatedClockProcessor,
            "heartbeat": HeartbeatProcessor,
            "index": IndexProcessor,
            "lap_count": LapCountProcessor,
            "lap_series": LapSeriesProcessor,
            "pit_lane_time_collection": PitLaneTimeCollectionProcessor,
            "position": PositionProcessor,
            "race_control_messages": RaceControlMessagesProcessor,
            "session_data": SessionDataProcessor,
            "session_info": SessionInfoProcessor,
            "session_status": SessionStatusProcessor,
            "timing_data": TimingDataProcessor,
            "timing_stats": TimingStatsProcessor,
            "tla_rcm": TlaRcmProcessor,
            "track_status": TrackStatusProcessor,
            "tyre_stint_series": TyreStintSeriesProcessor,
            "weather_data": WeatherDataProcessor,
        }

    @property
    def processors(self) -> List[str]:
        return self._processors.keys()

    @property
    def assets(self) -> List[str]:
        out = []
        for i in self._processors.values():
            out.extend(i.materializing_assets)
        return out

    def assets_definition(self, table: str, **kwargs):
        processor = self._processors.get(table)
        assets = dict([(i, AssetOut(**kwargs)) for i in processor.materializing_assets])
        return assets

    def build(
        self,
        table: str,
        metadata: LiveTimingSessionMetadata,
        context: OpExecutionContext,
    ) -> AbstractLiveTimingProcessor:
        processor = self._processors.get(table, None)
        return processor(context, metadata)
