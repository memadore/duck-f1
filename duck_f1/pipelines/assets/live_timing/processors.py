import io
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List

import pyarrow as pa
from dagster import OpExecutionContext

from .partitions import LiveTimingPartitionMetadata


class AbstractLiveTimingProcessor(ABC):
    def __init__(self, context: OpExecutionContext, metadata: LiveTimingPartitionMetadata) -> None:
        self.context = context
        self.metadata = metadata

    @abstractmethod
    def _processor(self, data: io.BytesIO) -> pa.Table:
        pass

    def _add_metadata(self, table: pa.Table) -> pa.Table:
        columns = self.metadata.dict()
        self.context.log.info("Metadata: %s", columns)
        table_len = table.num_rows
        for col, value in columns.items():
            match value:
                case int():
                    column_type = pa.int32()
                    column_value = value
                case datetime():
                    column_type = pa.timestamp("s")
                    column_value = value
                case _:
                    column_type = pa.string()
                    column_value = str(value)

            table = table.append_column(col, pa.array([column_value] * table_len, column_type))

        return table

    def run(self, data: io.BytesIO) -> pa.Table:
        table = self._processor(data)
        table = self._add_metadata(table)
        return table


class ArchiveStatusProcessor(AbstractLiveTimingProcessor):
    def _processor(self, data: dict) -> pa.Table:
        schema = pa.schema([("Status", pa.string())])

        table = pa.Table.from_pylist(data).cast(schema)
        return table


class AudioStreamsProcessor(AbstractLiveTimingProcessor):
    @staticmethod
    def _row_processor(ts: str, streams: List[dict]) -> List[dict]:
        out = list(map(lambda item: dict(item, ts=ts), streams))
        return out

    def _processor(self, data: dict) -> pa.Table:
        schema = pa.schema(
            [
                ("Name", pa.string()),
                ("Language", pa.string()),
                ("Uri", pa.string()),
                ("Path", pa.string()),
                ("Utc", pa.string()),
                ("ts", pa.string()),
            ]
        )

        processed_data = []

        for i in data:
            processed_data.extend(
                AudioStreamsProcessor._row_processor(ts=i["ts"], streams=i["Streams"])
            )

        table = pa.Table.from_pylist(processed_data).cast(schema)
        return table


class CarDataProcessor(AbstractLiveTimingProcessor):
    @staticmethod
    def _explode(capture_ts: str, car_number: str, channel_data: dict):
        out = []
        for channel, value in channel_data.items():
            out.append(
                {
                    "capture_ts": capture_ts,
                    "car_number": car_number,
                    "channel": channel,
                    "value": value,
                }
            )
        return out

    @staticmethod
    def _entry_transformer(entry: dict) -> List[dict]:
        out = []
        capture_ts = entry["Utc"]
        for car_number, car_data in entry["Cars"].items():
            records = CarDataProcessor._explode(
                capture_ts=capture_ts, car_number=car_number, channel_data=car_data["Channels"]
            )
            out.extend(records)

        return out

    @staticmethod
    def _row_processor(ts: str, entries: List[dict]) -> List[dict]:
        out = []
        for entry in entries:
            out.extend(CarDataProcessor._entry_transformer(entry))

        out = list(map(lambda item: dict(item, ts=ts), out))
        return out

    def _processor(self, data: List[dict]) -> pa.Table:
        schema = pa.schema(
            [
                ("capture_ts", pa.string()),
                ("car_number", pa.int16()),
                ("channel", pa.int16()),
                # ("value", pa.decimal128(5, 2)),
                ("value", pa.int16()),
                ("ts", pa.string()),
            ]
        )

        processed_data = []

        for i in data:
            processed_data.extend(CarDataProcessor._row_processor(ts=i["ts"], entries=i["Entries"]))

        table = pa.Table.from_pylist(processed_data).cast(schema)
        return table


class ChampionshipPredictionProcessor(AbstractLiveTimingProcessor):
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
                    "entity": entity,
                    "identifier": identifier,
                    "metric": key,
                    "value": value,
                }
            )

        return out

    @staticmethod
    def _row_processor(ts: str, entity: str, data: dict) -> List[dict]:
        out = []
        for key, value in data.items():
            if len(key) == 0:
                continue
            out.extend(ChampionshipPredictionProcessor._explode(entity, key, value))

        out = list(map(lambda item: dict(item, ts=ts), out))
        return out

    def _processor(self, data: dict) -> pa.Table:
        schema = pa.schema(
            [
                ("entity", pa.string()),
                ("identifier", pa.string()),
                ("metric", pa.string()),
                ("value", pa.decimal128(5, 2)),
                ("ts", pa.string()),
            ]
        )

        processed_data = []

        for i in data:
            if "Drivers" in i:
                processed_data.extend(
                    ChampionshipPredictionProcessor._row_processor(
                        ts=i["ts"], entity="driver", data=i["Drivers"]
                    )
                )

            if "Teams" in i:
                processed_data.extend(
                    ChampionshipPredictionProcessor._row_processor(
                        ts=i["ts"], entity="team", data=i["Teams"]
                    )
                )

        table = pa.Table.from_pylist(processed_data).cast(schema)
        return table


class CurrentTyresProcessor(AbstractLiveTimingProcessor):
    @staticmethod
    def _row_processor(ts: str, tyres: List[dict]) -> List[dict]:
        out = []
        for driver, data in tyres.items():
            if len(driver) == 0:
                continue

            out.append(
                {
                    "Driver": driver,
                    "Compound": data.get("Compound", "UNKNOWN"),
                    "New": data.get("New", None),
                }
            )

        out = list(map(lambda item: dict(item, ts=ts), out))
        return out

    def _processor(self, data: dict) -> pa.Table:
        schema = pa.schema(
            [
                ("Driver", pa.string()),
                ("Compound", pa.string()),
                ("New", pa.bool_()),
                ("ts", pa.string()),
            ]
        )

        processed_data = []

        for i in data:
            processed_data.extend(
                CurrentTyresProcessor._row_processor(ts=i["ts"], tyres=i["Tyres"])
            )

        table = pa.Table.from_pylist(processed_data).cast(schema)
        return table


class DriverRaceInfoProcessor(AbstractLiveTimingProcessor):
    @staticmethod
    def _row_processor(data: dict) -> List[dict]:
        out = []
        ts = data.pop("ts")
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
                    "ts": ts,
                }
            )

        out = list(map(lambda item: dict(item, ts=ts), out))
        return out

    def _processor(self, data: dict) -> pa.Table:
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
                ("ts", pa.string()),
            ]
        )

        processed_data = []

        for i in data:
            processed_data.extend(DriverRaceInfoProcessor._row_processor(i))

        table = pa.Table.from_pylist(processed_data).cast(schema)
        return table


class ExtrapolatedClockProcessor(AbstractLiveTimingProcessor):
    def _processor(self, data: dict) -> pa.Table:
        schema = pa.schema(
            [
                ("ts", pa.string()),
                ("Utc", pa.string()),
                ("Remaining", pa.string()),
                ("Extrapolating", pa.bool_()),
            ]
        )

        table = pa.Table.from_pylist(data).cast(schema)
        return table


class HeartbeatProcessor(AbstractLiveTimingProcessor):
    def _processor(self, data: dict) -> pa.Table:
        schema = pa.schema([("ts", pa.string()), ("Utc", pa.string())])

        table = pa.Table.from_pylist(data).cast(schema)
        return table


class IndexProcessor(AbstractLiveTimingProcessor):
    @staticmethod
    def _feed_processor(feeds: [dict]) -> List[dict]:
        out = []
        for _, value in feeds.items():
            out.append(value)

        return out

    def _processor(self, data: dict) -> pa.Table:
        schema = pa.schema([("KeyFramePath", pa.string()), ("StreamPath", pa.string())])

        processed_data = []

        for i in data:
            processed_data.extend(IndexProcessor._feed_processor(feeds=i["Feeds"]))

        table = pa.Table.from_pylist(processed_data).cast(schema)
        return table


class LapCountProcessor(AbstractLiveTimingProcessor):
    @staticmethod
    def _row_processor(data: dict) -> List[dict]:
        out = []
        ts = data.pop("ts")
        for key, value in data.items():
            if len(key) == 0:
                continue

            out.append({"ts": ts, "metric": key, "value": value})

        out = list(map(lambda item: dict(item, ts=ts), out))
        return out

    def _processor(self, data: dict) -> pa.Table:
        schema = pa.schema(
            [
                ("ts", pa.string()),
                ("metric", pa.string()),
                ("value", pa.int16()),
            ]
        )

        processed_data = []

        for i in data:
            processed_data.extend(LapCountProcessor._row_processor(i))

        table = pa.Table.from_pylist(processed_data).cast(schema)
        return table


class PositionProcessor(AbstractLiveTimingProcessor):
    @staticmethod
    def _entry_transformer(entry: dict) -> List[dict]:
        out = []
        capture_ts = entry["Timestamp"]
        for driver_number, position_data in entry["Entries"].items():
            out.append({"Timestamp": capture_ts, "Driver": driver_number, **position_data})

        return out

    @staticmethod
    def _row_processor(ts: str, positions: List[dict]) -> List[dict]:
        out = []
        for i in positions:
            out.extend(PositionProcessor._entry_transformer(i))

        out = list(map(lambda item: dict(item, ts=ts), out))
        return out

    def _processor(self, data: List[dict]) -> pa.Table:
        schema = pa.schema(
            [
                ("Timestamp", pa.string()),
                ("Driver", pa.string()),
                ("Status", pa.string()),
                ("X", pa.int16()),
                ("Y", pa.int16()),
                ("Z", pa.int16()),
                ("ts", pa.string()),
            ]
        )

        processed_data = []

        for i in data:
            processed_data.extend(
                PositionProcessor._row_processor(ts=i["ts"], positions=i["Position"])
            )

        table = pa.Table.from_pylist(processed_data).cast(schema)
        return table


class WeatherDataProcessor(AbstractLiveTimingProcessor):
    def _processor(self, data: dict) -> pa.Table:
        schema = pa.schema(
            [
                ("ts", pa.string()),
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
        return table


class LiveTimingProcessorBuilder:
    def __init__(self):
        self._processors = {
            "archive_status": ArchiveStatusProcessor,
            "audio_streams": AudioStreamsProcessor,
            "car_data": CarDataProcessor,
            "championship_prediction": ChampionshipPredictionProcessor,
            "current_tyres": CurrentTyresProcessor,
            "driver_race_info": DriverRaceInfoProcessor,
            "extrapolated_clock": ExtrapolatedClockProcessor,
            "heartbeat": HeartbeatProcessor,
            "index": IndexProcessor,
            "lap_count": LapCountProcessor,
            "position": PositionProcessor,
            "weather_data": WeatherDataProcessor,
        }

    @property
    def processors(self) -> List[str]:
        return self._processors.keys()

    def build(
        self, table: str, metadata: LiveTimingPartitionMetadata, context: OpExecutionContext
    ) -> AbstractLiveTimingProcessor:
        processor = self._processors.get(table, None)
        return processor(context, metadata)
