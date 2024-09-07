import copy
import json
import unittest
from typing import List
from unittest.mock import MagicMock

import duck_f1.pipelines.assets.live_timing.processors as processors


def get_json_data(key: str) -> dict:
    base_path = "./tests/unit/assets"
    asset_key = f"live_timing_{key}.json"
    file_path = "/".join([base_path, asset_key])
    with open(file_path, "r", encoding="utf-8") as file:
        data = json.load(file)

    return data


def get_output(
    key: str, outputs: List[processors.LiveTimingAsset]
) -> processors.LiveTimingAsset:
    for i in outputs:
        if i.key == key:
            return i

    return None


class TestLiveTimingArchiveStatusProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        data = get_json_data("archive_status")
        processor = processors.ArchiveStatusProcessor(MagicMock(), MagicMock())
        cls.outputs = processor._processor(data)

    def test_live_timing_archive_status_assets(self):
        count = len(self.outputs)
        asset = self.outputs[0]

        self.assertEqual(count, 1)
        self.assertEqual(asset.key, "archive_status")

    def test_live_timing_archive_status_shape(self):
        shape = self.outputs[0].output.shape
        self.assertEqual(shape, (1, 1))

    def test_live_timing_archive_status_columns(self):
        columns = self.outputs[0].output.column_names
        expected_columns = ["Status"]

        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingAudioStreamsProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        data = get_json_data("audio_streams")
        processor = processors.AudioStreamsProcessor(MagicMock(), MagicMock())
        cls.outputs = processor._processor(data)

    def test_live_timing_audio_streams_assets(self):
        count = len(self.outputs)
        asset = self.outputs[0]

        self.assertEqual(count, 1)
        self.assertEqual(asset.key, "audio_streams")

    def test_live_timing_audio_streams_shape(self):
        shape = self.outputs[0].output.shape
        self.assertEqual(shape, (1, 6))

    def test_live_timing_audio_streams_columns(self):
        columns = self.outputs[0].output.column_names
        expected_columns = [
            "Name",
            "Language",
            "Uri",
            "Path",
            "Utc",
            "_StreamTimestamp",
        ]

        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingCarDataProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = get_json_data("car_data")
        processor = processors.CarDataProcessor(MagicMock(), MagicMock())
        cls.outputs = processor._processor(cls.data)

    def test_live_timing_car_data_assets(self):
        count = len(self.outputs)
        asset = self.outputs[0]

        self.assertEqual(count, 1)
        self.assertEqual(asset.key, "car_data")

    def test_live_timing_car_data_shape(self):
        shape = self.outputs[0].output.shape
        rows = 0
        for row in self.data:
            rows += len(row["Entries"])

        self.assertEqual(shape, (rows * 20, 9))  # 20 drivers, 6 channels

    def test_live_timing_car_data_columns(self):
        columns = self.outputs[0].output.column_names
        expected_columns = [
            "CaptureTimestamp",
            "CarNumber",
            "EngineRpm",
            "CarSpeed",
            "EngineGear",
            "ThrottlePosition",
            "BrakePosition",
            "DrsStatus",
            "_StreamTimestamp",
        ]
        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingChampionshipPredictionProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        data = get_json_data("championship_prediction")
        processor = processors.ChampionshipPredictionProcessor(MagicMock(), MagicMock())
        cls.outputs = processor._processor(data)

    def test_live_timing_championship_prediction_assets(self):
        count = len(self.outputs)
        asset = self.outputs[0]

        self.assertEqual(count, 1)
        self.assertEqual(asset.key, "championship_prediction")

    def test_live_timing_championship_prediction_shape(self):
        shape = self.outputs[0].output.shape
        self.assertEqual(shape, (137, 5))

    def test_live_timing_championship_prediction_columns(self):
        columns = self.outputs[0].output.column_names
        expected_columns = [
            "Entity",
            "Identifier",
            "Metric",
            "Value",
            "_StreamTimestamp",
        ]

        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingCurrentTyresProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = get_json_data("current_tyres")
        processor = processors.CurrentTyresProcessor(MagicMock(), MagicMock())
        cls.outputs = processor._processor(cls.data)

    def test_live_timing_current_tyres_assets(self):
        count = len(self.outputs)
        asset = self.outputs[0]

        self.assertEqual(count, 1)
        self.assertEqual(asset.key, "current_tyres")

    def test_live_timing_current_tyres_shape(self):
        shape = self.outputs[0].output.shape
        rows = 0
        for row in self.data:
            rows += len(row["Tyres"])

        self.assertEqual(shape, (rows, 4))

    def test_live_timing_current_tyres_columns(self):
        columns = self.outputs[0].output.column_names
        expected_columns = [
            "Driver",
            "Compound",
            "New",
            "_StreamTimestamp",
        ]

        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingDriverListProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = get_json_data("driver_list")
        processor = processors.DriverListProcessor(MagicMock(), MagicMock())
        cls.outputs = processor._processor(copy.deepcopy(cls.data))

    def test_live_timing_driver_list_assets(self):
        count = len(self.outputs)
        asset = self.outputs[0]

        self.assertEqual(count, 1)
        self.assertEqual(asset.key, "driver_list")

    def test_live_timing_driver_list_shape(self):
        shape = self.outputs[0].output.shape
        rows = 0
        for row in self.data[:1]:
            rows += len(row) - 1

        self.assertEqual(shape, (rows, 12))

    def test_live_timing_driver_list_columns(self):
        columns = self.outputs[0].output.column_names
        expected_columns = [
            "_StreamTimestamp",
            "RacingNumber",
            "BroadcastName",
            "FullName",
            "Tla",
            "Line",
            "TeamName",
            "TeamColour",
            "FirstName",
            "LastName",
            "Reference",
            "HeadshotUrl",
        ]

        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingDriverRaceInfoProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = get_json_data("driver_race_info")
        processor = processors.DriverRaceInfoProcessor(MagicMock(), MagicMock())
        cls.outputs = processor._processor(copy.deepcopy(cls.data))

    def test_live_timing_driver_race_info_assets(self):
        count = len(self.outputs)
        asset = self.outputs[0]

        self.assertEqual(count, 1)
        self.assertEqual(asset.key, "driver_race_info")

    def test_live_timing_driver_race_info_shape(self):
        shape = self.outputs[0].output.shape
        rows = 0
        for row in self.data:
            rows += len(row) - 1

        self.assertEqual(shape, (rows, 9))

    def test_live_timing_driver_race_info_columns(self):
        columns = self.outputs[0].output.column_names
        expected_columns = [
            "Driver",
            "Position",
            "Gap",
            "Interval",
            "PitStops",
            "Catching",
            "OvertakeState",
            "IsOut",
            "_StreamTimestamp",
        ]

        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingExtrapolatedClockProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = get_json_data("extrapolated_clock")
        processor = processors.ExtrapolatedClockProcessor(MagicMock(), MagicMock())
        cls.outputs = processor._processor(cls.data)

    def test_live_timing_extrapolated_clock_assets(self):
        count = len(self.outputs)
        asset = self.outputs[0]

        self.assertEqual(count, 1)
        self.assertEqual(asset.key, "extrapolated_clock")

    def test_live_timing_extrapolated_clock_shape(self):
        shape = self.outputs[0].output.shape

        self.assertEqual(shape, (len(self.data), 4))

    def test_live_timing_extrapolated_clock_columns(self):
        columns = self.outputs[0].output.column_names
        expected_columns = ["_StreamTimestamp", "Utc", "Remaining", "Extrapolating"]

        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingHeartbeatProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = get_json_data("heartbeat")
        processor = processors.HeartbeatProcessor(MagicMock(), MagicMock())
        cls.outputs = processor._processor(cls.data)

    def test_live_timing_heartbeat_assets(self):
        count = len(self.outputs)
        asset = self.outputs[0]

        self.assertEqual(count, 1)
        self.assertEqual(asset.key, "heartbeat")

    def test_live_timing_heartbeat_shape(self):
        shape = self.outputs[0].output.shape

        self.assertEqual(shape, (len(self.data), 2))

    def test_live_timing_heartbeat_columns(self):
        columns = self.outputs[0].output.column_names
        expected_columns = ["_StreamTimestamp", "Utc"]

        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingIndexProcessorProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = get_json_data("index")
        processor = processors.IndexProcessor(MagicMock(), MagicMock())
        cls.outputs = processor._processor(cls.data)

    def test_live_timing_index_assets(self):
        count = len(self.outputs)
        asset = self.outputs[0]

        self.assertEqual(count, 1)
        self.assertEqual(asset.key, "index")

    def test_live_timing_index_shape(self):
        shape = self.outputs[0].output.shape
        rows = 0
        for row in self.data:
            rows += len(row["Feeds"])

        self.assertEqual(shape, (rows, 2))

    def test_live_timing_index_columns(self):
        columns = self.outputs[0].output.column_names
        expected_columns = ["KeyFramePath", "StreamPath"]

        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingLapCountProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = get_json_data("lap_count")
        processor = processors.LapCountProcessor(MagicMock(), MagicMock())
        cls.outputs = processor._processor(copy.deepcopy(cls.data))

    def test_live_timing_lap_count_assets(self):
        count = len(self.outputs)
        asset = self.outputs[0]

        self.assertEqual(count, 1)
        self.assertEqual(asset.key, "lap_count")

    def test_live_timing_lap_count_shape(self):
        shape = self.outputs[0].output.shape
        rows = 0
        for row in self.data:
            rows += len(row) - 1

        self.assertEqual(shape, (rows, 3))

    def test_live_timing_lap_count_columns(self):
        columns = self.outputs[0].output.column_names
        expected_columns = ["_StreamTimestamp", "Metric", "Value"]

        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingLapSeriesProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = get_json_data("lap_series")
        processor = processors.LapSeriesProcessor(MagicMock(), MagicMock())
        cls.outputs = processor._processor(copy.deepcopy(cls.data))

    def test_live_timing_lap_series_assets(self):
        count = len(self.outputs)
        asset = self.outputs[0]

        self.assertEqual(count, 1)
        self.assertEqual(asset.key, "lap_series")

    def test_live_timing_lap_series_shape(self):
        shape = self.outputs[0].output.shape
        rows = 0
        for row in self.data:
            rows += len(row) - 1

        self.assertEqual(shape, (rows, 4))

    def test_live_timing_lap_series_columns(self):
        columns = self.outputs[0].output.column_names
        expected_columns = [
            "DriverNumber",
            "LapNumber",
            "LapPosition",
            "_StreamTimestamp",
        ]

        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingPitLaneTimeCollectionProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = get_json_data("pit_lane_time_collection")
        processor = processors.PitLaneTimeCollectionProcessor(MagicMock(), MagicMock())
        cls.outputs = processor._processor(cls.data)

    def test_live_timing_pit_lane_time_collection_assets(self):
        count = len(self.outputs)
        asset = self.outputs[0]

        self.assertEqual(count, 1)
        self.assertEqual(asset.key, "pit_lane_time_collection")

    def test_live_timing_pit_lane_time_collection_shape(self):
        shape = self.outputs[0].output.shape
        rows = 0
        for row in self.data:
            rows += len(row["PitTimes"])

        self.assertEqual(shape, (rows - 1, 4))

    def test_live_timing_pit_lane_time_collection_columns(self):
        columns = self.outputs[0].output.column_names
        expected_columns = ["Driver", "Duration", "Lap", "_StreamTimestamp"]

        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingPositionProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = get_json_data("position")
        processor = processors.PositionProcessor(MagicMock(), MagicMock())
        cls.outputs = processor._processor(cls.data)

    def test_live_timing_position_assets(self):
        count = len(self.outputs)
        asset = self.outputs[0]

        self.assertEqual(count, 1)
        self.assertEqual(asset.key, "position")

    def test_live_timing_position_shape(self):
        shape = self.outputs[0].output.shape
        rows = 0
        for row in self.data:
            rows += len(row["Position"])

        self.assertEqual(shape, (rows * 20, 7))  # 20 drivers

    def test_live_timing_position_columns(self):
        columns = self.outputs[0].output.column_names
        expected_columns = [
            "Timestamp",
            "Driver",
            "Status",
            "X",
            "Y",
            "Z",
            "_StreamTimestamp",
        ]
        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingRaceControlMessagesProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = get_json_data("race_control_messages")
        processor = processors.RaceControlMessagesProcessor(MagicMock(), MagicMock())
        cls.outputs = processor._processor(copy.deepcopy(cls.data))

    def test_live_timing_race_control_messages_assets(self):
        count = len(self.outputs)
        asset = self.outputs[0]

        self.assertEqual(count, 1)
        self.assertEqual(asset.key, "race_control_messages")

    def test_live_timing_race_control_messages_shape(self):
        shape = self.outputs[0].output.shape
        rows = 0
        for row in self.data:
            rows += len(row["Messages"])

        self.assertEqual(shape, (rows, 6))

    def test_live_timing_race_control_messages_columns(self):
        columns = self.outputs[0].output.column_names
        expected_columns = [
            "MessageId",
            "Utc",
            "Lap",
            "Category",
            "MessageData",
            "_StreamTimestamp",
        ]
        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingSessionDataProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = get_json_data("session_data")
        processor = processors.SessionDataProcessor(MagicMock(), MagicMock())
        cls.outputs = processor._processor(copy.deepcopy(cls.data))

    def test_live_timing_session_data_assets(self):
        count = len(self.outputs)
        asset = self.outputs[0]

        self.assertEqual(count, 1)
        self.assertEqual(asset.key, "session_data")

    def test_live_timing_session_data_shape(self):
        shape = self.outputs[0].output.shape
        rows = 0
        for row in self.data:
            rows += len(row["Series"]) + len(row["StatusSeries"])

        self.assertEqual(shape, (rows, 4))

    def test_live_timing_session_data_columns(self):
        columns = self.outputs[0].output.column_names
        expected_columns = [
            "Key",
            "Utc",
            "MetricName",
            "MetricValue",
        ]
        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingSessionInfoProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = get_json_data("session_info")
        processor = processors.SessionInfoProcessor(MagicMock(), MagicMock())
        cls.outputs = processor._processor(cls.data)

    def test_live_timing_session_info_assets(self):
        count = len(self.outputs)
        asset = self.outputs[0]

        self.assertEqual(count, 1)
        self.assertEqual(asset.key, "session_info")

    def test_live_timing_session_info_shape(self):
        shape = self.outputs[0].output.shape
        self.assertEqual(shape, (1, 18))

    def test_live_timing_session_info_columns(self):
        columns = self.outputs[0].output.column_names
        expected_columns = [
            "MeetingKey",
            "MeetingName",
            "MeetingOfficialName",
            "MeetingLocation",
            "MeetingCountryKey",
            "MeetingCountryCode",
            "MeetingCountryName",
            "MeetingCircuitKey",
            "MeetingCircuitShortName",
            "ArchiveStatusStatus",
            "Key",
            "Type",
            "Name",
            "StartDate",
            "EndDate",
            "GmtOffset",
            "Path",
            "Number",
        ]

        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingSessionStatusProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        data = get_json_data("session_status")
        processor = processors.SessionStatusProcessor(MagicMock(), MagicMock())
        cls.outputs = processor._processor(data)

    def test_live_timing_session_status_assets(self):
        count = len(self.outputs)
        asset = self.outputs[0]

        self.assertEqual(count, 1)
        self.assertEqual(asset.key, "session_status")

    def test_live_timing_tla_rcm_shape(self):
        shape = self.outputs[0].output.shape
        self.assertEqual(shape, (5, 2))

    def test_live_timing_tla_rcm_columns(self):
        columns = self.outputs[0].output.column_names
        expected_columns = ["_StreamTimestamp", "Status"]
        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingTimingDataProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = get_json_data("timing_data")
        processor = processors.TimingDataProcessor(MagicMock(), MagicMock())
        cls.outputs = processor._processor(copy.deepcopy(cls.data))

    def test_live_timing_timing_data_assets(self):
        expected_output_keys = processors.TimingDataProcessor.materializing_assets

        output_count = len(self.outputs)
        output_keys = [i.key for i in self.outputs]

        self.assertEqual(output_count, len(expected_output_keys))
        self.assertCountEqual(output_keys, expected_output_keys)

    def test_live_timing_timing_data_status_shape(self):
        output = get_output("timing_data_status", self.outputs).output
        shape = output.shape
        self.assertEqual(shape, (11, 4))

    def test_live_timing_timing_data_status_columns(self):
        columns = get_output("timing_data_status", self.outputs).output.column_names
        expected_columns = ["MetricName", "MetricValue", "Driver", "_StreamTimestamp"]
        self.assertCountEqual(columns, expected_columns)

    def test_live_timing_timing_data_interval_shape(self):
        output = get_output("timing_data_interval", self.outputs).output
        shape = output.shape
        self.assertEqual(shape, (6, 4))

    def test_live_timing_timing_data_interval_columns(self):
        columns = get_output("timing_data_interval", self.outputs).output.column_names
        expected_columns = ["Value", "Catching", "Driver", "_StreamTimestamp"]
        self.assertCountEqual(columns, expected_columns)

    def test_live_timing_timing_data_best_lap_shape(self):
        output = get_output("timing_data_best_lap", self.outputs).output
        shape = output.shape
        self.assertEqual(shape, (2, 4))

    def test_live_timing_timing_data_best_lap_columns(self):
        columns = get_output("timing_data_best_lap", self.outputs).output.column_names
        expected_columns = ["Value", "Lap", "Driver", "_StreamTimestamp"]
        self.assertCountEqual(columns, expected_columns)

    def test_live_timing_timing_data_last_lap_shape(self):
        output = get_output("timing_data_last_lap", self.outputs).output
        shape = output.shape
        self.assertEqual(shape, (2, 6))

    def test_live_timing_timing_data_last_lap_columns(self):
        columns = get_output("timing_data_last_lap", self.outputs).output.column_names
        expected_columns = [
            "Value",
            "Status",
            "OverallFastest",
            "PersonalFastest",
            "Driver",
            "_StreamTimestamp",
        ]
        self.assertCountEqual(columns, expected_columns)

    def test_live_timing_timing_data_sectors_shape(self):
        output = get_output("timing_data_sectors", self.outputs).output
        shape = output.shape
        self.assertEqual(shape, (72, 9))

    def test_live_timing_timing_data_sectors_columns(self):
        columns = get_output("timing_data_sectors", self.outputs).output.column_names
        expected_columns = [
            "SectorKey",
            "Stopped",
            "Value",
            "PreviousValue",
            "Status",
            "OverallFastest",
            "PersonalFastest",
            "Driver",
            "_StreamTimestamp",
        ]
        self.assertCountEqual(columns, expected_columns)

    def test_live_timing_timing_data_sector_segments_shape(self):
        output = get_output("timing_data_sector_segments", self.outputs).output
        shape = output.shape
        self.assertEqual(shape, (33, 5))

    def test_live_timing_timing_data_sector_segments_columns(self):
        columns = get_output(
            "timing_data_sector_segments", self.outputs
        ).output.column_names
        expected_columns = [
            "SectorKey",
            "SegmentKey",
            "Status",
            "Driver",
            "_StreamTimestamp",
        ]
        self.assertCountEqual(columns, expected_columns)

    def test_live_timing_timing_data_speeds_shape(self):
        output = get_output("timing_data_speeds", self.outputs).output
        shape = output.shape
        self.assertEqual(shape, (5, 7))

    def test_live_timing_timing_data_speeds_columns(self):
        columns = get_output("timing_data_speeds", self.outputs).output.column_names
        expected_columns = [
            "SpeedKey",
            "Value",
            "Status",
            "OverallFastest",
            "PersonalFastest",
            "Driver",
            "_StreamTimestamp",
        ]
        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingTimingStatsProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = get_json_data("timing_stats")
        processor = processors.TimingStatsProcessor(MagicMock(), MagicMock())
        cls.outputs = processor._processor(copy.deepcopy(cls.data))

    def test_live_timing_timing_stats_assets(self):
        expected_output_keys = processors.TimingStatsProcessor.materializing_assets

        output_count = len(self.outputs)
        output_keys = [i.key for i in self.outputs]

        self.assertEqual(output_count, len(expected_output_keys))
        self.assertCountEqual(output_keys, expected_output_keys)

    def test_live_timing_timing_stats_lap_times(self):
        output = get_output("timing_stats_lap_times", self.outputs).output
        shape = output.shape
        self.assertEqual(shape, (1, 5))

    def test_live_timing_timing_stats_lap_times_columns(self):
        columns = get_output("timing_stats_lap_times", self.outputs).output.column_names
        expected_columns = ["Value", "Lap", "Position", "Driver", "_StreamTimestamp"]
        self.assertCountEqual(columns, expected_columns)

    def test_live_timing_timing_stats_sectors(self):
        output = get_output("timing_stats_sectors", self.outputs).output
        shape = output.shape
        self.assertEqual(shape, (1, 5))

    def test_live_timing_timing_stats_sectors_columns(self):
        columns = get_output("timing_stats_sectors", self.outputs).output.column_names
        expected_columns = [
            "SectorKey",
            "Value",
            "Position",
            "Driver",
            "_StreamTimestamp",
        ]
        self.assertCountEqual(columns, expected_columns)

    def test_live_timing_timing_stats_speeds(self):
        output = get_output("timing_stats_speeds", self.outputs).output
        shape = output.shape
        self.assertEqual(shape, (5, 5))

    def test_live_timing_timing_stats_speeds_columns(self):
        columns = get_output("timing_stats_speeds", self.outputs).output.column_names
        expected_columns = [
            "SpeedTrapKey",
            "Value",
            "Position",
            "Driver",
            "_StreamTimestamp",
        ]
        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingTlaRcmProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        data = get_json_data("tla_rcm")
        processor = processors.TlaRcmProcessor(MagicMock(), MagicMock())
        cls.outputs = processor._processor(data)

    def test_live_timing_tla_rcm_assets(self):
        count = len(self.outputs)
        asset = self.outputs[0]

        self.assertEqual(count, 1)
        self.assertEqual(asset.key, "tla_rcm")

    def test_live_timing_tla_rcm_shape(self):
        shape = self.outputs[0].output.shape
        self.assertEqual(shape, (5, 3))

    def test_live_timing_tla_rcm_columns(self):
        columns = self.outputs[0].output.column_names
        expected_columns = ["_StreamTimestamp", "Timestamp", "Message"]
        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingTrackStatusProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        data = get_json_data("track_status")
        processor = processors.TrackStatusProcessor(MagicMock(), MagicMock())
        cls.outputs = processor._processor(data)

    def test_live_timing_track_status_assets(self):
        count = len(self.outputs)
        asset = self.outputs[0]

        self.assertEqual(count, 1)
        self.assertEqual(asset.key, "track_status")

    def test_live_timing_track_status_shape(self):
        shape = self.outputs[0].output.shape
        self.assertEqual(shape, (5, 3))

    def test_live_timing_track_status_columns(self):
        columns = self.outputs[0].output.column_names
        expected_columns = ["_StreamTimestamp", "Status", "Message"]
        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingTyreStintSeriesProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = get_json_data("tyre_stint_series")
        processor = processors.TyreStintSeriesProcessor(MagicMock(), MagicMock())
        cls.outputs = processor._processor(copy.deepcopy(cls.data))

    def test_live_timing_tyre_stint_series_assets(self):
        count = len(self.outputs)
        asset = self.outputs[0]

        self.assertEqual(count, 1)
        self.assertEqual(asset.key, "tyre_stint_series")

    def test_live_timing_tyre_stint_series_shape(self):
        shape = self.outputs[0].output.shape
        rows = 0
        for row in self.data:
            for stints in row["Stints"].values():
                if isinstance(stints, list):
                    continue

                rows += len(stints)

        self.assertEqual(shape, (rows, 8))

    def test_live_timing_tyre_stint_series_columns(self):
        columns = self.outputs[0].output.column_names
        expected_columns = [
            "Driver",
            "Stint",
            "Compound",
            "New",
            "TyresNotChanged",
            "TotalLaps",
            "StartLaps",
            "_StreamTimestamp",
        ]
        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingWeatherDataProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        data = get_json_data("weather_data")
        processor = processors.WeatherDataProcessor(MagicMock(), MagicMock())
        cls.outputs = processor._processor(data)

    def test_live_timing_weather_data_assets(self):
        count = len(self.outputs)
        asset = self.outputs[0]

        self.assertEqual(count, 1)
        self.assertEqual(asset.key, "weather_data")

    def test_live_timing_weather_data_shape(self):
        shape = self.outputs[0].output.shape
        self.assertEqual(shape, (5, 8))

    def test_live_timing_weather_data_columns(self):
        columns = self.outputs[0].output.column_names
        expected_columns = [
            "_StreamTimestamp",
            "AirTemp",
            "Humidity",
            "Pressure",
            "Rainfall",
            "TrackTemp",
            "WindDirection",
            "WindSpeed",
        ]
        self.assertCountEqual(columns, expected_columns)
