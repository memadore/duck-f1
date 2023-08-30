import copy
import json
import unittest
from unittest.mock import MagicMock

import duck_f1.pipelines.assets.live_timing.processors as processors


def get_json_data(key: str) -> dict:
    base_path = "./tests/unit/assets"
    asset_key = f"live_timing_{key}.json"
    file_path = "/".join([base_path, asset_key])
    with open(file_path, "r", encoding="utf-8") as file:
        data = json.load(file)

    return data


class TestLiveTimingArchiveStatusProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        data = get_json_data("archive_status")
        processor = processors.ArchiveStatusProcessor(MagicMock(), MagicMock())
        cls.output = processor._processor(data)

    def test_live_timing_archive_status_shape(self):
        shape = self.output.shape
        self.assertEqual(shape, (1, 1))

    def test_live_timing_archive_status_columns(self):
        columns = self.output.column_names
        expected_columns = ["Status"]

        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingAudioStreamsProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        data = get_json_data("audio_streams")
        processor = processors.AudioStreamsProcessor(MagicMock(), MagicMock())
        cls.output = processor._processor(data)

    def test_live_timing_audio_streams_shape(self):
        shape = self.output.shape
        self.assertEqual(shape, (1, 6))

    def test_live_timing_audio_streams_columns(self):
        columns = self.output.column_names
        expected_columns = [
            "Name",
            "Language",
            "Uri",
            "Path",
            "Utc",
            "ts",
        ]

        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingCarDataProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = get_json_data("car_data")
        processor = processors.CarDataProcessor(MagicMock(), MagicMock())
        cls.output = processor._processor(cls.data)

    def test_live_timing_car_data_shape(self):
        shape = self.output.shape
        rows = 0
        for row in self.data:
            rows += len(row["Entries"])

        self.assertEqual(shape, (rows * 20 * 6, 5))  # 20 drivers, 6 channels

    def test_live_timing_car_data_columns(self):
        columns = self.output.column_names
        expected_columns = [
            "capture_ts",
            "car_number",
            "channel",
            "value",
            "ts",
        ]
        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingChampionshipPredictionProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        data = get_json_data("championship_prediction")
        processor = processors.ChampionshipPredictionProcessor(MagicMock(), MagicMock())
        cls.output = processor._processor(data)

    def test_live_timing_championship_prediction_shape(self):
        shape = self.output.shape
        self.assertEqual(shape, (137, 5))

    def test_live_timing_championship_prediction_columns(self):
        columns = self.output.column_names
        expected_columns = [
            "entity",
            "identifier",
            "metric",
            "value",
            "ts",
        ]

        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingCurrentTyresProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = get_json_data("current_tyres")
        processor = processors.CurrentTyresProcessor(MagicMock(), MagicMock())
        cls.output = processor._processor(cls.data)

    def test_live_timing_current_tyres_shape(self):
        shape = self.output.shape
        rows = 0
        for row in self.data:
            rows += len(row["Tyres"])

        self.assertEqual(shape, (rows, 4))

    def test_live_timing_current_tyres_columns(self):
        columns = self.output.column_names
        expected_columns = [
            "Driver",
            "Compound",
            "New",
            "ts",
        ]

        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingDriverRaceInfoProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = get_json_data("driver_race_info")
        processor = processors.DriverRaceInfoProcessor(MagicMock(), MagicMock())
        cls.output = processor._processor(copy.deepcopy(cls.data))

    def test_live_timing_driver_race_info_shape(self):
        shape = self.output.shape
        rows = 0
        for row in self.data:
            rows += len(row) - 1

        self.assertEqual(shape, (rows, 9))

    def test_live_timing_driver_race_info_columns(self):
        columns = self.output.column_names
        expected_columns = [
            "Driver",
            "Position",
            "Gap",
            "Interval",
            "PitStops",
            "Catching",
            "OvertakeState",
            "IsOut",
            "ts",
        ]

        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingExtrapolatedClockProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = get_json_data("extrapolated_clock")
        processor = processors.ExtrapolatedClockProcessor(MagicMock(), MagicMock())
        cls.output = processor._processor(cls.data)

    def test_live_timing_extrapolated_clock_shape(self):
        shape = self.output.shape

        self.assertEqual(shape, (len(self.data), 4))

    def test_live_timing_extrapolated_clock_columns(self):
        columns = self.output.column_names
        expected_columns = ["ts", "Utc", "Remaining", "Extrapolating"]

        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingHeartbeatProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = get_json_data("heartbeat")
        processor = processors.HeartbeatProcessor(MagicMock(), MagicMock())
        cls.output = processor._processor(cls.data)

    def test_live_timing_heartbeat_shape(self):
        shape = self.output.shape

        self.assertEqual(shape, (len(self.data), 2))

    def test_live_timing_heartbeat_columns(self):
        columns = self.output.column_names
        expected_columns = ["ts", "Utc"]

        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingIndexProcessorProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = get_json_data("index")
        processor = processors.IndexProcessor(MagicMock(), MagicMock())
        cls.output = processor._processor(cls.data)

    def test_live_timing_index_shape(self):
        shape = self.output.shape
        rows = 0
        for row in self.data:
            rows += len(row["Feeds"])

        self.assertEqual(shape, (rows, 2))

    def test_live_timing_index_columns(self):
        columns = self.output.column_names
        expected_columns = ["KeyFramePath", "StreamPath"]

        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingLapCountProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = get_json_data("lap_count")
        processor = processors.LapCountProcessor(MagicMock(), MagicMock())
        cls.output = processor._processor(copy.deepcopy(cls.data))

    def test_live_timing_lap_count_shape(self):
        shape = self.output.shape
        rows = 0
        for row in self.data:
            rows += len(row) - 1

        self.assertEqual(shape, (rows, 3))

    def test_live_timing_lap_count_columns(self):
        columns = self.output.column_names
        expected_columns = ["ts", "metric", "value"]

        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingLapSeriesProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = get_json_data("lap_series")
        processor = processors.LapSeriesProcessor(MagicMock(), MagicMock())
        cls.output = processor._processor(copy.deepcopy(cls.data))

    def test_live_timing_lap_series_shape(self):
        shape = self.output.shape
        rows = 0
        for row in self.data:
            rows += len(row) - 1

        self.assertEqual(shape, (rows, 4))

    def test_live_timing_lap_series_columns(self):
        columns = self.output.column_names
        expected_columns = ["driver_number", "lap_number", "lap_position", "ts"]

        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingPositionProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = get_json_data("position")
        processor = processors.PositionProcessor(MagicMock(), MagicMock())
        cls.output = processor._processor(cls.data)

    def test_live_timing_position_shape(self):
        shape = self.output.shape
        rows = 0
        for row in self.data:
            rows += len(row["Position"])

        self.assertEqual(shape, (rows * 20, 7))  # 20 drivers

    def test_live_timing_position_columns(self):
        columns = self.output.column_names
        expected_columns = [
            "Timestamp",
            "Driver",
            "Status",
            "X",
            "Y",
            "Z",
            "ts",
        ]
        self.assertCountEqual(columns, expected_columns)


class TestLiveTimingWeatherDataProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        data = get_json_data("weather_data")
        processor = processors.WeatherDataProcessor(MagicMock(), MagicMock())
        cls.output = processor._processor(data)

    def test_live_timing_weather_data_shape(self):
        shape = self.output.shape
        self.assertEqual(shape, (5, 8))

    def test_live_timing_weather_data_columns(self):
        columns = self.output.column_names
        expected_columns = [
            "ts",
            "AirTemp",
            "Humidity",
            "Pressure",
            "Rainfall",
            "TrackTemp",
            "WindDirection",
            "WindSpeed",
        ]
        self.assertCountEqual(columns, expected_columns)
