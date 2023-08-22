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


class TestLiveTimingCarDataProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = get_json_data("car_data")
        processor = processors.CarDataProcessor(MagicMock(), MagicMock())
        cls.output = processor._processor(cls.data)

    def test_live_timing_weather_data_shape(self):
        shape = self.output.shape
        rows = 0
        for row in self.data:
            rows += len(row["Entries"])

        self.assertEqual(shape, (rows * 20 * 6, 5))  # 20 drivers, 6 channels

    def test_live_timing_weather_data_columns(self):
        columns = self.output.column_names
        expected_columns = [
            "capture_ts",
            "car_number",
            "channel",
            "value",
            "ts",
        ]
        self.assertCountEqual(columns, expected_columns)