import unittest

from dagster import AssetsDefinition

from duck_f1.pipelines.assets.live_timing import LiveTimingAssetManager


class TestLiveTimingAssetManager(unittest.TestCase):
    def setUp(cls) -> None:
        cls.live_timing_assets_manager = LiveTimingAssetManager()

    def test_get_ergast_parquet_assets(self):
        expected_assets = {
            "live_timing__archive_status",
            "live_timing__audio_streams",
            "live_timing__car_data",
            "live_timing__championship_prediction",
            "live_timing__current_tyres",
            "live_timing__driver_list",
            "live_timing__driver_race_info",
            "live_timing__extrapolated_clock",
            "live_timing__heartbeat",
            "live_timing__index",
            "live_timing__lap_count",
            "live_timing__lap_series",
            "live_timing__pit_lane_time_collection",
            "live_timing__position",
            "live_timing__race_control_messages",
            "live_timing__session_data",
            "live_timing__session_info",
            "live_timing__session_status",
            "live_timing__timing_data_best_lap",
            "live_timing__timing_data_interval",
            "live_timing__timing_data_last_lap",
            "live_timing__timing_data_sector_segments",
            "live_timing__timing_data_sectors",
            "live_timing__timing_data_speeds",
            "live_timing__timing_data_status",
            "live_timing__timing_stats_lap_times",
            "live_timing__timing_stats_sectors",
            "live_timing__timing_stats_speeds",
            "live_timing__tla_rcm",
            "live_timing__track_status",
            "live_timing__tyre_stint_series",
            "live_timing__weather_data",
        }

        parquet_assets = self.live_timing_assets_manager.create_parquet_assets()

        asset_keys = []
        for i in parquet_assets:
            asset_keys.extend(i.keys)

        actual_assets = set([i.to_python_identifier() for i in asset_keys])

        self.assertTrue(all(isinstance(i, AssetsDefinition) for i in parquet_assets))
        self.assertEqual(
            len(parquet_assets),
            len(self.live_timing_assets_manager.processor_builder.processors),
        )
        self.assertEqual(actual_assets, expected_assets)

    def test_get_live_timing_duck_db_assets(self):
        expected_assets = {
            "duckdb__ingress__live_timing__archive_status",
            "duckdb__ingress__live_timing__audio_streams",
            "duckdb__ingress__live_timing__car_data",
            "duckdb__ingress__live_timing__championship_prediction",
            "duckdb__ingress__live_timing__current_tyres",
            "duckdb__ingress__live_timing__driver_list",
            "duckdb__ingress__live_timing__driver_race_info",
            "duckdb__ingress__live_timing__extrapolated_clock",
            "duckdb__ingress__live_timing__heartbeat",
            "duckdb__ingress__live_timing__index",
            "duckdb__ingress__live_timing__lap_count",
            "duckdb__ingress__live_timing__lap_series",
            "duckdb__ingress__live_timing__pit_lane_time_collection",
            "duckdb__ingress__live_timing__position",
            "duckdb__ingress__live_timing__race_control_messages",
            "duckdb__ingress__live_timing__sessions",
            "duckdb__ingress__live_timing__session_data",
            "duckdb__ingress__live_timing__session_info",
            "duckdb__ingress__live_timing__session_status",
            "duckdb__ingress__live_timing__timing_data_best_lap",
            "duckdb__ingress__live_timing__timing_data_interval",
            "duckdb__ingress__live_timing__timing_data_last_lap",
            "duckdb__ingress__live_timing__timing_data_sector_segments",
            "duckdb__ingress__live_timing__timing_data_sectors",
            "duckdb__ingress__live_timing__timing_data_speeds",
            "duckdb__ingress__live_timing__timing_data_status",
            "duckdb__ingress__live_timing__timing_stats_lap_times",
            "duckdb__ingress__live_timing__timing_stats_sectors",
            "duckdb__ingress__live_timing__timing_stats_speeds",
            "duckdb__ingress__live_timing__tla_rcm",
            "duckdb__ingress__live_timing__track_status",
            "duckdb__ingress__live_timing__tyre_stint_series",
            "duckdb__ingress__live_timing__weather_data",
        }

        duckdb_assets = self.live_timing_assets_manager.create_duckdb_assets()

        actual_assets = set([i.key.to_python_identifier() for i in duckdb_assets])

        self.assertTrue(all(isinstance(i, AssetsDefinition) for i in duckdb_assets))
        self.assertEqual(actual_assets, expected_assets)
