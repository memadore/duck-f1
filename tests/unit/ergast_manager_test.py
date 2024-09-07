import unittest

from dagster import AssetsDefinition

from duck_f1.pipelines.assets.ergast import ErgastAssetsManager


class TestErgastAssetManager(unittest.TestCase):
    def setUp(cls) -> None:
        cls.ergast_assets_manager = ErgastAssetsManager()

    def test_get_ergast_parquet_assets(self):
        expected_assets = {
            "ergast__circuits",
            "ergast__constructor_results",
            "ergast__constructor_standings",
            "ergast__constructors",
            "ergast__driver_standings",
            "ergast__drivers",
            "ergast__lap_times",
            "ergast__pit_stops",
            "ergast__qualifying",
            "ergast__races",
            "ergast__results",
            "ergast__seasons",
            "ergast__sprint_results",
            "ergast__status",
        }

        static_assets = self.ergast_assets_manager.create_parquet_assets()

        asset_keys = []
        for i in static_assets:
            asset_keys.extend(i.keys)

        actual_assets = set([i.to_python_identifier() for i in asset_keys])

        self.assertTrue(all(isinstance(i, AssetsDefinition) for i in static_assets))
        self.assertEqual(len(static_assets), 1)
        self.assertEqual(actual_assets, expected_assets)

    def test_get_ergast_duck_db_assets(self):
        expected_assets = {
            "duckdb__ingress__ergast__circuits",
            "duckdb__ingress__ergast__constructor_results",
            "duckdb__ingress__ergast__constructor_standings",
            "duckdb__ingress__ergast__constructors",
            "duckdb__ingress__ergast__driver_standings",
            "duckdb__ingress__ergast__drivers",
            "duckdb__ingress__ergast__lap_times",
            "duckdb__ingress__ergast__pit_stops",
            "duckdb__ingress__ergast__qualifying",
            "duckdb__ingress__ergast__races",
            "duckdb__ingress__ergast__results",
            "duckdb__ingress__ergast__seasons",
            "duckdb__ingress__ergast__sprint_results",
            "duckdb__ingress__ergast__status",
        }

        duckdb_assets = self.ergast_assets_manager.create_duckdb_assets()

        actual_assets = set([i.key.to_python_identifier() for i in duckdb_assets])

        self.assertTrue(all(isinstance(i, AssetsDefinition) for i in duckdb_assets))
        self.assertEqual(actual_assets, expected_assets)
