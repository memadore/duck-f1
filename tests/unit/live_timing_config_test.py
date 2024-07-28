import unittest
from pathlib import Path

from duck_f1.pipelines.assets.live_timing import LiveTimingAssetManager
from duck_f1.pipelines.assets.live_timing.config import (
    LiveTimingConfig,
    LiveTimingDataset,
    LiveTimingEvent,
)


class TestLiveTimingConfig(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        test_config_path = (
            Path(__file__).parent / "assets" / "live_timing_config.yaml"
        ).resolve()
        cls.asset_manager = LiveTimingAssetManager(test_config_path)

    def test_live_timing_configuration_init(self):
        self.assertIsInstance(self.asset_manager.config, LiveTimingConfig)

    def test_live_timing_configuration_datasets(self):
        self.assertTrue(
            all(isinstance(i, LiveTimingDataset) for i in self.asset_manager.datasets)
        )
        self.assertEqual(len(self.asset_manager.datasets), 1)

    def test_live_timing_configuration_events(self):
        self.assertTrue(
            all(isinstance(i, LiveTimingEvent) for i in self.asset_manager.events)
        )
        self.assertEqual(len(self.asset_manager.events), 2)
