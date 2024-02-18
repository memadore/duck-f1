import unittest

from duck_f1.pipelines.assets.live_timing.sessions import (
    LiveTimingConfig,
    LiveTimingConfigManager,
    LiveTimingDataset,
    LiveTimingEvent,
)


class TestLiveTimingConfig(unittest.TestCase):
    CONFIG_PATH = "./tests/unit/assets/live_timing_config.yaml"

    @classmethod
    def setUpClass(cls):
        cls.config_manager = LiveTimingConfigManager(cls.CONFIG_PATH)

    def test_live_timing_configuration_init(self):
        self.assertIsInstance(self.config_manager.config, LiveTimingConfig)

    def test_live_timing_configuration_datasets(self):
        self.assertTrue(all(isinstance(i, LiveTimingDataset) for i in self.config_manager.datasets))
        self.assertEqual(len(self.config_manager.datasets), 1)

    def test_live_timing_configuration_events(self):
        self.assertTrue(all(isinstance(i, LiveTimingEvent) for i in self.config_manager.events))
        self.assertEqual(len(self.config_manager.events), 1)
