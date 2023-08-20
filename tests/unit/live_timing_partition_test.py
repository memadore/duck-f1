import unittest
from datetime import datetime

from duck_f1.pipelines.assets.live_timing.partitions import (
    LiveTimingConfigManager,
    LiveTimingEvent,
    LiveTimingPartition,
    LiveTimingPartitionManager,
    LiveTimingSession,
)


class TestLiveTimingKeyGenerators(unittest.TestCase):
    SESSION = LiveTimingSession(date=datetime(2018, 3, 6, 12), name="Practice 1", type="Practice_1")
    EVENT = LiveTimingEvent(
        country="Australia",
        date=datetime(2018, 3, 8, 18, 10),
        gmt_offset="+11:00",
        location="Melbourne",
        name="Australian Grand Prix",
        official_event_name="FORMULA 1 2018 ROLEX AUSTRALIAN GRAND PRIX",
        round_number=1,
        sessions=[SESSION],
    )

    def test_live_timing_event_key(self):
        key = LiveTimingPartitionManager._create_event_key(self.EVENT, self.SESSION)
        expected = "2018/2018-03-08_Australian_Grand_Prix/2018-03-06_Practice_1"
        self.assertEqual(key, expected)

    def test_live_timing_partition_key(self):
        key = LiveTimingPartitionManager._create_partitions_key(self.EVENT, self.SESSION)
        expected = "2018/03/08/practice_1"
        self.assertEqual(key, expected)


class TestLiveTimingConfigManager(unittest.TestCase):
    CONFIG_PATH = "./tests/unit/assets/live_timing_config.yaml"

    @classmethod
    def setUpClass(cls):
        cls._config_manager = LiveTimingConfigManager(cls.CONFIG_PATH)
        cls._partition_manager = LiveTimingPartitionManager(cls._config_manager.events)

    def test_live_timing_configuration_init(self):
        self.assertTrue(
            all(isinstance(i, LiveTimingPartition) for i in self._partition_manager.partitions)
        )
        self.assertEqual(len(self._partition_manager.partitions), 5)

    def test_live_timing_partitions_keys(self):
        self.assertTrue(all(isinstance(i, str) for i in self._partition_manager.partition_keys))
        self.assertEqual(len(self._partition_manager.partition_keys), 5)
