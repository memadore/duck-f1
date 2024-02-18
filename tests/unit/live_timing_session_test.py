import unittest
from datetime import datetime

from duck_f1.pipelines.assets.live_timing.sessions import (
    LiveTimingConfigManager,
    LiveTimingEvent,
    LiveTimingSession,
    LiveTimingSessionDetail,
    LiveTimingSessionManager,
)


class TestLiveTimingKeyGenerators(unittest.TestCase):
    SESSION = LiveTimingSessionDetail(
        sha="684e2b65", date=datetime(2018, 3, 6, 12), name="Practice 1", type="Practice_1"
    )
    EVENT = LiveTimingEvent(
        sha="d7abca72",
        country="Australia",
        date=datetime(2018, 3, 8, 18, 10),
        gmt_offset="+11:00",
        location="Melbourne",
        name="Australian Grand Prix",
        official_event_name="FORMULA 1 2018 ROLEX AUSTRALIAN GRAND PRIX",
        round_number=1,
        sessions=[SESSION],
    )

    def test_live_timing_event_path(self):
        key = LiveTimingSessionManager._create_event_path(self.EVENT, self.SESSION)
        expected = "2018/2018-03-08_Australian_Grand_Prix/2018-03-06_Practice_1"
        self.assertEqual(key, expected)

    def test_live_timing_sessions_key(self):
        key = LiveTimingSessionManager._create_session_key(self.EVENT, self.SESSION)
        expected = "2018/03/08/practice_1"
        self.assertEqual(key, expected)


class TestLiveTimingConfigManager(unittest.TestCase):
    CONFIG_PATH = "./tests/unit/assets/live_timing_config.yaml"

    @classmethod
    def setUpClass(cls):
        cls._config_manager = LiveTimingConfigManager(cls.CONFIG_PATH)
        cls._sessions_manager = LiveTimingSessionManager(cls._config_manager.events)

    def test_live_timing_configuration_init(self):
        self.assertTrue(
            all(isinstance(i, LiveTimingSession) for i in self._sessions_manager.sessions)
        )
        self.assertEqual(len(self._sessions_manager.sessions), 5)

    def test_live_timing_sessions_keys(self):
        self.assertTrue(all(isinstance(i, str) for i in self._sessions_manager.session_keys))
        self.assertEqual(len(self._sessions_manager.session_keys), 5)
