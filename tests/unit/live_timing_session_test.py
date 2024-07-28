import unittest
from datetime import datetime
from pathlib import Path

from duck_f1.pipelines.assets.live_timing import (
    LiveTimingAssetManager,
    LiveTimingSessionManager,
)
from duck_f1.pipelines.assets.live_timing.config import (
    LiveTimingEvent,
    LiveTimingSession,
    LiveTimingSessionDetail,
)


class TestLiveTimingKeyGenerators(unittest.TestCase):
    SESSION = LiveTimingSessionDetail(
        sha="684e2b65",
        date=datetime(2018, 3, 6, 12),
        name="Practice 1",
        type="Practice_1",
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
    @classmethod
    def setUpClass(cls):
        test_config_path = (
            Path(__file__).parent / "assets" / "live_timing_config.yaml"
        ).resolve()
        asset_manager = LiveTimingAssetManager(test_config_path)
        cls.session_manager = asset_manager.session_manager

    def test_live_timing_configuration_init(self):
        self.assertTrue(
            all(isinstance(i, LiveTimingSession) for i in self.session_manager.sessions)
        )
        self.assertEqual(len(self.session_manager.sessions), 10)

    def test_live_timing_sessions_keys(self):
        self.assertTrue(
            all(isinstance(i, str) for i in self.session_manager.session_keys)
        )
        self.assertEqual(len(self.session_manager.session_keys), 10)

    def test_live_timing_sessions_filter(self):
        # seasons
        season_filter = self.session_manager.filter_sessions(season=[2018])
        self.assertEqual(len(season_filter), 5)

        season_filter_array = self.session_manager.filter_sessions(season=[2018, 2019])
        self.assertEqual(len(season_filter_array), 10)

        # event_shas
        event_sha_filter = self.session_manager.filter_sessions(event_sha=["d7abca72"])
        self.assertEqual(len(event_sha_filter), 5)

        event_sha_filter_array = self.session_manager.filter_sessions(
            event_sha=["d7abca72", "4bb2bbff"]
        )
        self.assertEqual(len(event_sha_filter_array), 10)

        # event_locations
        event_location_filter = self.session_manager.filter_sessions(
            event_location=["Melbourne"]
        )
        self.assertEqual(len(event_location_filter), 5)

        event_location_filter_array = self.session_manager.filter_sessions(
            event_location=["Melbourne", "Sakhir"]
        )
        self.assertEqual(len(event_location_filter_array), 10)

        # event_countries
        event_country_filter = self.session_manager.filter_sessions(
            event_country=["Australia"]
        )
        self.assertEqual(len(event_country_filter), 5)

        event_country_filter_array = self.session_manager.filter_sessions(
            event_country=["Australia", "Bahrain"]
        )
        self.assertEqual(len(event_country_filter_array), 10)

        # session_shas
        session_sha_filter = self.session_manager.filter_sessions(
            session_sha=["79ca4465"]
        )
        self.assertEqual(len(session_sha_filter), 1)

        session_sha_filter_array = self.session_manager.filter_sessions(
            session_sha=["79ca4465", "d6ee7fc4"]
        )
        self.assertEqual(len(session_sha_filter_array), 2)

        # session_types
        session_type_filter = self.session_manager.filter_sessions(
            session_type=["race"]
        )
        self.assertEqual(len(session_type_filter), 2)

        session_type_filter_array = self.session_manager.filter_sessions(
            session_type=["qualifying", "race"]
        )
        self.assertEqual(len(session_type_filter_array), 4)
