from .live_timing import live_timing_files
from .sessions import LiveTimingConfigManager, LiveTimingSessionManager

config_manager = LiveTimingConfigManager("./config/live_timing.yaml")
sessions_manager = LiveTimingSessionManager(config_manager.events)

live_timing_assets = live_timing_files(sessions_manager, config_manager.datasets)
