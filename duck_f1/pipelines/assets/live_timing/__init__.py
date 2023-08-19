from .live_timing import live_timing_files
from .partitions import LiveTimingConfigManager, LiveTimingPartitionManager

config_manager = LiveTimingConfigManager("./config/live_timing.yaml")
partition_manager = LiveTimingPartitionManager(config_manager.events)

live_timing_assets = live_timing_files(partition_manager, config_manager.datasets)
