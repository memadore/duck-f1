from dagster import AssetSelection, define_asset_job

ergast_job = define_asset_job(name="ergast", selection=AssetSelection.groups("ergast"))
live_timing_job = define_asset_job(
    name="live_timing", selection=AssetSelection.groups("live_timing")
)
