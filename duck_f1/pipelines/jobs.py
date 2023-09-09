from dagster import AssetSelection, define_asset_job

ergast_job = define_asset_job(
    name="ergast",
    selection=AssetSelection.key_prefixes("ergast")
    | AssetSelection.key_prefixes(["duckdb", "ergast"]),
)
live_timing_job = define_asset_job(
    name="live_timing",
    selection=AssetSelection.key_prefixes("live_timing")
    | AssetSelection.key_prefixes(["duckdb", "live_timing"]),
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 4,
                },
            }
        }
    },
)
