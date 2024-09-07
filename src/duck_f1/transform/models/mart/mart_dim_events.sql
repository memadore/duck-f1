select
    {{
        dbt_utils.star(
            from=ref("stg_events"),
            except=["_ergast_race_id", "_live_timing_event_sha"],
        )
    }}
from {{ ref("stg_events") }}
