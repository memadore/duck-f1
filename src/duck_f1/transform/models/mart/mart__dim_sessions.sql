select
    {{
        dbt_utils.star(
            from=ref("stg_sessions"),
            except=["_ergast_race_id", "_live_timing_session_sha"],
        )
    }}
from {{ ref("stg_sessions") }}
