with
raw_sessions as (
    select * from {{ source("src_live_timing", "live_timing__sessions") }}
),

formatted as (
    select
        {{ dbt_utils.generate_surrogate_key([
            "date_part('year', event_date)",
            "event_round_number"]
        ) }} as event_id,
        {{ dbt_utils.generate_surrogate_key([
            "date_part('year', event_date)",
            "event_round_number",
            "session_type"]
        ) }} as session_id,
        session_sha as _live_timing_session_sha,
        session_type,
        session_name,
        session_date as session_start_local
    from raw_sessions
    where event_round_number > 0
)

select *
from formatted
