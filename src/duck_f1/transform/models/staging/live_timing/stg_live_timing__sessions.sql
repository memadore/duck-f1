with
    raw_sessions as (
        select * from {{ source("ing__live_timing", "live_timing__sessions") }}
    ),
    formatted as (
        select
            event_country,
            event_date,
            event_gmt_offset,
            event_sha,
            event_location,
            event_name,
            event_official_event_name,
            event_round_number,
            session_date,
            session_sha,
            session_name,
            session_type
        from raw_sessions
    )
select *
from formatted
