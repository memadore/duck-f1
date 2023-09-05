with
    raw_sessions as (select * from {{ source('live_timing', 'sessions') }}),
    formatted as (
        select
            event_country,
            event_date,
            event_gmt_offset,
            event_key,
            event_location,
            event_name,
            event_official_event_name,
            event_round_number,
            session_date,
            session_key,
            session_name,
            session_type
        from raw_sessions
    )
select *
from formatted