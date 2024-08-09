with
raw_sessions as (
    select * from {{ source("src_live_timing", "live_timing__sessions") }}
),

formatted as (
    select
        {{ dbt_utils.generate_surrogate_key([
            "date_part('year', event_date)",
            "event_round_number"
        ]) }} as event_id,
        event_sha as _live_timing_event_sha,
        date_part('year', event_date) as season,
        event_round_number as round,
        event_name,
        event_official_event_name as event_official_name,
        event_country,
        event_location
    from raw_sessions
    qualify
        row_number() over (partition by event_id order by event_id) = 1
)

select *
from formatted
