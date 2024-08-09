with
ergast_events as (select * from {{ ref("stg_ergast__events") }}),

live_timing_events as (
    select *
    from {{ ref("stg_live_timing__events") }}
),

formatted as (
    select
        ergast_event.event_id,
        ergast_event.circuit_id,
        ergast_event._ergast_race_id,
        live_timing_event._live_timing_event_sha,
        ergast_event.season,
        ergast_event.round,
        live_timing_event.event_country,
        live_timing_event.event_location,
        ergast_event.event_name,
        live_timing_event.event_official_name,
        ergast_event.wikipedia_url
    from ergast_events as ergast_event
    left join live_timing_events as live_timing_event
        on ergast_event.event_id = live_timing_event.event_id
)

select *
from formatted
order by season desc, round asc
