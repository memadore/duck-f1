with
ergast_sessions as (
    select
        event_id,
        session_id,
        _ergast_race_id,
        null::integer as _live_timing_session_sha,
        session_type,
        session_name,
        session_start_utc,
        null::integer as session_start_local
    from {{ ref("stg_ergast__sessions") }}
    where session_start_utc < '2018-01-01'::date
),

ergast_live_timing_sessions as (
    select
        _session.session_id,
        _session._ergast_race_id,
        _session.session_start_utc
    from {{ ref("stg_ergast__sessions") }} as _session
    where _session.session_type in ('qualifying', 'sprint', 'race')
),

live_timing_sessions as (
    select
        live_timing_session.event_id,
        live_timing_session.session_id,
        ergast_session._ergast_race_id,
        live_timing_session._live_timing_session_sha,
        live_timing_session.session_type,
        live_timing_session.session_name,
        ergast_session.session_start_utc,
        live_timing_session.session_start_local
    from {{ ref("stg_live_timing__sessions") }} as live_timing_session
    left join ergast_live_timing_sessions as ergast_session
        on live_timing_session.session_id = ergast_session.session_id
),

formatted as (
    select *
    from live_timing_sessions
    union all
    select *
    from ergast_sessions
)

select *
from formatted
