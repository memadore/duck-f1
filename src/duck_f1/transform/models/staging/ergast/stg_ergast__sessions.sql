with
raw_sessions as (select * from {{ source("src_ergast", "ergast__races") }}),

fp1_sessions as (
    select
        'practice_1' as session_type,
        'Practice 1' as session_name,
        {{ dbt_utils.generate_surrogate_key([
            "_session.year",
            "_session.round"
        ]) }} as event_id,
        {{ dbt_utils.generate_surrogate_key([
            "_session.year",
            "_session.round",
            "session_type"
            ]) }} as session_id,
        _session.raceid as _ergast_race_id,
        if(_session.fp1_date = '\N', null, _session.fp1_date) as session_date,
        if(_session.fp1_time = '\N', '00:00:00', _session.fp1_time) as session_time
    from raw_sessions as _session
    where _session.fp1_date not like '\N'
),

fp2_sessions as (
    select
        'practice_2' as session_type,
        'Practice 2' as session_name,
        {{ dbt_utils.generate_surrogate_key([
            "_session.year",
            "_session.round"
        ]) }} as event_id,
        {{ dbt_utils.generate_surrogate_key([
            "_session.year",
            "_session.round",
            "session_type"
        ]) }} as session_id,
        _session.raceid as _ergast_race_id,
        if(_session.fp2_date = '\N', null, _session.fp2_date) as session_date,
        if(_session.fp2_time = '\N', '00:00:00', _session.fp2_time) as session_time
    from raw_sessions as _session
    where _session.fp2_date not like '\N'
),

fp3_sessions as (
    select
        'practice_3' as session_type,
        'Practice 3' as session_name,
        {{ dbt_utils.generate_surrogate_key([
            "_session.year",
            "_session.round"
        ]) }} as event_id,
        {{ dbt_utils.generate_surrogate_key([
            "_session.year",
            "_session.round",
            "session_type"
        ]) }} as session_id,
        _session.raceid as _ergast_race_id,
        if(_session.fp3_date = '\N', null, _session.fp3_date) as session_date,
        if(_session.fp3_time = '\N', '00:00:00', _session.fp3_time) as session_time
    from raw_sessions as _session
    where _session.fp3_date not like '\N'
),

qualifying_sessions as (
    select
        'qualifying' as session_type,
        'Qualifying' as session_name,
        {{ dbt_utils.generate_surrogate_key([
            "_session.year",
            "_session.round"
        ]) }} as event_id,
        {{ dbt_utils.generate_surrogate_key([
            "_session.year",
            "_session.round",
            "session_type"
        ]) }} as session_id,
        _session.raceid as _ergast_race_id,
        if(_session.quali_date = '\N', null, _session.quali_date) as session_date,
        if(_session.quali_time = '\N', '00:00:00', _session.quali_time) as session_time
    from raw_sessions as _session
),

sprint_sessions as (
    select
        'sprint' as session_type,
        'Sprint' as session_name,
        {{ dbt_utils.generate_surrogate_key([
            "_session.year",
            "_session.round"
        ]) }} as event_id,
        {{ dbt_utils.generate_surrogate_key([
            "_session.year",
            "_session.round",
            "session_type"
        ]) }} as session_id,
        _session.raceid as _ergast_race_id,
        if(_session.sprint_date = '\N', null, _session.sprint_date) as session_date,
        if(_session.sprint_time = '\N', '00:00:00', _session.sprint_time) as session_time
    from raw_sessions as _session
    where _session.sprint_date not like '\N'
),

race_sessions as (
    select
        'race' as session_type,
        'Race' as session_name,
        {{ dbt_utils.generate_surrogate_key([
            "_session.year",
            "_session.round"
        ]) }} as event_id,
        {{ dbt_utils.generate_surrogate_key([
            "_session.year",
            "_session.round",
            "session_type"
        ]) }} as session_id,
        _session.raceid as _ergast_race_id,
        _session.date as session_date,
        if(_session.time = '\N', '00:00:00', _session.time) as session_time
    from raw_sessions as _session
),

ergast_sessions as (
    select *
    from fp1_sessions
    union all
    select *
    from fp2_sessions
    union all
    select *
    from fp3_sessions
    union all
    select *
    from qualifying_sessions
    union all
    select *
    from sprint_sessions
    union all
    select *
    from race_sessions
),

formatted as (
    select
        event_id,
        session_id,
        _ergast_race_id,
        session_type,
        session_name,
        if(
            session_date is null,
            null,
            concat(session_date, ' ', session_time)
        )::timestamp as session_start_utc
    from ergast_sessions
)

select *
from formatted
