with
raw_qualifying as (select * from {{ source("src_ergast", "ergast__qualifying") }}),

constructor_ids as (
    select
        constructor_id,
        _ergast_constructor_id
    from {{ ref("stg_ergast__constructors") }}
),

driver_ids as (
    select
        driver_id,
        _ergast_driver_id
    from {{ ref("stg_ergast__drivers") }}
),

session_ids as (
    select
        session_id,
        _ergast_race_id
    from {{ ref("stg_sessions") }}
    where session_type = 'qualifying'
),

qualifying_results as (
    select
        driver.driver_id,
        _session.session_id,
        constructor.constructor_id,
        qualifying.position,
        if(qualifying.q1 = '\N' or len(qualifying.q1) = 0, null, qualifying.q1) as q1_time_label,
        if(
            qualifying.q1 = '\N' or len(qualifying.q1) = 0,
            null,
            {{ varchar_lap_time_to_interval("qualifying.q1") }}
        ) as q1_time,
        if(qualifying.q2 = '\N' or len(qualifying.q2) = 0, null, qualifying.q2) as q2_time_label,
        if(
            qualifying.q2 = '\N' or len(qualifying.q2) = 0,
            null,
            {{ varchar_lap_time_to_interval("qualifying.q2") }}
        ) as q2_time,
        if(qualifying.q3 = '\N' or len(qualifying.q3) = 0, null, qualifying.q3) as q3_time_label,
        if(
            qualifying.q3 = '\N' or len(qualifying.q3) = 0,
            null,
            {{ varchar_lap_time_to_interval("qualifying.q3") }}
        ) as q3_time
    from raw_qualifying as qualifying
    inner join
        constructor_ids as constructor
        on qualifying.constructorid = constructor._ergast_constructor_id
    inner join driver_ids as driver on qualifying.driverid = driver._ergast_driver_id
    inner join session_ids as _session on qualifying.raceid = _session._ergast_race_id
),

qualifying_windows as (
    select
        *,
        row_number() over (partition by session_id order by q1_time nulls last) as q1_position,
        q1_time
        - first(q1_time) over (partition by session_id order by q1_time nulls last) as q1_interval,
        row_number() over (partition by session_id order by q2_time nulls last) as q2_position,
        q2_time
        - first(q2_time) over (partition by session_id order by q2_time nulls last) as q2_interval,
        row_number() over (partition by session_id order by q3_time nulls last) as q3_position,
        q3_time
        - first(q3_time) over (partition by session_id order by q3_time nulls last) as q3_interval
    from qualifying_results
),

formatted as (
    select
        qualifying.session_id,
        qualifying.constructor_id,
        qualifying.driver_id,
        qualifying.position,
        qualifying.q1_time,
        qualifying.q1_time_label,
        qualifying.q1_interval,
        qualifying.q2_time,
        qualifying.q2_time_label,
        qualifying.q2_interval,
        qualifying.q3_time,
        qualifying.q3_time_label,
        qualifying.q3_interval,
        if(qualifying.q1_time is null, qualifying.position, qualifying.q1_position) as q1_position,
        if(qualifying.q2_time is null, null, qualifying.q2_position) as q2_position,
        if(qualifying.q3_time is null, null, qualifying.q3_position) as q3_position
    from qualifying_windows as qualifying
)

select *
from formatted
