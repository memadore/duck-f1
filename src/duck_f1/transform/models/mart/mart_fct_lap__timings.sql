with
ergast_lap_times as (
    select
        session_id,
        driver_id,
        lap_id,
        lap_number,
        lap_time as estimated_lap_time,
        lap_time,
        'ergast' as lap_time_source,
        0 as _lap_time_source_priority
    from {{ ref("stg_ergast__lap__timings") }}
),

live_timing_lap_times as (
    select
        lap_time.session_id,
        _session.driver_id,
        {{ dbt_utils.generate_surrogate_key(
            [
            "lap_time.session_id",
            "_session.driver_id",
            "lap_time.lap_number"
            ]
        ) }} as lap_id,
        lap_time.lap_number,
        lap_time.estimated_lap_time,
        lap_time.lap_time,
        'live_timing' as lap_time_source,
        1 as _lap_time_source_priority
    from {{ ref("int_live_timing__lap__timing") }} as lap_time
    left join {{ ref("mart_fct_session__drivers") }} as _session
        on
            lap_time.session_id = _session.session_id
            and lap_time.car_number = _session.car_number
    where lap_time.lap_number is not null
),

lap_times as (
    select *
    from ergast_lap_times
    union all
    select *
    from live_timing_lap_times
),

unique_lap_times as (
    select *
    from lap_times
    qualify
        row_number()
            over (partition by session_id, driver_id, lap_id order by _lap_time_source_priority)
        = 1
),

live_timing_sectors as (
    select
        sector.session_id,
        _session.driver_id,
        {{ dbt_utils.generate_surrogate_key(
            [
            "sector.session_id",
            "_session.driver_id",
            "sector.lap_number"
            ]
        ) }} as lap_id,
        any_value(if(sector.sector_key = 0, sector.sector_time, null) order by sector.session_ts)
            as sector_1_time,
        any_value(if(sector.sector_key = 1, sector.sector_time, null) order by sector.session_ts)
            as sector_2_time,
        any_value(if(sector.sector_key = 2, sector.sector_time, null) order by sector.session_ts)
            as sector_3_time
    from {{ ref("int_live_timing__lap__sector_times") }} as sector
    left join {{ ref("mart_fct_session__drivers") }} as _session
        on
            sector.session_id = _session.session_id
            and sector.car_number = _session.car_number
    where sector.lap_number is not null
    group by sector.session_id, _session.driver_id, lap_id
),

live_timing_speeds as (
    select
        speed.session_id,
        _session.driver_id,
        {{ dbt_utils.generate_surrogate_key(
            [
            "speed.session_id",
            "_session.driver_id",
            "speed.lap_number"
            ]
        ) }} as lap_id,
        any_value(if(speed.speed_key = 'I1', speed.speed_value, null) order by speed.session_ts)
            as sector_1_speed,
        any_value(if(speed.speed_key = 'I2', speed.speed_value, null) order by speed.session_ts)
            as sector_2_speed,
        any_value(if(speed.speed_key = 'FL', speed.speed_value, null) order by speed.session_ts)
            as finish_line_speed,
        any_value(if(speed.speed_key = 'ST', speed.speed_value, null) order by speed.session_ts)
            as speed_trap
    from {{ ref("int_live_timing__lap__speed_traps") }} as speed
    left join {{ ref("mart_fct_session__drivers") }} as _session
        on
            speed.session_id = _session.session_id
            and speed.car_number = _session.car_number
    where speed.lap_number is not null
    group by speed.session_id, _session.driver_id, lap_id
),

formatted as (
    select
        lap_time.session_id,
        lap_time.driver_id,
        lap_time.lap_id,
        lap_time.lap_number,
        lap_time.estimated_lap_time,
        lap_time.lap_time,
        lap_time.lap_time_source,
        sector_time.sector_1_time,
        sector_time.sector_2_time,
        sector_time.sector_3_time,
        speed.sector_1_speed,
        speed.sector_2_speed,
        speed.finish_line_speed,
        speed.speed_trap
    from unique_lap_times as lap_time
    left join live_timing_sectors as sector_time
        on
            lap_time.session_id = sector_time.session_id
            and lap_time.driver_id = sector_time.driver_id
            and lap_time.lap_id = sector_time.lap_id
    left join live_timing_speeds as speed
        on
            lap_time.session_id = speed.session_id
            and lap_time.driver_id = speed.driver_id
            and lap_time.lap_id = speed.lap_id
)

select *
from formatted
