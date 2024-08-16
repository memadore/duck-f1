with
raw_timing_data_sectors as (
    select *
    from {{ ref("stg_live_timing__timing_data_sectors") }}
),

first_second_sectors as (
    select
        sector.session_id,
        sector.car_number,
        lap_series.lap_number,
        sector.sector_key,
        sector.sector_time,
        sector.session_ts
    from raw_timing_data_sectors as sector
    left join {{ ref("stg_live_timing__lap_series") }} as lap_series
        on
            sector.session_id = lap_series.session_id
            and sector.car_number = lap_series.car_number
            and (
                sector.session_ts > lap_series.lap_start_ts
                and sector.session_ts <= lap_series.lap_end_ts
            )
    where sector.sector_key < 2
),

third_sector as (
    select
        sector.session_id,
        sector.car_number,
        lap_series.lap_number,
        sector.sector_key,
        sector.sector_time,
        sector.session_ts
    from raw_timing_data_sectors as sector
    left join {{ ref("stg_live_timing__lap_series") }} as lap_series
        on
            sector.session_id = lap_series.session_id
            and sector.car_number = lap_series.car_number
            and (
                sector.session_ts >= lap_series.lap_end_ts - to_milliseconds(5000)
                and sector.session_ts <= lap_series.lap_end_ts + to_milliseconds(5000)
            )
    where sector.sector_key = 2
),

formatted as (
    select *
    from first_second_sectors
    union all
    select *
    from third_sector
)

select *
from formatted
