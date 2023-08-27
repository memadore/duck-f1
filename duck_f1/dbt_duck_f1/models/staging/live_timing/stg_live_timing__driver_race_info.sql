with
    raw_driver_race_info as (select * from {{ source('live_timing', 'driver_race_info') }}),
    formatted as (
        select
            ts as ts,
            Driver as driver_number,
            Position as track_position,
            Gap as gap,
            Interval as interval,
            PitStops as pitstop_count,
            Catching as is_catching,
            OvertakeState as overtake_count,
            IsOut as is_out,
            {{ live_timing__metadata_raw_columns() }}
        from raw_driver_race_info
    )
select *
from formatted