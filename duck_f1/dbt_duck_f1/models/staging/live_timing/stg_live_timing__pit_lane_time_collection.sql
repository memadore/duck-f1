with
    raw_pit_lane_time_collection as (select * from {{ source('live_timing', 'pit_lane_time_collection') }}),
    formatted as (
        select
            ts as ts,
            Driver as driver,
            Duration as pit_lane_time_duration,
            lap as lap_number,
            {{ live_timing__metadata_raw_columns() }}
        from raw_pit_lane_time_collection
    )
select *
from formatted