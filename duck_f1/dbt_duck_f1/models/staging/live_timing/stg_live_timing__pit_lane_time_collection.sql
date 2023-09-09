with
    raw_pit_lane_time_collection as (select * from {{ source('live_timing', 'pit_lane_time_collection') }}),
    formatted as (
        select
            Driver as driver,
            Duration as pit_lane_time_duration,
            Lap as lap_number,
            _StreamTimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_pit_lane_time_collection
    )
select *
from formatted