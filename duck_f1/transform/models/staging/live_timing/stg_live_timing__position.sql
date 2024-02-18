with
    raw_position as (
        select * from {{ source("ing__live_timing", "live_timing__position") }}
    ),
    formatted as (
        select
            timestamp as event_utc_ts,
            driver as driver,
            status as status,
            x as x_position,
            y as y_position,
            z as z_position,
            _streamtimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_position
    )
select *
from formatted
