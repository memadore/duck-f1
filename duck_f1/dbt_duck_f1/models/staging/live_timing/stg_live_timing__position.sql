with
    raw_position as (select * from {{ source('live_timing', 'position') }}),
    formatted as (
        select
            Timestamp as event_utc_ts,
            Driver as driver,
            Status as status,
            X as x_position,
            Y as y_position,
            Z as z_position,
            _StreamTimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_position
    )
select *
from formatted