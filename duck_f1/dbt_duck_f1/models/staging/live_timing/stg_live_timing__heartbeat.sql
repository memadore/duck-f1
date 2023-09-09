with
    raw_heartbeat as (select * from {{ source('live_timing', 'heartbeat') }}),
    formatted as (
        select
            Utc as utc_ts,
            _StreamTimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_heartbeat
    )
select *
from formatted