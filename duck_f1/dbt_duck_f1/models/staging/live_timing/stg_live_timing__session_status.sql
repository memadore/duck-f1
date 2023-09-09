with
    raw_session_status as (select * from {{ source('live_timing', 'session_status') }}),
    formatted as (
        select
            Status as session_status,
            _StreamTimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_session_status
    )
select *
from formatted