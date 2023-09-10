with
    raw_session_status as (select * from {{ source("live_timing", "session_status") }}),
    formatted as (
        select
            status as session_status,
            _streamtimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_session_status
    )
select *
from formatted
