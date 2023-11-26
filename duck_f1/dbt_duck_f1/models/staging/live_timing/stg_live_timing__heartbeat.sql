with
    raw_heartbeat as (select * from {{ source("live_timing", "heartbeat") }}),
    formatted as (
        select
            utc::timestamp as utc_ts,
            _streamtimestamp::interval as _stream_ts,
            utc_ts - _stream_ts as start_utc,
            {{ live_timing__metadata_raw_columns() }}
        from raw_heartbeat
    )
select *
from formatted
