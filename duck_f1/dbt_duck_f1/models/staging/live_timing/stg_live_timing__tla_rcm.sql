with
    raw_tla_rcm as (select * from {{ source('live_timing', 'tla_rcm') }}),
    formatted as (
        select
            Timestamp as event_local_ts,
            Message as race_control_message,
            _StreamTimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_tla_rcm
    )
select *
from formatted