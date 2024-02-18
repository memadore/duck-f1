with
    raw_tla_rcm as (
        select * from {{ source("ing__live_timing", "live_timing__tla_rcm") }}
    ),
    formatted as (
        select
            timestamp as event_local_ts,
            message as race_control_message,
            _streamtimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_tla_rcm
    )
select *
from formatted
