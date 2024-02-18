with
    raw_track_status as (
        select * from {{ source("ing__live_timing", "live_timing__track_status") }}
    ),
    formatted as (
        select
            status as status_id,
            message as status_message,
            _streamtimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_track_status
    )
select *
from formatted
