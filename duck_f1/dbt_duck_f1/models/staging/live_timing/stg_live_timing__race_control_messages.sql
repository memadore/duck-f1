with
    raw_race_control_messages as (
        select * from {{ source("live_timing", "race_control_messages") }}
    ),
    formatted as (
        select
            messageid as message_id,
            utc as utc_ts,
            lap as lap_number,
            category as message_category,
            messagedata as message_data,
            _streamtimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_race_control_messages
    )
select *
from formatted
