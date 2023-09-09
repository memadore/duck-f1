with
    raw_race_control_messages as (select * from {{ source('live_timing', 'race_control_messages') }}),
    formatted as (
        select
            MessageId as message_id,
            Utc as utc_ts,
            Lap as lap_number,
            Category as message_category,
            MessageData as message_data,
            _StreamTimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_race_control_messages
    )
select *
from formatted