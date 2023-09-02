with
    raw_track_status as (select * from {{ source('live_timing', 'track_status') }}),
    formatted as (
        select
            ts as ts,
            Status as status_id,
            message as status_message,
            {{ live_timing__metadata_raw_columns() }}
        from raw_track_status
    )
select *
from formatted