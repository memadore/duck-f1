with
    raw_index as (select * from {{ source("live_timing", "index") }}),
    formatted as (
        select
            keyframepath as key_frame_path,
            streampath as stream_path,
            {{ live_timing__metadata_raw_columns() }}
        from raw_index
    )
select *
from formatted
