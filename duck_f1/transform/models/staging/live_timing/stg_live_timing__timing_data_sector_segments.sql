with
    raw_timing_data_sector_segments as (
        select *
        from
            {{ source("ing__live_timing", "live_timing__timing_data_sector_segments") }}
    ),
    formatted as (
        select
            sectorkey as sector_key,
            segmentkey as segment_key,
            status as sector_status,
            driver as driver,
            _streamtimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_timing_data_sector_segments
    )
select *
from formatted
