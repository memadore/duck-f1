with
    raw_timing_data_sector_segments as (select * from {{ source('live_timing', 'timing_data_sector_segments') }}),
    formatted as (
        select
            SectorKey as sector_key,
            SegmentKey as segment_key,
            Status as sector_status,
            Driver as driver,
            _StreamTimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_timing_data_sector_segments
    )
select *
from formatted