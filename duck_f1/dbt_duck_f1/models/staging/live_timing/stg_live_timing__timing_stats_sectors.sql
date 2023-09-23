with
    raw_timing_stats_sectors as (
        select * from {{ source("live_timing", "timing_stats_sectors") }}
    ),
    formatted as (
        select
            sectorkey as sector_key,
            value as sector_time,
            position as position,
            driver as driver,
            _streamtimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_timing_stats_sectors
    )
select *
from formatted
