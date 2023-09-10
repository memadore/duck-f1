with
    raw_timing_data_sectors as (
        select * from {{ source("live_timing", "timing_data_sectors") }}
    ),
    formatted as (
        select
            sectorkey as sector_key,
            stopped as is_stopped,
            value as sector_time,
            previousvalue as previous_value,
            status as sector_status,
            overallfastest as is_overall_fastest,
            personalfastest as is_personal_fastest,
            driver as driver,
            _streamtimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_timing_data_sectors
    )
select *
from formatted
