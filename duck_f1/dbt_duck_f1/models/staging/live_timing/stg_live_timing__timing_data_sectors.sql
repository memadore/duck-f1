with
    raw_timing_data_sectors as (select * from {{ source('live_timing', 'timing_data_sectors') }}),
    formatted as (
        select
            SectorKey as sector_key,
            Stopped as is_stopped,
            Value as sector_time,
            PreviousValue as previous_value,
            Status as sector_status,
            OverallFastest as is_overall_fastest,
            PersonalFastest as is_personal_fastest,
            Driver as driver,
            _StreamTimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_timing_data_sectors
    )
select *
from formatted