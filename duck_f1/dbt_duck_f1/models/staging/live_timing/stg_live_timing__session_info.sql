with
    raw_session_info as (select * from {{ source('live_timing', 'session_info') }}),
    formatted as (
        select
            MeetingKey as session_key,
            MeetingName as session_name,
            MeetingLocation as session_location,
            MeetingCountryKey as country_key,
            MeetingCountryCode as country_code,
            MeetingCountryName as country_name,
            MeetingCircuitKey as circuit_key,
            MeetingCircuitShortName as circuit_short_name,
            ArchiveStatusStatus as archive_status,
            Key as session_keym,
            Type as session_type,
            Name as session_name,
            StartDate as session_start_date,
            EndDate as session_end_data,
            GmtOffset as gmt_offset,
            Path as session_path,
            {{ live_timing__metadata_raw_columns() }}
        from raw_session_info
    )
select *
from formatted