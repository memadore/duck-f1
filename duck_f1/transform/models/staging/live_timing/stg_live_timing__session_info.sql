with
    raw_session_info as (
        select * from {{ source("ing__live_timing", "live_timing__session_info") }}
    ),
    formatted as (
        select
            meetingkey as session_sha,
            meetingname as session_name,
            meetinglocation as session_location,
            meetingcountrykey as country_key,
            meetingcountrycode as country_code,
            meetingcountryname as country_name,
            meetingcircuitkey as circuit_key,
            meetingcircuitshortname as circuit_short_name,
            archivestatusstatus as archive_status,
            key as session_key,
            type as session_type,
            name as session_name,
            startdate as session_start_date,
            enddate as session_end_data,
            gmtoffset as gmt_offset,
            path as session_path,
            {{ live_timing__metadata_raw_columns() }}
        from raw_session_info
    )
select *
from formatted
