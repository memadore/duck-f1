with
raw_session_info as (
        {% if check_if_source_exists(
            "src_live_timing", "live_timing__session_info"
        ) | trim == "True" %}

        select * from {{ source("src_live_timing", "live_timing__session_info") }}

    {% else %}

            select
                null::integer as meetingkey,
                null::integer as meetingname,
                null::integer as meetinglocation,
                null::integer as meetingcountrykey,
                null::integer as meetingcountrycode,
                null::integer as meetingcountryname,
                null::integer as meetingcircuitkey,
                null::integer as meetingcircuitshortname,
                null::integer as archivestatusstatus,
                null::integer as key,
                null::integer as type,
                null::integer as name,
                null::integer as startdate,
                null::integer as enddate,
                null::integer as gmtoffset,
                null::integer as path,
                {{ live_timing__empty_metadata() }}
            where false

        {% endif %}
),

formatted as (
    select
        meetingkey as meeting_key,
        meetingname as meeting_name,
        meetinglocation as meeting_locatio,
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
        {{ live_timing__metadata() }}
    from raw_session_info
)

select *
from formatted
