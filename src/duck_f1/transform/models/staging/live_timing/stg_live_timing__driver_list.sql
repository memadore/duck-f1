with
raw_driver_list as (
        {% if check_if_source_exists(
            "ing__live_timing", "live_timing__driver_list"
        ) | trim == "True" %}

        select * from {{ source("ing__live_timing", "live_timing__driver_list") }}

    {% else %}

            select
                null::integer as racingnumber,
                null::integer as broadcastname,
                null::integer as fullname,
                null::integer as tla,
                null::integer as line,
                null::integer as teamname,
                null::integer as teamcolour,
                null::integer as firstname,
                null::integer as lastname,
                null::integer as reference,
                null::integer as headshoturl,
                null::integer as _streamtimestamp,
                {{ live_timing__empty_metadata() }}
            where false

        {% endif %}
),

formatted as (
    select
        racingnumber as racing_number,
        broadcastname as broadcast_name,
        fullname as full_name,
        tla as driver_abbr,
        line as starting_position,
        teamname as team_name,
        teamcolour as team_color,
        firstname as first_name,
        lastname as last_name,
        reference as driver_id,
        headshoturl as headshort_url,
        _streamtimestamp as _stream_ts,
        {{ live_timing__metadata() }}
    from raw_driver_list
)

select *
from formatted
