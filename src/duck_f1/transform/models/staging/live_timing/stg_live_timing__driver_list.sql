with
raw_driver_list as (
        {% if check_if_source_exists(
            "src_live_timing", "live_timing__driver_list"
        ) | trim == "True" %}

        select
            *,
            {{ live_timing__metadata() }}
        from {{ source("src_live_timing", "live_timing__driver_list") }}

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

computed as (
    select
        session_id,
        reference as _live_timing_driver_id,
        racingnumber::integer as car_number,
        tla as driver_code,
        line as starting_position,
        teamname as team_name,
        teamcolour as team_color,
        headshoturl as head_shot_url,
        _streamtimestamp::interval as session_ts,
        trim(broadcastname) as broadcast_name,
        trim(fullname) as full_name,
        trim(broadcast_name[3:]) as _last_name,
        trim(string_split(full_name, _last_name)[1]) as first_name
    from raw_driver_list
),

formatted as (
    select
        session_id,
        _live_timing_driver_id,
        car_number,
        broadcast_name,
        first_name,
        driver_code,
        starting_position,
        team_name,
        team_color,
        head_shot_url,
        session_ts,
        _last_name[1] || lower(_last_name[2:]) as last_name,
        concat(first_name, ' ', last_name) as full_name,
        lower(strip_accents(concat(first_name, last_name))) as _full_name_key
    from computed
)

select *
from formatted
