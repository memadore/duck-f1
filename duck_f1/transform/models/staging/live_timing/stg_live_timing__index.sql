with
    raw_index as (
        {% if check_if_source_exists(
            "ing__live_timing", "live_timing__index"
        ) | trim == "True" %}

            select * from {{ source("ing__live_timing", "live_timing__index") }}

        {% else %}

            select
                null::integer as keyframepath,
                null::integer as streampath,
                {{ live_timing__empty_metadata() }}
            where false

        {% endif %}
    ),
    formatted as (
        select
            keyframepath as key_frame_path,
            streampath as stream_path,
            {{ live_timing__metadata() }}
        from raw_index
    )
select *
from formatted
