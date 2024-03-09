with
    raw_track_status as (
        {% if check_if_source_exists(
            "ing__live_timing", "live_timing__track_status"
        ) | trim == "True" %}

            select * from {{ source("ing__live_timing", "live_timing__track_status") }}

        {% else %}

            select
                null::integer as status,
                null::integer as message,
                null::integer as _streamtimestamp,
                {{ live_timing__empty_metadata() }}
            where false

        {% endif %}
    ),
    formatted as (
        select
            status as status_id,
            message as status_message,
            _streamtimestamp as _stream_ts,
            {{ live_timing__metadata() }}
        from raw_track_status
    )
select *
from formatted
