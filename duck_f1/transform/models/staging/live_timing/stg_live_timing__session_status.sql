with
    raw_session_status as (
        {% if check_if_source_exists(
            "ing__live_timing", "live_timing__session_status"
        ) | trim == "True" %}

            select *
            from {{ source("ing__live_timing", "live_timing__session_status") }}

        {% else %}

            select
                null::integer as status,
                null::integer as _streamtimestamp,
                {{ live_timing__empty_metadata() }}
            where false

        {% endif %}
    ),
    formatted as (
        select
            status as session_status,
            _streamtimestamp as _stream_ts,
            {{ live_timing__metadata() }}
        from raw_session_status
    )
select *
from formatted
