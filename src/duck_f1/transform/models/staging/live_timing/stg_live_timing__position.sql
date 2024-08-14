with
raw_position as (
        {% if check_if_source_exists(
            "src_live_timing", "live_timing__position"
        ) | trim == "True" %}

        select * from {{ source("src_live_timing", "live_timing__position") }}

    {% else %}

            select
                null::integer as timestamp,
                null::integer as driver,
                null::integer as status,
                null::integer as x,
                null::integer as y,
                null::integer as z,
                null::integer as _streamtimestamp,
                {{ live_timing__empty_metadata() }}
            where false

        {% endif %}
),

formatted as (
    select
        timestamp as event_utc_ts,
        driver,
        status,
        x as x_position,
        y as y_position,
        z as z_position,
        _streamtimestamp::interval as _stream_ts,
        {{ live_timing__metadata() }}
    from raw_position
)

select *
from formatted
