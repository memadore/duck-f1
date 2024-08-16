with
raw_race_control_messages as (
        {% if check_if_source_exists(
            "src_live_timing", "live_timing__race_control_messages"
        ) | trim == "True" %}

        select *
        from {{ source("src_live_timing", "live_timing__race_control_messages") }}

    {% else %}

            select
                null::integer as messageid,
                null::integer as utc,
                null::integer as lap,
                null::integer as category,
                null::integer as messagedata,
                null::integer as _streamtimestamp,
                {{ live_timing__empty_metadata() }}
            where false

        {% endif %}
),

formatted as (
    select
        messageid as message_id,
        utc as utc_ts,
        lap as lap_number,
        category as message_category,
        messagedata as message_data,
        _streamtimestamp::interval as session_ts,
        {{ live_timing__metadata() }}
    from raw_race_control_messages
)

select *
from formatted
