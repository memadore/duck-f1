with
raw_current_tyres as (
        {% if check_if_source_exists(
            "ing__live_timing", "live_timing__current_tyres"
        ) | trim == "True" %}

        select * from {{ source("ing__live_timing", "live_timing__current_tyres") }}

    {% else %}

            select
                null::integer as driver,
                null::integer as compound,
                null::integer as new,
                null::integer as _streamtimestamp,
                {{ live_timing__empty_metadata() }}
            where false

        {% endif %}
),

formatted as (
    select
        driver as driver_number,
        compound as tyre_compound,
        new as is_new,
        _streamtimestamp as _stream_ts,
        {{ live_timing__metadata() }}
    from raw_current_tyres
)

select *
from formatted