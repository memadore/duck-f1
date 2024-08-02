with
raw_lap_series as (
        {% if check_if_source_exists(
            "src_live_timing", "live_timing__lap_series"
        ) | trim == "True" %}

        select * from {{ source("src_live_timing", "live_timing__lap_series") }}

    {% else %}

            select
                null::integer as drivernumber,
                null::integer as lapnumber,
                null::integer as laposition,
                null::integer as _streamtimestamp,
                {{ live_timing__empty_metadata() }}
            where false

        {% endif %}
),

formatted as (
    select
        drivernumber as driver_number,
        lapnumber as lap_number,
        lapposition as lap_position,
        _streamtimestamp as _stream_ts,
        {{ live_timing__metadata() }}
    from raw_lap_series
)

select *
from formatted
