with
raw_tyre_stint_series as (
        {% if check_if_source_exists(
            "src_live_timing", "live_timing__tyre_stint_series"
        ) | trim == "True" %}

        select *
        from {{ source("src_live_timing", "live_timing__tyre_stint_series") }}

    {% else %}

            select
                null::integer as driver,
                null::integer as stint,
                null::integer as compound,
                null::integer as new,
                null::integer as tyresnotchanged,
                null::integer as totallaps,
                null::integer as startlaps,
                null::integer as _streamtimestamp,
                {{ live_timing__empty_metadata() }}
            where false

        {% endif %}
),

formatted as (
    select
        driver as car_number,
        stint::integer + 1 as stint_sequence,
        compound as tyre_compound,
        new as is_new,
        tyresnotchanged as tyres_not_changed,
        totallaps as total_laps,
        startlaps as start_laps,
        _streamtimestamp::interval as session_ts,
        {{ live_timing__metadata() }}
    from raw_tyre_stint_series
)

select *
from formatted
