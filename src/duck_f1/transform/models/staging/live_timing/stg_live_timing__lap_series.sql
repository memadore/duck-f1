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

renamed as (
    select
        drivernumber as car_number,
        lapnumber as lap_number,
        lapposition as lap_position,
        _streamtimestamp::interval as session_ts,
        {{ live_timing__metadata() }}
    from raw_lap_series
),

formatted as (
    select
        session_id,
        session_type,
        car_number,
        lap_number,
        lap_position,
        session_ts as lap_end_ts,
        lag(session_ts)
            over (partition by session_id, car_number order by session_ts)
            as lap_start_ts,
        lap_end_ts - lap_start_ts as lap_time
    from renamed
)

select *
from formatted
