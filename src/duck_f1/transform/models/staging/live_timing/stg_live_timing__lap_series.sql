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
        _streamtimestamp::interval as _stream_ts,
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
        _stream_ts as lap_end_ts,
        lag(_stream_ts)
            over (partition by session_id, car_number order by _stream_ts)
            as lap_start_ts,
        lap_end_ts - lap_start_ts as lap_time
    from renamed
)

select *
from formatted
