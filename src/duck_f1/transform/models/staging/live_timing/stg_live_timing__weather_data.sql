with
raw_weather_data as (
        {% if check_if_source_exists(
            "src_live_timing", "live_timing__weather_data"
        ) | trim == "True" %}

        select * from {{ source("src_live_timing", "live_timing__weather_data") }}

    {% else %}

            select
                null::integer as airtemp,
                null::integer as humidity,
                null::integer as pressure,
                null::integer as rainfall,
                null::integer as tracktemp,
                null::integer as winddirection,
                null::integer as windspeed,
                null::integer as _streamtimestamp,
                {{ live_timing__empty_metadata() }}
            where false

        {% endif %}

),

formatted as (
    select
        airtemp as air_temperature,
        humidity as relative_humidity,
        pressure as air_pressure,
        rainfall as rain_accumulation,
        tracktemp as track_temperature,
        winddirection as wind_direction,
        windspeed as wind_speed,
        _streamtimestamp as _stream_ts,
        {{ live_timing__stream_ts_to_ms("_stream_ts") }},
        {{ live_timing__metadata() }}
    from raw_weather_data
)

select *
from formatted
