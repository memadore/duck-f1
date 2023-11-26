with
    raw_weather_data as (select * from {{ source("live_timing", "weather_data") }}),
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
            {{ live_timing__metadata_raw_columns() }}
        from raw_weather_data
    )
select *
from formatted
