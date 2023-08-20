with
    raw_weather_data as (select * from {{ source('live_timing', 'weather_data') }}),
    formatted as (
        select
            ts as ts,
            AirTemp as air_temperature,
            Humidity as relative_humidity,
            Pressure as air_pressure,
            Rainfall as rain_accumulation,
            TrackTemp as track_temperature,
            WindDirection as wind_direction,
            WindSpeed as wind_speed,
            {{ live_timing__metadata_raw_columns() }}
        from raw_weather_data
    )
select *
from formatted