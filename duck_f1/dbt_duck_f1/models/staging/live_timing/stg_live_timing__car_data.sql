with
    raw_car_data as (select * from {{ source('live_timing', 'car_data') }}),
    formatted as (
        select
            ts as ts,
            capture_ts as capture_ts,
            car_number as car_number,
            channel as channel,
            value as value,
            {{ live_timing__metadata_raw_columns() }}
        from raw_car_data
    )
select *
from formatted