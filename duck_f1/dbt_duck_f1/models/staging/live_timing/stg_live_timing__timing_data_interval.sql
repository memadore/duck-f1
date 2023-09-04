with
    raw_timing_data_interval as (select * from {{ source('live_timing', 'timing_data_interval') }}),
    formatted as (
        select
            Value as next_car_interval,
            Catching as is_catching,
            Driver as driver,
            ts as ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_timing_data_interval
    )
select *
from formatted