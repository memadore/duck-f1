with
driver_list as (
    select * from {{ ref("stg_live_timing__driver_list") }}
),

formatted as (
    select distinct
        car_number,
        first_name,
        last_name,
        full_name,
        _full_name_key,
        driver_code,
        first_value(_live_timing_driver_id ignore nulls)
            over (partition by full_name)
            as _live_timing_driver_id
    from driver_list
)

select
    _live_timing_driver_id,
    _full_name_key,
    first_name,
    last_name,
    full_name,
    driver_code,
    array_agg(car_number) as car_numbers
from formatted
group by
    _live_timing_driver_id,
    _full_name_key,
    first_name,
    last_name,
    full_name,
    driver_code
