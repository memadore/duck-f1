select
    driver_list.session_id,
    driver.driver_id,
    driver_list.car_number
from {{ ref("stg_live_timing__driver_list") }} as driver_list
left join {{ ref("int_drivers") }} as driver
    on driver_list._full_name_key = driver._full_name_key
