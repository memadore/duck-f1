with formatted as (
    select
        driver_id,
        first_name,
        last_name,
        full_name,
        driver_code,
        is_driver_code_generated,
        car_numbers,
        date_of_birth,
        age_years,
        age_days,
        age_label,
        nationality,
        wikipedia_url
    from {{ ref("int_drivers") }}
)

select *
from formatted
