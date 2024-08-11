with

ergast_drivers as (
    select
        driver_id,
        _ergast_driver_id,
        _live_timing_driver_id,
        _full_name_key,
        first_name,
        last_name,
        full_name,
        driver_code,
        is_driver_code_generated,
        date_of_birth,
        age_years,
        age_days,
        age_label,
        nationality,
        wikipedia_url,
        [car_number] as car_numbers
    from {{ ref("stg_ergast__drivers") }}
),

live_timing_drivers as (
    select
        {{ dbt_utils.generate_surrogate_key(["_live_timing_driver_id"]) }}
            as driver_id,
        null::integer as _ergast_driver_id,
        _live_timing_driver_id,
        _full_name_key,
        first_name,
        last_name,
        full_name,
        driver_code,
        false as is_driver_code_generated,
        null::integer as date_of_birth,
        null::integer as age_years,
        null::integer as age_days,
        null::integer as age_label,
        null::integer as nationality,
        null::integer as wikipedia_url,
        car_numbers
    from {{ ref("int_live_timing__drivers") }}
    where _full_name_key not in (
        select _full_name_key
        from {{ ref("stg_ergast__drivers") }}
    )
),

drivers as (
    select *
    from ergast_drivers
    union
    select *
    from live_timing_drivers
)

select *
from drivers
