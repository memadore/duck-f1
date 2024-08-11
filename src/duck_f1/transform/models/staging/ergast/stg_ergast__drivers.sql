with
raw_drivers as (select * from {{ source("src_ergast", "ergast__drivers") }}),

computed as (
    select
        driverid as _ergast_driver_id,
        driverref as driver_reference,
        if(number = '\N', null, number)::integer as car_number,
        forename as first_name,
        surname as last_name,
        concat(first_name, ' ', last_name) as full_name,
        lower(strip_accents(concat(first_name, last_name))) as _full_name_key,
        upper(
            concat(
                replace(strip_accents(first_name), ' ', '')[0:3],
                replace(strip_accents(last_name), ' ', '')[0:3]
            )
        ) as _live_timing_driver,
        if(code = '\N', upper(replace(strip_accents(last_name), ' ', ''))[0:3], code)
            as driver_code,
        if(code = '\N', true, false) as is_driver_code_generated,
        dob::date as date_of_birth,
        row_number()
            over (partition by _live_timing_driver order by date_of_birth)
            as _driver_id_rank,
        format('{:s}{:02d}', _live_timing_driver, _driver_id_rank) as _live_timing_driver_id,
        {{ dbt_utils.generate_surrogate_key(["_live_timing_driver_id"]) }}
            as driver_id,
        datesub('y', date_of_birth, today())::integer as age_years,
        datesub('d', date_of_birth, today())::integer as age_days,
        concat(
            age_years,
            ' years, ',
            datesub('d', date_of_birth + to_years(age_years), today()),
            ' days'
        ) as age_label,
        nationality,
        url as wikipedia_url
    from raw_drivers
),

formatted as (
    select
        driver_id,
        _ergast_driver_id,
        _live_timing_driver_id,
        _full_name_key,
        car_number,
        first_name,
        last_name,
        full_name,
        driver_reference,
        driver_code,
        is_driver_code_generated,
        date_of_birth,
        age_years,
        age_days,
        age_label,
        nationality,
        wikipedia_url
    from computed
)

select *
from formatted
