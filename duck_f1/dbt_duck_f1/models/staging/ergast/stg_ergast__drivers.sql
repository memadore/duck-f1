with
    raw_drivers as (select * from {{ source("ergast", "drivers") }}),
    formatted as (
        select
            {{ dbt_utils.generate_surrogate_key(["forename", "surname", "dob"]) }}
            as driver_id,
            driverid as ergast_driver_id,
            driverref as driver_reference,
            number as driver_number,
            code as driver_code,
            forename as first_name,
            surname as last_name,
            concat_ws(' ', forename, surname) as full_name,
            strptime(dob, '%Y-%m-%d') as date_of_birth,
            datesub('y', date_of_birth, today())::integer as age_years,
            datesub('d', date_of_birth, today())::integer as age_days,
            nationality as nationality,
            url as wiki_url
        from raw_drivers
    )
select
    *,
    concat(
        age_years,
        ' years, ',
        datesub('d', date_of_birth + to_years(age_years), today()),
        ' days'
    ) as age_label,
from formatted
