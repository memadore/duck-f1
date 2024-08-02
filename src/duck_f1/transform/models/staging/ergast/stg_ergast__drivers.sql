with
raw_drivers as (select * from {{ source("src_ergast", "ergast__drivers") }}),

formatted as (
    select
        {{ dbt_utils.generate_surrogate_key(["forename", "surname", "dob"]) }}
            as driver_id,
        driverid as ergast_driver_id,
        driverref as driver_reference,
        case when number not like '\N' then number end as driver_number,
        case when code not like '\N' then code end as driver_code,
        forename as first_name,
        surname as last_name,
        concat_ws(' ', forename, surname) as full_name,
        dob as date_of_birth,
        cast(datesub('y', date_of_birth, today()) as integer) as age_years,
        cast(datesub('d', date_of_birth, today()) as integer) as age_days,
        nationality,
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
    ) as age_label
from formatted
