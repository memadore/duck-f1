with
    raw_drivers as (select * from {{ source('ergast', 'drivers') }}),
    formatted as (
        select
            driverid as driver_id,
            driverref as driver_reference,
            number as driver_number,
            code as driver_code,
            forename as first_name,
            surname as last_name,
            dob as date_of_birth,
            nationality as nationality,
            url as url
        from raw_drivers
    )
select *
from formatted