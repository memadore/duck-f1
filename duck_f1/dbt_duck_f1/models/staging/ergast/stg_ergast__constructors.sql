with
    raw_constructors as (select * from {{ source("ergast", "constructors") }}),
    formatted as (
        select
            constructorid as ergast_constructor_id,
            constructorref as constructor_reference,
            name as constructor_name,
            nationality as position_label,
            url as url
        from raw_constructors
    )
select *
from formatted
