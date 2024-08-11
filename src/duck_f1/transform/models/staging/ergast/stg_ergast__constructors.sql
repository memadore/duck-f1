with
raw_constructors as (
    select * from {{ source("src_ergast", "ergast__constructors") }}
),

formatted as (
    select
        {{ dbt_utils.generate_surrogate_key(["name", "nationality"]) }}
            as constructor_id,
        constructorid as _ergast_constructor_id,
        constructorref as constructor_reference,
        name as constructor_name,
        nationality,
        url as wikipedia_url
    from raw_constructors
)

select *
from formatted
