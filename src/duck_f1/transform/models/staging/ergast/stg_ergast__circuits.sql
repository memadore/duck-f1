with
raw_circuits as (select * from {{ source("ing__ergast", "ergast__circuits") }}),

formatted as (
    select
        {{ dbt_utils.generate_surrogate_key(["circuitref"]) }} as circuit_id,
        circuitid as ergast_circuit_id,
        circuitref as circuit_ref,
        name as circuit_name,
        location as circuit_location,
        country as circuit_country,
        lat as latitude,
        lng as longitude,
        alt as altitude,
        url as circuit_url
    from raw_circuits
)

select *
from formatted
