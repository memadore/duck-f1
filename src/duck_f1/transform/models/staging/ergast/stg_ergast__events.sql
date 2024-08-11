with
raw_events as (select * from {{ source("src_ergast", "ergast__races") }}),

circuit_ids as (
    select
        circuit_id,
        _ergast_circuit_id
    from {{ ref("stg_ergast__circuits") }}
),

formatted as (
    select
        {{ dbt_utils.generate_surrogate_key(["_event.year", "_event.round"]) }} as event_id,
        circuit.circuit_id,
        _event.raceid as _ergast_race_id,
        _event.year as season,
        _event.round,
        _event.name as event_name,
        _event.url as wikipedia_url
    from raw_events as _event
    inner join circuit_ids as circuit on _event.circuitid = circuit._ergast_circuit_id
)

select *
from formatted
