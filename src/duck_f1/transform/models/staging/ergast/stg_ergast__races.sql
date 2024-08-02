with
raw_races as (select * from {{ source("src_ergast", "ergast__races") }}),

circuit_ids as (
    select
        circuit_id,
        ergast_circuit_id
    from {{ ref("stg_ergast__circuits") }}
),

formatted as (
    select
        {{ dbt_utils.generate_surrogate_key(["race.date", "race.name"]) }} as race_id,
        circuit.circuit_id,
        race.raceid as ergast_race_id,
        race.year,
        race.round,
        race.name,
        race.date,
        race.time as event_time,
        cast(concat(
            race.date,
            ' ',
            case when race.time like '\N' then '00:00:00' else race.time end,
            ' GMT'
        ) as timestamptz) as race_time_utc,
        race.url,
        race.fp1_date,
        race.fp1_time,
        race.fp2_date,
        race.fp2_time,
        race.fp3_date,
        race.fp3_time,
        race.quali_date,
        race.quali_time,
        race.sprint_date,
        race.sprint_time
    from raw_races as race
    inner join circuit_ids as circuit on race.circuitid = circuit.ergast_circuit_id
)

select *
from formatted
