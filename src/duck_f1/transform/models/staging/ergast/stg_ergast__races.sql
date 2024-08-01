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
        if(race.time = '\N', null, race.time) as event_time,
        cast(concat(
            race.date,
            ' ',
            case when race.time like '\N' then '00:00:00' else race.time end,
            ' GMT'
        ) as timestamptz) as race_time_utc,
        race.url,
        if(race.fp1_date = '\N', null, race.fp1_date) as fp1_date,
        if(race.fp1_time = '\N', null, race.fp1_time) as fp1_time,
        if(race.fp2_date = '\N', null, race.fp2_date) as fp2_date,
        if(race.fp2_time = '\N', null, race.fp2_time) as fp2_time,
        if(race.fp3_date = '\N', null, race.fp3_date) as fp3_date,
        if(race.fp3_time = '\N', null, race.fp3_time) as fp3_time,
        if(race.quali_date = '\N', null, race.quali_date) as quali_date,
        if(race.quali_time = '\N', null, race.quali_time) as quali_time,
        if(race.sprint_date = '\N', null, race.sprint_date) as sprint_date,
        if(race.sprint_time = '\N', null, race.sprint_time) as sprint_time

    from raw_races as race
    inner join circuit_ids as circuit on race.circuitid = circuit.ergast_circuit_id
)

select *
from formatted
