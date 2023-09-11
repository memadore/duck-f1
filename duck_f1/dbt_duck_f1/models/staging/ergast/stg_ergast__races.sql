with
    raw_races as (select * from {{ source("ergast", "races") }}),
    foreign_keys as (
        select *
        from raw_races race
        join
            (
                select circuit_id, ergast_circuit_id
                from {{ ref("stg_ergast__circuits") }}
            ) circuit
            on race.circuitid = circuit.ergast_circuit_id
    ),
    formatted as (
        select
            {{ dbt_utils.generate_surrogate_key(["date", "name"]) }} as race_id,
            circuit_id,
            raceid as ergast_race_id,
            year as year,
            round as round,
            name as name,
            date as date,
            time as event_time,
            url as url,
            fp1_date as fp1_date,
            fp1_time as fp1_time,
            fp2_date as fp2_date,
            fp2_time as fp2_time,
            fp3_date as fp3_date,
            fp3_time as fp3_time,
            quali_date as quali_date,
            quali_time as quali_time,
            sprint_date as sprint_date,
            sprint_time as sprint_time
        from foreign_keys
    )
select *
from formatted
