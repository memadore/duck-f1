with
    raw_races as (select * from {{ source('ergast', 'races') }}),
    formatted as (
        select
            raceid as race_id,
            year as year,
            round as round,
            circuitid as circuit_id,
            name as name,
            date as date,
            time as q2_time_label,
            url as q3_time_label,
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
        from raw_races
    )
select *
from formatted