with
    raw_constructor_results as (
        select * from {{ source("ing__ergast", "ergast__constructor_results") }}
    ),
    foreign_keys as (
        select *
        from raw_constructor_results cr
        join
            (
                select constructor_id, ergast_constructor_id
                from {{ ref("stg_ergast__constructors") }}
            ) construc
            on cr.constructorid = construc.ergast_constructor_id
        join
            (select race_id, ergast_race_id from {{ ref("stg_ergast__races") }}) race
            on cr.raceid = race.ergast_race_id
    ),
    formatted as (select constructor_id, race_id, points, status from foreign_keys)
select *
from formatted
