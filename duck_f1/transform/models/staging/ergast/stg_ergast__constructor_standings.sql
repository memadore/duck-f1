with
    raw_constructor_standings as (
        select * from {{ source("ing__ergast", "ergast__constructor_standings") }}
    ),
    foreign_keys as (
        select *
        from raw_constructor_standings cs
        join
            (
                select constructor_id, ergast_constructor_id
                from {{ ref("stg_ergast__constructors") }}
            ) construc
            on cs.constructorid = construc.ergast_constructor_id
        join
            (select race_id, ergast_race_id from {{ ref("stg_ergast__races") }}) race
            on cs.raceid = race.ergast_race_id
    ),
    formatted as (
        select
            constructor_id,
            race_id,
            points as points,
            position as position,
            positiontext as position_label,
            wins as win_count
        from foreign_keys
    )
select *
from formatted
