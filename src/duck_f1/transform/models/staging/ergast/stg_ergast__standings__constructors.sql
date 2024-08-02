with
raw_constructor_standings as (
    select * from {{ source("src_ergast", "ergast__constructor_standings") }}
),

constructor_ids as (
    select
        constructor_id,
        ergast_constructor_id
    from {{ ref("stg_ergast__constructors") }}
),

race_ids as (
    select
        race_id,
        ergast_race_id
    from {{ ref("stg_ergast__races") }}
),

formatted as (
    select
        constructor.constructor_id,
        race.race_id,
        constructor_standings.points,
        constructor_standings.position,
        constructor_standings.positiontext as position_label,
        constructor_standings.wins as win_count
    from raw_constructor_standings as constructor_standings
    inner join
        constructor_ids as constructor
        on constructor_standings.constructorid = constructor.ergast_constructor_id
    inner join race_ids as race on constructor_standings.raceid = race.ergast_race_id
)

select *
from formatted
