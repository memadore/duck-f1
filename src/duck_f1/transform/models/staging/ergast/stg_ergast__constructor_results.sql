with
raw_constructor_results as (
    select * from {{ source("src_ergast", "ergast__constructor_results") }}
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
        constructor_result.points,
        constructor_result.status
    from raw_constructor_results as constructor_result
    inner join
        constructor_ids as constructor
        on constructor_result.constructorid = constructor.ergast_constructor_id
    inner join race_ids as race on constructor_result.raceid = race.ergast_race_id
)

select *
from formatted
