with
raw_qualifying as (select * from {{ source("ing__ergast", "ergast__qualifying") }}),

constructor_ids as (
    select
        constructor_id,
        ergast_constructor_id
    from {{ ref("stg_ergast__constructors") }}
),

driver_ids as (
    select
        driver_id,
        ergast_driver_id
    from {{ ref("stg_ergast__drivers") }}
),

race_ids as (
    select
        race_id,
        ergast_race_id
    from {{ ref("stg_ergast__races") }}
),

formatted as (
    select
        driver.driver_id,
        race.race_id,
        constructor.constructor_id,
        qualifying.number as driver_number,
        qualifying.position,
        qualifying.q1 as q1_time_label,
        qualifying.q2 as q2_time_label,
        qualifying.q3 as q3_time_label
    from raw_qualifying as qualifying
    inner join
        constructor_ids as constructor
        on qualifying.constructorid = constructor.ergast_constructor_id
    inner join driver_ids as driver on qualifying.driverid = driver.ergast_driver_id
    inner join race_ids as race on qualifying.raceid = race.ergast_race_id
)

select *
from formatted
