with
    raw_qualifying as (select * from {{ source("ing__ergast", "ergast__qualifying") }}),
    foreign_keys as (
        select *
        from raw_qualifying quali
        join
            (
                select constructor_id, ergast_constructor_id
                from {{ ref("stg_ergast__constructors") }}
            ) construc
            on quali.constructorid = construc.ergast_constructor_id
        join
            (
                select driver_id, ergast_driver_id from {{ ref("stg_ergast__drivers") }}
            ) driver
            on quali.driverid = driver.ergast_driver_id
        join
            (select race_id, ergast_race_id from {{ ref("stg_ergast__races") }}) race
            on quali.raceid = race.ergast_race_id
    ),
    formatted as (
        select
            driver_id,
            race_id,
            constructor_id,
            number as driver_number,
            position as position,
            q1 as q1_time_label,
            q2 as q2_time_label,
            q3 as q3_time_label
        from foreign_keys
    )
select *
from formatted
