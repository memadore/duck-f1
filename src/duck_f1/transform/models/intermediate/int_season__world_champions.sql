with
driver_championship as (
    select
        _session.year as season,
        standing.driver_id
    from {{ ref("stg_ergast__standings__drivers") }} as standing
    inner join
        {{ ref("stg_ergast__races") }} as _session
        on standing.session_id = _session.session_id
    qualify
        row_number()
            over (partition by _session.year order by _session.round desc, standing.points desc)
        = 1
),

constructor_championship as (
    select
        _session.year as season,
        standing.constructor_id
    from {{ ref("stg_ergast__standings__constructors") }} as standing
    inner join
        {{ ref("stg_ergast__races") }} as _session
        on standing.session_id = _session.session_id
    qualify
        row_number()
            over (partition by _session.year order by _session.round desc, standing.points desc)
        = 1
)

select
    season.year as season,
    driver.driver_id,
    constructor.constructor_id
from {{ ref("mart_dim_seasons") }} as season
left join driver_championship as driver on season.year = driver.season
left join constructor_championship as constructor on season.year = constructor.season
