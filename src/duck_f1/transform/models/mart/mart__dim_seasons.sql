select
    {{
        dbt_utils.star(
            from=ref("stg_ergast__seasons"),
            except=["ergast_season_id"],
        )
    }}
from {{ ref("stg_ergast__seasons") }}
