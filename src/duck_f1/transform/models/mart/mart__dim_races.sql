select
    {{
        dbt_utils.star(
            from=ref("stg_ergast__races"),
            except=["ergast_race_id"],
        )
    }}
from {{ ref("stg_ergast__races") }}
