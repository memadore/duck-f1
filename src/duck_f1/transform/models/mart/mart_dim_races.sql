select
    {{
        dbt_utils.star(
            from=ref("stg_ergast__races"),
            except=["_ergast_race_id"],
        )
    }}
from {{ ref("stg_ergast__races") }}
