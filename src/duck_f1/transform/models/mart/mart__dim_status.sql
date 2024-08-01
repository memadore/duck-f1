select
    {{
        dbt_utils.star(
            from=ref("stg_ergast__status"),
            except=["ergast_status_id"]
        )
    }}
from {{ ref("stg_ergast__status") }}
