with
raw_lap_count as (
        {% if check_if_source_exists(
            "src_live_timing", "live_timing__lap_count"
        ) | trim == "True" %}

        select * from {{ source("src_live_timing", "live_timing__lap_count") }}

    {% else %}

            select
                null::integer as metric,
                null::integer as value,
                null::integer as _streamtimestamp,
                {{ live_timing__empty_metadata() }}
            where false

        {% endif %}
),

formatted as (
    select
        metric as metric_lable,
        value as metric_value,
        _streamtimestamp::interval as session_ts,
        {{ live_timing__metadata() }}
    from raw_lap_count
)

select *
from formatted
