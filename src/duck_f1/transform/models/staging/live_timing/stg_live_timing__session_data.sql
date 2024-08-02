with
raw_session_data as (
        {% if check_if_source_exists(
            "src_live_timing", "live_timing__session_data"
        ) | trim == "True" %}

        select * from {{ source("src_live_timing", "live_timing__session_data") }}

    {% else %}

            select
                null::integer as key,
                null::integer as utc,
                null::integer as metricname,
                null::integer as metricvalue,
                {{ live_timing__empty_metadata() }}
            where false

        {% endif %}
),

formatted as (
    select
        key as serie_key,
        utc as utc_ts,
        metricname as metric_name,
        metricvalue as metric_value,
        {{ live_timing__metadata() }}
    from raw_session_data
)

select *
from formatted
