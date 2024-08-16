with
raw_timing_data_status as (
        {% if check_if_source_exists(
            "src_live_timing", "live_timing__timing_data_status"
        ) | trim == "True" %}

        select *
        from {{ source("src_live_timing", "live_timing__timing_data_status") }}

    {% else %}

            select
                null::integer as driver,
                null::integer as metricname,
                null::integer as metricvalue,
                null::integer as _streamtimestamp,
                {{ live_timing__empty_metadata() }}
            where false

        {% endif %}
),

formatted as (
    select
        driver,
        metricname as metric_name,
        metricvalue as metric_value,
        _streamtimestamp::interval as session_ts,
        {{ live_timing__metadata() }}
    from raw_timing_data_status
)

select *
from formatted
