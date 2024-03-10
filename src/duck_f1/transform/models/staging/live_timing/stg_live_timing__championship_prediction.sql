with
    raw_championship_prediction as (
        {% if check_if_source_exists(
            "ing__live_timing", "live_timing__championship_prediction"
        ) | trim == "True" %}

            select *
            from
                {{ source("ing__live_timing", "live_timing__championship_prediction") }}

        {% else %}

            select
                null::integer as entity,
                null::integer as identifier,
                null::integer as metric,
                null::integer as value,
                null::integer as _streamtimestamp,
                {{ live_timing__empty_metadata() }}
            where false

        {% endif %}
    ),
    formatted as (
        select
            entity as entity,
            identifier as identifier,
            metric as metric_name,
            value as metric_value,
            _streamtimestamp as _stream_ts,
            {{ live_timing__metadata() }}
        from raw_championship_prediction
    )
select *
from formatted
