{% macro live_timing__empty_metadata() %}
    null::integer as season_round,
    null::integer as event_sha,
    null::integer as event_country,
    null::integer as event_date,
    null::integer as event_name,
    null::integer as session_sha,
    null::integer as session_type,
    null::integer as session_date
{% endmacro %}
