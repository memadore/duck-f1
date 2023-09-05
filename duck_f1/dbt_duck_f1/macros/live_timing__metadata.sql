{% macro live_timing__metadata_raw_columns() %}
    season_round as season_round,
    event_key as event_key,
    event_country as event_country,
    event_date as event_date,
    event_name as event_name,
    session_key as session_key,
    session_type as session_type,
    session_date as session_date
{% endmacro %}