{% macro live_timing__metadata_raw_columns() %}
    season_round as season_round,
    event_country as event_country,
    event_date as event_date,
    event_name as event_name,
    session_type as session_type,
    session_date as session_date
{% endmacro %}