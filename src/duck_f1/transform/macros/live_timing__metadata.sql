{% macro live_timing__metadata() %}
    {{ dbt_utils.generate_surrogate_key([
            "date_part('year', event_date)",
            "event_round_number"
    ]) }} as event_id,
    event_round_number,
    event_sha,
    event_country,
    event_date as event_start_local,
    event_name,
    {{ dbt_utils.generate_surrogate_key([
            "date_part('year', event_date)",
            "event_round_number",
            "session_type"
    ]) }} as session_id,
    session_sha,
    session_type,
    session_date as session_start_local
{% endmacro %}
