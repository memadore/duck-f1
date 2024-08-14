{% macro varchar_lap_time_to_interval(column) %}
    to_milliseconds(
        (split_part({{ column }}, ':', 1)::integer * 60 * 1000)
        + (floor(split_part({{ column }}, ':', 2)::double)::integer * 1000)
        + (split_part({{ column }}, '.', 2)::integer)
    )
{% endmacro %}
