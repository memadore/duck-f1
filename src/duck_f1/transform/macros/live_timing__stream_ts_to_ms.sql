{% macro live_timing__stream_ts_to_ms(column) %}
    split_part({{ column }}, ':', 1)::integer * 60 * 60 * 1000
    + split_part({{ column }}, ':', 2)::integer * 60 * 1000
    + floor(split_part({{ column }}, ':', 3)::double) * 1000
    + split_part({{ column }}, '.', 2)::integer as stream_ms,
    to_milliseconds(
        (ceiling(stream_ms / 1000 * 10) / 10 * 1000)::integer
    ) as stream_bucket
{% endmacro %}
