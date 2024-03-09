{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if default_schema | length == 0 -%} {{ custom_schema_name | lower }}
    {%- else -%} {{ default_schema | lower }}_{{ custom_schema_name | trim | lower }}
    {%- endif -%}

{%- endmacro %}
