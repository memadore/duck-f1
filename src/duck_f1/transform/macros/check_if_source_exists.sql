{% macro check_if_source_exists(source_key, table_key) %}

    {% set source_relation = adapter.get_relation(
        database=source(source_key, table_key).database,
        schema=source(source_key, table_key).schema,
        identifier=source(source_key, table_key).name,
    ) %}

    {% set table_exists = source_relation is not none %}

    {{ table_exists }}

{% endmacro %}
