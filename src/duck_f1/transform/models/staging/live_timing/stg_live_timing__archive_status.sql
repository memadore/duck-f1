with
raw_archive_status as (
        {% if check_if_source_exists(
            "src_live_timing", "live_timing__archive_status"
        ) | trim == "True" %}

        select *
        from {{ source("src_live_timing", "live_timing__archive_status") }}

    {% else %}

            select
            null:integer as status,
            {{ live_timing__empty_metadata() }}
            where false

        {% endif %}
),

formatted as (
    select
        status,
        {{ live_timing__metadata() }}
    from raw_archive_status
)

select *
from formatted
