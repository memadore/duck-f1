with
    raw_timing_data_last_lap as (select * from {{ source('live_timing', 'timing_data_last_lap') }}),
    formatted as (
        select
            Value as lap_time,
            Status as lap_time_status,
            OverallFastest as is_overall_fastest,
            PersonalFastest as is_personal_fastest,
            Driver as driver,
            ts as ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_timing_data_last_lap
    )
select *
from formatted