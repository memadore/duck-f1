with
    raw_timing_data_speeds as (select * from {{ source('live_timing', 'timing_data_speeds') }}),
    formatted as (
        select
            SpeedKey as speed_key,
            Value as speed_value,
            Status as speed_status,
            OverallFastest as is_overall_fastest,
            PersonalFastest as is_personal_fastest,
            Driver as driver,
            ts as ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_timing_data_speeds
    )
select *
from formatted