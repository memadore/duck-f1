with
    raw_driver_list as (
        select * from {{ source("ing__live_timing", "live_timing__driver_list") }}
    ),
    formatted as (
        select
            racingnumber as racing_number,
            broadcastname as broadcast_name,
            fullname as full_name,
            tla as driver_abbr,
            line as starting_position,
            teamname as team_name,
            teamcolour as team_color,
            firstname as first_name,
            lastname as last_name,
            reference as driver_id,
            headshoturl as headshort_url,
            _streamtimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_driver_list
    )
select *
from formatted
