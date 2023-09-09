with
    raw_driver_list as (select * from {{ source('live_timing', 'driver_list') }}),
    formatted as (
        select
            RacingNumber as racing_number,
            BroadcastName as broadcast_name,
            FullName as file_name,
            Tla as driver_abbr,
            Line as starting_position,
            TeamName as team_name,
            TeamColour as team_color,
            FirstName as first_name,
            LastName as last_name,
            Reference as driver_id,
            HeadshotUrl as headshort_url,
            _StreamTimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_driver_list
    )
select *
from formatted