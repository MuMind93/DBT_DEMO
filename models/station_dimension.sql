WITH BIKE as(

    select 
    distinct
    start_statio_id as station_id,
    start_station_name as station_name,
    START_LAT as start_lat,
    START_LNG as start_lng

    from {{ ref('stg_bike') }}
    where RIDE_ID != 'ride_id'

)

select
*
from BIKE 