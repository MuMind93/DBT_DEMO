WITH daily_weather as(

    select
    
    date(time) as daily_weather,
    weather,
    temp,
    pressure,
    humidity,
    clouds

    from {{ source('demo', 'weather') }}

    limit 10
),

daily_weather_agg as (

select
daily_weather,
weather,
count(weather),

ROW_NUMBER() OVER (PARTITION BY daily_weather ORDER BY count(weather) desc) AS row_number

from daily_weather
group by daily_weather, weather

)


select
*
from daily_weather_agg