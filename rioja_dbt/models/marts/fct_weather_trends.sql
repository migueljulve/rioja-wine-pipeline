{{ config(materialized='table') }}

with daily_weather as (
    -- Reference the cleaned staging weather data
    select * from {{ ref('stg_weather') }}
    -- Exclude the current incomplete year to avoid skewed averages
    where extract(year from record_date) < 2026
),

-- Step 1: Aggregate metrics per Station and Year to avoid double-counting 
-- when multiple stations report data for the same day.
metrics_per_station as (
    select
        extract(year from record_date) as year,
        station_name,
        avg(temp_avg) as station_avg_temp,
        sum(precipitation_acc_mm) as station_total_precip,
        -- Count frost days specifically in Spring (April, May, June)
        count(case 
            when temp_min <= 0 and extract(month from record_date) in (4, 5, 6) 
            then 1 
        end) as station_frost_days,
        -- Count extreme heat days specifically in Summer (July, August)
        count(case 
            when temp_max >= 35 and extract(month from record_date) in (7, 8) 
            then 1 
        end) as station_heat_days
    from daily_weather
    group by year, station_name
)

-- Step 2: Average the station results to get a regional representative value for Rioja
select
    year,
    round(avg(station_avg_temp), 2) as avg_yearly_temp,
    round(avg(station_frost_days), 1) as avg_spring_frost_days,
    round(avg(station_heat_days), 1) as avg_extreme_heat_days,
    round(avg(station_total_precip), 2) as avg_annual_precipitation
from metrics_per_station
group by year
order by year desc