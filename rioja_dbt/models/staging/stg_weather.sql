with source as (
    select * from {{ source('rioja_raw', 'weather_stations_raw') }}
)

select
    -- 1. Convert Timestamp to clean DATE
    date(date) as record_date,

    -- 2. ALL Weather Metrics (numbers in Float format)
    temp_avg,
    temp_max,
    temp_min,
    rel_hum_avg,
    rel_hum_max,
    rel_hum_min,
    solar_rad_acc,
    wind_speed_ms_avg,
    wind_speed_ms_max,
    wind_speed_kmh_avg,
    wind_speed_kmh_max,
    wind_dir_avg,
    wind_dir_max,
    precip_acc as precipitation_acc_mm, -- Renamed for clarity
    eto_penman_monteith,
    soil_temp_avg,
    soil_temp_max,
    soil_temp_min,
    leaf_wetness_1,
    leaf_wetness_2,
    station_name
    
from source