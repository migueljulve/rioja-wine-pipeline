-- Staging Layer: Cleaning and preparing the historical wine production table
with source as (
    -- Extract raw data from the source defined in sources.yml
    select * from {{ source('rioja_raw', 'rioja_wine_history') }}
)

select
    -- 1. Temporal Axis
    year,

    -- 2. Surface and Yield Metrics (Hectares)
    dry_ha,
    irr_ha,
    area_ha,
    dry_prod_ha,
    irr_prod_ha,
    prod_ha,
    yield_dry as yield_dry_kg_ha,  
    yield_irr as yield_irr_kg_ha,

    -- 3. Grape and Wine Production (Tons and Hectoliters)
    grape_t as grape_tons_total,
    wine_white_hl as white_wine_total_hl,
    wine_red_hl as red_wine_total_hl,
    wine_rose_hl as rose_wine_total_hl,
    wine_total_hl,

    -- 4. Pricing and Market Values (Currency: EUR)
    price_grape_red as price_grape_red_eur_kg,
    price_grape_white as price_grape_white_eur_kg,
    price_wine_red as price_wine_red_eur_hl,
    price_wine_rose as price_wine_rose_eur_hl,
    price_wine_white as price_wine_white_eur_hl,
    mkt_value_k as market_value_k_eur,

    -- 5. Quality and Scoring
    -- Converting descriptive quality_name into a numeric vintage_score
    case 
        when lower(quality_name) = 'excellent' then 5
        when lower(quality_name) = 'very good' then 4
        when lower(quality_name) = 'good' then 3
        else null -- Handle missing or different labels
    end as vintage_score_numeric,
    
    quality_name,
    parker_score,

    -- NEW COLUMN: Categorization based on the official Parker Scale rules
    case 
        when parker_score >= 96 then 'Extraordinary'
        when parker_score >= 90 then 'Outstanding'
        when parker_score >= 80 then 'Above Average to Excellent'
        when parker_score >= 70 then 'Average'
        when parker_score >= 60 then 'Below Average'
        else 'Appalling'
    end as parker_category,

    -- 6. Milestones and Specific Dates
    date_start AS haverst_date_start,
    date_peak AS haverst_date_peak,
    climate_milestone

from source