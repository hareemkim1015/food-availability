CREATE TABLE IF NOT EXISTS crop_supply AS 
    SELECT * FROM read_parquet('$LOADPATH');

-- view of 'wheat'
CREATE OR REPLACE VIEW wheat_view AS (
    SELECT * 
    FROM crop_supply
    WHERE ingredient = 'wheat'
);

-- view of 'past year'
CREATE OR REPLACE VIEW past_year_view AS (
    SELECT *
    FROM crop_supply
    WHERE date >= CURRENT_DATE - INTERVAL 1 YEAR
);

-- crop supply trend by crop, status, and date
CREATE OR REPLACE VIEW supply_trend_by_time AS (
    SELECT 
        ingredient, 
        status,
        EXTRACT(YEAR FROM date) AS year,
        EXTRACT(MONTH FROM date) AS month, 
        AVG(value), 
        SUM(value) 
    FROM crop_supply
    GROUP BY ROLLUP (ingredient, status, year, month)
);

-- ingredient availability for current year
CREATE OR REPLACE VIEW ingredient_avail_this_year AS (
    SELECT 
        brand,
        id, 
        ingredient, 
        status,
        EXTRACT(YEAR FROM date) AS year,
        EXTRACT(MONTH FROM date) AS month,  
        SUM(value) 
    FROM crop_supply
    WHERE EXTRACT(YEAR FROM date) >= 2024
    GROUP BY ROLLUP (brand, id, ingredient, status, year, month)
);

-- pivot from long to wide format (by status) 
CREATE OR REPLACE VIEW crop_supply_status_pivot AS (
    SELECT
        brand,
        id,
        ingredient,
        date,
        "area planted_sum(""value"")" AS area_planted_value,
        "area planted_""first""(unit)" AS area_planted_unit,
        "area harvested_sum(""value"")" AS area_harvested_value,
        "area harvested_""first""(unit)" AS area_harvested_unit,
        "yield_sum(""value"")" AS yield_value,
        "yield_""first""(unit)" AS yield_unit,
        "production_sum(""value"")" AS production_value,
        "production_""first""(unit)" AS production_unit,
        "sales_sum(""value"")" AS sales_value,
        "sales_""first""(unit)" AS sales_unit
    FROM
        crop_supply
    PIVOT (
        SUM(value), FIRST(unit)
        FOR status IN ('area harvested', 'yield', 'production', 'sales', 'area planted'))
);

-- pivot from long to wide format (by year) 
CREATE OR REPLACE VIEW crop_supply_year_pivot AS
SELECT * EXCLUDE (status_order)
FROM
    crop_supply
PIVOT (
    SUM(value)
    FOR EXTRACT(YEAR FROM date) IN (2020, 2021, 2022, 2023, 2024, 2025)
);
