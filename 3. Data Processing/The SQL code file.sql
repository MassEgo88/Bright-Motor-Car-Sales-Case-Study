SELECT * FROM `car-sales-494311.Load_data.Care Sales` LIMIT 1000;
------------

SELECT * FROM `car-sales-494311.Load_data.Care Sales` LIMIT 600000;

-----------------------------------------------------------------------------
---1. Data Cleaning (Filtering Invalid Records)
---This query removes invalid or incomplete records to ensure only clean data is used for analysis.
-------------------------------------------------------------------------

SELECT *
FROM `car-sales-494311.Load_data.Care Sales`
WHERE
  sellingprice > 0
  AND odometer > 0
  AND saledate IS NOT NULL
  AND year IS NOT NULL
  AND make IS NOT NULL
  AND model IS NOT NULL;

--------------------------------------------------------------------------
----2. Deduplication (Latest Vehicle Record per VIN)
--- Keeps only the latest record per vehicle using ROW_NUMBER window function.
--------------------------------------------------------------------------

SELECT *
FROM
  (
    SELECT
      *, ROW_NUMBER() OVER (PARTITION BY vin ORDER BY saledate DESC) AS row_num
    FROM `car-sales-494311.Load_data.Care Sales`
    WHERE
      sellingprice > 0
      AND odometer > 0
      AND saledate IS NOT NULL
      AND year IS NOT NULL
      AND make IS NOT NULL
      AND model IS NOT NULL
      AND condition IS NOT NULL
      AND transmission IS NOT NULL
      AND vin IS NOT NULL
  )
WHERE row_num = 1;

-------------------------------------------------------------------------------
----3. Total Dataset Size
--- --- ---- --- Counts total number of cars in dataset.
--------------------------------------------------------------------------------
SELECT 
    COUNT(*) AS total_cars
 FROM `car-sales-494311.Load_data.Care Sales`;

------------------------------------------------------------------------------------
----4. Vehicle Segment Analysis (Make/Model/Body)
----Analyzes most sold vehicle types, revenue and pricing trends.
-------------------------------------------------------------------------------------

SELECT
  make,
  model,
  body,
  COUNT(*) AS car_count,
  AVG(sellingprice) AS avg_price,
  SUM(sellingprice) AS total_revenue
FROM `car-sales-494311.Load_data.Care Sales`
GROUP BY make, model, body
ORDER BY car_count DESC;

-------------------------------------------------------------------------
---5. Transmission Distribution
---Shows distribution of transmission types.
--------------------------------------------------------------------------

SELECT transmission, COUNT(*) AS car_count
FROM `car-sales-494311.Load_data.Care Sales`
WHERE transmission IS NOT NULL
GROUP BY transmission
ORDER BY car_count DESC;

------------------------------------------------------------------------------------
---6. Dealership Performance Analysis
---Ranks dealerships by sales volume, revenue and pricing.
------------------------------------------------------------------------------------

SELECT
  seller AS dealership_name,
  state,
  COUNT(*) AS cars_sold,
  AVG(sellingprice) AS avg_price,
  SUM(sellingprice) AS total_revenue
FROM `car-sales-494311.Load_data.Care Sales`
GROUP BY seller, state
ORDER BY cars_sold DESC;

---------------------------------------------------------------------------
---7. State Standardization (Example CASE statement)
---Converts state codes into full readable state names.
----------------------------------------------------------------------------------

SELECT
  CASE
    WHEN LOWER(state) = 'ca' THEN 'California'
    WHEN LOWER(state) = 'tx' THEN 'Texas'
    WHEN LOWER(state) = 'ny' THEN 'New York'
    ELSE state
    END
    AS state_full
FROM `car-sales-494311.Load_data.Care Sales`;

---------------------------------------------------------------------------
---8. Profitability Framework (P&L Model)
---Calculates revenue, cost, profit and margins.
---------------------------------------------------------------------------

SELECT
  SUM(sellingprice) AS sales,
  SUM(mmr) AS cost_of_sales,
  SUM(sellingprice - mmr) AS gross_profit,
  COUNT(*) AS units_sold,
  AVG(sellingprice) AS avg_price,
  (SUM(sellingprice - mmr) / SUM(sellingprice)) * 100 AS profit_margin
FROM `car-sales-494311.Load_data.Care Sales`
WHERE sellingprice > 0 AND mmr > 0;

--------------------------------------------------------------------------------
---9. Car Age Profitability Analysis
---Compares new vs old cars in terms of performance and profit.
----------------------------------------------------------------------------------
SELECT
  CASE
    WHEN year >= 2010 THEN 'New Cars (2010+)'
    ELSE 'Old Cars'
    END
    AS category,
  COUNT(*) AS units_sold,
  SUM(sellingprice) AS revenue,
  SUM(sellingprice - mmr) AS profit,
  AVG(sellingprice - mmr) AS avg_profit
FROM `car-sales-494311.Load_data.Care Sales`
GROUP BY category;

---------------------------------------------------------------------
---10. Market Share Analysis
---Calculates market share by make and model.
-----------------------------------------------------------------------

SELECT make, model, COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS market_share_pct
FROM `car-sales-494311.Load_data.Care Sales`
GROUP BY make, model
ORDER BY market_share_pct DESC;

---------------------------------------------------------------------
---11. Seasonality Analysis
---Tracks sales trends across time (months and seasons).
---------------------------------------------------------------------

SELECT
  EXTRACT(
    YEAR FROM SAFE.PARSE_TIMESTAMP('%a %b %d %Y', SUBSTR(saledate, 1, 15)))
    AS year,
  EXTRACT(
    MONTH FROM SAFE.PARSE_TIMESTAMP('%a %b %d %Y', SUBSTR(saledate, 1, 15)))
    AS month,
  COUNT(*) AS cars_sold
FROM `car-sales-494311.Load_data.Care Sales`
GROUP BY year, month
ORDER BY year, month;

---------------------------------------------------------------------------------
---12. Full Advanced Analytics Dataset (Final Model)
---Final enriched dataset combining cleaning, time intelligence, geography, profitability and segmentation.
-------------------------------------------------------------------------------------

SELECT
  year,
  make,
  model,
  state,
  seller,
  sellingprice,
  mmr,
  SAFE.PARSE_TIMESTAMP('%a %b %d %Y', SUBSTR(saledate, 1, 15)) AS sale_date,
  CASE WHEN year >= 2010 THEN 'New' ELSE 'Old' END AS car_age_category,
  sellingprice - mmr AS profit
FROM `car-sales-494311.Load_data.Care Sales`
WHERE sellingprice > 0 AND mmr > 0;

------------------------------------------------------------------------------
---13. Master Query
------------------------------------------------------------------------------

SELECT

  -- Base fields
  year,
  make,
  model,
  trim,
  body,
  transmission,
  vin,
  state,
  condition,
  odometer,
  color,
  interior,
  seller,
  mmr,
  sellingprice,
  saledate,

  -- Parsed date
  SAFE.PARSE_TIMESTAMP('%a %b %d %Y', SUBSTR(saledate, 1, 15))
    AS sale_date_parsed,

  -- Time dimensions
  EXTRACT(
    YEAR FROM SAFE.PARSE_TIMESTAMP('%a %b %d %Y', SUBSTR(saledate, 1, 15)))
    AS sale_year,
  EXTRACT(
    QUARTER FROM SAFE.PARSE_TIMESTAMP('%a %b %d %Y', SUBSTR(saledate, 1, 15)))
    AS sale_quarter,
  CASE
    WHEN
      EXTRACT(
        QUARTER
        FROM SAFE.PARSE_TIMESTAMP('%a %b %d %Y', SUBSTR(saledate, 1, 15)))
      <= 2
      THEN 1
    ELSE 2
    END
    AS sale_semester,
  CASE
    WHEN
      EXTRACT(
        QUARTER
        FROM SAFE.PARSE_TIMESTAMP('%a %b %d %Y', SUBSTR(saledate, 1, 15)))
      = 1
      THEN 'Winter'
    WHEN
      EXTRACT(
        QUARTER
        FROM SAFE.PARSE_TIMESTAMP('%a %b %d %Y', SUBSTR(saledate, 1, 15)))
      = 2
      THEN 'Spring'
    WHEN
      EXTRACT(
        QUARTER
        FROM SAFE.PARSE_TIMESTAMP('%a %b %d %Y', SUBSTR(saledate, 1, 15)))
      = 3
      THEN 'Summer'
    ELSE 'Fall'
    END
    AS sale_season,
  EXTRACT(
    MONTH FROM SAFE.PARSE_TIMESTAMP('%a %b %d %Y', SUBSTR(saledate, 1, 15)))
    AS sale_month,
  FORMAT_TIMESTAMP(
    '%B', SAFE.PARSE_TIMESTAMP('%a %b %d %Y', SUBSTR(saledate, 1, 15)))
    AS sale_month_name,
  EXTRACT(
    WEEK FROM SAFE.PARSE_TIMESTAMP('%a %b %d %Y', SUBSTR(saledate, 1, 15)))
    AS sale_week,
  EXTRACT(
    DAYOFWEEK FROM SAFE.PARSE_TIMESTAMP('%a %b %d %Y', SUBSTR(saledate, 1, 15)))
    AS sale_day_of_week,
  CASE
    WHEN
      EXTRACT(
        DAYOFWEEK
        FROM SAFE.PARSE_TIMESTAMP('%a %b %d %Y', SUBSTR(saledate, 1, 15)))
      IN (1, 7)
      THEN 'Weekend'
    ELSE 'Weekday'
    END
    AS sale_day_type,
  TIMESTAMP_TRUNC(
    SAFE.PARSE_TIMESTAMP('%a %b %d %Y', SUBSTR(saledate, 1, 15)), MONTH)
    AS sale_month_start,
  TIMESTAMP_TRUNC(
    SAFE.PARSE_TIMESTAMP('%a %b %d %Y', SUBSTR(saledate, 1, 15)), WEEK)
    AS sale_week_start,

  -- State full name
  CASE
    WHEN LOWER(state) = 'al' THEN 'Alabama'
    WHEN LOWER(state) = 'ak' THEN 'Alaska'
    WHEN LOWER(state) = 'az' THEN 'Arizona'
    WHEN LOWER(state) = 'ar' THEN 'Arkansas'
    WHEN LOWER(state) = 'ca' THEN 'California'
    WHEN LOWER(state) = 'co' THEN 'Colorado'
    WHEN LOWER(state) = 'ct' THEN 'Connecticut'
    WHEN LOWER(state) = 'de' THEN 'Delaware'
    WHEN LOWER(state) = 'fl' THEN 'Florida'
    WHEN LOWER(state) = 'ga' THEN 'Georgia'
    WHEN LOWER(state) = 'hi' THEN 'Hawaii'
    WHEN LOWER(state) = 'id' THEN 'Idaho'
    WHEN LOWER(state) = 'il' THEN 'Illinois'
    WHEN LOWER(state) = 'in' THEN 'Indiana'
    WHEN LOWER(state) = 'ia' THEN 'Iowa'
    WHEN LOWER(state) = 'ks' THEN 'Kansas'
    WHEN LOWER(state) = 'ky' THEN 'Kentucky'
    WHEN LOWER(state) = 'la' THEN 'Louisiana'
    WHEN LOWER(state) = 'me' THEN 'Maine'
    WHEN LOWER(state) = 'md' THEN 'Maryland'
    WHEN LOWER(state) = 'ma' THEN 'Massachusetts'
    WHEN LOWER(state) = 'mi' THEN 'Michigan'
    WHEN LOWER(state) = 'mn' THEN 'Minnesota'
    WHEN LOWER(state) = 'ms' THEN 'Mississippi'
    WHEN LOWER(state) = 'mo' THEN 'Missouri'
    WHEN LOWER(state) = 'mt' THEN 'Montana'
    WHEN LOWER(state) = 'ne' THEN 'Nebraska'
    WHEN LOWER(state) = 'nv' THEN 'Nevada'
    WHEN LOWER(state) = 'nh' THEN 'New Hampshire'
    WHEN LOWER(state) = 'nj' THEN 'New Jersey'
    WHEN LOWER(state) = 'nm' THEN 'New Mexico'
    WHEN LOWER(state) = 'ny' THEN 'New York'
    WHEN LOWER(state) = 'nc' THEN 'North Carolina'
    WHEN LOWER(state) = 'nd' THEN 'North Dakota'
    WHEN LOWER(state) = 'oh' THEN 'Ohio'
    WHEN LOWER(state) = 'ok' THEN 'Oklahoma'
    WHEN LOWER(state) = 'or' THEN 'Oregon'
    WHEN LOWER(state) = 'pa' THEN 'Pennsylvania'
    WHEN LOWER(state) = 'ri' THEN 'Rhode Island'
    WHEN LOWER(state) = 'sc' THEN 'South Carolina'
    WHEN LOWER(state) = 'sd' THEN 'South Dakota'
    WHEN LOWER(state) = 'tn' THEN 'Tennessee'
    WHEN LOWER(state) = 'tx' THEN 'Texas'
    WHEN LOWER(state) = 'ut' THEN 'Utah'
    WHEN LOWER(state) = 'vt' THEN 'Vermont'
    WHEN LOWER(state) = 'va' THEN 'Virginia'
    WHEN LOWER(state) = 'wa' THEN 'Washington'
    WHEN LOWER(state) = 'wv' THEN 'West Virginia'
    WHEN LOWER(state) = 'wi' THEN 'Wisconsin'
    WHEN LOWER(state) = 'wy' THEN 'Wyoming'
    ELSE state
    END
    AS state_full_name,

  -- Car age category

  CASE
    WHEN year >= 2010 THEN 'New Cars (2010+)'
    ELSE 'Old Cars (Pre-2010)'
    END
    AS car_age_category,

  -- Profit calculations
  sellingprice - mmr AS profit,
  CASE
    WHEN sellingprice > 0 THEN ((sellingprice - mmr) / sellingprice) * 100
    ELSE 0
    END
    AS profit_margin_pct,

  -- Currency formatted fields
  CONCAT('$', FORMAT("%'d", CAST(sellingprice AS INT64)))
    AS selling_price_formatted,
  CONCAT('$', FORMAT("%'d", CAST(mmr AS INT64))) AS mmr_formatted,
  CONCAT('$', FORMAT("%'d", CAST(sellingprice - mmr AS INT64)))
    AS profit_formatted
FROM
  (
    SELECT
      *, ROW_NUMBER() OVER (PARTITION BY vin ORDER BY saledate DESC) AS row_num
    FROM `car-sales-494311.Load_data.Care Sales`
    WHERE

      -- Data quality filters

      year IS NOT NULL
      AND make IS NOT NULL
      AND model IS NOT NULL
      AND vin IS NOT NULL
      AND state IS NOT NULL
      AND condition IS NOT NULL
      AND transmission IS NOT NULL
      AND saledate IS NOT NULL
      AND sellingprice > 0
      AND mmr > 0
      AND odometer > 0

      -- Range validation

      AND year BETWEEN 1900 AND 2026
      AND condition BETWEEN 1 AND 50
  )
WHERE row_num = 1;
