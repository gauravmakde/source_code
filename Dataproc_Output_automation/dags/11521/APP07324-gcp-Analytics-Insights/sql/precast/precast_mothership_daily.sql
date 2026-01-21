

/*
Table Name: T2DL_DAS_FORECASTING_SOLUTIONS.precast_daily
Owner: Selina Song, Sierra Broussard 
Date Modified: 06/01/2023

Notes: 
-- PreCast is a collection of highly aggregated KPI time series. It is formatted in a way to allow flexibility 
in the number and type of KPIs while also providing a structure for automated forecasting.
-- Update cadence: daily
-- Lookback window: 60 days, to match up with mothership

1. Incremental functionality available in ISF - features are updated on schedule. DELETE FROM criteria to be set for all available features.
2. New feature integration - Backfill/History dag specifically created to load historical data for new feature. SQL script would persist in repo. DAG would be created AS needed.
3. When onboarding new features in the future, feature_id will start from current max(feature_id) + 1, and then use dense_rank()
to generate. 
4. The file name is different from the table name for distinction purpose. This one is purely from mothership, and we will have other scripts to pull other metrics but to the same table.
*/ 


-- demand and sales
CREATE TEMPORARY TABLE IF NOT EXISTS demand_sales AS (
WITH temp_fsdf1 AS
(SELECT tran_date AS day_date,
        business_unit_desc_bopus_in_digital_demand,
        'total' AS dimension_type,
        'TOTAL' AS feature_dimension,
        SUM(demand_amt_bopus_in_digital) AS demand_digital_bopus_amt, -- attribute bopus to digital
        SUM(demand_canceled_amt) AS cancels_amt,
        SUM(demand_units_bopus_in_digital) AS demand_digital_bopus_unit,
        SUM(demand_canceled_units) AS cancels_units,
        SUM(gross_merch_sales_amt) AS gross_merch_amt,
        SUM(merch_returns_amt) AS returns_amt,
        SUM(net_merch_sales_amt) AS net_merch_amt,
        SUM(gross_operational_sales_amt) AS gross_op_amt,
        SUM(net_operational_sales_amt) AS net_op_amt,
        SUM(gross_merch_sales_units) AS gross_merch_unit,
        SUM(merch_returns_units) AS returns_unit,
        SUM(net_merch_sales_units) AS net_merch_unit,
        SUM(gross_operational_sales_units) AS gross_op_unit,
        SUM(net_operational_sales_units) AS net_op_unit
FROM `{{params.gcp_project_id}}`.t2dl_das_mothership.finance_sales_demand_fact 
WHERE tran_date BETWEEN {{params.start_date}} AND {{params.end_date}} 
    AND LOWER(business_unit_desc_bopus_in_digital_demand) IN (LOWER('N.COM'),LOWER('OFFPRICE ONLINE'),LOWER('FULL LINE'),LOWER('RACK')) --US only
GROUP BY day_date,business_unit_desc_bopus_in_digital_demand,dimension_type,feature_dimension
UNION DISTINCT
SELECT tran_date AS day_date,
        business_unit_desc_bopus_in_digital_demand,
        'merch_division' AS dimension_type,
        merch_division AS feature_dimension,
        SUM(demand_amt_bopus_in_digital) AS demand_digital_bopus_amt,
        SUM(demand_canceled_amt) AS cancels_amt,
        SUM(demand_units_bopus_in_digital) AS demand_digital_bopus_unit,
        SUM(demand_canceled_units) cancels_units,
        SUM(gross_merch_sales_amt) AS gross_merch_amt,
        SUM(merch_returns_amt) AS returns_amt,
        SUM(net_merch_sales_amt) AS net_merch_amt,
        SUM(gross_operational_sales_amt) AS gross_op_amt,
        SUM(net_operational_sales_amt) AS net_op_amt,
        SUM(gross_merch_sales_units) AS gross_merch_unit,
        SUM(merch_returns_units) AS returns_unit,
        SUM(net_merch_sales_units) AS net_merch_unit,
        SUM(gross_operational_sales_units) AS gross_op_unit,
        SUM(net_operational_sales_units) AS net_op_unit
FROM `{{params.gcp_project_id}}`.t2dl_das_mothership.finance_sales_demand_fact 
WHERE tran_date BETWEEN {{params.start_date}} AND {{params.end_date}}
    AND LOWER(business_unit_desc_bopus_in_digital_demand) IN (LOWER('N.COM'),LOWER('OFFPRICE ONLINE'),LOWER('FULL LINE'),LOWER('RACK'))
    AND LOWER(merch_division) IN (LOWER('ACCESSORIES'),LOWER('APPAREL'),LOWER('BEAUTY'),LOWER('DESIGNER'),LOWER('HOME'),LOWER('MERCH PROJECTS'),LOWER('SHOES')) -- only keep main divisions
GROUP BY day_date,business_unit_desc_bopus_in_digital_demand,dimension_type,feature_dimension
),
temp_fsdf2 AS (
SELECT
    day_date,
    business_unit_desc_bopus_in_digital_demand AS business_unit_desc,
        dimension_type,
        feature_dimension,
        SUM(demand_digital_bopus_amt) AS demand_digital_bopus_amt,
        SUM(cancels_amt) AS cancels_amt,
        SUM(demand_digital_bopus_unit) AS demand_digital_bopus_unit,
        SUM(cancels_units) AS cancels_units,
        SUM(gross_merch_amt) AS gross_merch_amt,
        SUM(returns_amt) AS returns_amt,
        SUM(net_merch_amt) AS net_merch_amt,
        SUM(gross_op_amt) AS gross_op_amt,
        SUM(net_op_amt) AS net_op_amt,
        SUM(gross_merch_unit) AS gross_merch_unit,
        SUM(returns_unit) AS returns_unit,
        SUM(net_merch_unit) AS net_merch_unit,
        SUM(gross_op_unit) AS gross_op_unit,
        SUM(net_op_unit) AS net_op_unit
FROM temp_fsdf1
    WHERE feature_dimension IS NOT NULL
GROUP BY day_date,business_unit_desc,dimension_type,feature_dimension
    )
SELECT *
FROM temp_fsdf2 UNPIVOT(feature_value FOR feature_name IN (
        demand_digital_bopus_amt,
        cancels_amt,
        demand_digital_bopus_unit,
        cancels_units,
        gross_merch_amt,
        returns_amt,
        net_merch_amt,
        gross_op_amt,
        net_op_amt,
        gross_merch_unit,
        returns_unit,
        net_merch_unit,
        gross_op_unit,
        net_op_unit
)) AS temp_fsdf_pivot)
;

-- orders and transaction
CREATE TEMPORARY TABLE IF NOT EXISTS orders_trans AS (
WITH temp_sdotf1 AS
(SELECT tran_date AS day_date,
        business_unit_desc,
        'total' AS dimension_type,
        'TOTAL' AS feature_dimension,
        SUM(orders) AS orders,
        SUM(visitors) AS udv,
        SUM(viewing_visitors) AS viewing_udv,
        SUM(adding_visitors) AS adding_udv,
        SUM(ordering_visitors) AS ordering_udv,
        SUM(store_purchase_trips) AS purchase_trips,
        SUM(store_traffic) AS store_traffic,
        SUM(transactions_total) AS transactions
FROM `{{params.gcp_project_id}}`.t2dl_das_mothership.sales_demand_orders_traffic_fact
WHERE tran_date BETWEEN {{params.start_date}} AND {{params.end_date}}
    AND LOWER(business_unit_desc) IN (LOWER('N.COM'),LOWER('OFFPRICE ONLINE'),LOWER('FULL LINE'),LOWER('RACK'))
GROUP BY day_date,business_unit_desc,dimension_type,feature_dimension
),
temp_sdotf2 AS (
    SELECT
    day_date,
    business_unit_desc,
        dimension_type,
        feature_dimension,
        SUM(orders) AS orders,
        SUM(udv) AS udv,
        SUM(viewing_udv) AS viewing_udv,
        SUM(adding_udv) AS adding_udv,
        SUM(ordering_udv) AS ordering_udv,
        SUM(purchase_trips) AS purchase_trips,
        SUM(store_traffic) AS store_traffic,
        SUM(transactions) AS transactions
FROM temp_sdotf1
    WHERE feature_dimension IS NOT NULL
GROUP BY day_date,business_unit_desc,dimension_type,feature_dimension
    )

SELECT *
FROM temp_sdotf2 UNPIVOT(feature_value FOR feature_name IN (
        orders,
        udv,
        viewing_udv,
        adding_udv,
        ordering_udv,
        purchase_trips,
        store_traffic,
        transactions
)) AS temp_fsdf_pivot)
;

-- delete any overlapping records from destination table prior to INSERT of new data
DELETE FROM `{{params.gcp_project_id}}`.{{params.forecasting_solutions_t2_schema}}.precast_daily
WHERE day_date BETWEEN {{params.start_date}} AND {{params.end_date}};


INSERT INTO `{{params.gcp_project_id}}`.{{params.forecasting_solutions_t2_schema}}.precast_daily
WITH temp_final AS (
    SELECT day_date,
            business_unit_desc,
            dimension_type,
            feature_dimension,
            feature_name,
            feature_value
    FROM demand_sales
    UNION ALL
    SELECT day_date,
            business_unit_desc,
            dimension_type,
            feature_dimension,
            feature_name,
            feature_value
    FROM orders_trans
)
SELECT 
    day_date,
    business_unit_desc,
    dimension_type,
    feature_dimension,
    feature_name,
    feature_value,
    DENSE_RANK() OVER(ORDER BY business_unit_desc, feature_name, dimension_type, feature_dimension) AS feature_id,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM temp_final;


