
DROP TABLE IF EXISTS default.aggregated_sale;

CREATE TABLE IF NOT EXISTS default.aggregated_sales (
    rms_sku_id STRING,
    channel_brand STRING,
    total_sales_last_two_months INT,
    total_sales_last_week INT,
    total_sales_week_to_date INT,
    average_sales_last_seven_days DECIMAL(10, 2),
    location_id STRING
)
STORED as PARQUET
PARTITIONED BY ( location_id )
LOCATION 's3://{s3_bucket_sip}/sales-aggregated/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

MSCK REPAIR TABLE default.aggregated_sales;

CREATE or REPLACE temporary view sales_agg_temp as (
SELECT
    rms_sku_num as rms_sku_id,
    CASE
        WHEN intent_store_num = '808' THEN '584'
        ELSE intent_store_num
        END as location_id,
    channel_brand,
    SUM(CASE
            WHEN CAST(demand_date AS DATE) >= CAST(DATE_ADD(CURRENT_DATE(), -61) AS DATE)
            THEN CAST(line_item_quantity AS INT)
            ELSE 0
        END) AS total_sales_last_two_months,
    SUM(CASE
            WHEN CAST(demand_date AS DATE) >= DATE_TRUNC('WEEK', CURRENT_DATE())
            THEN CAST(line_item_quantity AS INT)
            ELSE 0
        END) AS total_sales_week_to_date,
    SUM(CASE
            WHEN CAST(demand_date AS DATE)
                BETWEEN     DATE_ADD( CURRENT_DATE(), -7 - (EXTRACT(DAYOFWEEK FROM CURRENT_DATE()) - 1)) AND
                            DATE_ADD( CURRENT_DATE(), -1 - (EXTRACT(DAYOFWEEK FROM CURRENT_DATE()) - 1))
            THEN CAST(line_item_quantity AS INT)
            ELSE 0
        END) AS total_sales_last_week,
    CAST(SUM(CASE
            WHEN CAST(demand_date AS DATE) >= CAST(DATE_ADD(CURRENT_DATE(), -8) AS DATE)
            THEN CAST(line_item_quantity AS INT)
            ELSE 0
        END)/7 AS DECIMAL(10, 2)) AS average_sales_last_seven_days
FROM
    default.jwn_demand_lifecycle_fact
WHERE (intent_store_num = '808' AND fulfilled_from_location = '584')
    OR (intent_store_num <> '808')
GROUP BY
    rms_sku_num,
    intent_store_num,
    channel_brand
    );


INSERT overwrite default.aggregated_sales partition (location_id)
SELECT  rms_sku_id, channel_brand, total_sales_last_two_months, total_sales_week_to_date,
        total_sales_last_week, average_sales_last_seven_days, location_id
FROM sales_agg_temp;


-- Add outliers to audit table: 
INSERT into table sales_aggregation_audit partition ( batch_date )
SELECT rms_sku_id, location_id, channel_brand, current_date() as batch_date
FROM sales_agg_temp
WHERE   (total_sales_last_two_months < -100 OR total_sales_last_two_months > 300000) OR
        (total_sales_week_to_date < -100 OR total_sales_week_to_date > 2000) OR
        (total_sales_last_week < -100 OR total_sales_last_week > 20000) OR 
        (average_sales_last_seven_days < -100 OR average_sales_last_seven_days > 20000); 
