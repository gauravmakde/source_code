
BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP08176;
DAG_ID=mothership_clarity_finance_sales_census_fact_11521_ACE_ENG;
---     Task_Name=clarity_finance_sales_census_fact;'*/
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS jogmv AS
SELECT jogmv.business_day_date,
 jogmv.demand_date,
 dc.year_num,
 jogmv.business_unit_desc,
 jogmv.intent_store_num AS store_num,
 jogmv.dept_num,
  CASE
  WHEN LOWER(jogmv.delivery_method) IN (LOWER('BOPUS'), LOWER('Store Take'), LOWER('Charge Send'))
  THEN jogmv.delivery_method
  WHEN LOWER(jogmv.order_platform_type) = LOWER('Direct to Customer (DTC)')
  THEN jogmv.order_platform_type
  WHEN LOWER(jogmv.delivery_method) LIKE LOWER('Ship %')
  THEN 'Shipped'
  ELSE 'Other'
  END AS delivery_method,
  CASE
  WHEN LOWER(jogmv.loyalty_status) IN (LOWER('UNKNOWN_VALUE'), LOWER('NOT_APPLICABLE'))
  THEN NULL
  ELSE jogmv.loyalty_status
  END AS loyalty_status,
 jogmv.acp_id,
 jogmv.order_num,
 jogmv.jwn_fulfilled_demand_ind,
 jogmv.jwn_operational_gmv_ind,
 jogmv.jwn_fulfilled_demand_usd_amt,
 jogmv.line_item_quantity,
 jogmv.operational_gmv_usd_amt,
 jogmv.operational_gmv_units
FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_operational_gmv_metric_vw AS jogmv
 INNER JOIN  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal AS dc ON jogmv.business_day_date = dc.day_date
WHERE jogmv.business_day_date BETWEEN {{params.start_date}} AND  {{params.end_date}}
 AND jogmv.business_day_date BETWEEN  '2021-01-31' AND (DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY))
 AND LOWER(jogmv.zero_value_unit_ind) = LOWER('N')
 AND (LOWER(jogmv.business_unit_country) <> LOWER('CA') OR jogmv.business_day_date <=  '2023-02-25');


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS jogmv_order_not_null
-- CLUSTER BY order_num, demand_date
AS
SELECT business_day_date,
 demand_date,
 business_unit_desc,
 store_num,
 dept_num,
 delivery_method,
 loyalty_status,
 order_num,
 jwn_fulfilled_demand_ind,
 jwn_operational_gmv_ind,
 jwn_fulfilled_demand_usd_amt,
 line_item_quantity,
 operational_gmv_usd_amt,
 operational_gmv_units
FROM jogmv
WHERE order_num IS NOT NULL;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;


CREATE TEMPORARY TABLE IF NOT EXISTS jogmv_order_null AS
SELECT business_day_date,
 year_num,
 business_unit_desc,
 store_num,
 dept_num,
 delivery_method,
 loyalty_status,
 acp_id,
 jwn_fulfilled_demand_ind,
 jwn_operational_gmv_ind,
 jwn_fulfilled_demand_usd_amt,
 line_item_quantity,
 operational_gmv_usd_amt,
 operational_gmv_units
FROM jogmv
WHERE order_num IS NULL;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;


CREATE TEMPORARY TABLE IF NOT EXISTS jogmv_order_null_acp_null AS
SELECT business_day_date,
 business_unit_desc,
 store_num,
 dept_num,
 delivery_method,
 loyalty_status,
 acp_id,
 jwn_fulfilled_demand_ind,
 jwn_operational_gmv_ind,
 jwn_fulfilled_demand_usd_amt,
 line_item_quantity,
 operational_gmv_usd_amt,
 operational_gmv_units
FROM jogmv_order_null
WHERE acp_id IS NULL;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;


CREATE TEMPORARY TABLE IF NOT EXISTS jogmv_order_null_acp_not_null
-- CLUSTER BY year_num, acp_id
AS
SELECT *
FROM jogmv_order_null
WHERE acp_id IS NOT NULL;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (year_num, acp_id) ON jogmv_order_null_acp_not_null;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS jogmv_order_null_acp_not_null_census1 AS
SELECT jogmv.business_day_date,
 jogmv.business_unit_desc,
 jogmv.store_num,
 jogmv.dept_num,
 jogmv.delivery_method,
 jogmv.loyalty_status,
 jogmv.acp_id,
 census.state_fips_code,
 census.county_fips_code,
 census.tract_code,
 census.block_code,
 jogmv.jwn_fulfilled_demand_ind,
 jogmv.jwn_operational_gmv_ind,
 jogmv.jwn_fulfilled_demand_usd_amt,
 jogmv.line_item_quantity,
 jogmv.operational_gmv_usd_amt,
 jogmv.operational_gmv_units
FROM jogmv_order_null_acp_not_null AS jogmv
 LEFT JOIN t2dl_das_real_estate.acp_census AS census ON LOWER(census.acp_id) = LOWER(jogmv.acp_id) AND jogmv.year_num =
   census.year_num;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;


CREATE TEMPORARY TABLE IF NOT EXISTS jogmv_order_null_acp_not_null_census_not_null AS
SELECT business_day_date,
 business_unit_desc,
 store_num,
 dept_num,
 delivery_method,
 loyalty_status,
 state_fips_code,
 county_fips_code,
 tract_code,
 block_code,
 jwn_fulfilled_demand_ind,
 jwn_operational_gmv_ind,
 jwn_fulfilled_demand_usd_amt,
 line_item_quantity,
 operational_gmv_usd_amt,
 operational_gmv_units
FROM jogmv_order_null_acp_not_null_census1 AS jogmv
WHERE tract_code IS NOT NULL
 AND block_code IS NOT NULL;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;


CREATE TEMPORARY TABLE IF NOT EXISTS jogmv_order_null_acp_not_null_census_null
-- CLUSTER BY acp_id
AS
SELECT business_day_date,
 business_unit_desc,
 store_num,
 dept_num,
 delivery_method,
 loyalty_status,
 acp_id,
 jwn_fulfilled_demand_ind,
 jwn_operational_gmv_ind,
 jwn_fulfilled_demand_usd_amt,
 line_item_quantity,
 operational_gmv_usd_amt,
 operational_gmv_units
FROM jogmv_order_null_acp_not_null_census1 AS jogmv
WHERE tract_code IS NULL
 AND block_code IS NULL;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (acp_id) ON jogmv_order_null_acp_not_null_census_null;
BEGIN
SET _ERROR_CODE  =  0;


CREATE TEMPORARY TABLE IF NOT EXISTS census_agg
-- CLUSTER BY acp_id
AS
SELECT acp_id,
 state_fips_code,
 county_fips_code,
 tract_code,
 block_code
FROM t2dl_das_real_estate.acp_census
QUALIFY (ROW_NUMBER() OVER (PARTITION BY acp_id ORDER BY year_num DESC)) = 1;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (acp_id) ON census_agg;
BEGIN
SET _ERROR_CODE  =  0;


CREATE TEMPORARY TABLE IF NOT EXISTS jogmv_order_null_acp_not_null_census2 AS
SELECT jogmv.business_day_date,
 jogmv.business_unit_desc,
 jogmv.store_num,
 jogmv.dept_num,
 jogmv.delivery_method,
 jogmv.loyalty_status,
 census_agg.state_fips_code,
 census_agg.county_fips_code,
 census_agg.tract_code,
 census_agg.block_code,
 jogmv.jwn_fulfilled_demand_ind,
 jogmv.jwn_operational_gmv_ind,
 jogmv.jwn_fulfilled_demand_usd_amt,
 jogmv.line_item_quantity,
 jogmv.operational_gmv_usd_amt,
 jogmv.operational_gmv_units
FROM jogmv_order_null_acp_not_null_census_null AS jogmv
 LEFT JOIN census_agg ON LOWER(census_agg.acp_id) = LOWER(jogmv.acp_id);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;


CREATE TEMPORARY TABLE IF NOT EXISTS oldf
-- CLUSTER BY order_num, order_line_id
AS
SELECT DISTINCT order_num,
 order_line_id
FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.order_line_detail_fact AS oldf
WHERE (LOWER(promise_type_code) = LOWER('SHIP_TO_STORE') OR destination_node_num IS NOT NULL)
 AND order_date_pacific BETWEEN DATE_SUB(({{params.start_date}}), INTERVAL 180 DAY) AND ({{params.end_date}});


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (order_num, order_line_id) ON oldf;
BEGIN
SET _ERROR_CODE  =  0;


CREATE TEMPORARY TABLE IF NOT EXISTS census
-- CLUSTER BY order_num, order_line_id
AS
SELECT order_num,
 order_line_id,
 order_date_pacific,
 state_fips_code,
 county_fips_code,
 tract_code,
 block_code
FROM t2dl_das_customer.order_line_postal_address_geocoder_lkup_vw
WHERE order_date_pacific BETWEEN DATE_SUB({{params.start_date}}, INTERVAL 180 DAY) AND {{params.end_date}}
 AND LOWER(match_address) IN (LOWER('Exact'), LOWER('Non_Exact'))
 AND tract_code IS NOT NULL
UNION ALL
SELECT order_num,
 order_line_id,
 order_date_pacific,
 state_fips_code,
 county_fips_code,
 tract_code,
 block_code
FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.order_line_postal_address_geocoder_lkup
WHERE order_date_pacific BETWEEN DATE_SUB({{params.start_date}}, INTERVAL 180 DAY) AND {{params.end_date}}
 AND order_date_pacific >  '2024-08-27'
 AND LOWER(match_address) IN (LOWER('Exact'), LOWER('Non_Exact'))
 AND tract_code IS NOT NULL;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (order_num, order_line_id) ON census;
BEGIN
SET _ERROR_CODE  =  0;


CREATE TEMPORARY TABLE IF NOT EXISTS census2
-- CLUSTER BY order_num, order_date_pacific
AS
SELECT DISTINCT order_num,
 order_date_pacific,
 state_fips_code,
 county_fips_code,
 tract_code,
 block_code
FROM census
WHERE order_line_id NOT IN (SELECT order_line_id
   FROM oldf);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (order_num,order_date_pacific) ON census2;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS jogmv_order_not_null_census AS
SELECT jogmv.business_day_date,
 jogmv.business_unit_desc,
 jogmv.store_num,
 jogmv.dept_num,
 jogmv.delivery_method,
 jogmv.loyalty_status,
 census2.state_fips_code,
 census2.county_fips_code,
 census2.tract_code,
 census2.block_code,
 jogmv.jwn_fulfilled_demand_ind,
 jogmv.jwn_operational_gmv_ind,
 jogmv.jwn_fulfilled_demand_usd_amt,
 jogmv.line_item_quantity,
 jogmv.operational_gmv_usd_amt,
 jogmv.operational_gmv_units
FROM jogmv_order_not_null AS jogmv
 LEFT JOIN census2 ON LOWER(jogmv.order_num) = LOWER(census2.order_num) AND jogmv.demand_date = census2.order_date_pacific;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS combo AS
SELECT *
FROM jogmv_order_null_acp_not_null_census_not_null
UNION ALL
SELECT *
FROM jogmv_order_null_acp_not_null_census2
UNION ALL
SELECT *
FROM jogmv_order_not_null_census
UNION ALL
SELECT business_day_date,
 business_unit_desc,
 store_num,
 dept_num,
 delivery_method,
 loyalty_status,
 SUBSTR(NULL, 1, 10) AS state_fips_code,
 SUBSTR(NULL, 1, 10) AS county_fips_code,
 SUBSTR(NULL, 1, 10) AS tract_code,
 SUBSTR(NULL, 1, 10) AS block_code,
 jwn_fulfilled_demand_ind,
 jwn_operational_gmv_ind,
 jwn_fulfilled_demand_usd_amt,
 line_item_quantity,
 operational_gmv_usd_amt,
 operational_gmv_units
FROM jogmv_order_null_acp_null;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS combo_agg AS
SELECT business_day_date,
 business_unit_desc,
 store_num,
 dept_num,
 delivery_method,
 loyalty_status,
 state_fips_code,
 county_fips_code,
 tract_code,
 block_code,
 SUM(CASE
   WHEN LOWER(jwn_fulfilled_demand_ind) = LOWER('Y')
   THEN jwn_fulfilled_demand_usd_amt
   ELSE 0
   END) AS fulfilled_demand_usd_amt,
 SUM(CASE
   WHEN LOWER(jwn_fulfilled_demand_ind) = LOWER('Y')
   THEN line_item_quantity
   ELSE 0
   END) AS fulfilled_demand_units,
 SUM(CASE
   WHEN LOWER(jwn_operational_gmv_ind) = LOWER('Y')
   THEN operational_gmv_usd_amt
   ELSE 0
   END) AS op_gmv_usd_amt,
 SUM(CASE
   WHEN LOWER(jwn_operational_gmv_ind) = LOWER('Y')
   THEN operational_gmv_units
   ELSE 0
   END) AS op_gmv_units,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM combo
GROUP BY business_day_date,
 business_unit_desc,
 store_num,
 dept_num,
 delivery_method,
 loyalty_status,
 state_fips_code,
 county_fips_code,
 tract_code,
 block_code;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

DELETE FROM  `{{params.gcp_project_id}}`.{{params.clarity_schema}}.finance_sales_census_fact
WHERE business_day_date BETWEEN  {{params.start_date}} AND  {{params.end_date}};
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

INSERT INTO  `{{params.gcp_project_id}}`.{{params.clarity_schema}}.finance_sales_census_fact (row_id, business_day_date, business_unit_desc, store_num,
 dept_num, delivery_method, loyalty_status, state_fips_code, county_fips_code, tract_code, block_code,
 fulfilled_demand_usd_amt, fulfilled_demand_units, op_gmv_usd_amt, op_gmv_units, dw_sys_load_tmstp)
(SELECT COALESCE((SELECT MAX(row_id)
     FROM  `{{params.gcp_project_id}}`.{{params.clarity_schema}}.finance_sales_census_fact), 0) + (ROW_NUMBER() OVER ()) AS row_id,
  business_day_date,
  business_unit_desc,
  store_num,
  dept_num,
  delivery_method,
  loyalty_status,
  CAST(TRUNC(CAST(state_fips_code AS FLOAT64)) AS INTEGER) AS state_fips_code,
  CAST(TRUNC(CAST(county_fips_code AS FLOAT64)) AS INTEGER) AS county_fips_code,
  CAST(TRUNC(CAST(tract_code AS FLOAT64)) AS INTEGER) AS tract_code,
  CAST(TRUNC(CAST(block_code AS FLOAT64)) AS INTEGER) AS block_code,
  CAST(fulfilled_demand_usd_amt AS NUMERIC) AS fulfilled_demand_usd_amt,
  fulfilled_demand_units,
  CAST(op_gmv_usd_amt AS NUMERIC) AS op_gmv_usd_amt,
  op_gmv_units,
  dw_sys_load_tmstp
 FROM combo_agg);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
