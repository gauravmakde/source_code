-- SET QUERY_BAND = 'App_ID=APP08176;
--      DAG_ID=mothership_finance_sales_demand_orders_traffic_fact_11521_ACE_ENG;
--      Task_Name=sales_demand_orders_traffic_fact;'
--      FOR SESSION VOLATILE;
 
 /*
T2DL_DAS_MOTHERSHIP.SALES_DEMAND_ORDERS_TRAFFIC_FACT 
Description - This ddl creates a 7 box (n.com, r.com, n.ca, FULL LINE, FULL LINE CANADA, RACK, RACK CANADA) table of hourly sales, demand, orders, and traffic split by store number and platform (device, eg ios, web, mow)  
Full documenation: https://confluence.nordstrom.com/display/AS/FINANCE_SALES_DEMAND_FACT+and+SALES_DEMAND_ORDERS_TRAFFIC_FACT
Contacts: Matthew Bond, Analytics
*/

-- 3 dependencies- T2DL_DAS_MOTHERSHIP.FINANCE_SALES_DEMAND_FACT, T2DL_DAS_FLS_Traffic_Model.rack_traffic_daily, T2DL_DAS_FLS_Traffic_Model.fls_traffic_daily
--daily refresh schedule, needs to pull last 45 days to cover any changes to funnel data, which updates over last 40 days. 


/* step 1
get loyalty information into format to show changes in loyalty status over time
*/


CREATE TEMPORARY TABLE IF NOT EXISTS loy AS
SELECT DISTINCT acp_id,
 rewards_level,
 start_day_date,
 end_day_date,
 LEAD(DATE_SUB(start_day_date, INTERVAL 1 DAY), 1) OVER (PARTITION BY acp_id ORDER BY start_day_date) AS end_day_date_2
 ,
 COALESCE(LEAD(DATE_SUB(start_day_date, INTERVAL 1 DAY), 1) OVER (PARTITION BY acp_id ORDER BY start_day_date),
  end_day_date) AS end_day_date_lead
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.loyalty_level_lifecycle_fact_vw
WHERE end_day_date > DATE '2018-12-31'
 AND start_day_date < CURRENT_DATE('PST8PDT')
 AND start_day_date < end_day_date;



-- COLLECT STATISTICS COLUMN (acp_id) ON loy;
-- COLLECT STATISTICS COLUMN (acp_id, start_day_date, end_day_date) ON loy;
-- COLLECT STATISTICS COLUMN (start_day_date) ON loy;
-- COLLECT STATISTICS COLUMN (end_day_date_lead) ON loy;
-- COLLECT STATISTICS COLUMN (end_day_date) ON loy;



CREATE TEMPORARY TABLE IF NOT EXISTS pre_oldf

AS
SELECT order_num,
 source_platform_code,
 order_date_pacific,
 order_line_currency_code,
 source_store_num,
 ent_cust_id,
 shopper_id,
  CASE
  WHEN LOWER(source_channel_country_code) = LOWER('US') AND LOWER(source_channel_code) = LOWER('FULL_LINE')
  THEN 'N.COM'
  WHEN LOWER(source_channel_country_code) = LOWER('CA') AND LOWER(source_channel_code) = LOWER('FULL_LINE')
  THEN 'N.CA'
  WHEN LOWER(source_channel_country_code) = LOWER('US') AND LOWER(source_channel_code) = LOWER('RACK')
  THEN 'OFFPRICE ONLINE'
  ELSE NULL
  END AS business_unit_desc
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.order_line_detail_fact
WHERE order_date_pacific BETWEEN DATE_SUB({{params.start_date}}, INTERVAL 365 DAY) AND {{params.end_date}} 
 AND (LOWER(CASE
       WHEN LOWER(source_channel_country_code) = LOWER('US') AND LOWER(source_channel_code) = LOWER('FULL_LINE')
       THEN 'N.COM'
       WHEN LOWER(source_channel_country_code) = LOWER('CA') AND LOWER(source_channel_code) = LOWER('FULL_LINE')
       THEN 'N.CA'
       WHEN LOWER(source_channel_country_code) = LOWER('US') AND LOWER(source_channel_code) = LOWER('RACK')
       THEN 'OFFPRICE ONLINE'
       ELSE NULL
       END) IN (LOWER('N.COM'), LOWER('N.CA')) AND LOWER(COALESCE(fraud_cancel_ind, 'N')) = LOWER('N') OR LOWER(CASE
       WHEN LOWER(source_channel_country_code) = LOWER('US') AND LOWER(source_channel_code) = LOWER('FULL_LINE')
       THEN 'N.COM'
       WHEN LOWER(source_channel_country_code) = LOWER('CA') AND LOWER(source_channel_code) = LOWER('FULL_LINE')
       THEN 'N.CA'
       WHEN LOWER(source_channel_country_code) = LOWER('US') AND LOWER(source_channel_code) = LOWER('RACK')
       THEN 'OFFPRICE ONLINE'
       ELSE NULL
       END) = LOWER('OFFPRICE ONLINE') AND order_date_pacific >= DATE '2020-11-21')
 AND LOWER(COALESCE(beauty_sample_ind, 'N')) = LOWER('N')
 AND LOWER(COALESCE(gift_with_purchase_ind, 'N')) = LOWER('N')
 AND order_line_amount > 0
QUALIFY (ROW_NUMBER() OVER (PARTITION BY order_num ORDER BY source_platform_code)) = 1;



-- COLLECT STATISTICS COLUMN (ent_cust_id) ON pre_oldf ;
-- COLLECT STATISTICS COLUMN (order_num) ON pre_oldf;
-- COLLECT STATISTICS COLUMN (source_platform_code) ON pre_oldf;
-- COLLECT STATISTICS COLUMN (order_date_pacific) ON pre_oldf;



CREATE TEMPORARY TABLE IF NOT EXISTS oldf_not_null

AS
SELECT order_num,
 source_platform_code,
 order_date_pacific,
 order_line_currency_code,
 source_store_num,
 business_unit_desc,
 ent_cust_id
FROM pre_oldf
WHERE ent_cust_id IS NOT NULL;

-- COLLECT STATISTICS COLUMN (order_num) ON oldf_not_null;
-- COLLECT STATISTICS COLUMN (ent_cust_id) ON oldf_not_null;

CREATE TEMPORARY TABLE IF NOT EXISTS oldf_null

AS
SELECT order_num,
 source_platform_code,
 order_date_pacific,
 order_line_currency_code,
 source_store_num,
 business_unit_desc,
 ent_cust_id,
 shopper_id
FROM pre_oldf
WHERE ent_cust_id IS NULL;



-- COLLECT STATISTICS COLUMN (order_date_pacific) ON oldf_null;
-- COLLECT STATISTICS COLUMN (shopper_id) ON oldf_null;
-- COLLECT STATISTICS COLUMN (shopper_id,order_num) ON oldf_null;



CREATE TEMPORARY TABLE IF NOT EXISTS aacx

AS
SELECT acp_id,
 cust_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.acp_analytical_cust_xref AS t0
WHERE LOWER(cust_source) = LOWER('ICON')
 AND cust_id IN (SELECT DISTINCT ent_cust_id
   FROM pre_oldf)
QUALIFY (ROW_NUMBER() OVER (PARTITION BY cust_id ORDER BY dw_batch_date DESC)) = 1;


--COLLECT STATISTICS COLUMN (cust_id) ON aacx ;


/* step 11
add acp_id to oldf FROM aacx
*/

CREATE TEMPORARY TABLE IF NOT EXISTS oldf_not_null_acp

AS
SELECT oldf_not_null.order_num,
 oldf_not_null.source_platform_code,
 oldf_not_null.order_date_pacific,
 oldf_not_null.order_line_currency_code,
 oldf_not_null.source_store_num,
 oldf_not_null.business_unit_desc,
 aacx.acp_id AS aacx_acp_id
FROM oldf_not_null
 LEFT JOIN aacx ON LOWER(oldf_not_null.ent_cust_id) = LOWER(aacx.cust_id);



/* step 12
cut down aapx to get a single record per acp_id
*/

CREATE TEMPORARY TABLE IF NOT EXISTS aapx_acp

AS
SELECT DISTINCT program_index_id,
 acp_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.acp_analytical_program_xref AS aapx
WHERE LOWER(program_name) IN (LOWER('WEB'))
 AND acp_id IS NOT NULL
 AND program_index_id IS NOT NULL
QUALIFY (ROW_NUMBER() OVER (PARTITION BY acp_id ORDER BY program_index_id DESC)) = 1;


/* step 12
cut down aapx to get a single record per acp_id
*/

CREATE TEMPORARY TABLE IF NOT EXISTS oldf_null_acp

AS
SELECT oldf_null.order_num,
 oldf_null.source_platform_code,
 oldf_null.order_date_pacific,
 oldf_null.order_line_currency_code,
 oldf_null.source_store_num,
 oldf_null.business_unit_desc,
 aapx_acp.acp_id AS shopper_acp_id
FROM oldf_null
 LEFT JOIN aapx_acp ON LOWER(oldf_null.shopper_id) = LOWER(aapx_acp.program_index_id) AND oldf_null.order_date_pacific <
   DATE '2020-02-12';



-- COLLECT STATISTICS COLUMN (order_num) ON oldf_null_acp;


/* step 14
combine oldf_null_acp AND oldf_not_null_acp
*/

CREATE TEMPORARY TABLE IF NOT EXISTS oldf_acp

AS
SELECT order_num,
 source_platform_code,
 order_date_pacific,
 order_line_currency_code,
 source_store_num,
 business_unit_desc,
 shopper_acp_id AS oldf_acp_id
FROM oldf_null_acp
UNION ALL
SELECT order_num,
 source_platform_code,
 order_date_pacific,
 order_line_currency_code,
 source_store_num,
 business_unit_desc,
 aacx_acp_id AS oldf_acp_id
FROM oldf_not_null_acp;



-- COLLECT STATISTICS COLUMN (oldf_acp_id) ON oldf_acp;
-- COLLECT STATISTICS COLUMN (order_num) ON oldf_acp;


/* step 15
get only oldf_acp WHERE acp_id is not null to make join to customer variables easier (no NULLs IN index)
*/

CREATE TEMPORARY TABLE IF NOT EXISTS oldf_acp_not_null

AS
SELECT order_num,
 source_platform_code,
 order_date_pacific,
 order_line_currency_code,
 source_store_num,
 business_unit_desc,
 oldf_acp_id AS acp_id
FROM oldf_acp
WHERE oldf_acp_id IS NOT NULL;


-- COLLECT STATISTICS COLUMN (acp_id) ON oldf_acp_not_null;
-- COLLECT STATISTICS COLUMN (order_date_pacific) ON oldf_acp_not_null;

/* step 16                  
get only oldf_acp WHERE acp_id is null for join to rtdf to get acp_id
*/

CREATE TEMPORARY TABLE IF NOT EXISTS oldf_acp_null

AS
SELECT order_num,
 source_platform_code,
 order_date_pacific,
 order_line_currency_code,
 source_store_num,
 business_unit_desc
FROM oldf_acp
WHERE oldf_acp_id IS NULL;



-- COLLECT STATISTICS COLUMN (order_num) ON oldf_acp_null;


/* step 17
add customer data to oldf using acp_id
*/

CREATE TEMPORARY TABLE IF NOT EXISTS oldf_cust

AS
SELECT oldf.order_num,
 oldf.source_platform_code,
 oldf.order_date_pacific,
 oldf.order_line_currency_code,
 oldf.source_store_num,
 oldf.business_unit_desc,
 loy.rewards_level
FROM oldf_acp_not_null AS oldf
 LEFT JOIN loy ON LOWER(loy.acp_id) = LOWER(oldf.acp_id) AND oldf.order_date_pacific <= loy.end_day_date_lead AND oldf.order_date_pacific
    >= loy.start_day_date;



-- COLLECT STATISTICS COLUMN (order_num) ON oldf_cust;

 
/* step 18
combine oldf_acp_null and oldf_cust
*/

CREATE TEMPORARY TABLE IF NOT EXISTS oldf_cust_all

AS
SELECT order_num, source_platform_code, order_date_pacific, order_line_currency_code, source_store_num, business_unit_desc--, cts_segment, rfm_segment
    , rewards_level
FROM oldf_cust
UNION ALL
SELECT order_num,
 source_platform_code,
 order_date_pacific,
 order_line_currency_code,
 source_store_num,
 business_unit_desc,
 SUBSTR(NULL, 1, 40) AS rewards_level
FROM oldf_acp_null;

-- COLLECT STATISTICS COLUMN (order_num) ON oldf_cust_all;
-- COLLECT STATISTICS COLUMN (order_date_pacific) ON oldf_cust_all;
-- COLLECT STATISTICS COLUMN (source_platform_code) ON oldf_cust_all;
    


DELETE FROM `{{params.gcp_project_id}}`.{{params.mothership_t2_schema}}.sales_demand_orders_traffic_fact
WHERE tran_date BETWEEN {{params.start_date}} AND {{params.end_date}};



/* step 32
final insert statement
*/

INSERT INTO `{{params.gcp_project_id}}`.{{params.mothership_t2_schema}}.sales_demand_orders_traffic_fact
(
tran_date,
business_unit_desc,
data_source,
currency_code,
store_num,
cts_segment,
rfm_segment,
rewards_level,
platform,
demand_amt_excl_bopus,
bopus_attr_store_amt,
bopus_attr_digital_amt,
demand_canceled_amt,
demand_units_excl_bopus,
bopus_attr_store_units,
bopus_attr_digital_units,
demand_canceled_units,
gross_merch_sales_amt,
merch_returns_amt,
net_merch_sales_amt,
gross_operational_sales_amt,
operational_returns_amt,
net_operational_sales_amt,
gross_merch_sales_units,
merch_returns_units,
net_merch_sales_units,
gross_operational_sales_units,
operational_returns_units,
net_operational_sales_units,
orders,
visitors,
viewing_visitors,
adding_visitors,
ordering_visitors,
sessions,
viewing_sessions,
adding_sessions,
ordering_sessions,
store_purchase_trips,
store_traffic,
ordering_visitors_or_store_purchase_trips,
sessions_or_store_traffic,
transactions_total,
transactions_purchase,
transactions_return,
visits_total,
visits_purchase,
visits_return,
dw_sys_load_tmstp
)
WITH sales_demand AS (SELECT tran_date,
 business_unit_desc_bopus_in_store_demand AS business_unit_desc,
 SUBSTR(data_source, 1, 20) AS data_source,
 currency_code,
 store_num_bopus_in_store_demand AS store_num,
 CAST(cts_segment AS STRING),
 rfm_segment,
 rewards_level,
 platform,
 SUM(CASE
   WHEN LOWER(customer_journey) <> LOWER('BOPUS')
   THEN demand_amt_bopus_in_store
   ELSE 0
   END) AS demand_amt_excl_bopus,
 SUM(CASE
   WHEN LOWER(customer_journey) = LOWER('BOPUS')
   THEN demand_amt_bopus_in_store
   ELSE 0
   END) AS bopus_attr_store_amt,
 0.00 AS bopus_attr_digital_amt,
 SUM(demand_canceled_amt) AS demand_canceled_amt,
 SUM(CASE
   WHEN LOWER(customer_journey) <> LOWER('BOPUS')
   THEN demand_units_bopus_in_store
   ELSE 0
   END) AS demand_units_excl_bopus,
 SUM(CASE
   WHEN LOWER(customer_journey) = LOWER('BOPUS')
   THEN demand_units_bopus_in_store
   ELSE 0
   END) AS bopus_attr_store_units,
 0 AS bopus_attr_digital_units,
 SUM(demand_canceled_units) AS demand_canceled_units,
 SUM(gross_merch_sales_amt) AS gross_merch_sales_amt,
 SUM(merch_returns_amt) AS merch_returns_amt,
 SUM(net_merch_sales_amt) AS net_merch_sales_amt,
 SUM(gross_operational_sales_amt) AS gross_operational_sales_amt,
 SUM(operational_returns_amt) AS operational_returns_amt,
 SUM(net_operational_sales_amt) AS net_operational_sales_amt,
 SUM(gross_merch_sales_units) AS gross_merch_sales_units,
 SUM(merch_returns_units) AS merch_returns_units,
 SUM(net_merch_sales_units) AS net_merch_sales_units,
 SUM(gross_operational_sales_units) AS gross_operational_sales_units,
 SUM(operational_returns_units) AS operational_returns_units,
 SUM(net_operational_sales_units) AS net_operational_sales_units
FROM `{{params.gcp_project_id}}`.{{params.mothership_t2_schema}}.finance_sales_demand_fact AS fsdf
WHERE LOWER(business_unit_desc_bopus_in_store_demand) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('RACK'),
   LOWER('RACK CANADA'))
GROUP BY tran_date,
 business_unit_desc,
 data_source,
 currency_code,
 store_num,
 cts_segment,
 rfm_segment,
 rewards_level,
 platform
UNION ALL
SELECT tran_date,
 business_unit_desc_bopus_in_digital_demand AS business_unit_desc,
 SUBSTR(data_source, 1, 20) AS data_source,
 currency_code,
 store_num_bopus_in_digital_demand AS store_num,
 cts_segment,
 rfm_segment,
 rewards_level,
 platform,
 SUM(CASE
   WHEN LOWER(COALESCE(customer_journey, FORMAT('%4d', 0))) <> LOWER('BOPUS')
   THEN CAST(demand_amt_bopus_in_digital AS NUMERIC)
   ELSE 0
   END) AS demand_amt_excl_bopus,
 0.00 AS bopus_attr_store_amt,
 SUM(CASE
   WHEN LOWER(COALESCE(customer_journey, FORMAT('%4d', 0))) = LOWER('BOPUS')
   THEN CAST(demand_amt_bopus_in_digital AS NUMERIC)
   ELSE 0
   END) AS bopus_attr_digital_amt,
 SUM(demand_canceled_amt) AS demand_canceled_amt,
 SUM(CASE
   WHEN LOWER(customer_journey) <> LOWER('BOPUS')
   THEN demand_units_bopus_in_digital
   ELSE 0
   END) AS demand_units_excl_bopus,
 0 AS bopus_attr_store_units,
 SUM(CASE
   WHEN LOWER(customer_journey) = LOWER('BOPUS')
   THEN demand_units_bopus_in_digital
   ELSE 0
   END) AS bopus_attr_digital_units,
 SUM(demand_canceled_units) AS demand_canceled_units,
 SUM(gross_merch_sales_amt) AS gross_merch_sales_amt,
 SUM(merch_returns_amt) AS merch_returns_amt,
 SUM(net_merch_sales_amt) AS net_merch_sales_amt,
 SUM(gross_operational_sales_amt) AS gross_operational_sales_amt,
 SUM(operational_returns_amt) AS operational_returns_amt,
 SUM(net_operational_sales_amt) AS net_operational_sales_amt,
 SUM(gross_merch_sales_units) AS gross_merch_sales_units,
 SUM(merch_returns_units) AS merch_returns_units,
 SUM(net_merch_sales_units) AS net_merch_sales_units,
 SUM(gross_operational_sales_units) AS gross_operational_sales_units,
 SUM(operational_returns_units) AS operational_returns_units,
 SUM(net_operational_sales_units) AS net_operational_sales_units
FROM `{{params.gcp_project_id}}`.{{params.mothership_t2_schema}}.finance_sales_demand_fact AS fsdf
WHERE LOWER(business_unit_desc_bopus_in_digital_demand) IN (LOWER('OFFPRICE ONLINE'), LOWER('N.COM'), LOWER('N.CA'))
GROUP BY tran_date,
 business_unit_desc,
 data_source,
 currency_code,
 store_num,
 cts_segment,
 rfm_segment,
 rewards_level,
 platform)

,bot_restatement AS (SELECT day_date,
  CASE
  WHEN LOWER(box) = LOWER('rcom')
  THEN 'OFFPRICE ONLINE'
  WHEN LOWER(box) = LOWER('ncom')
  THEN 'N.COM'
  ELSE NULL
  END AS business_unit,
 device_type,
 SUM(suspicious_visitors) AS suspicious_visitors,
 SUM(suspicious_viewing_visitors) AS suspicious_viewing_visitors,
 SUM(suspicious_adding_visitors) AS suspicious_adding_visitors
FROM `{{params.gcp_project_id}}`.{{params.mothership_t2_schema}}.bot_traffic_fix
GROUP BY day_date,
 business_unit,
 device_type)

,combo AS (SELECT tran_date,
 business_unit_desc,
 SUBSTR(data_source, 1, 20) AS data_source,
 currency_code,
 store_num,
 rewards_level,
 platform,
 SUM(demand_amt_excl_bopus) AS demand_amt_excl_bopus,
 SUM(bopus_attr_store_amt) AS bopus_attr_store_amt,
 SUM(bopus_attr_digital_amt) AS bopus_attr_digital_amt,
 SUM(COALESCE(demand_canceled_amt, 0)) AS demand_canceled_amt,
 SUM(demand_units_excl_bopus) AS demand_units_excl_bopus,
 SUM(bopus_attr_store_units) AS bopus_attr_store_units,
 SUM(bopus_attr_digital_units) AS bopus_attr_digital_units,
 SUM(COALESCE(demand_canceled_units, 0)) AS demand_canceled_units,
 SUM(gross_merch_sales_amt) AS gross_merch_sales_amt,
 SUM(merch_returns_amt) AS merch_returns_amt,
 SUM(net_merch_sales_amt) AS net_merch_sales_amt,
 SUM(gross_operational_sales_amt) AS gross_operational_sales_amt,
 SUM(operational_returns_amt) AS operational_returns_amt,
 SUM(net_operational_sales_amt) AS net_operational_sales_amt,
 SUM(gross_merch_sales_units) AS gross_merch_sales_units,
 SUM(merch_returns_units) AS merch_returns_units,
 SUM(net_merch_sales_units) AS net_merch_sales_units,
 SUM(gross_operational_sales_units) AS gross_operational_sales_units,
 SUM(operational_returns_units) AS operational_returns_units,
 SUM(net_operational_sales_units) AS net_operational_sales_units,
 CAST(NULL AS BIGINT) AS orders,
 CAST(NULL AS NUMERIC) AS visitors,
 CAST(NULL AS NUMERIC) AS viewing_visitors,
 CAST(NULL AS NUMERIC) AS adding_visitors,
 CAST(NULL AS INTEGER) AS ordering_visitors,
 CAST(NULL AS INTEGER) AS sessions,
 CAST(NULL AS INTEGER) AS viewing_sessions,
 CAST(NULL AS INTEGER) AS adding_sessions,
 CAST(NULL AS INTEGER) AS ordering_sessions,
 CAST(NULL AS NUMERIC) AS store_purchase_trips,
 CAST(NULL AS NUMERIC) AS store_traffic,
 CAST(NULL AS INTEGER) AS ordering_visitors_or_store_purchase_trips,
 CAST(NULL AS INTEGER) AS sessions_or_store_traffic,
 NULL AS transactions_total,
 NULL AS transactions_purchase,
 NULL AS transactions_return,
 NULL AS visits_total,
 NULL AS visits_purchase,
 NULL AS visits_return
FROM sales_demand
GROUP BY tran_date,
 business_unit_desc,
 data_source,
 currency_code,
 store_num,
 rewards_level,
 platform
UNION ALL
SELECT order_date_pacific AS tran_date,
 business_unit_desc,
 'NAP_ERTM' AS data_source,
 order_line_currency_code AS currency_code,
  CASE
  WHEN LOWER(source_platform_code) = LOWER('POS')
  THEN source_store_num
  WHEN LOWER(business_unit_desc) = LOWER('N.COM')
  THEN 808
  WHEN LOWER(business_unit_desc) = LOWER('N.CA')
  THEN 867
  WHEN LOWER(business_unit_desc) = LOWER('OFFPRICE ONLINE')
  THEN 828
  ELSE NULL
  END AS store_num,
 rewards_level,
  CASE
  WHEN LOWER(business_unit_desc) IN (LOWER('N.COM'), LOWER('N.CA'))
  THEN source_platform_code
  WHEN LOWER(business_unit_desc) = LOWER('OFFPRICE ONLINE') AND order_date_pacific >= DATE '2021-03-06'
  THEN source_platform_code
  ELSE 'total_nrhl'
  END AS platform,
 CAST(NULL AS NUMERIC) AS demand_amt_excl_bopus,
 CAST(NULL AS NUMERIC) AS bopus_attr_store_amt,
 CAST(NULL AS NUMERIC) AS bopus_attr_digital_amt,
 CAST(NULL AS NUMERIC) AS demand_canceled_amt,
 CAST(NULL AS NUMERIC) AS demand_units_excl_bopus,
 CAST(NULL AS NUMERIC) AS bopus_attr_store_units,
 CAST(NULL AS NUMERIC) AS bopus_attr_digital_units,
 CAST(NULL AS NUMERIC) AS demand_canceled_units,
 CAST(NULL AS NUMERIC) AS gross_merch_sales_amt,
 CAST(NULL AS NUMERIC) AS merch_returns_amt,
 CAST(NULL AS NUMERIC) AS net_merch_sales_amt,
 CAST(NULL AS NUMERIC) AS gross_operational_sales_amt,
 CAST(NULL AS NUMERIC) AS operational_returns_amt,
 CAST(NULL AS NUMERIC) AS net_operational_sales_amt,
 CAST(NULL AS NUMERIC) AS gross_merch_sales_units,
 CAST(NULL AS NUMERIC) AS merch_returns_units,
 CAST(NULL AS NUMERIC) AS net_merch_sales_units,
 CAST(NULL AS NUMERIC) AS gross_operational_sales_units,
 CAST(NULL AS NUMERIC) AS operational_returns_units,
 CAST(NULL AS NUMERIC) AS net_operational_sales_units,
 COUNT(DISTINCT order_num) AS orders,
 CAST(NULL AS NUMERIC) AS visitors,
 CAST(NULL AS NUMERIC) AS viewing_visitors,
 CAST(NULL AS NUMERIC) AS adding_visitors,
 CAST(NULL AS INTEGER) AS ordering_visitors,
 CAST(NULL AS INTEGER) AS sessions,
 CAST(NULL AS INTEGER) AS viewing_sessions,
 CAST(NULL AS INTEGER) AS adding_sessions,
 CAST(NULL AS INTEGER) AS ordering_sessions,
 CAST(NULL AS NUMERIC) AS store_purchase_trips,
 CAST(NULL AS NUMERIC) AS store_traffic,
 CAST(NULL AS INTEGER) AS ordering_visitors_or_store_purchase_trips,
 CAST(NULL AS INTEGER) AS sessions_or_store_traffic,
 NULL AS transactions_total,
 NULL AS transactions_purchase,
 NULL AS transactions_return,
 NULL AS visits_total,
 NULL AS visits_purchase,
 NULL AS visits_return
FROM oldf_cust_all
WHERE order_date_pacific BETWEEN {{params.start_date}} AND {{params.end_date}} 
GROUP BY tran_date,
 business_unit_desc,
 data_source,
 currency_code,
 store_num,
 rewards_level,
 platform
UNION ALL
SELECT CAST(day_date AS DATE) AS tran_date,
 'OFFPRICE ONLINE' AS business_unit_desc,
 'NRHL_redshift' AS data_source,
 'USD' AS currency_code,
 828 AS store_num,
 'total_nrhl' AS rewards_level,
 'total_nrhl' AS platform,
 CAST(NULL AS NUMERIC) AS demand_amt_excl_bopus,
 CAST(NULL AS NUMERIC) AS bopus_attr_store_amt,
 CAST(NULL AS NUMERIC) AS bopus_attr_digital_amt,
 CAST(NULL AS NUMERIC) AS demand_canceled_amt,
 CAST(NULL AS NUMERIC) AS demand_units_excl_bopus,
 CAST(NULL AS NUMERIC) AS bopus_attr_store_units,
 CAST(NULL AS NUMERIC) AS bopus_attr_digital_units,
 CAST(NULL AS NUMERIC) AS demand_canceled_units,
 CAST(NULL AS NUMERIC) AS gross_merch_sales_amt,
 CAST(NULL AS NUMERIC) AS merch_returns_amt,
 CAST(NULL AS NUMERIC) AS net_merch_sales_amt,
 CAST(NULL AS NUMERIC) AS gross_operational_sales_amt,
 CAST(NULL AS NUMERIC) AS operational_returns_amt,
 CAST(NULL AS NUMERIC) AS net_operational_sales_amt,
 CAST(NULL AS NUMERIC) AS gross_merch_sales_units,
 CAST(NULL AS NUMERIC) AS merch_returns_units,
 CAST(NULL AS NUMERIC) AS net_merch_sales_units,
 CAST(NULL AS NUMERIC) AS gross_operational_sales_units,
 CAST(NULL AS NUMERIC) AS operational_returns_units,
 CAST(NULL AS NUMERIC) AS net_operational_sales_units,
 CAST(orders AS NUMERIC) AS orders,
 CAST(NULL AS NUMERIC) AS visitors,
 CAST(NULL AS NUMERIC) AS viewing_visitors,
 CAST(NULL AS NUMERIC) AS adding_visitors,
 CAST(NULL AS INTEGER) AS ordering_visitors,
 CAST(NULL AS INTEGER) AS sessions,
 CAST(NULL AS INTEGER) AS viewing_sessions,
 CAST(NULL AS INTEGER) AS adding_sessions,
 CAST(NULL AS INTEGER) AS ordering_sessions,
 CAST(NULL AS NUMERIC) AS store_purchase_trips,
 CAST(NULL AS NUMERIC) AS store_traffic,
 CAST(NULL AS INTEGER) AS ordering_visitors_or_store_purchase_trips,
 CAST(NULL AS INTEGER) AS sessions_or_store_traffic,
 NULL AS transactions_total,
 NULL AS transactions_purchase,
 NULL AS transactions_return,
 NULL AS visits_total,
 NULL AS visits_purchase,
 NULL AS visits_return
FROM `{{params.gcp_project_id}}`.{{params.mothership_t2_schema}}.nrhl_history_redshift_birdseye_hourly
WHERE LOWER(day_date) BETWEEN LOWER('2019-02-03') AND (LOWER('2020-11-20'))
 AND CAST(day_date AS DATE) BETWEEN {{params.start_date}} AND {{params.end_date}}
UNION ALL
SELECT vff.activity_date AS tran_date,
  CASE
  WHEN LOWER(vff.channel_country) = LOWER('US') AND LOWER(vff.purchase_channel) = LOWER('FULL_LINE')
  THEN 'N.COM'
  WHEN LOWER(vff.channel_country) = LOWER('CA') AND LOWER(vff.purchase_channel) = LOWER('FULL_LINE')
  THEN 'N.CA'
  WHEN LOWER(vff.channel_country) = LOWER('US') AND LOWER(vff.purchase_channel) = LOWER('RACK')
  THEN 'OFFPRICE ONLINE'
  ELSE NULL
  END AS business_unit_desc,
 'NAP_ERTM' AS data_source,
  CASE
  WHEN LOWER(vff.channel_country) = LOWER('US')
  THEN 'USD'
  WHEN LOWER(vff.channel_country) = LOWER('CA')
  THEN 'CAD'
  ELSE NULL
  END AS currency_code,
  CASE
  WHEN LOWER(CASE
     WHEN LOWER(vff.channel_country) = LOWER('US') AND LOWER(vff.purchase_channel) = LOWER('FULL_LINE')
     THEN 'N.COM'
     WHEN LOWER(vff.channel_country) = LOWER('CA') AND LOWER(vff.purchase_channel) = LOWER('FULL_LINE')
     THEN 'N.CA'
     WHEN LOWER(vff.channel_country) = LOWER('US') AND LOWER(vff.purchase_channel) = LOWER('RACK')
     THEN 'OFFPRICE ONLINE'
     ELSE NULL
     END) = LOWER('N.COM')
  THEN 808
  WHEN LOWER(CASE
     WHEN LOWER(vff.channel_country) = LOWER('US') AND LOWER(vff.purchase_channel) = LOWER('FULL_LINE')
     THEN 'N.COM'
     WHEN LOWER(vff.channel_country) = LOWER('CA') AND LOWER(vff.purchase_channel) = LOWER('FULL_LINE')
     THEN 'N.CA'
     WHEN LOWER(vff.channel_country) = LOWER('US') AND LOWER(vff.purchase_channel) = LOWER('RACK')
     THEN 'OFFPRICE ONLINE'
     ELSE NULL
     END) = LOWER('N.CA')
  THEN 867
  WHEN LOWER(CASE
     WHEN LOWER(vff.channel_country) = LOWER('US') AND LOWER(vff.purchase_channel) = LOWER('FULL_LINE')
     THEN 'N.COM'
     WHEN LOWER(vff.channel_country) = LOWER('CA') AND LOWER(vff.purchase_channel) = LOWER('FULL_LINE')
     THEN 'N.CA'
     WHEN LOWER(vff.channel_country) = LOWER('US') AND LOWER(vff.purchase_channel) = LOWER('RACK')
     THEN 'OFFPRICE ONLINE'
     ELSE NULL
     END) = LOWER('OFFPRICE ONLINE')
  THEN 828
  ELSE NULL
  END AS store_num,
 'total_digital' AS rewards_level,
 vff.platform,
 CAST(NULL AS NUMERIC) AS demand_amt_excl_bopus,
 CAST(NULL AS NUMERIC) AS bopus_attr_store_amt,
 CAST(NULL AS NUMERIC) AS bopus_attr_digital_amt,
 CAST(NULL AS NUMERIC) AS demand_canceled_amt,
 CAST(NULL AS NUMERIC) AS demand_units_excl_bopus,
 CAST(NULL AS NUMERIC) AS bopus_attr_store_units,
 CAST(NULL AS NUMERIC) AS bopus_attr_digital_units,
 CAST(NULL AS NUMERIC) AS demand_canceled_units,
 CAST(NULL AS NUMERIC) AS gross_merch_sales_amt,
 CAST(NULL AS NUMERIC) AS merch_returns_amt,
 CAST(NULL AS NUMERIC) AS net_merch_sales_amt,
 CAST(NULL AS NUMERIC) AS gross_operational_sales_amt,
 CAST(NULL AS NUMERIC) AS operational_returns_amt,
 CAST(NULL AS NUMERIC) AS net_operational_sales_amt,
 CAST(NULL AS NUMERIC) AS gross_merch_sales_units,
 CAST(NULL AS NUMERIC) AS merch_returns_units,
 CAST(NULL AS NUMERIC) AS net_merch_sales_units,
 CAST(NULL AS NUMERIC) AS gross_operational_sales_units,
 CAST(NULL AS NUMERIC) AS operational_returns_units,
 CAST(NULL AS NUMERIC) AS net_operational_sales_units,
 CAST(NULL AS NUMERIC) AS orders,
  SUM(vff.visitor_count) - MAX(COALESCE(b.suspicious_visitors, 0)) AS visitors,
  SUM(vff.product_view_visitors) - MAX(COALESCE(b.suspicious_viewing_visitors, 0)) AS viewing_visitors,
  SUM(vff.cart_add_visitors) - MAX(COALESCE(b.suspicious_adding_visitors, 0)) AS adding_visitors,
 SUM(vff.ordering_visitors) AS ordering_visitors,
 CAST(NULL AS INTEGER) AS sessions,
 CAST(NULL AS INTEGER) AS viewing_sessions,
 CAST(NULL AS INTEGER) AS adding_sessions,
 CAST(NULL AS INTEGER) AS ordering_sessions,
 CAST(NULL AS NUMERIC) AS store_purchase_trips,
 CAST(NULL AS NUMERIC) AS store_traffic,
 SUM(vff.ordering_visitors) AS ordering_visitors_or_store_purchase_trips,
 CAST(NULL AS INTEGER) AS sessions_or_store_traffic,
 NULL AS transactions_total,
 NULL AS transactions_purchase,
 NULL AS transactions_return,
 NULL AS visits_total,
 NULL AS visits_purchase,
 NULL AS visits_return
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.visitor_funnel_fact AS vff
 LEFT JOIN bot_restatement AS b ON vff.activity_date = b.day_date AND LOWER(CASE
      WHEN LOWER(vff.channel_country) = LOWER('US') AND LOWER(vff.purchase_channel) = LOWER('FULL_LINE')
      THEN 'N.COM'
      WHEN LOWER(vff.channel_country) = LOWER('CA') AND LOWER(vff.purchase_channel) = LOWER('FULL_LINE')
      THEN 'N.CA'
      WHEN LOWER(vff.channel_country) = LOWER('US') AND LOWER(vff.purchase_channel) = LOWER('RACK')
      THEN 'OFFPRICE ONLINE'
      ELSE NULL
      END) = LOWER(b.business_unit) AND LOWER(vff.platform) = LOWER(b.device_type)
WHERE vff.activity_date BETWEEN DATE '2019-02-03' AND (DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY))
 AND vff.activity_date BETWEEN {{params.start_date}} AND {{params.end_date}}
 AND (LOWER(CASE
      WHEN LOWER(vff.channel_country) = LOWER('US') AND LOWER(vff.purchase_channel) = LOWER('FULL_LINE')
      THEN 'N.COM'
      WHEN LOWER(vff.channel_country) = LOWER('CA') AND LOWER(vff.purchase_channel) = LOWER('FULL_LINE')
      THEN 'N.CA'
      WHEN LOWER(vff.channel_country) = LOWER('US') AND LOWER(vff.purchase_channel) = LOWER('RACK')
      THEN 'OFFPRICE ONLINE'
      ELSE NULL
      END) IN (LOWER('N.COM'), LOWER('N.CA')) OR LOWER(CASE
       WHEN LOWER(vff.channel_country) = LOWER('US') AND LOWER(vff.purchase_channel) = LOWER('FULL_LINE')
       THEN 'N.COM'
       WHEN LOWER(vff.channel_country) = LOWER('CA') AND LOWER(vff.purchase_channel) = LOWER('FULL_LINE')
       THEN 'N.CA'
       WHEN LOWER(vff.channel_country) = LOWER('US') AND LOWER(vff.purchase_channel) = LOWER('RACK')
       THEN 'OFFPRICE ONLINE'
       ELSE NULL
       END) = LOWER('OFFPRICE ONLINE') AND vff.activity_date >= DATE '2021-06-13')
 AND LOWER(vff.platform) <> LOWER('POS')
 AND LOWER(vff.bot_traffic_ind) NOT IN (LOWER('Y'))
GROUP BY tran_date,
 business_unit_desc,
 data_source,
 currency_code,
 store_num,
 rewards_level,
 vff.platform
UNION ALL
SELECT activity_date_pacific AS tran_date,
  CASE
  WHEN LOWER(channelcountry) = LOWER('US') AND LOWER(channel) = LOWER('NORDSTROM')
  THEN 'N.COM'
  WHEN LOWER(channelcountry) = LOWER('CA') AND LOWER(channel) = LOWER('NORDSTROM')
  THEN 'N.CA'
  WHEN LOWER(channelcountry) = LOWER('US') AND LOWER(channel) IN (LOWER('NORDSTROM_RACK'), LOWER('HAUTELOOK'))
  THEN 'OFFPRICE ONLINE'
  ELSE channel
  END AS business_unit_desc,
 'NAP_ERTM' AS data_source,
  CASE
  WHEN LOWER(channelcountry) = LOWER('US')
  THEN 'USD'
  WHEN LOWER(channelcountry) = LOWER('CA')
  THEN 'CAD'
  ELSE NULL
  END AS currency_code,
  CASE
  WHEN LOWER(CASE
     WHEN LOWER(channelcountry) = LOWER('US') AND LOWER(channel) = LOWER('NORDSTROM')
     THEN 'N.COM'
     WHEN LOWER(channelcountry) = LOWER('CA') AND LOWER(channel) = LOWER('NORDSTROM')
     THEN 'N.CA'
     WHEN LOWER(channelcountry) = LOWER('US') AND LOWER(channel) IN (LOWER('NORDSTROM_RACK'), LOWER('HAUTELOOK'))
     THEN 'OFFPRICE ONLINE'
     ELSE channel
     END) = LOWER('N.COM')
  THEN 808
  WHEN LOWER(CASE
     WHEN LOWER(channelcountry) = LOWER('US') AND LOWER(channel) = LOWER('NORDSTROM')
     THEN 'N.COM'
     WHEN LOWER(channelcountry) = LOWER('CA') AND LOWER(channel) = LOWER('NORDSTROM')
     THEN 'N.CA'
     WHEN LOWER(channelcountry) = LOWER('US') AND LOWER(channel) IN (LOWER('NORDSTROM_RACK'), LOWER('HAUTELOOK'))
     THEN 'OFFPRICE ONLINE'
     ELSE channel
     END) = LOWER('N.CA')
  THEN 867
  WHEN LOWER(CASE
     WHEN LOWER(channelcountry) = LOWER('US') AND LOWER(channel) = LOWER('NORDSTROM')
     THEN 'N.COM'
     WHEN LOWER(channelcountry) = LOWER('CA') AND LOWER(channel) = LOWER('NORDSTROM')
     THEN 'N.CA'
     WHEN LOWER(channelcountry) = LOWER('US') AND LOWER(channel) IN (LOWER('NORDSTROM_RACK'), LOWER('HAUTELOOK'))
     THEN 'OFFPRICE ONLINE'
     ELSE channel
     END) = LOWER('OFFPRICE ONLINE')
  THEN 828
  ELSE NULL
  END AS store_num,
 'total_digital' AS rewards_level,
  CASE
  WHEN LOWER(experience) = LOWER('CARE_PHONE')
  THEN 'CSR_PHONE'
  WHEN LOWER(experience) = LOWER('ANDROID_APP')
  THEN 'ANDROID'
  WHEN LOWER(experience) = LOWER('MOBILE_WEB')
  THEN 'MOW'
  WHEN LOWER(experience) = LOWER('DESKTOP_WEB')
  THEN 'WEB'
  WHEN LOWER(experience) = LOWER('POINT_OF_SALE')
  THEN 'POS'
  WHEN LOWER(experience) = LOWER('IOS_APP')
  THEN 'IOS'
  WHEN LOWER(experience) = LOWER('VENDOR')
  THEN 'THIRD_PARTY_VENDOR'
  ELSE experience
  END AS platform,
 CAST(NULL AS NUMERIC) AS demand_amt_excl_bopus,
 CAST(NULL AS NUMERIC) AS bopus_attr_store_amt,
 CAST(NULL AS NUMERIC) AS bopus_attr_digital_amt,
 CAST(NULL AS NUMERIC) AS demand_canceled_amt,
 CAST(NULL AS NUMERIC) AS demand_units_excl_bopus,
 CAST(NULL AS NUMERIC) AS bopus_attr_store_units,
 CAST(NULL AS NUMERIC) AS bopus_attr_digital_units,
 CAST(NULL AS NUMERIC) AS demand_canceled_units,
 CAST(NULL AS NUMERIC) AS gross_merch_sales_amt,
 CAST(NULL AS NUMERIC) AS merch_returns_amt,
 CAST(NULL AS NUMERIC) AS net_merch_sales_amt,
 CAST(NULL AS NUMERIC) AS gross_operational_sales_amt,
 CAST(NULL AS NUMERIC) AS operational_returns_amt,
 CAST(NULL AS NUMERIC) AS net_operational_sales_amt,
 CAST(NULL AS NUMERIC) AS gross_merch_sales_units,
 CAST(NULL AS NUMERIC) AS merch_returns_units,
 CAST(NULL AS NUMERIC) AS net_merch_sales_units,
 CAST(NULL AS NUMERIC) AS gross_operational_sales_units,
 CAST(NULL AS NUMERIC) AS operational_returns_units,
 CAST(NULL AS NUMERIC) AS net_operational_sales_units,
 CAST(NULL AS NUMERIC) AS orders,
 CAST(NULL AS NUMERIC) AS visitors,
 CAST(NULL AS NUMERIC) AS viewing_visitors,
 CAST(NULL AS NUMERIC) AS adding_visitors,
 CAST(NULL AS INTEGER) AS ordering_visitors,
 SUM(sessions) AS sessions,
 SUM(CASE
   WHEN product_views >= 1
   THEN sessions
   ELSE 0
   END) AS viewing_sessions,
 SUM(CASE
   WHEN cart_adds >= 1
   THEN sessions
   ELSE 0
   END) AS adding_sessions,
 SUM(CASE
   WHEN web_orders >= 1
   THEN sessions
   ELSE 0
   END) AS ordering_sessions,
 CAST(NULL AS NUMERIC) AS store_purchase_trips,
 CAST(NULL AS NUMERIC) AS store_traffic,
 CAST(NULL AS INTEGER) AS ordering_visitors_or_store_purchase_trips,
 SUM(sessions) AS sessions_or_store_traffic,
 NULL AS transactions_total,
 NULL AS transactions_purchase,
 NULL AS transactions_return,
 NULL AS visits_total,
 NULL AS visits_purchase,
 NULL AS visits_return
FROM t2dl_das_sessions.dior_session_fact_daily
WHERE activity_date_pacific >= DATE '2022-01-31'
 AND activity_date_pacific BETWEEN {{params.start_date}} AND {{params.end_date}}
 AND LOWER(CASE
    WHEN LOWER(experience) = LOWER('CARE_PHONE')
    THEN 'CSR_PHONE'
    WHEN LOWER(experience) = LOWER('ANDROID_APP')
    THEN 'ANDROID'
    WHEN LOWER(experience) = LOWER('MOBILE_WEB')
    THEN 'MOW'
    WHEN LOWER(experience) = LOWER('DESKTOP_WEB')
    THEN 'WEB'
    WHEN LOWER(experience) = LOWER('POINT_OF_SALE')
    THEN 'POS'
    WHEN LOWER(experience) = LOWER('IOS_APP')
    THEN 'IOS'
    WHEN LOWER(experience) = LOWER('VENDOR')
    THEN 'THIRD_PARTY_VENDOR'
    ELSE experience
    END) <> LOWER('POS')
 AND LOWER(CASE
    WHEN LOWER(channelcountry) = LOWER('US') AND LOWER(channel) = LOWER('NORDSTROM')
    THEN 'N.COM'
    WHEN LOWER(channelcountry) = LOWER('CA') AND LOWER(channel) = LOWER('NORDSTROM')
    THEN 'N.CA'
    WHEN LOWER(channelcountry) = LOWER('US') AND LOWER(channel) IN (LOWER('NORDSTROM_RACK'), LOWER('HAUTELOOK'))
    THEN 'OFFPRICE ONLINE'
    ELSE channel
    END) NOT IN (LOWER('TRUNK_CLUB'))
GROUP BY tran_date,
 business_unit_desc,
 data_source,
 currency_code,
 store_num,
 rewards_level,
 platform
UNION ALL
SELECT st.day_date AS tran_date,
 sd.business_unit_desc,
 'wifi_camera_model' AS data_source,
  CASE
  WHEN LOWER(sd.business_unit_desc) LIKE LOWER('%Canada')
  THEN 'CAD'
  ELSE 'USD'
  END AS currency_code,
 st.store_number AS store_num,
 'total_store' AS rewards_level,
 'total_store' AS platform,
 CAST(NULL AS NUMERIC) AS demand_amt_excl_bopus,
 CAST(NULL AS NUMERIC) AS bopus_attr_store_amt,
 CAST(NULL AS NUMERIC) AS bopus_attr_digital_amt,
 CAST(NULL AS NUMERIC) AS demand_canceled_amt,
 CAST(NULL AS NUMERIC) AS demand_units_excl_bopus,
 CAST(NULL AS NUMERIC) AS bopus_attr_store_units,
 CAST(NULL AS NUMERIC) AS bopus_attr_digital_units,
 CAST(NULL AS NUMERIC) AS demand_canceled_units,
 CAST(NULL AS NUMERIC) AS gross_merch_sales_amt,
 CAST(NULL AS NUMERIC) AS merch_returns_amt,
 CAST(NULL AS NUMERIC) AS net_merch_sales_amt,
 CAST(NULL AS NUMERIC) AS gross_operational_sales_amt,
 CAST(NULL AS NUMERIC) AS operational_returns_amt,
 CAST(NULL AS NUMERIC) AS net_operational_sales_amt,
 CAST(NULL AS NUMERIC) AS gross_merch_sales_units,
 CAST(NULL AS NUMERIC) AS merch_returns_units,
 CAST(NULL AS NUMERIC) AS net_merch_sales_units,
 CAST(NULL AS NUMERIC) AS gross_operational_sales_units,
 CAST(NULL AS NUMERIC) AS operational_returns_units,
 CAST(NULL AS NUMERIC) AS net_operational_sales_units,
 CAST(NULL AS NUMERIC) AS orders,
 CAST(NULL AS NUMERIC) AS visitors,
 CAST(NULL AS NUMERIC) AS viewing_visitors,
 CAST(NULL AS NUMERIC) AS adding_visitors,
 CAST(NULL AS INTEGER) AS ordering_visitors,
 CAST(NULL AS INTEGER) AS sessions,
 CAST(NULL AS INTEGER) AS viewing_sessions,
 CAST(NULL AS INTEGER) AS adding_sessions,
 CAST(NULL AS INTEGER) AS ordering_sessions,
 st.purchase_trips AS store_purchase_trips,
 st.traffic AS store_traffic,
 st.purchase_trips AS ordering_visitors_or_store_purchase_trips,
 st.traffic AS sessions_or_store_traffic,
 NULL AS transactions_total,
 NULL AS transactions_purchase,
 NULL AS transactions_return,
 NULL AS visits_total,
 NULL AS visits_purchase,
 NULL AS visits_return
FROM t2dl_das_fls_traffic_model.store_traffic_daily_vw AS st
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS sd ON st.store_number = sd.store_num
WHERE st.day_date BETWEEN DATE '2019-02-03' AND (DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY))
 AND st.day_date BETWEEN {{params.start_date}} AND {{params.end_date}}
UNION ALL
SELECT CAST(day_date AS DATE) AS tran_date,
 'OFFPRICE ONLINE' AS business_unit_desc,
 'NRHL_redshift' AS data_source,
 'USD' AS currency_code,
 828 AS store_num,
 'total_nrhl' AS rewards_level,
 'ANDROID' AS platform,
 CAST(NULL AS NUMERIC) AS demand_amt_excl_bopus,
 CAST(NULL AS NUMERIC) AS bopus_attr_store_amt,
 CAST(NULL AS NUMERIC) AS bopus_attr_digital_amt,
 CAST(NULL AS NUMERIC) AS demand_canceled_amt,
 CAST(NULL AS NUMERIC) AS demand_units_excl_bopus,
 CAST(NULL AS NUMERIC) AS bopus_attr_store_units,
 CAST(NULL AS NUMERIC) AS bopus_attr_digital_units,
 CAST(NULL AS NUMERIC) AS demand_canceled_units,
 CAST(NULL AS NUMERIC) AS gross_merch_sales_amt,
 CAST(NULL AS NUMERIC) AS merch_returns_amt,
 CAST(NULL AS NUMERIC) AS net_merch_sales_amt,
 CAST(NULL AS NUMERIC) AS gross_operational_sales_amt,
 CAST(NULL AS NUMERIC) AS operational_returns_amt,
 CAST(NULL AS NUMERIC) AS net_operational_sales_amt,
 CAST(NULL AS NUMERIC) AS gross_merch_sales_units,
 CAST(NULL AS NUMERIC) AS merch_returns_units,
 CAST(NULL AS NUMERIC) AS net_merch_sales_units,
 CAST(NULL AS NUMERIC) AS gross_operational_sales_units,
 CAST(NULL AS NUMERIC) AS operational_returns_units,
 CAST(NULL AS NUMERIC) AS net_operational_sales_units,
 CAST(NULL AS NUMERIC) AS orders,
 SUM(visitors_android) AS visitors,
 SUM(viewing_visitors_android) AS viewing_visitors,
 SUM(adding_visitors_android) AS adding_visitors,
 SUM(ordering_visitors_android) AS ordering_visitors,
 CAST(NULL AS INTEGER) AS sessions,
 CAST(NULL AS INTEGER) AS viewing_sessions,
 CAST(NULL AS INTEGER) AS adding_sessions,
 CAST(NULL AS INTEGER) AS ordering_sessions,
 CAST(NULL AS NUMERIC) AS store_purchase_trips,
 CAST(NULL AS NUMERIC) AS store_traffic,
 SUM(ordering_visitors_android) AS ordering_visitors_or_store_purchase_trips,
 CAST(NULL AS NUMERIC) AS sessions_or_store_traffic,
 NULL AS transactions_total,
 NULL AS transactions_purchase,
 NULL AS transactions_return,
 NULL AS visits_total,
 NULL AS visits_purchase,
 NULL AS visits_return
FROM `{{params.gcp_project_id}}`.{{params.mothership_t2_schema}}.daily_vistors_ga_vff AS fun
WHERE day_date BETWEEN DATE '2019-02-03' AND DATE '2021-06-12'
 AND day_date BETWEEN {{params.start_date}} AND {{params.end_date}}
GROUP BY tran_date,
 business_unit_desc,
 data_source,
 currency_code,
 store_num,
 rewards_level,
 platform
UNION ALL
SELECT CAST(day_date AS DATE) AS tran_date,
 'OFFPRICE ONLINE' AS business_unit_desc,
 'NRHL_redshift' AS data_source,
 'USD' AS currency_code,
 828 AS store_num,
 'total_nrhl' AS rewards_level,
 'MOW' AS platform,
 CAST(NULL AS NUMERIC) AS demand_amt_excl_bopus,
 CAST(NULL AS NUMERIC) AS bopus_attr_store_amt,
 CAST(NULL AS NUMERIC) AS bopus_attr_digital_amt,
 CAST(NULL AS NUMERIC) AS demand_canceled_amt,
 CAST(NULL AS NUMERIC) AS demand_units_excl_bopus,
 CAST(NULL AS NUMERIC) AS bopus_attr_store_units,
 CAST(NULL AS NUMERIC) AS bopus_attr_digital_units,
 CAST(NULL AS NUMERIC) AS demand_canceled_units,
 CAST(NULL AS NUMERIC) AS gross_merch_sales_amt,
 CAST(NULL AS NUMERIC) AS merch_returns_amt,
 CAST(NULL AS NUMERIC) AS net_merch_sales_amt,
 CAST(NULL AS NUMERIC) AS gross_operational_sales_amt,
 CAST(NULL AS NUMERIC) AS operational_returns_amt,
 CAST(NULL AS NUMERIC) AS net_operational_sales_amt,
 CAST(NULL AS NUMERIC) AS gross_merch_sales_units,
 CAST(NULL AS NUMERIC) AS merch_returns_units,
 CAST(NULL AS NUMERIC) AS net_merch_sales_units,
 CAST(NULL AS NUMERIC) AS gross_operational_sales_units,
 CAST(NULL AS NUMERIC) AS operational_returns_units,
 CAST(NULL AS NUMERIC) AS net_operational_sales_units,
 CAST(NULL AS NUMERIC) AS orders,
 SUM(visitors_mow) AS visitors,
 SUM(viewing_visitors_mow) AS viewing_visitors,
 SUM(adding_visitors_mow) AS adding_visitors,
 SUM(ordering_visitors_mow) AS ordering_visitors,
 CAST(NULL AS INTEGER) AS sessions,
 CAST(NULL AS INTEGER) AS viewing_sessions,
 CAST(NULL AS INTEGER) AS adding_sessions,
 CAST(NULL AS INTEGER) AS ordering_sessions,
 CAST(NULL AS NUMERIC) AS store_purchase_trips,
 CAST(NULL AS NUMERIC) AS store_traffic,
 SUM(ordering_visitors_mow) AS ordering_visitors_or_store_purchase_trips,
 CAST(NULL AS NUMERIC) AS sessions_or_store_traffic,
 NULL AS transactions_total,
 NULL AS transactions_purchase,
 NULL AS transactions_return,
 NULL AS visits_total,
 NULL AS visits_purchase,
 NULL AS visits_return
FROM `{{params.gcp_project_id}}`.{{params.mothership_t2_schema}}.daily_vistors_ga_vff AS fun
WHERE day_date BETWEEN DATE '2019-02-03' AND DATE '2021-06-12'
 AND day_date BETWEEN {{params.start_date}} AND {{params.end_date}}
GROUP BY tran_date,
 business_unit_desc,
 data_source,
 currency_code,
 store_num,
 rewards_level,
 platform
UNION ALL
SELECT CAST(day_date AS DATE) AS tran_date,
 'OFFPRICE ONLINE' AS business_unit_desc,
 'NRHL_redshift' AS data_source,
 'USD' AS currency_code,
 828 AS store_num,
 'total_nrhl' AS rewards_level,
 'IOS' AS platform,
 CAST(NULL AS NUMERIC) AS demand_amt_excl_bopus,
 CAST(NULL AS NUMERIC) AS bopus_attr_store_amt,
 CAST(NULL AS NUMERIC) AS bopus_attr_digital_amt,
 CAST(NULL AS NUMERIC) AS demand_canceled_amt,
 CAST(NULL AS NUMERIC) AS demand_units_excl_bopus,
 CAST(NULL AS NUMERIC) AS bopus_attr_store_units,
 CAST(NULL AS NUMERIC) AS bopus_attr_digital_units,
 CAST(NULL AS NUMERIC) AS demand_canceled_units,
 CAST(NULL AS NUMERIC) AS gross_merch_sales_amt,
 CAST(NULL AS NUMERIC) AS merch_returns_amt,
 CAST(NULL AS NUMERIC) AS net_merch_sales_amt,
 CAST(NULL AS NUMERIC) AS gross_operational_sales_amt,
 CAST(NULL AS NUMERIC) AS operational_returns_amt,
 CAST(NULL AS NUMERIC) AS net_operational_sales_amt,
 CAST(NULL AS NUMERIC) AS gross_merch_sales_units,
 CAST(NULL AS NUMERIC) AS merch_returns_units,
 CAST(NULL AS NUMERIC) AS net_merch_sales_units,
 CAST(NULL AS NUMERIC) AS gross_operational_sales_units,
 CAST(NULL AS NUMERIC) AS operational_returns_units,
 CAST(NULL AS NUMERIC) AS net_operational_sales_units,
 CAST(NULL AS NUMERIC) AS orders,
 SUM(visitors_ios) AS visitors,
 SUM(viewing_visitors_ios) AS viewing_visitors,
 SUM(adding_visitors_ios) AS adding_visitors,
 SUM(ordering_visitors_ios) AS ordering_visitors,
 CAST(NULL AS INTEGER) AS sessions,
 CAST(NULL AS INTEGER) AS viewing_sessions,
 CAST(NULL AS INTEGER) AS adding_sessions,
 CAST(NULL AS INTEGER) AS ordering_sessions,
 CAST(NULL AS NUMERIC) AS store_purchase_trips,
 CAST(NULL AS NUMERIC) AS store_traffic,
 SUM(ordering_visitors_ios) AS ordering_visitors_or_store_purchase_trips,
 CAST(NULL AS NUMERIC) AS sessions_or_store_traffic,
 NULL AS transactions_total,
 NULL AS transactions_purchase,
 NULL AS transactions_return,
 NULL AS visits_total,
 NULL AS visits_purchase,
 NULL AS visits_return
FROM `{{params.gcp_project_id}}`.{{params.mothership_t2_schema}}.daily_vistors_ga_vff AS fun
WHERE day_date BETWEEN DATE '2019-02-03' AND DATE '2021-06-12'
 AND day_date BETWEEN {{params.start_date}} AND {{params.end_date}}
GROUP BY tran_date,
 business_unit_desc,
 data_source,
 currency_code,
 store_num,
 rewards_level,
 platform
UNION ALL
SELECT CAST(day_date AS DATE) AS tran_date,
 'OFFPRICE ONLINE' AS business_unit_desc,
 'NRHL_redshift' AS data_source,
 'USD' AS currency_code,
 828 AS store_num,
 'total_nrhl' AS rewards_level,
 'WEB' AS platform,
 CAST(NULL AS NUMERIC) AS demand_amt_excl_bopus,
 CAST(NULL AS NUMERIC) AS bopus_attr_store_amt,
 CAST(NULL AS NUMERIC) AS bopus_attr_digital_amt,
 CAST(NULL AS NUMERIC) AS demand_canceled_amt,
 CAST(NULL AS NUMERIC) AS demand_units_excl_bopus,
 CAST(NULL AS NUMERIC) AS bopus_attr_store_units,
 CAST(NULL AS NUMERIC) AS bopus_attr_digital_units,
 CAST(NULL AS NUMERIC) AS demand_canceled_units,
 CAST(NULL AS NUMERIC) AS gross_merch_sales_amt,
 CAST(NULL AS NUMERIC) AS merch_returns_amt,
 CAST(NULL AS NUMERIC) AS net_merch_sales_amt,
 CAST(NULL AS NUMERIC) AS gross_operational_sales_amt,
 CAST(NULL AS NUMERIC) AS operational_returns_amt,
 CAST(NULL AS NUMERIC) AS net_operational_sales_amt,
 CAST(NULL AS NUMERIC) AS gross_merch_sales_units,
 CAST(NULL AS NUMERIC) AS merch_returns_units,
 CAST(NULL AS NUMERIC) AS net_merch_sales_units,
 CAST(NULL AS NUMERIC) AS gross_operational_sales_units,
 CAST(NULL AS NUMERIC) AS operational_returns_units,
 CAST(NULL AS NUMERIC) AS net_operational_sales_units,
 CAST(NULL AS NUMERIC) AS orders,
 SUM(visitors_web) AS visitors,
 SUM(viewing_visitors_web) AS viewing_visitors,
 SUM(adding_visitors_web) AS adding_visitors,
 SUM(ordering_visitors_web) AS ordering_visitors,
 CAST(NULL AS INTEGER) AS sessions,
 CAST(NULL AS INTEGER) AS viewing_sessions,
 CAST(NULL AS INTEGER) AS adding_sessions,
 CAST(NULL AS INTEGER) AS ordering_sessions,
 CAST(NULL AS NUMERIC) AS store_purchase_trips,
 CAST(NULL AS NUMERIC) AS store_traffic,
 SUM(ordering_visitors_web) AS ordering_visitors_or_store_purchase_trips,
 CAST(NULL AS NUMERIC) AS sessions_or_store_traffic,
 NULL AS transactions_total,
 NULL AS transactions_purchase,
 NULL AS transactions_return,
 NULL AS visits_total,
 NULL AS visits_purchase,
 NULL AS visits_return
FROM `{{params.gcp_project_id}}`.{{params.mothership_t2_schema}}.daily_vistors_ga_vff AS fun
WHERE day_date BETWEEN DATE '2019-02-03' AND DATE '2021-06-12'
 AND day_date BETWEEN {{params.start_date}} AND {{params.end_date}}
GROUP BY tran_date,
 business_unit_desc,
 data_source,
 currency_code,
 store_num,
 rewards_level,
 platform)




SELECT tran_date,
 business_unit_desc,
 data_source,
 currency_code,
 store_num,
 cast(NULL as string) AS cts_segment,
 CAST(NULL AS STRING) AS rfm_segment,
 rewards_level,
 platform,
 Cast(SUM(demand_amt_excl_bopus) as NUMERIC) AS demand_amt_excl_bopus,
 SUM(bopus_attr_store_amt) AS bopus_attr_store_amt,
 SUM(bopus_attr_digital_amt) AS bopus_attr_digital_amt,
 SUM(demand_canceled_amt) AS demand_canceled_amt,
 SUM(demand_units_excl_bopus) AS demand_units_excl_bopus,
 SUM(bopus_attr_store_units) AS bopus_attr_store_units,
 SUM(bopus_attr_digital_units) AS bopus_attr_digital_units,
 SUM(demand_canceled_units) AS demand_canceled_units,
 SUM(gross_merch_sales_amt) AS gross_merch_sales_amt,
 SUM(merch_returns_amt) AS merch_returns_amt,
 SUM(net_merch_sales_amt) AS net_merch_sales_amt,
 SUM(gross_operational_sales_amt) AS gross_operational_sales_amt,
 SUM(operational_returns_amt) AS operational_returns_amt,
 SUM(net_operational_sales_amt) AS net_operational_sales_amt,
 SUM(gross_merch_sales_units) AS gross_merch_sales_units,
 SUM(merch_returns_units) AS merch_returns_units,
 SUM(net_merch_sales_units) AS net_merch_sales_units,
 SUM(gross_operational_sales_units) AS gross_operational_sales_units,
 SUM(operational_returns_units) AS operational_returns_units,
 SUM(net_operational_sales_units) AS net_operational_sales_units,
 SUM(orders) AS orders,
 SUM(visitors) AS visitors,
 SUM(viewing_visitors) AS viewing_visitors,
 SUM(adding_visitors) AS adding_visitors,
 SUM(ordering_visitors) AS ordering_visitors,
 SUM(sessions) AS sessions,
 SUM(viewing_sessions) AS viewing_sessions,
 SUM(adding_sessions) AS adding_sessions,
 SUM(ordering_sessions) AS ordering_sessions,
 SUM(store_purchase_trips) AS store_purchase_trips,
 SUM(store_traffic) AS store_traffic,
 SUM(ordering_visitors_or_store_purchase_trips) AS ordering_visitors_or_store_purchase_trips,
 SUM(sessions_or_store_traffic) AS sessions_or_store_traffic,
 SUM(transactions_total) AS transactions_total,
 SUM(transactions_purchase) AS transactions_purchase,
 SUM(transactions_return) AS transactions_return,
 SUM(visits_total) AS visits_total,
 SUM(visits_purchase) AS visits_purchase,
 SUM(visits_return) AS visits_return,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM combo
WHERE tran_date BETWEEN {{params.start_date}} AND {{params.end_date}}
GROUP BY tran_date,
 business_unit_desc,
 data_source,
 currency_code,
 store_num,
 cts_segment,
 rewards_level,
 platform,
 rfm_segment;



-- COLLECT STATISTICS COLUMN(tran_date), COLUMN(business_unit_desc), COLUMN(store_num), COLUMN(data_source), COLUMN(platform) ON {mothership_t2_schema}.SALES_DEMAND_ORDERS_TRAFFIC_FACT; --, COLUMN(rfm_segment), COLUMN(cts_segment)
-- SET QUERY_BAND = NONE FOR SESSION;