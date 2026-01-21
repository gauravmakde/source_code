BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP08176;
DAG_ID=mothership_finance_sdotf_traffic_only_11521_ACE_ENG;
---     Task_Name=sdotf_traffic_only;'*/
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS sdotf
AS
SELECT tran_date,
 business_unit_desc,
 data_source,
 currency_code,
 store_num,
 rewards_level,
 platform,
 SUM(demand_amt_excl_bopus) AS demand_amt_excl_bopus,
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
 NULL AS store_purchase_trips,
 NULL AS store_traffic,
 SUM(ordering_visitors) AS ordering_visitors_or_store_purchase_trips,
 SUM(sessions) AS sessions_or_store_traffic,
 SUM(transactions_total) AS transactions_total,
 SUM(transactions_purchase) AS transactions_purchase,
 SUM(transactions_return) AS transactions_return,
 SUM(visits_total) AS visits_total,
 SUM(visits_purchase) AS visits_purchase,
 SUM(visits_return) AS visits_return
FROM `{{params.gcp_project_id}}`.t2dl_das_mothership.sales_demand_orders_traffic_fact
WHERE tran_date BETWEEN {{params.start_date}} AND {{params.end_date}}
GROUP BY tran_date,
 business_unit_desc,
 data_source,
 currency_code,
 store_num,
 rewards_level,
 platform;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM 
`{{params.gcp_project_id}}`.{{params.mothership_t2_schema}}.sales_demand_orders_traffic_fact
WHERE tran_date BETWEEN {{params.start_date}} and {{params.end_date}};
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


INSERT INTO --t2dl_das_mothership.SALES_DEMAND_ORDERS_TRAFFIC_FACT
`{{params.gcp_project_id}}`.{{params.mothership_t2_schema}}.sales_demand_orders_traffic_fact
with combo AS (
SELECT
day_date AS tran_date

, sd.business_unit_desc
, 'wifi_camera_model' AS data_source
, CASE WHEN sd.business_unit_desc like '%Canada' THEN 'CAD' ELSE 'USD' END AS currency_code
, store_number AS store_num
, 'total_store' AS rewards_level
, 'total_store' AS platform
, NULL AS demand_amt_excl_bopus
, NULL AS bopus_attr_store_amt
, NULL AS bopus_attr_digital_amt
, NULL AS demand_canceled_amt
, NULL AS demand_units_excl_bopus
, NULL AS bopus_attr_store_units
, NULL AS bopus_attr_digital_units
, NULL AS demand_canceled_units
, NULL AS gross_merch_sales_amt
, NULL AS merch_returns_amt
, NULL AS net_merch_sales_amt
, NULL AS gross_operational_sales_amt
, NULL AS operational_returns_amt
, NULL AS net_operational_sales_amt
, NULL AS gross_merch_sales_units
, NULL AS merch_returns_units
, NULL AS net_merch_sales_units
, NULL AS gross_operational_sales_units
, NULL AS operational_returns_units
, NULL AS net_operational_sales_units
, NULL AS orders
, NULL AS visitors
, NULL AS viewing_visitors
, NULL AS adding_visitors
, NULL AS ordering_visitors
, NULL AS sessions
, NULL AS viewing_sessions
, NULL AS adding_sessions
, NULL AS ordering_sessions
, purchase_trips AS store_purchase_trips
, traffic AS store_traffic
, purchase_trips AS ordering_visitors_or_store_purchase_trips
, traffic AS sessions_or_store_traffic
, NULL AS transactions_total
, NULL AS transactions_purchase
, NULL AS transactions_return
, NULL AS visits_total
, NULL AS visits_purchase
, NULL AS visits_return
FROM `{{params.gcp_project_id}}`.T2DL_DAS_FLS_TRAFFIC_MODEL.STORE_TRAFFIC_DAILY_VW AS st
LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_USR_VWS.STORE_DIM sd
on sd.store_num = st.store_number
WHERE 1=1
and day_date BETWEEN '2019-02-03' AND current_date('PST8PDT') -1
AND day_date BETWEEN {{params.start_date}} AND {{params.end_date}}
UNION ALL
select
tran_date
, business_unit_desc
, data_source
, currency_code
, store_num
, rewards_level
, platform
, demand_amt_excl_bopus
, bopus_attr_store_amt
, bopus_attr_digital_amt
, demand_canceled_amt
, demand_units_excl_bopus
, bopus_attr_store_units
, bopus_attr_digital_units
, demand_canceled_units
, gross_merch_sales_amt
, merch_returns_amt
, net_merch_sales_amt
, gross_operational_sales_amt
, operational_returns_amt
, net_operational_sales_amt
, gross_merch_sales_units
, merch_returns_units
, net_merch_sales_units
, gross_operational_sales_units
, operational_returns_units
, net_operational_sales_units
, orders
, visitors
, viewing_visitors
, adding_visitors
, ordering_visitors
, sessions
, viewing_sessions
, adding_sessions
, ordering_sessions
, NULL as store_purchase_trips
, NULL as store_traffic
, ordering_visitors_or_store_purchase_trips
, sessions_or_store_traffic
, transactions_total
, transactions_purchase
, transactions_return
, visits_total
, visits_purchase
, visits_return
FROM sdotf
)
select
tran_date
, business_unit_desc
, data_source
, currency_code
, store_num
, 'null' as cts_segment
, 'null' as rfm_segment
, rewards_level
, platform
, SUM(demand_amt_excl_bopus) AS demand_amt_excl_bopus
, SUM(bopus_attr_store_amt) AS bopus_attr_store_amt
, SUM(bopus_attr_digital_amt) AS bopus_attr_digital_amt
, SUM(demand_canceled_amt) AS demand_canceled_amt
, SUM(demand_units_excl_bopus) AS demand_units_excl_bopus
, SUM(bopus_attr_store_units) AS bopus_attr_store_units
, SUM(bopus_attr_digital_units) AS bopus_attr_digital_units
, SUM(demand_canceled_units) AS demand_canceled_units
, SUM(gross_merch_sales_amt) AS gross_merch_sales_amt
, SUM(merch_returns_amt) AS merch_returns_amt
, SUM(net_merch_sales_amt) AS net_merch_sales_amt
, SUM(gross_operational_sales_amt) AS gross_operational_sales_amt
, SUM(operational_returns_amt) AS operational_returns_amt
, SUM(net_operational_sales_amt) AS net_operational_sales_amt
, SUM(gross_merch_sales_units) AS gross_merch_sales_units
, SUM(merch_returns_units) AS merch_returns_units
, SUM(net_merch_sales_units) AS net_merch_sales_units
, SUM(gross_operational_sales_units) AS gross_operational_sales_units
, SUM(operational_returns_units) AS operational_returns_units
, SUM(net_operational_sales_units) AS net_operational_sales_units
, SUM(orders) AS orders
, SUM(visitors) AS visitors
, SUM(viewing_visitors) AS viewing_visitors
, SUM(adding_visitors) AS adding_visitors
, SUM(ordering_visitors) AS ordering_visitors
, SUM(sessions) AS sessions
, SUM(viewing_sessions) AS viewing_sessions
, SUM(adding_sessions) AS adding_sessions
, SUM(ordering_sessions) AS ordering_sessions
, SUM(store_purchase_trips) AS store_purchase_trips
, SUM(store_traffic) AS store_traffic
, SUM(ordering_visitors_or_store_purchase_trips) AS ordering_visitors_or_store_purchase_trips
, SUM(sessions_or_store_traffic) AS sessions_or_store_traffic
, SUM(transactions_total) AS transactions_total
, SUM(transactions_purchase) AS transactions_purchase
, SUM(transactions_return) AS transactions_return
, SUM(visits_total) AS visits_total
, SUM(visits_purchase) AS visits_purchase
, SUM(visits_return) AS visits_return
, CURRENT_DATETIME('PST8PDT') as dw_sys_load_tmstp
FROM combo
WHERE 1=1
and tran_date BETWEEN {{params.start_date}} AND {{params.end_date}} 
GROUP BY 1,2,3,4,5,6,7,8,9
;


END;
