SET QUERY_BAND = 'App_ID=APP08176;
     DAG_ID=mothership_finance_sdotf_traffic_only_11521_ACE_ENG;
     Task_Name=sdotf_traffic_only;'
     FOR SESSION VOLATILE;
 
 /*
T2DL_DAS_MOTHERSHIP.SALES_DEMAND_ORDERS_TRAFFIC_FACT 
Description - This ddl creates a 7 box (n.com, r.com, n.ca, FULL LINE, FULL LINE CANADA, RACK, RACK CANADA) table of hourly sales, demand, orders, and traffic split by store number and platform (device, eg ios, web, mow)  
Full documenation: https://confluence.nordstrom.com/display/AS/FINANCE_SALES_DEMAND_FACT+and+SALES_DEMAND_ORDERS_TRAFFIC_FACT
Contacts: Matthew Bond, Analytics
*/

-- 3 dependencies- T2DL_DAS_MOTHERSHIP.FINANCE_SALES_DEMAND_FACT, T2DL_DAS_FLS_Traffic_Model.rack_traffic_daily, T2DL_DAS_FLS_Traffic_Model.fls_traffic_daily
--daily refresh schedule, needs to pull last 45 days to cover any changes to funnel data, which updates over last 40 days. 

/* step 1
save a temporary table before deleting data from t2dl table
*/
CREATE MULTISET VOLATILE TABLE sdotf AS (
select
tran_date
, business_unit_desc
, data_source
, currency_code
, store_num
, rewards_level
, platform
, sum(demand_amt_excl_bopus) as demand_amt_excl_bopus
, sum(bopus_attr_store_amt) as bopus_attr_store_amt
, sum(bopus_attr_digital_amt) as bopus_attr_digital_amt
, sum(demand_canceled_amt) as demand_canceled_amt
, sum(demand_units_excl_bopus) as demand_units_excl_bopus
, sum(bopus_attr_store_units) as bopus_attr_store_units
, sum(bopus_attr_digital_units) as bopus_attr_digital_units
, sum(demand_canceled_units) as demand_canceled_units
, sum(gross_merch_sales_amt) as gross_merch_sales_amt
, sum(merch_returns_amt) as merch_returns_amt
, sum(net_merch_sales_amt) as net_merch_sales_amt
, sum(gross_operational_sales_amt) as gross_operational_sales_amt
, sum(operational_returns_amt) as operational_returns_amt
, sum(net_operational_sales_amt) as net_operational_sales_amt
, sum(gross_merch_sales_units) as gross_merch_sales_units
, sum(merch_returns_units) as merch_returns_units
, sum(net_merch_sales_units) as net_merch_sales_units
, sum(gross_operational_sales_units) as gross_operational_sales_units
, sum(operational_returns_units) as operational_returns_units
, sum(net_operational_sales_units) as net_operational_sales_units
, sum(orders) as orders
, sum(visitors) as visitors     
, sum(viewing_visitors) as viewing_visitors 
, sum(adding_visitors) as adding_visitors  
, sum(ordering_visitors) as ordering_visitors
, sum(sessions) as sessions
, sum(viewing_sessions) as viewing_sessions 
, sum(adding_sessions) as adding_sessions
, sum(ordering_sessions) as ordering_sessions
, NULL as store_purchase_trips 
, NULL as store_traffic
, sum(ordering_visitors) as ordering_visitors_or_store_purchase_trips 
, sum(sessions) as sessions_or_store_traffic
, sum(transactions_total) as transactions_total
, sum(transactions_purchase) as transactions_purchase
, sum(transactions_return) as transactions_return
, sum(visits_total) as visits_total
, sum(visits_purchase) as visits_purchase
, sum(visits_return) as visits_return
FROM T2DL_DAS_MOTHERSHIP.SALES_DEMAND_ORDERS_TRAFFIC_FACT --
WHERE 1=1
and tran_date BETWEEN {start_date} AND {end_date}
group by 1,2,3,4,5,6,7
)
WITH DATA PRIMARY INDEX(tran_date) ON COMMIT PRESERVE ROWS;


DELETE FROM {mothership_t2_schema}.SALES_DEMAND_ORDERS_TRAFFIC_FACT WHERE tran_date BETWEEN {start_date} and {end_date};


/* step 2
final insert statement
*/
INSERT INTO {mothership_t2_schema}.SALES_DEMAND_ORDERS_TRAFFIC_FACT
with combo AS (
SELECT 
day_date AS tran_date	
--, 'total_store' AS hour_var
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
FROM T2DL_DAS_FLS_TRAFFIC_MODEL.STORE_TRAFFIC_DAILY_VW AS st
LEFT JOIN PRD_NAP_USR_VWS.STORE_DIM sd 
on sd.store_num = st.store_number
WHERE 1=1		
and day_date BETWEEN '2019-02-03' AND current_date -1
AND day_date BETWEEN {start_date} AND {end_date}
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
, null as cts_segment
, null as rfm_segment
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
, CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM combo
WHERE 1=1		
and tran_date BETWEEN {start_date} AND {end_date} 
GROUP BY 1,2,3,4,5,6,7,8,9
;


COLLECT STATISTICS COLUMN(tran_date), COLUMN(business_unit_desc), COLUMN(store_num), COLUMN(data_source), COLUMN(platform) ON {mothership_t2_schema}.SALES_DEMAND_ORDERS_TRAFFIC_FACT; --, COLUMN(rfm_segment), COLUMN(cts_segment)
SET QUERY_BAND = NONE FOR SESSION;