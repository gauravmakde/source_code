SET QUERY_BAND = 'App_ID=APP08176;
     DAG_ID=mothership_finance_sales_demand_orders_traffic_fact_11521_ACE_ENG;
     Task_Name=sales_demand_orders_traffic_fact;'
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
get loyalty information into format to show changes in loyalty status over time
*/
CREATE MULTISET VOLATILE TABLE loy AS (
SELECT DISTINCT 
acp_id
, rewards_level
, start_day_date
, end_day_date
, lead(start_day_date-1, 1) OVER (PARTITION BY acp_id ORDER BY start_day_date) AS end_day_date_2
-- original end_day_date = next start_day_date, so create new end_day_date that doesn't overlap
, COALESCE (end_day_date_2, end_day_date) AS end_day_date_lead
FROM PRD_NAP_USR_VWS.LOYALTY_LEVEL_LIFECYCLE_FACT_VW 
WHERE end_day_date > '2018-12-31' AND start_day_date < current_date
and start_day_date < end_day_date --remove a few duplicate rows WHERE the first day of the year is a lower status but instantly changes to higher status 
)
WITH DATA NO PRIMARY INDEX ON COMMIT PRESERVE ROWS; --optimized by TD team


COLLECT STATISTICS COLUMN (acp_id) ON loy;
COLLECT STATISTICS COLUMN (acp_id, start_day_date, end_day_date) ON loy;
COLLECT STATISTICS COLUMN (start_day_date) ON loy;
COLLECT STATISTICS COLUMN (end_day_date_lead) ON loy;
COLLECT STATISTICS COLUMN (end_day_date) ON loy;


/* step 3
get segmentation model information into format to show changes in segment over time
*/
-- CREATE MULTISET VOLATILE TABLE segment AS (
-- SELECT
-- acp_id
-- , predicted_segment AS cts_segment
-- , CASE WHEN Row_Number() OVER (PARTITION BY acp_id ORDER BY scored_date) = 1 THEN CAST('2019-01-01' AS DATE) ELSE scored_date END AS scored_date_start
-- -- case statement above replaces oldest scored date (~sept 2020) to estimate historical segment for shoppers before they were scored
-- , LEAD (scored_date-1, 1, Current_Date+1) OVER (PARTITION BY acp_id ORDER BY scored_date) AS scored_date_end --create END date to use IN join later
-- FROM {mothership_t2_schema}.CT_SEGMENT_AGG
-- QUALIFY scored_date_end >= {start_date} AND scored_date_start <= {end_date}
-- )
-- WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;
--
--
-- COLLECT STATISTICS COLUMN (acp_id) ON segment ;
-- COLLECT STATISTICS COLUMN (SCORED_DATE_END) ON segment ;
-- COLLECT STATISTICS COLUMN (SCORED_DATE_START) ON segment ;


/* step 4
dedupe xref table for joins later

CREATE MULTISET VOLATILE TABLE session_xref AS (
SELECT --DISTINCT 
session_id
, acp_id
, activity_date_pacific 
FROM PRD_NAP_USR_VWS.CUSTOMER_SESSION_XREF 
WHERE activity_date_pacific >= '2022-01-31' --first date of complete sessions data. Adding sessions is empty until 2022-02-09
and acp_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY dw_batch_date desc) =1
--some session_id's have more than one acp_id, so pick the most recently loaded one to avoid duplication
)
WITH DATA PRIMARY INDEX(session_id, activity_date_pacific) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (session_id, acp_id, activity_date_pacific) ON session_xref;
*/

/* step 5
cut down RTDF_vw to only needed columns to make joining faster later on
*/
-- CREATE MULTISET VOLATILE TABLE rtdf AS (
-- SELECT --DISTINCT
-- business_day_date, order_num, acp_id, intent_store_num, ringing_store_num, global_tran_id, line_item_seq_num, line_item_activity_type_desc, line_item_net_amt_currency_code, original_line_item_amt_currency_code
-- FROM PRD_NAP_USR_VWS.RETAIL_TRAN_DETAIL_FACT_vw
-- WHERE 1=1
-- AND line_net_usd_amt <> 0 --remove gift WITH purchase FROM units calculations
-- AND business_day_date BETWEEN {start_date} AND {end_date}
-- --and business_day_date IN ('2019-02-04', '2020-02-04', '2021-02-04', '2021-07-04', '2022-02-06', '2021-11-11') --for testing code
-- and ((intent_store_num = 828 AND business_day_date BETWEEN '2020-08-30' AND CURRENT_DATE - 1)
-- --r.com data before this period is based ON order date NOT shipped date.
-- --shipped date IN fact_shipment_items can't be linked back to rtdf (not enough info for which order lines are IN which shipment)
-- or (COALESCE(intent_store_num, 0) <> 828 AND business_day_date BETWEEN '2019-02-03' AND CURRENT_DATE - 1))
-- --add this to remove cases WHERE order_num is null for a single line item leading to duplication
-- QUALIFY ROW_NUMBER() OVER (PARTITION BY COALESCE(order_num,global_tran_id) ORDER BY tran_line_id desc) =1
-- )
-- WITH DATA PRIMARY INDEX(business_day_date, global_tran_id, line_item_seq_num) ON COMMIT PRESERVE ROWS;
--
--
-- COLLECT STATS ON rtdf COLUMN(acp_id,order_num, line_item_seq_num, global_tran_id,business_day_date);
-- COLLECT STATISTICS COLUMN (intent_store_num) ON rtdf;
--
--
-- /* step 6
-- create table to join oldf WITH rtdf later without inaccuracy
-- */
-- CREATE MULTISET VOLATILE TABLE pt_join AS (
-- SELECT --DISTINCT
-- order_num
-- , order_line_id
-- , business_day_date
-- , global_tran_id
-- , line_item_seq_num
-- FROM T2DL_DAS_SALES_RETURNS.SALES_AND_RETURNS_FACT_BASE sarf
-- WHERE 1=1
-- AND business_day_date BETWEEN ({start_date} - 365) AND {end_date} --need to extend start date to account for lag BETWEEN order AND sale
-- --and business_day_date IN ('2019-02-04', '2020-02-04', '2021-02-04', '2021-07-04', '2022-02-06', '2021-11-11') --for testing code
-- and ((intent_store_num = 828 AND business_day_date BETWEEN '2020-08-30' AND CURRENT_DATE - 1)
-- --r.com data before this period is based ON order date NOT shipped date.
-- --shipped date IN fact_shipment_items can't be linked back to rtdf (not enough info for which order lines are IN which shipment)
-- or (COALESCE(intent_store_num, 0) <> 828 AND business_day_date BETWEEN '2019-02-03' AND CURRENT_DATE - 1))
-- )
-- WITH DATA PRIMARY INDEX(business_day_date, global_tran_id, line_item_seq_num) ON COMMIT PRESERVE ROWS;
--
--
-- COLLECT STATISTICS COLUMN (business_day_date, global_tran_id, line_item_seq_num) ON pt_join;


/* step 7
cut down OLDF to only needed columns to make joining faster later on, mainly need device of transaction 
*/
CREATE MULTISET VOLATILE TABLE pre_oldf AS (
SELECT 
order_num--, order_line_num, order_line_id
, source_platform_code, order_date_pacific, order_line_currency_code, source_store_num, ent_cust_id, shopper_id
, CASE
    WHEN source_channel_country_code = 'US' AND source_channel_code = 'FULL_LINE' THEN 'N.COM'
    WHEN source_channel_country_code = 'CA' AND source_channel_code = 'FULL_LINE' THEN 'N.CA'
    WHEN source_channel_country_code = 'US' AND source_channel_code = 'RACK' THEN 'OFFPRICE ONLINE'
    ELSE null
END AS business_unit_desc
FROM PRD_NAP_USR_VWS.ORDER_LINE_DETAIL_FACT
WHERE 1=1
AND order_date_pacific BETWEEN ({start_date} - 365) AND {end_date} --need to extend start date to account for lag BETWEEN order AND sale 
--and order_date_pacific BETWEEN cast('2019-02-04' as date) - interval '365' day and '2022-02-06' --for testing code
--and order_date_pacific IN ('2019-02-04', '2020-02-04', '2021-02-04', '2021-07-04', '2022-02-06', '2021-11-11') --for testing code 
AND ((business_unit_desc IN ('N.COM','N.CA') AND COALESCE(fraud_cancel_ind,'N')='N')  --ncom orders include cancels AND exclude fraud
       OR (business_unit_desc = 'OFFPRICE ONLINE' AND order_date_pacific >= '2020-11-21'))   --rcom orders include cancels AND fraud
AND COALESCE(beauty_sample_ind,'N')='N'
AND COALESCE(gift_with_purchase_ind,'N')='N'
AND order_line_amount > 0  --Exclude fragrance AND beauty samples
--add this to remove order lines which erroneously come through WITH platform = UNKNOWN but the rest of the lines are known
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_num ORDER BY source_platform_code ASC) =1
)
WITH DATA PRIMARY INDEX(order_num) ON COMMIT PRESERVE ROWS;--, order_line_num


COLLECT STATISTICS COLUMN (ent_cust_id) ON pre_oldf ;
COLLECT STATISTICS COLUMN (order_num) ON pre_oldf;
COLLECT STATISTICS COLUMN (source_platform_code) ON pre_oldf;
COLLECT STATISTICS COLUMN (order_date_pacific) ON pre_oldf;


/* step 8
get only OLDF WHERE ent_cust_id is not null to make join to aacx easier to get acp_id
*/
CREATE MULTISET VOLATILE TABLE oldf_not_null AS (
SELECT 
pre_oldf.order_num, source_platform_code, order_date_pacific, order_line_currency_code, source_store_num, business_unit_desc, ent_cust_id
FROM pre_oldf
WHERE ent_cust_id IS NOT NULL
)
WITH DATA PRIMARY INDEX(order_num) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (order_num) ON oldf_not_null;
COLLECT STATISTICS COLUMN (ent_cust_id) ON oldf_not_null;


/* step 9                   
get only OLDF WHERE ent_cust_id is null for join to rtdf to get acp_id
*/
CREATE MULTISET VOLATILE TABLE oldf_null AS (
SELECT 
pre_oldf.order_num, source_platform_code, order_date_pacific, order_line_currency_code, source_store_num, business_unit_desc, ent_cust_id, shopper_id
FROM pre_oldf
WHERE ent_cust_id IS NULL
)
WITH DATA PRIMARY INDEX(shopper_id,order_num) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (order_date_pacific) ON oldf_null;
COLLECT STATISTICS COLUMN (shopper_id) ON oldf_null;
COLLECT STATISTICS COLUMN (shopper_id,order_num) ON oldf_null;


/* step 10
cut down PRD_NAP_USR_VWS.ACP_ANALYTICAL_CUST_XREF to make joins later easier 
*/
CREATE MULTISET VOLATILE TABLE aacx AS (
SELECT acp_id, cust_id 
FROM PRD_NAP_USR_VWS.ACP_ANALYTICAL_CUST_XREF 
WHERE cust_source = 'ICON' 
AND cust_id IN (SELECT DISTINCT ent_cust_id FROM pre_oldf) --try to reduce size for troublesome join later
QUALIFY Row_Number() OVER (PARTITION BY cust_id ORDER BY dw_batch_date DESC) = 1 
--some cust_id's have more than one acp_id, so pick the most recently loaded one to avoid duplication
)
WITH DATA PRIMARY INDEX(cust_id) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (cust_id) ON aacx ;


/* step 11
add acp_id to oldf FROM aacx
*/
CREATE MULTISET VOLATILE TABLE oldf_not_null_acp AS (
SELECT 
oldf_not_null.order_num, source_platform_code, order_date_pacific, order_line_currency_code, source_store_num, business_unit_desc, aacx.acp_id AS aacx_acp_id
FROM oldf_not_null
LEFT JOIN aacx ON oldf_not_null.ent_cust_id = aacx.cust_id
)
WITH DATA PRIMARY INDEX(order_num) ON COMMIT PRESERVE ROWS;


/* step 12
cut down aapx to get a single record per acp_id
*/
CREATE MULTISET VOLATILE TABLE aapx_acp AS (
SELECT 
DISTINCT program_index_id, acp_id 
FROM PRD_NAP_USR_VWS.ACP_ANALYTICAL_PROGRAM_XREF AS aapx
QUALIFY Row_Number() OVER (PARTITION BY acp_id ORDER BY program_index_id DESC) = 1 
WHERE acp_id IS NOT NULL 
AND program_index_id IS NOT NULL
AND program_name IN ('WEB') --removed ON filter
)
WITH DATA PRIMARY INDEX(program_index_id) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (program_index_id) ON aapx_acp ;


/* step 13
add acp_id to oldf FROM aapx (before '2020-02-12' oldf was missing ent_cust_id)
*/
CREATE MULTISET VOLATILE TABLE oldf_null_acp AS (
SELECT 
oldf_null.order_num, source_platform_code, order_date_pacific, order_line_currency_code, source_store_num, business_unit_desc, acp_id AS shopper_acp_id --, program_name
FROM oldf_null 
LEFT JOIN aapx_acp
ON oldf_null.shopper_id = aapx_acp.program_index_id AND order_date_pacific < '2020-02-12' 
)
WITH DATA PRIMARY INDEX(order_num) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (order_num) ON oldf_null_acp;


/* step 14
combine oldf_null_acp AND oldf_not_null_acp
*/
CREATE MULTISET VOLATILE TABLE oldf_acp AS (
SELECT 
oldf_null_acp.order_num, source_platform_code, order_date_pacific, order_line_currency_code, source_store_num, business_unit_desc, shopper_acp_id AS oldf_acp_id
FROM oldf_null_acp
	UNION ALL
SELECT 
oldf_not_null_acp.order_num, source_platform_code, order_date_pacific, order_line_currency_code, source_store_num, business_unit_desc, aacx_acp_id  AS oldf_acp_id
FROM oldf_not_null_acp
)
WITH DATA PRIMARY INDEX(order_num) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (oldf_acp_id) ON oldf_acp;
COLLECT STATISTICS COLUMN (order_num) ON oldf_acp;


/* step 15
get only oldf_acp WHERE acp_id is not null to make join to customer variables easier (no NULLs IN index)
*/
CREATE MULTISET VOLATILE TABLE oldf_acp_not_null AS (
SELECT 
order_num, source_platform_code, order_date_pacific, order_line_currency_code, source_store_num, business_unit_desc, oldf_acp_id AS acp_id
FROM oldf_acp
WHERE acp_id IS NOT NULL
)
WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (acp_id) ON oldf_acp_not_null;
COLLECT STATISTICS COLUMN (order_date_pacific) ON oldf_acp_not_null;

/* step 16                  
get only oldf_acp WHERE acp_id is null for join to rtdf to get acp_id
*/
CREATE MULTISET VOLATILE TABLE oldf_acp_null AS (
 SELECT 
order_num, source_platform_code, order_date_pacific, order_line_currency_code, source_store_num, business_unit_desc
FROM oldf_acp
WHERE oldf_acp_id IS NULL
)
WITH DATA PRIMARY INDEX(order_num) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (order_num) ON oldf_acp_null;


/* step 17
add customer data to oldf using acp_id
*/
CREATE MULTISET VOLATILE TABLE oldf_cust AS (
SELECT 
order_num, source_platform_code, order_date_pacific, order_line_currency_code, source_store_num, business_unit_desc--, cts_segment, rfm_4year_segment AS rfm_segment
    , rewards_level
FROM oldf_acp_not_null AS oldf
    --LEFT JOIN segment ON segment.acp_id = oldf.acp_id AND oldf.order_date_pacific >= segment.scored_date_start AND oldf.order_date_pacific <= segment.scored_date_end
    --LEFT JOIN T2DL_DAS_CAL.CUSTOMER_ATTRIBUTES_TRANSACTIONS AS cat ON cat.acp_id = oldf.acp_id  --for rfm segment
    LEFT JOIN loy ON loy.acp_id = oldf.acp_id AND oldf.order_date_pacific <= loy.end_day_date_LEAD AND oldf.order_date_pacific >= loy.start_day_date
)
WITH DATA PRIMARY INDEX(order_num) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (order_num) ON oldf_cust;

 
/* step 18
combine oldf_acp_null and oldf_cust
*/
CREATE MULTISET VOLATILE TABLE oldf_cust_all AS (
 SELECT 
order_num, source_platform_code, order_date_pacific, order_line_currency_code, source_store_num, business_unit_desc--, cts_segment, rfm_segment
    , rewards_level
FROM oldf_cust
	UNION ALL
SELECT 
order_num, source_platform_code, order_date_pacific, order_line_currency_code, source_store_num, business_unit_desc--, CAST(NULL AS VARCHAR(40)) AS cts_segment, CAST(NULL AS VARCHAR(40)) AS rfm_segment
    , CAST(NULL AS VARCHAR(40)) AS rewards_level
FROM oldf_acp_null
)
WITH DATA PRIMARY INDEX(order_num) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (order_num) ON oldf_cust_all;
COLLECT STATISTICS COLUMN (order_date_pacific) ON oldf_cust_all;
COLLECT STATISTICS COLUMN (source_platform_code) ON oldf_cust_all;
    

/* step 19
-- prep for joining rtdf to oldf
*/
-- CREATE MULTISET VOLATILE TABLE rtdf_pre_oldf AS (
-- SELECT rtdf.business_day_date, rtdf.global_tran_id
-- , line_item_activity_type_desc, acp_id, line_item_net_amt_currency_code, original_line_item_amt_currency_code, ringing_store_num, intent_store_num, pt_join.order_num
-- FROM  rtdf
-- LEFT JOIN pt_join ON pt_join.business_day_date = rtdf.business_day_date AND pt_join.global_tran_id = rtdf.global_tran_id AND pt_join.line_item_seq_num = rtdf.line_item_seq_num --and oldf.cancel_reason_code is null
-- WHERE 1=1
--   AND intent_store_num NOT IN (141, 173)-- first filter to exclude trunk club (trunk club needs validation)- second filter is IN sales query
--  )
-- WITH DATA NO PRIMARY INDEX ON COMMIT PRESERVE ROWS; --optimized by TD team
--
--
-- COLLECT STATISTICS COLUMN (acp_id,order_num,business_day_date) ON rtdf_pre_oldf;
--
--
-- /* step 20
-- -- join rtdf to oldf to get platform
-- */
-- CREATE MULTISET VOLATILE TABLE rtdf_oldf AS (
-- SELECT rtdf.business_day_date, rtdf.order_num, rtdf.global_tran_id
-- , line_item_activity_type_desc, acp_id, line_item_net_amt_currency_code, original_line_item_amt_currency_code, ringing_store_num, intent_store_num, order_date_pacific, source_platform_code
-- FROM  rtdf_pre_oldf AS rtdf
-- LEFT JOIN pre_oldf AS oldf
--   ON CASE WHEN  rtdf.order_num IS NULL THEN RANDOM(-1000000,-20) ELSE rtdf.order_num END = oldf.order_num --optimized by TD team
--  )
-- WITH DATA PRIMARY INDEX(acp_id,order_num,business_day_date) ON COMMIT PRESERVE ROWS;
--
--
-- COLLECT STATISTICS COLUMN (acp_id,order_num,business_day_date) ON rtdf_oldf;


/* step 21
-- join cts_segment to sales
*/
-- CREATE MULTISET VOLATILE TABLE sales_rtdf_segment AS (
--  SELECT business_day_date, order_num, global_tran_id, line_item_activity_type_desc, rtdf.acp_id, line_item_net_amt_currency_code, original_line_item_amt_currency_code, ringing_store_num, intent_store_num, order_date_pacific
-- , source_platform_code, cts_segment
-- FROM rtdf_oldf rtdf
-- LEFT JOIN segment
-- ON segment.acp_id = rtdf.acp_id AND rtdf.business_day_date >= segment.scored_date_start AND rtdf.business_day_date <= segment.scored_date_end
-- )
-- WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;
--
--
-- COLLECT STATISTICS COLUMN (acp_id) ON sales_rtdf_segment;
--
--
-- /* step 22
-- -- join rfm_segment to sales
-- */
-- CREATE MULTISET VOLATILE TABLE sales_rtdf_segment2 AS (
--  SELECT business_day_date, order_num, global_tran_id, line_item_activity_type_desc, rtdf.acp_id, line_item_net_amt_currency_code, original_line_item_amt_currency_code, ringing_store_num, intent_store_num, order_date_pacific
-- , source_platform_code, cts_segment, rfm_4year_segment AS rfm_segment
-- FROM sales_rtdf_segment rtdf
-- LEFT JOIN T2DL_DAS_CAL.CUSTOMER_ATTRIBUTES_TRANSACTIONS AS cat ON cat.acp_id = rtdf.acp_id  --for rfm segment
-- )
-- WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;
--
--
-- COLLECT STATISTICS COLUMN (acp_id) ON sales_rtdf_segment2;
-- COLLECT STATISTICS COLUMN (business_day_date) ON  sales_rtdf_segment2;


/* step 23
-- join loyalty to sales
*/
-- CREATE MULTISET VOLATILE TABLE sales_rtdf_segment_loy AS (
--  SELECT business_day_date, order_num, global_tran_id, line_item_activity_type_desc, rtdf_seg.acp_id, line_item_net_amt_currency_code, original_line_item_amt_currency_code, ringing_store_num, intent_store_num, order_date_pacific
-- , source_platform_code--, cts_segment, rfm_segment
--     , rewards_level
-- FROM rtdf_oldf as rtdf_seg --sales_rtdf_segment2
-- LEFT JOIN loy
-- ON loy.acp_id = rtdf_seg.acp_id AND rtdf_seg.business_day_date <= loy.end_day_date_lead AND rtdf_seg.business_day_date >= loy.start_day_date
-- )
-- WITH DATA PRIMARY INDEX(global_tran_id, business_day_date) ON COMMIT PRESERVE ROWS;
--
--
-- COLLECT STATISTICS COLUMN (business_day_date, source_platform_code, rewards_level) ON sales_rtdf_segment_loy; --cts_segment, rfm_segment,
-- COLLECT STATISTICS COLUMN (business_day_date, global_tran_id) ON sales_rtdf_segment_loy;
-- COLLECT STATISTICS COLUMN (intent_store_num) ON  sales_rtdf_segment_loy;


/* step 24
-- cut down sessions table to make join easier
*/
/*
CREATE MULTISET VOLATILE TABLE pre_sessions AS (
 SELECT activity_date_pacific AS tran_date
--, CAST(EXTRACT(HOUR FROM session_starttime_utc at time zone 'america pacific') AS varchar(15)) AS hour_var
, CASE
    WHEN channelcountry = 'US' AND channel = 'NORDSTROM' THEN 'N.COM'
    WHEN channelcountry = 'CA' AND channel = 'NORDSTROM' THEN 'N.CA'
    WHEN channelcountry = 'US' AND channel IN ('NORDSTROM_RACK', 'HAUTELOOK') THEN 'OFFPRICE ONLINE'
    ELSE channel END AS business_unit_desc
, 'NAP_ERTM' AS data_source
, CASE WHEN channelcountry = 'US' THEN 'USD'
    WHEN channelcountry = 'CA' THEN 'CAD'
    ELSE NULL END AS currency_code
, CASE WHEN business_unit_desc = 'N.COM' THEN 808
	WHEN business_unit_desc = 'N.CA' THEN 867
	WHEN business_unit_desc = 'OFFPRICE ONLINE' THEN 828
	ELSE NULL END AS store_num
, session_id, product_views, cart_adds
 , CASE WHEN experience = 'CARE_PHONE' THEN 'CSR_PHONE'
	WHEN experience = 'ANDROID_APP' THEN 'ANDROID'
	WHEN experience = 'MOBILE_WEB' THEN 'MOW'
	WHEN experience = 'DESKTOP_WEB' THEN 'WEB'
	WHEN experience = 'POINT_OF_SALE' THEN 'POS'
	WHEN experience = 'IOS_APP' THEN 'IOS'
	WHEN experience = 'VENDOR' THEN 'THIRD_PARTY_VENDOR'
	ELSE experience END AS platform
FROM PRD_NAP_USR_VWS.CUSTOMER_SESSION_FACT as csf
WHERE csf.activity_date_pacific >= '2022-01-31' --first date of complete sessions data. Adding sessions is empty until 2022-02-09
--and csf.activity_date_pacific = '2022-04-01' --for testing code
AND csf.activity_date_pacific BETWEEN {start_date} AND {end_date}
and business_unit_desc NOT IN ('TRUNK_CLUB')
and platform <> 'POS'
)
WITH DATA PRIMARY INDEX(tran_date, session_id) ON COMMIT PRESERVE ROWS;
 

COLLECT STATISTICS COLUMN (tran_date, session_id) ON pre_sessions;
*/

/* step 25
-- join sessions to acp_id

CREATE MULTISET VOLATILE TABLE sessions_acp AS (
select
tran_date, business_unit_desc, 'NAP_ERTM' AS data_source
, currency_code, store_num, platform, csf.session_id, product_views, cart_adds, acp_id
FROM pre_sessions as csf
LEFT JOIN session_xref AS csx ON csf.session_id = csx.session_id AND csf.tran_date = csx.activity_date_pacific 
)
WITH DATA PRIMARY INDEX(tran_date, acp_id) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (tran_date, acp_id) ON sessions_acp;
*/

/* step 26
-- split out null acp_id to make joins easier

CREATE MULTISET VOLATILE TABLE sessions_null AS (
select
tran_date, business_unit_desc, data_source, currency_code, store_num, platform, session_id, product_views, cart_adds, acp_id
FROM sessions_acp
where acp_id is null
)
WITH DATA PRIMARY INDEX(tran_date) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (tran_date) ON sessions_null;
*/

/* step 27
-- split out not-null acp_id to make joins easier

CREATE MULTISET VOLATILE TABLE sessions_not_null AS (
select
tran_date, business_unit_desc, data_source, currency_code, store_num, platform, session_id, product_views, cart_adds, acp_id
FROM sessions_acp
where acp_id is not null
)
WITH DATA PRIMARY INDEX(tran_date, acp_id) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (tran_date, acp_id) ON sessions_not_null;
*/

/* step 28
-- join cts_segment to sessions

CREATE MULTISET VOLATILE TABLE sessions_segment AS (
 SELECT tran_date, business_unit_desc, data_source, currency_code, store_num, platform, session_id, product_views, cart_adds, csf.acp_id, cts_segment
FROM sessions_not_null as csf
LEFT JOIN segment ON segment.acp_id = csf.acp_id AND csf.tran_date >= segment.scored_date_start AND csf.tran_date <= segment.scored_date_end
)
WITH DATA PRIMARY INDEX(tran_date, acp_id) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (tran_date, acp_id) ON sessions_segment;
*/

/* step 29
-- join rfm_segment to sessions

CREATE MULTISET VOLATILE TABLE sessions_segment2 AS (
 SELECT tran_date, business_unit_desc, data_source, currency_code, store_num, platform, session_id, product_views, cart_adds, csf.acp_id, cts_segment, rfm_4year_segment AS rfm_segment
FROM sessions_segment as csf
LEFT JOIN T2DL_DAS_CAL.CUSTOMER_ATTRIBUTES_TRANSACTIONS AS cat ON cat.acp_id = csf.acp_id  --for rfm segment
)
WITH DATA PRIMARY INDEX(tran_date, acp_id) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (tran_date, acp_id) ON sessions_segment2;
*/

/* step 30
-- join loyalty to sessions

CREATE MULTISET VOLATILE TABLE sessions_segment_loy AS (
 SELECT tran_date, business_unit_desc, data_source, currency_code, store_num, platform, session_id, product_views, cart_adds, cts_segment, rfm_segment, rewards_level	
FROM sessions_segment2 as csf
LEFT JOIN loy ON loy.acp_id = csf.acp_id AND csf.tran_date >= loy.start_day_date  AND csf.tran_date <= loy.end_day_date_lead
)
WITH DATA PRIMARY INDEX(tran_date) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (tran_date) ON sessions_segment_loy;
*/

/* step 31
combine sessions with null and not_null acp_id

CREATE MULTISET VOLATILE TABLE sessions_all AS (
 SELECT 
tran_date, business_unit_desc, data_source, currency_code, store_num, platform, session_id, product_views, cart_adds, cts_segment, rfm_segment, rewards_level
FROM sessions_segment_loy
	UNION ALL
SELECT 
tran_date, business_unit_desc, data_source, currency_code, store_num, platform, session_id, product_views, cart_adds
, CAST(NULL AS VARCHAR(40)) AS cts_segment, CAST(NULL AS VARCHAR(40)) AS rfm_segment, CAST(NULL AS VARCHAR(40)) AS rewards_level
FROM sessions_null
)
WITH DATA PRIMARY INDEX(tran_date) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (tran_date, session_id) ON sessions_all;
*/


DELETE FROM {mothership_t2_schema}.SALES_DEMAND_ORDERS_TRAFFIC_FACT WHERE tran_date BETWEEN {start_date} and {end_date};


/* step 32
final insert statement
*/
INSERT INTO {mothership_t2_schema}.SALES_DEMAND_ORDERS_TRAFFIC_FACT
WITH sales_demand AS ( --reassign bopus to store or digital
SELECT 
tran_date
--, CAST(hour_var AS varchar(15)) AS hour_var
, business_unit_desc_bopus_in_store_demand AS business_unit_desc
, CAST(data_source AS VARCHAR(20)) AS data_source
, currency_code
, store_num_bopus_in_store_demand AS store_num
, cts_segment
, rfm_segment
, rewards_level	
, platform
, SUM(CASE WHEN customer_journey <> 'BOPUS' THEN demand_amt_bopus_in_store ELSE 0 END) AS demand_amt_excl_bopus
, SUM(CASE WHEN customer_journey = 'BOPUS' THEN demand_amt_bopus_in_store ELSE 0 END) AS bopus_attr_store_amt
, CAST(0.00 AS decimal(38,2)) AS bopus_attr_digital_amt
, SUM(demand_canceled_amt) AS demand_canceled_amt
, SUM(CASE WHEN customer_journey <> 'BOPUS' THEN demand_units_bopus_in_store ELSE 0 END) AS demand_units_excl_bopus
, SUM(CASE WHEN customer_journey = 'BOPUS' THEN demand_units_bopus_in_store ELSE 0 END) AS bopus_attr_store_units
, CAST(0 AS bigint) AS bopus_attr_digital_units
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
FROM {mothership_t2_schema}.finance_sales_demand_fact fsdf 
WHERE 1=1 
and business_unit_desc IN ('FULL LINE', 'FULL LINE CANADA', 'RACK', 'RACK CANADA')
GROUP BY 1,2,3,4,5,6,7,8,9
	UNION ALL	
SELECT 
tran_date
--, CAST(hour_var AS varchar(15)) AS hour_var
, business_unit_desc_bopus_in_digital_demand AS business_unit_desc
, CAST(data_source AS VARCHAR(20)) AS data_source
, currency_code
, store_num_bopus_in_digital_demand AS store_num
, cts_segment
, rfm_segment
, rewards_level	
, platform
, SUM(CASE WHEN COALESCE(customer_journey,0) <> 'BOPUS' THEN CAST(demand_amt_bopus_in_digital AS decimal(38,2)) ELSE 0 END) AS demand_amt_excl_bopus
, CAST(0.00 AS decimal(38,2)) AS bopus_attr_store_amt
, SUM(CASE WHEN COALESCE(customer_journey,0) = 'BOPUS' THEN CAST(demand_amt_bopus_in_digital AS decimal(38,2)) ELSE 0 END) AS bopus_attr_digital_amt
, SUM(demand_canceled_amt) AS demand_canceled_amt
, SUM(CASE WHEN customer_journey <> 'BOPUS' THEN demand_units_bopus_in_digital ELSE 0 END) AS demand_units_excl_bopus
, CAST(0 AS bigint) AS bopus_attr_store_units
, SUM(CASE WHEN customer_journey = 'BOPUS' THEN demand_units_bopus_in_digital ELSE 0 END) AS bopus_attr_digital_units
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
FROM {mothership_t2_schema}.finance_sales_demand_fact fsdf 
WHERE 1=1 
and business_unit_desc IN ('OFFPRICE ONLINE', 'N.COM', 'N.CA')
GROUP BY 1,2,3,4,5,6,7,8,9
)
, bot_restatement as
(
    select
        day_date,
        case when box = 'rcom' then 'OFFPRICE ONLINE' when box = 'ncom' then 'N.COM' else null end as business_unit,
        device_type,
        sum(suspicious_visitors) as suspicious_visitors,
        sum(suspicious_viewing_visitors) as suspicious_viewing_visitors,
        sum(suspicious_adding_visitors) as suspicious_adding_visitors
        
	from
	    {mothership_t2_schema}.bot_traffic_fix
	group by 1,2,3
)
, combo AS ( --reassign bopus to store AND digital
SELECT 
tran_date
--, hour_var
, business_unit_desc
, CAST(data_source AS VARCHAR(20)) AS data_source
, currency_code
, store_num
, rewards_level	
, platform
, SUM(demand_amt_excl_bopus) AS demand_amt_excl_bopus
, SUM(bopus_attr_store_amt) AS bopus_attr_store_amt
, SUM(bopus_attr_digital_amt) AS bopus_attr_digital_amt
, SUM(COALESCE(demand_canceled_amt,0)) AS demand_canceled_amt
, SUM(demand_units_excl_bopus) AS demand_units_excl_bopus
, SUM(bopus_attr_store_units) AS bopus_attr_store_units
, SUM(bopus_attr_digital_units) AS bopus_attr_digital_units
, SUM(COALESCE(demand_canceled_units,0)) AS demand_canceled_units
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
, NULL AS orders
, NULL AS visitors     
, NULL AS viewing_visitors 
, NULL AS adding_visitors  
, NULL AS ordering_visitors
, NULL AS sessions
, NULL AS viewing_sessions 
, NULL AS adding_sessions
, NULL AS ordering_sessions
, NULL AS store_purchase_trips 
, NULL AS store_traffic
, NULL AS ordering_visitors_or_store_purchase_trips 
, NULL AS sessions_or_store_traffic
, NULL AS transactions_total
, NULL AS transactions_purchase
, NULL AS transactions_return
, NULL AS visits_total
, NULL AS visits_purchase
, NULL AS visits_return
FROM sales_demand
GROUP BY 1,2,3,4,5,6,7
	UNION ALL
SELECT  --add orders
order_date_pacific AS tran_date
--, CAST(EXTRACT(HOUR FROM ORDER_TMSTP_PACIFIC) AS varchar(15)) AS hour_var
, business_unit_desc
, 'NAP_ERTM' AS data_source
, order_line_currency_code AS currency_code 
, CASE WHEN source_platform_code = 'POS' THEN source_store_num
	WHEN business_unit_desc = 'N.COM' THEN 808
	WHEN business_unit_desc = 'N.CA' THEN 867
	WHEN business_unit_desc = 'OFFPRICE ONLINE' THEN 828
	ELSE NULL END AS store_num
, rewards_level	
, CASE WHEN business_unit_desc IN ('N.COM','N.CA') THEN source_platform_code
	WHEN business_unit_desc = 'OFFPRICE ONLINE' AND order_date_pacific >= '2021-03-06' THEN source_platform_code 
	ELSE 'total_nrhl' END AS platform --before 2021-03-06 OLDF isn't accurate at device level for r.com
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
, COUNT(DISTINCT(order_num)) AS orders
, NULL AS visitors     
, NULL AS viewing_visitors 
, NULL AS adding_visitors  
, NULL AS ordering_visitors
, NULL AS sessions
, NULL AS viewing_sessions 
, NULL AS adding_sessions
, NULL AS ordering_sessions
, NULL AS store_purchase_trips 
, NULL AS store_traffic
, NULL AS ordering_visitors_or_store_purchase_trips 
, NULL AS sessions_or_store_traffic
, NULL AS transactions_total
, NULL AS transactions_purchase
, NULL AS transactions_return
, NULL AS visits_total
, NULL AS visits_purchase
, NULL AS visits_return
FROM oldf_cust_all
WHERE 1=1
--and order_date_pacific IN ('2019-02-04', '2020-02-04', '2021-02-04', '2021-07-04', '2022-02-06', '2021-11-11') --for testing code 
AND order_date_pacific BETWEEN {start_date} AND {end_date} 
GROUP BY 1,2,3,4,5,6,7
--	UNION ALL
-- SELECT --add transaction metrics (roughly align WITH buyerflow definitions)
-- rtdf.business_day_date AS tran_date
-- 	--,CAST(EXTRACT(HOUR FROM rtdf.tran_time) AS varchar(15)) AS hour_var
-- 	,st.business_unit_desc
-- 	, 'NAP_ERTM' AS data_source
-- 	,CASE WHEN rtdf.line_item_net_amt_currency_code <> rtdf.original_line_item_amt_currency_code AND rtdf.original_line_item_amt_currency_code IS NOT NULL
-- 		THEN rtdf.original_line_item_amt_currency_code ELSE rtdf.line_item_net_amt_currency_code END AS currency_code 	--change currency code to account for cross border returns
-- 	,intent_store_num AS store_num
-- 	, cts_segment
-- 	, rfm_segment
-- 	, rewards_level
-- 	, CASE WHEN st.business_unit_desc IN ('N.COM','N.CA') THEN source_platform_code
-- 		WHEN st.business_unit_desc = 'OFFPRICE ONLINE' AND order_date_pacific >= '2021-03-06' THEN source_platform_code
-- 		ELSE 'total_nrhl' END AS platform --before 2021-03-06 OLDF isn't accurate at device level for r.com
-- 	, NULL AS demand_amt_excl_bopus
-- 	, NULL AS bopus_attr_store_amt
-- 	, NULL AS bopus_attr_digital_amt
-- 	, NULL AS demand_canceled_amt
-- 	, NULL AS demand_units_excl_bopus
-- 	, NULL AS bopus_attr_store_units
-- 	, NULL AS bopus_attr_digital_units
-- 	, NULL AS demand_canceled_units
-- 	, NULL AS gross_merch_sales_amt
-- 	, NULL AS merch_returns_amt
-- 	, NULL AS net_merch_sales_amt
-- 	, NULL AS gross_operational_sales_amt
-- 	, NULL AS operational_returns_amt
-- 	, NULL AS net_operational_sales_amt
-- 	, NULL AS gross_merch_sales_units
-- 	, NULL AS merch_returns_units
-- 	, NULL AS net_merch_sales_units
-- 	, NULL AS gross_operational_sales_units
-- 	, NULL AS operational_returns_units
-- 	, NULL AS net_operational_sales_units
-- 	, NULL AS orders
-- 	, NULL AS visitors
-- 	, NULL AS viewing_visitors
-- 	, NULL AS adding_visitors
-- 	, NULL AS ordering_visitors
-- 	, NULL AS sessions
-- 	, NULL AS viewing_sessions
-- 	, NULL AS adding_sessions
--   , NULL AS ordering_sessions
-- 	, NULL AS store_purchase_trips
-- 	, NULL AS store_traffic
-- 	, NULL AS ordering_visitors_or_store_purchase_trips
-- 	, NULL AS sessions_or_store_traffic
-- 	--below 3 are different than grant/buyerflow logic because finance focuses ON transaction date, NOT order date
-- 	, COUNT(DISTINCT CASE WHEN rtdf.line_item_activity_type_desc = 'SALE' THEN COALESCE(rtdf.order_num,rtdf.global_tran_id) WHEN rtdf.line_item_activity_type_desc = 'RETURN' THEN rtdf.global_tran_id ELSE null END) AS transactions_total
--     , COUNT(DISTINCT CASE WHEN rtdf.line_item_activity_type_desc = 'SALE' THEN COALESCE(rtdf.order_num,rtdf.global_tran_id) ELSE null END) AS transactions_purchase
--     , COUNT(DISTINCT CASE WHEN rtdf.line_item_activity_type_desc = 'RETURN' THEN rtdf.global_tran_id ELSE null END) AS transactions_return
-- 	, NULL AS visits_total
-- 	, NULL AS visits_purchase
-- 	, NULL AS visits_return
-- FROM sales_rtdf_segment_loy as rtdf
-- INNER JOIN PRD_NAP_USR_VWS.STORE_DIM AS st ON st.store_num = rtdf.intent_store_num
-- WHERE 1=1
-- --AND rtdf.business_day_date IN ('2019-02-04', '2020-02-04', '2021-02-04', '2022-02-06') --for testing code
-- AND rtdf.business_day_date BETWEEN {start_date} AND {end_date}
-- and st.business_unit_desc <> ('TRUNK CLUB')
-- --and intent_store_num NOT IN (141, 173)-- both filters are required to exclude trunk club (trunk club needs validation)
-- GROUP BY 1,2,3,4,5,6,7,8,9
-- 	UNION ALL
-- SELECT --add visit metrics (roughly align WITH buyerflow definitions)
-- rtdf.business_day_date AS tran_date
-- --,CAST(EXTRACT(HOUR FROM rtdf.tran_time) AS varchar(15)) AS hour_var
-- 	,st.business_unit_desc
-- 	, 'NAP_ERTM' AS data_source
-- 	,CASE WHEN rtdf.line_item_net_amt_currency_code <> rtdf.original_line_item_amt_currency_code AND rtdf.original_line_item_amt_currency_code IS NOT NULL
-- 		THEN rtdf.original_line_item_amt_currency_code ELSE rtdf.line_item_net_amt_currency_code END AS currency_code 	--change currency code to account for cross border returns
-- 	,CASE WHEN line_item_activity_type_desc = 'RETURN' THEN ringing_store_num
-- 		WHEN line_item_activity_type_desc = 'SALE' THEN intent_store_num
-- 		ELSE NULL END AS store_num
-- 	, cts_segment
-- 	, rfm_segment
-- 	, rewards_level
-- 	, 'total' AS platform --can't split by device because same acp_id can have multiple orders ON multiple devices AND inflate the total visit count
-- 	, NULL AS demand_amt_excl_bopus
-- 	, NULL AS bopus_attr_store_amt
-- 	, NULL AS bopus_attr_digital_amt
-- 	, NULL AS demand_canceled_amt
-- 	, NULL AS demand_units_excl_bopus
-- 	, NULL AS bopus_attr_store_units
-- 	, NULL AS bopus_attr_digital_units
-- 	, NULL AS demand_canceled_units
-- 	, NULL AS gross_merch_sales_amt
-- 	, NULL AS merch_returns_amt
-- 	, NULL AS net_merch_sales_amt
-- 	, NULL AS gross_operational_sales_amt
-- 	, NULL AS operational_returns_amt
-- 	, NULL AS net_operational_sales_amt
-- 	, NULL AS gross_merch_sales_units
-- 	, NULL AS merch_returns_units
-- 	, NULL AS net_merch_sales_units
-- 	, NULL AS gross_operational_sales_units
-- 	, NULL AS operational_returns_units
-- 	, NULL AS net_operational_sales_units
-- 	, NULL AS orders
-- 	, NULL AS visitors
-- 	, NULL AS viewing_visitors
-- 	, NULL AS adding_visitors
-- 	, NULL AS ordering_visitors
-- 	, NULL AS sessions
-- 	, NULL AS viewing_sessions
-- 	, NULL AS adding_sessions
--   , NULL AS ordering_sessions
-- 	, NULL AS store_purchase_trips
-- 	, NULL AS store_traffic
-- 	, NULL AS ordering_visitors_or_store_purchase_trips
-- 	, NULL AS sessions_or_store_traffic
-- 	, NULL AS transactions_total
-- 	, NULL AS transactions_purchase
-- 	, NULL AS transactions_return
-- 	--below 3 are different than grant/buyerflow logic because finance focuses ON transaction date, NOT order date
--     , COUNT(DISTINCT CASE WHEN rtdf.line_item_activity_type_desc = 'SALE' THEN rtdf.acp_id WHEN rtdf.line_item_activity_type_desc = 'RETURN' THEN rtdf.acp_id ELSE null END) AS visits_total
--     , COUNT(DISTINCT CASE WHEN rtdf.line_item_activity_type_desc = 'SALE' THEN rtdf.acp_id ELSE null END) AS visits_purchase
--     , COUNT(DISTINCT CASE WHEN rtdf.line_item_activity_type_desc = 'RETURN' THEN rtdf.acp_id ELSE null END) AS visits_return
-- FROM sales_rtdf_segment_loy as rtdf
-- INNER JOIN PRD_NAP_USR_VWS.STORE_DIM AS st ON st.store_num = rtdf.intent_store_num
-- WHERE 1=1
-- --AND rtdf.business_day_date IN ('2019-02-04', '2020-02-04', '2021-02-04', '2022-02-06') --for testing code
-- AND rtdf.business_day_date BETWEEN {start_date} AND {end_date}
-- and st.business_unit_desc <> ('TRUNK CLUB')
-- --and intent_store_num NOT IN (141, 173)-- both filters are required to exclude trunk club (trunk club needs validation)
-- GROUP BY 1,2,3,4,5,6,7,8,9
 	UNION ALL
--add r.com historical orders data (pre project rocket)- 
--before 2021-03-06 OLDF isn't accurate at device level for r.com (a lot of "unknown") but we have no data that's better so oh well I guess
SELECT 
CAST(day_date AS date) AS tran_date
--, CAST(hour_id AS varchar(15)) AS hour_var 
, 'OFFPRICE ONLINE' AS business_unit_desc
, 'NRHL_redshift' AS data_source
, 'USD' AS currency_code
, 828 AS store_num
, 'total_nrhl' AS rewards_level	
, 'total_nrhl' AS platform
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
, CAST(orders AS decimal(38,0)) AS orders --incl cancels AND fraud
, NULL AS visitors     
, NULL AS viewing_visitors 
, NULL AS adding_visitors  
, NULL AS ordering_visitors
, NULL AS sessions
, NULL AS viewing_sessions 
, NULL AS adding_sessions
, NULL AS ordering_sessions
, NULL AS store_purchase_trips 
, NULL AS store_traffic
, NULL AS ordering_visitors_or_store_purchase_trips 
, NULL AS sessions_or_store_traffic
, NULL AS transactions_total
, NULL AS transactions_purchase
, NULL AS transactions_return
, NULL AS visits_total
, NULL AS visits_purchase
, NULL AS visits_return
FROM T2DL_DAS_MOTHERSHIP.nrhl_history_redshift_birdseye_hourly --historical data FROM T3DL_ACE_OP.birdseye_hourly 
WHERE day_date BETWEEN '2019-02-03' AND '2020-11-20'
AND day_date BETWEEN {start_date} AND {end_date} --so historical doesn't run in daily job
	UNION ALL		--add funnel
SELECT 
activity_date AS tran_date
--, 'total_digital' AS hour_var
, CASE
    WHEN CHANNEL_COUNTRY = 'US' AND purchase_channel = 'FULL_LINE' THEN 'N.COM'
    WHEN CHANNEL_COUNTRY = 'CA' AND purchase_channel = 'FULL_LINE' THEN 'N.CA'
    WHEN CHANNEL_COUNTRY = 'US' AND purchase_channel = 'RACK' THEN 'OFFPRICE ONLINE'
    ELSE null
END AS business_unit_desc
, 'NAP_ERTM' AS data_source
, CASE WHEN CHANNEL_COUNTRY = 'US' THEN 'USD'
    WHEN CHANNEL_COUNTRY = 'CA' THEN 'CAD'
    ELSE NULL END AS currency_code
, CASE WHEN business_unit_desc = 'N.COM' THEN 808
	WHEN business_unit_desc = 'N.CA' THEN 867
	WHEN business_unit_desc = 'OFFPRICE ONLINE' THEN 828
	ELSE NULL END AS store_num
, 'total_digital' AS rewards_level	
, platform
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
, SUM(visitor_count)-max(coalesce(b.suspicious_visitors,0)) AS visitors
, SUM(product_view_visitors)-max(coalesce(b.suspicious_viewing_visitors,0)) AS viewing_visitors
, SUM(cart_add_visitors)-max(coalesce(b.suspicious_adding_visitors,0)) AS adding_visitors
, SUM(ordering_visitors) AS ordering_visitors
, NULL AS sessions
, NULL AS viewing_sessions 
, NULL AS adding_sessions
, NULL AS ordering_sessions
, NULL AS store_purchase_trips 
, NULL AS store_traffic
, SUM(ordering_visitors) AS ordering_visitors_or_store_purchase_trips
, NULL AS sessions_or_store_traffic
, NULL AS transactions_total
, NULL AS transactions_purchase
, NULL AS transactions_return
, NULL AS visits_total
, NULL AS visits_purchase
, NULL AS visits_return
FROM PRD_NAP_USR_VWS.VISITOR_FUNNEL_FACT vff 
left join bot_restatement b on vff.activity_date = b.day_date and business_unit_desc = b.business_unit and vff.platform = b.device_type
WHERE 1=1
and activity_date BETWEEN '2019-02-03' AND current_date-1
AND activity_date BETWEEN {start_date} AND {end_date}
and ((business_unit_desc IN ('N.COM','N.CA')) OR (business_unit_desc = 'OFFPRICE ONLINE' AND activity_date >= '2021-06-13')) --r.com traffic before END of project rocket isn't valid           
and bot_traffic_ind is NOT IN ('Y')
and platform <> 'POS' --remove DTC, it should be accounted for IN store traffic
--ideally move bopus too, but it's NOT able to be split by orders either- at least that's consistent by conversion rate (orders/visitors)
GROUP BY 1,2,3,4,5,6,7
	UNION ALL --add sessions
select
activity_date_pacific AS tran_date
, CASE
    WHEN channelcountry = 'US' AND channel = 'NORDSTROM' THEN 'N.COM'
    WHEN channelcountry = 'CA' AND channel = 'NORDSTROM' THEN 'N.CA'
    WHEN channelcountry = 'US' AND channel IN ('NORDSTROM_RACK', 'HAUTELOOK') THEN 'OFFPRICE ONLINE'
    ELSE channel END AS business_unit_desc
, 'NAP_ERTM' AS data_source
, CASE WHEN channelcountry = 'US' THEN 'USD'
    WHEN channelcountry = 'CA' THEN 'CAD'
    ELSE NULL END AS currency_code
, CASE WHEN business_unit_desc = 'N.COM' THEN 808
	WHEN business_unit_desc = 'N.CA' THEN 867
	WHEN business_unit_desc = 'OFFPRICE ONLINE' THEN 828
	ELSE NULL END AS store_num
, 'total_digital' AS rewards_level
, CASE WHEN experience = 'CARE_PHONE' THEN 'CSR_PHONE'
	WHEN experience = 'ANDROID_APP' THEN 'ANDROID'
	WHEN experience = 'MOBILE_WEB' THEN 'MOW'
	WHEN experience = 'DESKTOP_WEB' THEN 'WEB'
	WHEN experience = 'POINT_OF_SALE' THEN 'POS'
	WHEN experience = 'IOS_APP' THEN 'IOS'
	WHEN experience = 'VENDOR' THEN 'THIRD_PARTY_VENDOR'
	ELSE experience END AS platform
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
, SUM(sessions) AS sessions
, SUM(CASE WHEN product_views >= 1 THEN sessions ELSE 0 END) AS viewing_sessions
, SUM(CASE WHEN cart_adds >= 1 THEN sessions ELSE 0 END) AS adding_sessions
, SUM(CASE WHEN web_orders >= 1 THEN sessions ELSE 0 END) AS ordering_sessions
, NULL AS store_purchase_trips 
, NULL AS store_traffic
, NULL AS ordering_visitors_or_store_purchase_trips
, SUM(sessions) AS sessions_or_store_traffic
, NULL AS transactions_total
, NULL AS transactions_purchase
, NULL AS transactions_return
, NULL AS visits_total
, NULL AS visits_purchase
, NULL AS visits_return
--FROM sessions_all AS csf
FROM T2DL_DAS_SESSIONS.dior_session_fact_daily
WHERE activity_date_pacific >= '2022-01-31' --first date of complete sessions data. Adding sessions is empty until 2022-02-09
AND activity_date_pacific BETWEEN {start_date} AND {end_date}
and business_unit_desc NOT IN ('TRUNK_CLUB')
and platform <> 'POS'
GROUP BY 1,2,3,4,5,6,7
	UNION ALL --store traffic. This may change to add an orders equivalent AS well by hour
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
	--r.com historical funnel
SELECT 
CAST(day_date AS date) AS tran_date
--, 'total_nrhl' AS hour_var
, 'OFFPRICE ONLINE' AS business_unit_desc
, 'NRHL_redshift' AS data_source
, 'USD' AS currency_code
, 828 AS store_num
, 'total_nrhl' AS rewards_level
, 'ANDROID' AS platform
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
, SUM(visitors_android) AS visitors 
, SUM(viewing_visitors_android) AS viewing_visitors  
, SUM(adding_visitors_android) AS adding_visitors  
, SUM(ordering_visitors_android) AS ordering_visitors
, NULL AS sessions
, NULL AS viewing_sessions 
, NULL AS adding_sessions
, NULL AS ordering_sessions
, NULL AS store_purchase_trips 
, NULL AS store_traffic
, SUM(ordering_visitors_android) AS ordering_visitors_or_store_purchase_trips 
, NULL AS sessions_or_store_traffic
, NULL AS transactions_total
, NULL AS transactions_purchase
, NULL AS transactions_return
, NULL AS visits_total
, NULL AS visits_purchase
, NULL AS visits_return
FROM T2DL_DAS_MOTHERSHIP.daily_vistors_GA_VFF fun --historical data from T3DL_ACE_OP.daily_vistors_GA_VFF fun	
WHERE 1=1		
and day_date BETWEEN '2019-02-03' AND '2021-06-12' --hard code for historical data
AND day_date BETWEEN {start_date} AND {end_date} --so historical doesn't run in daily job
GROUP BY 1,2,3,4,5,6,7
	UNION ALL		
SELECT 
CAST(day_date AS date) AS tran_date
--, 'total_nrhl' AS hour_var
, 'OFFPRICE ONLINE' AS business_unit_desc
, 'NRHL_redshift' AS data_source
, 'USD' AS currency_code
, 828 AS store_num
, 'total_nrhl' AS rewards_level
, 'MOW' AS platform			
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
, SUM(visitors_mow) AS visitors   
, SUM(viewing_visitors_mow) AS viewing_visitors  
, SUM(adding_visitors_mow) AS adding_visitors  
, SUM(ordering_visitors_mow) AS ordering_visitors
, NULL AS sessions
, NULL AS viewing_sessions 
, NULL AS adding_sessions
, NULL AS ordering_sessions
, NULL AS store_purchase_trips 
, NULL AS store_traffic
, SUM(ordering_visitors_mow) AS ordering_visitors_or_store_purchase_trips 
, NULL AS sessions_or_store_traffic
, NULL AS transactions_total
, NULL AS transactions_purchase
, NULL AS transactions_return
, NULL AS visits_total
, NULL AS visits_purchase
, NULL AS visits_return
FROM T2DL_DAS_MOTHERSHIP.daily_vistors_GA_VFF fun --historical data from T3DL_ACE_OP.daily_vistors_GA_VFF fun	
WHERE 1=1		
and day_date BETWEEN '2019-02-03' AND '2021-06-12' --hard code for historical data
AND day_date BETWEEN {start_date} AND {end_date} --so historical doesn't run in daily job
GROUP BY 1,2,3,4,5,6,7
	UNION ALL		
SELECT 
CAST(day_date AS date) AS tran_date	
--, 'total_nrhl' AS hour_var
, 'OFFPRICE ONLINE' AS business_unit_desc
, 'NRHL_redshift' AS data_source
, 'USD' AS currency_code
, 828 AS store_num
, 'total_nrhl' AS rewards_level
, 'IOS' AS platform		
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
, SUM(visitors_ios) AS visitors  
, SUM(viewing_visitors_ios) AS viewing_visitors 
, SUM(adding_visitors_ios) AS adding_visitors  
, SUM(ordering_visitors_ios) AS ordering_visitors
, NULL AS sessions
, NULL AS viewing_sessions 
, NULL AS adding_sessions
, NULL AS ordering_sessions
, NULL AS store_purchase_trips 
, NULL AS store_traffic
, SUM(ordering_visitors_ios) AS ordering_visitors_or_store_purchase_trips 
, NULL AS sessions_or_store_traffic
, NULL AS transactions_total
, NULL AS transactions_purchase
, NULL AS transactions_return
, NULL AS visits_total
, NULL AS visits_purchase
, NULL AS visits_return
FROM T2DL_DAS_MOTHERSHIP.daily_vistors_GA_VFF fun --historical data from T3DL_ACE_OP.daily_vistors_GA_VFF fun	
WHERE 1=1		
and day_date BETWEEN '2019-02-03' AND '2021-06-12' --hard code for historical data
AND day_date BETWEEN {start_date} AND {end_date}  --so historical doesn't run in daily job
GROUP BY 1,2,3,4,5,6,7
	UNION ALL		
SELECT 
CAST(day_date AS date) AS tran_date	
--, 'total_nrhl' AS hour_var
, 'OFFPRICE ONLINE' AS business_unit_desc
, 'NRHL_redshift' AS data_source
, 'USD' AS currency_code
, 828 AS store_num
, 'total_nrhl' AS rewards_level
, 'WEB' AS platform	  	
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
, SUM(visitors_web) AS visitors     
, SUM(viewing_visitors_web) AS viewing_visitors 
, SUM(adding_visitors_web) AS adding_visitors  
, SUM(ordering_visitors_web) AS ordering_visitors
, NULL AS sessions
, NULL AS viewing_sessions 
, NULL AS adding_sessions
, NULL AS ordering_sessions
, NULL AS store_purchase_trips 
, NULL AS store_traffic
, SUM(ordering_visitors_web) AS ordering_visitors_or_store_purchase_trips  
, NULL AS sessions_or_store_traffic
, NULL AS transactions_total
, NULL AS transactions_purchase
, NULL AS transactions_return
, NULL AS visits_total
, NULL AS visits_purchase
, NULL AS visits_return
FROM T2DL_DAS_MOTHERSHIP.daily_vistors_GA_VFF fun --historical data from T3DL_ACE_OP.daily_vistors_GA_VFF fun	
WHERE 1=1		
and day_date BETWEEN '2019-02-03' AND '2021-06-12' --hard code for historical data
AND day_date BETWEEN {start_date} AND {end_date}  --so historical doesn't run in daily job
GROUP BY 1,2,3,4,5,6,7
)
select
tran_date	
--, hour_var
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
--and tran_date IN ('2019-02-04', '2020-02-04', '2021-02-04', '2021-07-04', '2022-02-06', '2021-11-11')--for testing
and tran_date BETWEEN {start_date} AND {end_date} 
GROUP BY 1,2,3,4,5,6,7,8,9
--ORDER BY 1,2,3	
;


COLLECT STATISTICS COLUMN(tran_date), COLUMN(business_unit_desc), COLUMN(store_num), COLUMN(data_source), COLUMN(platform) ON {mothership_t2_schema}.SALES_DEMAND_ORDERS_TRAFFIC_FACT; --, COLUMN(rfm_segment), COLUMN(cts_segment)
SET QUERY_BAND = NONE FOR SESSION;