SET QUERY_BAND = 'App_ID=APP08176; 
     DAG_ID=mothership_finance_sales_demand_fact_11521_ACE_ENG;
     Task_Name=finance_sales_demand_fact;'
     FOR SESSION VOLATILE;
 
/*
T2DL_DAS_MOTHERSHIP.FINANCE_SALES_DEMAND_FACT
Description - This ddl creates a 7 box (n.com, r.com, n.ca, FULL LINE, FULL LINE CANADA, RACK, RACK CANADA) table of hourly sales AND demand split by store number, merch division, platform (device, eg ios, web, mow) AND customer journey (eg bopus, store fulfill, ship to store, etc.)
Full documenation: https://confluence.nordstrom.com/display/AS/FINANCE_SALES_DEMAND_FACT+and+SALES_DEMAND_ORDERS_TRAFFIC_FACT
Contacts: Matthew Bond, Analytics
*/

--daily refresh schedule, needs to pull last 40 days to cover any changes made by audit of last FM AND demand cancels changing over time (if that doesn't bog down the system)


/* step 1
get loyalty information into format to show changes IN loyalty status over time
*/
CREATE MULTISET VOLATILE TABLE loy AS (
SELECT DISTINCT 
acp_id
, rewards_level
, start_day_date
, end_day_date
, LEAD(start_day_date-1, 1) OVER (partition by acp_id order by start_day_date) AS end_day_date_2
-- original END_day_date = next start_day_date, so create new END_day_date that doesn't overlap
, COALESCE (end_day_date_2, end_day_date) AS end_day_date_lead
FROM PRD_NAP_USR_VWS.LOYALTY_LEVEL_LIFECYCLE_FACT_VW 
WHERE end_day_date >= {start_date} and start_day_date <= {end_date} 
and start_day_date < end_day_date --remove a few duplicate rows WHERE the first day of the year is a lower status but instantly changes to higher status 
)
WITH DATA NO PRIMARY INDEX ON COMMIT PRESERVE ROWS; --optimized by TD team


COLLECT STATISTICS COLUMN (acp_id) ON loy;
COLLECT STATISTICS COLUMN (acp_id,start_day_date, end_day_date) ON loy;
COLLECT STATISTICS COLUMN (start_day_date) ON loy;
COLLECT STATISTICS COLUMN (end_day_date_lead) ON loy;
COLLECT STATISTICS COLUMN (end_day_date) ON loy;


/* step 2
get segmentation model but only keep rows where segment changes. Fewer rows = faster join later
*//*
CREATE MULTISET VOLATILE TABLE cts1 AS (
SELECT 
acp_id
, scored_Date
, predicted_segment 
, LAG (predicted_segment, 1) OVER (PARTITION BY acp_id ORDER BY scored_date) AS seg_lag
, LEAD (predicted_segment, 1) OVER (PARTITION BY acp_id ORDER BY scored_date) AS seg_lead
FROM T2DL_DAS_MOTHERSHIP.core_target_hist --t3dl_ace_ma.core_target_hist
QUALIFY predicted_segment <> seg_lag OR predicted_segment <> seg_lead --qualify is like where, but for Ordered Analytical Functions
)
WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (acp_id) ON cts1;
*/

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
cut down RTDF_vw to only needed columns to make joining faster later on
*/
CREATE MULTISET VOLATILE TABLE rtdf AS (
SELECT 
business_day_date, order_num, tran_line_id, upc_num, acp_id, global_tran_id, line_item_seq_num
, line_item_activity_type_desc, line_net_amt, line_item_quantity, tran_type_code, sku_num, order_date
, line_item_net_amt_currency_code, original_line_item_amt, original_line_item_amt_currency_code, intent_store_num, line_item_order_type, line_item_fulfillment_type, merch_dept_num
FROM PRD_NAP_USR_VWS.RETAIL_TRAN_DETAIL_FACT_vw
WHERE 1=1
AND line_net_usd_amt <> 0 --remove gift WITH purchase FROM units calculations
and business_day_date BETWEEN {start_date} AND {end_date}
--and business_day_date IN ('2019-02-04', '2020-02-04', '2021-02-04', '2021-07-04', '2022-02-06', '2021-11-11') --for testing code 
and ((intent_store_num = 828 AND business_day_date BETWEEN '2020-08-30' AND CURRENT_DATE - 1) 
--r.com data before this period is based on order date not shipped date. 
--shipped date IN fact_shipment_items can't be linked back to rtdf (not enough info for which order lines are IN which shipment)
or (COALESCE(intent_store_num, 0) <> 828 AND business_day_date BETWEEN '2019-02-03' AND CURRENT_DATE - 1))
)
-- ORG PI WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;
WITH DATA PRIMARY INDEX(acp_id, order_num, tran_line_id, business_day_date) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN(acp_id,order_num, tran_line_id, business_day_date) ON rtdf;
COLLECT STATISTICS COLUMN (order_num, tran_line_id) ON rtdf;
COLLECT STATISTICS COLUMN (intent_store_num) ON rtdf;


/* step 5
cut down OLDF to only needed columns to make joining faster later on
*/
CREATE MULTISET VOLATILE TABLE pre_oldf AS (
SELECT 
oldf.order_num, order_line_num, source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code --for sales calculations
, fraud_cancel_ind, cancel_reason_code, order_line_currency_code, order_line_amount, order_line_quantity, order_line_id, ent_cust_id, shopper_id, rms_sku_num, source_channel_country_code, order_date_pacific, order_tmstp_pacific--, sku_num --for demand calculations
, CASE
    WHEN source_channel_country_code = 'US' AND source_channel_code = 'FULL_LINE' THEN 'N.COM'
    WHEN source_channel_country_code = 'CA' AND source_channel_code = 'FULL_LINE' THEN 'N.CA'
    WHEN source_channel_country_code = 'US' AND source_channel_code = 'RACK' THEN 'OFFPRICE ONLINE'
    ELSE NULL END AS business_unit_desc
, CASE WHEN COALESCE(order_line_promotion_discount_amount_usd,0) - COALESCE(order_line_employee_discount_amount_usd,0) > 0 THEN 1
    ELSE 0 END AS promo_flag
, CASE WHEN source_platform_code = 'POS' THEN source_store_num
    WHEN business_unit_desc = 'N.COM' THEN 808
    WHEN business_unit_desc = 'N.CA' THEN 867
    WHEN business_unit_desc = 'OFFPRICE ONLINE' THEN 828
    ELSE NULL END AS source_store_num
FROM PRD_NAP_USR_VWS.ORDER_LINE_DETAIL_FACT AS oldf
WHERE 1=1
and order_date_pacific BETWEEN ({start_date} - 182) AND {end_date}   --need to extend start date to account for lag between order,sale,return when using this for platform and customer journey
--and order_date_pacific IN ('2019-02-04', '2020-02-04', '2021-02-04', '2021-07-04', '2022-02-06', '2021-11-11') --for testing code 
AND ((business_unit_desc IN ('N.COM','N.CA') AND COALESCE(fraud_cancel_ind,'N')='N')  --ncom orders include cancels AND exclude fraud
       OR (business_unit_desc = 'OFFPRICE ONLINE' AND order_date_pacific >= '2020-11-21'))   --rcom orders include cancels AND fraud
AND COALESCE(beauty_sample_ind,'N')='N'
AND COALESCE(gift_WITH_purchase_ind,'N')='N'
AND order_line_amount > 0  --Exclude fragrance AND beauty samples
)
WITH DATA PRIMARY INDEX(order_num, order_line_num) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (ent_cust_id) ON pre_oldf ;
COLLECT STATISTICS COLUMN (order_num, order_line_num) ON pre_oldf;
COLLECT STATISTICS COLUMN (source_platform_code) ON pre_oldf;
COLLECT STATISTICS COLUMN (order_date_pacific) ON pre_oldf;


/* step 6
get only OLDF WHERE ent_cust_id is not null to make join to aacx easier to get acp_id
*/
CREATE MULTISET VOLATILE TABLE oldf_not_null AS (
SELECT 
pre_oldf.order_num, order_line_num, source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code --for sales calculations
, cancel_reason_code, order_line_currency_code, order_line_amount, order_line_quantity, order_line_id, ent_cust_id, rms_sku_num, source_channel_country_code, order_date_pacific, source_store_num, order_tmstp_pacific--, sku_num --for demand calculations
, business_unit_desc, promo_flag
FROM pre_oldf
WHERE ent_cust_id IS NOT NULL
)
WITH DATA PRIMARY INDEX(order_num, order_line_num) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (order_num, order_line_num) ON oldf_not_null;
COLLECT STATISTICS COLUMN (ent_cust_id) ON oldf_not_null;


/* step 7                
get only OLDF WHERE ent_cust_id is null for join to rtdf to get acp_id
*/
CREATE MULTISET VOLATILE TABLE oldf_null AS (
SELECT 
pre_oldf.order_num, order_line_num, source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code --for sales calculations
, cancel_reason_code, order_line_currency_code, order_line_amount, order_line_quantity, order_line_id, ent_cust_id, shopper_id, rms_sku_num, source_channel_country_code, order_date_pacific, source_store_num, order_tmstp_pacific--, sku_num --for demand calculations
, business_unit_desc, promo_flag
FROM pre_oldf
WHERE ent_cust_id IS NULL
)
WITH DATA PRIMARY INDEX(shopper_id,order_num, order_line_num) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (order_date_pacific) ON oldf_null;
COLLECT STATISTICS COLUMN (shopper_id) ON oldf_null;
COLLECT STATISTICS COLUMN (shopper_id,order_num, order_line_num) ON oldf_null;

DROP TABLE pre_oldf;

/* step 8
cut down PRD_NAP_USR_VWS.ACP_ANALYTICAL_CUST_XREF to make joins later easier 
*/
CREATE MULTISET VOLATILE TABLE aacx AS (
SELECT acp_id, cust_id 
FROM PRD_NAP_USR_VWS.ACP_ANALYTICAL_CUST_XREF 
WHERE cust_source = 'ICON' 
AND cust_id IN (SELECT DISTINCT ent_cust_id FROM oldf_not_null) --try to reduce size for troublesome join later
QUALIFY Row_Number() OVER (PARTITION BY cust_id ORDER BY dw_batch_date DESC) = 1 
--some cust_id's have more than one acp_id, so pick the most recently loaded one to avoid duplication
)
WITH DATA PRIMARY INDEX(cust_id) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (cust_id) ON aacx ;


/* step 9
add acp_id to oldf FROM aacx
*/
CREATE MULTISET VOLATILE TABLE oldf_not_null_acp AS (
SELECT 
oldf_not_null.order_num, order_line_num, source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code --for sales calculations
, cancel_reason_code, order_line_currency_code, order_line_amount, order_line_quantity, order_line_id, ent_cust_id, rms_sku_num, source_channel_country_code, order_date_pacific, source_store_num, order_tmstp_pacific--, sku_num --for demand calculations
, business_unit_desc, promo_flag, aacx.acp_id AS aacx_acp_id
FROM oldf_not_null
LEFT JOIN aacx ON oldf_not_null.ent_cust_id = aacx.cust_id
)
WITH DATA PRIMARY INDEX(order_num, order_line_num) ON COMMIT PRESERVE ROWS;


/* step 10
cut down aapx to get a single record per acp_id
*/
CREATE MULTISET VOLATILE TABLE aapx_acp AS (
SELECT 
DISTINCT program_index_id, acp_id 
FROM PRD_NAP_USR_VWS.ACP_ANALYTICAL_PROGRAM_XREF AS aapx
QUALIFY ROW_NUMBER() OVER (PARTITION BY acp_id ORDER BY program_index_id DESC) = 1 
    AND ROW_NUMBER() OVER (partition BY program_index_id ORDER BY acp_id DESC) = 1
WHERE acp_id IS NOT NULL 
AND program_index_id IS NOT NULL
AND program_name IN ('WEB') --removed ON filter
)
WITH DATA PRIMARY INDEX(program_index_id) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (program_index_id) ON aapx_acp ;


/* step 11
add acp_id to oldf FROM aapx (before '2020-02-12' oldf was missing ent_cust_id)
*/
CREATE MULTISET VOLATILE TABLE oldf_null_acp AS (
SELECT 
oldf_null.order_num, order_line_num, source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code --for sales calculations
, cancel_reason_code, order_line_currency_code, order_line_amount, order_line_quantity, order_line_id, ent_cust_id, rms_sku_num, source_channel_country_code, order_date_pacific, source_store_num, order_tmstp_pacific --for demand calculations
, business_unit_desc, promo_flag, acp_id AS shopper_acp_id --, program_name
FROM oldf_null 
LEFT JOIN aapx_acp
ON oldf_null.shopper_id = aapx_acp.program_index_id AND order_date_pacific < '2020-02-12' 
)
WITH DATA PRIMARY INDEX(order_num, order_line_num) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (order_num, order_line_num) ON oldf_null_acp;


/* step 12
combine oldf_null_acp AND oldf_not_null_acp
*/
CREATE MULTISET VOLATILE TABLE oldf_acp AS (
SELECT 
oldf_null_acp.order_num, order_line_num, source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code --for sales calculations
, cancel_reason_code, order_line_currency_code, order_line_amount, order_line_quantity, order_line_id, ent_cust_id, rms_sku_num, source_channel_country_code, order_date_pacific, source_store_num, order_tmstp_pacific--, sku_num --for demand calculations
, business_unit_desc, promo_flag, shopper_acp_id AS oldf_acp_id
FROM oldf_null_acp
	UNION ALL
SELECT 
oldf_not_null_acp.order_num, order_line_num, source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code --for sales calculations
, cancel_reason_code, order_line_currency_code, order_line_amount, order_line_quantity, order_line_id, ent_cust_id, rms_sku_num, source_channel_country_code, order_date_pacific, source_store_num, order_tmstp_pacific--, sku_num --for demand calculations
, business_unit_desc, promo_flag, aacx_acp_id  AS oldf_acp_id
FROM oldf_not_null_acp
)
WITH DATA PRIMARY INDEX(order_num, order_line_num) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (oldf_acp_id) ON oldf_acp;
COLLECT STATISTICS COLUMN (order_num, order_line_num) ON oldf_acp;


/* step 13
get only oldf_acp WHERE acp_id is not null to make join to customer variables easier (no NULLs IN index)
*/
CREATE MULTISET VOLATILE TABLE oldf_acp_not_null AS (
SELECT 
order_num, order_line_num, source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code --for sales calculations
, cancel_reason_code, order_line_currency_code, order_line_amount, order_line_quantity, order_line_id, ent_cust_id, rms_sku_num, source_channel_country_code, order_date_pacific, source_store_num, order_tmstp_pacific--, sku_num --for demand calculations
, business_unit_desc, promo_flag, oldf_acp_id AS acp_id
FROM oldf_acp
WHERE acp_id IS NOT NULL
)
WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (acp_id) ON oldf_acp_not_null;
COLLECT STATISTICS COLUMN (ORDER_DATE_PACIFIC) ON oldf_acp_not_null;


/* step 14                
get only oldf_acp WHERE acp_id is null for join to rtdf to get acp_id
*/
CREATE MULTISET VOLATILE TABLE oldf_acp_null AS (
 SELECT 
order_num, order_line_num, source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code --for sales calculations
, cancel_reason_code, order_line_currency_code, order_line_amount, order_line_quantity, order_line_id, ent_cust_id, rms_sku_num, source_channel_country_code, order_date_pacific, source_store_num, order_tmstp_pacific--, sku_num --for demand calculations
, business_unit_desc, promo_flag
FROM oldf_acp
WHERE oldf_acp_id IS NULL
)
WITH DATA PRIMARY INDEX(order_num, order_line_num) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (order_num, order_line_num) ON oldf_acp_null;


/* step 15
add customer data to oldf using acp_id
*/
CREATE MULTISET VOLATILE TABLE oldf_cust AS (
SELECT 
order_num, order_line_num, source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code --for sales calculations
, cancel_reason_code, order_line_currency_code, order_line_amount, order_line_quantity, order_line_id, ent_cust_id, rms_sku_num, source_channel_country_code, order_date_pacific, source_store_num, order_tmstp_pacific--, sku_num --for demand calculations
, business_unit_desc, promo_flag--, cts_segment, rfm_4year_segment AS rfm_segment
    , rewards_level
FROM oldf_acp_not_null AS oldf
    --LEFT JOIN segment ON segment.acp_id = oldf.acp_id AND oldf.order_date_pacific >= segment.scored_date_start AND oldf.order_date_pacific <= segment.scored_date_end
    --LEFT JOIN T2DL_DAS_CAL.CUSTOMER_ATTRIBUTES_TRANSACTIONS AS cat ON cat.acp_id = oldf.acp_id  --for rfm segment
    LEFT JOIN loy ON loy.acp_id = oldf.acp_id AND oldf.order_date_pacific <= loy.end_day_date_LEAD AND oldf.order_date_pacific >= loy.start_day_date
)
WITH DATA PRIMARY INDEX(order_num, order_line_num) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (order_num, order_line_num) ON oldf_cust;

 
/* step 16
combine oldf_acp_null and oldf_cust
*/
CREATE MULTISET VOLATILE TABLE oldf_cust_all AS (
 SELECT 
order_num, order_line_num, source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code --for sales calculations
, cancel_reason_code, order_line_currency_code, order_line_amount, order_line_quantity, order_line_id, ent_cust_id, rms_sku_num, source_channel_country_code, order_date_pacific, source_store_num, order_tmstp_pacific--, sku_num --for demand calculations
, business_unit_desc, promo_flag--, cts_segment, rfm_segment
    , rewards_level
FROM oldf_cust
	UNION ALL
SELECT 
order_num, order_line_num, source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code --for sales calculations
, cancel_reason_code, order_line_currency_code, order_line_amount, order_line_quantity, order_line_id, ent_cust_id, rms_sku_num, source_channel_country_code, order_date_pacific, source_store_num, order_tmstp_pacific--, sku_num --for demand calculations
, business_unit_desc, promo_flag--, CAST(NULL AS VARCHAR(40)) AS cts_segment, CAST(NULL AS VARCHAR(40)) AS rfm_segment
    , CAST(NULL AS VARCHAR(40)) AS rewards_level
FROM oldf_acp_null
)
WITH DATA PRIMARY INDEX(order_num, order_line_num) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (order_num, order_line_num) ON oldf_cust_all;
COLLECT STATISTICS COLUMN (rms_sku_num) ON oldf_cust_all;
COLLECT STATISTICS COLUMN (source_channel_country_code) ON oldf_cust_all;
COLLECT STATISTICS COLUMN (cancel_reason_code) ON oldf_cust_all;
COLLECT STATISTICS COLUMN (order_date_pacific) ON oldf_cust_all;
COLLECT STATISTICS COLUMN (source_platform_code) ON oldf_cust_all;
    
DROP TABLE oldf_cust;
DROP TABLE oldf_acp_null;

/* step 17
use case statement to simplify later join to price type data (mostly for canceled items IN demand calculations- sales are all calculated using retail_tran_price_type_fact)
*/
CREATE MULTISET VOLATILE TABLE price_type AS (
SELECT
rms_sku_num
, (eff_begin_tmstp + INTERVAL '1' SECOND) AS eff_begin_tmstp --add 1 second to prevent duplication of items that were ordered exactly when the price type changed. yes, it can happen
, eff_end_tmstp
, case when selling_retail_price_type_code = 'REGULAR' then 'R'
    when selling_retail_price_type_code = 'CLEARANCE' then 'C'
    when selling_retail_price_type_code = 'PROMOTION' then 'P'
    else null end as current_price_type
, case when ownership_retail_price_type_code = 'REGULAR' then 'R'
    when ownership_retail_price_type_code = 'CLEARANCE' then 'C'
    when ownership_retail_price_type_code = 'PROMOTION' then 'P'
    else null end as ownership_price_type
, store_num
, CASE
WHEN ppd.channel_brand= 'NORDSTROM_RACK' AND ppd.selling_channel = 'STORE' AND ppd.channel_country = 'CA' THEN 'RACK CANADA'
WHEN ppd.channel_brand= 'NORDSTROM_RACK' AND ppd.selling_channel = 'STORE' AND ppd.channel_country = 'US' THEN 'RACK'
WHEN ppd.channel_brand= 'NORDSTROM_RACK' AND ppd.selling_channel = 'ONLINE' AND ppd.channel_country = 'US' THEN 'OFFPRICE ONLINE'
WHEN ppd.channel_brand= 'NORDSTROM' AND ppd.selling_channel = 'STORE' AND ppd.channel_country = 'CA' THEN 'FULL LINE CANADA'
WHEN ppd.channel_brand= 'NORDSTROM' AND ppd.selling_channel = 'STORE' AND ppd.channel_country = 'US' THEN 'FULL LINE'
WHEN ppd.channel_brand= 'NORDSTROM' AND ppd.selling_channel = 'ONLINE' AND ppd.channel_country = 'CA' THEN 'N.CA'
WHEN ppd.channel_brand= 'NORDSTROM' AND ppd.selling_channel = 'ONLINE' AND ppd.channel_country = 'US' THEN 'N.COM'
ELSE NULL END AS business_unit_desc
FROM PRD_NAP_USR_VWS.PRODUCT_PRICE_TIMELINE_DIM ppd
WHERE eff_end_tmstp >= {start_date} and eff_begin_tmstp <= {end_date}
)
WITH DATA PRIMARY INDEX(rms_sku_num, eff_begin_tmstp, eff_end_tmstp, business_unit_desc) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN(rms_sku_num, eff_begin_tmstp, eff_end_tmstp, business_unit_desc) ON price_type;


/* step 18
create table to join oldf WITH rtdf_price_type table for demand query 
*/
CREATE MULTISET VOLATILE TABLE pt_join AS (
SELECT   --DISTINCT 
sarf.order_num 
, sarf.order_line_id 
, sarf.business_day_date 
, sarf.global_tran_id 
, sarf.line_item_seq_num 
FROM T2DL_DAS_SALES_RETURNS.SALES_AND_RETURNS_FACT_BASE sarf 
WHERE 1=1
and order_date BETWEEN {start_date} AND {end_date} --need to extend start date to account for lag BETWEEN order AND sale
--and business_day_date IN ('2019-02-04', '2020-02-04', '2021-02-04', '2021-07-04', '2022-02-06', '2021-11-11') --for testing code 
and ((intent_store_num = 828 AND sarf.business_day_date BETWEEN '2020-08-30' AND CURRENT_DATE - 1) 
--r.com data before this period is based on order date not shipped date. 
--shipped date IN fact_shipment_items can't be linked back to rtdf (not enough info for which order lines are IN which shipment)
or (COALESCE(intent_store_num, 0) <> 828 AND sarf.business_day_date BETWEEN '2019-02-03' AND CURRENT_DATE - 1))
)
WITH DATA PRIMARY INDEX(business_day_date, global_tran_id, line_item_seq_num) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (business_day_date, global_tran_id, line_item_seq_num) ON pt_join;


/* step 19
-- cut down price type to make later joins more efficient
*/
CREATE MULTISET VOLATILE TABLE pt_price_type AS (
SELECT   --DISTINCT 
  rtp.business_day_date 
, rtp.global_tran_id 
, rtp.line_item_seq_num 
, rtp.price_type
FROM T2DL_DAS_SALES_RETURNS.retail_tran_price_type_fact rtp 
WHERE 1=1 
AND rtp.business_day_date BETWEEN {start_date} AND ({end_date} + 365)   --need to extend end date to account for lag between order,sale,return 
)
WITH DATA PRIMARY INDEX(business_day_date, global_tran_id, line_item_seq_num) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (business_day_date, global_tran_id, line_item_seq_num) ON pt_price_type;


/* step 20
-- pre-join price type WITH the pt_join helper table to join to OLDF 
*/
CREATE MULTISET VOLATILE TABLE pt_join_price_type AS (
 SELECT   --DISTINCT 
pt.order_num 
, pt.order_line_id 
, pt.business_day_date 
, pt.global_tran_id 
, pt.line_item_seq_num
, ppt.price_type 
FROM pt_join pt 
LEFT JOIN pt_price_type ppt 
ON ppt.business_day_date = pt.business_day_date AND ppt.global_tran_id = pt.global_tran_id AND ppt.line_item_seq_num = pt.line_item_seq_num --and oldf.cancel_reason_code is NULL 
QUALIFY Row_Number() OVER (PARTITION BY pt.order_num,pt.order_line_id ORDER BY pt.business_day_date DESC) = 1
)
WITH DATA PRIMARY INDEX(business_day_date, global_tran_id, line_item_seq_num) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (business_day_date, global_tran_id, line_item_seq_num) ON pt_join_price_type;


/* step 21
get only oldf without cancels to make joins to price type easier
*/
CREATE MULTISET VOLATILE TABLE oldf_excl_cancels AS (
SELECT
order_num, order_line_num, source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code --for sales calculations
, cancel_reason_code, order_line_currency_code, order_line_amount, order_line_quantity, order_line_id, ent_cust_id, rms_sku_num, source_channel_country_code, order_date_pacific, source_store_num, order_tmstp_pacific--, sku_num --for demand calculations
, business_unit_desc, promo_flag--, cts_segment, rfm_segment
    , rewards_level
FROM oldf_cust_all
WHERE cancel_reason_code is NULL
)
WITH DATA PRIMARY INDEX(order_num, order_line_num) ON COMMIT PRESERVE ROWS;


/* step 22
get only oldf cancels to make joins to price type easier
*/
CREATE MULTISET VOLATILE TABLE oldf_cancels AS (
SELECT
order_num, order_line_num, source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code --for sales calculations
, cancel_reason_code, order_line_currency_code, order_line_amount, order_line_quantity, order_line_id, ent_cust_id, rms_sku_num, source_channel_country_code, order_date_pacific, source_store_num, order_tmstp_pacific--, sku_num --for demand calculations
, business_unit_desc, promo_flag--, cts_segment, rfm_segment
    , rewards_level
FROM oldf_cust_all
WHERE cancel_reason_code is NOT NULL
)
WITH DATA PRIMARY INDEX(order_num, order_line_num) ON COMMIT PRESERVE ROWS;


/* step 23
add price type to oldf for cancels, which are not in the more accurate table T2DL_DAS_SALES_RETURNS.retail_tran_price_type_fact
*/
CREATE MULTISET VOLATILE TABLE oldf_cancels_pt AS (
SELECT
oldf.order_num, order_line_num, source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code --for sales calculations
, cancel_reason_code, order_line_currency_code, order_line_amount, order_line_quantity, oldf.order_line_id, ent_cust_id, oldf.rms_sku_num, source_channel_country_code, order_date_pacific, source_store_num, order_tmstp_pacific--, sku_num --for demand calculations
, oldf.business_unit_desc, promo_flag--, cts_segment, rfm_segment
    , rewards_level
, cast(CASE WHEN promo_flag = 1 AND ptc.current_price_type = 'R' then 'P'
    WHEN ptc.ownership_price_type = 'C' then ptc.ownership_price_type
    ELSE ptc.current_price_type END AS VARCHAR(20)) AS merch_price_type
FROM oldf_cancels AS oldf
LEFT JOIN price_type AS ptc ON oldf.rms_sku_num = ptc.rms_sku_num AND oldf.business_unit_desc = ptc.business_unit_desc AND order_tmstp_pacific BETWEEN ptc.eff_begin_tmstp AND ptc.eff_end_tmstp
)
WITH DATA PRIMARY INDEX(order_num, order_line_num) ON COMMIT PRESERVE ROWS;


/* step 24
add price type to oldf for shipped sales from more accurate table T2DL_DAS_SALES_RETURNS.retail_tran_price_type_fact
*/
CREATE MULTISET VOLATILE TABLE oldf_excl_cancels_pt_ss AS (
SELECT
oldf.order_num, order_line_num, source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code --for sales calculations
, cancel_reason_code, order_line_currency_code, order_line_amount, order_line_quantity, oldf.order_line_id, ent_cust_id, oldf.rms_sku_num, source_channel_country_code, order_date_pacific, source_store_num, order_tmstp_pacific--, sku_num --for demand calculations
, oldf.business_unit_desc, promo_flag--, cts_segment, rfm_segment
    , rewards_level
, cast(price_type AS VARCHAR(20)) AS merch_price_type
FROM oldf_excl_cancels AS oldf
LEFT JOIN pt_join_price_type pt ON pt.order_num = oldf.order_num AND pt.order_line_id = oldf.order_line_id
)
WITH DATA PRIMARY INDEX(order_num, order_line_num) ON COMMIT PRESERVE ROWS;


/* step 25
pull out null price_types (where shipped sales table join left nulls- mostly for items that haven't shipped yet which are mostly between current_date and current_date-8)
*/
CREATE MULTISET VOLATILE TABLE oldf_excl_cancels_pt_ss_null AS (
SELECT
order_num, order_line_num, source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code --for sales calculations
, cancel_reason_code, order_line_currency_code, order_line_amount, order_line_quantity, order_line_id, ent_cust_id, rms_sku_num, source_channel_country_code, order_date_pacific, source_store_num, order_tmstp_pacific--, sku_num --for demand calculations
, business_unit_desc, promo_flag--, cts_segment, rfm_segment
    , rewards_level
FROM oldf_excl_cancels_pt_ss
WHERE merch_price_type is NULL
)
WITH DATA PRIMARY INDEX(order_num, order_line_num) ON COMMIT PRESERVE ROWS;


/* step 26
add price type to oldf for items which haven't shipped yet (mostly most recent 8 days) so are not in the more accurate table T2DL_DAS_SALES_RETURNS.retail_tran_price_type_fact
*/
CREATE MULTISET VOLATILE TABLE oldf_excl_cancels_pt_8 AS (
SELECT
oldf.order_num, order_line_num, source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code --for sales calculations
, cancel_reason_code, order_line_currency_code, order_line_amount, order_line_quantity, oldf.order_line_id, ent_cust_id, oldf.rms_sku_num, source_channel_country_code, order_date_pacific, source_store_num, order_tmstp_pacific--, sku_num --for demand calculations
, oldf.business_unit_desc, promo_flag--, cts_segment, rfm_segment
    , rewards_level
, cast(CASE WHEN promo_flag = 1 AND pt8.current_price_type = 'R' then 'P'
    WHEN pt8.ownership_price_type = 'C' then pt8.ownership_price_type
    ELSE pt8.current_price_type END AS VARCHAR(20)) AS merch_price_type
FROM oldf_excl_cancels_pt_ss_null AS oldf
LEFT JOIN price_type AS pt8 ON oldf.rms_sku_num = pt8.rms_sku_num AND oldf.business_unit_desc = pt8.business_unit_desc AND order_tmstp_pacific BETWEEN pt8.eff_begin_tmstp AND pt8.eff_end_tmstp AND oldf.source_store_num = pt8.store_num
)
WITH DATA PRIMARY INDEX(order_num, order_line_num) ON COMMIT PRESERVE ROWS;


/* step 27
combine oldf_acp_null and oldf_cust
*/
CREATE MULTISET VOLATILE TABLE oldf_pt AS (
SELECT
order_num, order_line_num, source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code --for sales calculations
, cancel_reason_code, order_line_currency_code, order_line_amount, order_line_quantity, order_line_id, ent_cust_id, rms_sku_num, source_channel_country_code, order_date_pacific, source_store_num, order_tmstp_pacific--, sku_num --for demand calculations
, business_unit_desc, promo_flag--, cts_segment, rfm_segment
    , rewards_level, merch_price_type
FROM oldf_cancels_pt
	UNION ALL
SELECT
order_num, order_line_num, source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code --for sales calculations
, cancel_reason_code, order_line_currency_code, order_line_amount, order_line_quantity, order_line_id, ent_cust_id, rms_sku_num, source_channel_country_code, order_date_pacific, source_store_num, order_tmstp_pacific--, sku_num --for demand calculations
, business_unit_desc, promo_flag--, cts_segment, rfm_segment
    , rewards_level, merch_price_type
FROM oldf_excl_cancels_pt_8
	UNION ALL
SELECT
order_num, order_line_num, source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code --for sales calculations
, cancel_reason_code, order_line_currency_code, order_line_amount, order_line_quantity, order_line_id, ent_cust_id, rms_sku_num, source_channel_country_code, order_date_pacific, source_store_num, order_tmstp_pacific--, sku_num --for demand calculations
, business_unit_desc, promo_flag--, cts_segment, rfm_segment
    , rewards_level, merch_price_type
FROM oldf_excl_cancels_pt_ss
WHERE merch_price_type is NOT NULL
)
WITH DATA PRIMARY INDEX(order_num, order_line_num) ON COMMIT PRESERVE ROWS;


/* step 28
-- primary demand calculations
*/
CREATE MULTISET VOLATILE TABLE DEMAND AS (
 SELECT
        order_date_pacific AS tran_date
        --, CAST(EXTRACT(HOUR FROM ORDER_TMSTP_PACIFIC) AS varchar(15)) AS hour_var
        , oldf.business_unit_desc
        , CAST('NAP_ERTM' AS VARCHAR(20)) AS data_source
        , order_line_currency_code AS currency_code
        , CASE WHEN customer_journey = 'BOPUS' THEN destination_node_num
        	WHEN customer_journey = 'DirectToConsumer' THEN source_store_num
        	WHEN oldf.business_unit_desc = 'N.COM' THEN 808
        	WHEN oldf.business_unit_desc = 'N.CA' THEN 867
        	WHEN oldf.business_unit_desc = 'OFFPRICE ONLINE' THEN 828
        	ELSE NULL END AS store_num_bopus_in_store_demand
        , CASE WHEN customer_journey = 'DirectToConsumer' THEN source_store_num
        	WHEN oldf.business_unit_desc = 'N.COM' THEN 808
        	WHEN oldf.business_unit_desc = 'N.CA' THEN 867
        	WHEN oldf.business_unit_desc = 'OFFPRICE ONLINE' THEN 828
        	ELSE NULL END AS store_num_bopus_in_digital_demand
        , CASE WHEN psd.div_desc LIKE 'INACT%' THEN 'INACT' ELSE psd.div_desc END AS merch_division
        	, CASE WHEN dpt.subdivision_num IN (770, 790, 775, 780, 710, 705, 700, 785) THEN dpt.subdivision_name ELSE NULL END AS merch_subdivision
        --break out the 2 largest divisions (Apparel + Shoes) into more helpful chunks (Women/Men/Kid Apparel, Women/Men Specialized, Women/Men/Kid Shoes)
        	, merch_price_type	
        	, CASE WHEN oldf.business_unit_desc IN ('OFFPRICE ONLINE', 'RACK', 'RACK CANADA') THEN th.rack_role_desc ELSE th.nord_role_desc END AS merch_role
	        --, cts_segment
	        --, rfm_segment
        	, rewards_level	
        , CASE WHEN oldf.business_unit_desc IN ('N.COM','N.CA') THEN source_platform_code
        	WHEN oldf.business_unit_desc = 'OFFPRICE ONLINE' AND order_date_pacific >= '2021-03-06' THEN source_platform_code 
        	ELSE 'total_nrhl' END AS platform --before 2021-03-06 OLDF isn't accurate at device level for r.com
        , CASE WHEN COALESCE(source_platform_code, 0) = 'POS' THEN 'DirectToConsumer'
        		WHEN delivery_method_code = 'PICK' AND destination_node_num <> '808' AND (destination_node_num > 0) THEN 'BOPUS'
        		    WHEN COALESCE(destination_node_num,-1) > 0 AND destination_node_num <> '808' THEN CASE
        				WHEN COALESCE(last_released_node_type_code, shipped_node_type_code) = 'FC' THEN 'ShipToStore_from_FulfillmentCenter'
        				WHEN COALESCE(last_released_node_type_code, shipped_node_type_code) = 'DS' THEN 'ShipToStore_from_DropShip'
        				WHEN (COALESCE(last_released_node_type_code, shipped_node_type_code) = 'FL' OR COALESCE(last_released_node_type_code, shipped_node_type_code) = 'RK') THEN 'ShipToStore_from_StoreFulfill'  
        				ELSE 'ShipToStore_OtherUnknown' END
        	        WHEN COALESCE(destination_node_num,-1) <= 0 THEN CASE 
        				WHEN COALESCE(last_released_node_type_code, shipped_node_type_code) = 'FC' THEN 'ShipToHome_from_FulfillmentCenter'
        				WHEN COALESCE(last_released_node_type_code, shipped_node_type_code) = 'DS' THEN 'ShipToHome_from_DropShip'
        				WHEN (COALESCE(last_released_node_type_code, shipped_node_type_code) = 'FL' OR COALESCE(last_released_node_type_code, shipped_node_type_code) = 'RK') THEN 'ShipToHome_from_StoreFulfill'
        				ELSE 'ShipToHome_OtherUnknown' END
                ELSE 'OtherUnknown'
                END AS customer_journey
        , SUM(CAST(order_line_amount AS DECIMAL(20,2))) AS demand
        , SUM(CASE WHEN cancel_reason_code IS NOT NULL THEN order_line_amount ELSE NULL END) AS canceled_demand
        , SUM(order_line_quantity) AS units
        , SUM(CASE WHEN cancel_reason_code IS NOT NULL THEN order_line_quantity ELSE NULL END) AS canceled_units
        , CAST(0.00 AS DECIMAL(20,2)) AS gross_merch_sales_amt
        , CAST(0.00 AS DECIMAL(20,2)) AS merch_returns_amt
        , CAST(0.00 AS DECIMAL(20,2)) AS net_merch_sales_amt
        , CAST(0.00 AS DECIMAL(20,2)) AS gross_merch_sales_units
        , CAST(0.00 AS DECIMAL(20,2)) AS merch_returns_units
        , CAST(0.00 AS DECIMAL(20,2)) AS net_merch_sales_units
        , CAST(0.00 AS DECIMAL(20,2)) AS gross_operational_sales_amt
        , CAST(0.00 AS DECIMAL(20,2)) AS operational_returns_amt
        , CAST(0.00 AS DECIMAL(20,2)) AS net_operational_sales_amt
        , CAST(0.00 AS DECIMAL(20,2)) AS gross_operational_sales_units
        , CAST(0.00 AS DECIMAL(20,2)) AS operational_returns_units
        , CAST(0.00 AS DECIMAL(20,2)) AS net_operational_sales_units
        FROM oldf_pt AS oldf
        LEFT JOIN prd_nap_usr_vws.product_sku_dim AS psd ON psd.rms_sku_num = oldf.rms_sku_num AND psd.channel_country = oldf.source_channel_country_code
        LEFT JOIN (SELECT epm_style_num, class_num, dept_num FROM PRD_NAP_USR_VWS.PRODUCT_STYLE_DIM QUALIFY Row_Number() OVER (PARTITION BY epm_style_num ORDER BY channel_country DESC, dw_batch_date DESC) = 1) AS pdc ON pdc.epm_style_num = psd.epm_style_num
        LEFT JOIN t2dl_das_po_visibility.ccs_merch_themes th  ON psd.dept_num = th.dept_idnt AND psd.class_num = th.class_idnt AND psd.sbclass_num = th.sbclass_idnt -- contact Ivie Okieimen for access
        LEFT JOIN PRD_NAP_USR_VWS.DEPARTMENT_DIM AS dpt ON dpt.dept_num = pdc.dept_num --INNER JOIN excludes non-merch items bc they are missing UPC_num   
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
 )
WITH DATA PRIMARY INDEX(tran_date, business_unit_desc, store_num_bopus_in_store_demand, merch_division) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (tran_date, business_unit_desc, store_num_bopus_in_store_demand, merch_division) ON DEMAND;
 

/* step 29
get original journey information (line_item_fulfillment_type) for returned online orders 
*/
CREATE MULTISET VOLATILE TABLE return_journey AS (
SELECT --DISTINCT 
return_date
, return_global_tran_id
, return_line_item_seq_num
, line_item_fulfillment_type AS line_item_fulfillment_type2
, order_num
, order_line_id
FROM T2DL_DAS_SALES_RETURNS.SALES_AND_RETURNS_FACT_BASE sarf  
WHERE intent_store_num IN (808, 828, 867)
AND line_item_fulfillment_type IS NOT NULL
AND return_line_item_seq_num IS NOT NULL 
AND return_date BETWEEN {start_date} AND {end_date}
AND ((intent_store_num = 828 AND return_date BETWEEN '2020-08-30' AND CURRENT_DATE - 1) 
--r.com data before this period is based on order date not shipped date. 
--shipped date IN fact_shipment_items can't be linked back to rtdf (not enough info for which order lines are IN which shipment)
OR (COALESCE(intent_store_num, 0) <> 828 AND return_date BETWEEN '2019-02-03' AND CURRENT_DATE - 1))
)
WITH DATA PRIMARY INDEX(return_date, return_global_tran_id, return_line_item_seq_num) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (return_date, return_global_tran_id, return_line_item_seq_num) ON return_journey;

/* step 30
-- add destination_node_num from oldf to sarf return info because some of it is missing for returns
*/
CREATE MULTISET VOLATILE TABLE return_journey_oldf AS (
SELECT return_date, return_global_tran_id, return_line_item_seq_num, line_item_fulfillment_type2, rj.order_num, rj.order_line_id
, source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code
FROM  return_journey AS rj
LEFT JOIN oldf_acp AS oldf ON rj.order_num = oldf.ORDER_NUM AND rj.order_line_id = oldf.order_line_id
)
WITH DATA PRIMARY INDEX(return_date, return_global_tran_id, return_line_item_seq_num) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (return_date, return_global_tran_id, return_line_item_seq_num) ON return_journey_oldf;


/* step 31
-- join in oldf to get fulfillment AND platform
*/
CREATE MULTISET VOLATILE TABLE sales_rtdf AS (
SELECT business_day_date, rtdf.order_num, rtdf.tran_line_id, upc_num, acp_id, global_tran_id, line_item_seq_num
, line_item_activity_type_desc, line_net_amt, line_item_quantity, tran_type_code, sku_num, order_date
, line_item_net_amt_currency_code, original_line_item_amt, original_line_item_amt_currency_code, intent_store_num, line_item_order_type, line_item_fulfillment_type, merch_dept_num
, source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code 
FROM  rtdf  
LEFT JOIN oldf_acp AS oldf ON rtdf.order_num = oldf.order_num AND rtdf.tran_line_id = oldf.order_line_num
WHERE 1=1
  AND intent_store_num NOT IN (141, 173)-- first filter to exclude trunk club (trunk club needs validation)- second filter is IN sales query
 )
WITH DATA PRIMARY INDEX(acp_id,order_num,tran_line_id,business_day_date) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (acp_id) ON sales_rtdf;
COLLECT STATISTICS COLUMN (business_day_date) ON sales_rtdf;
COLLECT STATISTICS COLUMN (acp_id,order_num,tran_line_id,business_day_date) ON sales_rtdf;

DROP TABLE rtdf;

/* step 32
-- join cts_segment to sales
*/
-- CREATE MULTISET VOLATILE TABLE sales_rtdf_segment AS (
--  SELECT business_day_date, rtdf.order_num, rtdf.tran_line_id, upc_num, rtdf.acp_id, global_tran_id, line_item_seq_num
-- , line_item_activity_type_desc, line_net_amt, line_item_quantity, tran_type_code, sku_num, order_date
-- , line_item_net_amt_currency_code, original_line_item_amt, original_line_item_amt_currency_code, intent_store_num, line_item_order_type, line_item_fulfillment_type, merch_dept_num
-- , source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code
-- , cts_segment
-- FROM sales_rtdf rtdf
-- LEFT JOIN segment
-- ON segment.acp_id = rtdf.acp_id AND rtdf.business_day_date >= segment.scored_date_start AND rtdf.business_day_date <= segment.scored_date_end
-- )
-- WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;
--
--
-- COLLECT STATISTICS COLUMN (acp_id) ON sales_rtdf_segment;


/* step 33
-- join rfm_segment to sales
*/
-- CREATE MULTISET VOLATILE TABLE sales_rtdf_segment2 AS (
--  SELECT business_day_date, rtdf.order_num, rtdf.tran_line_id, upc_num, rtdf.acp_id, global_tran_id, line_item_seq_num
-- , line_item_activity_type_desc, line_net_amt, line_item_quantity, tran_type_code, sku_num, order_date
-- , line_item_net_amt_currency_code, original_line_item_amt, original_line_item_amt_currency_code, intent_store_num, line_item_order_type, line_item_fulfillment_type, merch_dept_num
-- , source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code
-- , cts_segment, rfm_4year_segment AS rfm_segment
-- FROM sales_rtdf_segment rtdf
-- LEFT JOIN T2DL_DAS_CAL.CUSTOMER_ATTRIBUTES_TRANSACTIONS AS cat ON cat.acp_id = rtdf.acp_id  --for rfm segment
-- )
-- WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;
--
--
-- COLLECT STATISTICS COLUMN (acp_id) ON sales_rtdf_segment2;
-- COLLECT STATISTICS COLUMN (business_day_date) ON  sales_rtdf_segment2;


/* step 34
-- join loyalty to sales
*/
CREATE MULTISET VOLATILE TABLE sales_rtdf_segment_loy AS (
SELECT business_day_date, rtdf_seg.order_num, rtdf_seg.tran_line_id, upc_num, rtdf_seg.acp_id, global_tran_id, line_item_seq_num
, line_item_activity_type_desc, line_net_amt, line_item_quantity, tran_type_code, sku_num, order_date
, line_item_net_amt_currency_code, original_line_item_amt, original_line_item_amt_currency_code, intent_store_num, line_item_order_type, line_item_fulfillment_type, merch_dept_num
, source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code 
--, cts_segment, rfm_segment
    , rewards_level
FROM sales_rtdf as rtdf_seg
LEFT JOIN loy 
ON loy.acp_id = rtdf_seg.acp_id AND rtdf_seg.business_day_date <= loy.end_day_date_lead AND rtdf_seg.business_day_date >= loy.start_day_date 
)
WITH DATA PRIMARY INDEX(global_tran_id, business_day_date, line_item_seq_num) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (business_day_date) ON  sales_rtdf_segment_loy;
COLLECT STATISTICS COLUMN (business_day_date, global_tran_id, line_item_seq_num) ON sales_rtdf_segment_loy;


/* step 35
-- join price type to sales
*/
CREATE MULTISET VOLATILE TABLE sales_rtdf_segment_loy_pt AS (
SELECT rtdf_seg_loy.business_day_date, rtdf_seg_loy.order_num, rtdf_seg_loy.tran_line_id, upc_num, rtdf_seg_loy.acp_id, rtdf_seg_loy.global_tran_id, rtdf_seg_loy.line_item_seq_num
, line_item_activity_type_desc, line_net_amt, line_item_quantity, tran_type_code, sku_num, order_date
, line_item_net_amt_currency_code, original_line_item_amt, original_line_item_amt_currency_code, intent_store_num, line_item_order_type, line_item_fulfillment_type, merch_dept_num
, source_platform_code, destination_node_num, last_released_node_type_code, shipped_node_type_code, delivery_method_code
--, cts_segment, rfm_segment
    , rewards_level, CAST(pt.price_type AS VARCHAR(20)) AS merch_price_type
FROM sales_rtdf_segment_loy rtdf_seg_loy
LEFT JOIN pt_price_type pt
ON rtdf_seg_loy.business_day_date = pt.business_day_date
AND rtdf_seg_loy.global_tran_id = pt.global_tran_id AND rtdf_seg_loy.line_item_seq_num = pt.line_item_seq_num
)
WITH DATA PRIMARY INDEX(global_tran_id, business_day_date, line_item_seq_num) ON COMMIT PRESERVE ROWS;
    

COLLECT STATISTICS COLUMN (business_day_date, source_platform_code, rewards_level, merch_price_type) ON sales_rtdf_segment_loy_pt; --cts_segment, rfm_segment,
COLLECT STATISTICS COLUMN (upc_num) ON sales_rtdf_segment_loy_pt;
COLLECT STATISTICS COLUMN (business_day_date, global_tran_id, line_item_seq_num) ON sales_rtdf_segment_loy_pt;
COLLECT STATISTICS COLUMN (intent_store_num) ON  sales_rtdf_segment_loy_pt;


/* step 36
calculate sales data
*/
CREATE MULTISET VOLATILE TABLE sales AS (
SELECT
rtdf.business_day_date AS tran_date  
	--,CAST(EXTRACT(HOUR FROM rtdf.tran_time) AS varchar(15)) AS hour_var
	,st.business_unit_desc
	,CAST('NAP_ERTM' AS VARCHAR(20)) AS data_source
	,CASE WHEN rtdf.line_item_net_amt_currency_code <> rtdf.original_line_item_amt_currency_code AND rtdf.original_line_item_amt_currency_code IS NOT NULL 
		THEN rtdf.original_line_item_amt_currency_code ELSE rtdf.line_item_net_amt_currency_code END AS currency_code 	--change currency code to account for cross border returns
	, rtdf.intent_store_num AS store_num 
	, CASE WHEN psd.div_desc LIKE 'INACT%' THEN 'INACT' ELSE psd.div_desc END AS merch_division
	, CASE WHEN dpt.subdivision_num IN (770, 790, 775, 780, 710, 705, 700, 785) THEN dpt.subdivision_name ELSE NULL END AS merch_subdivision
--break out the 2 largest divisions (Apparel + Shoes) into more helpful chunks (Women/Men/Kid Apparel, Women/Men Specialized, Women/Men/Kid Shoes)
	, rtdf.merch_price_type
	, CASE WHEN st.business_unit_desc IN ('OFFPRICE ONLINE', 'RACK', 'RACK CANADA') THEN th.rack_role_desc ELSE th.nord_role_desc END AS merch_role
	--, cts_segment
	--, rfm_segment
	, rewards_level	
	, coalesce(rtdf.source_platform_code, rj.source_platform_code) as platform
	   --try to id journey FROM RTDF columns (order AND fulfillment type) first, then use OLDF columns to add detail to small amount of orders 
	,CASE
        WHEN line_item_order_type = 'StoreInitStoreTake'        THEN 'StoreTake'
        WHEN line_item_order_type = 'StoreInitSameStrSend'      THEN 'ChargeSend'
        WHEN line_item_fulfillment_type = 'StorePickUp'         THEN 'BOPUS'		
        WHEN line_item_order_type IN ('StoreInitDTCAuto',
                                      'StoreInitDTCManual')     THEN 'DirectToConsumer'
        WHEN COALESCE(rtdf.source_platform_code, rj.source_platform_code, 0) = 'POS' THEN 'DirectToConsumer'
        WHEN line_item_order_type IN ('CustInitPhoneOrder',
                                      'CustInitWebOrder',
                                      'StoreInitPhoneOrder',
                                      'StoreInitWebOrder')      THEN CASE 
	   --first COALESCE is for returns WHERE fulfillment_type is NULL (~30% of 2019 AND 2020 data), fulfillment_type2 is FROM original transaction
	   --second COALESCE is for items released node is NULL but shipped node is not NULL (<1% of data)
		    WHEN COALESCE(rtdf.destination_node_num,rj.destination_node_num,-1) > 0 AND COALESCE(rtdf.destination_node_num,rj.destination_node_num) <> '808' THEN CASE
		    	    WHEN COALESCE(line_item_fulfillment_type, line_item_fulfillment_type2) = 'FulfillmentCenter' THEN 'ShipToStore_from_FulfillmentCenter'
	          	WHEN COALESCE(line_item_fulfillment_type, line_item_fulfillment_type2) = 'StoreShipSend' THEN 'ShipToStore_from_StoreFulfill'
	          	WHEN COALESCE(line_item_fulfillment_type, line_item_fulfillment_type2) = 'VendorDropShip'  	THEN 'ShipToStore_from_DropShip'
				      WHEN COALESCE(rtdf.last_released_node_type_code, rtdf.shipped_node_type_code, rj.last_released_node_type_code) = 'FC' THEN 'ShipToStore_from_FulfillmentCenter'
				      WHEN COALESCE(rtdf.last_released_node_type_code, rtdf.shipped_node_type_code, rj.last_released_node_type_code) = 'DS' THEN 'ShipToStore_from_DropShip'
				      WHEN (COALESCE(rtdf.last_released_node_type_code, rtdf.shipped_node_type_code, rj.last_released_node_type_code) = 'FL' OR COALESCE(rtdf.last_released_node_type_code, rtdf.shipped_node_type_code, rj.last_released_node_type_code) = 'RK') THEN 'ShipToStore_from_StoreFulfill'  
				      ELSE 'ShipToStore_OtherUnknown' END
	      WHEN COALESCE(rtdf.destination_node_num,rj.destination_node_num,-1) <= 0 THEN CASE 
				      WHEN COALESCE(line_item_fulfillment_type, line_item_fulfillment_type2) = 'FulfillmentCenter' THEN 'ShipToHome_from_FulfillmentCenter'
	          	WHEN COALESCE(line_item_fulfillment_type, line_item_fulfillment_type2) = 'StoreShipSend' THEN 'ShipToHome_from_StoreFulfill'
	          	WHEN COALESCE(line_item_fulfillment_type, line_item_fulfillment_type2) = 'VendorDropShip'  	THEN 'ShipToHome_from_DropShip'
	          	WHEN COALESCE(rtdf.last_released_node_type_code, rtdf.shipped_node_type_code, rj.last_released_node_type_code) = 'FC' THEN 'ShipToHome_from_FulfillmentCenter'
				      WHEN COALESCE(rtdf.last_released_node_type_code, rtdf.shipped_node_type_code, rj.last_released_node_type_code) = 'DS' THEN 'ShipToHome_from_DropShip'
				      WHEN (COALESCE(rtdf.last_released_node_type_code, rtdf.shipped_node_type_code, rj.last_released_node_type_code) = 'FL' OR COALESCE(rtdf.last_released_node_type_code, rtdf.shipped_node_type_code, rj.last_released_node_type_code) = 'RK') THEN 'ShipToHome_from_StoreFulfill'
				      ELSE 'ShipToHome_OtherUnknown' END
	        END    
		WHEN COALESCE(rtdf.delivery_method_code, rj.delivery_method_code) = 'PICK' AND COALESCE(rtdf.destination_node_num,rj.destination_node_num) <> '808' AND (COALESCE(rtdf.destination_node_num,rj.destination_node_num) > 0) THEN 'BOPUS'
		ELSE 'OtherUnknown'
        END AS customer_journey
-- Sales measures:
    ,SUM(CASE WHEN dpt.merch_dept_ind = 'Y' AND rtdf.line_item_activity_type_desc = 'SALE' THEN
	         CASE WHEN rtdf.line_item_net_amt_currency_code <> rtdf.original_line_item_amt_currency_code AND rtdf.original_line_item_amt_currency_code IS NOT NULL 
	         THEN rtdf.original_line_item_amt
	         ELSE rtdf.line_net_amt END --Cross-border returns, use original amt 
         ELSE 0 END) AS gross_merch_sales_amt
    ,SUM(CASE WHEN dpt.merch_dept_ind = 'Y' AND rtdf.line_item_activity_type_desc = 'RETURN' THEN
         	CASE WHEN rtdf.line_item_net_amt_currency_code <> rtdf.original_line_item_amt_currency_code AND rtdf.original_line_item_amt_currency_code IS NOT NULL 
         	THEN -1*rtdf.original_line_item_amt 
            ELSE rtdf.line_net_amt END * -1 --Cross-border returns, use original amt AND sign flip
         ELSE 0 END) AS merch_returns_amt                    
    ,gross_merch_sales_amt   - merch_returns_amt   AS net_merch_sales_amt          
    ,SUM(CASE WHEN rtdf.line_item_activity_type_desc = 'SALE' THEN
         	CASE WHEN rtdf.line_item_net_amt_currency_code <> rtdf.original_line_item_amt_currency_code AND rtdf.original_line_item_amt_currency_code IS NOT NULL 
         	THEN rtdf.original_line_item_amt
            ELSE rtdf.line_net_amt END --Cross-border returns, use original amt 
         ELSE 0 END) AS gross_operational_sales_amt
    ,SUM(CASE WHEN rtdf.line_item_activity_type_desc = 'RETURN' THEN
         	CASE WHEN rtdf.line_item_net_amt_currency_code <> rtdf.original_line_item_amt_currency_code AND rtdf.original_line_item_amt_currency_code IS NOT NULL 
         	THEN -1*rtdf.original_line_item_amt
            ELSE rtdf.line_net_amt END * -1 --Cross-border returns, use original amt AND sign flip 
         ELSE 0 END) AS operational_returns_amt
    ,gross_operational_sales_amt   - operational_returns_amt AS net_operational_sales_amt 
    ,SUM(CASE WHEN dpt.merch_dept_ind = 'Y' AND rtdf.line_item_activity_type_desc = 'SALE' THEN rtdf.line_item_quantity ELSE 0 END) AS gross_merch_sales_units
    ,SUM(CASE WHEN dpt.merch_dept_ind = 'Y' AND rtdf.line_item_activity_type_desc = 'RETURN' THEN rtdf.line_item_quantity ELSE 0 END) AS merch_returns_units
    ,gross_merch_sales_units - merch_returns_units AS net_merch_sales_units       
    ,SUM(CASE WHEN rtdf.line_item_activity_type_desc = 'SALE' THEN rtdf.line_item_quantity ELSE 0 END) AS gross_operational_sales_units
    ,SUM(CASE WHEN rtdf.line_item_activity_type_desc = 'RETURN' THEN rtdf.line_item_quantity ELSE 0 END) AS operational_returns_units
    ,gross_operational_sales_units - operational_returns_units AS net_operational_sales_units
FROM  SALES_rtdf_segment_loy_pt  AS rtdf 
INNER JOIN PRD_NAP_USR_VWS.STORE_DIM AS st ON st.store_num = rtdf.intent_store_num
LEFT JOIN (SELECT upc_num, rms_sku_num, epm_sku_num FROM PRD_NAP_USR_VWS.PRODUCT_UPC_DIM QUALIFY Row_Number() OVER (PARTITION BY upc_num ORDER BY dw_batch_date DESC, epm_sku_num DESC) = 1) AS pud  ON pud.upc_num = rtdf.upc_num
LEFT JOIN (SELECT epm_sku_num, epm_style_num, rms_style_num, div_desc, sbclass_num, class_num, dept_num  FROM PRD_NAP_USR_VWS.PRODUCT_SKU_DIM QUALIFY Row_Number() OVER (PARTITION BY epm_sku_num ORDER BY channel_country DESC, dw_batch_date DESC) = 1) AS psd ON psd.epm_sku_num = pud.epm_sku_num
LEFT JOIN (SELECT epm_style_num, class_num, dept_num FROM PRD_NAP_USR_VWS.PRODUCT_STYLE_DIM QUALIFY Row_Number() OVER (PARTITION BY epm_style_num ORDER BY channel_country DESC, dw_batch_date DESC) = 1) AS pdc ON pdc.epm_style_num = psd.epm_style_num
LEFT JOIN t2dl_das_po_visibility.ccs_merch_themes th  ON psd.dept_num = th.dept_idnt AND psd.class_num = th.class_idnt AND psd.sbclass_num = th.sbclass_idnt -- contact Ivie Okieimen for access
LEFT JOIN return_journey_oldf AS rj ON RTDF.business_day_date = rj.return_date AND RTDF.global_tran_id = rj.return_global_tran_id AND RTDF.line_item_seq_num = rj.return_line_item_seq_num 
INNER JOIN PRD_NAP_USR_VWS.DEPARTMENT_DIM  AS dpt  ON dpt.dept_num = COALESCE(pdc.dept_num,rtdf.merch_dept_num) --INNER JOIN excludes non-merch items bc they are missing UPC_num
WHERE 1=1
  AND dpt.DIVISION_NUM NOT IN ('800','900','100') -- Leased, Leased Boutiques, AND Other/Non-Merch. 
  AND st.business_unit_desc <> ('TRUNK CLUB') -- second filter to exclude trunk club (trunk club needs validation). First (above) is: AND intent_store_num NOT IN (141, 173)  
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
HAVING (gross_merch_sales_amt + Abs(merch_returns_amt) + Abs(net_merch_sales_amt) +  
gross_operational_sales_amt + Abs(operational_returns_amt) + Abs(net_operational_sales_amt) +  
gross_merch_sales_units + Abs(merch_returns_units) + Abs(net_merch_sales_units) +  
gross_operational_sales_units + Abs(operational_returns_units) + Abs(net_operational_sales_units)  
) <> 0 --remove rows WITH no actual information
	UNION ALL
--add r.com data before RTDF started capturing data ('2020-08-30')
--before this date r.com sales were based on order_date not tran_date
SELECT 
bdd.day_date AS tran_date
--, 'total_nrhl' AS hour_var
, 'OFFPRICE ONLINE' AS business_unit_desc
, 'NRHL_redshift' AS data_source
, 'USD' AS currency_code
, 828 AS store_num
, 'total_nrhl' AS merch_division
, 'total_nrhl' AS merch_subdivision
, 'total_nrhl' AS merch_price_type	
, 'total_nrhl' AS merch_role
--, 'total_nrhl' AS cts_segment
--, 'total_nrhl' AS rfm_segment
, 'total_nrhl' AS rewards_level	
, 'total_nrhl' AS platform
, 'total_nrhl' AS customer_journey
, bdd.RevenueShipped AS gross_merch_sales_amt
, r.merch_returns_amt
, bdd.RevenueNetsales AS net_merch_sales_amt
, bdd.RevenueShipped AS gross_operational_sales_amt
, r.merch_returns_amt AS operational_returns_amt
, bdd.RevenueNetsales AS net_operational_sales_amt 
, u.units_shipped AS gross_merch_sales_units
, r.merch_returns_units
, u.units_shipped - r.merch_returns_units AS net_merch_sales_units
, u.units_shipped  AS gross_operational_sales_units
, r.merch_returns_units AS operational_returns_units
, u.units_shipped - r.merch_returns_units AS net_operational_sales_units
FROM T2DL_DAS_MOTHERSHIP.nrhl_history_redshift_merch_revenue AS bdd -- original table = T3DL_ACE_OP.redshift_Birdseye_Merch_revenue
LEFT JOIN (SELECT day_date, units_shipped FROM T2DL_DAS_MOTHERSHIP.nrhl_history_redshift_merch_units ) AS u --original table = T3DL_ACE_OP.Redshift_Birdseye_Merch_Units	
ON bdd.day_date = u.day_date
LEFT JOIN (SELECT date_id, SUM(merch_returns_amt) AS merch_returns_amt, SUM(merch_returns_units) AS merch_returns_units FROM T2DL_DAS_MOTHERSHIP.nrhl_history_redshift_cancels_todt_returns GROUP BY 1) AS r	--original table = T3DL_ACE_OP.birdseye_cancels_todt_returns_historical 
ON bdd.day_date = r.date_id
WHERE bdd.day_date BETWEEN '2019-02-03' AND '2020-08-29' --hard coded for historical data
--and bdd.day_date IN ('2019-02-04', '2020-02-04') -- for testing
and bdd.day_date BETWEEN {start_date} AND {end_date} --so historical data doesn't run when not running backfills
)--;
WITH DATA PRIMARY INDEX(tran_date,business_unit_desc,store_num) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (tran_date,business_unit_desc,store_num) ON sales;



DELETE FROM {mothership_t2_schema}.FINANCE_SALES_DEMAND_FACT WHERE tran_date BETWEEN {start_date} AND {end_date};


--------------------------------------------------------------------
/* step 37
final table creation- add demand to sales 
*/
--T2DL_DAS_MOTHERSHIP.FINANCE_SALES_DEMAND_FACT
INSERT INTO {mothership_t2_schema}.FINANCE_SALES_DEMAND_FACT
WITH can as
(
SELECT 
date_id
--, CAST(hour_id AS varchar(15)) AS hour_var 
, SUM(units_cancelled) AS canceled_units
, SUM(revenue_cancelled) AS canceled_demand
FROM T2DL_DAS_MOTHERSHIP.nrhl_history_redshift_cancels_todt_returns --originally T3DL_ACE_OP.birdseye_cancels_todt_returns_historical
WHERE date_id BETWEEN '2019-02-03' AND '2020-11-20'
GROUP BY 1
)
, bd AS 
(
SELECT 
day_date
--, CAST(hour_id AS varchar(15)) AS hour_var 
, SUM(demand) AS demand --incl cancels AND fraud
, SUM(units) AS units --incl cancels AND fraud
FROM T2DL_DAS_MOTHERSHIP.nrhl_history_redshift_birdseye_hourly --originally T3DL_ACE_OP.birdseye_hourly
WHERE day_date BETWEEN '2019-02-03' AND '2020-11-20'
GROUP BY 1
)
, combo AS 
( SELECT
        tran_date
        --, CAST(EXTRACT(HOUR FROM ORDER_TMSTP_PACIFIC) AS varchar(15)) AS hour_var
        , business_unit_desc
        , data_source
        , currency_code
        , store_num_bopus_in_store_demand
        , store_num_bopus_in_digital_demand
        , merch_division
      	, merch_subdivision
      --break out the 2 largest divisions (Apparel + Shoes) into more helpful chunks (Women/Men/Kid Apparel, Women/Men Specialized, Women/Men/Kid Shoes)
      	, merch_price_type	
      	, merch_role
        , null as cts_segment
        , null as rfm_segment
      	, rewards_level	
        , platform --before 2021-03-06 OLDF isn't accurate at device level for r.com
        , customer_journey
        , demand
        , canceled_demand
        , units
        , canceled_units
        , gross_merch_sales_amt
        , merch_returns_amt
        , net_merch_sales_amt
        , gross_merch_sales_units
        , merch_returns_units
        , net_merch_sales_units
        , gross_operational_sales_amt
        , operational_returns_amt
        , net_operational_sales_amt
        , gross_operational_sales_units
        , operational_returns_units
        , net_operational_sales_units
        FROM DEMAND
        	UNION ALL
        --add r.com historical demand data (pre project rocket)- before 2021-03-06 OLDF isn't accurate at device level for r.com
        --before 2020-11-21 OLDF isn't accurate at total demand/units level for r.com
        SELECT 
        CAST(bd.day_date AS date) AS tran_date
        --, bd.hour_var 
        , 'OFFPRICE ONLINE' AS business_unit_desc
        , 'NRHL_redshift' AS data_source
        , 'USD' AS currency_code
        , 828 AS store_num_bopus_in_store_demand
        , 828 AS store_num_bopus_in_digital_demand
        , 'total_nrhl' AS merch_division
        , 'total_nrhl' AS merch_subdivision
        , 'total_nrhl' AS merch_price_type	
        , 'total_nrhl' AS merch_role
        , null AS cts_segment
        , null AS rfm_segment
        , 'total_nrhl' AS rewards_level	
        , 'total_nrhl' AS platform
        , 'total_nrhl' AS customer_journey
        , CAST(demand AS decimal(20,2)) AS demand
        , CAST(canceled_demand AS decimal(20,2)) AS canceled_demand
        , CAST(units AS decimal(20,0)) AS units
        , CAST(canceled_units AS decimal(20,0)) AS canceled_units
        --, CAST(orders AS decimal(38,0)) AS orders 
        , CAST(0.00 AS decimal(20,2)) AS gross_merch_sales_amt
        , CAST(0.00 AS decimal(20,2)) AS merch_returns_amt
        , CAST(0.00 AS decimal(20,2)) AS net_merch_sales_amt
        , CAST(0.00 AS decimal(20,2)) AS gross_merch_sales_units
        , CAST(0.00 AS decimal(20,2)) AS merch_returns_units
        , CAST(0.00 AS decimal(20,2)) AS net_merch_sales_units
        , CAST(0.00 AS decimal(20,2)) AS gross_operational_sales_amt
        , CAST(0.00 AS decimal(20,2)) AS operational_returns_amt
        , CAST(0.00 AS decimal(20,2)) AS net_operational_sales_amt
        , CAST(0.00 AS decimal(20,2)) AS gross_operational_sales_units
        , CAST(0.00 AS decimal(20,2)) AS operational_returns_units
        , CAST(0.00 AS decimal(20,2)) AS net_operational_sales_units
        FROM bd 
        INNER JOIN can 
        on bd.day_date = can.date_id --and bd.hour_var = can.hour_var 
        WHERE bd.day_date BETWEEN '2019-02-03' AND '2020-11-20' 
        --and bd.day_date IN ('2019-02-04', '2020-02-04') --for testing
        AND bd.day_date BETWEEN {start_date} AND {end_date} --so historical data doesn't run when not running backfills
        	UNION ALL
        --sales data
        SELECT
        tran_date
        --, hour_var
        , business_unit_desc
        , data_source
        , currency_code
        , store_num AS store_num_bopus_in_store_demand
        , store_num AS store_num_bopus_in_digital_demand
        , merch_division
        , merch_subdivision
        , merch_price_type	
        , merch_role
        , null as cts_segment
        , null as rfm_segment
        , rewards_level	
        , platform 
        , customer_journey
        , NULL AS demand
        , NULL AS canceled_demand
        , NULL AS units
        , NULL AS canceled_units
        , SUM(gross_merch_sales_amt) AS gross_merch_sales_amt
        , SUM(merch_returns_amt) AS merch_returns_amt
        , SUM(net_merch_sales_amt) AS net_merch_sales_amt
        , SUM(gross_merch_sales_units) AS gross_merch_sales_units
        , SUM(merch_returns_units) AS merch_returns_units
        , SUM(net_merch_sales_units) AS net_merch_sales_units
        , SUM(gross_operational_sales_amt) AS gross_operational_sales_amt
        , SUM(operational_returns_amt) AS operational_returns_amt
        , SUM(net_operational_sales_amt) AS net_operational_sales_amt
        , SUM(gross_operational_sales_units) AS gross_operational_sales_units
        , SUM(operational_returns_units) AS operational_returns_units
        , SUM(net_operational_sales_units) AS net_operational_sales_units
        FROM sales AS fsf
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)
SELECT 
-- Time dimensions:
tran_date
--, hour_var
-- Other dimensions:
, sd.business_unit_desc AS business_unit_desc_bopus_in_store_demand
, sd2.business_unit_desc AS business_unit_desc_bopus_in_digital_demand
, data_source
, currency_code
, combo.store_num_bopus_in_store_demand
, combo.store_num_bopus_in_digital_demand
, merch_division
, merch_subdivision
, merch_price_type	
, merch_role
, cts_segment
, rfm_segment
, rewards_level	
, platform
, customer_journey
-- demand
, CASE WHEN business_unit_desc_bopus_in_store_demand IN ('N.CA', 'N.COM', 'OFFPRICE ONLINE') then SUM(demand)
	ELSE (SUM(CASE WHEN customer_journey IN ('BOPUS', 'DirectToConsumer') then COALESCE(demand,0) ELSE gross_merch_sales_amt END)) 
	END AS demand_amt_bopus_in_store --shift bopus AND DTC demand FROM digital to store. 	
, CASE WHEN business_unit_desc_bopus_in_digital_demand IN ('N.CA', 'N.COM', 'OFFPRICE ONLINE') then SUM(demand)
	ELSE (SUM(CASE WHEN customer_journey IN ('BOPUS', 'DirectToConsumer') then COALESCE(demand,0) ELSE gross_merch_sales_amt END)) 
	END AS demand_amt_bopus_in_digital --shift DTC demand FROM digital to store, keep bopus IN digital.
, SUM(canceled_demand) AS demand_canceled_amt
, CASE WHEN business_unit_desc_bopus_in_store_demand IN ('N.CA', 'N.COM', 'OFFPRICE ONLINE') then SUM(units)
	ELSE (SUM(CASE WHEN customer_journey IN ('BOPUS', 'DirectToConsumer') then COALESCE(units,0) ELSE gross_merch_sales_units END))
	END AS demand_units_bopus_in_store --shift bopus AND DTC units FROM digital to store. 	
, CASE WHEN business_unit_desc_bopus_in_digital_demand IN ('N.CA', 'N.COM', 'OFFPRICE ONLINE') then SUM(units)
	ELSE (SUM(CASE WHEN customer_journey IN ('BOPUS', 'DirectToConsumer') then COALESCE(units,0) ELSE gross_merch_sales_units END))
	END AS demand_units_bopus_in_digital --shift DTC units FROM digital to store, keep bopus IN digital.	
, SUM(canceled_units) AS demand_canceled_units
-- sales
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
, CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM combo
LEFT JOIN PRD_NAP_USR_VWS.STORE_DIM sd ON sd.store_num = combo.store_num_bopus_in_store_demand
LEFT JOIN PRD_NAP_USR_VWS.STORE_DIM sd2 ON sd2.store_num = combo.store_num_bopus_in_digital_demand
--WHERE tran_date IN ('2021-07-01', '2021-07-20', '2021-08-08', '2021-08-31') --for testing
WHERE tran_date BETWEEN {start_date} AND {end_date} --make sure start_date -365 isn't inserted
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
;
 
      
COLLECT STATISTICS COLUMN(tran_date), COLUMN(customer_journey), COLUMN(merch_division)
	, COLUMN(business_unit_desc_bopus_in_store_demand), COLUMN(business_unit_desc_bopus_in_digital_demand)
	, COLUMN(store_num_bopus_in_store_demand), COLUMN(store_num_bopus_in_digital_demand)  ON {mothership_t2_schema}.FINANCE_SALES_DEMAND_FACT;

SET QUERY_BAND = NONE FOR SESSION;