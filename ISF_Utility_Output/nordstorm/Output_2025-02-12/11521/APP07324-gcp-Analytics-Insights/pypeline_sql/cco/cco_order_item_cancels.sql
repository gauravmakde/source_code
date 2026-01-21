SET QUERY_BAND = 'App_ID=APP08240;
     DAG_ID=cco_cust_chan_yr_attributes_11521_ACE_ENG;
     Task_Name=cco_order_item_cancels;'
     FOR SESSION VOLATILE;



/*
CCO Strategy Cancels SQL file 
This script writes to table T2DL_DAS_STRATEGY.cco_order_item_cancels

Modified March 2023 for IsF
*/



create multiset volatile table date_lookup as (
select distinct 
  date'{cancels_start_date}' yr1_start_dt --2019-02-03
  ,date'{cancels_end_date}' yrN_start_dt  --2023-04-29
from prd_nap_usr_vws.day_cal
--group by 1
) with data primary index(yr1_start_dt) on commit preserve rows;

/***************************************************************************************/
/********* Step 3: Re-populate the order-line table *********/
/***************************************************************************************/
 
create multiset volatile table orders_extract as 
(select
order_num, 
order_line_id, 
order_line_num, 
order_date_pacific, 
order_line_amount_usd,
order_line_employee_discount_amount_usd, 
order_line_quantity, 
requested_max_promise_date_pacific,
upc_code, 
destination_node_num, 
destination_city, 
destination_state, 
destination_zip_code, 
destination_country_code,
requested_level_of_service_code,
delivery_method_code ,
promise_type_code,
backorder_display_ind, 
source_channel_country_code, 
source_channel_code, 
source_platform_code, 
source_store_num, 
canceled_date_pacific,
cancel_reason_code,
shopper_id
from prd_nap_usr_vws.order_line_detail_fact as f 
where cancel_reason_code is not null
and cancel_reason_code not in ('FRAUD_CHECK_FAILED', 'FRAUD_MANUAL_CANCEL')--, 'CUSTOMER_REQUEST', 'PAYMENT_DECLINE', 'UNKNOWN')
and order_date_pacific BETWEEN (select yr1_start_dt from date_lookup) AND (select yrN_start_dt from date_lookup))
with data primary index(order_num, order_line_id) on commit preserve rows;

COLLECT STATISTICS 
COLUMN(order_num, order_line_id) 
ON orders_extract;

create multiset volatile table order_rls as 
(SELECT olr.order_num,
       olr.order_line_id
 FROM prd_nap_usr_vws.order_line_release_detail_fact olr
   WHERE olr.carrier_code in ('NORD', NULL)
   AND olr.order_date_pacific BETWEEN (select yr1_start_dt from date_lookup) AND (select yrN_start_dt from date_lookup))
with data primary index(order_num, order_line_id) on commit preserve rows;

COLLECT STATISTICS 
COLUMN(order_num, order_line_id) 
ON order_rls;
 

create multiset volatile table sarf_acp_ids as 
(SELECT sarf.shopper_id, sarf.acp_id     
 FROM t2dl_das_sales_returns.sales_and_returns_fact  sarf
   WHERE  sarf.shopper_id is not null
   and sarf.acp_id is not null 
   and business_day_date <= current_date()
   QUALIFY ROW_NUMBER() OVER(PARTITION BY sarf.shopper_id ORDER BY sarf.acp_id) = 1)
with data primary index(shopper_id) on commit preserve rows;

COLLECT STATISTICS 
column(shopper_id)
ON sarf_acp_ids;


create multiset volatile table acx_acp_ids as 
(SELECT acx.program_index_id, acx.acp_id     
 FROM prd_nap_usr_vws.acp_analytical_program_xref as acx 
   WHERE 1=1
   and acx.program_name = 'web'
   QUALIFY ROW_NUMBER() OVER(PARTITION BY acx.program_index_id ORDER BY acx.acp_id) = 1)
with data primary index(program_index_id) on commit preserve rows;

COLLECT STATISTICS 
column(program_index_id)
ON acx_acp_ids;


create multiset volatile table orders  as 
(select
f.order_num, f.order_line_id, order_line_num, order_date_pacific, order_line_amount_usd
    , case when order_line_employee_discount_amount_usd is null then 0 else 1 end as employee_discount_flag
    , order_line_quantity, requested_max_promise_date_pacific
    , upc_code, destination_node_num, destination_city, destination_state, destination_zip_code, destination_country_code
    , CASE WHEN TRIM(UPPER(f.promise_type_code)) = 'SAME_DAY_COURIER' THEN 'SAME_DAY_DELIVERY'
            WHEN f.requested_level_of_service_code = '07' THEN 'NEXT_DAY_BOPUS' --Part of "Next-Day Pickup"
            WHEN UPPER(COALESCE(F.delivery_method_code,'-1')) = 'PICK' THEN 'SAME_DAY_BOPUS'
            WHEN olrd.order_line_id IS NOT NULL AND f.requested_level_of_service_code = '11' THEN 'FC_BOPUS' -- see note at top
            WHEN f.requested_level_of_service_code = '11' THEN 'NEXT_DAY_SHIP_TO_STORE' --Part of "Next-Day Pickup"
            WHEN COALESCE(f.destination_node_num,-1) > 0 AND UPPER(COALESCE(F.delivery_method_code,'-1')) <> 'PICK' AND F.requested_level_of_service_code = '42' THEN 'FREE_2DAY_SHIP_TO_STORE' --Part of "Next-Day Pickup"
            WHEN f.requested_level_of_service_code = '42' THEN 'FREE_2DAY_DELIVERY'
            WHEN (substring(UPPER(f.promise_type_code),1,3) IN ('ONE','TWO','EXP') OR UPPER(f.promise_type_code) LIKE '%BUSINESSDAY%') AND COALESCE(f.destination_node_num,-1) > 0 --may need to move up in case statement
            THEN 'PAID_EXPEDITED_SHIP_TO_STORE'
            WHEN (substring(UPPER(f.promise_type_code),1,3) IN ('ONE','TWO','EXP') OR UPPER(f.promise_type_code) LIKE '%BUSINESSDAY%') AND COALESCE(f.destination_node_num,-1) < 0 --may need to move up in case statement
            THEN 'PAID_EXPEDITED_SHIP_TO_HOME'
            WHEN COALESCE(f.destination_node_num,-1) > 0 AND UPPER(COALESCE(F.delivery_method_code,'-1')) <> 'PICK' THEN 'SHIP_TO_STORE'
    ELSE 'SHIP_TO_HOME' END AS item_delivery_method
    , backorder_display_ind, source_channel_country_code, source_channel_code, source_platform_code, source_store_num, canceled_date_pacific
    , cancel_reason_code
    , shopper_id
from orders_extract as f
LEFT JOIN  
       order_rls as olrd
         ON f.order_num = olrd.order_num  
		 AND f.order_line_id = olrd.order_line_id 
 where f.shopper_id is not null
 and f.shopper_id <> 'unknown')       
with data primary index(order_num) on commit preserve rows;

COLLECT STATISTICS 
COLUMN(order_num, order_line_id) 
ON orders_extract;


DELETE FROM {cco_t2_schema}.cco_order_item_cancels all;
 

INSERT INTO {cco_t2_schema}.cco_order_item_cancels
 select
	  coalesce(sarf.acp_id,acx.acp_id) as oic_acp_id 
    , orders.order_num, orders.order_line_id, orders.order_line_num, order_date_pacific, order_line_amount_usd
    , orders.employee_discount_flag, order_line_quantity, requested_max_promise_date_pacific
    , upc_code, orders.destination_node_num, destination_city, destination_state, orders.destination_zip_code  
	, destination_country_code, item_delivery_method
    , backorder_display_ind , source_channel_country_code, source_channel_code, orders.source_platform_code
    , source_store_num, canceled_date_pacific, cancel_reason_code
from orders 
left join sarf_acp_ids sarf
    on orders.shopper_id = sarf.shopper_Id
left join acx_acp_ids as acx 
    on orders.shopper_id = acx.program_index_id  
where oic_acp_id is not null;

COLLECT STATISTICS 
COLUMN(acp_id), COLUMN(order_num), COLUMN(order_line_id), 
COLUMN(order_line_num), COLUMN(order_date_pacific) 
ON {cco_t2_schema}.cco_order_item_cancels;


 
SET QUERY_BAND = NONE FOR SESSION;