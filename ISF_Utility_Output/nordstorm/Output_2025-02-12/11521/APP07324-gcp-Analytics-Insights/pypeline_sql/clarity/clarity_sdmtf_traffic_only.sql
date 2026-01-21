SET QUERY_BAND = 'App_ID=APP08176;
     DAG_ID=mothership_clarity_FSDMTF_traffic_only_11521_ACE_ENG;
     Task_Name=mothership_clarity_FSDMTF_traffic_only;'
     FOR SESSION VOLATILE; 

/*
PRD_NAP_JWN_METRICS_USR_VWS.finance_sales_demand_margin_traffic_fact
Description - updates table above only for store traffic data (which changes frequently or has issues with timing delays)
Full documenation: https://confluence.nordstrom.com/x/M4fITg
Contacts: Matthew Bond, Analytics
*/


CREATE MULTISET VOLATILE TABLE pre_store_traffic AS (
select
store_num,
day_date,
channel,
sum(purchase_trips) as purchase_trips,
sum(case when traffic_source in ('retailnext_cam') then traffic else 0 end) as traffic_rn_cam,
sum(case when traffic_source in ('placer_channel_scaled_to_retailnext', 'placer_complex_scaled_to_retailnext', 'placer_complex_scaled_to_est_retailnext', 'placer_store_scaled_to_retailnext') then traffic else 0 end) as traffic_scaled_placer
from {fls_traffic_model_t2_schema}.store_traffic_daily as stdn
where 1=1
and day_date between '2021-01-31' and current_date -1
and day_date between {start_date} and {end_date}
group by 1,2,3
) 
WITH DATA PRIMARY INDEX(store_num, day_date) ON COMMIT PRESERVE ROWS;


--load data (except store traffic) from SDMTF table and union in fresh traffic data from the source table
CREATE MULTISET VOLATILE TABLE fsdmtf AS (
select
tran_date
,business_unit_desc
,store_num
,bill_zip_code
,line_item_currency_code
,record_source
,platform_code
,loyalty_status
,buyerflow_code
,AARE_acquired_ind
,AARE_activated_ind
,AARE_retained_ind
,AARE_engaged_ind
,engagement_cohort
,mrtk_chnl_type_code
,mrtk_chnl_finance_rollup_code
,mrtk_chnl_finance_detail_code
,tran_tender_cash_flag
,tran_tender_check_flag
,tran_tender_nordstrom_card_flag
,tran_tender_non_nordstrom_credit_flag
,tran_tender_non_nordstrom_debit_flag
,tran_tender_nordstrom_gift_card_flag
,tran_tender_nordstrom_note_flag
,tran_tender_paypal_flag
,gross_demand_usd_amt_excl_bopus
,bopus_attr_store_gross_demand_usd_amt
,bopus_attr_digital_gross_demand_usd_amt
,gross_demand_units_excl_bopus
,bopus_attr_store_gross_demand_units
,bopus_attr_digital_gross_demand_units
,canceled_gross_demand_usd_amt_excl_bopus
,bopus_attr_store_canceled_gross_demand_usd_amt
,bopus_attr_digital_canceled_gross_demand_usd_amt
,canceled_gross_demand_units_excl_bopus
,bopus_attr_store_canceled_gross_demand_units
,bopus_attr_digital_canceled_gross_demand_units
,reported_demand_usd_amt_excl_bopus
,bopus_attr_store_reported_demand_usd_amt
,bopus_attr_digital_reported_demand_usd_amt
,reported_demand_units_excl_bopus
,bopus_attr_store_reported_demand_units
,bopus_attr_digital_reported_demand_units
,canceled_reported_demand_usd_amt_excl_bopus
,bopus_attr_store_canceled_reported_demand_usd_amt
,bopus_attr_digital_canceled_reported_demand_usd_amt
,canceled_reported_demand_units_excl_bopus
,bopus_attr_store_canceled_reported_demand_units
,bopus_attr_digital_canceled_reported_demand_units
,emp_disc_gross_demand_usd_amt_excl_bopus
,bopus_attr_store_emp_disc_gross_demand_usd_amt
,bopus_attr_digital_emp_disc_gross_demand_usd_amt
,emp_disc_gross_demand_units_excl_bopus
,bopus_attr_store_emp_disc_gross_demand_units
,bopus_attr_digital_emp_disc_gross_demand_units
,emp_disc_reported_demand_usd_amt_excl_bopus
,bopus_attr_store_emp_disc_reported_demand_usd_amt
,bopus_attr_digital_emp_disc_reported_demand_usd_amt
,emp_disc_reported_demand_units_excl_bopus
,bopus_attr_store_emp_disc_reported_demand_units
,bopus_attr_digital_emp_disc_reported_demand_units
,emp_disc_fulfilled_demand_usd_amt
,emp_disc_fulfilled_demand_units
,emp_disc_op_gmv_usd_amt
,emp_disc_op_gmv_units
,fulfilled_demand_usd_amt
,fulfilled_demand_units
,post_fulfill_price_adj_usd_amt
,same_day_store_return_usd_amt
,same_day_store_return_units
,last_chance_usd_amt
,last_chance_units
,actual_product_returns_usd_amt
,actual_product_returns_units
,op_gmv_usd_amt
,op_gmv_units
,reported_gmv_usd_amt
,reported_gmv_units
,merch_net_sales_usd_amt
,merch_net_sales_units
,reported_net_sales_usd_amt
,reported_net_sales_units
,variable_cost_usd_amt
,gross_contribution_margin_usd_amt
,net_contribution_margin_usd_amt
,orders_count
,canceled_fraud_orders_count
,emp_disc_orders_count
,visitors
,viewing_visitors
,adding_visitors
,ordering_visitors
,sessions
,viewing_sessions
,adding_sessions
,checkout_sessions
,ordering_sessions
,acp_count
,unique_cust_count
,total_pages_visited
,total_unique_pages_visited
,bounced_sessions
,browse_sessions
,search_sessions
,wishlist_sessions
,cast(null as integer) as store_purchase_trips
,cast(null as integer) as store_traffic
,dw_sys_load_tmstp
from {clarity_schema}.finance_sales_demand_margin_traffic_fact
where 1=1
and tran_date between {start_date} and {end_date}
and tran_date between '2021-01-31' and current_date -1
  union all
select   
day_date as tran_date
,channel as business_unit_desc
,store_num
,'other' as bill_zip_code
,case when channel like '%Canada' then 'CAD' else 'USD' end as line_item_currency_code
,'Z' as record_source
,'total_store' as platform_code
,'total_store' as loyalty_status
,'total_store' as buyerflow_code
,'N' as AARE_acquired_ind
,'N' as AARE_activated_ind
,'N' as AARE_retained_ind
,'N' as AARE_engaged_ind
,'total_store' as engagement_cohort
,'total_store' as mrtk_chnl_type_code
,'total_store' as mrtk_chnl_finance_rollup_code
,'total_store' as mrtk_chnl_finance_detail_code
,'N' as tran_tender_cash_flag
,'N' as tran_tender_check_flag
,'N' as tran_tender_nordstrom_card_flag
,'N' as tran_tender_non_nordstrom_credit_flag
,'N' as tran_tender_non_nordstrom_debit_flag
,'N' as tran_tender_nordstrom_gift_card_flag
,'N' as tran_tender_nordstrom_note_flag
,'N' as tran_tender_paypal_flag
,null as gross_demand_usd_amt_excl_bopus
,null as bopus_attr_store_gross_demand_usd_amt
,null as bopus_attr_digital_gross_demand_usd_amt
,null as gross_demand_units_excl_bopus
,null as bopus_attr_store_gross_demand_units
,null as bopus_attr_digital_gross_demand_units
,null as canceled_gross_demand_usd_amt_excl_bopus
,null as bopus_attr_store_canceled_gross_demand_usd_amt
,null as bopus_attr_digital_canceled_gross_demand_usd_amt
,null as canceled_gross_demand_units_excl_bopus
,null as bopus_attr_store_canceled_gross_demand_units
,null as bopus_attr_digital_canceled_gross_demand_units
,null as reported_demand_usd_amt_excl_bopus
,null as bopus_attr_store_reported_demand_usd_amt
,null as bopus_attr_digital_reported_demand_usd_amt
,null as reported_demand_units_excl_bopus
,null as bopus_attr_store_reported_demand_units
,null as bopus_attr_digital_reported_demand_units
,null as canceled_reported_demand_usd_amt_excl_bopus
,null as bopus_attr_store_canceled_reported_demand_usd_amt
,null as bopus_attr_digital_canceled_reported_demand_usd_amt
,null as canceled_reported_demand_units_excl_bopus
,null as bopus_attr_store_canceled_reported_demand_units
,null as bopus_attr_digital_canceled_reported_demand_units
,null as emp_disc_gross_demand_usd_amt_excl_bopus
,null as bopus_attr_store_emp_disc_gross_demand_usd_amt
,null as bopus_attr_digital_emp_disc_gross_demand_usd_amt
,null as emp_disc_gross_demand_units_excl_bopus
,null as bopus_attr_store_emp_disc_gross_demand_units
,null as bopus_attr_digital_emp_disc_gross_demand_units
,null as emp_disc_reported_demand_usd_amt_excl_bopus
,null as bopus_attr_store_emp_disc_reported_demand_usd_amt
,null as bopus_attr_digital_emp_disc_reported_demand_usd_amt
,null as emp_disc_reported_demand_units_excl_bopus
,null as bopus_attr_store_emp_disc_reported_demand_units
,null as bopus_attr_digital_emp_disc_reported_demand_units
,null as emp_disc_fulfilled_demand_usd_amt
,null as emp_disc_fulfilled_demand_units
,null as emp_disc_op_gmv_usd_amt
,null as emp_disc_op_gmv_units
,null as fulfilled_demand_usd_amt
,null as fulfilled_demand_units
,null as post_fulfill_price_adj_usd_amt
,null as same_day_store_return_usd_amt
,null as same_day_store_return_units
,null as last_chance_usd_amt
,null as last_chance_units
,null as actual_product_returns_usd_amt
,null as actual_product_returns_units
,null as op_gmv_usd_amt
,null as op_gmv_units
,null as reported_gmv_usd_amt
,null as reported_gmv_units
,null as merch_net_sales_usd_amt
,null as merch_net_sales_units
,null as reported_net_sales_usd_amt
,null as reported_net_sales_units
,null as variable_cost_usd_amt
,null as gross_contribution_margin_usd_amt
,null as net_contribution_margin_usd_amt
,null as orders_count
,null as canceled_fraud_orders_count
,null as emp_disc_orders_count
,null as visitors
,null as viewing_visitors
,null as adding_visitors
,null as ordering_visitors
,null as sessions
,null as viewing_sessions
,null as adding_sessions
,null as checkout_sessions
,null as ordering_sessions
,null as acp_count
,null as unique_cust_count
,null as total_pages_visited
,null as total_unique_pages_visited
,null as bounced_sessions
,null as browse_sessions
,null as search_sessions
,null as wishlist_sessions
,sum(purchase_trips) as store_purchase_trips
,sum(case when traffic_rn_cam > 0 then traffic_rn_cam else traffic_scaled_placer end) as store_traffic
,current_timestamp as dw_sys_load_tmstp
from pre_store_traffic as st 
where 1=1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
) 
WITH DATA  NO PRIMARY INDEX  ON COMMIT PRESERVE ROWS;


DELETE FROM {clarity_schema}.finance_sales_demand_margin_traffic_fact WHERE tran_date BETWEEN {start_date} AND {end_date};


INSERT INTO {clarity_schema}.finance_sales_demand_margin_traffic_fact
(
  tran_date
  ,business_unit_desc
  ,store_num
  ,bill_zip_code
  ,line_item_currency_code
  ,record_source
  ,platform_code
  ,loyalty_status
  ,buyerflow_code
  ,AARE_acquired_ind
  ,AARE_activated_ind
  ,AARE_retained_ind
  ,AARE_engaged_ind
  ,engagement_cohort
  ,mrtk_chnl_type_code
  ,mrtk_chnl_finance_rollup_code
  ,mrtk_chnl_finance_detail_code
  ,tran_tender_cash_flag
  ,tran_tender_check_flag
  ,tran_tender_nordstrom_card_flag
  ,tran_tender_non_nordstrom_credit_flag
  ,tran_tender_non_nordstrom_debit_flag
  ,tran_tender_nordstrom_gift_card_flag
  ,tran_tender_nordstrom_note_flag
  ,tran_tender_paypal_flag
  ,gross_demand_usd_amt_excl_bopus
  ,bopus_attr_store_gross_demand_usd_amt
  ,bopus_attr_digital_gross_demand_usd_amt
  ,gross_demand_units_excl_bopus
  ,bopus_attr_store_gross_demand_units
  ,bopus_attr_digital_gross_demand_units
  ,canceled_gross_demand_usd_amt_excl_bopus
  ,bopus_attr_store_canceled_gross_demand_usd_amt
  ,bopus_attr_digital_canceled_gross_demand_usd_amt
  ,canceled_gross_demand_units_excl_bopus
  ,bopus_attr_store_canceled_gross_demand_units
  ,bopus_attr_digital_canceled_gross_demand_units
  ,reported_demand_usd_amt_excl_bopus
  ,bopus_attr_store_reported_demand_usd_amt
  ,bopus_attr_digital_reported_demand_usd_amt
  ,reported_demand_units_excl_bopus
  ,bopus_attr_store_reported_demand_units
  ,bopus_attr_digital_reported_demand_units
  ,canceled_reported_demand_usd_amt_excl_bopus
  ,bopus_attr_store_canceled_reported_demand_usd_amt
  ,bopus_attr_digital_canceled_reported_demand_usd_amt
  ,canceled_reported_demand_units_excl_bopus
  ,bopus_attr_store_canceled_reported_demand_units
  ,bopus_attr_digital_canceled_reported_demand_units
  ,emp_disc_gross_demand_usd_amt_excl_bopus
  ,bopus_attr_store_emp_disc_gross_demand_usd_amt
  ,bopus_attr_digital_emp_disc_gross_demand_usd_amt
  ,emp_disc_gross_demand_units_excl_bopus
  ,bopus_attr_store_emp_disc_gross_demand_units
  ,bopus_attr_digital_emp_disc_gross_demand_units
  ,emp_disc_reported_demand_usd_amt_excl_bopus
  ,bopus_attr_store_emp_disc_reported_demand_usd_amt
  ,bopus_attr_digital_emp_disc_reported_demand_usd_amt
  ,emp_disc_reported_demand_units_excl_bopus
  ,bopus_attr_store_emp_disc_reported_demand_units
  ,bopus_attr_digital_emp_disc_reported_demand_units
  ,emp_disc_fulfilled_demand_usd_amt
  ,emp_disc_fulfilled_demand_units
  ,emp_disc_op_gmv_usd_amt
  ,emp_disc_op_gmv_units
  ,fulfilled_demand_usd_amt
  ,fulfilled_demand_units
  ,post_fulfill_price_adj_usd_amt
  ,same_day_store_return_usd_amt
  ,same_day_store_return_units
  ,last_chance_usd_amt
  ,last_chance_units
  ,actual_product_returns_usd_amt
  ,actual_product_returns_units
  ,op_gmv_usd_amt
  ,op_gmv_units
  ,reported_gmv_usd_amt
  ,reported_gmv_units
  ,merch_net_sales_usd_amt
  ,merch_net_sales_units
  ,reported_net_sales_usd_amt
  ,reported_net_sales_units
  ,variable_cost_usd_amt
  ,gross_contribution_margin_usd_amt
  ,net_contribution_margin_usd_amt
  ,orders_count
  ,canceled_fraud_orders_count
  ,emp_disc_orders_count
  ,visitors
  ,viewing_visitors
  ,adding_visitors
  ,ordering_visitors
  ,sessions
  ,viewing_sessions
  ,adding_sessions
  ,checkout_sessions
  ,ordering_sessions
  ,acp_count
  ,unique_cust_count
  ,total_pages_visited
  ,total_unique_pages_visited
  ,bounced_sessions
  ,browse_sessions
  ,search_sessions
  ,wishlist_sessions
  ,store_purchase_trips
  ,store_traffic
  ,dw_sys_load_tmstp
  )
  SELECT
  tran_date
  ,business_unit_desc
  ,store_num
  ,bill_zip_code
  ,line_item_currency_code
  ,record_source
  ,platform_code
  ,loyalty_status
  ,buyerflow_code
  ,AARE_acquired_ind
  ,AARE_activated_ind
  ,AARE_retained_ind
  ,AARE_engaged_ind
  ,engagement_cohort
  ,mrtk_chnl_type_code
  ,mrtk_chnl_finance_rollup_code
  ,mrtk_chnl_finance_detail_code
  ,tran_tender_cash_flag
  ,tran_tender_check_flag
  ,tran_tender_nordstrom_card_flag
  ,tran_tender_non_nordstrom_credit_flag
  ,tran_tender_non_nordstrom_debit_flag
  ,tran_tender_nordstrom_gift_card_flag
  ,tran_tender_nordstrom_note_flag
  ,tran_tender_paypal_flag
  ,sum(gross_demand_usd_amt_excl_bopus) as gross_demand_usd_amt_excl_bopus
  ,sum(bopus_attr_store_gross_demand_usd_amt) as bopus_attr_store_gross_demand_usd_amt
  ,sum(bopus_attr_digital_gross_demand_usd_amt) as bopus_attr_digital_gross_demand_usd_amt
  ,sum(gross_demand_units_excl_bopus) as gross_demand_units_excl_bopus
  ,sum(bopus_attr_store_gross_demand_units) as bopus_attr_store_gross_demand_units
  ,sum(bopus_attr_digital_gross_demand_units) as bopus_attr_digital_gross_demand_units
  ,sum(canceled_gross_demand_usd_amt_excl_bopus) as canceled_gross_demand_usd_amt_excl_bopus
  ,sum(bopus_attr_store_canceled_gross_demand_usd_amt) as bopus_attr_store_canceled_gross_demand_usd_amt
  ,sum(bopus_attr_digital_canceled_gross_demand_usd_amt) as bopus_attr_digital_canceled_gross_demand_usd_amt
  ,sum(canceled_gross_demand_units_excl_bopus) as canceled_gross_demand_units_excl_bopus
  ,sum(bopus_attr_store_canceled_gross_demand_units) as bopus_attr_store_canceled_gross_demand_units
  ,sum(bopus_attr_digital_canceled_gross_demand_units) as bopus_attr_digital_canceled_gross_demand_units
  ,sum(reported_demand_usd_amt_excl_bopus) as reported_demand_usd_amt_excl_bopus
  ,sum(bopus_attr_store_reported_demand_usd_amt) as bopus_attr_store_reported_demand_usd_amt
  ,sum(bopus_attr_digital_reported_demand_usd_amt) as bopus_attr_digital_reported_demand_usd_amt
  ,sum(reported_demand_units_excl_bopus) as reported_demand_units_excl_bopus
  ,sum(bopus_attr_store_reported_demand_units) as bopus_attr_store_reported_demand_units
  ,sum(bopus_attr_digital_reported_demand_units) as bopus_attr_digital_reported_demand_units
  ,sum(canceled_reported_demand_usd_amt_excl_bopus) as canceled_reported_demand_usd_amt_excl_bopus
  ,sum(bopus_attr_store_canceled_reported_demand_usd_amt) as bopus_attr_store_canceled_reported_demand_usd_amt
  ,sum(bopus_attr_digital_canceled_reported_demand_usd_amt) as bopus_attr_digital_canceled_reported_demand_usd_amt
  ,sum(canceled_reported_demand_units_excl_bopus) as canceled_reported_demand_units_excl_bopus
  ,sum(bopus_attr_store_canceled_reported_demand_units) as bopus_attr_store_canceled_reported_demand_units
  ,sum(bopus_attr_digital_canceled_reported_demand_units) as bopus_attr_digital_canceled_reported_demand_units
  ,sum(emp_disc_gross_demand_usd_amt_excl_bopus) as emp_disc_gross_demand_usd_amt_excl_bopus
  ,sum(bopus_attr_store_emp_disc_gross_demand_usd_amt) as bopus_attr_store_emp_disc_gross_demand_usd_amt
  ,sum(bopus_attr_digital_emp_disc_gross_demand_usd_amt) as bopus_attr_digital_emp_disc_gross_demand_usd_amt
  ,sum(emp_disc_gross_demand_units_excl_bopus) as emp_disc_gross_demand_units_excl_bopus
  ,sum(bopus_attr_store_emp_disc_gross_demand_units) as bopus_attr_store_emp_disc_gross_demand_units
  ,sum(bopus_attr_digital_emp_disc_gross_demand_units) as bopus_attr_digital_emp_disc_gross_demand_units
  ,sum(emp_disc_reported_demand_usd_amt_excl_bopus) as emp_disc_reported_demand_usd_amt_excl_bopus
  ,sum(bopus_attr_store_emp_disc_reported_demand_usd_amt) as bopus_attr_store_emp_disc_reported_demand_usd_amt
  ,sum(bopus_attr_digital_emp_disc_reported_demand_usd_amt) as bopus_attr_digital_emp_disc_reported_demand_usd_amt
  ,sum(emp_disc_reported_demand_units_excl_bopus) as emp_disc_reported_demand_units_excl_bopus
  ,sum(bopus_attr_store_emp_disc_reported_demand_units) as bopus_attr_store_emp_disc_reported_demand_units
  ,sum(bopus_attr_digital_emp_disc_reported_demand_units) as bopus_attr_digital_emp_disc_reported_demand_units
  ,sum(emp_disc_fulfilled_demand_usd_amt) as emp_disc_fulfilled_demand_usd_amt
  ,sum(emp_disc_fulfilled_demand_units) as emp_disc_fulfilled_demand_units
  ,sum(emp_disc_op_gmv_usd_amt) as emp_disc_op_gmv_usd_amt
  ,sum(emp_disc_op_gmv_units) as emp_disc_op_gmv_units
  ,sum(fulfilled_demand_usd_amt) as fulfilled_demand_usd_amt
  ,sum(fulfilled_demand_units) as fulfilled_demand_units
  ,sum(post_fulfill_price_adj_usd_amt) as post_fulfill_price_adj_usd_amt
  ,sum(same_day_store_return_usd_amt) as same_day_store_return_usd_amt
  ,sum(same_day_store_return_units) as same_day_store_return_units
  ,sum(last_chance_usd_amt) as last_chance_usd_amt
  ,sum(last_chance_units) as last_chance_units
  ,sum(actual_product_returns_usd_amt) as actual_product_returns_usd_amt
  ,sum(actual_product_returns_units) as actual_product_returns_units
  ,sum(op_gmv_usd_amt) as op_gmv_usd_amt
  ,sum(op_gmv_units) as op_gmv_units
  ,sum(reported_gmv_usd_amt) as reported_gmv_usd_amt
  ,sum(reported_gmv_units) as reported_gmv_units
  ,sum(merch_net_sales_usd_amt) as merch_net_sales_usd_amt
  ,sum(merch_net_sales_units) as merch_net_sales_units
  ,sum(reported_net_sales_usd_amt) as reported_net_sales_usd_amt
  ,sum(reported_net_sales_units) as reported_net_sales_units
  ,sum(variable_cost_usd_amt) as variable_cost_usd_amt
  ,sum(gross_contribution_margin_usd_amt) as gross_contribution_margin_usd_amt
  ,sum(net_contribution_margin_usd_amt) as net_contribution_margin_usd_amt
  ,sum(orders_count) as orders_count
  ,sum(canceled_fraud_orders_count) as canceled_fraud_orders_count
  ,sum(emp_disc_orders_count) as emp_disc_orders_count
  ,sum(visitors) as visitors
  ,sum(viewing_visitors) as viewing_visitors
  ,sum(adding_visitors) as adding_visitors
  ,sum(ordering_visitors) as ordering_visitors
  ,sum(sessions) as sessions
  ,sum(viewing_sessions) as viewing_sessions
  ,sum(adding_sessions) as adding_sessions
  ,sum(checkout_sessions) as checkout_sessions
  ,sum(ordering_sessions) as ordering_sessions
  ,sum(acp_count) as acp_count
  ,sum(unique_cust_count) as unique_cust_count
  ,sum(total_pages_visited) as total_pages_visited
  ,sum(total_unique_pages_visited) as total_unique_pages_visited
  ,sum(bounced_sessions) as bounced_sessions
  ,sum(browse_sessions) as browse_sessions
  ,sum(search_sessions) as search_sessions
  ,sum(wishlist_sessions) as wishlist_sessions
  ,sum(store_purchase_trips) as store_purchase_trips
  ,sum(store_traffic) as store_traffic
  ,max(dw_sys_load_tmstp) as dw_sys_load_tmstp
 FROM 
fsdmtf
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
;


SET QUERY_BAND = NONE FOR SESSION;