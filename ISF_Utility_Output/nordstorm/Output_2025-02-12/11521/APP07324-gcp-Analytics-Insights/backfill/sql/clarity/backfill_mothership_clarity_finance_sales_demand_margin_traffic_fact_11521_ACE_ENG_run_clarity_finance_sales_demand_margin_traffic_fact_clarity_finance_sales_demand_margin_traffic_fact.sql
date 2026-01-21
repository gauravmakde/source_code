SET QUERY_BAND = 'App_ID=APP08176;
     DAG_ID=mothership_clarity_finance_sales_demand_margin_traffic_fact_11521_ACE_ENG;
     Task_Name=clarity_finance_sales_demand_margin_traffic_fact;'
     FOR SESSION VOLATILE; 

/*
PRD_NAP_JWN_METRICS_USR_VWS.finance_sales_demand_margin_traffic_fact
Description - This ddl creates a 7 box (n.com, r.com, n.ca, FULL LINE, FULL LINE CANADA, RACK, RACK CANADA) 
table of daily sales, demand, margin, and traffic split by store, platform (device), and customer categories
Full documenation: https://confluence.nordstrom.com/x/M4fITg
Contacts: Matthew Bond, Analytics

SLA:
I would like an SLA on this of 6am (9am EST) 
but I don't know how to set that up since this job has no schedule since it's downstream of FSDF and that is dependent on raw clarity data
*/


CREATE MULTISET VOLATILE TABLE combo_sum AS ( 
with sales_demand as ( --reassign bopus to store or digital
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
,mrtk_chnl_type_code
,mrtk_chnl_finance_rollup_code
,mrtk_chnl_finance_detail_code
,engagement_cohort
,tran_tender_cash_flag
,tran_tender_check_flag
,tran_tender_nordstrom_card_flag
,tran_tender_non_nordstrom_credit_flag
,tran_tender_non_nordstrom_debit_flag
,tran_tender_nordstrom_gift_card_flag
,tran_tender_nordstrom_note_flag
,tran_tender_paypal_flag
,sum(case when not (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then gross_demand_usd_amt else 0 end) as gross_demand_usd_amt_excl_bopus
,sum(case when (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then gross_demand_usd_amt else 0 end) as bopus_attr_store_gross_demand_usd_amt
,cast(0.00 as decimal(20,2)) as bopus_attr_digital_gross_demand_usd_amt
,sum(case when not (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then gross_demand_units else 0 end) as gross_demand_units_excl_bopus
,sum(case when (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then gross_demand_units else 0 end) as bopus_attr_store_gross_demand_units
,cast(0 as int) as bopus_attr_digital_gross_demand_units
,sum(case when not (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then canceled_gross_demand_usd_amt else 0 end) as canceled_gross_demand_usd_amt_excl_bopus
,sum(case when (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then canceled_gross_demand_usd_amt else 0 end) as bopus_attr_store_canceled_gross_demand_usd_amt
,cast(0.00 as decimal(20,2)) as bopus_attr_digital_canceled_gross_demand_usd_amt
,sum(case when not (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then canceled_gross_demand_units else 0 end) as canceled_gross_demand_units_excl_bopus
,sum(case when (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then canceled_gross_demand_units else 0 end) as bopus_attr_store_canceled_gross_demand_units
,cast(0 as int) as bopus_attr_digital_canceled_gross_demand_units
,sum(case when not (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then reported_demand_usd_amt else 0 end) as reported_demand_usd_amt_excl_bopus
,sum(case when (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then reported_demand_usd_amt else 0 end) as bopus_attr_store_reported_demand_usd_amt
,cast(0.00 as decimal(20,2)) as bopus_attr_digital_reported_demand_usd_amt
,sum(case when not (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then reported_demand_units else 0 end) as reported_demand_units_excl_bopus
,sum(case when (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then reported_demand_units else 0 end) as bopus_attr_store_reported_demand_units
,cast(0 as int) as bopus_attr_digital_reported_demand_units
,sum(case when not (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then canceled_reported_demand_usd_amt else 0 end) as canceled_reported_demand_usd_amt_excl_bopus
,sum(case when (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then canceled_reported_demand_usd_amt else 0 end) as bopus_attr_store_canceled_reported_demand_usd_amt
,cast(0.00 as decimal(20,2)) as bopus_attr_digital_canceled_reported_demand_usd_amt
,sum(case when not (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then canceled_reported_demand_units else 0 end) as canceled_reported_demand_units_excl_bopus
,sum(case when (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then canceled_reported_demand_units else 0 end) as bopus_attr_store_canceled_reported_demand_units
,cast(0 as int) as bopus_attr_digital_canceled_reported_demand_units
,sum(case when not (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then emp_disc_gross_demand_usd_amt else 0 end) as emp_disc_gross_demand_usd_amt_excl_bopus
,sum(case when (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then emp_disc_gross_demand_usd_amt else 0 end) as bopus_attr_store_emp_disc_gross_demand_usd_amt
,cast(0.00 as decimal(20,2)) as bopus_attr_digital_emp_disc_gross_demand_usd_amt
,sum(case when not (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then emp_disc_gross_demand_units else 0 end) as emp_disc_gross_demand_units_excl_bopus
,sum(case when (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then emp_disc_gross_demand_units else 0 end) as bopus_attr_store_emp_disc_gross_demand_units
,cast(0 as int) as bopus_attr_digital_emp_disc_gross_demand_units
,sum(case when not (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then emp_disc_reported_demand_usd_amt else 0 end) as emp_disc_reported_demand_usd_amt_excl_bopus
,sum(case when (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then emp_disc_reported_demand_usd_amt else 0 end) as bopus_attr_store_emp_disc_reported_demand_usd_amt
,cast(0.00 as decimal(20,2)) as bopus_attr_digital_emp_disc_reported_demand_usd_amt
,sum(case when not (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then emp_disc_reported_demand_units else 0 end) as emp_disc_reported_demand_units_excl_bopus
,sum(case when (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then emp_disc_reported_demand_units else 0 end) as bopus_attr_store_emp_disc_reported_demand_units
,cast(0 as int) as bopus_attr_digital_emp_disc_reported_demand_units
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
,cast(0 as int) as orders_count
,cast(0 as int) as canceled_fraud_orders_count
,cast(0 as int) as emp_disc_orders_count
from PRD_NAP_DSA_AI_BASE_VWS.finance_sales_demand_fact fsdf
where 1=1
and business_unit_desc in ('FULL LINE', 'FULL LINE CANADA', 'RACK', 'RACK CANADA', 'MARKETPLACE')
and tran_date between date'2023-01-29' and date'2024-02-03'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
	union all
select
tran_date
,case when (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then 'N.COM' else business_unit_desc end as business_unit_desc
,case when (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then 808 else store_num end as store_num
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
,mrtk_chnl_type_code
,mrtk_chnl_finance_rollup_code
,mrtk_chnl_finance_detail_code
,engagement_cohort
,tran_tender_cash_flag
,tran_tender_check_flag
,tran_tender_nordstrom_card_flag
,tran_tender_non_nordstrom_credit_flag
,tran_tender_non_nordstrom_debit_flag
,tran_tender_nordstrom_gift_card_flag
,tran_tender_nordstrom_note_flag
,tran_tender_paypal_flag
,sum(case when not (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then gross_demand_usd_amt else 0 end) as gross_demand_usd_amt_excl_bopus
,cast(0.00 as decimal(20,2)) as bopus_attr_store_gross_demand_usd_amt
,sum(case when (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then gross_demand_usd_amt else 0 end) as bopus_attr_digital_gross_demand_usd_amt
,sum(case when not (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then gross_demand_units else 0 end) as gross_demand_units_excl_bopus
,cast(0 as int) as bopus_attr_store_gross_demand_units
,sum(case when (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then gross_demand_units else 0 end) as bopus_attr_digital_gross_demand_units
,sum(case when not (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then canceled_gross_demand_usd_amt else 0 end) as canceled_gross_demand_usd_amt_excl_bopus
,cast(0.00 as decimal(20,2)) as bopus_attr_store_canceled_gross_demand_usd_amt
,sum(case when (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then canceled_gross_demand_usd_amt else 0 end) as bopus_attr_digital_canceled_gross_demand_usd_amt
,sum(case when not (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then canceled_gross_demand_units else 0 end) as canceled_gross_demand_units_excl_bopus
,cast(0 as int) as bopus_attr_store_canceled_gross_demand_units
,sum(case when (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then canceled_gross_demand_units else 0 end) as bopus_attr_digital_canceled_gross_demand_units
,sum(case when not (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then reported_demand_usd_amt else 0 end) as reported_demand_usd_amt_excl_bopus
,cast(0.00 as decimal(20,2)) as bopus_attr_store_reported_demand_usd_amt
,sum(case when (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then reported_demand_usd_amt else 0 end) as bopus_attr_digital_reported_demand_usd_amt
,sum(case when not (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then reported_demand_units else 0 end) as reported_demand_units_excl_bopus
,cast(0 as int) as bopus_attr_store_reported_demand_units
,sum(case when (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then reported_demand_units else 0 end) as bopus_attr_digital_reported_demand_units
,sum(case when not (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then canceled_reported_demand_usd_amt else 0 end) as canceled_reported_demand_usd_amt_excl_bopus
,cast(0.00 as decimal(20,2)) as bopus_attr_store_canceled_reported_demand_usd_amt
,sum(case when (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then canceled_reported_demand_usd_amt else 0 end) as bopus_attr_digital_canceled_reported_demand_usd_amt
,sum(case when not (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then canceled_reported_demand_units else 0 end) as canceled_reported_demand_units_excl_bopus
,cast(0 as int) as bopus_attr_store_canceled_reported_demand_units
,sum(case when (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then canceled_reported_demand_units else 0 end) as bopus_attr_digital_canceled_reported_demand_units
,sum(case when not (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then emp_disc_gross_demand_usd_amt else 0 end) as emp_disc_gross_demand_usd_amt_excl_bopus
,cast(0.00 as decimal(20,2)) as bopus_attr_store_emp_disc_gross_demand_usd_amt
,sum(case when (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then emp_disc_gross_demand_usd_amt else 0 end) as bopus_attr_digital_emp_disc_gross_demand_usd_amt
,sum(case when not (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then emp_disc_gross_demand_units else 0 end) as emp_disc_gross_demand_units_excl_bopus
,cast(0 as int) as bopus_attr_store_emp_disc_gross_demand_units
,sum(case when (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then emp_disc_gross_demand_units else 0 end) as bopus_attr_digital_emp_disc_gross_demand_units
,sum(case when not (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then emp_disc_reported_demand_usd_amt else 0 end) as emp_disc_reported_demand_usd_amt_excl_bopus
,cast(0.00 as decimal(20,2)) as bopus_attr_store_emp_disc_reported_demand_usd_amt
,sum(case when (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then emp_disc_reported_demand_usd_amt else 0 end) as bopus_attr_digital_emp_disc_reported_demand_usd_amt
,sum(case when not (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then emp_disc_reported_demand_units else 0 end) as emp_disc_reported_demand_units_excl_bopus
,cast(0 as int) as bopus_attr_store_emp_disc_reported_demand_units
,sum(case when (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')) then emp_disc_reported_demand_units else 0 end) as bopus_attr_digital_emp_disc_reported_demand_units
,sum(case when business_unit_desc in ('OFFPRICE ONLINE', 'N.COM', 'N.CA') then emp_disc_fulfilled_demand_usd_amt else 0 end) as emp_disc_fulfilled_demand_usd_amt
,sum(case when business_unit_desc in ('OFFPRICE ONLINE', 'N.COM', 'N.CA') then emp_disc_fulfilled_demand_units else 0 end) as emp_disc_fulfilled_demand_units
,sum(case when business_unit_desc in ('OFFPRICE ONLINE', 'N.COM', 'N.CA') then emp_disc_op_gmv_usd_amt else 0 end) as emp_disc_op_gmv_usd_amt
,sum(case when business_unit_desc in ('OFFPRICE ONLINE', 'N.COM', 'N.CA') then emp_disc_op_gmv_units else 0 end) as emp_disc_op_gmv_units
,sum(case when business_unit_desc in ('OFFPRICE ONLINE', 'N.COM', 'N.CA') then fulfilled_demand_usd_amt else 0 end) as fulfilled_demand_usd_amt
,sum(case when business_unit_desc in ('OFFPRICE ONLINE', 'N.COM', 'N.CA') then fulfilled_demand_units else 0 end) as fulfilled_demand_units
,sum(case when business_unit_desc in ('OFFPRICE ONLINE', 'N.COM', 'N.CA') then post_fulfill_price_adj_usd_amt else 0 end) as post_fulfill_price_adj_usd_amt
,sum(case when business_unit_desc in ('OFFPRICE ONLINE', 'N.COM', 'N.CA') then same_day_store_return_usd_amt else 0 end) as same_day_store_return_usd_amt
,sum(case when business_unit_desc in ('OFFPRICE ONLINE', 'N.COM', 'N.CA') then same_day_store_return_units else 0 end) as same_day_store_return_units
,sum(case when business_unit_desc in ('OFFPRICE ONLINE', 'N.COM', 'N.CA') then last_chance_usd_amt else 0 end) as last_chance_usd_amt
,sum(case when business_unit_desc in ('OFFPRICE ONLINE', 'N.COM', 'N.CA') then last_chance_units else 0 end) as last_chance_units
,sum(case when business_unit_desc in ('OFFPRICE ONLINE', 'N.COM', 'N.CA') then actual_product_returns_usd_amt else 0 end) as actual_product_returns_usd_amt
,sum(case when business_unit_desc in ('OFFPRICE ONLINE', 'N.COM', 'N.CA') then actual_product_returns_units else 0 end) as actual_product_returns_units
,sum(case when business_unit_desc in ('OFFPRICE ONLINE', 'N.COM', 'N.CA') then op_gmv_usd_amt else 0 end) as op_gmv_usd_amt
,sum(case when business_unit_desc in ('OFFPRICE ONLINE', 'N.COM', 'N.CA') then op_gmv_units else 0 end) as op_gmv_units
,sum(orders_count) as orders_count
,sum(canceled_fraud_orders_count) as canceled_fraud_orders_count
,sum(emp_disc_orders_count) as emp_disc_orders_count
FROM PRD_NAP_DSA_AI_BASE_VWS.finance_sales_demand_fact fsdf
WHERE 1=1
and (business_unit_desc in ('OFFPRICE ONLINE', 'N.COM', 'N.CA') or (business_unit_desc = 'FULL LINE' and record_source = 'O' and platform_code not in ('Direct to Customer (DTC)', 'Store POS')))
and tran_date between date'2023-01-29' and date'2024-02-03'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
)
, visitor_bots as
(
    select
        day_date,
        case when box = 'rcom' then 'OFFPRICE ONLINE' when box = 'ncom' then 'N.COM' else null end as business_unit,
        device_type,
        sum(suspicious_visitors) as suspicious_visitors,
        sum(suspicious_viewing_visitors) as suspicious_viewing_visitors,
        sum(suspicious_adding_visitors) as suspicious_adding_visitors
	from
	    T2DL_DAS_MOTHERSHIP.bot_traffic_fix
	group by 1,2,3
)
, pre_store_traffic as
(
select
store_num,
day_date,
channel,
sum(purchase_trips) as purchase_trips,
sum(case when traffic_source in ('retailnext_cam') then traffic else 0 end) as traffic_rn_cam,
sum(case when traffic_source in ('placer_channel_scaled_to_retailnext', 'placer_closed_store_scaled_to_retailnext',
                                'placer_vertical_interference_scaled_to_retailnext', 'placer_vertical_interference_scaled_to_est_retailnext',
                                'placer_store_scaled_to_retailnext') then traffic else 0 end) as traffic_scaled_placer
from T2DL_DAS_FLS_TRAFFIC_MODEL.store_traffic_daily as stdn
where 1=1
and day_date between '2021-01-31' and current_date -1
and day_date between date'2023-01-29' and date'2024-02-03'
group by 1,2,3
)
, combo AS ( --reassign bopus to store AND digital
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
,mrtk_chnl_type_code
,mrtk_chnl_finance_rollup_code
,mrtk_chnl_finance_detail_code
,engagement_cohort
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
,cast(null as decimal (25,5)) as reported_gmv_usd_amt
,cast(null as integer) as reported_gmv_units
,cast(null as decimal (25,5)) as merch_net_sales_usd_amt
,cast(null as integer) as merch_net_sales_units
,cast(null as decimal (25,5)) as reported_net_sales_usd_amt
,cast(null as integer) as reported_net_sales_units
,cast(null as decimal (25,5)) as variable_cost_usd_amt
,cast(null as decimal (25,5)) as gross_contribution_margin_usd_amt
,cast(null as decimal (25,5)) as net_contribution_margin_usd_amt
,sum(orders_count) as orders_count
,sum(canceled_fraud_orders_count) as canceled_fraud_orders_count
,sum(emp_disc_orders_count) as emp_disc_orders_count
,cast(null as integer) as visitors
,cast(null as integer) as viewing_visitors
,cast(null as integer) as adding_visitors
,cast(null as integer) as ordering_visitors
,cast(null as integer) as sessions
,cast(null as integer) as viewing_sessions
,cast(null as integer) as adding_sessions
,cast(null as integer) as checkout_sessions
,cast(null as integer) as ordering_sessions
,cast(null as integer) as acp_count
,cast(null as integer) as unique_cust_count
,cast(null as integer) as total_pages_visited
,cast(null as integer) as total_unique_pages_visited
,cast(null as integer) as bounced_sessions
,cast(null as integer) as browse_sessions
,cast(null as integer) as search_sessions
,cast(null as integer) as wishlist_sessions
,cast(null as integer) as store_purchase_trips
,cast(null as integer) as store_traffic
from sales_demand
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
	union all
select  --add margin
business_day_date as tran_date
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
,mrtk_chnl_type_code
,mrtk_chnl_finance_rollup_code
,mrtk_chnl_finance_detail_code
,engagement_cohort
,tran_tender_cash_flag
,tran_tender_check_flag
,tran_tender_nordstrom_card_flag
,tran_tender_non_nordstrom_credit_flag
,tran_tender_non_nordstrom_debit_flag
,tran_tender_nordstrom_gift_card_flag
,tran_tender_nordstrom_note_flag
,tran_tender_paypal_flag
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
,sum(reported_gmv_usd_amt) as reported_gmv_usd_amt
,sum(reported_gmv_units) as reported_gmv_units
,sum(merch_net_sales_usd_amt) as merch_net_sales_usd_amt
,sum(merch_net_sales_units) as merch_net_sales_units
,sum(reported_net_sales_usd_amt) as reported_net_sales_usd_amt
,sum(reported_net_sales_units) as reported_net_sales_units
,sum(variable_cost_usd_amt) as variable_cost_usd_amt
,sum(gross_contribution_margin_usd_amt) as gross_contribution_margin_usd_amt
,sum(net_contribution_margin_usd_amt) as net_contribution_margin_usd_amt
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
,null as store_purchase_trips
,null as store_traffic
from PRD_NAP_DSA_AI_BASE_VWS.finance_margin_fact as margin
where 1=1
and tran_date between date'2023-01-29' and date'2024-02-03'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
    union all
select  --add funnel
activity_date as tran_date
,case when CHANNEL_COUNTRY = 'US' and purchase_channel = 'FULL_LINE' then 'N.COM'
    when CHANNEL_COUNTRY = 'CA' and purchase_channel = 'FULL_LINE' then 'N.CA'
    when CHANNEL_COUNTRY = 'US' and purchase_channel = 'RACK' then 'OFFPRICE ONLINE'
    else null end as business_unit_desc
,case when business_unit_desc = 'N.COM' then 808
	when business_unit_desc = 'N.CA' then 867
	when business_unit_desc = 'OFFPRICE ONLINE' then 828
	else null end as store_num
,'other' as bill_zip_code
,case when CHANNEL_COUNTRY = 'US' then 'USD'
    when CHANNEL_COUNTRY = 'CA' then 'CAD'
    else null end as line_item_currency_code
,'Z' as record_source
,case when platform = 'CSR_PHONE' then 'Other'
	when platform = 'ANDROID' then 'Android'
	when platform = 'MOW' then 'Mobile Web'
	when platform = 'WEB' then 'Desktop/Tablet'
	when platform = 'POS' then 'Store POS'
	when platform = 'IOS' then 'IOS'
	when platform = 'THIRD_PARTY_VENDOR' then 'Other'
	else platform end as platform_code
,'total_digital' as loyalty_status
,'total_digital' as buyerflow_code
,'N' as AARE_acquired_ind
,'N' as AARE_activated_ind
,'N' as AARE_retained_ind
,'N' as AARE_engaged_ind
,'total_digital' as mrtk_chnl_type_code
,'total_digital' as mrtk_chnl_finance_rollup_code
,'total_digital' as mrtk_chnl_finance_detail_code
,'total_digital' as engagement_cohort
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
,sum(visitor_count)-max(coalesce(b.suspicious_visitors,0)) as visitors
,sum(product_view_visitors)-max(coalesce(b.suspicious_viewing_visitors,0)) as viewing_visitors
,sum(cart_add_visitors)-max(coalesce(b.suspicious_adding_visitors,0)) as adding_visitors
,sum(ordering_visitors) as ordering_visitors
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
,null as store_purchase_trips
,null as store_traffic
from PRD_NAP_BASE_VWS.VISITOR_FUNNEL_FACT vff
left join visitor_bots b on vff.activity_date = b.day_date and business_unit_desc = b.business_unit and vff.platform = b.device_type
where 1=1
and activity_date between '2021-01-31' and current_date-1
and activity_date between date'2023-01-29' and date'2024-02-03'
and ((business_unit_desc in ('N.COM','N.CA')) or (business_unit_desc = 'OFFPRICE ONLINE' and activity_date >= '2021-06-13')) --r.com traffic before END of project rocket isn't valid
and bot_traffic_ind <> 'Y'
and platform <> 'POS' --remove DTC, it should be accounted for in store traffic
--ideally move bopus too, but it's not able to be split by orders either- at least that's consistent by conversion rate (orders/visitors)
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
	union all
select  --add sessions
activity_date_pacific as tran_date
,case when channel = 'NORDSTROM' then 'N.COM'
    when channel in ('NORDSTROM_RACK', 'HAUTELOOK') then 'OFFPRICE ONLINE'
    else null end as business_unit_desc
,case when business_unit_desc = 'N.COM' then 808
	when business_unit_desc = 'OFFPRICE ONLINE' then 828
	else null end as store_num
,'other' as bill_zip_code
,'USD' as line_item_currency_code
,'Z' as record_source
,case when experience = 'CARE_PHONE' then 'Other'
	when experience = 'ANDROID_APP' then 'Android'
	when experience = 'MOBILE_WEB' then 'Mobile Web'
	when experience = 'DESKTOP_WEB' then 'Desktop/Tablet'
	when experience = 'POINT_OF_SALE' then 'Store POS'
	when experience = 'IOS_APP' then 'IOS'
	when experience = 'VENDOR' then 'Other'
	else experience end as platform_code
,'total_digital' as loyalty_status
,'total_digital' as buyerflow_code
,'N' as AARE_acquired_ind
,'N' as AARE_activated_ind
,'N' as AARE_retained_ind
,'N' as AARE_engaged_ind
,mrkt_type as mrtk_chnl_type_code
,finance_rollup as mrtk_chnl_finance_rollup_code
,finance_detail as mrtk_chnl_finance_detail_code
,'total_digital' as engagement_cohort
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
,count(distinct (session_id)) AS sessions
,count(distinct CASE WHEN product_views >= 1 THEN session_id ELSE null END) AS viewing_sessions
,count(distinct CASE WHEN cart_adds >= 1 THEN session_id ELSE null END) AS adding_sessions
,null as checkout_sessions
,count(distinct CASE WHEN web_orders >= 1 THEN session_id ELSE null END) AS ordering_sessions
,null as acp_count
,null as unique_cust_count
,sum(product_views) as total_pages_visited
,null as total_unique_pages_visited
,count(distinct CASE WHEN bounce_flag = 1 THEN session_id ELSE null END) AS bounced_sessions
,null as browse_sessions
,null as search_sessions
,null as wishlist_sessions
,null as store_purchase_trips
,null as store_traffic
from T2DL_DAS_SESSIONS.dior_session_fact
where activity_date_pacific between '2022-01-30' and current_date -1
--'2022-01-31' is first date of complete sessions data. Adding sessions is empty until 2022-02-09
and activity_date_pacific between date'2023-01-29' and date'2024-02-03'
and business_unit_desc not in ('TRUNK_CLUB')
and ((coalesce(active_session_flag,0) = 1 or web_orders >= 1) or --remove inactive sessions (mostly bots) but add back a few inactive sessions that do place orders (maybe using an adblocker)
    ((web_orders >= 1 or cart_adds >= 1 or product_views >= 1) and activity_date_pacific between '2022-01-30' and '2022-03-22') or
    ((web_orders >= 1 or cart_adds >= 1 or product_views >= 1) and activity_date_pacific between '2022-05-15' and '2022-05-17') or
    ((web_orders >= 1 or cart_adds >= 1 or product_views >= 1) and activity_date_pacific between '2022-08-28' and '2022-09-08') or
    ((web_orders >= 1 or cart_adds >= 1 or product_views >= 1) and activity_date_pacific between '2022-11-06' and '2022-11-07'))
--for fy 2022 dates with bad data (Sessionizer broke or p0) estimate sessions using view/add/order sessions- backfill may come eventually from session team
and platform_code <> 'Store POS'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
    union all
select   --add store traffic
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
,'total_store' as mrtk_chnl_type_code
,'total_store' as mrtk_chnl_finance_rollup_code
,'total_store' as mrtk_chnl_finance_detail_code
,'total_store' as engagement_cohort
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
,sum(case when traffic_rn_cam > 0 and store_num not in (4,5,358) then traffic_rn_cam else traffic_scaled_placer end) as store_traffic
from pre_store_traffic as st 
where 1=1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
	union all
--r.com hard coded historical funnel
select
cast(day_date as date) as tran_date
,'OFFPRICE ONLINE' as business_unit_desc
,828 as store_num
,'other' as bill_zip_code
,'USD' as line_item_currency_code
,'Z' as record_source
,'Android' as platform_code
,'total_digital' as loyalty_status
,'total_digital' as buyerflow_code
,'N' as AARE_acquired_ind
,'N' as AARE_activated_ind
,'N' as AARE_retained_ind
,'N' as AARE_engaged_ind
,'total_digital' as mrtk_chnl_type_code
,'total_digital' as mrtk_chnl_finance_rollup_code
,'total_digital' as mrtk_chnl_finance_detail_code
,'total_digital' as engagement_cohort
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
,sum(visitors_android) as visitors
,sum(viewing_visitors_android) as viewing_visitors
,sum(adding_visitors_android) as adding_visitors
,sum(ordering_visitors_android) as ordering_visitors
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
,null as store_purchase_trips
,null as store_traffic
from T2DL_DAS_MOTHERSHIP.daily_vistors_GA_VFF fun --historical data from T3DL_ACE_OP.daily_vistors_GA_VFF fun
where 1=1
and tran_date between '2021-01-31' and '2021-06-12' --hard code for historical data
and tran_date between date'2023-01-29' and date'2024-02-03' --so historical doesn't run in daily job
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
    union all
select
cast(day_date as date) as tran_date
,'OFFPRICE ONLINE' as business_unit_desc
,828 as store_num
,'other' as bill_zip_code
,'USD' as line_item_currency_code
,'Z' as record_source
,'Mobile Web' as platform_code
,'total_digital' as loyalty_status
,'total_digital' as buyerflow_code
,'N' as AARE_acquired_ind
,'N' as AARE_activated_ind
,'N' as AARE_retained_ind
,'N' as AARE_engaged_ind
,'total_digital' as mrtk_chnl_type_code
,'total_digital' as mrtk_chnl_finance_rollup_code
,'total_digital' as mrtk_chnl_finance_detail_code
,'total_digital' as engagement_cohort
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
,sum(visitors_mow) as visitors
,sum(viewing_visitors_mow) as viewing_visitors
,sum(adding_visitors_mow) as adding_visitors
,sum(ordering_visitors_mow) as ordering_visitors
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
,null as store_purchase_trips
,null as store_traffic
from T2DL_DAS_MOTHERSHIP.daily_vistors_GA_VFF fun --historical data from T3DL_ACE_OP.daily_vistors_GA_VFF fun
where 1=1
and tran_date between '2021-01-31' and '2021-06-12' --hard code for historical data
and tran_date between date'2023-01-29' and date'2024-02-03' --so historical doesn't run in daily job
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
    union all
select
cast(day_date as date) as tran_date
,'OFFPRICE ONLINE' as business_unit_desc
,828 as store_num
,'other' as bill_zip_code
,'USD' as line_item_currency_code
,'Z' as record_source
,'IOS' as platform_code
,'total_digital' as loyalty_status
,'total_digital' as buyerflow_code
,'N' as AARE_acquired_ind
,'N' as AARE_activated_ind
,'N' as AARE_retained_ind
,'N' as AARE_engaged_ind
,'total_digital' as mrtk_chnl_type_code
,'total_digital' as mrtk_chnl_finance_rollup_code
,'total_digital' as mrtk_chnl_finance_detail_code
,'total_digital' as engagement_cohort
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
,sum(visitors_ios) as visitors
,sum(viewing_visitors_ios) as viewing_visitors
,sum(adding_visitors_ios) as adding_visitors
,sum(ordering_visitors_ios) as ordering_visitors
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
,null as store_purchase_trips
,null as store_traffic
from T2DL_DAS_MOTHERSHIP.daily_vistors_GA_VFF fun --historical data from T3DL_ACE_OP.daily_vistors_GA_VFF fun
where 1=1
and tran_date between '2021-01-31' and '2021-06-12' --hard code for historical data
and tran_date between date'2023-01-29' and date'2024-02-03' --so historical doesn't run in daily job
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
    union all
select
cast(day_date as date) as tran_date
,'OFFPRICE ONLINE' as business_unit_desc
,828 as store_num
,'other' as bill_zip_code
,'USD' as line_item_currency_code
,'Z' as record_source
,'Desktop/Tablet' as platform_code
,'total_digital' as loyalty_status
,'total_digital' as buyerflow_code
,'N' as AARE_acquired_ind
,'N' as AARE_activated_ind
,'N' as AARE_retained_ind
,'N' as AARE_engaged_ind
,'total_digital' as mrtk_chnl_type_code
,'total_digital' as mrtk_chnl_finance_rollup_code
,'total_digital' as mrtk_chnl_finance_detail_code
,'total_digital' as engagement_cohort
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
,sum(visitors_web) as visitors
,sum(viewing_visitors_web) as viewing_visitors
,sum(adding_visitors_web) as adding_visitors
,sum(ordering_visitors_web) as ordering_visitors
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
,null as store_purchase_trips
,null as store_traffic
from T2DL_DAS_MOTHERSHIP.daily_vistors_GA_VFF fun --historical data from T3DL_ACE_OP.daily_vistors_GA_VFF fun
where 1=1
and tran_date between '2021-01-31' and '2021-06-12' --hard code for historical data
and tran_date between date'2023-01-29' and date'2024-02-03' --so historical doesn't run in daily job
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
)
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
,mrtk_chnl_type_code
,mrtk_chnl_finance_rollup_code
,mrtk_chnl_finance_detail_code
,engagement_cohort
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
,current_timestamp as dw_sys_load_tmstp
from combo
where 1=1
and tran_date between date'2023-01-29' and date'2024-02-03'
and tran_date between '2021-01-31' and current_date -1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
) 
WITH DATA  NO PRIMARY INDEX  ON COMMIT PRESERVE ROWS;


DELETE FROM PRD_NAP_DSA_AI_BASE_VWS.finance_sales_demand_margin_traffic_fact WHERE tran_date BETWEEN date'2023-01-29' AND date'2024-02-03';


INSERT INTO PRD_NAP_DSA_AI_BASE_VWS.finance_sales_demand_margin_traffic_fact
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
 FROM 
COMBO_sum;


SET QUERY_BAND = NONE FOR SESSION;