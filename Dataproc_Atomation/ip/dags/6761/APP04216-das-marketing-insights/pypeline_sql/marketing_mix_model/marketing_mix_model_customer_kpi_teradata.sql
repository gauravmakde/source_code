SET QUERY_BAND = 'App_ID=app04216; DAG_ID=marketing_mix_model_customer_kpi_teradata_6761_DAS_MARKETING_das_marketing_insights; Task_Name=marketing_mix_model_customer_kpi_teradata_job;'
FOR SESSION VOLATILE;

ET;

----JWN CUSTOMERS WITH THEIR AEC BUCKET----
create multiset volatile table aec as (
select distinct
acp_id
,execution_qtr_start_dt
,execution_qtr_end_dt
,engagement_cohort
from {t2dl_aec_schema}.audience_engagement_cohorts
where
	1=1
	and year(execution_qtr_start_dt) >= year(current_date)
)
with data primary index(acp_id,engagement_cohort) on commit preserve rows;

ET;

----JWN CUSTOMERS WITH ACQUIRED TRANSACTION IDS----
create multiset volatile table customers as (
select distinct
	aare_global_tran_id
	,aare_status_date
	,acp_id
from {db_env}_nap_usr_vws.customer_ntn_status_fact ntn
where
	aare_status_date between (current_date - 14) and (current_date - 1)
)
with data primary index(acp_id, aare_global_tran_id) on commit preserve rows;

ET;

----GENERATING FINAL VIEW WITH ALL DESIRED ATTRIBUTES AND METRICS----

create multiset volatile table daily_customers as (

select
 dtl.demand_date as activity_date
 ,case
	when dtl.line_item_order_type = 'CUSTINITWEBORDER'
		and dtl.line_item_fulfillment_type = 'STOREPICKxUP'
		and dtl.line_item_currency_code = 'USD' then 808
   else dtl.intent_store_num end as store_num
,case
 	when str.business_unit_desc = 'OFFPRICE ONLINE' then 'R.COM'
	when str.business_unit_desc = 'FULL LINE' then 'NORDSTROM STORE'
	when str.business_unit_desc = 'RACK' then 'RACK STORE'
  else str.business_unit_desc end as business_unit_desc
,dtl.tran_type_code
,dtl.acp_id
,upper(aec.engagement_cohort) as engagement_cohort
,case
	when dtl.order_platform_type = 'Mobile App' then 'APP'
	when dtl.order_platform_type = 'Store POS' then 'POS'
	when dtl.order_platform_type in ('Phone (Customer Care)', 'BorderFree','Direct to Customer (DTC))') then 'OTHER'
	when dtl.order_platform_type in ('Mobile Web', 'Desktop/Tablet') then 'WEB'
  else 'UNKNOWN' end as order_platform_type
,case
    when ntn.acp_id is not null then 'NEW'
    when dtl.acp_id is null then 'UNKNOWN'
  else 'EXISTING' end as cust_status
,dtl.loyalty_status
from {db_env}_NAP_JWN_METRICS_BASE_VWS.JWN_OPERATIONAL_GMV_METRIC_VW  as dtl
left join {db_env}_nap_usr_vws.store_dim as str
   on dtl.intent_store_num = str.store_num
   and str.business_unit_desc in ('FULL LINE','N.COM','OFFPRICE ONLINE','RACK')
left join aec
  on aec.acp_id = dtl.acp_id
  and dtl.demand_date between aec.execution_qtr_start_dt and aec.execution_qtr_end_dt
left join customers ntn 
  on dtl.global_tran_id = ntn.aare_global_tran_id
where 
	dtl.demand_date between (current_date - 14) and (current_date - 1)
	and dtl.acp_id is not NULL 
	and dtl.channel_country = 'US'
	and dtl.business_unit_desc in ('FULL LINE','N.COM','OFFPRICE ONLINE','RACK')
	and dtl.record_source = 'S'
	UNION ALL 
	
select  
 dtl.demand_date as activity_date
 ,case
	when dtl.line_item_order_type = 'CUSTINITWEBORDER'
		and dtl.line_item_fulfillment_type = 'STOREPICKxUP'
		and dtl.line_item_currency_code = 'USD' then 808
   else dtl.intent_store_num end as store_num
,case
 	when str.business_unit_desc = 'OFFPRICE ONLINE' then 'R.COM'
	when str.business_unit_desc = 'FULL LINE' then 'NORDSTROM STORE'
	when str.business_unit_desc = 'RACK' then 'RACK STORE'
  else str.business_unit_desc end as business_unit_desc
,dtl.tran_type_code
,dtl.acp_id
,'UNKNOWN' as engagement_cohort
,case
	when dtl.order_platform_type = 'Mobile App' then 'APP'
	when dtl.order_platform_type = 'Store POS' then 'POS'
	when dtl.order_platform_type in ('Phone (Customer Care)', 'BorderFree','Direct to Customer (DTC))') then 'OTHER'
	when dtl.order_platform_type in ('Mobile Web', 'Desktop/Tablet') then 'WEB'
  else 'UNKNOWN' end as order_platform_type
,'UNKNOWN' as cust_status
,case when dtl.loyalty_status is null then 'UNKNOWN' else dtl.loyalty_status end as loyalty_status
from {db_env}_NAP_JWN_METRICS_BASE_VWS.JWN_OPERATIONAL_GMV_METRIC_VW dtl
left join {db_env}_nap_usr_vws.store_dim as str
   on dtl.intent_store_num = str.store_num
   and str.business_unit_desc in ('FULL LINE','N.COM','OFFPRICE ONLINE','RACK')
where 
	dtl.demand_date between (current_date - 14) and (current_date - 1)
	and dtl.acp_id IS NULL 
	and dtl.channel_country = 'US'		
	and dtl.business_unit_desc in ('FULL LINE','N.COM','OFFPRICE ONLINE','RACK')
	and dtl.record_source = 'S'
) with data primary index (activity_date,store_num,business_unit_desc,engagement_cohort,loyalty_status) on commit preserve rows;

ET;

DELETE FROM {proto_schema}.MMM_CUSTOMER_KPI_LDG ALL;

INSERT INTO {proto_schema}.MMM_CUSTOMER_KPI_LDG
select
activity_date,
store_num,
business_unit_desc,
tran_type_code,
engagement_cohort,
order_platform_type,
cust_status,
loyalty_status,
count(distinct acp_id) as cust_cnt,
current_date as dw_batch_date,
current_timestamp as dw_sys_load_tmstp
from daily_customers
group by 1,2,3,4,5,6,7,8
having count(distinct acp_id) > 0;

ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;