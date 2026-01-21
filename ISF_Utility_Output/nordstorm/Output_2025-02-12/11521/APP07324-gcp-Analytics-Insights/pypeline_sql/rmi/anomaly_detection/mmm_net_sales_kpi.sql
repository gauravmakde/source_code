SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=mmm_net_sales_kpi_11521_ACE_ENG;
     Task_Name=mmm_net_sales_kpi;'
     FOR SESSION VOLATILE;


--T2/Table Name: T2DL_DAS_MOA_KPI.mmm_net_sales_kpi
--Team/Owner: Analytics Engineering
--Date Created/Modified: 2024-12-26
--Note:
-- This table supports the Anamoly Detection Dashboard.

create multiset volatile table _variables as (
 select
 week_start_day_date as start_date,
 week_end_day_date as end_date
 from prd_nap_usr_vws.day_cal_454_dim
 where day_date = {start_date}
) with data primary index(start_date,end_date) on commit preserve rows;




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
	and year(execution_qtr_start_dt) >= (select year(end_date) from _variables)
)
with data primary index(acp_id,engagement_cohort) on commit preserve rows;



----JWN CUSTOMERS WITH ACQUIRED TRANSACTION IDS----
create multiset volatile table customers as (
select distinct
	aare_global_tran_id
	,aare_status_date
	,acp_id
from prd_nap_usr_vws.customer_ntn_status_fact ntn
where
    1=1
	and aare_status_date >= '2021-05-01'
)
with data primary index(acp_id, aare_global_tran_id) on commit preserve rows;



----GENERATING A BASE NET TABLE WITH TRANSACTIONS WITH AN ACP ID----
create multiset volatile table daily_net_base_acp as (
	select
	jogm.business_day_date as business_date
	,jogm.acp_id
	,jogm.global_tran_id
	,case
			when jogm.business_unit_desc = 'OFFPRICE ONLINE' then 'R.COM'
			when jogm.business_unit_desc = 'FULL LINE' then 'NORDSTROM STORE'
			when jogm.business_unit_desc = 'RACK' then 'RACK STORE'
		    else jogm.business_unit_desc
		end as business_unit_desc
	,jogm.intent_store_num as store_num
	,jogm.bill_zip_code
	,case
			when jogm.price_type = 'Regular Price' then 'REGULAR'
			else upper(jogm.price_type)
		end as price_type
	,case when jogm.order_platform_type = 'Mobile App' then 'APP'
	      when jogm.order_platform_type = 'Store POS' then 'POS'
	      when jogm.order_platform_type in ('Phone (Customer Care)', 'BorderFree','Direct to Customer (DTC))') then 'OTHER'
	      when jogm.order_platform_type in ('Mobile Web', 'Desktop/Tablet') then 'WEB'
	      else 'UNKNOWN' end as order_platform_type
	,jogm.loyalty_status
	,jogm.division_name
	,jogm.subdivision_name
	,jogm.line_net_usd_amt
	from prd_nap_jwn_metrics_base_vws.jwn_operational_gmv_metric_vw jogm
	where
		1=1
		and jogm.business_day_date between (select start_date from _variables) and (select end_date from _variables)
		and jogm.acp_id is not null
		and jogm.zero_value_unit_ind ='N'
		and jogm.channel_country = 'US'
) with data primary index(business_date,business_unit_desc,store_num,bill_zip_code,price_type, order_platform_type) on commit preserve rows;



----GENERATING A BASE NET TABLE WITH TRANSACTIONS WITHOUT AN ACP ID----
create multiset volatile table daily_net_base_no_acp as (
	select
	jogm.business_day_date as business_date
	,jogm.acp_id
	,jogm.global_tran_id
	,case
			when jogm.business_unit_desc = 'OFFPRICE ONLINE' then 'R.COM'
			when jogm.business_unit_desc = 'FULL LINE' then 'NORDSTROM STORE'
			when jogm.business_unit_desc = 'RACK' then 'RACK STORE'
		    else jogm.business_unit_desc
		end as business_unit_desc
	,jogm.intent_store_num as store_num
	,jogm.bill_zip_code
	,case
			when jogm.price_type = 'Regular Price' then 'REGULAR'
			else upper(jogm.price_type)
		end as price_type
	,case when jogm.order_platform_type = 'Mobile App' then 'APP'
	      when jogm.order_platform_type = 'Store POS' then 'POS'
	      when jogm.order_platform_type in ('Phone (Customer Care)', 'BorderFree','Direct to Customer (DTC))') then 'OTHER'
	      when jogm.order_platform_type in ('Mobile Web', 'Desktop/Tablet') then 'WEB'
	      else 'UNKNOWN' end as order_platform_type
	,jogm.loyalty_status
	,jogm.division_name
	,jogm.subdivision_name
	,jogm.line_net_usd_amt
	from prd_nap_jwn_metrics_base_vws.jwn_operational_gmv_metric_vw jogm
	where
		1=1
		and jogm.business_day_date between (select start_date from _variables) and (select end_date from _variables)
		and jogm.acp_id is null
		and jogm.zero_value_unit_ind ='N' --remove gift with purchase and beauty (smart) sample
		and jogm.channel_country = 'US'
) with data primary index(business_date,business_unit_desc,store_num,bill_zip_code,price_type, order_platform_type) on commit preserve rows;



----GENERATING FINAL VIEW WITH ALL DESIRED ATTRIBUTES FOR NET DEMAND----
create multiset volatile table daily_net as (
	select
	jogm.business_date
	,jogm.business_unit_desc
	,jogm.store_num
	,jogm.bill_zip_code
	,jogm.price_type
	,jogm.order_platform_type
	,jogm.loyalty_status
	,upper(aec.engagement_cohort) as engagement_cohort
	,case when ntn.aare_global_tran_id is not null then 'NEW'
		  else 'EXISTING' end as cust_status
	,jogm.division_name
	,jogm.subdivision_name
	,sum(jogm.line_net_usd_amt) as jwn_reported_net_sales_amt
	from daily_net_base_acp jogm
	left join aec
		on jogm.acp_id = aec.acp_id
		and jogm.business_date between aec.execution_qtr_start_dt and aec.execution_qtr_end_dt
	left join customers ntn
		on ntn.aare_global_tran_id = jogm.global_tran_id
	where
		1=1
		and jogm.business_date between (select start_date from _variables) and (select end_date from _variables)
	group by 1,2,3,4,5,6,7,8,9,10,11

	UNION ALL

	select
	jogm.business_date
	,jogm.business_unit_desc
	,jogm.store_num
	,jogm.bill_zip_code
	,jogm.price_type
	,jogm.order_platform_type
	,jogm.loyalty_status
	,'UNKNOWN' as engagement_cohort
	,'UNKNOWN' as cust_status
	,jogm.division_name
	,jogm.subdivision_name
	,sum(jogm.line_net_usd_amt) as jwn_reported_net_sales_amt
	from daily_net_base_no_acp jogm
	where
		1=1
		and jogm.business_date between (select start_date from _variables) and (select end_date from _variables)
	group by 1,2,3,4,5,6,7,8,9,10,11
) with data primary index(business_date,business_unit_desc,store_num,bill_zip_code,price_type, order_platform_type) on commit preserve rows;



--DELETING AND INSERTING DATA IN THE NET SALES TABLE
DELETE FROM {kpi_scorecard_t2_schema}.mmm_net_sales_kpi 
WHERE business_date between (select start_date from _variables) and (select end_date from _variables);

INSERT INTO {kpi_scorecard_t2_schema}.mmm_net_sales_kpi
select
business_date,
business_unit_desc,
store_num,
bill_zip_code,
price_type,
order_platform_type,
loyalty_status,
engagement_cohort,
cust_status,
division_name,
subdivision_name,
jwn_reported_net_sales_amt,
current_date as dw_batch_date,
current_timestamp as dw_sys_load_tmstp
from daily_net
WHERE business_date between (select start_date from _variables) and (select end_date from _variables);



SET QUERY_BAND = NONE FOR SESSION;

