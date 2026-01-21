SET QUERY_BAND = 'App_ID=app04216; DAG_ID=marketing_mix_model_finance_kpi_teradata_6761_DAS_MARKETING_das_marketing_insights; Task_Name=marketing_mix_model_finance_kpi_teradata_job;'
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
    1=1
	and aare_status_date >= '2021-05-01'
)
with data primary index(acp_id, aare_global_tran_id) on commit preserve rows;

ET;

----ADDING MSRP AMT BY SKU----
create multiset volatile table msrp as (
select
	distinct
	rms_sku_num
	,msrp_amt
from
	{db_env}_nap_usr_vws.product_sku_dim psd
where
	channel_country = 'US'
)
with data primary index(rms_sku_num,msrp_amt) on commit preserve rows;

ET;

----GENERATING A BASE GROSS TABLE WITH TRANSACTIONS WITH AN ACP ID----
create multiset volatile table daily_gross_base_acp as (
	select
	jdm.demand_date as tran_date
	,jdm.acp_id
	,jdm.global_tran_id
	,jdm.rms_sku_num
	,case
			when jdm.business_unit_desc = 'OFFPRICE ONLINE' then 'R.COM'
			when jdm.business_unit_desc = 'FULL LINE' then 'NORDSTROM STORE'
			when jdm.business_unit_desc = 'RACK' then 'RACK STORE'
		    else jdm.business_unit_desc
		end as business_unit_desc
	,jdm.intent_store_num as store_num
	,case
			when jdm.bill_zip_code is null then 'NOT_APPLICABLE'
			else jdm.bill_zip_code
		end as bill_zip_code
	,case
			when jdm.price_type = 'Regular Price' then 'REGULAR'
			else upper(jdm.price_type)
		end as price_type
	,case when jdm.order_platform_type = 'Mobile App' then 'APP'
	      when jdm.order_platform_type = 'Store POS' then 'POS'
	      when jdm.order_platform_type in ('Phone (Customer Care)', 'BorderFree','Direct to Customer (DTC))') then 'OTHER'
	      when jdm.order_platform_type in ('Mobile Web', 'Desktop/Tablet') then 'WEB'
	      else 'UNKNOWN' end as order_platform_type
	,jdm.loyalty_status
	,jdm.division_name
	,jdm.subdivision_name
	,jdm.jwn_reported_demand_usd_amt
	,jdm.demand_units
	from {db_env}_nap_jwn_metrics_base_vws.jwn_demand_metric_vw jdm
	where
		1=1
		and jdm.demand_date between (current_date - 14) and (current_date - 1)
		and jdm.acp_id is not null
		and jdm.zero_value_unit_ind ='N' --remove gift with purchase and beauty (smart) sample
		and jdm.channel_country = 'US'
) with data primary index(tran_date,business_unit_desc,store_num,bill_zip_code,price_type, order_platform_type) on commit preserve rows;

ET;

----GENERATING A BASE GROSS TABLE WITH TRANSACTIONS WITHOUT AN ACP ID----
create multiset volatile table daily_gross_base_no_acp as (
	select
	jdm.demand_date as tran_date
	,jdm.acp_id
	,jdm.global_tran_id
	,jdm.rms_sku_num
	,case
			when jdm.business_unit_desc = 'OFFPRICE ONLINE' then 'R.COM'
			when jdm.business_unit_desc = 'FULL LINE' then 'NORDSTROM STORE'
			when jdm.business_unit_desc = 'RACK' then 'RACK STORE'
		    else jdm.business_unit_desc
		end as business_unit_desc
	,jdm.intent_store_num as store_num
	,case
			when jdm.bill_zip_code is null then 'NOT_APPLICABLE'
			else jdm.bill_zip_code
		end as bill_zip_code
	,case
			when jdm.price_type = 'Regular Price' then 'REGULAR'
			else upper(jdm.price_type)
		end as price_type
	,case when jdm.order_platform_type = 'Mobile App' then 'APP'
	      when jdm.order_platform_type = 'Store POS' then 'POS'
	      when jdm.order_platform_type in ('Phone (Customer Care)', 'BorderFree','Direct to Customer (DTC))') then 'OTHER'
	      when jdm.order_platform_type in ('Mobile Web', 'Desktop/Tablet') then 'WEB'
	      else 'UNKNOWN' end as order_platform_type
	,jdm.loyalty_status
	,jdm.division_name
	,jdm.subdivision_name
	,jdm.jwn_reported_demand_usd_amt
	,jdm.demand_units
	from {db_env}_nap_jwn_metrics_base_vws.jwn_demand_metric_vw jdm
	where
		1=1
		and jdm.demand_date between (current_date - 14) and (current_date - 1)
		and jdm.acp_id is null
		and jdm.zero_value_unit_ind ='N'
		and jdm.channel_country = 'US'
) with data primary index(tran_date,business_unit_desc,store_num,bill_zip_code,price_type, order_platform_type) on commit preserve rows;

ET;

----GENERATING FINAL VIEW WITH ALL DESIRED ATTRIBUTES FOR GROSS DEMAND----
create multiset volatile table daily_gross as (
select
	jdm.tran_date
	,jdm.business_unit_desc
	,jdm.store_num
	,jdm.bill_zip_code
	,jdm.price_type
	,jdm.order_platform_type
	,jdm.loyalty_status
	,upper(aec.engagement_cohort) as engagement_cohort
	,case when ntn.aare_global_tran_id is not null then 'NEW'
		  else 'EXISTING' end as cust_status
	,jdm.division_name
	,jdm.subdivision_name
	,sum(case
			when msrp.msrp_amt is null
			and jdm.price_type = 'REGULAR' then jwn_reported_demand_usd_amt
		 else msrp.msrp_amt end) as total_msrp_amt
	,sum(jdm.jwn_reported_demand_usd_amt) as jwn_reported_gross_demand_amt
	,sum(jdm.demand_units) as jwn_demand_units
	from daily_gross_base_acp jdm
	left join aec
		on jdm.acp_id = aec.acp_id
		and jdm.tran_date between aec.execution_qtr_start_dt and aec.execution_qtr_end_dt
	left join customers ntn
		on ntn.aare_global_tran_id = jdm.global_tran_id
	left join msrp msrp
		on msrp.rms_sku_num = jdm.rms_sku_num
	where
		1=1
		and jdm.tran_date between (current_date - 14) and (current_date - 1)
	group by 1,2,3,4,5,6,7,8,9,10,11

	UNION ALL

	select
	jdm.tran_date
	,jdm.business_unit_desc
	,jdm.store_num
	,jdm.bill_zip_code
	,jdm.price_type
	,jdm.order_platform_type
	,jdm.loyalty_status
	,'UNKNOWN' as engagement_cohort
	,'UNKNOWN' as cust_status
	,jdm.division_name
	,jdm.subdivision_name
	,sum(case
			when msrp.msrp_amt is null
			and jdm.price_type = 'REGULAR' then jwn_reported_demand_usd_amt
		 else msrp.msrp_amt end) as total_msrp_amt
	,sum(jdm.jwn_reported_demand_usd_amt) as jwn_reported_gross_demand_amt
	,sum(jdm.demand_units) as jwn_demand_units
	from daily_gross_base_no_acp jdm
	left join msrp msrp
		on msrp.rms_sku_num = jdm.rms_sku_num
	where
		1=1
		and jdm.tran_date between (current_date - 14) and (current_date - 1)
	group by 1,2,3,4,5,6,7,8,9,10,11

) with data primary index(tran_date,business_unit_desc,store_num,bill_zip_code,price_type, order_platform_type) on commit preserve rows;

ET;

--DELETING AND INSERTING DATA IN THE GROSS LANDING TABLE
DELETE FROM {proto_schema}.MMM_FINANCE_GROSS_DEMAND_KPI_LDG ALL;

INSERT INTO {proto_schema}.MMM_FINANCE_GROSS_DEMAND_KPI_LDG
select
tran_date,
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
total_msrp_amt,
jwn_reported_gross_demand_amt,
jwn_demand_units,
current_date as dw_batch_date,
current_timestamp as dw_sys_load_tmstp
from daily_gross;

ET;

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
	from {db_env}_nap_jwn_metrics_base_vws.jwn_operational_gmv_metric_vw jogm
	where
		1=1
		and jogm.business_day_date between (current_date - 14) and (current_date - 1)
		and jogm.acp_id is not null
		and jogm.zero_value_unit_ind ='N'
		and jogm.channel_country = 'US'
) with data primary index(business_date,business_unit_desc,store_num,bill_zip_code,price_type, order_platform_type) on commit preserve rows;

ET;

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
	from {db_env}_nap_jwn_metrics_base_vws.jwn_operational_gmv_metric_vw jogm
	where
		1=1
		and jogm.business_day_date between (current_date - 14) and (current_date - 1)
		and jogm.acp_id is null
		and jogm.zero_value_unit_ind ='N' --remove gift with purchase and beauty (smart) sample
		and jogm.channel_country = 'US'
) with data primary index(business_date,business_unit_desc,store_num,bill_zip_code,price_type, order_platform_type) on commit preserve rows;

ET;

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
		and jogm.business_date between (current_date - 14) and (current_date - 1)
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
		and jogm.business_date between (current_date - 14) and (current_date - 1)
	group by 1,2,3,4,5,6,7,8,9,10,11
) with data primary index(business_date,business_unit_desc,store_num,bill_zip_code,price_type, order_platform_type) on commit preserve rows;

ET;

--DELETING AND INSERTING DATA IN THE NET LANDING TABLE
DELETE FROM {proto_schema}.MMM_FINANCE_NET_SALES_KPI_LDG ALL;

INSERT INTO {proto_schema}.MMM_FINANCE_NET_SALES_KPI_LDG
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
from daily_net;

ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;