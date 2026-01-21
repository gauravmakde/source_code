SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=mmm_traffic_kpi_11521_ACE_ENG;
     Task_Name=mmm_traffic_kpi;'
     FOR SESSION VOLATILE;


--T2/Table Name: T2DL_DAS_MOA_KPI.mmm_traffic_kpi
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

create multiset volatile table aec as (
select distinct
acp_id
,execution_qtr_start_dt
,execution_qtr_end_dt
,engagement_cohort
from t2dl_das_aec.audience_engagement_cohorts
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
from PRD_NAP_USR_VWS.CUSTOMER_NTN_STATUS_FACT ntn 
where
	aare_status_date >= '2021-05-01'
	-----change to the dates of the quarter for 2024-01-01 to 2024-03-31 we must query between '2024-01-01' and '2024-03-31'
)
with data primary index(acp_id, aare_global_tran_id) on commit preserve rows;


----JWN CUSTOMERS WITH THEIR REWARD LEVEL----
CREATE MULTISET VOLATILE TABLE loy AS (
SELECT DISTINCT
acp_id
, rewards_level
, start_day_date
, end_day_date
, LEAD(start_day_date-1, 1) OVER (partition by acp_id order by start_day_date) AS end_day_date_2
-- original END_day_date = next start_day_date, so create new END_day_date that doesn't overlap
, COALESCE (end_day_date_2, end_day_date) AS end_day_date_lead
FROM PRD_NAP_BASE_VWS.LOYALTY_LEVEL_LIFECYCLE_FACT
WHERE end_day_date >= (select start_date from _variables) and start_day_date <= (select end_date from _variables)
and start_day_date < end_day_date --remove a few duplicate rows WHERE the first day of the year is a lower status but instantly changes to higher status
)
WITH DATA NO PRIMARY INDEX ON COMMIT PRESERVE ROWS; 


create volatile multiset table store_traffic as (
with s1 as
(
select stdn.store_num, stdn.day_date,
sum(case when traffic_source in ('retailnext_cam') then traffic else 0 end) as traffic_rn_cam,
sum(case when traffic_source in ('placer_store_scaled_to_retailnext', 'placer_closed_store_scaled_to_retailnext','placer_vertical_interference_scaled_to_retailnext', 'placer_vertical_interference_scaled_to_est_retailnext','placer_channel_scaled_to_retailnext') then traffic else 0 end) as traffic_scaled_placer
from t2dl_das_fls_traffic_model.store_traffic_daily as stdn
where 1=1
and stdn.day_date between (select start_date from _variables) and (select end_date from _variables)
group by 1,2
)
select 
s1.store_num, 
day_date,   
case
			when str.business_unit_desc = 'OFFPRICE ONLINE' then 'R.COM'
			when str.business_unit_desc = 'FULL LINE' then 'NORDSTROM STORE'
			when str.business_unit_desc = 'RACK' then 'RACK STORE'
		    else str.business_unit_desc
		end as business_unit_desc,
sum(case when traffic_rn_cam > 0 then traffic_rn_cam else traffic_scaled_placer end) as store_traffic
from s1
inner join PRD_NAP_USR_VWS.STORE_DIM as str
      on s1.store_num = str.store_num
      and str.business_unit_desc in ('FULL LINE','N.COM','OFFPRICE ONLINE','RACK')
group by 1,2,3
) 
with data primary index (day_date) on commit preserve rows;


create volatile multiset table daily_sessions as (
select
	csf.activity_date_pacific
	,case
		when csf.channel = 'NORDSTROM' then 'N.COM'
		when csf.channel = 'NORDSTROM_RACK' then'R.COM'
		else 'UNKNOWN'
	end as business_unit_desc
	,case when csf.experience in ('ANDROID_APP','IOS_APP') then 'APP'
	      when csf.experience = 'POINT_OF_SALE' then 'POS'  
	      when csf.experience in ('CARE_PHONE', 'VENDOR') then 'OTHER'
	      when csf.experience in ('MOBILE_WEB', 'DESKTOP_WEB') then 'WEB' 
	      else 'UNKNOWN' end as platform_type
	      , csf.acp_id
	,aec.engagement_cohort
	,case when ntn.aare_status_date is not null then 'NEW'
	     else 'EXISTING' end as cust_status
	,case when loy.rewards_level is null then 'UNKNOWN' else loy.rewards_level end as loyalty_status
	,csf.session_id
from T2DL_DAS_SESSIONS.DIOR_SESSION_FACT csf
left join aec 
	on csf.activity_date_pacific between aec.execution_qtr_start_dt and aec.execution_qtr_end_dt
	and aec.acp_id = csf.acp_id 
left join loy 
	on csf.activity_date_pacific between loy.start_day_date and loy.end_day_date_lead
	and  loy.acp_id = csf.acp_id
left join customers ntn 
	on ntn.aare_status_date = csf.activity_date_pacific 
	and  csf.acp_id = ntn.acp_id
where
--------NOTE SESSIONS ONLY STARTED FROM 2022-01-31
	csf.activity_date_pacific between (select start_date from _variables) and (select end_date from _variables)
    and csf.acp_id is not null
    
union all 

select
	csf.activity_date_pacific
	,case
		when csf.channel = 'NORDSTROM' then 'N.COM'
		when csf.channel = 'NORDSTROM_RACK' then'R.COM'
		else 'UNKNOWN'
	end as business_unit_desc
	,case when csf.experience in ('ANDROID_APP','IOS_APP') then 'APP'
	      when csf.experience = 'POINT_OF_SALE' then 'POS'  
	      when csf.experience in ('CARE_PHONE', 'VENDOR') then 'OTHER'
	      when csf.experience in ('MOBILE_WEB', 'DESKTOP_WEB') then 'WEB' 
	      else 'UNKNOWN' end as platform_type
	         , csf.acp_id
	,'UNKNOWN'as engagement_cohort
	,'UNKNOWN'as cust_status
	,'UNKNOWN'as loyalty_status
	,csf.session_id
from T2DL_DAS_SESSIONS.DIOR_SESSION_FACT csf
where
--------NOTE SESSIONS ONLY STARTED FROM 2022-01-31
	csf.activity_date_pacific between (select start_date from _variables) and (select end_date from _variables)
   and csf.acp_id is null
) 
with data primary index (activity_date_pacific) 
on commit preserve rows;


create volatile multiset table  daily_trips as (
   select
    case
			when str.business_unit_desc = 'OFFPRICE ONLINE' then 'R.COM'
			when str.business_unit_desc = 'FULL LINE' then 'NORDSTROM STORE'
			when str.business_unit_desc = 'RACK' then 'RACK STORE'
		    else str.business_unit_desc
		end as business_unit_desc
    ,tran.store_num
    ,tran.order_platform_type
    ,aec.engagement_cohort
    ,tran.cust_status
    ,tran.loyalty_status
    ,tran.trip_date
    , trip_id
   from (
      select distinct
        dtl.acp_id
        ,dtl.loyalty_status
        ,case 
	        when ntn.aare_global_tran_id is not null then 'NEW'
		    when dtl.acp_id is null then 'UNKNOWN'
		  else 'EXISTING' end as cust_status
        ,case 
             when line_item_order_type = 'CUSTINITWEBORDER'
              and line_item_fulfillment_type = 'STOREPICKxUP'
              and line_item_currency_code = 'USD' then 808
            else intent_store_num end as store_num
         ,case 
	          when dtl.order_platform_type = 'Mobile App' then 'APP'
		      when dtl.order_platform_type = 'Store POS' then 'POS'  
		      when dtl.order_platform_type in ('Phone (Customer Care)', 'BorderFree','Direct to Customer (DTC))') then 'OTHER'
		      when dtl.order_platform_type in ('Mobile Web', 'Desktop/Tablet') then 'WEB' 
		    else 'UNKNOWN' end as order_platform_type
         ,demand_date as trip_date
        ,cast(dtl.acp_id||store_num||trip_date as varchar(150)) as trip_id
      from PRD_NAP_JWN_METRICS_BASE_VWS.JWN_DEMAND_METRIC_VW dtl
      left join customers ntn 
		  on dtl.global_tran_id = ntn.aare_global_tran_id
      where 
      	demand_date between (select start_date from _variables) and (select end_date from _variables)
      		-----change to the dates of the quarter for 2024-01-01 to 2024-03-31 we must query between '2024-01-01' and '2024-03-31'
        and dtl.acp_id is not null
        --and (coalesce(dtl.nonmerch_fee_code,'-999') <> '6666'
        and dtl.line_net_usd_amt > 0 ----EXCLUDE $0 GIFT TRIP
    ) as tran
    inner join PRD_NAP_USR_VWS.STORE_DIM as str
      on tran.store_num = str.store_num
      and str.business_unit_desc in ('FULL LINE','N.COM','OFFPRICE ONLINE','RACK')
    left join aec 
      on aec.acp_id = tran.acp_id
      and tran.trip_date between aec.execution_qtr_start_dt and aec.execution_qtr_end_dt
) 
with data primary index (trip_date,business_unit_desc,store_num,order_platform_type) on commit preserve rows;


create volatile multiset table daily_traffics as (
select 
	trip_date as activity_date_pacific
	,store_num
	,business_unit_desc
	,order_platform_type  as platform_type
	,cust_status
	,upper(engagement_cohort) as engagement_cohort
	,loyalty_status
	,null as total_traffic
	,count(distinct trip_id) as trips
from daily_trips
group by 1,2,3,4,5,6,7

UNION ALL 

select 
	day_date
	,store_num
	,business_unit_desc
	,cast(null as varchar(4)) as "platform_code"
	,cast(null as varchar(4)) as "cust_status"
	,cast(null as varchar(4)) as "engagement_cohort"
	,cast(null as varchar(4)) as "loyalty_status"
	,store_traffic as total_traffic
	,null as trips
from store_traffic


union all

select 
	activity_date_pacific
	,case
		when business_unit_desc = 'N.COM' then 808
		else 828
	end as store_num
	,business_unit_desc
	,platform_type
	,cust_status
	,upper(engagement_cohort) as engagement_cohort
	,loyalty_status
	,count(distinct session_id) as total_traffic
	, null as trips
from daily_sessions
group by 1,2,3,4,5,6,7

) with data primary index (activity_date_pacific,store_num,business_unit_desc,engagement_cohort,loyalty_status) on commit preserve rows
;


DELETE FROM {kpi_scorecard_t2_schema}.mmm_traffic_kpi 
WHERE activity_date_pacific between (select start_date from _variables) and (select end_date from _variables);

INSERT INTO {kpi_scorecard_t2_schema}.mmm_traffic_kpi 
select 
  	activity_date_pacific as activity_date_pacific
	,store_num as store_num
	,business_unit_desc as business_unit_desc
	,platform_type as platform_type
	,cust_status as cust_status
	,engagement_cohort as engagement_cohort
	,loyalty_status as loyalty_status
	,total_traffic as total_traffic
	,trips as trips
	,current_date as dw_batch_date
    ,current_timestamp as dw_sys_load_tmstp
 from daily_traffics
 WHERE activity_date_pacific between (select start_date from _variables) and (select end_date from _variables);


SET QUERY_BAND = NONE FOR SESSION;


