SET QUERY_BAND = 'App_ID=APP08176; 
     DAG_ID=mothership_finance_sessions_agg_11521_ACE_ENG;
     Task_Name=finance_sessions_agg;'
     FOR SESSION VOLATILE;
 
/*
PRD_NAP_DSA_AI_USR_VWS.FINANCE_SESSIONS_AGG
Description - This ddl creates a table of sessions split by platform (device), and customer categories
Full documenation: https://confluence.nordstrom.com/x/M4fITg
Contacts: Matthew Bond, Analytics

SLA:
Source data (usl_digital_funnel) finishes 1:30-2am, triggers this downstream
Checking with USL owner (Rathin) if there are DQ checks. I'm not sure I need to add an SLA here
*/


--make base table with session info
CREATE MULTISET VOLATILE TABLE sessions AS (
select
activity_date_partition as activity_date
,dc.quarter_num
,dc.year_num
,case when channel = 'NORDSTROM' then 'N.COM'
    when channel in ('NORDSTROM_RACK', 'HAUTELOOK') then 'OFFPRICE ONLINE'
    else null end as business_unit_desc
,case when experience = 'CARE_PHONE' then 'Other'
	when experience = 'ANDROID_APP' then 'Android'
	when experience = 'MOBILE_WEB' then 'Mobile Web'
	when experience = 'DESKTOP_WEB' then 'Desktop/Tablet'
	when experience = 'POINT_OF_SALE' then 'Store POS'
	when experience = 'IOS_APP' then 'IOS'
	when experience = 'VENDOR' then 'Other'
	else experience end as platform_code
,acp_id
,mrkt_type
,finance_rollup
,finance_detail
,session_count --total
,atb_sessions --adding
,pdp_sessions --viewing
,checkout_sessions --checkout
,ordered_sessions --ordering
,acp_count
,unique_cust_count
,total_pages_visited
,total_unique_pages_visited
,bounced_sessions
,browse_sessions
,search_sessions
,wishlist_sessions
FROM T2DL_DAS_USL.usl_digital_funnel as udf
inner join prd_nap_base_vws.day_cal as dc on udf.activity_date_partition = dc.day_date
where activity_date_partition between date'2023-01-29' AND date'2024-02-03'
)
WITH DATA PRIMARY INDEX(activity_date, business_unit_desc, platform_code, acp_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(activity_date, business_unit_desc, platform_code, acp_id) ON sessions;


--index and transform this table one time to de-skew the joins later
CREATE MULTISET VOLATILE TABLE buyerflow AS (
SELECT
case when channel = '1) Nordstrom Stores' then 'FULL LINE'
    when channel = '2) Nordstrom.com' then 'N.COM'
    when channel = '3) Rack Stores' then 'RACK'
    when channel = '4) Rack.com' then 'OFFPRICE ONLINE' else null end as business_unit_desc
,right(fiscal_year_shopped,2)+2000 as year_num
,acp_id
,buyer_flow as buyerflow_code
,case when AARE_acquired = 1 then upper('Y') else upper('N') end as AARE_acquired_ind
,case when AARE_activated = 1 then upper('Y') else upper('N') end as AARE_activated_ind
,case when AARE_retained = 1 then upper('Y') else upper('N') end as AARE_retained_ind
,case when AARE_engaged = 1 then upper('Y') else upper('N') end as AARE_engaged_ind
FROM T2DL_DAS_STRATEGY.cco_buyer_flow_fy
where 1=1
and channel in ('1) Nordstrom Stores', '2) Nordstrom.com', '3) Rack Stores', '4) Rack.com') --others are nordstrom banner, rack banner, jwn
and year_num in (select distinct year_num from PRD_NAP_base_vws.DAY_CAL where day_date between date'2023-01-29' AND date'2024-02-03')
)
WITH DATA PRIMARY INDEX(acp_id, year_num, business_unit_desc) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(acp_id, year_num, business_unit_desc) ON buyerflow;


--index this table one time to de-skew the joins later
CREATE MULTISET VOLATILE TABLE aec AS (
SELECT
acp_id
,execution_qtr
,engagement_cohort
FROM T2DL_DAS_AEC.audience_engagement_cohorts
where 1=1
and execution_qtr_end_dt >= date'2023-01-29' and execution_qtr_start_dt <= date'2024-02-03'
--and execution_qtr_end_dt >= current_Date-70 and execution_qtr_start_dt <= current_Date
)
WITH DATA PRIMARY INDEX(acp_id, execution_qtr) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(acp_id, execution_qtr) ON aec;


--get loyalty information into format to show changes IN loyalty status over time
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
WHERE end_day_date >= date'2023-01-29' and start_day_date <= date'2024-02-03'
and start_day_date < end_day_date --remove a few duplicate rows WHERE the first day of the year is a lower status but instantly changes to higher status
)
WITH DATA NO PRIMARY INDEX ON COMMIT PRESERVE ROWS; --optimized by TD team


COLLECT STATISTICS COLUMN (acp_id) ON loy;
COLLECT STATISTICS COLUMN (acp_id,start_day_date, end_day_date) ON loy;
COLLECT STATISTICS COLUMN (start_day_date) ON loy;
COLLECT STATISTICS COLUMN (end_day_date_lead) ON loy;
COLLECT STATISTICS COLUMN (end_day_date) ON loy;


CREATE MULTISET VOLATILE TABLE sessions_acp_null AS (
SELECT
activity_date
,quarter_num
,year_num
,business_unit_desc
,platform_code
,acp_id
,mrkt_type
,finance_rollup
,finance_detail
,session_count --total
,atb_sessions --adding
,pdp_sessions --viewing
,checkout_sessions --checkout
,ordered_sessions --ordering
,acp_count
,unique_cust_count
,total_pages_visited
,total_unique_pages_visited
,bounced_sessions
,browse_sessions
,search_sessions
,wishlist_sessions
from sessions
where acp_id is null)
WITH DATA NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE sessions_acp_not_null AS (
SELECT
activity_date
,quarter_num
,year_num
,business_unit_desc
,platform_code
,acp_id
,mrkt_type
,finance_rollup
,finance_detail
,session_count --total
,atb_sessions --adding
,pdp_sessions --viewing
,checkout_sessions --checkout
,ordered_sessions --ordering
,acp_count
,unique_cust_count
,total_pages_visited
,total_unique_pages_visited
,bounced_sessions
,browse_sessions
,search_sessions
,wishlist_sessions
from sessions
where acp_id is not null)
WITH DATA PRIMARY INDEX(acp_id, quarter_num) ON COMMIT PRESERVE ROWS;

collect statistics column (acp_id, quarter_num) on sessions_acp_not_null;


CREATE MULTISET VOLATILE TABLE sessions_aec AS (
SELECT
activity_date
,year_num
,business_unit_desc
,platform_code
,s.acp_id
,mrkt_type
,finance_rollup
,finance_detail
,aec.engagement_cohort
,session_count --total
,atb_sessions --adding
,pdp_sessions --viewing
,checkout_sessions --checkout
,ordered_sessions --ordering
,acp_count
,unique_cust_count
,total_pages_visited
,total_unique_pages_visited
,bounced_sessions
,browse_sessions
,search_sessions
,wishlist_sessions
from sessions_acp_not_null as s
left join aec on s.acp_id = aec.acp_id and s.quarter_num = aec.execution_qtr
)
WITH DATA PRIMARY INDEX(acp_id, year_num, business_unit_desc) ON COMMIT PRESERVE ROWS;

collect statistics column (acp_id, year_num, business_unit_desc) on sessions_aec;


CREATE MULTISET VOLATILE TABLE sessions_buyerflow AS (
SELECT
activity_date
,s.business_unit_desc
,platform_code
,s.acp_id
,mrkt_type
,finance_rollup
,finance_detail
,bf.buyerflow_code
,bf.AARE_acquired_ind
,bf.AARE_activated_ind
,bf.AARE_retained_ind
,bf.AARE_engaged_ind
,engagement_cohort
,session_count --total
,atb_sessions --adding
,pdp_sessions --viewing
,checkout_sessions --checkout
,ordered_sessions --ordering
,acp_count
,unique_cust_count
,total_pages_visited
,total_unique_pages_visited
,bounced_sessions
,browse_sessions
,search_sessions
,wishlist_sessions
from sessions_aec as s
left join buyerflow as bf on s.business_unit_desc = bf.business_unit_desc and s.year_num = bf.year_num and s.acp_id = bf.acp_id
)
WITH DATA PRIMARY INDEX(activity_date, acp_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN(activity_date, acp_id) ON sessions_buyerflow;


CREATE MULTISET VOLATILE TABLE sessions_loy AS (
SELECT
activity_date
,business_unit_desc
,platform_code
,s.acp_id
,mrkt_type
,finance_rollup
,finance_detail
,loy.rewards_level as loyalty_status
,buyerflow_code
,AARE_acquired_ind
,AARE_activated_ind
,AARE_retained_ind
,AARE_engaged_ind
,engagement_cohort
,session_count --total
,atb_sessions --adding
,pdp_sessions --viewing
,checkout_sessions --checkout
,ordered_sessions --ordering
,acp_count
,unique_cust_count
,total_pages_visited
,total_unique_pages_visited
,bounced_sessions
,browse_sessions
,search_sessions
,wishlist_sessions
from sessions_buyerflow as s
left join loy on s.acp_id = loy.acp_id and s.activity_date between loy.start_day_date and loy.end_day_date_lead
)
WITH DATA  NO PRIMARY INDEX  ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE sessions_agg AS (
SELECT
activity_date
,business_unit_desc
,platform_code
,loyalty_status
,buyerflow_code
,AARE_acquired_ind
,AARE_activated_ind
,AARE_retained_ind
,AARE_engaged_ind
,engagement_cohort
,mrkt_type as mrtk_chnl_type_code
,finance_rollup as mrtk_chnl_finance_rollup_code
,finance_detail as mrtk_chnl_finance_detail_code
,sum(session_count) as sessions
,sum(pdp_sessions) as viewing_sessions
,sum(atb_sessions) as adding_sessions
,sum(checkout_sessions) as checkout_sessions
,sum(ordered_sessions) as ordering_sessions
,sum(acp_count) as acp_count
,sum(unique_cust_count) as unique_cust_count
,sum(total_pages_visited) as total_pages_visited
,sum(total_unique_pages_visited) as total_unique_pages_visited
,sum(bounced_sessions) as bounced_sessions
,sum(browse_sessions) as browse_sessions
,sum(search_sessions) as search_sessions
,sum(wishlist_sessions) as wishlist_sessions
,current_timestamp as dw_sys_load_tmstp
from sessions_loy
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
    UNION ALL
SELECT
activity_date
,business_unit_desc
,platform_code
,cast(null as varchar(20)) as loyalty_status
,cast(null as varchar(20)) as buyerflow_code
,cast(null as varchar(1)) as AARE_acquired_ind
,cast(null as varchar(1)) as AARE_activated_ind
,cast(null as varchar(1)) as AARE_retained_ind
,cast(null as varchar(1)) as AARE_engaged_ind
,cast(null as varchar(20)) as engagement_cohort
,mrkt_type as mrtk_chnl_type_code
,finance_rollup as mrtk_chnl_finance_rollup_code
,finance_detail as mrtk_chnl_finance_detail_code
,sum(session_count) as sessions
,sum(pdp_sessions) as viewing_sessions
,sum(atb_sessions) as adding_sessions
,sum(checkout_sessions) as checkout_sessions
,sum(ordered_sessions) as ordering_sessions
,sum(acp_count) as acp_count
,sum(unique_cust_count) as unique_cust_count
,sum(total_pages_visited) as total_pages_visited
,sum(total_unique_pages_visited) as total_unique_pages_visited
,sum(bounced_sessions) as bounced_sessions
,sum(browse_sessions) as browse_sessions
,sum(search_sessions) as search_sessions
,sum(wishlist_sessions) as wishlist_sessions
,current_timestamp as dw_sys_load_tmstp
from sessions_acp_null
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)
WITH DATA  NO PRIMARY INDEX  ON COMMIT PRESERVE ROWS;

DELETE FROM PRD_NAP_DSA_AI_BASE_VWS.finance_sessions_agg WHERE activity_date BETWEEN date'2023-01-29' AND date'2024-02-03';

INSERT INTO PRD_NAP_DSA_AI_BASE_VWS.finance_sessions_agg
(
    activity_date
    ,business_unit_desc
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
    ,dw_sys_load_tmstp
  )
  SELECT
    activity_date
    ,business_unit_desc
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
    ,dw_sys_load_tmstp
 FROM
sessions_agg;
 
      
SET QUERY_BAND = NONE FOR SESSION;