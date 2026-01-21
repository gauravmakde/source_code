/* 
SQL script must begin with autocommit_on and QUERY_BAND SETTINGS
*/

SET QUERY_BAND = 'App_ID=app08742;
     DAG_ID=customer_cohort_trips_11521_ACE_ENG; 
     Task_Name=customer_cohort_trips;'
     FOR SESSION VOLATILE;

 
/*
T2/Table Name:
Team/Owner:
Date Created/Modified:

Note:
-- What is the the purpose of the table
-- What is the update cadence/lookback window

*/
 
 

/*
Temp table notes here if applicable
*/
create volatile multiset table user_trips as (
with cal as (
SELECT
        DISTINCT day_date,
        week_start_day_date,
        week_end_day_date,
        month_start_day_date,
        month_end_day_date, 
        quarter_start_day_date,
        quarter_end_day_date,
        fiscal_day_num,
        fiscal_week_num,
        fiscal_month_num,
        fiscal_quarter_num, 
        fiscal_year_num,
		quarter_idnt
FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
WHERE day_date between {start_date} and {end_date}
)
select 
case when source_platform_code is null and business_unit_desc in ('RACK','FULL LINE') then 'INSTORE'
when source_platform_code ='POS'  then 'INSTORE'
else source_platform_code end as source_platform_code, 
case when SOURCE_channel_CODE is null and business_unit_desc  ='RACK' then business_unit_desc 
when SOURCE_channel_CODE is null and business_unit_desc  ='FULL LINE' then 'FULL_LINE'
else SOURCE_channel_CODE end as SOURCE_channel_CODE,
business_unit_desc,
a.acp_id,
tran_date,tran_type_code,
engagement_cohort, 
    coalesce(count(distinct case when business_unit_desc in ('FULL LINE','RACK') 
        then a.acp_id||store_num||tran_date end),0) as trips_stores,
    coalesce(count(distinct case when business_unit_desc in ('N.COM','OFFPRICE ONLINE') 
        then a.acp_id||store_num||tran_date end),0) as trips_online,
    coalesce(sum( case when business_unit_desc in ('FULL LINE','RACK') 
        then line_gross_usd_amt end),0) as trips_stores_gross_usd_amt,
    coalesce(sum( case when business_unit_desc in ('N.COM','OFFPRICE ONLINE') 
        then line_gross_usd_amt end),0) as trips_online_gross_usd_amt
from (
select
	dtl.acp_id,
	source_platform_code,SOURCE_channel_CODE,
	case
		when dtl.line_net_usd_amt > 0 then coalesce(dtl.order_date,
		dtl.tran_date)
		else dtl.tran_date
	end as tran_date,
	dtl.tran_type_code,
	case
		when dtl.line_item_order_type = 'CustInitWebOrder'
		and dtl.line_item_fulfillment_type = 'StorePickUp'
		and dtl.line_item_net_amt_currency_code = 'USD' then 808
		when dtl.line_item_order_type = 'CustInitWebOrder'
		and dtl.line_item_fulfillment_type = 'StorePickUp'
		and dtl.line_item_net_amt_currency_code = 'CAD' then 867															
		else dtl.intent_store_num
	end as store_num,
	st.business_unit_desc,
	sum(
            case
                when dtl.line_net_usd_amt > 0
                    and dtl.line_item_merch_nonmerch_ind = 'MERCH' then dtl.line_item_quantity
                else 0
            end
        ) as sale_items,
	sum(
            case
                when dtl.line_net_usd_amt < 0
                    and dtl.line_item_merch_nonmerch_ind = 'MERCH' then dtl.line_item_quantity
                else 0
            end
        ) as returned_items,
	sum(case when dtl.line_net_usd_amt > 0 then dtl.line_net_usd_amt else 0 end) as line_gross_usd_amt,
	sum(dtl.line_net_usd_amt) as line_net_usd_amt
from
	prd_nap_usr_vws.RETAIL_TRAN_DETAIL_FACT_VW dtl
join prd_nap_usr_vws.STORE_DIM st
    on
	(
            case
                when dtl.line_item_order_type = 'CustInitWebOrder'
			and dtl.line_item_fulfillment_type = 'StorePickUp'
			and dtl.line_item_net_amt_currency_code = 'USD' then 808
			when dtl.line_item_order_type = 'CustInitWebOrder'
			and dtl.line_item_fulfillment_type = 'StorePickUp'
			and dtl.line_item_net_amt_currency_code = 'CAD' then 867  											
			else dtl.intent_store_num
		end) = st.store_num
left join 
(
	select 
		order_num,
		source_platform_code,
		source_channel_code
	from prd_nap_usr_vws.order_line_detail_fact
	where ORDER_DATE_PACIFIC between {start_date}-1 and {end_date}+1
) c 
on dtl.ORDER_NUM = c.order_num
where
	(dtl.order_date between {start_date} and {end_date}
		or dtl.tran_date between {start_date} and {end_date})
	and dtl.business_day_date >= {start_date} - 40	
	and (dtl.nonmerch_fee_code <> '6666'								
		or dtl.nonmerch_fee_code is null)
	and dtl.acp_id is not null
	and dtl.line_net_usd_amt > 0
	and st.business_unit_desc in (
            'FULL LINE',
            'N.COM',
            'OFFPRICE ONLINE',
            'RACK'
    )
group by
	1,
	2,
	3,
	4,
	5,
	6,
	7
) a
left join
	(select distinct 
	acp_id,
	quarter_start_day_date,
	quarter_end_day_date,
	engagement_cohort
	from T2DL_DAS_AEC.audience_engagement_cohorts a 
	left join cal 
	on a.execution_qtr = cal.quarter_idnt
	) cohort
on a.acp_id = cohort.acp_id
and a.tran_date >=cohort.quarter_start_day_date
and a.tran_date <=cohort.quarter_end_day_date
where tran_date between {start_date} and {end_date}
group by 1 ,2 ,3 ,4 ,5 ,6, 7
)with data primary index(acp_id,tran_date) on commit preserve rows;



/*
--------------------------------------------
DELETE any overlapping records from destination 
table prior to INSERT of new data
--------------------------------------------
*/
DELETE 
FROM    {deg_t2_schema}.customer_cohort_trips
WHERE   tran_date >= {start_date}
AND     tran_date <= {end_date}
;


INSERT INTO {deg_t2_schema}.customer_cohort_trips
SELECT  source_platform_code ,
        SOURCE_channel_CODE ,
        business_unit_desc ,
        tran_type_code ,
        acp_id ,
        engagement_cohort ,
        trips_stores ,
        trips_online ,
        trips_stores_gross_usd_amt,
        trips_online_gross_usd_amt
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp
        , tran_date 
FROM	user_trips 
WHERE   tran_date >= {start_date}
AND     tran_date <= {end_date}
;


COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (acp_id,tran_date), -- column names used for primary index
                    COLUMN (tran_date)  -- column names used for partition
on {deg_t2_schema}.customer_cohort_trips;


/*  
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;
   