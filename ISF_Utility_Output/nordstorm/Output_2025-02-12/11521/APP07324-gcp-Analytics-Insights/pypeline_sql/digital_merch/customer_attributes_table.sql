/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP09142;
     DAG_ID=customer_attributes_table_11521_ACE_ENG;
     Task_Name=customer_attributes_table;'
     FOR SESSION VOLATILE;


/*
T2/Table Name: t2dl_das_digitalmerch_flash
Team/Owner: Nicole Miao, Rathin Deshpande, Rae Ann Boswell
Date Created/Modified: 08/21/2024
*/


----------------------------------------------------------------
----------------------ACP ID Lookup by Day------------------------
----------------------------------------------------------------
create multiset volatile table customer_daily_lookup as (
select a.activity_date_pacific,
		a.channel,
		a.experience as platform,
       a.session_id,
       max(x.acp_id) as acp_id
from prd_nap_usr_vws.customer_session_fact a
left join prd_nap_usr_vws.customer_session_xref x
    on a.session_id = x.session_id
where a.activity_date_pacific between {start_date} and {end_date}
group by 1,2,3,4
 ) with data primary index (activity_date_pacific, session_id, acp_id)
    on commit preserve rows;
collect stats primary index (activity_date_pacific, session_id, acp_id) on customer_daily_lookup;


create multiset volatile table customer_loyalty_lkp as (
    select    distinct
        rwd.acp_id,
       rewards_level,
       start_date,
       start_day_date,
       end_date,
       end_day_date
    from prd_nap_usr_vws.loyalty_level_lifecycle_fact_vw rwd
) with data primary index (acp_id) on commit preserve rows;

----------------------------------------------------------------
----------------------ACP to Loyalty------------------------
----------------------------------------------------------------
create multiset volatile table customer_cardmember_lkp as (
    select distinct acp_id,
       member_ind,
       cardmember_enroll_date,
       cardmember_close_date,
       member_enroll_date,
       member_close_date
from prd_nap_usr_vws.loyalty_member_dim_vw
) with data primary index (acp_id) on commit preserve rows;

create multiset volatile table customer_loyalty_summary as (
    select lkp.acp_id,
       lkp.activity_date_pacific,
     lkp.session_id,
     lkp.channel,
     lkp.platform,
       max(case when lkp.activity_date_pacific between rwd.start_day_date and rwd.end_day_date then rewards_level end) as loyalty_status,
       max(case when ccl.cardmember_enroll_date <= lkp.activity_date_pacific and (ccl.cardmember_close_Date > lkp.activity_date_pacific or  ccl.cardmember_close_Date is null) then 1 else 0 end) as cardmember_flag,
       max(case when ccl.member_enroll_date <= lkp.activity_date_pacific and (ccl.member_close_date  > lkp.activity_date_pacific or ccl.member_close_date is null) then 1 else 0 end) as member_flag
from customer_daily_lookup lkp
left join customer_loyalty_lkp rwd on rwd.acp_id = lkp.acp_id
left join customer_cardmember_lkp ccl on ccl.acp_id = lkp.acp_id
where lkp.acp_id is not null
group by 1,2,3,4,5
    ) with data primary index (acp_id, session_id, activity_date_pacific) on commit preserve rows
;
collect stats primary index  (acp_id, session_id, activity_date_pacific) on customer_loyalty_summary;



----------------------------------------------------------------
----------------------ACP to Geo Mapping------------------------
----------------------------------------------------------------
create multiset volatile table cust_billing as (
    select sub1.*,
            ouzd.us_dma_code,
            od.dma_desc,
            zcd.city_name,
            zcd.state_name
    from (
        select acp_id,
            address_types,
            day_date,
            max(postal_code) as billing_zipcode
    from PRD_NAP_USR_VWS.ACP_ANALYTICAL_POSTAL_XREF xref
    left join PRD_NAP_USR_VWS.DAY_CAL dc
        on dc.day_date = xref.dw_batch_date
    where address_types like '%BILLING%'
        and xref.country_code = 'US'
    group by 1,2,3
        ) sub1
    left join PRD_NAP_USR_VWS.ORG_US_ZIP_DMA ouzd on sub1.billing_zipcode = ouzd.us_zip_code
    left join prd_nap_usr_vws.zip_codes_dim zcd on zcd.ZIP_CODE = sub1.billing_zipcode and zcd.country_code = 'US'
    left join PRD_NAP_USR_VWS.ORG_DMA od on od.dma_code = ouzd.us_dma_code
   ) with data primary index(acp_id, billing_zipcode) on commit preserve rows
;

create multiset volatile table cust_billing_most_recent as (
select distinct sub.acp_id,
       billing_zipcode,
       us_dma_code,
       dma_desc,
       city_name,
       state_name
from (
select c.*,
       row_number() OVER (PARTITION BY acp_id ORDER BY day_date desc) ranking
from cust_billing c
     where day_date <= {end_date}
) sub
inner join customer_daily_lookup lkp on sub.acp_id = lkp.acp_id
where ranking = 1
       ) with data primary index (acp_id) on commit preserve rows
;
collect stats primary index (acp_id) on cust_billing_most_recent;


----------------------------------------------------------------
----------------ACP to Digital Acquisition Mapping--------------
----------------------------------------------------------------
create multiset volatile table ntn_tbl as (
    select sub.acp_id,
            sub.aare_chnl_code,
            sub.aare_status_date,
            sub.most_recent_ntn_date
    from (
     select distinct ca.acp_id,
                    case when ca.aare_chnl_code = 'NCOM' then 'N.COM'
                         when ca.aare_chnl_code = 'NRHL' then 'OFFPRICE ONLINE'
                         when ca.aare_chnl_code = 'RACK' then 'RACK'
                         when ca.aare_chnl_code = 'FLS' then 'FULL LINE' end as aare_chnl_code,
                    aare_status_date,
                    most_recent_ntn_date,
                    row_number() OVER (PARTITION BY ca.acp_id order by aare_status_date desc) as rnk
    from prd_nap_usr_vws.CUSTOMER_NTN_STATUS_FACT ca
    left join (select acp_id,
                      aare_chnl_code,
                   max(aare_status_date) as most_recent_ntn_date
               from prd_nap_usr_vws.CUSTOMER_NTN_STATUS_FACT
               where 1 = 1
                    and aare_chnl_code in ('NCOM','NRHL')
               group by 1,2) dc
                    on dc.acp_id = ca.acp_id and dc.aare_chnl_code = ca.aare_chnl_code
    left join PRD_NAP_USR_VWS.DAY_CAL cal on ca.aare_status_date = cal.day_date
    ) sub
    where 1 = 1
        and rnk = 1
) with data primary index (acp_id, aare_status_date) on commit preserve rows;
collect stats  primary index (acp_id, aare_status_date) on ntn_tbl;


----------------------------------------------------------------
----------------ACP to Digital Activation Mapping--------------
----------------------------------------------------------------
create multiset volatile table act_tbl as (
select --top 100 *
    acp_id,
    case when ca.activated_chnl_code = 'NCOM' then 'N.COM'
                         when ca.activated_chnl_code = 'NRHL' then 'OFFPRICE ONLINE'
                         when ca.activated_chnl_code = 'RACK' then 'RACK'
                         when ca.activated_chnl_code = 'FLS' then 'FULL LINE' end as activated_chnl_code,
    activated_date
    from PRD_NAP_USR_VWS.CUSTOMER_ACTIVATED_FACT ca
    left join prd_nap_usr_vws.day_cal dc on ca.acquired_date = dc.day_date
    where 1 = 1
        and activated_chnl_code in ('NRHL', 'NCOM')
        and year_num >= 2022
) with data primary index (acp_id) on commit preserve rows;
collect stats  primary index (acp_id) on act_tbl;

----------------------------------------------------------------
----------------------ACP to AEC Mapping------------------------
----------------------------------------------------------------
create volatile multiset table customer_aec_lkp as (
      select distinct 
                acp_id,
                execution_qtr,
                case when engagement_cohort in ('Acquire & Activate','Acquired Mid-Qtr') then 'Acquire & Activate'
                        else engagement_cohort end as engagement_cohort
    from t2dl_das_aec.audience_engagement_cohorts
    where execution_qtr in (select quarter_num from prd_nap_usr_vws.day_cal where day_date between {start_date} and {end_date})
) with data primary index(acp_id,engagement_cohort) on commit preserve rows;
collect stats  primary index (acp_id,engagement_cohort) on customer_aec_lkp;



----------------------------------------------------------------
----------------------ACP to First Digital------------------------
----------------------------------------------------------------
create multiset volatile table first_digital_transaction as(
    select acp_id
            ,min(case when business_unit_desc = 'N.COM' and ranking = 1 then tran_date end) as ncom_first_digital_transaction
            ,min(case when business_unit_desc = 'N.COM' and ranking = 2 then tran_date end) as ncom_second_digital_transaction
            ,min(case when business_unit_desc = 'OFFPRICE ONLINE' and ranking = 1 then tran_date end) as rcom_first_digital_transaction
            ,min(case when business_unit_desc = 'OFFPRICE ONLINE' and ranking = 2 then tran_date end) as rcom_second_digital_transaction
            ,min(case when ranking = 1 then tran_date end) as first_digital_transaction
            ,min(case when ranking = 2 then tran_date end) as second_digital_transaction
    from (
    select
        acp_id
        , tran_date
        , business_unit_desc
        , ROW_NUMBER() OVER (PARTITION BY acp_id, business_unit_desc ORDER BY tran_date ASC) as ranking
    from(
        select
                acp_id
                , business_unit_desc
                , coalesce(order_num, global_tran_id) as order_num
                ,min(tran_date) tran_date
        from T2DL_DAS_SALES_RETURNS.sales_and_returns_fact
        where business_unit_desc in ('N.COM', 'OFFPRICE ONLINE')
            and acp_id is not null
            and coalesce(order_num, global_tran_id) is not null 
        group by 1,2,3
        ) sub_1
    ) sub
    where ranking <= 2
    group by 1
) with data primary index (acp_id) on commit preserve rows;
collect stats  primary index (acp_id) on first_digital_transaction;



create multiset volatile table last_digital_transaction as(
        select acp_id
                ,max(case when business_unit_desc = 'N.COM' then tran_date end) as ncom_last_digital_transaction
                ,max(case when business_unit_desc = 'OFFPRICE ONLINE' then tran_date end) as rcom_last_digital_transaction
                ,max(tran_date) as last_digital_transaction
        from (
        select
            acp_id
            , tran_date
            , business_unit_desc
            , ROW_NUMBER() OVER (PARTITION BY acp_id, business_unit_desc ORDER BY tran_date DESC) as ranking
        from(
            select
                    acp_id
                    , business_unit_desc
                    , coalesce(order_num, global_tran_id) as order_num
                    ,min(tran_date) tran_date
            from T2DL_DAS_SALES_RETURNS.sales_and_returns_fact
            where business_unit_desc in ('N.COM', 'OFFPRICE ONLINE')
                and acp_id is not null
                and coalesce(order_num, global_tran_id) is not null 
            group by 1,2,3
            ) sub_1
        ) sub
        where ranking <= 1
        group by 1
    ) with data primary index (acp_id) on commit preserve rows;
collect stats  primary index (acp_id) on last_digital_transaction;



create multiset volatile table prior_2022_digital_transaction as(
select acp_id,
        sum(order_count) as digital_order_count,
        sum(demand) as digital_demand,
        sum(units) as digital_units,
        sum(case when business_unit_desc = 'N.COM' then order_count end) as ncom_order_count,
        sum(case when business_unit_desc = 'N.COM' then demand else 0 end) as ncom_demand,
        sum(case when business_unit_desc = 'N.COM' then units else 0 end) as ncom_units,
        sum(case when business_unit_desc = 'OFFPRICE ONLINE' then order_count end) as rcom_order_count,
        sum(case when business_unit_desc = 'OFFPRICE ONLINE' then demand else 0 end) as rcom_demand,
        sum(case when business_unit_desc = 'OFFPRICE ONLINE' then units else 0 end) as rcom_units
from(
    select
        acp_id
        , business_unit_desc
        , count(coalesce(order_num, global_tran_id)) as order_count
        , sum(coalesce(shipped_sales, 0)) as demand
        , sum(coalesce(shipped_qty,0)) as units
    from T2DL_DAS_SALES_RETURNS.sales_and_returns_fact
    where business_unit_desc in ('N.COM', 'OFFPRICE ONLINE')
    and acp_id is not null
    and tran_date < '2022-01-30'
    group by 1,2
) sub
group by 1
    ) with data primary index (acp_id) on commit preserve rows;
collect stats  primary index (acp_id) on prior_2022_digital_transaction;



----------------------------------------------------------------
---------------------- ACP to Age       ------------------------
----------------------------------------------------------------
create multiset volatile table date_lookup_year as (
    select
        cast(year_num as varchar(4)) as year_id,
        min(month_num) as start_month,
        max(month_num) as end_month,
        min(day_date) as start_dt,
        max(day_date) as end_dt
    from T2DL_DAS_USL.usl_rolling_52wk_calendar as cal
    group by 1
    where year_num >= 2022
        and month_num < (select max(month_num) -- get only through last completed month  (this impacts the end_dt for FY-24)
	        from T2DL_DAS_USL.usl_rolling_52wk_calendar
	        where day_date=current_date)
)with data primary index (year_id) on commit preserve rows;

create multiset volatile table customer_age_tbl as (
with cte as (select cls.acp_id, activity_date_pacific, session_id, channel, platform, dc.year_num
             from customer_loyalty_summary cls
                      left join PRD_NAP_USR_VWS.day_cal dc on cls.activity_date_pacific = dc.day_date)
select distinct a.year_num,
       a.acp_id,
       case when cast(age_value - (age_date - yr.end_dt)/365.25 as integer) between 14 and 22 then '01) Young Adult'
             when cast(age_value - (age_date - yr.end_dt)/365.25 as integer) between 23 and 29 then '02) Early Career'
             when cast(age_value - (age_date - yr.end_dt)/365.25 as integer) between 30 and 44 then '03) Mid Career'
             when cast(age_value - (age_date - yr.end_dt)/365.25 as integer) between 45 and 64 then '04) Late Career'
             when cast(age_value - (age_date - yr.end_dt)/365.25 as integer) >= 65 then '05) Retired'
             else 'Unknown'end lifestage
       ,CASE WHEN cast(age_value - (age_date - yr.end_dt)/365.25 as integer) < 18 THEN '0) <18 yrs'
            WHEN cast(age_value - (age_date - yr.end_dt)/365.25 as integer) >= 18 AND cast(age_value - (age_date - yr.end_dt)/365.25 as integer) <= 24 THEN '1) 18-24 yrs'
            WHEN cast(age_value - (age_date - yr.end_dt)/365.25 as integer) > 24  AND cast(age_value - (age_date - yr.end_dt)/365.25 as integer) <= 34 THEN '2) 25-34 yrs'
            WHEN cast(age_value - (age_date - yr.end_dt)/365.25 as integer) > 34  AND cast(age_value - (age_date - yr.end_dt)/365.25 as integer) <= 44 THEN '3) 35-44 yrs'
            WHEN cast(age_value - (age_date - yr.end_dt)/365.25 as integer) > 44  AND cast(age_value - (age_date - yr.end_dt)/365.25 as integer) <= 54 THEN '4) 45-54 yrs'
            WHEN cast(age_value - (age_date - yr.end_dt)/365.25 as integer) > 54  AND cast(age_value - (age_date - yr.end_dt)/365.25 as integer) <= 64 THEN '5) 55-64 yrs'
            WHEN cast(age_value - (age_date - yr.end_dt)/365.25 as integer) > 64 THEN '6) 65+ yrs'
            ELSE 'Unknown' END as age_group
from cte a
left join T2DL_DAS_STRATEGY.customer_age_dim_vw b
on a.acp_id = b.acp_id
join date_lookup_year yr
    on a.year_num = yr.year_id
    ) with data primary index (acp_id) on commit preserve rows;
collect stats  primary index (acp_id) on customer_age_tbl;


------------------------------------------------------------------------------
--- Consolidate the Tables
------------------------------------------------------------------------------
DELETE 
FROM    {digital_merch_t2_schema}.customer_attributes_table
;


INSERT INTO {digital_merch_t2_schema}.customer_attributes_table
select distinct  a.*

       ,coalesce(aec.engagement_cohort,last_value(aec.engagement_cohort IGNORE NULLS) OVER (PARTITION BY a.acp_id ORDER BY a.activity_date_pacific )) as engagement_cohort
       ,cb.billing_zipcode
        ,cb.us_dma_code
        ,cb.dma_desc
        ,cb.city_name
        ,cb.state_name

        ,coalesce(seg.predicted_ct_segment,last_value(seg.predicted_ct_segment IGNORE NULLS) OVER (PARTITION BY a.acp_id ORDER BY a.activity_date_pacific )) as predicted_ct_segment
        ,seg.clv_jwn
        ,seg.clv_op
        ,seg.clv_fp

        ,ntn.aare_chnl_code
        ,ntn.aare_status_date

        ,act.activated_chnl_code
        ,act.activated_date

        ,fdt.ncom_first_digital_transaction
        ,fdt.ncom_second_digital_transaction
        ,ldt.ncom_last_digital_transaction
        ,fdt.rcom_first_digital_transaction
        ,fdt.rcom_second_digital_transaction
        ,ldt.rcom_last_digital_transaction
        ,fdt.first_digital_transaction
        ,fdt.second_digital_transaction
        ,ldt.last_digital_transaction

        ,digital_order_count as pre_22_digital_order_count
        ,digital_demand as pre_22_digital_demand
        ,digital_units as pre_22_digital_units
        ,ncom_order_count as pre_22_ncom_order_count
        ,ncom_demand as pre_22_ncom_demand
        ,ncom_units as pre_22_ncom_units
        ,rcom_order_count as pre_22_rcom_order_count
        ,rcom_demand as pre_22_rcom_demand
        ,rcom_units as pre_22_rcom_units

        ,cat.age_group
        ,cat.lifestage

        ,CURRENT_TIMESTAMP as dw_sys_load_tmstp 
from customer_loyalty_summary a
left join prd_nap_usr_vws.day_cal dc on a.activity_date_pacific = dc.day_date
left join customer_aec_lkp aec on a.acp_id = aec.acp_id and dc.quarter_num = aec.execution_qtr
left join cust_billing_most_recent cb on cb.acp_id = a.acp_id
left join T2DL_DAS_CAL.CUSTOMER_ATTRIBUTES_SCORES seg on a.acp_id = seg.acp_id
left join ntn_tbl ntn on ntn.acp_id = a.acp_id
left join act_tbl act on act.acp_id = a.acp_id
left join first_digital_transaction fdt on fdt.acp_id = a.acp_id
left join last_digital_transaction ldt on ldt.acp_id = a.acp_id
left join prior_2022_digital_transaction p2dt on p2dt.acp_id = a.acp_id
left join customer_age_tbl cat on cat.acp_id = a.acp_id
;


COLLECT STATISTICS 
COLUMN(PARTITION),
COLUMN (acp_id, session_id,activity_date_pacific), -- column names used for primary index
COLUMN (activity_date_pacific)
ON {digital_merch_t2_schema}.customer_attributes_table;


/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;

