SET QUERY_BAND = 'App_ID=APP08240;
DAG_ID=cco_tables_11521_ACE_ENG;
Task_Name=run_cco_job_3_cco_buyer_flow_fy;'
FOR SESSION VOLATILE;



/************************************************************************************/
/************************************************************************************/
/************************************************************************************
 *
 * Build a Customer/Year/Channel-level table for AARE & Buyer Flow 
 *
 ************************************************************************************/
/************************************************************************************/
/************************************************************************************/


/************************************************************************************/
/************************************************************************************
 * PART I) Build reference tables:
 *  A) Realigned fiscal calendar to replace DAY_CAL
 *  B) Date lookup tables: start & end months of past 5 years, start & end dates of past 5 years, names of past 5 years
 *  C) Customer Driver tables
 ************************************************************************************/
/************************************************************************************/

/***************************************************************************************/
/********* Part I-A: create realigned fiscal calendar (going back far enough to include 4+ years of Cohorts) *********/
/***************************************************************************************/

/********* Step I-A-i: Going back 8 years from today, find all years with a "53rd week" *********/
create multiset volatile table week_53_yrs as (
select year_num
  ,rank() over (order by year_num desc) recency_rank
from
  (
  select distinct year_num
  from PRD_NAP_USR_VWS.DAY_CAL
  where week_of_fyr = 53
    and day_date between date'2009-01-01' and current_date
  ) x 
) with data primary index(year_num) on commit preserve rows;

/********* Step I-A-ii: Count the # years with a "53rd week" *********/
create multiset volatile table week_53_yr_count as (
select count(distinct year_num) year_count
from week_53_yrs x 
) with data primary index(year_count) on commit preserve rows;

/********* Step I-A-iii: Create an empty realigned calendar *********/
create multiset volatile table realigned_calendar ,NO FALLBACK ,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO 
(
  day_date date 
  ,day_num integer 
  ,day_desc varchar(30)
  ,week_num integer
  ,week_desc varchar(30)
  ,month_num integer
  ,month_short_desc varchar(30)
  ,quarter_num integer
  ,halfyear_num integer
  ,year_num integer
  ,month_454_num integer
  ,year_454_num integer
) primary index (day_date) on commit preserve rows;

/********* Step I-A-iv: insert data into realigned calendar 
 *  (any weeks before the latest 53rd week should get started 7 days late,
 *   any weeks before the 2nd-latest 53rd week--if there was one--should get started 14 days late)
 * *********/
insert into realigned_calendar
select
  case when year_num > (select year_num from week_53_yrs where recency_rank=1) then day_date
       when (select year_count from week_53_yr_count) = 1 and year_num <= (select year_num from week_53_yrs where recency_rank=1) then day_date + 7
       when (select year_count from week_53_yr_count) = 2 and year_num >  (select year_num from week_53_yrs where recency_rank=2) then day_date + 7
       when (select year_count from week_53_yr_count) = 2 and year_num <= (select year_num from week_53_yrs where recency_rank=2) then day_date + 14
       when (select year_count from week_53_yr_count) = 3 and year_num >  (select year_num from week_53_yrs where recency_rank=2) then day_date + 7
       when (select year_count from week_53_yr_count) = 3 and year_num >  (select year_num from week_53_yrs where recency_rank=3) then day_date + 14
       when (select year_count from week_53_yr_count) = 3 and year_num <= (select year_num from week_53_yrs where recency_rank=3) then day_date + 21
       when (select year_count from week_53_yr_count) = 4 and year_num >  (select year_num from week_53_yrs where recency_rank=2) then day_date + 7
       when (select year_count from week_53_yr_count) = 4 and year_num >  (select year_num from week_53_yrs where recency_rank=3) then day_date + 14
       when (select year_count from week_53_yr_count) = 4 and year_num >  (select year_num from week_53_yrs where recency_rank=4) then day_date + 21
       when (select year_count from week_53_yr_count) = 4 and year_num <= (select year_num from week_53_yrs where recency_rank=4) then day_date + 28
       else null
   end day_date
  ,day_num
  ,day_desc
  ,week_num
  ,week_desc
  ,month_num
  ,month_short_desc
  ,quarter_num
  ,halfyear_num
  ,year_num
  ,month_454_num
  ,year_num year_454_num
from PRD_NAP_USR_VWS.DAY_CAL
where day_date between date'2009-01-01' and current_date
  and week_of_fyr <> 53
;



/************************************************************************************
 * PART I-B) Date lookup tables
 ************************************************************************************/

/********* Step I-B-i: Determine the current & most-recently complete fiscal month *********/
create multiset volatile table curr_mo_lkp as (
select distinct month_num curr_mo
     , case when (month_num mod 100) = 1
            then (month_num - 89)
            else (month_num - 1 )
        end prior_mo
     , year_num curr_year
from T2DL_DAS_USL.usl_rolling_52wk_calendar
where day_date = current_date
) with data primary index(curr_mo) on commit preserve rows;

/********* Step I-B-ii: Get Start & End dates needed for the CCO Buyer FY tables *********/
create multiset volatile table date_lookup as (
select min(case when year_num = 2018 then day_date else null end) yr0_start_dt
     , max(case when year_num = 2018 then day_date else null end) yr0_end_dt
     , min(case when year_num = 2019 then day_date else null end) yr1_start_dt
     , max(case when month_num < (select curr_mo from curr_mo_lkp)
                then day_date
                else null end) yrN_end_dt
from T2DL_DAS_USL.usl_rolling_52wk_calendar
) with data primary index (yr1_start_dt) on commit preserve rows;

--dates for Pre-Prod testing
-- create multiset volatile table date_lookup as (
-- select date'2024-09-10'-2 yr0_start_dt
--       ,date'2024-09-10'-1 yr0_end_dt
--       ,date'2024-09-10'    yr1_start_dt
--       ,date'2024-09-10'+7 yrN_end_dt
-- ) with data primary index(yr1_start_dt) on commit preserve rows;


/********* Step I-B-iii: Generate names for the earliest rolling-12-mo period *********/
create multiset volatile table month_names as (
select cast('FY-'||cast((year0 mod 2000) as varchar(2)) as varchar(8)) earliest_fy
from
  (
  select min(year_num) year0
  from realigned_calendar
  where day_date between (select yr0_start_dt from date_lookup) and (select yrN_end_dt from date_lookup)
  ) x
) with data primary index(earliest_fy) on commit preserve rows;


/************************************************************************************
 * PART I-C) Create "driver" tables (dates + customers in-scope)
 ************************************************************************************/


/********* Step I-C-i: customer/year-level driver table *********/
create multiset volatile table cust_year_driver as (
select distinct a.acp_id
  ,a.fiscal_year_shopped
from {cco_t2_schema}.cco_line_items a
where data_source='1) TRANSACTION'
  and date_shopped between (select yr1_start_dt from date_lookup) and (select yrN_end_dt from date_lookup)
)with data primary index(acp_id,fiscal_year_shopped) on commit preserve rows;

/********* Step I-C-ii: customer/channelyear-level driver table *********/
create multiset volatile table cust_chan_year_driver as (
select distinct a.acp_id
  ,a.channel
  ,a.fiscal_year_shopped
from {cco_t2_schema}.cco_line_items a
where data_source='1) TRANSACTION'
    and date_shopped between (select yr1_start_dt from date_lookup) and (select yrN_end_dt from date_lookup)
)with data primary index(acp_id,channel,fiscal_year_shopped) on commit preserve rows;


/************************************************************************************/
/************************************************************************************
 * PART II) Build 5 lookup tables:
 *  A) Acquisition date/channel
 *  B) Activation date/Channel
 *  C) New-to-Channel/Banner lookup
 *  D) Table containing customer/channel from 1 prior year (Sep17-Aug18)
 *  E) Create customer/channel/year-level table from the past 5 years (4 years from line-item table + 1 year from "I-D")
 *  F) Create customer/channel/year-level table from the past 4 years (flagging whether they shopped 1 year before)
 ************************************************************************************/
/************************************************************************************/

/************************************************************************************
 * PART II-A) Acquisition date/channel
 ************************************************************************************/
create multiset volatile table customer_acquisition_prep as (
select distinct a.acp_id
  -- ,case when b.month_num between (select yr1_start_mo from month_lookup) and (select yr1_end_mo from month_lookup) 
  --         then (select yr1_name from month_names)
  --       when b.month_num between (select yr2_start_mo from month_lookup) and (select yr2_end_mo from month_lookup) 
  --         then (select yr2_name from month_names)
  --       when b.month_num between (select yr3_start_mo from month_lookup) and (select yr3_end_mo from month_lookup) 
  --         then (select yr3_name from month_names)
  --       when b.month_num between (select yr4_start_mo from month_lookup) and (select yr4_end_mo from month_lookup) 
  --         then (select yr4_name from month_names)
  --       else null end fiscal_year_shopped
  ,cast('FY-'||cast((b.year_num mod 2000) as varchar(2)) as varchar(8)) fiscal_year_shopped
  ,a.aare_status_date acquisition_date
  ,case when a.aare_chnl_code = 'FLS'  then '1) Nordstrom Stores'
        when a.aare_chnl_code = 'NCOM' then '2) Nordstrom.com'
        when a.aare_chnl_code = 'RACK' then '3) Rack Stores'
        when a.aare_chnl_code = 'NRHL' then '4) Rack.com'
        else null end acquisition_channel
  ,case when a.aare_chnl_code in ('FLS','NCOM')  then '5) Nordstrom Banner'
        when a.aare_chnl_code in ('RACK','NRHL') then '6) Rack Banner'
        else null end acquisition_banner
from prd_nap_usr_vws.customer_ntn_status_fact a
  join realigned_calendar b on a.aare_status_date = b.day_date
  join cust_year_driver dr on a.acp_id = dr.acp_id
where a.aare_status_date between (select yr1_start_dt from date_lookup) and (select yrN_end_dt from date_lookup)
) with data primary index (acp_id,fiscal_year_shopped) on commit preserve rows;


create multiset volatile table customer_acquisition ,--NO FALLBACK ,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
(
     acp_id VARCHAR(50)
    ,fiscal_year_shopped VARCHAR(10)
    ,channel VARCHAR(20)
) primary index (acp_id,fiscal_year_shopped,channel) on commit preserve rows;

insert into customer_acquisition
select distinct acp_id
  ,fiscal_year_shopped
  ,acquisition_channel channel
from customer_acquisition_prep
;

insert into customer_acquisition
select distinct acp_id
  ,fiscal_year_shopped
  ,acquisition_banner channel
from customer_acquisition_prep
;

insert into customer_acquisition
select distinct acp_id
  ,fiscal_year_shopped
  ,'7) JWN' channel
from customer_acquisition_prep
;


create multiset volatile table customer_acquisition_jwn as (
select distinct acp_id
  ,fiscal_year_shopped
from customer_acquisition
) with data primary index (acp_id,fiscal_year_shopped) on commit preserve rows;


/************************************************************************************
 * PART II-B) Activation date/Channel
 ************************************************************************************/


create multiset volatile table customer_activation_prep as (
select distinct a.acp_id
  -- ,case when b.month_num between (select yr1_start_mo from month_lookup) and (select yr1_end_mo from month_lookup) 
  --         then (select yr1_name from month_names)
  --       when b.month_num between (select yr2_start_mo from month_lookup) and (select yr2_end_mo from month_lookup) 
  --         then (select yr2_name from month_names)
  --       when b.month_num between (select yr3_start_mo from month_lookup) and (select yr3_end_mo from month_lookup) 
  --         then (select yr3_name from month_names)
  --       when b.month_num between (select yr4_start_mo from month_lookup) and (select yr4_end_mo from month_lookup) 
  --         then (select yr4_name from month_names)
  --       else null end fiscal_year_shopped
  ,cast('FY-'||cast((b.year_num mod 2000) as varchar(2)) as varchar(8)) fiscal_year_shopped
  ,a.activated_date activation_date
  ,case when a.activated_chnl_code = 'FLS'  then '1) Nordstrom Stores'
        when a.activated_chnl_code = 'NCOM' then '2) Nordstrom.com'
        when a.activated_chnl_code = 'RACK' then '3) Rack Stores'
        when a.activated_chnl_code = 'NRHL' then '4) Rack.com'
        else null end activation_channel
  ,case when a.activated_chnl_code in ('FLS','NCOM')  then '5) Nordstrom Banner'
        when a.activated_chnl_code in ('RACK','NRHL') then '6) Rack Banner'
        else null end activation_banner
from PRD_NAP_USR_VWS.CUSTOMER_ACTIVATED_FACT a
  join realigned_calendar b on a.activated_date = b.day_date
  join cust_year_driver dr on a.acp_id = dr.acp_id
where a.activated_date between (select yr1_start_dt from date_lookup) and (select yrN_end_dt from date_lookup)
) with data primary index (acp_id,fiscal_year_shopped) on commit preserve rows;



create multiset volatile table customer_activation ,--NO FALLBACK ,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
(
     acp_id VARCHAR(50)
    ,fiscal_year_shopped VARCHAR(10)
    ,channel VARCHAR(20)
) primary index (acp_id,fiscal_year_shopped,channel) on commit preserve rows;

insert into customer_activation
select distinct acp_id
  ,fiscal_year_shopped
  ,activation_channel channel
from customer_activation_prep
;

insert into customer_activation
select distinct acp_id
  ,fiscal_year_shopped
  ,activation_banner channel
from customer_activation_prep
;

insert into customer_activation
select distinct acp_id
  ,fiscal_year_shopped
  ,'7) JWN' channel
from customer_activation_prep
;


/************************************************************************************
 * PART II-C) New-to-Channel/Banner lookup:
 ************************************************************************************/

create multiset volatile table cco_new_2_channel_lkup as (
select acp_id
  ,channel
  ,max(acquired_date) as ntx_date
from prd_nap_usr_vws.customer_channel_ntn_status_fact
group by 1,2
union all
select acp_id
  ,banner
  ,max(acquired_date) as ntx_date
from prd_nap_usr_vws.customer_banner_ntn_status_fact
group by 1,2
) with data primary index (acp_id ,channel) on commit preserve rows;


COLLECT STATISTICS COLUMN(acp_id  ) ON cco_new_2_channel_lkup;
COLLECT STATISTICS COLUMN(channel ) ON cco_new_2_channel_lkup;
COLLECT STATISTICS COLUMN(ntx_date) ON cco_new_2_channel_lkup;


create multiset volatile table customer_new2channel_prep as (
select distinct a.acp_id
  -- ,case when b.month_num between (select yr1_start_mo from month_lookup) and (select yr1_end_mo from month_lookup) 
  --         then (select yr1_name from month_names)
  --       when b.month_num between (select yr2_start_mo from month_lookup) and (select yr2_end_mo from month_lookup) 
  --         then (select yr2_name from month_names)
  --       when b.month_num between (select yr3_start_mo from month_lookup) and (select yr3_end_mo from month_lookup) 
  --         then (select yr3_name from month_names)
  --       when b.month_num between (select yr4_start_mo from month_lookup) and (select yr4_end_mo from month_lookup) 
  --         then (select yr4_name from month_names)
  --       else null end fiscal_year_shopped
  ,cast('FY-'||cast((b.year_num mod 2000) as varchar(2)) as varchar(8)) fiscal_year_shopped
  ,a.ntx_date new_to_channel_date
  ,case when trim(a.channel) = 'N_STORE'   then '1) Nordstrom Stores'
        when trim(a.channel) = 'N_COM'     then '2) Nordstrom.com'
        when trim(a.channel) = 'R_STORE'   then '3) Rack Stores'
        when trim(a.channel) = 'R_COM'     then '4) Rack.com'
        when trim(a.channel) = 'NORDSTROM' then '5) Nordstrom Banner'
        when trim(a.channel) = 'RACK'      then '6) Rack Banner'
        else null end channel
from cco_new_2_channel_lkup a
  join realigned_calendar b on a.ntx_date = b.day_date
  join cust_year_driver dr on a.acp_id = dr.acp_id
where a.ntx_date between (select yr1_start_dt from date_lookup) and (select yrN_end_dt from date_lookup)
) with data primary index (acp_id,fiscal_year_shopped) on commit preserve rows;


create multiset volatile table customer_new2channel ,--NO FALLBACK ,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
(
     acp_id VARCHAR(50)
    ,fiscal_year_shopped VARCHAR(10)
    ,channel VARCHAR(20)
) primary index (acp_id,fiscal_year_shopped,channel) on commit preserve rows;


insert into customer_new2channel
select distinct acp_id
  ,fiscal_year_shopped
  ,channel
from customer_new2channel_prep
;

insert into customer_new2channel
select distinct acp_id
  ,fiscal_year_shopped
  ,'7) JWN' channel
from customer_acquisition_prep   --Yes, this comes from the Acquisition table (when "channel" = JWN)
;


/************************************************************************************
 * II-D) Prior Year data (used for flagging "Retained" customers, folks who shopped last year):
 *
 *  (this part queries 1 year prior to earliest year in t2dl_das_strategy.cco_line_items)
 *
 ************************************************************************************/

create multiset volatile table cust_pre_cco_line_data as (
SELECT distinct dtl.acp_id
  ,case when st.business_unit_desc in ('FULL LINE','FULL LINE CANADA') then '1) Nordstrom Stores'
        when st.business_unit_desc in ('N.CA','N.COM','TRUNK CLUB','MARKETPLACE') then '2) Nordstrom.com'
        when st.business_unit_desc in ('RACK', 'RACK CANADA') then '3) Rack Stores'
        when st.business_unit_desc in ('OFFPRICE ONLINE') then '4) Rack.com'
        else null end as channel
  -- ,case when b.month_num between (select yr1_start_mo from month_lookup) and (select yr1_end_mo from month_lookup) 
  --         then (select yr1_name from month_names)
  --       when b.month_num between (select yr2_start_mo from month_lookup) and (select yr2_end_mo from month_lookup) 
  --         then (select yr2_name from month_names)
  --       when b.month_num between (select yr3_start_mo from month_lookup) and (select yr3_end_mo from month_lookup) 
  --         then (select yr3_name from month_names)
  --       when b.month_num between (select yr4_start_mo from month_lookup) and (select yr4_end_mo from month_lookup) 
  --         then (select yr4_name from month_names)
  --       else null end fiscal_year_shopped
  ,cast('FY-'||cast((cal.year_num mod 2000) as varchar(2)) as varchar(8)) fiscal_year_shopped
FROM prd_nap_usr_vws.RETAIL_TRAN_DETAIL_FACT_VW dtl
  join prd_nap_usr_vws.store_dim as ist on dtl.intent_store_num=ist.store_num
  join prd_nap_usr_vws.store_dim as st
    on (case
          when dtl.line_item_order_type in ('CustInitWebOrder','CustInitPhoneOrder') and ist.store_type_code='RK' then 828
          when dtl.line_item_order_type in ('CustInitWebOrder','CustInitPhoneOrder') and dtl.line_item_net_amt_currency_code = 'CAD' then 867
          when dtl.line_item_order_type in ('CustInitWebOrder','CustInitPhoneOrder') then 808
          when dtl.intent_store_num=5405 then 808
          else dtl.intent_store_num end) = st.store_num
    join realigned_calendar as cal on coalesce(dtl.order_date,dtl.tran_date) = cal.day_date
WHERE coalesce(dtl.order_date,dtl.tran_date) between (select yr0_start_dt from date_lookup) and (select yr0_end_dt from date_lookup)
     and dtl.business_day_date between (select yr0_start_dt from date_lookup) and  (select yr0_end_dt+14 from date_lookup)
     and dtl.acp_id is not null
     and st.business_unit_desc
       in ('FULL LINE','FULL LINE CANADA','N.CA','N.COM','TRUNK CLUB','RACK', 'RACK CANADA','OFFPRICE ONLINE'
          ,'MARKETPLACE')
     and dtl.line_net_usd_amt>=0
) with data primary index (acp_id,channel,fiscal_year_shopped) on commit preserve rows;


/************************************************************************************
 * PART II-E) Create cust/channel/year-level table for all reported years (+ 1 earlier year)
 ************************************************************************************/

/********* PART II-E-i) Create empty table *********/
create MULTISET volatile table  cust_channel_yr ,--NO FALLBACK ,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
(
     acp_id VARCHAR(50)
    ,fiscal_year_shopped VARCHAR(10)
    ,channel VARCHAR(20)
) primary index (acp_id,fiscal_year_shopped,channel) on commit preserve rows;

/********* PART II-E-ii) Insert the rows for Channels as "Channels" *********/

--Insert whatever history t2dl_das_strategy.cco_line_items has
insert into cust_channel_yr
select distinct acp_id
  ,fiscal_year_shopped
  ,channel
from cust_chan_year_driver
;

--insert any earlier history that t2dl_das_strategy.cco_line_items doesn't have
insert into cust_channel_yr
select distinct acp_id
  ,fiscal_year_shopped
  ,channel
from cust_pre_cco_line_data
;


/********* PART II-E-iii) Insert the rows for Banners as "Channels" *********/

--Insert whatever history t2dl_das_strategy.cco_line_items has
insert into cust_channel_yr
select distinct acp_id
  ,fiscal_year_shopped
  ,case when substring(channel,1,1) in ('1','2') then '5) Nordstrom Banner'
        when substring(channel,1,1) in ('3','4') then '6) Rack Banner'
        else null end channel
from cust_chan_year_driver
;

--insert any earlier history that t2dl_das_strategy.cco_line_items doesn't have
insert into cust_channel_yr
select distinct acp_id
  ,fiscal_year_shopped
  ,case when substring(channel,1,1) in ('1','2') then '5) Nordstrom Banner'
        when substring(channel,1,1) in ('3','4') then '6) Rack Banner'
        else null end channel
from cust_pre_cco_line_data
;

/********* PART II-E-iv) Insert the rows for JWN as "Channel" *********/

--Insert whatever history t2dl_das_strategy.cco_line_items has
insert into cust_channel_yr
select distinct acp_id
  ,fiscal_year_shopped
  ,'7) JWN' channel
from cust_year_driver
;

--insert any earlier history that t2dl_das_strategy.cco_line_items doesn't have
insert into cust_channel_yr
select distinct acp_id
  ,fiscal_year_shopped
  ,'7) JWN' channel
from cust_pre_cco_line_data
;


/************************************************************************************
 * PART II-F) Create customer/channel/year-level table from the past 4 years (flagging whether they shopped 1 year before)
 ************************************************************************************/

/********* PART I-F-i) Create a "LY" column = the year before the current year *********/

create multiset volatile table cust_year_chan_ly as (
select acp_id
  ,fiscal_year_shopped
  ,channel
  -- ,case when fiscal_year_shopped = (select yr1_name from month_names) then (select yr0_name from month_names)
  --       when fiscal_year_shopped = (select yr2_name from month_names) then (select yr1_name from month_names)
  --       when fiscal_year_shopped = (select yr3_name from month_names) then (select yr2_name from month_names)
  --       when fiscal_year_shopped = (select yr4_name from month_names) then (select yr3_name from month_names)
  --       else null end ly 
  ,cast('FY-'||cast((cast(substring(fiscal_year_shopped,4,5) as int) - 1) as varchar(2)) as varchar(8) ) ly
from cust_channel_yr
) with data primary index (acp_id,fiscal_year_shopped,channel) on commit preserve rows;

/********* PART I-F-ii) Join the current year to the prior year on:
 *    a) customer/channel/year... to get a channel-level retention indicators
 *    b) customer/year ... to get JWN-level retention indicators
 *  *********/

--drop table cust_year_chan;
create multiset volatile table cust_year_chan as (
select a.acp_id
  ,a.fiscal_year_shopped
  ,a.channel
  ,case when a.acp_id=b.acp_id and a.channel=b.channel and a.ly = b.fiscal_year_shopped then 1 else 0 end chan_ret_flag
  ,case when a.acp_id=c.acp_id and a.ly = c.fiscal_year_shopped then 1 else 0 end jwn_ret_flag
from  cust_year_chan_ly a
  left join cust_year_chan_ly b
    on a.acp_id=b.acp_id and a.channel=b.channel and a.ly = b.fiscal_year_shopped
  left join
    (select distinct acp_id
      ,fiscal_year_shopped
     from cust_year_chan_ly) c on a.acp_id=c.acp_id and a.ly = c.fiscal_year_shopped
where a.fiscal_year_shopped<>(select earliest_fy from month_names)
    -- in ( (select yr1_name from month_names)
    --     ,(select yr2_name from month_names)
    --     ,(select yr3_name from month_names)
    --     ,(select yr4_name from month_names) )
) with data primary index (acp_id,fiscal_year_shopped,channel) on commit preserve rows;


/************************************************************************************/
/************************************************************************************
 * PART III) Join all tables together to create Buyer-Flow & AARE Indicators
 ************************************************************************************/
/************************************************************************************/

/************************************************************************************
 * PART III-A) Join all tables together to create indicators
 * **********************************************************************************/

COLLECT STATISTICS COLUMN (ACP_ID ,fiscal_year_shopped     ,CHANNEL) ON customer_new2channel;
COLLECT STATISTICS COLUMN (ACP_ID ,fiscal_year_shopped)              ON cust_year_chan;
COLLECT STATISTICS COLUMN (ACP_ID ,fiscal_year_shopped)              ON customer_acquisition_jwn;
COLLECT STATISTICS COLUMN (ACP_ID ,fiscal_year_shopped     ,CHANNEL) ON customer_acquisition;
COLLECT STATISTICS COLUMN (ACP_ID ,fiscal_year_shopped     ,CHANNEL) ON cust_year_chan;
COLLECT STATISTICS COLUMN (ACP_ID ,fiscal_year_shopped     ,CHANNEL) ON customer_activation;


create multiset volatile table aare_buyer_flow_prep as (
--explain
select distinct a.acp_id
  ,a.fiscal_year_shopped
  ,a.channel
  ,a.chan_ret_flag
  ,a.jwn_ret_flag
  ,case when a.acp_id=c.acp_id and a.fiscal_year_shopped=c.fiscal_year_shopped
              and a.jwn_ret_flag=0 then 1
        else 0 end ntn_jwn_flag
  ,case when a.acp_id=b.acp_id and a.fiscal_year_shopped=b.fiscal_year_shopped and a.channel=b.channel
              and a.jwn_ret_flag=0 and ntn_jwn_flag=1 then 1
        else 0 end ntn_chan_flag
  ,case when a.acp_id=d.acp_id and a.fiscal_year_shopped=d.fiscal_year_shopped and a.channel=d.channel
              and (a.jwn_ret_flag=1 or ntn_jwn_flag=1) then 1
        else 0 end act_flag
  ,case when a.acp_id=e.acp_id and a.fiscal_year_shopped=e.fiscal_year_shopped and a.channel=e.channel
              and a.chan_ret_flag=0 then 1
        when ntn_jwn_flag=1 then 1
        else 0 end ntc_flag
from cust_year_chan a
  left join customer_acquisition b on a.acp_id=b.acp_id and a.fiscal_year_shopped=b.fiscal_year_shopped and a.channel=b.channel
  left join customer_acquisition_jwn c on a.acp_id=c.acp_id and a.fiscal_year_shopped=c.fiscal_year_shopped
  left join customer_activation d on a.acp_id=d.acp_id and a.fiscal_year_shopped=d.fiscal_year_shopped and a.channel=d.channel
  left join customer_new2channel e on a.acp_id=e.acp_id and a.fiscal_year_shopped=e.fiscal_year_shopped and a.channel=e.channel
) with data primary index(acp_id,fiscal_year_shopped,channel) on commit preserve rows;


/************************************************************************************
 * PART III-B) Create Buyer-Flow & AARE columns
 * **********************************************************************************/


/********* PART III-B-i) Create the Buyer-Flow segments + Channel-level AARE indicators
 * 1) Create the Buyer-Flow segments first,
 * 2) then create the AARE columns in a way that Buyer-Flow & AARE don't conflict
 * *********/

create multiset volatile table buyer_flow_all_views as (
select distinct acp_id
  ,fiscal_year_shopped
  ,channel
  ,ntn_chan_flag
  ,ntn_jwn_flag
  ,act_flag
  ,ntc_flag
  ,chan_ret_flag
  ,jwn_ret_flag
  ,case when chan_ret_flag = 1                    then '3) Retained-to-Channel'
        when ntn_chan_flag = 1 and jwn_ret_flag=0 then '1) New-to-JWN'
        when ntc_flag = 1 and ntn_chan_flag = 0   then '2) New-to-Channel (not JWN)'
                                                  else '4) Reactivated-to-Channel' end buyer_flow
  ,case when ntn_jwn_flag = 1 and jwn_ret_flag = 0 and substring(buyer_flow,1,1) not in ('3','4') then 1 else 0 end AARE_acquired
  ,case when act_flag = 1 and (ntn_jwn_flag = 1 or jwn_ret_flag = 1) and substring(buyer_flow,1,1) not in ('4') then 1 else 0 end AARE_activated
  ,case when jwn_ret_flag = 1 and substring(buyer_flow,1,1) <> '1' then 1 else 0 end AARE_retained
  ,case when ntc_flag = 1 and ntn_chan_flag = 0 and chan_ret_flag=0 and substring(buyer_flow,1,1) = '2' then 1 else 0 end AARE_engaged
from aare_buyer_flow_prep
--where substring(channel,1,1) in ('1','2','3','4')
) with data primary index(acp_id,fiscal_year_shopped,channel) on commit preserve rows;


/********* PART III-B-ii) Create the Banner-level AARE indicators
 * Ideally, these are the "max" of the channel-level indicators... but must be changed if they conflict with Buyer-Flow values
 * *********/

create multiset volatile table aare_banner_level as (
--explain
select distinct a.acp_id
    ,a.fiscal_year_shopped
    ,a.channel
    ,b.buyer_flow
    ,a.ntn_chan_flag
    ,a.ntn_jwn_flag
    ,a.act_flag
    ,b.ntc_flag
    ,a.chan_ret_flag
    ,a.jwn_ret_flag
    ,case when ntn_jwn_flag = 1 and jwn_ret_flag = 0 and substring(buyer_flow,1,1) not in ('3','4') then 1 else 0 end AARE_acquired
    ,case when act_flag = 1 and (ntn_jwn_flag = 1 or jwn_ret_flag = 1) and substring(buyer_flow,1,1) not in ('4') then 1 else 0 end AARE_activated
    ,case when jwn_ret_flag = 1 and substring(buyer_flow,1,1) <> '1' then 1 else 0 end AARE_retained
    ,a.AARE_engaged
from
  (
  select acp_id
    ,fiscal_year_shopped
    ,case when substring(channel,1,1) in ('1','2') then '5) Nordstrom Banner'
          when substring(channel,1,1) in ('3','4') then '6) Rack Banner'
          else null end channel
    ,max(ntn_chan_flag) ntn_chan_flag
    ,max(ntn_jwn_flag) ntn_jwn_flag
    ,max(act_flag) act_flag
    ,max(chan_ret_flag) chan_ret_flag
    ,max(jwn_ret_flag) jwn_ret_flag
    ,max(AARE_acquired) AARE_acquired
    ,max(AARE_activated) AARE_activated
    ,max(AARE_retained) AARE_retained
    ,max(AARE_engaged) AARE_engaged
  from buyer_flow_all_views
  where substring(channel,1,1) in ('1','2','3','4')
  group by 1,2,3
  ) a
join
  (
  select distinct acp_id
    ,fiscal_year_shopped
    ,channel
    ,buyer_flow
    ,ntc_flag
  from buyer_flow_all_views
  where substring(channel,1,1) in ('5','6')
  ) b
  on a.acp_id=b.acp_id and a.fiscal_year_shopped=b.fiscal_year_shopped and a.channel=b.channel
) with data primary index(acp_id,fiscal_year_shopped,channel) on commit preserve rows;


/********* PART III-B-iii) Create the JWN-level AARE indicators
 * Ideally, these are the "max" of the banner-level indicators... but must be changed if they conflict with Buyer-Flow values
 * *********/

create multiset volatile table aare_jwn_level as (
--explain
select distinct a.acp_id
    ,a.fiscal_year_shopped
    ,a.channel
    ,b.buyer_flow
    ,a.ntn_chan_flag
    ,a.ntn_jwn_flag
    ,a.act_flag
    ,a.chan_ret_flag
    ,a.jwn_ret_flag
    ,case when ntn_jwn_flag = 1 and jwn_ret_flag = 0 and substring(buyer_flow,1,1) not in ('3','4') then 1 else 0 end AARE_acquired
    ,case when act_flag = 1 and (ntn_jwn_flag = 1 or jwn_ret_flag = 1) and substring(buyer_flow,1,1) not in ('4') then 1 else 0 end AARE_activated
    ,case when jwn_ret_flag = 1 /*and ntn_chan_flag = 0 and ntn_jwn_flag = 0*/ and substring(buyer_flow,1,1) <> '1' then 1 else 0 end AARE_retained
    ,a.AARE_engaged
from
  (
  select acp_id
    ,fiscal_year_shopped
    ,'7) JWN' channel
    ,max(ntn_chan_flag) ntn_chan_flag
    ,max(ntn_jwn_flag) ntn_jwn_flag
    ,max(act_flag) act_flag
    ,max(chan_ret_flag) chan_ret_flag
    ,max(jwn_ret_flag) jwn_ret_flag
    ,max(AARE_acquired) AARE_acquired
    ,max(AARE_activated) AARE_activated
    ,max(AARE_retained) AARE_retained
    ,max(AARE_engaged) AARE_engaged
  from aare_banner_level
  group by 1,2,3
  ) a
join
  (
  select distinct acp_id
    ,fiscal_year_shopped
    ,channel
    ,buyer_flow
    ,ntc_flag
  from buyer_flow_all_views
  where substring(channel,1,1) ='7'
  ) b
  on a.acp_id=b.acp_id and a.fiscal_year_shopped=b.fiscal_year_shopped and a.channel=b.channel
) with data primary index(acp_id,fiscal_year_shopped,channel) on commit preserve rows;


/************************************************************************************
 * PART III-C) Insert Buyer-Flow & AARE values into final table
 * **********************************************************************************/

/********* PART III-C-i) insert Channel-level Buyer-Flow segments & AARE flags *********/

DELETE FROM {cco_t2_schema}.cco_buyer_flow_fy ALL;

INSERT INTO {cco_t2_schema}.cco_buyer_flow_fy
select distinct acp_id
  ,fiscal_year_shopped
  ,channel
  ,buyer_flow
  ,AARE_acquired
  ,AARE_activated
  ,AARE_retained
  ,AARE_engaged
  ,current_timestamp(6)
  ,current_timestamp(6)
from buyer_flow_all_views
where substring(channel,1,1) in ('1','2','3','4')

/********* PART III-C-ii) insert Banner-level Buyer-Flow segments & AARE flags *********/

union all
select distinct acp_id
  ,fiscal_year_shopped
  ,channel
  ,buyer_flow
  ,AARE_acquired
  ,AARE_activated
  ,AARE_retained
  ,AARE_engaged
  ,current_timestamp(6)
  ,current_timestamp(6)
from aare_banner_level

/********* PART III-C-iii) insert JWN-level Buyer-Flow segments & AARE flags *********/

union all
select distinct acp_id
  ,fiscal_year_shopped
  ,channel
  ,buyer_flow
  ,AARE_acquired
  ,AARE_activated
  ,AARE_retained
  ,AARE_engaged
  ,current_timestamp(6)
  ,current_timestamp(6)
from aare_jwn_level;

collect statistics column (acp_id) on {cco_t2_schema}.cco_buyer_flow_fy;
collect statistics column (fiscal_year_shopped) on {cco_t2_schema}.cco_buyer_flow_fy;
collect statistics column (channel) on {cco_t2_schema}.cco_buyer_flow_fy;
collect statistics column (buyer_flow) on {cco_t2_schema}.cco_buyer_flow_fy;
collect statistics column (acp_id,fiscal_year_shopped,channel) on {cco_t2_schema}.cco_buyer_flow_fy;

SET QUERY_BAND = NONE FOR SESSION;
