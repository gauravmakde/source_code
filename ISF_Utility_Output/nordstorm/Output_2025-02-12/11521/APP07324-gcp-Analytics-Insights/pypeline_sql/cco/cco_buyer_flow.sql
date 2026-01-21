SET QUERY_BAND = 'App_ID=APP08240;
     DAG_ID=cco_cust_chan_yr_attributes_11521_ACE_ENG;
     Task_Name=cco_buyer_flow;'
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
--drop table week_53_yrs;
create MULTISET volatile table week_53_yrs as  ( 
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
--drop table week_53_yr_count;
create MULTISET volatile table week_53_yr_count as (
select count(distinct year_num) year_count
from week_53_yrs x 
) with data primary index(year_count) on commit preserve rows;

/********* Step I-A-iii: Create an empty realigned calendar *********/
--drop table realigned_calendar;
create MULTISET volatile table realigned_calendar ,NO FALLBACK ,
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
        else null end day_date
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
 * PART I-B) date lookup tables
 ************************************************************************************/

/********* Step I-B-i: Start & End months of past 5 years *********/
--drop table month_lookup;
create multiset volatile table month_lookup as (
select distinct month_num as current_mo
  ,case when (month_num mod 100) = 1 then (month_num - 89) else (month_num - 1) end yr4_end_mo
  ,current_mo - 100 yr4_start_mo
  ,yr4_end_mo - 100 yr3_end_mo
  ,current_mo - 200 yr3_start_mo
  ,yr4_end_mo - 200 yr2_end_mo
  ,current_mo - 300 yr2_start_mo
  ,yr4_end_mo - 300 yr1_end_mo
  ,current_mo - 400 yr1_start_mo
  ,yr4_end_mo - 400 yr0_end_mo
  ,current_mo - 500 yr0_start_mo
from realigned_calendar 
where day_date = date'{buyer_end_date2}'+1
) with data primary index(current_mo) on commit preserve rows;

/********* Step I-B-ii: Start & End dates of past 5 years *********/
--drop table date_lookup;
create multiset volatile table date_lookup as (
select 
   date'{buyer_start_date1}' yr0_start_dt
  ,date'{buyer_end_date1}' yr0_end_dt
  ,date'{buyer_start_date2}' yr1_start_dt
  ,max(case when month_num = (select yr1_end_mo   from month_lookup) then day_date else null end) yr1_end_dt
  ,min(case when month_num = (select yr2_start_mo from month_lookup) then day_date else null end) yr2_start_dt
  ,max(case when month_num = (select yr2_end_mo   from month_lookup) then day_date else null end) yr2_end_dt
  ,min(case when month_num = (select yr3_start_mo from month_lookup) then day_date else null end) yr3_start_dt
  ,max(case when month_num = (select yr3_end_mo   from month_lookup) then day_date else null end) yr3_end_dt
  ,min(case when month_num = (select yr4_start_mo from month_lookup) then day_date else null end) yr4_start_dt
  ,max(case when month_num = (select yr4_end_mo   from month_lookup) then day_date else null end) yr4_end_dt
from realigned_calendar
) with data primary index(yr1_start_dt) on commit preserve rows;


/********* Step I-B-iii: Generate names for the past 5 rolling-12-mo periods *********/
create multiset volatile table month_names as (
select 
  max(case when month_num=(select yr0_end_mo   from month_lookup) 
          then cast(case when month_short_desc='JAN' then 'FY-'||cast((year_num mod 2000) as varchar(2))
                         else 'YE-'
                            ||substring(month_short_desc,1,1)
                            ||lower(substring(month_short_desc,2,2))
                            ||cast((year_num mod 2000) as varchar(2)) end
                as varchar(8)) else null end) 
   yr0_name
   ,max(case when month_num=(select yr1_end_mo   from month_lookup) 
          then cast(case when month_short_desc='JAN' then 'FY-'||cast((year_num mod 2000) as varchar(2))
                         else 'YE-'
                            ||substring(month_short_desc,1,1)
                            ||lower(substring(month_short_desc,2,2))
                            ||cast((year_num mod 2000) as varchar(2)) end
                as varchar(8)) else null end) 
   yr1_name
   ,max(case when month_num=(select yr2_end_mo   from month_lookup) 
          then cast(case when month_short_desc='JAN' then 'FY-'||cast((year_num mod 2000) as varchar(2))
                         else 'YE-'
                            ||substring(month_short_desc,1,1)
                            ||lower(substring(month_short_desc,2,2))
                            ||cast((year_num mod 2000) as varchar(2)) end
                as varchar(8)) else null end) 
   yr2_name
   ,max(case when month_num=(select yr3_end_mo   from month_lookup) 
          then cast(case when month_short_desc='JAN' then 'FY-'||cast((year_num mod 2000) as varchar(2))
                         else 'YE-'
                            ||substring(month_short_desc,1,1)
                            ||lower(substring(month_short_desc,2,2))
                            ||cast((year_num mod 2000) as varchar(2)) end
                as varchar(8)) else null end) 
   yr3_name
   ,max(case when month_num=(select yr4_end_mo   from month_lookup) 
          then cast(case when month_short_desc='JAN' then 'FY-'||cast((year_num mod 2000) as varchar(2))
                         else 'YE-'
                            ||substring(month_short_desc,1,1)
                            ||lower(substring(month_short_desc,2,2))
                            ||cast((year_num mod 2000) as varchar(2)) end
                as varchar(8)) else null end) 
   yr4_name
from realigned_calendar
where month_num in (
  (select yr0_end_mo   from month_lookup)
  ,(select yr1_end_mo   from month_lookup)
  ,(select yr2_end_mo   from month_lookup)
  ,(select yr3_end_mo   from month_lookup)
  ,(select yr4_end_mo   from month_lookup)
 )
) with data primary index(yr1_name) on commit preserve rows;
;

/************************************************************************************
 * PART I-C) Create "driver" tables (dates + customers in-scope)
 ************************************************************************************/

-- create multiset volatile table date_lookup as
-- (select date'2018-02-04' as start_dt
--        ,date'2022-08-27' as end_dt
-- )with data primary index(start_dt) on commit preserve rows;

/********* Step I-C-i: customer/year-level driver table *********/
create multiset volatile table cust_year_driver as (
select distinct a.acp_id
  ,a.reporting_year_shopped
from {cco_t2_schema}.cco_line_items a
where data_source='1) TRANSACTION'
  and date_shopped between (select yr1_start_dt from date_lookup) and  (select yr4_end_dt from date_lookup)
)with data primary index(acp_id,reporting_year_shopped) on commit preserve rows;

/********* Step I-C-ii: customer/channelyear-level driver table *********/
create multiset volatile table cust_chan_year_driver as (
select distinct a.acp_id
  ,a.channel
  ,a.reporting_year_shopped
from {cco_t2_schema}.cco_line_items a
where data_source='1) TRANSACTION'
    and date_shopped between (select yr1_start_dt from date_lookup) and  (select yr4_end_dt from date_lookup)
)with data primary index(acp_id,channel,reporting_year_shopped) on commit preserve rows;


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
  ,case when b.month_num between (select yr1_start_mo from month_lookup) and (select yr1_end_mo from month_lookup) 
          then (select yr1_name from month_names)
        when b.month_num between (select yr2_start_mo from month_lookup) and (select yr2_end_mo from month_lookup) 
          then (select yr2_name from month_names)
        when b.month_num between (select yr3_start_mo from month_lookup) and (select yr3_end_mo from month_lookup) 
          then (select yr3_name from month_names)
        when b.month_num between (select yr4_start_mo from month_lookup) and (select yr4_end_mo from month_lookup) 
          then (select yr4_name from month_names)
        else null end reporting_year_shopped
  ,a.aare_status_date acquisition_date
  ,case when a.aare_chnl_code = 'FLS'  then '1) Nordstrom Stores'
        when a.aare_chnl_code = 'NCOM' then '2) Nordstrom.com'
        when a.aare_chnl_code = 'RACK' then '3) Rack Stores'
        when a.aare_chnl_code = 'NRHL' then '4) Rack.com'
        else null end acquisition_channel
  ,case when a.aare_chnl_code in ('FLS','NCOM')  then '5) Nordstrom Banner'
        when a.aare_chnl_code in ('RACK','NRHL') then '6) Rack Banner'
        else null end acquisition_banner
from prd_nap_usr_vws.customer_ntn_status_fact  a
  join realigned_calendar b on a.aare_status_date=b.day_date
  join cust_year_driver dr on a.acp_id = dr.acp_id
where a.aare_status_date between (select yr1_start_dt from date_lookup) and  (select yr4_end_dt from date_lookup)
) with data primary index (acp_id,reporting_year_shopped) on commit preserve rows;


create MULTISET volatile table  customer_acquisition ,--NO FALLBACK ,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
(
     acp_id VARCHAR(50)
    ,reporting_year_shopped VARCHAR(10)
    ,channel VARCHAR(20)

) primary index (acp_id,reporting_year_shopped,channel) on commit preserve rows;

insert into customer_acquisition
select distinct acp_id
  ,reporting_year_shopped
  ,acquisition_channel channel
from customer_acquisition_prep
;


insert into customer_acquisition
select distinct acp_id
  ,reporting_year_shopped
  ,acquisition_banner channel
from customer_acquisition_prep
;

insert into customer_acquisition
select distinct acp_id
  ,reporting_year_shopped
  ,'7) JWN' channel
from customer_acquisition_prep
;


create MULTISET volatile table  customer_acquisition_jwn as (
select distinct acp_id
  ,reporting_year_shopped
from customer_acquisition
) with data primary index (acp_id,reporting_year_shopped) on commit preserve rows;


/************************************************************************************
 * PART II-B) Activation date/Channel
 ************************************************************************************/


create multiset volatile table customer_activation_prep as (
select distinct a.acp_id
  ,case when b.month_num between (select yr1_start_mo from month_lookup) and (select yr1_end_mo from month_lookup) 
          then (select yr1_name from month_names)
        when b.month_num between (select yr2_start_mo from month_lookup) and (select yr2_end_mo from month_lookup) 
          then (select yr2_name from month_names)
        when b.month_num between (select yr3_start_mo from month_lookup) and (select yr3_end_mo from month_lookup) 
          then (select yr3_name from month_names)
        when b.month_num between (select yr4_start_mo from month_lookup) and (select yr4_end_mo from month_lookup) 
          then (select yr4_name from month_names)
        else null end reporting_year_shopped
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
  join realigned_calendar b on a.activated_date=b.day_date
  join cust_year_driver dr on a.acp_id = dr.acp_id
where a.activated_date between (select yr1_start_dt from date_lookup) and  (select yr4_end_dt from date_lookup)
) with data primary index (acp_id,reporting_year_shopped) on commit preserve rows;



create MULTISET volatile table  customer_activation ,--NO FALLBACK ,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
(
     acp_id VARCHAR(50)
    ,reporting_year_shopped VARCHAR(10)
    ,channel VARCHAR(20)

) primary index (acp_id,reporting_year_shopped,channel) on commit preserve rows;

insert into customer_activation
select distinct acp_id
  ,reporting_year_shopped
  ,activation_channel channel
from customer_activation_prep
;

insert into customer_activation
select distinct acp_id
  ,reporting_year_shopped
  ,activation_banner channel
from customer_activation_prep
;

insert into customer_activation
select distinct acp_id
  ,reporting_year_shopped
  ,'7) JWN' channel
from customer_activation_prep
;


/************************************************************************************
 * PART II-C) New-to-Channel/Banner lookup:
 *      t2dl_das_strategy.cco_new2channel_lkup (already built)
 ************************************************************************************/

--drop table customer_new2channel_prep;
create multiset volatile table customer_new2channel_prep as (
select distinct a.acp_id
  ,case when b.month_num between (select yr1_start_mo from month_lookup) and (select yr1_end_mo from month_lookup) 
          then (select yr1_name from month_names)
        when b.month_num between (select yr2_start_mo from month_lookup) and (select yr2_end_mo from month_lookup) 
          then (select yr2_name from month_names)
        when b.month_num between (select yr3_start_mo from month_lookup) and (select yr3_end_mo from month_lookup) 
          then (select yr3_name from month_names)
        when b.month_num between (select yr4_start_mo from month_lookup) and (select yr4_end_mo from month_lookup) 
          then (select yr4_name from month_names)
        else null end reporting_year_shopped
  ,a.ntx_date new_to_channel_date
  ,case when trim(a.channel) = 'FL'  then '1) Nordstrom Stores'
        when trim(a.channel) = 'FC' then '2) Nordstrom.com'
        when trim(a.channel) = 'RK' then '3) Rack Stores'
        when trim(a.channel) = 'HL' then '4) Rack.com'
        when trim(a.channel) = 'FP'   then '5) Nordstrom Banner'
        when trim(a.channel) = 'OP'   then '6) Rack Banner'
        else null end channel
from {cco_t2_schema}.cco_new_to_channel_lkup a
  join realigned_calendar b on a.ntx_date=b.day_date
  join cust_year_driver dr on a.acp_id = dr.acp_id
where a.ntx_date between (select yr1_start_dt from date_lookup) and  (select yr4_end_dt from date_lookup)
) with data primary index (acp_id,reporting_year_shopped) on commit preserve rows;

--drop table customer_new2channel;
create MULTISET volatile table  customer_new2channel ,--NO FALLBACK ,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
(
     acp_id VARCHAR(50)
    ,reporting_year_shopped VARCHAR(10)
    ,channel VARCHAR(20)

) primary index (acp_id,reporting_year_shopped,channel) on commit preserve rows;


insert into customer_new2channel
select distinct acp_id
  ,reporting_year_shopped
  ,channel
from customer_new2channel_prep
;

insert into customer_new2channel
select distinct acp_id
  ,reporting_year_shopped
  ,'7) JWN' channel
from customer_acquisition_prep    --Yes, this comes from the Acquisition table (when "channel" = JWN)
;

-- sel count(*), count(distinct acp_id||channel) from customer_new2channel_prep;
-- sel count(*), count(distinct acp_id||channel) from customer_new2channel;


/************************************************************************************
 * II-D) Prior Year data (used for flagging "Retained" customers, folks who shopped last year):
 *
 *  (this part queries 1 year prior to earliest year in t2dl_das_strategy.cco_line_items)
 *
 ************************************************************************************/

create multiset volatile table cust_pre_cco_line_data as (
SELECT distinct dtl.acp_id
  ,case when st.business_unit_desc in ('FULL LINE','FULL LINE CANADA') then '1) Nordstrom Stores'
          when st.business_unit_desc in ('N.CA','N.COM','TRUNK CLUB') then '2) Nordstrom.com'
          when st.business_unit_desc in ('RACK', 'RACK CANADA') then '3) Rack Stores'
          when st.business_unit_desc in ('OFFPRICE ONLINE') then '4) Rack.com'
        else null end as channel
  ,case when cal.month_num between (select yr0_start_mo from month_lookup) and (select yr0_end_mo from month_lookup) 
          then (select yr0_name from month_names)
        when cal.month_num between (select yr1_start_mo from month_lookup) and (select yr1_end_mo from month_lookup) 
          then (select yr1_name from month_names)
        when cal.month_num between (select yr2_start_mo from month_lookup) and (select yr2_end_mo from month_lookup) 
          then (select yr2_name from month_names)
        when cal.month_num between (select yr3_start_mo from month_lookup) and (select yr3_end_mo from month_lookup) 
          then (select yr3_name from month_names)
        when cal.month_num between (select yr4_start_mo from month_lookup) and (select yr4_end_mo from month_lookup) 
          then (select yr4_name from month_names)
        else null end reporting_year_shopped
FROM prd_nap_usr_vws.RETAIL_TRAN_DETAIL_FACT_VW dtl
    join prd_nap_usr_vws.store_dim st on
       (case
        when dtl.line_item_order_type = 'CustInitWebOrder'
          and dtl.line_item_fulfillment_type = 'StorePickUp'
          and dtl.line_item_net_amt_currency_code = 'USD'
          then 808
        when dtl.line_item_order_type = 'CustInitWebOrder'
          and dtl.line_item_fulfillment_type = 'StorePickUp'
          and dtl.line_item_net_amt_currency_code = 'CAD'
          then 867
        else dtl.intent_store_num end) = st.store_num
    join realigned_calendar as cal on coalesce(dtl.order_date,dtl.tran_date) = cal.day_date
WHERE coalesce(dtl.order_date,dtl.tran_date) between (select yr0_start_dt from date_lookup) and  (select yr0_end_dt from date_lookup) 
     and dtl.business_day_date between (select yr0_start_dt from date_lookup) and  (select yr0_end_dt+14 from date_lookup)
     and dtl.acp_id is not null
     and st.business_unit_desc
       in ('FULL LINE','FULL LINE CANADA','N.CA','N.COM','TRUNK CLUB','RACK', 'RACK CANADA','OFFPRICE ONLINE')
     and dtl.line_net_usd_amt>=0
) with data primary index (acp_id,channel,reporting_year_shopped) on commit preserve rows;




-- sel count(*), count(distinct acp_id||channel) from cust_pre_cco_line_data;
-- sel count(*), count(distinct acp_id||channel) from cust_channel_ly;

/************************************************************************************
 * PART II-E) Create cust/channel/year-level table for all reported years (+ 1 earlier year)
 ************************************************************************************/

 /********* PART II-E-i) Create empty table *********/
--drop table cust_channel_yr;
create MULTISET volatile table  cust_channel_yr ,--NO FALLBACK ,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
(
     acp_id VARCHAR(50)
    ,reporting_year_shopped VARCHAR(10)
    ,channel VARCHAR(20)

) primary index (acp_id,reporting_year_shopped,channel) on commit preserve rows;

 /********* PART II-E-ii) Insert the rows for Channels as "Channels" *********/

--Insert whatever history t2dl_das_strategy.cco_line_items has
insert into cust_channel_yr
select distinct acp_id
  ,reporting_year_shopped
  ,channel
from cust_chan_year_driver
;

--insert any earlier history that t2dl_das_strategy.cco_line_items doesn't have
insert into cust_channel_yr
select distinct acp_id
  ,reporting_year_shopped
  ,channel
from cust_pre_cco_line_data
;


 /********* PART II-E-iii) Insert the rows for Banners as "Channels" *********/

 --Insert whatever history t2dl_das_strategy.cco_line_items has
insert into cust_channel_yr
select distinct acp_id
  ,reporting_year_shopped
  ,case when substring(channel,1,1) in ('1','2') then '5) Nordstrom Banner'
        when substring(channel,1,1) in ('3','4') then '6) Rack Banner'
        else null end channel
from cust_chan_year_driver
;

--insert any earlier history that t2dl_das_strategy.cco_line_items doesn't have
insert into cust_channel_yr
select distinct acp_id
  ,reporting_year_shopped
  ,case when substring(channel,1,1) in ('1','2') then '5) Nordstrom Banner'
        when substring(channel,1,1) in ('3','4') then '6) Rack Banner'
        else null end channel
from cust_pre_cco_line_data
;

/********* PART II-E-iv) Insert the rows for JWN as "Channel" *********/

--Insert whatever history t2dl_das_strategy.cco_line_items has
insert into cust_channel_yr
select distinct acp_id
  ,reporting_year_shopped
  ,'7) JWN' channel
from cust_year_driver
;

--insert any earlier history that t2dl_das_strategy.cco_line_items doesn't have
insert into cust_channel_yr
select distinct acp_id
  ,reporting_year_shopped
  ,'7) JWN' channel
from cust_pre_cco_line_data
;


/************************************************************************************
 * PART II-F) Create customer/channel/year-level table from the past 4 years (flagging whether they shopped 1 year before)
 ************************************************************************************/

/********* PART I-F-i) Create a "LY" column = the year before the current year *********/

--drop table cust_year_chan_ly;
create multiset volatile table cust_year_chan_ly as (
select acp_id
  ,reporting_year_shopped
  ,channel
  ,case when reporting_year_shopped = (select yr1_name from month_names) then (select yr0_name from month_names)
        when reporting_year_shopped = (select yr2_name from month_names) then (select yr1_name from month_names)
        when reporting_year_shopped = (select yr3_name from month_names) then (select yr2_name from month_names)
        when reporting_year_shopped = (select yr4_name from month_names) then (select yr3_name from month_names)
        else null end ly 
from cust_channel_yr
) with data primary index (acp_id,reporting_year_shopped,channel) on commit preserve rows;

/********* PART I-F-ii) Join the current year to the prior year on:
 *    a) customer/channel/year... to get a channel-level retention indicators
 *    b) customer/year ... to get JWN-level retention indicators
 *  *********/

--drop table cust_year_chan;
create multiset volatile table cust_year_chan as (
select a.acp_id
  ,a.reporting_year_shopped
  ,a.channel
  ,case when a.acp_id=b.acp_id and a.channel=b.channel and a.ly = b.reporting_year_shopped then 1 else 0 end chan_ret_flag
  ,case when a.acp_id=c.acp_id and a.ly = c.reporting_year_shopped then 1 else 0 end jwn_ret_flag
from  cust_year_chan_ly a
  left join cust_year_chan_ly b
    on a.acp_id=b.acp_id and a.channel=b.channel and a.ly = b.reporting_year_shopped
  left join
    (select distinct acp_id
      ,reporting_year_shopped
     from cust_year_chan_ly) c on a.acp_id=c.acp_id and a.ly = c.reporting_year_shopped
where a.reporting_year_shopped
    in ((select yr1_name from month_names)
        ,(select yr2_name from month_names)
        ,(select yr3_name from month_names)
        ,(select yr4_name from month_names)
       )
) with data primary index (acp_id,reporting_year_shopped,channel) on commit preserve rows;

-- sel count(*), count(distinct acp_id||channel) from cust_chan_year_driver;
-- sel count(*), count(distinct acp_id||channel) from cust_channel_yr;


/************************************************************************************/
/************************************************************************************
 * PART III) Join all tables together to create Buyer-Flow & AARE Indicators
 ************************************************************************************/
/************************************************************************************/

/************************************************************************************
 * PART III-A) Join all tables together to create indicators
 * **********************************************************************************/

COLLECT STATISTICS COLUMN (ACP_ID ,REPORTING_YEAR_SHOPPED     ,CHANNEL) ON customer_new2channel;
COLLECT STATISTICS COLUMN (ACP_ID ,REPORTING_YEAR_SHOPPED) ON     cust_year_chan;
COLLECT STATISTICS COLUMN (ACP_ID ,REPORTING_YEAR_SHOPPED) ON     customer_acquisition_jwn;
COLLECT STATISTICS COLUMN (ACP_ID ,REPORTING_YEAR_SHOPPED     ,CHANNEL) ON customer_acquisition;
COLLECT STATISTICS COLUMN (ACP_ID ,REPORTING_YEAR_SHOPPED     ,CHANNEL) ON cust_year_chan;
COLLECT STATISTICS COLUMN (ACP_ID ,REPORTING_YEAR_SHOPPED     ,CHANNEL) ON customer_activation;

--drop table aare_buyer_flow_prep;
create multiset volatile table aare_buyer_flow_prep as (
--explain
select distinct a.acp_id
  ,a.reporting_year_shopped
  ,a.channel

  ,a.chan_ret_flag
  ,a.jwn_ret_flag
  ,case when a.acp_id=c.acp_id and a.reporting_year_shopped=c.reporting_year_shopped
              and a.jwn_ret_flag=0 then 1
        else 0 end ntn_jwn_flag
  ,case when a.acp_id=b.acp_id and a.reporting_year_shopped=b.reporting_year_shopped and a.channel=b.channel
              and a.jwn_ret_flag=0 and ntn_jwn_flag=1 then 1
        else 0 end ntn_chan_flag
  ,case when a.acp_id=d.acp_id and a.reporting_year_shopped=d.reporting_year_shopped and a.channel=d.channel
              and (a.jwn_ret_flag=1 or ntn_jwn_flag=1) then 1
        else 0 end act_flag
  ,case when a.acp_id=e.acp_id and a.reporting_year_shopped=e.reporting_year_shopped and a.channel=e.channel
              and a.chan_ret_flag=0 then 1
        when ntn_jwn_flag=1 then 1
        else 0 end ntc_flag

from cust_year_chan a
  left join customer_acquisition b on a.acp_id=b.acp_id and a.reporting_year_shopped=b.reporting_year_shopped and a.channel=b.channel
  left join customer_acquisition_jwn c on a.acp_id=c.acp_id and a.reporting_year_shopped=c.reporting_year_shopped
  left join customer_activation d on a.acp_id=d.acp_id and a.reporting_year_shopped=d.reporting_year_shopped and a.channel=d.channel
  left join customer_new2channel e on a.acp_id=e.acp_id and a.reporting_year_shopped=e.reporting_year_shopped and a.channel=e.channel
) with data primary index(acp_id,reporting_year_shopped,channel) on commit preserve rows;


/************************************************************************************
 * PART III-B) Create Buyer-Flow & AARE columns
 * **********************************************************************************/


/********* PART III-B-i) Create the Buyer-Flow segments + Channel-level AARE indicators
 * 1) Create the Buyer-Flow segments first,
 * 2) then create the AARE columns in a way that Buyer-Flow & AARE don't conflict
 * *********/

--drop table buyer_flow_all_views;
create multiset volatile table buyer_flow_all_views as (
select distinct acp_id
  ,reporting_year_shopped
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
  ,case when act_flag = 1 and (ntn_jwn_flag = 1 or jwn_ret_flag = 1)and substring(buyer_flow,1,1) not in ('4') then 1 else 0 end AARE_activated
  ,case when jwn_ret_flag = 1 and substring(buyer_flow,1,1) <> '1' then 1 else 0 end AARE_retained
  ,case when ntc_flag = 1 and ntn_chan_flag = 0 and chan_ret_flag=0 and substring(buyer_flow,1,1) = '2' then 1 else 0 end AARE_engaged
from aare_buyer_flow_prep
--where substring(channel,1,1) in ('1','2','3','4')
) with data primary index(acp_id,reporting_year_shopped,channel) on commit preserve rows;


/********* PART III-B-ii) Create the Banner-level AARE indicators
 * Ideally, these are the "max" of the channel-level indicators... but must be changed if they conflict with Buyer-Flow values
 * *********/

--drop table aare_banner_level;
create multiset volatile table aare_banner_level as (
--explain
select distinct a.acp_id
    ,a.reporting_year_shopped
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
    ,reporting_year_shopped
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
    ,reporting_year_shopped
    ,channel
    ,buyer_flow
    ,ntc_flag
  from buyer_flow_all_views
  where substring(channel,1,1) in ('5','6')
  ) b
  on a.acp_id=b.acp_id and a.reporting_year_shopped=b.reporting_year_shopped and a.channel=b.channel
) with data primary index(acp_id,reporting_year_shopped,channel) on commit preserve rows;


/********* PART III-B-iii) Create the JWN-level AARE indicators
 * Ideally, these are the "max" of the banner-level indicators... but must be changed if they conflict with Buyer-Flow values
 * *********/

--drop table aare_jwn_level;
create multiset volatile table aare_jwn_level as (
--explain
select distinct a.acp_id
    ,a.reporting_year_shopped
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
    ,reporting_year_shopped
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
    ,reporting_year_shopped
    ,channel
    ,buyer_flow
    ,ntc_flag
  from buyer_flow_all_views
  where substring(channel,1,1) ='7'
  ) b
  on a.acp_id=b.acp_id and a.reporting_year_shopped=b.reporting_year_shopped and a.channel=b.channel
) with data primary index(acp_id,reporting_year_shopped,channel) on commit preserve rows;


/************************************************************************************
 * PART III-C) Insert Buyer-Flow & AARE values into final table
 * **********************************************************************************/

/********* PART III-C-i) insert Channel-level Buyer-Flow segments & AARE flags *********/

DELETE FROM {cco_t2_schema}.cco_buyer_flow ALL;

INSERT INTO {cco_t2_schema}.cco_buyer_flow
select distinct acp_id
  ,reporting_year_shopped
  ,channel
  ,buyer_flow
  ,AARE_acquired
  ,AARE_activated
  ,AARE_retained
  ,AARE_engaged
from buyer_flow_all_views
where substring(channel,1,1) in ('1','2','3','4')


/********* PART III-C-ii) insert Banner-level Buyer-Flow segments & AARE flags *********/

 
union all
select distinct acp_id
  ,reporting_year_shopped
  ,channel
  ,buyer_flow
  ,AARE_acquired
  ,AARE_activated
  ,AARE_retained
  ,AARE_engaged
from aare_banner_level

/********* PART III-C-iii) insert JWN-level Buyer-Flow segments & AARE flags *********/

 
union all
select distinct acp_id
  ,reporting_year_shopped
  ,channel
  ,buyer_flow
  ,AARE_acquired
  ,AARE_activated
  ,AARE_retained
  ,AARE_engaged
from aare_jwn_level;

collect statistics
column  (acp_id,reporting_year_shopped,channel)              
on  
{cco_t2_schema}.cco_buyer_flow;


SET QUERY_BAND = NONE FOR SESSION;