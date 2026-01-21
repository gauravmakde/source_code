SET QUERY_BAND = 'App_ID=APP08240;
DAG_ID=cco_tables_week_grain_11521_ACE_ENG;
Task_Name=run_cco_buyer_flow_cust_channel_week;'
FOR SESSION VOLATILE;


/************************************************************************************/
/************************************************************************************/
/************************************************************************************
 *
 * Build a Customer/Week/Channel-level table for AARE & Buyer Flow
 *
 * weekly version Aug 2024 from modification to yearly buyer flow sql
 * note that much of the code involves first developing an acp_id, week, channel level
 * record, and then programmatically inserting a banner level record, and total JWN record
 *
 ************************************************************************************/
 /***********************************************************************************/
 /***********************************************************************************/


/************************************************************************************
 * PART I-A) Dynamic dates for CCO tables
 *
 * This code uses the current_date to
 * 1) Determine the current and most-recently complete fiscal month
 * 2) Get the following dates needed for the CCO tables
 *    a) fy18_start_dt: Start of FY18
 *    b) fy18_end_dt: End of FY18
 *    c) fy19_start_dt: Start of FY18
 *    d) latest_mo_dt: End of most-recently complete fiscal month
 *    e) r4yr1_start_dt: Start of rolling-4-year period ending most-recently complete fiscal month
 *    f) r4yr0_start_dt: Start of year prior to
 *                       rolling-4-year period ending most-recently complete fiscal month
 *    g) r4yr0_end_dt: End of year prior to
 *                     rolling-4-year period ending most-recently complete fiscal month
 ************************************************************************************/

/*********** Determine the current & most-recently complete fiscal month ************/
create multiset volatile table curr_mo_lkp as (
select distinct month_num curr_mo
     , case when (month_num mod 100) = 1 then (month_num - 89) else (month_num - 1) end prior_mo
     , year_num curr_year
from t2dl_das_usl.usl_rolling_52wk_calendar
where day_date = current_date
) with data primary index(curr_mo) on commit preserve rows;


/*********** Get the dates needed for the CCO tables ********************************/
create multiset volatile table date_parameter_lookup as (
select min(case when year_num = 2018 then day_date else null end) fy18_start_dt
     , max(case when year_num = 2018 then day_date else null end) fy18_end_dt
     , min(case when year_num = 2019 then day_date else null end) fy19_start_dt
     , max(case when month_num < (select curr_mo from curr_mo_lkp)
                then day_date
                else null end) latest_mo_dt
     , min(case when month_num = (select curr_mo-400 from curr_mo_lkp)
                then day_date
                else null end) r4yr1_start_dt
     , min(case when month_num = (select curr_mo-500 from curr_mo_lkp)
                then day_date
                else null end) r4yr0_start_dt
     , max(case when month_num = (select prior_mo-400 from curr_mo_lkp)
                then day_date
                else null end) r4yr0_end_dt
from t2dl_das_usl.usl_rolling_52wk_calendar
) with data primary index(latest_mo_dt) on commit preserve rows;


/*********** Create Start & End dates lookup table **********************************/
create multiset volatile table date_lookup as (
select dp.fy19_start_dt cust_year_start_date
     , dp.latest_mo_dt cust_year_end_date
from date_parameter_lookup dp
) with data primary index(cust_year_start_date) on commit preserve rows;

--dates for Pre-Prod testing
-- create multiset volatile table date_lookup as (
-- select date'2024-09-10' cust_year_start_date
--      , date'2024-09-10'+7 cust_year_end_date
-- from date_parameter_lookup dp
-- ) with data primary index(cust_year_start_date) on commit preserve rows;

/************************************************************************************
 * PART I-B) Create "driver" tables based on the transactions in CCO_LINES
 * (i.e., transaction filtering has already happened when building CCO_LINES, so we don't need
 * to repeat it here - just pull the transactions from CCO_LINES as a driver for the
 * rest of this query. Some table pulls below will be by acp_id, and some by acp_id and channel,
 * so there are two drivers
 ************************************************************************************/

create multiset volatile table cco_lines_extract as (
select distinct a.acp_id
  ,a.channel
  ,a.date_shopped
from t2dl_das_strategy.cco_line_items a
where data_source = '1) TRANSACTION'
  and date_shopped between (select cust_year_start_date from date_lookup)
                       and (select cust_year_end_date from date_lookup)
) with data primary index (acp_id, channel, date_shopped)
partition by range_n(date_shopped between date '2017-01-01' and date '2027-12-31' each interval '1' day)
on commit preserve rows;

collect statistics column (partition) on cco_lines_extract;
collect statistics column (date_shopped) on cco_lines_extract;


/*********** Create customer/week-level driver table ********************************/
create multiset volatile table cust_week_driver as (
select distinct a.acp_id
  ,b.week_idnt
from cco_lines_extract a
join prd_nap_usr_vws.day_cal_454_dim b
  on a.date_shopped = b.day_date
) with data primary index (acp_id, week_idnt) on commit preserve rows;

collect statistics column (acp_id, week_idnt) on cust_week_driver;


/*********** Create customer/channel/week-level driver table ************************/
create multiset volatile table cust_chan_week_driver as (
select distinct a.acp_id
  ,a.channel
  ,b.week_idnt
from cco_lines_extract a
join prd_nap_usr_vws.day_cal_454_dim b
  on a.date_shopped = b.day_date
) with data primary index (acp_id, channel, week_idnt) on commit preserve rows;

collect statistics column (acp_id, channel, week_idnt) on cust_chan_week_driver;


/************************************************************************************/
/************************************************************************************
 * PART II) Build lookup tables:
 *  A) Acquisition date/channel
 *  B) Activation date/Channel
 *  C) New-to-Channel/Banner lookup
 *  D) Table containing customer/channel from 52 weeks prior
 *  E) Create customer/channel/week-level table from the past 5 years (4 years from line-item table + 1 year from "I-D")
 *  F) Create customer/channel/week-level table from the past 4 years (flagging whether they shopped 1 year before)
 ************************************************************************************/
/************************************************************************************/

/*******************************************
  most aare (acquisition, activation, ntn) just need to pull from associated tables for those values
  however, aare dates are by day, so need to convert to week
  also, they are by acp_id, and we just need acp_id's that are in cco_lines table
*******************************************/
create multiset volatile table day_week_idnt as (
select distinct day_date
  ,week_idnt
from prd_nap_usr_vws.day_cal_454_dim a
where day_date between (select cust_year_start_date from date_lookup)
                   and (select cust_year_end_date from date_lookup)
) with data primary index (day_date) on commit preserve rows;

collect statistics column (day_date) on day_week_idnt;


create multiset volatile table acp_id_list as (
select distinct a.acp_id
from cust_week_driver a
) with data primary index (acp_id) on commit preserve rows;

collect statistics column (acp_id) on acp_id_list;


/************************************************************************************
 * PART II-A) For each week, build a table of the 52 weeks preceding it
 ************************************************************************************/

create multiset volatile table extract_week_idnts as (
select distinct b.week_idnt
from prd_nap_usr_vws.day_cal_454_dim b
where b.day_date between (select cust_year_start_date - 365 from date_lookup)
                     and (select cust_year_end_date from date_lookup)
) with data primary index(week_idnt)
on commit preserve rows;

create multiset volatile table cust_52wks as (
select distinct b.week_idnt as curr_week_idnt
  ,lag(b.week_idnt, 52) over(order by b.week_idnt) as prev_52wk_start_week_idnt
  ,lag(b.week_idnt, 1) over(order by b.week_idnt) as prev_52wk_end_week_idnt
from extract_week_idnts b
) with data primary index(curr_week_idnt)
on commit preserve rows;

create multiset volatile table cust_52wks_expand_wks as (
select distinct
   wks.curr_week_idnt
  ,cal.week_idnt as prev_52_wks_wk_idnt
from cust_52wks wks
join extract_week_idnts cal
  on cal.week_idnt between wks.prev_52wk_start_week_idnt and prev_52wk_end_week_idnt
) with data primary index (curr_week_idnt, prev_52_wks_wk_idnt)
on commit preserve rows;

collect statistics column (curr_week_idnt, prev_52_wks_wk_idnt) on cust_52wks_expand_wks;
collect statistics column (curr_week_idnt) on cust_52wks_expand_wks;
collect statistics column (prev_52_wks_wk_idnt) on cust_52wks_expand_wks;

/************************************************************************************
 * PART II-B) Acquisition date/channel
 ************************************************************************************/

create multiset volatile table customer_acquisition_prep as (
select distinct a.acp_id
  ,c.week_idnt
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
join acp_id_list b
  on a.acp_id = b.acp_id
join day_week_idnt c
  on a.aare_status_date = c.day_date
) with data primary index (acp_id, week_idnt) on commit preserve rows;


create multiset volatile table customer_acquisition ,--NO FALLBACK ,
  CHECKSUM = DEFAULT,
  DEFAULT MERGEBLOCKRATIO
(
  acp_id VARCHAR(50)
  ,week_idnt INTEGER
  ,channel VARCHAR(20)
) primary index (acp_id, week_idnt, channel) on commit preserve rows;

insert into customer_acquisition
select distinct acp_id
  ,week_idnt
  ,acquisition_channel channel
from customer_acquisition_prep
;

insert into customer_acquisition
select distinct acp_id
  ,week_idnt
  ,acquisition_banner channel
from customer_acquisition_prep
;

insert into customer_acquisition
select distinct acp_id
  ,week_idnt
  ,'7) JWN' channel
from customer_acquisition_prep
;


create multiset volatile table customer_acquisition_jwn as (
select distinct acp_id
  ,week_idnt
from customer_acquisition
) with data primary index (acp_id, week_idnt) on commit preserve rows;


/************************************************************************************
 * PART II-C) Activation date/Channel
 ************************************************************************************/

create multiset volatile table customer_activation_prep as (
select distinct a.acp_id
  ,c.week_idnt
  ,a.activated_date activation_date
  ,case when a.activated_chnl_code = 'FLS'  then '1) Nordstrom Stores'
        when a.activated_chnl_code = 'NCOM' then '2) Nordstrom.com'
        when a.activated_chnl_code = 'RACK' then '3) Rack Stores'
        when a.activated_chnl_code = 'NRHL' then '4) Rack.com'
        else null end activation_channel
  ,case when a.activated_chnl_code in ('FLS','NCOM')  then '5) Nordstrom Banner'
        when a.activated_chnl_code in ('RACK','NRHL') then '6) Rack Banner'
        else null end activation_banner
from prd_nap_usr_vws.customer_activated_fact a
join acp_id_list b
  on a.acp_id = b.acp_id
join day_week_idnt c
  on a.activated_date = c.day_date
) with data primary index (acp_id, week_idnt) on commit preserve rows;


create multiset volatile table customer_activation ,--NO FALLBACK ,
  CHECKSUM = DEFAULT,
  DEFAULT MERGEBLOCKRATIO
(
  acp_id VARCHAR(50)
  ,week_idnt INTEGER
  ,channel VARCHAR(20)
) primary index (acp_id, week_idnt, channel) on commit preserve rows;

insert into customer_activation
select distinct acp_id
  ,week_idnt
  ,activation_channel channel
from customer_activation_prep
;

insert into customer_activation
select distinct acp_id
  ,week_idnt
  ,activation_banner channel
from customer_activation_prep
;

insert into customer_activation
select distinct acp_id
  ,week_idnt
  ,'7) JWN' channel
from customer_activation_prep
;


/************************************************************************************
 * PART II-D) New-to-Channel/Banner lookup:
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
) with data primary index (acp_id, channel) on commit preserve rows;

collect statistics column (acp_id  ) on cco_new_2_channel_lkup;
collect statistics column (channel ) on cco_new_2_channel_lkup;
collect statistics column (ntx_date) on cco_new_2_channel_lkup;


create multiset volatile table customer_new2channel_prep as (
select distinct a.acp_id
  ,c.week_idnt
  ,a.ntx_date new_to_channel_date
  ,case when trim(a.channel) = 'N_STORE'   then '1) Nordstrom Stores'
        when trim(a.channel) = 'N_COM'     then '2) Nordstrom.com'
        when trim(a.channel) = 'R_STORE'   then '3) Rack Stores'
        when trim(a.channel) = 'R_COM'     then '4) Rack.com'
        when trim(a.channel) = 'NORDSTROM' then '5) Nordstrom Banner'
        when trim(a.channel) = 'RACK'      then '6) Rack Banner'
        else null end channel
from cco_new_2_channel_lkup a
join acp_id_list b
  on a.acp_id = b.acp_id
join day_week_idnt c
  on a.ntx_date = c.day_date
) with data primary index (acp_id, week_idnt) on commit preserve rows;


create multiset volatile table customer_new2channel ,--NO FALLBACK ,
  CHECKSUM = DEFAULT,
  DEFAULT MERGEBLOCKRATIO
(
  acp_id VARCHAR(50)
  ,week_idnt INTEGER
  ,channel VARCHAR(20)
) primary index (acp_id, week_idnt, channel) on commit preserve rows;

insert into customer_new2channel
select distinct acp_id
  ,week_idnt
  ,channel
from customer_new2channel_prep
;

insert into customer_new2channel
select distinct acp_id
  ,week_idnt
  ,'7) JWN' channel
from customer_acquisition_prep;


/************************************************************************************
 * Part II-E) Prior data (used for flagging "Retained" customers, folks who shopped
 * in 52 wks prior to the 4 year (by week) window pulled from CCO_LINES
 ************************************************************************************/

create multiset volatile table cust_prev_cco_line_data,
no fallback, no before journal, no after journal,
checksum = default
(
  acp_id VARCHAR(50) CHARACTER SET UNICODE
  ,channel VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC
  ,week_idnt INTEGER
) primary index (acp_id, channel, week_idnt)
on commit preserve rows;

insert into cust_prev_cco_line_data
select distinct dtl.acp_id
  ,case when st.business_unit_desc in ('FULL LINE','FULL LINE CANADA') then '1) Nordstrom Stores'
        when st.business_unit_desc in ('N.CA','N.COM','TRUNK CLUB','MARKETPLACE') then '2) Nordstrom.com'
        when st.business_unit_desc in ('RACK','RACK CANADA') then '3) Rack Stores'
        when st.business_unit_desc in ('OFFPRICE ONLINE') then '4) Rack.com'
        else null end as channel
  ,c.week_idnt
from prd_nap_usr_vws.retail_tran_detail_fact_vw dtl
join prd_nap_usr_vws.store_dim as ist
  on dtl.intent_store_num = ist.store_num
join prd_nap_usr_vws.store_dim as st
  on (case
        when dtl.line_item_order_type in ('CustInitWebOrder','CustInitPhoneOrder') and ist.store_type_code = 'RK' then 828
        when dtl.line_item_order_type in ('CustInitWebOrder','CustInitPhoneOrder') and dtl.line_item_net_amt_currency_code = 'CAD' then 867
        when dtl.line_item_order_type in ('CustInitWebOrder','CustInitPhoneOrder') then 808
        when dtl.intent_store_num = 5405 then 808
        else dtl.intent_store_num end) = st.store_num
join acp_id_list b
  on dtl.acp_id = b.acp_id
join day_week_idnt c
  on coalesce(dtl.order_date, dtl.tran_date) = c.day_date
where dtl.business_day_date between (select cust_year_start_date - 728 from date_lookup)
                                and (select cust_year_start_date from date_lookup)
  and coalesce(dtl.order_date, dtl.tran_date) between (select cust_year_start_date - 721 from date_lookup)
                                                  and (select cust_year_start_date from date_lookup)
  and dtl.acp_id is not null
  and dtl.line_net_usd_amt >= 0
;


/************************************************************************************
 * PART II-F) Create cust/channel/week-level table for all report weeks (+ 1 earlier year)
 ************************************************************************************/

/*********** PART II-F-i) Create empty table ****************************************/
create multiset volatile table cust_week_channel_base ,--NO FALLBACK ,
  CHECKSUM = DEFAULT,
  DEFAULT MERGEBLOCKRATIO
(
  acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
  ,week_idnt INTEGER
  ,channel VARCHAR(20)
) primary index (acp_id, week_idnt, channel) on commit preserve rows;

/*********** PART II-F-ii) Insert the rows for Channels as "Channels" ***************/
insert into cust_week_channel_base
select distinct acp_id
  ,week_idnt
  ,channel
from cust_chan_week_driver;

-- insert any earlier history that cco_line_items doesn't have
insert into cust_week_channel_base
select distinct acp_id
  ,week_idnt
  ,channel
from cust_prev_cco_line_data;

collect statistics column (acp_id, week_idnt, channel) on cust_week_channel_base;
collect statistics column (acp_id, week_idnt) on cust_week_channel_base;
collect statistics column (acp_id, channel) on cust_week_channel_base;
collect statistics column (week_idnt) on cust_week_channel_base;


/*********** PART II-F-iii) Insert the rows for Banners as "Channels" ***************/
-- insert whatever history cco_line_items has
create multiset volatile table cust_week_channel_channel ,--NO FALLBACK ,
  CHECKSUM = DEFAULT,
  DEFAULT MERGEBLOCKRATIO
(
  acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
  ,week_idnt INTEGER
  ,channel VARCHAR(20)
) primary index (acp_id, week_idnt, channel) on commit preserve rows;

insert into cust_week_channel_channel
select distinct acp_id
  ,week_idnt
  ,case when substring(channel,1,1) in ('1','2') then '5) Nordstrom Banner'
        when substring(channel,1,1) in ('3','4') then '6) Rack Banner'
        else null end channel
from cust_chan_week_driver
;

--insert any earlier history that cco_line_items doesn't have
insert into cust_week_channel_channel
select distinct acp_id
  ,week_idnt
  ,case when substring(channel,1,1) in ('1','2') then '5) Nordstrom Banner'
        when substring(channel,1,1) in ('3','4') then '6) Rack Banner'
        else null end channel
from cust_prev_cco_line_data;

collect statistics column (acp_id, week_idnt, channel) on cust_week_channel_channel;
collect statistics column (acp_id, week_idnt) on cust_week_channel_channel;
collect statistics column (acp_id, channel) on cust_week_channel_channel;
collect statistics column (week_idnt) on cust_week_channel_channel;


/*********** PART II-F-iv) Insert the rows for JWN as "Channel" *********************/
create multiset volatile table cust_week_channel_banner ,--NO FALLBACK ,
  CHECKSUM = DEFAULT,
  DEFAULT MERGEBLOCKRATIO
(
  acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
  ,week_idnt INTEGER
  ,channel VARCHAR(20)
) primary index (acp_id,week_idnt,channel) on commit preserve rows;

insert into cust_week_channel_banner
select distinct acp_id
  ,week_idnt
  ,'7) JWN' channel
from cust_week_driver
;

-- insert any earlier history that cco_line_items doesn't have
insert into cust_week_channel_banner
select distinct acp_id
  ,week_idnt
  ,'7) JWN' channel
from cust_prev_cco_line_data;

collect statistics column (acp_id, week_idnt, channel) on cust_week_channel_banner;
collect statistics column (acp_id, week_idnt) on cust_week_channel_banner;
collect statistics column (acp_id, channel) on cust_week_channel_banner;
collect statistics column (week_idnt) on cust_week_channel_banner;


/******************************************************
 Make a table of weeks having previous 52 week purchases, so a
 successful join to this table means there was activity in
 the 52 wk period preceding this wk
 To not exceed cpu limits, it breaks into 3 pieces
 base, channel, and banner. Also exceeded cpu limits,
 so build three NOPI tables unioned into final table
 prev_52wk_table because teradata loads empty tables
 with block loads for speed.
 *****************************************************/

create multiset volatile table prev_52wk_table_base ,--NO FALLBACK ,
  CHECKSUM = DEFAULT,
  DEFAULT MERGEBLOCKRATIO
(
  acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
  ,week_idnt INTEGER
  ,channel VARCHAR(20)
) no primary index on commit preserve rows;

insert into prev_52wk_table_base
select distinct drv1.acp_id
  ,drv1.week_idnt
  ,drv1.channel
from cust_week_channel_base drv1
join cust_week_channel_base drv2
  on drv1.acp_id = drv2.acp_id
  and drv1.channel = drv2.channel
join cust_52wks_expand_wks wks
  on drv1.week_idnt = wks.curr_week_idnt
  and drv2.week_idnt = wks.prev_52_wks_wk_idnt;


create multiset volatile table prev_52wk_table_channel ,--NO FALLBACK ,
  CHECKSUM = DEFAULT,
  DEFAULT MERGEBLOCKRATIO
(
  acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
  ,week_idnt INTEGER
  ,channel VARCHAR(20)
) no primary index on commit preserve rows;

insert into prev_52wk_table_channel
select distinct drv1.acp_id
  ,drv1.week_idnt
  ,drv1.channel
from cust_week_channel_channel drv1
join cust_week_channel_channel drv2
  on drv1.acp_id = drv2.acp_id
  and drv1.channel = drv2.channel
join cust_52wks_expand_wks wks
  on drv1.week_idnt = wks.curr_week_idnt
  and drv2.week_idnt = wks.prev_52_wks_wk_idnt;


create multiset volatile table prev_52wk_table_banner ,--NO FALLBACK ,
  CHECKSUM = DEFAULT,
  DEFAULT MERGEBLOCKRATIO
(
  acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
  ,week_idnt INTEGER
  ,channel VARCHAR(20)
) no primary index on commit preserve rows;

insert into prev_52wk_table_banner
select distinct drv1.acp_id
  ,drv1.week_idnt
  ,drv1.channel
from cust_week_channel_banner drv1
join cust_week_channel_banner drv2
  on drv1.acp_id = drv2.acp_id
  and drv1.channel = drv2.channel
join cust_52wks_expand_wks wks
  on drv1.week_idnt = wks.curr_week_idnt
  and drv2.week_idnt = wks.prev_52_wks_wk_idnt;


create multiset volatile table prev_52wk_table ,--NO FALLBACK ,
  CHECKSUM = DEFAULT,
  DEFAULT MERGEBLOCKRATIO
(
  acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
  ,week_idnt INTEGER
  ,channel VARCHAR(20)
) primary index (acp_id, week_idnt, channel) on commit preserve rows;

insert into prev_52wk_table
select *
from prev_52wk_table_base
union all
select *
from prev_52wk_table_channel
union all
select *
from prev_52wk_table_banner;

collect statistics column (acp_id, week_idnt, channel) on prev_52wk_table;
collect statistics column (acp_id, week_idnt) on prev_52wk_table;


/*********** Join the current week to the prior 52 week year on:
 *    a) customer/channel/week... to get a channel-level retention indicators
 *    b) customer/week ... to get JWN-level retention indicators
 ***********/

create multiset volatile table cust_channel_wk as (
select *
from cust_week_channel_base
union all
select *
from cust_week_channel_channel
union all
select *
from cust_week_channel_banner
) with data primary index (acp_id, week_idnt, channel) on commit preserve rows;

collect statistics column (acp_id, week_idnt, channel) on cust_channel_wk;
collect statistics column (acp_id, week_idnt) on cust_channel_wk;


create multiset volatile table cust_week_chan_retention as (
select distinct a.acp_id
  ,a.week_idnt
  ,a.channel
  ,case when b.acp_id is not null then 1 else 0 end chan_ret_flag
  ,case when c.acp_id is not null then 1 else 0 end jwn_ret_flag
from cust_channel_wk a
left join prev_52wk_table b
  on a.acp_id = b.acp_id
  and a.week_idnt = b.week_idnt
  and a.channel = b.channel
left join (select distinct acp_id, week_idnt from prev_52wk_table) c
  on a.acp_id = c.acp_id
  and a.week_idnt = c.week_idnt
) with data primary index (acp_id, week_idnt, channel) on commit preserve rows;


/************************************************************************************/
/************************************************************************************
 * PART III) Join all tables together to create Buyer-Flow & AARE Indicators
 ************************************************************************************/
/************************************************************************************/

/************************************************************************************
 * PART III-A) Join all tables together to create indicators
 ************************************************************************************/

collect statistics column (acp_id, week_idnt, channel) on cust_week_chan_retention;
collect statistics column (acp_id, week_idnt) on cust_week_chan_retention;
collect statistics column (acp_id, week_idnt) on customer_acquisition_jwn;
collect statistics column (acp_id, week_idnt, channel) on customer_acquisition;
collect statistics column (acp_id, week_idnt, channel) on customer_activation;
collect statistics column (acp_id, week_idnt, channel) on customer_new2channel;


create multiset volatile table aare_buyer_flow_prep as (
select distinct retain.acp_id
  ,retain.week_idnt
  ,retain.channel
  ,retain.chan_ret_flag
  ,retain.jwn_ret_flag
  ,case when acqjwn.acp_id is not null
         and retain.jwn_ret_flag = 0 then 1
        else 0 end ntn_jwn_flag
  ,case when acq.acp_id is not null
         and retain.jwn_ret_flag = 0
         and ntn_jwn_flag = 1 then 1
        else 0 end ntn_chan_flag
  ,case when act.acp_id is not null
         and (retain.jwn_ret_flag = 1 or ntn_jwn_flag = 1) then 1
        else 0 end act_flag
  ,case when ntn.acp_id is not null
         and retain.chan_ret_flag = 0 then 1
        when ntn_jwn_flag = 1 then 1
        else 0 end ntc_flag
from cust_week_chan_retention retain
left join customer_acquisition acq
  on retain.acp_id = acq.acp_id
  and retain.week_idnt = acq.week_idnt
  and retain.channel = acq.channel
left join customer_acquisition_jwn acqjwn
  on retain.acp_id = acqjwn.acp_id
  and retain.week_idnt = acq.week_idnt
left join customer_activation act
  on retain.acp_id = act.acp_id
  and retain.week_idnt = acq.week_idnt
  and retain.channel = act.channel
left join customer_new2channel ntn
  on retain.acp_id = ntn.acp_id
  and retain.week_idnt = acq.week_idnt
  and retain.channel = ntn.channel
) with data primary index (acp_id, week_idnt, channel) on commit preserve rows;


/************************************************************************************
 * PART III-B) Create Buyer-Flow & AARE columns
 ************************************************************************************/

/*********** PART III-B-i) Create the Buyer-Flow segments + Channel-level AARE indicators
 * 1) Create the Buyer-Flow segments first,
 * 2) then create the AARE columns in a way that Buyer-Flow & AARE don't conflict
 ***********/
create multiset volatile table buyer_flow_all_views as (
select distinct acp_id
  ,week_idnt
  ,channel
  ,ntn_chan_flag
  ,ntn_jwn_flag
  ,act_flag
  ,ntc_flag
  ,chan_ret_flag
  ,jwn_ret_flag
  ,case when chan_ret_flag = 1                      then '3) Retained-to-Channel'
        when ntn_chan_flag = 1 and jwn_ret_flag = 0 then '1) New-to-JWN'
        when ntc_flag = 1 and ntn_chan_flag = 0     then '2) New-to-Channel (not JWN)'
                                                    else '4) Reactivated-to-Channel' end buyer_flow
  ,case when ntn_jwn_flag = 1
         and jwn_ret_flag = 0
         and substring(buyer_flow,1,1) not in ('3','4' )
        then 1 else 0 end AARE_acquired
  ,case when act_flag = 1
         and (ntn_jwn_flag = 1 or jwn_ret_flag = 1)
         and substring(buyer_flow,1,1) not in ('4')
        then 1 else 0 end AARE_activated
  ,case when jwn_ret_flag = 1
         and substring(buyer_flow,1,1) <> '1'
        then 1 else 0 end AARE_retained
  ,case when ntc_flag = 1
         and ntn_chan_flag = 0
         and chan_ret_flag = 0
         and substring(buyer_flow,1,1) = '2'
        then 1 else 0 end AARE_engaged
from aare_buyer_flow_prep
--where substring(channel,1,1) in ('1','2','3','4')
) with data primary index (acp_id, week_idnt, channel) on commit preserve rows;

collect statistics column (acp_id, week_idnt) on buyer_flow_all_views;
collect statistics column (channel) on buyer_flow_all_views;


/*********** PART III-B-ii) Create the Banner-level AARE indicators
 * Ideally, these are the "max" of the channel-level indicators...
 * but must be changed if they conflict with Buyer-Flow values
 ***********/
create multiset volatile table aare_banner_level as (
select distinct a.acp_id
    ,a.week_idnt
    ,a.channel
    ,b.buyer_flow
    ,a.ntn_chan_flag
    ,a.ntn_jwn_flag
    ,a.act_flag
    ,b.ntc_flag
    ,a.chan_ret_flag
    ,a.jwn_ret_flag
    ,case when ntn_jwn_flag = 1
           and jwn_ret_flag = 0
           and substring(buyer_flow,1,1) not in ('3','4')
          then 1 else 0 end AARE_acquired
    ,case when act_flag = 1
           and (ntn_jwn_flag = 1 or jwn_ret_flag = 1)
           and substring(buyer_flow,1,1) not in ('4')
          then 1 else 0 end AARE_activated
    ,case when jwn_ret_flag = 1 and substring(buyer_flow,1,1) <> '1'
          then 1 else 0 end AARE_retained
    ,a.AARE_engaged
from
(
  select acp_id
    ,week_idnt
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
    ,week_idnt
    ,channel
    ,buyer_flow
    ,ntc_flag
  from buyer_flow_all_views
  where substring(channel,1,1) in ('5','6')
) b
  on a.acp_id = b.acp_id
  and a.week_idnt = b.week_idnt
  and a.channel = b.channel
) with data primary index (acp_id, week_idnt, channel) on commit preserve rows;

collect statistics column (acp_id, week_idnt) on aare_banner_level;


/*********** PART III-B-iii) Create the JWN-level AARE indicators
 * Ideally, these are the "max" of the banner-level indicators...
 * but must be changed if they conflict with Buyer-Flow values
 ***********/
create multiset volatile table aare_jwn_level as (
select distinct a.acp_id
    ,a.week_idnt
    ,a.channel
    ,b.buyer_flow
    ,a.ntn_chan_flag
    ,a.ntn_jwn_flag
    ,a.act_flag
    ,a.chan_ret_flag
    ,a.jwn_ret_flag
    ,case when ntn_jwn_flag = 1
           and jwn_ret_flag = 0
           and substring(buyer_flow,1,1) not in ('3','4')
          then 1 else 0 end AARE_acquired
    ,case when act_flag = 1
           and (ntn_jwn_flag = 1 or jwn_ret_flag = 1)
           and substring(buyer_flow,1,1) not in ('4')
          then 1 else 0 end AARE_activated
    ,case when jwn_ret_flag = 1 /*and ntn_chan_flag = 0 and ntn_jwn_flag = 0*/
           and substring(buyer_flow,1,1) <> '1'
          then 1 else 0 end AARE_retained
    ,a.AARE_engaged
from
(
  select acp_id
    ,week_idnt
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
    ,week_idnt
    ,channel
    ,buyer_flow
    ,ntc_flag
  from buyer_flow_all_views
  where substring(channel,1,1) = '7'
) b
  on a.acp_id = b.acp_id
  and a.week_idnt = b.week_idnt
  and a.channel = b.channel
) with data primary index (acp_id, week_idnt, channel) on commit preserve rows;


/************************************************************************************
 * PART III-C) Insert Buyer-Flow & AARE values into final table
 ************************************************************************************/

delete from {cco_t2_schema}.cco_buyer_flow_cust_channel_week all;

/*********** PART III-C-i) insert Channel-level Buyer-Flow segments & AARE flags ****/
insert into {cco_t2_schema}.cco_buyer_flow_cust_channel_week
select distinct acp_id
  ,week_idnt
  ,channel
  ,buyer_flow
  ,AARE_acquired
  ,AARE_activated
  ,AARE_retained
  ,AARE_engaged
  ,current_timestamp(6) as dw_sys_load_tmstp
from buyer_flow_all_views
where substring(channel,1,1) in ('1','2','3','4')

/*********** PART III-C-ii) insert Banner-level Buyer-Flow segments & AARE flags ****/
union all
select distinct acp_id
  ,week_idnt
  ,channel
  ,buyer_flow
  ,AARE_acquired
  ,AARE_activated
  ,AARE_retained
  ,AARE_engaged
  ,current_timestamp(6) as dw_sys_load_tmstp
from aare_banner_level

/*********** PART III-C-iii) insert JWN-level Buyer-Flow segments & AARE flags ******/
union all
select distinct acp_id
  ,week_idnt
  ,channel
  ,buyer_flow
  ,AARE_acquired
  ,AARE_activated
  ,AARE_retained
  ,AARE_engaged
  ,current_timestamp(6) as dw_sys_load_tmstp
from aare_jwn_level;


SET QUERY_BAND = NONE FOR SESSION;
