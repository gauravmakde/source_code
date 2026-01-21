SET QUERY_BAND = 'App_ID=APP08240;
     DAG_ID=cco_cust_chan_yr_attributes_11521_ACE_ENG;
     Task_Name=cco_line_items;'
     FOR SESSION VOLATILE;





/************************************************************************************/
/************************************************************************************/
/************************************************************************************
 *
 * Build a line-item-level table facilitate analytics for the Aug/Sep22 "CCO Deep-dive"
 * *
 * The Steps involved are:
 * I) Create (empty) table
 *
 * II) Set the date parameters
 *
 * III) Gather the necessary transaction data
 *    a) build lookup tables to feed the transaction query
 *    b) bring all tables together in 1 place
 *
 * IV) Gather the necessary NSEAM data (for Alterations not captured at the cash register)
 *    a) build necessary lookup tables
 *    b) insert data into line-item-level table
 ************************************************************************************/
/************************************************************************************/
/************************************************************************************/



/************************************************************************************/
/************************************************************************************
 * PART II) Set the date parameters
 ************************************************************************************/
/************************************************************************************/


/***************************************************************************************/
/********* Step II-1: create realigned fiscal calendar (going back far enough to include 4+ years of Cohorts) *********/
/***************************************************************************************/

/********* Step II-1-a: Going back 8 years from today, find all years with a "53rd week" *********/
--drop table week_53_yrs;
create MULTISET volatile table week_53_yrs as (
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

/********* Step II-1-b: Count the # years with a "53rd week" *********/
--drop table week_53_yr_count;
create MULTISET volatile table week_53_yr_count as (
select count(distinct year_num) year_count
from week_53_yrs x 
) with data primary index(year_count) on commit preserve rows;

/********* Step II-1-c: Create an empty realigned calendar *********/
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

/********* Step II-1-d: insert data into realigned calendar 
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

/***************************************************************************************/
/********* Step II-2: Set date parameters *********/
/***************************************************************************************/

/********* Step II-2-a: Find the start & end month of the last 4 years (through latest complete fiscal month) *********/
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
from realigned_calendar 
where day_date=date'{lines_end_date}'+1  --"2023-04-29"
) with data primary index(current_mo) on commit preserve rows;

/********* Step II-2-b: Find the start & end dates of the last 4 years (through latest complete fiscal month) *********/
create multiset volatile table date_lookup as (
select 
     date'{lines_start_date}' yr1_start_dt  --"2019-02-03"
  ,max(case when month_num = (select yr1_end_mo   from month_lookup) then day_date else null end) yr1_end_dt
  ,min(case when month_num = (select yr2_start_mo from month_lookup) then day_date else null end) yr2_start_dt
  ,max(case when month_num = (select yr2_end_mo   from month_lookup) then day_date else null end) yr2_end_dt
  ,min(case when month_num = (select yr3_start_mo from month_lookup) then day_date else null end) yr3_start_dt
  ,max(case when month_num = (select yr3_end_mo   from month_lookup) then day_date else null end) yr3_end_dt
  ,min(case when month_num = (select yr4_start_mo from month_lookup) then day_date else null end) yr4_start_dt
  ,max(case when month_num = (select yr4_end_mo   from month_lookup) then day_date else null end) yr4_end_dt
from realigned_calendar
group by 1
) with data primary index(yr1_start_dt) on commit preserve rows;

/********* Step II-2-c: Generate names for the past 4 rolling-12-mo periods *********/
create multiset volatile table month_names as (
select 
   max(case when month_num=(select yr1_end_mo   from month_lookup) 
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
  (select yr1_end_mo   from month_lookup)
  ,(select yr2_end_mo   from month_lookup)
  ,(select yr3_end_mo   from month_lookup)
  ,(select yr4_end_mo   from month_lookup)
 )
) with data primary index(yr1_name) on commit preserve rows;

 
-- sel * from month_lookup;
-- sel * from date_lookup;
-- sel * from month_names;


/************************************************************************************/
/************************************************************************************
 * PART III) Gather the necessary transaction data & insert into final table
 *    a) build lookup tables to feed the transaction query
 *    b) bring all tables together in 1 place
 ************************************************************************************/
/************************************************************************************/


/************************************************************************************
 * PART III-a) create lookup table of "MERCH"-related fields unique on UPC/country
 ************************************************************************************/
--drop table upc_lookup;
create multiset volatile table upc_lookup as
(select distinct a.upc_num
                ,b.div_num
                ,b.div_desc
                ,b.grp_num as subdiv_num
                ,b.grp_desc as subdiv_desc
                ,b.dept_num
                ,b.dept_desc
                ,b.brand_name
                ,b.channel_country
                ,b.rms_sku_num
                ,coalesce (v.npg_flag,'N') as npg_flag 
                ,case when b.dept_num in (636,765) and trim(b.class_desc)='GIFT WRAPPING' then 1 else 0 end gift_wrap_service
                ,case when b.dept_num = 102
                            or (b.dept_num = 497 and b.class_num in (12,14) )
                            or (b.dept_num = 497 and (b.class_num in (10,11) or b.style_desc like '%NAIL%'))
                           then 1
                      else 0 end as beauty_service
from  prd_nap_usr_vws.product_upc_dim as a
  join 
  (
  SEL
  b.div_num
                ,b.div_desc
                ,b.grp_num 
                ,b.grp_desc
                ,b.dept_num
                ,b.dept_desc
                ,b.brand_name
                ,b.channel_country
                ,b.rms_sku_num
                ,b.class_desc
                ,class_num 
                ,b.style_desc
                ,B.prmy_supp_num
                FROM 
  prd_nap_usr_vws.product_sku_dim_vw B
  where b.div_num in (310,340,345,351,360,365,700,800,900)
  AND B.prmy_supp_num IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
UNION ALL
  SEL
  b.div_num
                ,b.div_desc
                ,b.grp_num 
                ,b.grp_desc
                ,b.dept_num
                ,b.dept_desc
                ,b.brand_name
                ,b.channel_country
                ,b.rms_sku_num
                ,b.class_desc
                ,class_num 
                ,b.style_desc
                ,B.prmy_supp_num
                FROM 
  prd_nap_usr_vws.product_sku_dim_vw B
  where b.div_num in (310,340,345,351,360,365,700,800,900)
  AND B.prmy_supp_num IS NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
  ) as b 
  on a.rms_sku_num = b.rms_sku_num 
  and a.channel_country=b.channel_country
  left join prd_nap_usr_vws.vendor_dim as v 
  on b.prmy_supp_num = v.vendor_num
)with data primary index(upc_num, channel_country) on commit preserve rows;

collect statistics
column (upc_num, channel_country)
on
upc_lookup;


/************************************************************************************
 * PART III-b) create lookup table of "ORDER FULFILLMENT"-related fields
 *      (order pickup & delivery method), unique on global_tran_id/line_item_seq_num
 ************************************************************************************/

 /*first create extract for efficiency*/
--drop table item_delivery_extract; 
create MULTISET volatile table  item_delivery_extract  ,--NO FALLBACK ,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
(
    business_day_date date
    ,global_tran_id bigint
    ,line_item_seq_num smallint
    ,intent_store_num integer
    ,item_delivery_method varchar(40)
    ,curbside_ind byteint
    ,picked_up_by_customer_node_num integer
    ,picked_up_by_customer_tmstp_pacific timestamp(6)
) NO PRIMARY INDEX on commit preserve rows;

insert into item_delivery_extract
select distinct a.business_day_date 
    ,a.global_tran_id
    ,a.line_item_seq_num
    ,a.intent_store_num
    ,a.item_delivery_method
    ,a.curbside_ind
    ,a.picked_up_by_customer_node_num  
    ,a.picked_up_by_customer_tmstp_pacific
FROM T2DL_DAS_ITEM_DELIVERY.item_delivery_method_funnel_daily a
where business_day_date between (select yr1_start_dt from date_lookup) and (select yr4_end_dt+14 from date_lookup)
    and a.order_date_pacific between (select yr1_start_dt from date_lookup) and (select yr4_end_dt from date_lookup)
    and a.item_delivery_method <> 'SHIP_TO_HOME'
    and a.canceled_date_pacific is null;

 
 --drop table order_fulfillment_activity;
create multiset volatile table order_fulfillment_activity as
(
select distinct a.business_day_date 
    ,a.global_tran_id
    ,a.line_item_seq_num
    ,a.item_delivery_method
    ,case when substring(trim(a.item_delivery_method),1,9) in ('FREE_2DAY','PAID_EXPE')
            or  trim(a.item_delivery_method)='SAME_DAY_DELIVERY' then 1
          else 0 end svc_group_exp_delivery
    ,case when a.item_delivery_method like '%BOPUS' or a.item_delivery_method like '%SHIP_TO_STORE' then 1
          else 0 end svc_group_order_pickup
    ,a.curbside_ind

    ,a.picked_up_by_customer_node_num pickup_store
    ,cast(a.picked_up_by_customer_tmstp_pacific as date) as pickup_date
    ,case when st.business_unit_desc in ('FULL LINE','FULL LINE CANADA') then '1) Nordstrom Stores'
          when st.business_unit_desc in ('N.CA','N.COM','TRUNK CLUB') then '2) Nordstrom.com'
          when st.business_unit_desc in ('RACK', 'RACK CANADA') then '3) Rack Stores'
          when st.business_unit_desc in ('OFFPRICE ONLINE') then '4) Rack.com'
          else null
      end as pickup_channel
    ,case when st.business_unit_desc in ('FULL LINE','FULL LINE CANADA','N.CA','N.COM','TRUNK CLUB')
                then 'NORDSTROM'
          when st.business_unit_desc in ('RACK', 'RACK CANADA','OFFPRICE ONLINE') then 'RACK'
          else null
      end as pickup_banner
    ,case when (st.business_unit_desc like '%CANADA' or st.business_unit_desc like '%.CA') then 'CA' else 'US' end pickup_country
    ,st.subgroup_desc as pickup_region
from item_delivery_extract a
    left join prd_nap_usr_vws.store_dim st on coalesce(a.picked_up_by_customer_node_num,-1*a.intent_store_num)=st.store_num
) with data primary index(business_day_date, global_tran_id, line_item_seq_num)
partition by range_n(business_day_date between date '2017-01-01' and date '2025-12-31' each interval '1' day,
unknown)
on commit preserve rows;

collect statistics
column (partition),
column (business_day_date, global_tran_id, line_item_seq_num)
on
order_fulfillment_activity;

/************************************************************************************
 * PART III-c) create lookup table of "RESTAURANT"-related fields (for Service reporting),
 *      unique on global_tran_id/line_item_seq_num
 ************************************************************************************/
 
/********* PART III-c-i) create lookup table of all Restaurant depts *********/
--drop table restaurant_service_lookup;
CREATE multiset volatile TABLE restaurant_service_lookup AS (
select distinct cast(dept_num as varchar(8)) dept_num
  ,dept_name dept_desc
  ,subdivision_num subdiv_num
  ,subdivision_name subdiv_desc
  ,division_num div_num
  ,division_name div_desc
  ,case when dept_num in (113,188,571,698) then 'G02) Coffee'
        when dept_num in (568,692,715)     then 'G03) Bar'
        else 'G01) Food' end restaurant_service
from prd_nap_usr_vws.DEPARTMENT_DIM
where division_num=70
) with data primary index(dept_num) on commit preserve rows;
--COLLECT STATISTICS COLUMN (DEPT_NUM) ON restaurant_service_lookup;

/********* PART III-c-ii) create lookup table of all Restaurant transaction line-items *********/
--drop table restaurant_tran_lines;
create MULTISET volatile table   restaurant_extract  ,--NO FALLBACK ,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
( business_day_date  date
  ,global_tran_id bigint
  ,line_item_seq_num smallint
  ,acp_id varchar(50) compress
  ,intent_store_num integer
  ,merch_dept_num varchar(8)
) NO PRIMARY INDEX on commit preserve rows;

insert into  restaurant_extract 
select distinct business_day_date
  ,global_tran_id
  ,line_item_seq_num
  ,acp_id
  ,intent_store_num
  ,merch_dept_num
FROM prd_nap_usr_vws.retail_tran_detail_fact_vw  dtl
where business_day_date between (select yr1_start_dt from date_lookup) and (select yr4_end_dt+14 from date_lookup)
 and coalesce(dtl.order_date,dtl.tran_date) between (select yr1_start_dt from date_lookup) and (select yr4_end_dt from date_lookup)
     and dtl.acp_id is not null
     and dtl.line_net_amt >0
     and dtl.data_source_code = 'rpos';
     
 
CREATE multiset volatile TABLE restaurant_tran_lines AS (
select distinct dtl.business_day_date 
  ,dtl.global_tran_id
  ,dtl.line_item_seq_num
  ,r.dept_num
  ,r.dept_desc
  ,r.subdiv_num
  ,r.subdiv_desc
  ,r.div_num
  ,r.div_desc
  ,r.restaurant_service
FROM restaurant_extract  dtl
  join prd_nap_usr_vws.store_dim st 
     on  dtl.intent_store_num   = st.store_num
     and st.business_unit_desc in ('FULL LINE','FULL LINE CANADA')
  join restaurant_service_lookup r 
  on coalesce(dtl.merch_dept_num,-1*dtl.intent_store_num)=r.dept_num
  )with data  primary index(global_tran_id,line_item_seq_num) 
  partition by range_n(business_day_date between date '2017-01-01' and date '2025-12-31' each interval '1' day,
unknown)
on commit preserve rows;

collect statistics
column (partition),
column (global_tran_id, line_item_seq_num)
on
restaurant_tran_lines;

--sel top 1000 * from restaurant_tran_lines


/************************************************************************************
 * PART III-d) create lookup table of "REMOTE-SELLING"-related fields (for Service reporting),
 *      unique on global_tran_id/UPC
 ************************************************************************************/

create volatile table boards as (
select distinct 
    business_day_date
   ,remote_sell_swimlane
  ,cast(lpad(trim(upc_num),15,'0')as varchar(32)) as upc_num
  ,cast(global_tran_id as bigint) global_tran_id
from T2DL_DAS_REMOTE_SELLING.remote_sell_transactions
where remote_sell_swimlane in ('STYLELINK_ATTRIBUTED', 'STYLEBOARD_ATTRIBUTED', 'PRIVATE_STYLING_ATTRIBUTED', 'TRUNK_CLUB')
  and upc_num is not null
  and global_tran_id is not null
  and business_day_date between (select yr1_start_dt from date_lookup) and (select yr4_end_dt+14 from date_lookup) 
)with data   primary index(upc_num, global_tran_id) 
partition by range_n(business_day_date between date '2017-01-01' and date '2025-12-31' each interval '1' day,
unknown)
on commit preserve rows;

collect statistics
column (partition),
column (upc_num, global_tran_id)
on
boards;
 
/************************************************************************************
 * PART III-e) create lookup table of "TENDER"-related indicators, unique on global_tran_id
 ************************************************************************************/

create multiset volatile table transaction_tenders as (
select business_day_date
  ,global_tran_id
  ,max(case when trim(card_type_code) in ('NC','NV') then 1 else 0 end) tender_nordstrom
  ,max(case when trim(tender_type_code) = 'NORDSTROM_NOTE' then 1 else 0 end) tender_nordstrom_note
  ,max(case when trim(tender_type_code) = 'CREDIT_CARD' then 1 else 0 end) tender_3rd_party_credit
  ,max(case when trim(tender_type_code) = 'DEBIT_CARD' then 1 else 0 end) tender_debit_card
  ,max(case when trim(tender_type_code) = 'GIFT_CARD' then 1 else 0 end) tender_gift_card
  ,max(case when trim(tender_type_code) = 'CASH' then 1 else 0 end) tender_cash
  ,max(case when trim(tender_type_code) = 'PAYPAL' then 1 else 0 end) tender_paypal
  ,max(case when trim(tender_type_code) = 'CHECK' then 1 else 0 end) tender_check
from prd_nap_usr_vws.RETAIL_TRAN_TENDER_FACT a
where business_day_date between (select yr1_start_dt-14 from date_lookup) and (select yr4_end_dt+14 from date_lookup) 
  and tender_item_usd_amt>0
  and tender_type_code is not null
group by 1,2
) with data primary index(global_tran_id) 
partition by range_n(business_day_date between date '2017-01-01' and date '2025-12-31' each interval '1' day,
unknown)
on commit preserve rows;

collect statistics
column (partition),
column (global_tran_id)
on
transaction_tenders;


/************************************************************************************
 * PART III-f) create lookup table of "EVENT DATES", unique on day
 ************************************************************************************/

--drop table event_dates;
create multiset volatile table event_dates as (
select a.day_date
  --,case when e.event_dt is not null then 1 else 0 end clear_the_rack_dates
  --Anniversary dates; assumes years run Sep thru Aug, so cuts-off any dates spilling into Sep (the following period)
  ,case when a.day_date between date'2018-07-11' and '2018-08-05' then 1
        when a.day_date between date'2019-07-09' and '2019-08-04' then 1
        when a.day_date between date'2020-08-04' and '2020-08-29' then 1  --2020 sale in Aug, spilled into Sep (so cut-off Sep dates)
        when a.day_date between date'2021-07-12' and '2021-08-08' then 1
        when a.day_date between date'2022-07-06' and '2022-07-31' then 1
        when a.day_date between date'2023-07-11' and '2023-08-06' then 1
        else 0 end anniv_dates_us
  ,case when a.day_date between date'2018-07-11' and '2018-08-05' then 1
        when a.day_date between date'2019-07-16' and '2019-08-04' then 1
        when a.day_date between date'2020-08-17' and '2020-08-29' then 1  --2020 sale in Aug, spilled into Sep (so cut-off Sep dates)
        when a.day_date between date'2021-07-25' and '2021-08-08' then 1
        when a.day_date between date'2022-07-14' and '2022-07-31' then 1
        else 0 end anniv_dates_ca
  ,case when a.month_454_num between 9 and 11 then 1 else 0 end holiday_dates
  ,case when a.month_num between (select yr1_start_mo from month_lookup) and (select yr1_end_mo from month_lookup) 
          then (select yr1_name from month_names)
        when a.month_num between (select yr2_start_mo from month_lookup) and (select yr2_end_mo from month_lookup) 
          then (select yr2_name from month_names)
        when a.month_num between (select yr3_start_mo from month_lookup) and (select yr3_end_mo from month_lookup) 
          then (select yr3_name from month_names)
        when a.month_num between (select yr4_start_mo from month_lookup) and (select yr4_end_mo from month_lookup) 
          then (select yr4_name from month_names)
        else null end reporting_year
  ,cast('FY-'||cast((year_num mod 2000) as varchar(2)) as varchar(8)) fiscal_year
from realigned_calendar a
  -- left join
  --   (select distinct event_dt
  --    from T2DL_DAS_STRATEGY.cco_liveramp_events
  --    where event_dt between  start_date  and  end_date 
  --      and clear_the_rack = 1) e
  --   on a.day_date=e.event_dt
where a.day_date between (select yr1_start_dt from date_lookup) and (select yr4_end_dt from date_lookup) 
) with data primary index(day_date) on commit preserve rows;

-- select reporting_year
--     ,min(day_date) min_date
--     ,max(day_date) max_date
-- from event_dates
-- group by 1
-- order by 1
-- ;

collect statistics
column (day_date)
on
event_dates;


/************************************************************************************
 * PART III-h) Insert Transaction data into final table
 ************************************************************************************/


/********* PART III-h-ii) "Monster query" to insert everything into final table *********/

/* for efficiency, extract just the sales_and_returns_fact data needed for this process*/
 --drop table sarf_extract
create multiset volatile table SARF_EXTRACT as
(select  
business_day_date,
global_tran_id,
line_item_seq_num,
order_num,
acp_id,
business_unit_desc,
case when  business_unit_desc in ('FULL LINE','FULL LINE CANADA','N.CA','N.COM','TRUNK CLUB')
            then 'NORDSTROM'
      when  business_unit_desc in ('RACK', 'RACK CANADA','OFFPRICE ONLINE') then 'RACK'
        else null end as banner,
 case when line_item_order_type = 'CustInitWebOrder' and line_item_fulfillment_type = 'StorePickUp' 
        then '2) Nordstrom.com'
      when  business_unit_desc in ('FULL LINE','FULL LINE CANADA') then '1) Nordstrom Stores'
      when  business_unit_desc in ('N.CA','N.COM','TRUNK CLUB') then '2) Nordstrom.com'
      when  business_unit_desc in ('RACK', 'RACK CANADA') then '3) Rack Stores'
      when  business_unit_desc in ('OFFPRICE ONLINE') then '4) Rack.com'
       else null end as channel,
case  
when line_item_order_type = 'CustInitWebOrder' and line_item_fulfillment_type = 'StorePickUp'
and (business_unit_desc like '%CANADA' or business_unit_desc like '%.CA') then 867
when line_item_order_type = 'CustInitWebOrder' and line_item_fulfillment_type = 'StorePickUp' then 808
else intent_store_num end as intent_store_num,
case when (business_unit_desc like '%CANADA' or business_unit_desc like '%.CA') 
    then 'CA' else 'US' end channel_country,
case when (business_unit_desc like '%CANADA' or business_unit_desc like '%.CA') 
then 'CA' else 'US' end store_country,
price_type,
payroll_dept,
hr_es_ind,
order_date,
coalesce( order_date, tran_date) as tran_date,
nonmerch_fee_code,
coalesce(shipped_usd_sales,0) shipped_usd_sales,
coalesce(return_usd_amt,0) return_usd_amt,
coalesce(shipped_qty,0) shipped_qty,
coalesce(return_qty,0) return_qty,
return_date,
days_to_return,
return_ringing_store_num,
source_platform_code,
employee_discount_usd_amt,
upc_num
from T2DL_DAS_SALES_RETURNS.SALES_AND_RETURNS_FACT 
 where 
    business_day_date between (select yr1_start_dt from date_lookup) and (select yr4_end_dt+14 from date_lookup) 
    and  coalesce( order_date, tran_date) between (select yr1_start_dt from date_lookup) and (select yr4_end_dt from date_lookup) 
    and  acp_id is not null
    and  business_unit_desc in ('FULL LINE','FULL LINE CANADA','N.CA','N.COM','TRUNK CLUB','RACK','RACK CANADA','OFFPRICE ONLINE')	
	and  shipped_usd_sales not < 0
    and shipped_usd_sales is not null
    and coalesce( return_usd_amt,0) not < 0 	
)with data primary index(business_day_date, global_tran_id, line_item_seq_num) 
partition by range_n(business_day_date between date '2017-01-01' and date '2025-12-31' each interval '1' day,
unknown)
on commit preserve rows;

collect statistics
column (partition),
column (business_day_date, global_tran_id, line_item_seq_num)
on
sarf_extract;

create multiset volatile table NTN_EXTRACT as  
(select  
aare_global_tran_id,
acp_id,
aare_chnl_code
from prd_nap_usr_vws.CUSTOMER_NTN_STATUS_FACT
) with data primary index(aare_global_tran_id) 
on commit preserve rows;

collect statistics
column (aare_global_tran_id)
on
NTN_EXTRACT;


/* for efficiency, process data derivations into a NOPI
  staging table, so the final load into prod table can be
  a simple copy*/


create multiset volatile table cco_line_items_stg,
no fallback, no before journal, no after  journal,
checksum =default
(
    global_tran_id BIGINT
    ,line_item_seq_num SMALLINT
    ,data_source varchar(20) COMPRESS ('1) TRANSACTION','2) NSEAM')
    ,order_num VARCHAR(32) compress
    ,acp_id VARCHAR(50) compress
    ,banner VARCHAR(9) compress ('NORDSTROM', 'RACK')
    ,channel VARCHAR(19) compress ( '1) Nordstrom Stores', '2) Nordstrom.com','3) Rack Stores', '4) Rack.com')
    ,channel_country VARCHAR(2) compress ('US', 'CA')
    ,store_num INTEGER compress
    ,store_region VARCHAR(30) compress ('FULL LINE CANADA','MIDWEST FLS','MIDWEST RACKS','N.CA','N.COM', 'NORTHEAST FLS','NORTHEAST RACKS','NORTHWEST FLS', 'NORTHWEST RACKS','OFFPRICE ONLINE','RACK CANADA','SCAL RACKS','SOUTHEAST FLS','SOUTHEAST RACKS','SOUTHERN CALIFORNIA FLS','SOUTHWEST FLS','SOUTHWEST RACKS')
    ,div_num INTEGER compress
    ,div_desc VARCHAR(30) compress ('ACCESSORIES','APPAREL','BEAUTY','DESIGNER','HOME','LEASED BOUTIQUES','MERCH PROJECTS','OTHER/NON-MERCH','RESTAURANT','SHOES')
    ,subdiv_num INTEGER compress
    ,subdiv_desc VARCHAR(30) compress ('ACCESSORIES','BAG FEES','BEAUTY SERVICES','CREATIVE PROJECTS','DEPOSITS/SERVICES','FRAGRANCE','HOME','KIDS APPAREL','KIDS DESIGNER','KIDS SHOES','LEASED BOUTIQUES','MAKEUP&SKINCARE','MENS APPAREL','MENS DESIGNER','MENS SHOES','MENS SPECIALIZED','MERCH PROJECTS','NEW CONCEPTS','NII NON INVENTORY ITEMS','RESTAURANT','WMNS DESIGNER','WOMENS APPAREL','WOMENS SHOES','WOMENS SPECIALIZED')
    ,dept_num INTEGER compress
    ,dept_desc VARCHAR(50) compress
     ('WMNS BETTER LC','YOUNG WOMENS SHOES','SPECIALTY COFFEE BAR','LINGERIE','WOMENS BETTER SHOES','DRESSES','YOUNG WOMENS APPAREL',
      'WMNS ACTIVEWEAR','MAKEUP','ADVANCED TECHNOLOGY/TOOLS','WMNS BETTER MC','JEWELRY','CAFE','MENS BETTER SPORTSWEAR','WMNS BETTER EC',
      'HDBG/SLG/LUG','CONTEMPORARY 1','WOMENS ACTIVE SHOES','ACCESSORIES','SKINCARE','WMNS FRAGRANCE','MENS ACTIVEWEAR','MENS DRESSWEAR',
      'YOUNG MENS APPAREL','BIG GIRL','BEAUTY GIFT','SLEEPWEAR','WMNS SWIM WEAR','NON ACTIVE KIDS SHOES','MENS SPECIALIZED',
      'DESIGNER','WMNS DENIM','ACTIVE KIDS SHOES','MENS BETTER SHOES','EYEWEAR','HOSIERY','MENS ACTIVE SHOES','MENS DENIM','BIG BOY' )
    ,brand_name VARCHAR(50) compress
    ,npg_flag VARCHAR(1) compress ('Y','N')
    ,price_type VARCHAR(1) compress ('R','C','P')
    ,store_country VARCHAR(2) compress ('US','CA')
    ,ntn_tran INTEGER compress
    ,date_shopped DATE compress
    ,fiscal_month_num INTEGER compress
    ,fiscal_qtr_num INTEGER compress
    ,fiscal_yr_num INTEGER compress
    --,reporting_year_shopped VARCHAR(10) compress  ('YE-Aug19','YE-Aug20','YE-Aug21','YE-Aug22')
    ,fiscal_year_shopped VARCHAR(10) compress 
    ,reporting_year_shopped VARCHAR(10) compress  --don't list unique values because they'll change with each run
    ,item_price_band VARCHAR(11) compress
    ,gross_sales DECIMAL(12,2) compress
    ,gross_incl_gc DECIMAL(12,2) compress
    ,return_amt DECIMAL(12,2) compress 0.00
    ,net_sales DECIMAL(13,2) compress 0.00
    ,gross_items INTEGER compress
    ,return_items INTEGER compress
    ,net_items INTEGER compress
    ,return_date DATE compress
    ,days_to_return INTEGER compress
    ,return_store INTEGER compress
    ,return_banner VARCHAR(9) compress ('NORDSTROM', 'RACK')
    ,return_channel VARCHAR(19) compress ( '1) Nordstrom Stores', '2) Nordstrom.com','3) Rack Stores', '4) Rack.com')
    ,item_delivery_method VARCHAR(40) compress ('FC_BOPUS','FREE_2DAY_DELIVERY','FREE_2DAY_SHIP_TO_STORE','NEXT_DAY_SHIP_TO_STORE','PAID_EXPEDITED_SHIP_TO_HOME','PAID_EXPEDITED_SHIP_TO_STORE','SAME_DAY_BOPUS','SAME_DAY_DELIVERY','SHIP_TO_STORE')
    ,pickup_store INTEGER compress
    ,pickup_date DATE compress
    ,pickup_channel VARCHAR(19) COMPRESS ( '1) Nordstrom Stores', '2) Nordstrom.com','3) Rack Stores', '4) Rack.com')
    ,pickup_banner VARCHAR(9) compress ('NORDSTROM', 'RACK')
    ,platform VARCHAR(40) compress ('WEB','IOS','MOW','POS','ANDROID','CSR_PHONE','THIRD_PARTY_VENDOR')
    ,employee_flag INTEGER compress
    ,tender_nordstrom INTEGER compress   (0,1)
    ,tender_nordstrom_note INTEGER compress  (0,1)
    ,tender_3rd_party_credit INTEGER compress (0,1)
    ,tender_debit_card INTEGER compress (0,1)
    ,tender_gift_card INTEGER compress (0,1)
    ,tender_cash INTEGER compress (0,1)
    ,tender_paypal INTEGER compress (0,1)
    ,tender_check INTEGER compress (0,1)
    --,event_ctr INTEGER  compress (0,1)
    ,event_holiday INTEGER compress (0,1)
    ,event_anniversary INTEGER  compress (0,1)
    ,svc_group_exp_delivery INTEGER compress (0,1)
    ,svc_group_order_pickup INTEGER  compress (0,1)
    ,svc_group_selling_relation INTEGER compress (0,1)
    ,svc_group_remote_selling INTEGER compress (0,1)
    ,svc_group_alterations INTEGER compress (0,1)
    ,svc_group_in_store INTEGER compress (0,1)
    ,svc_group_restaurant INTEGER compress (0,1)
    ,service_free_exp_delivery INTEGER compress (0,1)
    ,service_next_day_pickup INTEGER compress (0,1)
    ,service_same_day_bopus INTEGER compress (0,1)
    ,service_curbside_pickup INTEGER compress (0,1)
    ,service_style_boards INTEGER compress (0,1)
    ,service_gift_wrapping INTEGER compress (0,1)
    ,service_pop_in INTEGER compress (0,1)
)
NO PRIMARY INDEX
 on commit preserve rows;


COLLECT STATISTICS COLUMN (MONTH_NUM ,QUARTER_NUM ,YEAR_NUM) ON realigned_calendar; 
COLLECT STATISTICS COLUMN (ANNIV_DATES_CA ,HOLIDAY_DATES) ON event_dates; 
COLLECT STATISTICS COLUMN (RETURN_RINGING_STORE_NUM) ON sarf_extract; 
COLLECT STATISTICS COLUMN (CHANNEL_COUNTRY ,UPC_NUM) ON sarf_extract; 
COLLECT STATISTICS COLUMN (GLOBAL_TRAN_ID ,LINE_ITEM_SEQ_NUM) ON sarf_extract; 
COLLECT STATISTICS COLUMN (GLOBAL_TRAN_ID ,LINE_ITEM_SEQ_NUM) ON order_fulfillment_activity; 
COLLECT STATISTICS COLUMN (BUSINESS_DAY_DATE ,GLOBAL_TRAN_ID) ON sarf_extract; 
COLLECT STATISTICS COLUMN (BUSINESS_DAY_DATE ,UPC_NUM ,GLOBAL_TRAN_ID) ON boards; 
COLLECT STATISTICS COLUMN (TRAN_DATE) ON sarf_extract; 
COLLECT STATISTICS COLUMN (DAY_DATE) ON realigned_calendar; 
COLLECT STATISTICS COLUMN (BUSINESS_DAY_DATE) ON order_fulfillment_activity; 
COLLECT STATISTICS COLUMN (GLOBAL_TRAN_ID) ON restaurant_tran_lines; 
COLLECT STATISTICS COLUMN (GLOBAL_TRAN_ID) ON boards; 
COLLECT STATISTICS COLUMN (GLOBAL_TRAN_ID) ON order_fulfillment_activity; 
COLLECT STATISTICS COLUMN (PARTITION) ON order_fulfillment_activity; 
COLLECT STATISTICS COLUMN (PARTITION) ON boards; 
COLLECT STATISTICS COLUMN (PARTITION) ON restaurant_tran_lines; 
COLLECT STATISTICS COLUMN (PARTITION) ON transaction_tenders; 
COLLECT STATISTICS COLUMN (PARTITION) ON sarf_extract;

insert into cco_line_items_stg
select distinct 
     dtl.global_tran_id
    ,dtl.line_item_seq_num
    ,'1) TRANSACTION' data_source
    ,dtl.order_num
    ,dtl.acp_id
    ,dtl.banner
    ,dtl.channel
    ,dtl.channel_country
    ,dtl.intent_store_num  as store_num
    ,st.subgroup_desc as store_region
    ,coalesce(upc.div_num,r.div_num) div_num
    ,coalesce(upc.div_desc,r.div_desc ) div_desc
    ,coalesce(upc.subdiv_num,r.subdiv_num ) subdiv_num
    ,coalesce(upc.subdiv_desc,r.subdiv_desc ) subdiv_desc
    ,coalesce(upc.dept_num,r.dept_num ) dept_num
    ,coalesce(upc.dept_desc ,r.dept_desc ) dept_desc
    ,upc.brand_name
    ,upc.npg_flag
    ,coalesce(dtl.price_type,'R') as price_type --Reg/Promo/Clear
    ,store_country
    ,case when substring(dtl.channel,1,1) ='1' and trim(nts.aare_chnl_code) = 'FLS' then 1
          when substring(dtl.channel,1,1) ='2' and trim(nts.aare_chnl_code) = 'NCOM' then 1
          when substring(dtl.channel,1,1) ='3' and trim(nts.aare_chnl_code) = 'RACK' then 1
          when substring(dtl.channel,1,1) ='4' and trim(nts.aare_chnl_code) = 'NRHL' then 1
          else 0 end as ntn_tran
    ,tran_date  as date_shopped
    ,cal.month_num fiscal_month_num
    ,cal.quarter_num fiscal_qtr_num
    ,cal.year_num fiscal_yr_num
    ,cast('FY-'||cast((cal.year_num mod 2000) as varchar(2)) as varchar(8)) fiscal_year_shopped
    ,case when cal.month_num between (select yr1_start_mo from month_lookup) and (select yr1_end_mo from month_lookup) 
            then (select yr1_name from month_names)
          when cal.month_num between (select yr2_start_mo from month_lookup) and (select yr2_end_mo from month_lookup) 
            then (select yr2_name from month_names)
          when cal.month_num between (select yr3_start_mo from month_lookup) and (select yr3_end_mo from month_lookup) 
            then (select yr3_name from month_names)
          when cal.month_num between (select yr4_start_mo from month_lookup) and (select yr4_end_mo from month_lookup) 
            then (select yr4_name from month_names)
          else null end reporting_year_shopped
    ,case when coalesce(dtl.nonmerch_fee_code,'-999') <> '6666' then
	 case
                when dtl.shipped_usd_sales = 0                                   then '0) $0'          
                when dtl.shipped_usd_sales > 0   and dtl.shipped_usd_sales <= 25 then '1) $0-25'         
                when dtl.shipped_usd_sales > 25  and dtl.shipped_usd_sales <= 50 then '2) $25-50'         
                when dtl.shipped_usd_sales > 50  and dtl.shipped_usd_sales <= 75 then '3) $50-75'          
                when dtl.shipped_usd_sales > 75  and dtl.shipped_usd_sales <= 100 then '4) $75-100'          
                when dtl.shipped_usd_sales > 100  and dtl.shipped_usd_sales <= 150 then '5) $100-150'          
                when dtl.shipped_usd_sales > 150  and dtl.shipped_usd_sales <= 200 then '6) $150-200'          
                when dtl.shipped_usd_sales > 200                                   then '7) $200+'
          else null end end as item_price_band    
    -- ,case when coalesce(dtl.nonmerch_fee_code,'-999') <> '6666' then  dtl.shipped_usd_sales else 0 end gross_sales
    -- ,case when coalesce(dtl.nonmerch_fee_code,'-999') <> '6666'  then dtl.return_usd_amt else 0 end as return_amt
    -- ,case when coalesce(dtl.nonmerch_fee_code,'-999') <> '6666'
    --            then  dtl.shipped_usd_sales -  dtl.return_usd_amt 
    --       else 0 end net_sales
    -- ,case when coalesce(dtl.nonmerch_fee_code,'-999') <> '6666' then  dtl.shipped_qty  else 0 end gross_items
    -- ,case when coalesce(dtl.nonmerch_fee_code,'-999') <> '6666' then  dtl.return_qty  else 0 end return_items
    -- ,case when coalesce(dtl.nonmerch_fee_code,'-999') <> '6666'
    --            then  dtl.shipped_qty  -  dtl.return_qty 
    --       else 0 end net_items
    ,case when coalesce(dtl.nonmerch_fee_code,'-999') <> '6666' then coalesce(dtl.shipped_usd_sales,0) 
          else 0 end gross_sales
    ,coalesce(dtl.shipped_usd_sales,0) as gross_incl_gc
    ,case when coalesce(dtl.nonmerch_fee_code,'-999') <> '6666' and coalesce(dtl.return_usd_amt,0)>0 
                then coalesce(dtl.shipped_usd_sales,0) 
          else 0 end as return_amt
    ,case when coalesce(dtl.nonmerch_fee_code,'-999') <> '6666' and coalesce(dtl.return_usd_amt,0)>0 then 0
          when coalesce(dtl.nonmerch_fee_code,'-999') <> '6666' then coalesce(dtl.shipped_usd_sales,0)
          else 0 end net_sales
    ,case when coalesce(dtl.nonmerch_fee_code,'-999') <> '6666' and coalesce(dtl.shipped_usd_sales,0)>0 
                    then coalesce(dtl.shipped_qty,0) 
          else 0 end gross_items
    ,case when coalesce(dtl.nonmerch_fee_code,'-999') <> '6666' and coalesce(dtl.return_usd_amt,0)>0 
                  and coalesce(dtl.shipped_usd_sales,0)>0
                then coalesce(dtl.shipped_qty,0) 
          else 0 end return_items
    ,case when coalesce(dtl.nonmerch_fee_code,'-999') <> '6666' and coalesce(dtl.return_usd_amt,0)>0 
                  and coalesce(dtl.shipped_usd_sales,0)>0 
                then 0
          when coalesce(dtl.nonmerch_fee_code,'-999') <> '6666' and coalesce(dtl.shipped_usd_sales,0)>0 
                then coalesce(dtl.shipped_qty,0) 
          else 0 end net_items

    ,dtl.return_date
    ,dtl.days_to_return
    ,dtl.return_ringing_store_num return_store
    ,case when rst.business_unit_desc in ('FULL LINE','FULL LINE CANADA','N.CA','N.COM','TRUNK CLUB')
                then 'NORDSTROM'
          when rst.business_unit_desc in ('RACK', 'RACK CANADA','OFFPRICE ONLINE') then 'RACK'
          else null
      end as return_banner
    ,case when rst.business_unit_desc in ('FULL LINE','FULL LINE CANADA') then '1) Nordstrom Stores'
          when rst.business_unit_desc in ('N.CA','N.COM','TRUNK CLUB') then '2) Nordstrom.com'
          when rst.business_unit_desc in ('RACK', 'RACK CANADA') then '3) Rack Stores'
          when rst.business_unit_desc in ('OFFPRICE ONLINE') then '4) Rack.com'
          else null
      end as return_channel
    ,ofa.item_delivery_method
    ,ofa.pickup_store
    ,ofa.pickup_date
    ,ofa.pickup_channel
    ,ofa.pickup_banner
    ,dtl.source_platform_code platform
    ,case when coalesce(dtl.employee_discount_usd_amt,0) <> 0 then 1 else 0 end employee_flag

    ,coalesce(t.tender_nordstrom,0) tender_nordstrom
    ,coalesce(t.tender_nordstrom_note,0) tender_nordstrom_note
    ,coalesce(t.tender_3rd_party_credit,0) tender_3rd_party_credit
    ,coalesce(t.tender_debit_card,0) tender_debit_card
    ,coalesce(t.tender_gift_card,0) tender_gift_card
    ,coalesce(t.tender_cash,0) tender_cash
    ,coalesce(t.tender_paypal,0) tender_paypal
    ,coalesce(t.tender_check,0) tender_check
    --,case when st.business_unit_desc in ('RACK', 'RACK CANADA','OFFPRICE ONLINE') then coalesce(ed.clear_the_rack_dates,0) else 0 end event_ctr
    ,coalesce(ed.holiday_dates,0) event_holiday
    ,case when dtl.business_unit_desc in ('FULL LINE','N.COM') then coalesce(ed.anniv_dates_us,0)
          when dtl.business_unit_desc in ('FULL LINE CANADA','N.CA') then coalesce(ed.anniv_dates_ca,0)
          else 0 end event_anniversary

    ,coalesce(ofa.svc_group_exp_delivery,0) svc_group_exp_delivery
    ,coalesce(ofa.svc_group_order_pickup,0) svc_group_order_pickup
    ,case when dtl.payroll_dept in ('1196','1205','1404') or dtl.HR_ES_IND = 'Y' then 1 else 0 end svc_group_selling_relation
    ,case when coalesce(b.remote_sell_swimlane,'X') <> 'X' --includes Style Boards, Style Links, Trunks
                or dtl.upc_num in ('439027332977','439027332984') -- includes Nordstrom to You & Nordstrom to You "Closet"
              then 1
          else 0 end svc_group_remote_selling
    ,case when trim(dtl.nonmerch_fee_code) in ('1803','1926','108','809','116','272','817','663''3855','3786','647') then 1
          else 0 end svc_group_alterations
    ,case when coalesce(upc.beauty_service,0) = 1 or trim(dtl.nonmerch_fee_code) in ('8571','8463') then 1
          else 0 end svc_group_in_store
    ,case when r.global_tran_id is not null and dtl.shipped_usd_sales > 0 then 1 else 0 end svc_group_restaurant


    ,case when substring(trim(ofa.item_delivery_method),1,9) = 'FREE_2DAY' then 1 else 0 end service_free_exp_delivery
    ,case when substring(trim(ofa.item_delivery_method),1,8) = 'NEXT_DAY' then 1 else 0 end service_next_day_pickup
    ,case when trim(ofa.item_delivery_method) = 'SAME_DAY_BOPUS' then 1 else 0 end service_same_day_bopus
    ,coalesce(ofa.curbside_ind,0) service_curbside_pickup
    ,case when substring(b.remote_sell_swimlane,1,5) in ('STYLE','PRIVA') then 1 else 0 end service_style_boards
    ,coalesce(upc.gift_wrap_service,0) service_gift_wrapping
    ,case when upc.dept_num = 587 then 1 else 0 end service_pop_in


from sarf_extract as dtl  --t2dl_das_sales_returns.retail_sales_and_returns
join prd_nap_usr_vws.store_dim as st
    on dtl.intent_store_num = st.store_num
  join realigned_calendar as cal on dtl.tran_date = cal.day_date
  left join event_dates as ed on dtl.tran_date  = ed.day_date
  left join prd_nap_usr_vws.store_dim as rst on dtl.return_ringing_store_num = rst.store_num
  left join ntn_extract as nts on dtl.global_tran_id = nts.aare_global_tran_id
  left join transaction_tenders t on dtl.business_day_date = t.business_day_date and dtl.global_tran_id=t.global_tran_id
  left join upc_lookup as upc
            on coalesce(dtl.upc_num,'X'||substring(dtl.acp_id,12,11)) = upc.upc_num
                and dtl.channel_country = upc.channel_country
  left join restaurant_tran_lines r 
  on dtl.business_day_date = r.business_day_date 
  and dtl.global_tran_id=r.global_tran_id 
  and dtl.line_item_seq_num=r.line_item_seq_num
  left join boards as b 
  on dtl.business_day_date = b.business_day_date
  and dtl.global_tran_id=b.global_tran_id
  and coalesce(lpad(trim(dtl.upc_num),15,'0'),'X'||substring(dtl.acp_id,12,11)) = b.upc_num
  left join order_fulfillment_activity ofa  
  on dtl.business_day_date = ofa.business_day_date
  and dtl.global_tran_id = ofa.global_tran_id 
  and dtl.line_item_seq_num = ofa.line_item_seq_num;


collect statistics
column (partition),
column (global_tran_id, line_item_seq_num)
on
cco_line_items_stg;


DELETE FROM {cco_t2_schema}.cco_line_items all;
insert into {cco_t2_schema}.cco_line_items
select *
from
cco_line_items_stg;


--WHERE date_shopped BETWEEN (select start_dt from date_lookup) and  (select end_dt from date_lookup);


/************************************************************************************/
/************************************************************************************
 * PART IV) Gather the necessary NSEAM data (for Alterations not captured at the cash register)
 *    a) build necessary lookup tables
 *    b) insert data into line-item-level table
 ************************************************************************************/
/************************************************************************************/


/************************************************************************************
 * PART IV-a) create lookup table of "NSEAM-ONLY ALTERAIONS TRIPS"
 *  (customer/store/day combinations found in NSEAM, but not in our Alterations transaction data)
 ************************************************************************************/

/********* PART IV-a-i) find Alteration trips in {cco_t2_schema}.cco_line_item_table *********/
--drop table alteration_trips_POS;
create volatile multiset table alteration_trips_POS as(
select distinct acp_id
  ,store_num
  ,date_shopped
from {cco_t2_schema}.cco_line_items
where svc_group_alterations = 1
  and data_source='1) TRANSACTION'
)with data primary index(acp_id,store_num,date_shopped) 
partition by range_n(date_shopped between date '2017-01-01' and date '2025-12-31' each interval '1' day,
unknown)
on commit preserve rows;

collect statistics
column (partition),
column (acp_id,store_num,date_shopped)
on
alteration_trips_POS;

/********* PART IV-a-ii) find Alterations trips in NSEAM *********/
--drop table nseam_trips;
create multiset volatile table nseam_trips as (
select distinct xr.acp_id
  ,a.store_origin store_num
  ,a.ticket_date date_shopped
from prd_nap_usr_vws.ALTERATION_TICKET_FACT a
  join
      (select distinct ticket_id
       from prd_nap_usr_vws.ALTERATION_TICKET_GARMENT_FACT
       where lower(garment_status) not like '%delete%'
       ) ntg on a.ticket_id=ntg.ticket_id
  join prd_nap_usr_vws.STORE_DIM st on a.store_origin=st.store_num
  jOIN prd_nap_usr_vws.ACP_ANALYTICAL_CUST_XREF xr on a.customer_id=xr.cust_id
where st.business_unit_desc in ('FULL LINE','FULL LINE CANADA','N.CA','N.COM','TRUNK CLUB','RACK','RACK CANADA','OFFPRICE ONLINE')
  and a.ticket_date between (select yr1_start_dt from date_lookup) and  (select yr4_end_dt from date_lookup)
  and a.store_origin is not null
  and xr.acp_id is not null
  and lower(a.ticket_status) not like '%delete%'
) with data primary index(acp_id,store_num,date_shopped) 
partition by range_n(date_shopped between date '2017-01-01' and date '2025-12-31' each interval '1' day,
unknown)
on commit preserve rows;

collect statistics
column (partition),
column (acp_id,store_num,date_shopped)
on
alteration_trips_POS;

/********* PART IV-a-iii) combine Alteration trips, flagging which are from tran data & NSEAM *********/
--drop table alterations_trips_combined;
create multiset volatile table alterations_trips_combined as (
select acp_id, store_num, date_shopped
  ,max(pos_ind) pos_ind
  ,max(nseam_ind) nseam_ind
from
  (
  select acp_id, store_num, date_shopped, 1 pos_ind, 0 nseam_ind from alteration_trips_POS
  UNION ALL
  select acp_id, store_num, date_shopped, 0 pos_ind, 1 nseam_ind from nseam_trips
  ) x
group by 1,2,3
) with data primary index(acp_id,store_num,date_shopped) 
partition by range_n(date_shopped between date '2017-01-01' and date '2025-12-31' each interval '1' day,
unknown)
on commit preserve rows;

collect statistics
column (partition),
column (acp_id,store_num,date_shopped)
on
alterations_trips_combined;

-- select pos_ind, nseam_ind, count(*)
-- from alterations_trips_combined
-- group by 1,2
-- order by 1,2;

/********* PART IV-a-iv) keep only Alteration trips found in NSEAM but NOT transaction data *********/
create multiset volatile table nseam_only_alteration_trips as (
select distinct acp_id, store_num, date_shopped
from alterations_trips_combined
where pos_ind=0
  and nseam_ind=1
) with data primary index(acp_id,store_num,date_shopped) 
partition by range_n(date_shopped between date '2017-01-01' and date '2025-12-31' each interval '1' day,
unknown)
on commit preserve rows;

collect statistics
column (partition),
column (acp_id,store_num,date_shopped)
on
nseam_only_alteration_trips;

/********* PART IV-a-v) create a "bogus" global_tran_id that's a) unique
 *      & b) won't overlap with global_tran_id values from transaction data
 *      (we want this to be unique after we insert into the final table) *********/
create multiset volatile table nseam_only_alterations_ranked as (
select acp_id, store_num, date_shopped
  ,-1*row_number() over (order by acp_id, store_num, date_shopped) global_tran_id
from nseam_only_alteration_trips
) with data primary index(acp_id,store_num,date_shopped) 
partition by range_n(date_shopped between date '2017-01-01' and date '2025-12-31' each interval '1' day,
unknown)
on commit preserve rows;

collect statistics
column (partition),
column (acp_id,store_num,date_shopped)
on
nseam_only_alterations_ranked;

/************************************************************************************
 * PART IV-b) Insert Transaction data into final table
 ************************************************************************************/

/********* PART IV-b-i) Collect all the table statistics Teradata recommends *********/
/*COLLECT STATISTICS COLUMN (STORE_NUM) ON     nseam_only_alterations_ranked;
COLLECT STATISTICS COLUMN (DATE_SHOPPED) ON     nseam_only_alterations_ranked;
COLLECT STATISTICS COLUMN (ACP_ID) ON     nseam_only_alterations_ranked;*/


/********* PART III-B-ii) "Monster query" to insert everything into final table *********/

-- CALL SYS_MGMT.drop_if_exists_sp('T3DL_ACE_CORP', 'cco_line_item_table', OUT_RETURN_MSG);
-- create multiset table {cco_t2_schema}.cco_line_item_table as (
--create multiset volatile table cco_line_item_table as (

--delete records for same run dates
--DELETE FROM table
--WHERE date_shopped BETWEEN (select start_dt from date_lookup) and  (select end_dt from date_lookup);

insert into {cco_t2_schema}.cco_line_items
--explain
select distinct dtl.global_tran_id
    ,1 line_item_seq_num
    ,'2) NSEAM' data_source
    ,null order_num
    ,dtl.acp_id
    ,case when st.business_unit_desc in ('FULL LINE','FULL LINE CANADA','N.CA','N.COM','TRUNK CLUB')
                then 'NORDSTROM'
          when st.business_unit_desc in ('RACK', 'RACK CANADA','OFFPRICE ONLINE') then 'RACK'
          else null end as banner
    ,case when st.business_unit_desc in ('FULL LINE','FULL LINE CANADA') then '1) Nordstrom Stores'
          when st.business_unit_desc in ('N.CA','N.COM','TRUNK CLUB') then '2) Nordstrom.com'
          when st.business_unit_desc in ('RACK', 'RACK CANADA') then '3) Rack Stores'
          when st.business_unit_desc in ('OFFPRICE ONLINE') then '4) Rack.com'
          else null end as channel
    ,case when (st.business_unit_desc like '%CANADA' or st.business_unit_desc like '%.CA') then 'CA' else 'US' end channel_country
    ,dtl.store_num
    ,st.subgroup_desc as store_region
    ,null div_num
    ,null div_desc
    ,null subdiv_num
    ,null subdiv_desc
    ,null dept_num
    ,null dept_desc
    ,null brand_name
    ,null npg_flag
    ,null as price_type --Reg/Promo/Clear
    ,case when (st.business_unit_desc like '%CANADA' or st.business_unit_desc like '%.CA') then 'CA' else 'US' end store_country
    ,0 as ntn_tran
    ,dtl.date_shopped
    ,cal.month_num fiscal_month_num
    ,cal.quarter_num fiscal_qtr_num
    ,cal.year_num fiscal_yr_num
    ,cast('FY-'||cast((cal.year_num mod 2000) as varchar(2)) as varchar(8)) fiscal_year_shopped
    ,case when cal.month_num between (select yr1_start_mo from month_lookup) and (select yr1_end_mo from month_lookup) 
            then (select yr1_name from month_names)
          when cal.month_num between (select yr2_start_mo from month_lookup) and (select yr2_end_mo from month_lookup) 
            then (select yr2_name from month_names)
          when cal.month_num between (select yr3_start_mo from month_lookup) and (select yr3_end_mo from month_lookup) 
            then (select yr3_name from month_names)
          when cal.month_num between (select yr4_start_mo from month_lookup) and (select yr4_end_mo from month_lookup) 
            then (select yr4_name from month_names)
          else null end reporting_year_shopped
    ,'0) $0' item_price_band
    ,0 gross_sales
    ,0 gross_incl_gc
    ,0 return_amt
    ,0 net_sales
    ,0 gross_items
    ,0 return_items
    ,0 net_items
    --,gross_items - return_items net_items
    ,null return_date
    ,null days_to_return
    ,null return_store
    ,null return_banner
    ,null return_channel
    ,null item_delivery_method
    ,null pickup_store
    ,null pickup_date
    ,null pickup_channel
    ,null pickup_banner
    ,null platform
    ,0 employee_flag

    ,0 tender_nordstrom
    ,0 tender_nordstrom_note
    ,0 tender_3rd_party_credit
    ,0 tender_debit_card
    ,0 tender_gift_card
    ,0 tender_cash
    ,0 tender_paypal
    ,0 tender_check
    --,case when st.business_unit_desc in ('RACK', 'RACK CANADA','OFFPRICE ONLINE') then coalesce(ed.clear_the_rack_dates,0) else 0 end event_ctr
    ,coalesce(ed.holiday_dates,0) event_holiday
    ,case when st.business_unit_desc in ('FULL LINE','N.COM') then coalesce(ed.anniv_dates_us,0)
          when st.business_unit_desc in ('FULL LINE CANADA','N.CA') then coalesce(ed.anniv_dates_ca,0)
          else 0 end event_anniversary

    ,0 svc_group_exp_delivery
    ,0 svc_group_order_pickup
    ,0 svc_group_selling_relation
    ,0 svc_group_remote_selling
    ,1 svc_group_alterations
    ,0 svc_group_in_store
    ,0 svc_group_restaurant


    ,0 service_free_exp_delivery
    ,0 service_next_day_pickup
    ,0 service_same_day_bopus
    ,0 service_curbside_pickup
    ,0 service_style_boards
    ,0 service_gift_wrapping
    ,0 service_pop_in

from nseam_only_alterations_ranked as dtl  --t2dl_das_sales_returns.retail_sales_and_returns
  join prd_nap_usr_vws.store_dim as st 
  on dtl.store_num = st.store_num
  join realigned_calendar as cal 
  on dtl.date_shopped = cal.day_date
  left join event_dates as ed 
  on dtl.date_shopped = ed.day_date
where dtl.date_shopped between (select yr1_start_dt from date_lookup) and  (select yr4_end_dt from date_lookup)
    and dtl.acp_id is not null
    and st.business_unit_desc 
       in ('FULL LINE','FULL LINE CANADA','N.CA','N.COM','TRUNK CLUB','RACK','RACK CANADA','OFFPRICE ONLINE');


collect statistics
column (partition),
column (acp_id,reporting_year_shopped)
on
{cco_t2_schema}.cco_line_items;

-- sel data_source
--   ,count(*)
--   ,count(distinct global_tran_id||line_item_seq_num)
-- from {cco_t2_schema}.cco_line_item_table
-- group by 1
-- order by 1;

SET QUERY_BAND = NONE FOR SESSION;