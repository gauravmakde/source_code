SET QUERY_BAND = 'App_ID=APP09037;
DAG_ID=mothership_ets_primary_11521_ACE_ENG;
Task_Name=ets_primary;'
FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_mothership.ets_primary
Team/Owner: tech_ffp_analytics/Matthew Bond
Date Modified: 12/15/2023

Notes:
ETS = Executive Telemetry Scorecard tableau dashboard: https://tableau.nordstrom.com/#/site/AS/workbooks/32064/views
creates intermediate data table to make dashboard run quickly, the primary table used to power the dashboard
*/

------------------------------------------------------------------------------
create volatile multiset table date_lookup as (
 select
   case when month_num >= 202301 then month_num else null end as month_num,
   month_454_num,
   quarter_num,
   halfyear_num,
   year_num,
   min(day_date) as start_dt,
   max(day_date) as end_dt
 from prd_nap_usr_vws.day_cal
   where 1=1
   and month_num between 202401 and (select max(report_period) from dl_cma_cmbr.month_num_buyerflow)
   group by 1,2,3,4,5
) with data primary index(quarter_num) on commit preserve rows;


-- TY customer and transactional behavior
create volatile multiset table ty_positive as (
 select
   case
       when str.business_unit_desc in ('FULL LINE','FULL LINE CANADA') then 'FLS'
       when str.business_unit_desc in ('N.CA','N.COM','TRUNK CLUB') then 'NCOM'
       when str.business_unit_desc in ('RACK', 'RACK CANADA') then 'RACK'
       when str.business_unit_desc in ('OFFPRICE ONLINE') then 'NRHL'
     else null end as channel,
   case when channel in ('FLS','NCOM') then 'FP' else 'OP' end as brand,
   acp_id,
   ytd_flag,
   qtd_q1_flag,
   qtd_q2_flag,
   qtd_q3_flag,
   qtd_q4_flag,
   ly_flag,
   h1_flag,
   q1_flag,
   feb_flag,
   count(distinct(case when ytd_flag = 1 then trip_id else null end)) as trips_ytd,
   count(distinct(case when qtd_q1_flag = 1 then trip_id else null end)) as trips_qtd_q1,
   count(distinct(case when qtd_q2_flag = 1 then trip_id else null end)) as trips_qtd_q2,
   count(distinct(case when qtd_q3_flag = 1 then trip_id else null end)) as trips_qtd_q3,
   count(distinct(case when qtd_q4_flag = 1 then trip_id else null end)) as trips_qtd_q4,
   count(distinct(case when ly_flag = 1 then trip_id else null end)) as trips_ly,
   count(distinct(case when h1_flag = 1 then trip_id else null end)) as trips_h1,
   count(distinct(case when q1_flag = 1 then trip_id else null end)) as trips_q1,
   count(distinct(case when feb_flag = 1 then trip_id else null end)) as trips_feb
 from (
     select
       dtl.acp_id,
         -- change bopus intent_store_num to ringing_store_num 808 or 867
         case
           when line_item_order_type = 'CustInitWebOrder'
             and line_item_fulfillment_type = 'StorePickUp'
             and line_item_net_amt_currency_code = 'USD'
             then 808
           when line_item_order_type = 'CustInitWebOrder'
             and line_item_fulfillment_type = 'StorePickUp'
             and line_item_net_amt_currency_code = 'CAD'
             then 867
           else intent_store_num end as store_num,
       -- use order date is there is one
       coalesce(dtl.order_date,dtl.tran_date) as sale_date,
       case when coalesce(dtl.order_date,dtl.tran_date) >= (select min(start_dt) from date_lookup where year_num = 2024) then 1 else 0 end as ytd_flag,
       case when coalesce(dtl.order_date,dtl.tran_date) >= (select min(start_dt) from date_lookup where month_num in (202401,202402))
           and  coalesce(dtl.order_date,dtl.tran_date) <= (select max(end_dt) from date_lookup where month_num in (202401,202402)) then 1 else 0 end as qtd_q1_flag,
       case when coalesce(dtl.order_date,dtl.tran_date) >= (select min(start_dt) from date_lookup where month_num in (202404,202405))
           and  coalesce(dtl.order_date,dtl.tran_date) <= (select max(end_dt) from date_lookup where month_num in (202404,202405)) then 1 else 0 end as qtd_q2_flag,
       case when coalesce(dtl.order_date,dtl.tran_date) >= (select min(start_dt) from date_lookup where month_num in (202407,202408))
           and  coalesce(dtl.order_date,dtl.tran_date) <= (select max(end_dt) from date_lookup where month_num in (202407,202408)) then 1 else 0 end as qtd_q3_flag,
       case when coalesce(dtl.order_date,dtl.tran_date) >= (select min(start_dt) from date_lookup where month_num in (202410,202411))
           and  coalesce(dtl.order_date,dtl.tran_date) <= (select max(end_dt) from date_lookup where month_num in (202410,202411)) then 1 else 0 end as qtd_q4_flag,
       case when coalesce(dtl.order_date,dtl.tran_date) >= (select min(start_dt) from date_lookup where year_num = 2023)
           and  coalesce(dtl.order_date,dtl.tran_date) <= (select max(end_dt) from date_lookup where year_num = 2023) then 1 else 0 end as ly_flag,
       case when coalesce(dtl.order_date,dtl.tran_date) >= (select min(start_dt) from date_lookup where halfyear_num = 20241)
           and  coalesce(dtl.order_date,dtl.tran_date) <= (select max(end_dt) from date_lookup where halfyear_num = 20241) then 1 else 0 end as h1_flag,
       case when coalesce(dtl.order_date,dtl.tran_date) >= (select min(start_dt) from date_lookup where quarter_num = 20241)
           and  coalesce(dtl.order_date,dtl.tran_date) <= (select max(end_dt) from date_lookup where quarter_num = 20241) then 1 else 0 end as q1_flag,
       case when coalesce(dtl.order_date,dtl.tran_date) >= (select min(start_dt) from date_lookup where month_num = 202401)
           and  coalesce(dtl.order_date,dtl.tran_date) <= (select max(end_dt) from date_lookup where month_num = 202401) then 1 else 0 end as feb_flag,
       -- define trip based on store_num + date
       cast(dtl.acp_id||store_num||sale_date as varchar(150)) as trip_id
     from prd_nap_usr_vws.retail_tran_detail_fact_vw as dtl
     where coalesce(dtl.order_date,dtl.tran_date) >= (select min(start_dt) from date_lookup)
       and coalesce(dtl.order_date,dtl.tran_date) <= (select max(end_dt) from date_lookup)
       and not dtl.acp_id is null
       and dtl.line_net_usd_amt > 0
       -- and dtl.tran_type_code in ('SALE','EXCH')
   ) as tran
   inner join prd_nap_usr_vws.store_dim as str
       on tran.store_num = str.store_num
     and str.business_unit_desc in (
        'FULL LINE',
        'FULL LINE CANADA',
        'N.CA',
        'N.COM',
        'OFFPRICE ONLINE',
        'RACK',
        'RACK CANADA',
        'TRUNK CLUB')
 group by 1,2,3,4,5,6,7,8,9,10,11,12
) with data primary index(acp_id) on commit preserve rows;


create volatile multiset table ty_negative as (
 select
   case
       when str.business_unit_desc in ('FULL LINE','FULL LINE CANADA') then 'FLS'
       when str.business_unit_desc in ('N.CA','N.COM','TRUNK CLUB') then 'NCOM'
       when str.business_unit_desc in ('RACK', 'RACK CANADA') then 'RACK'
       when str.business_unit_desc in ('OFFPRICE ONLINE') then 'NRHL'
     else null end as channel,
   case when channel in ('FLS','NCOM') then 'FP' else 'OP' end as brand,
   acp_id,
   ytd_flag,
   qtd_q1_flag,
   qtd_q2_flag,
   qtd_q3_flag,
   qtd_q4_flag,
   ly_flag,
   h1_flag,
   q1_flag,
   feb_flag
 from (
     select
       dtl.acp_id,
       -- change bopus intent_store_num to ringing_store_num 808 or 867
       case
         when line_item_order_type = 'CustInitWebOrder'
           and line_item_fulfillment_type = 'StorePickUp'
           and line_item_net_amt_currency_code = 'USD'
           then 808
         when line_item_order_type = 'CustInitWebOrder'
           and line_item_fulfillment_type = 'StorePickUp'
           and line_item_net_amt_currency_code = 'CAD'
           then 867
         else intent_store_num end as store_num,
           --add flag to separate YTD and QTD calculations
       case when dtl.tran_date >= (select min(start_dt) from date_lookup where year_num = 2024) then 1 else 0 end as ytd_flag,
       case when coalesce(dtl.order_date,dtl.tran_date) >= (select min(start_dt) from date_lookup where month_num in (202401,202402))
           and  coalesce(dtl.order_date,dtl.tran_date) <= (select max(end_dt) from date_lookup where month_num in (202401,202402)) then 1 else 0 end as qtd_q1_flag,
       case when coalesce(dtl.order_date,dtl.tran_date) >= (select min(start_dt) from date_lookup where month_num in (202404,202405))
           and  coalesce(dtl.order_date,dtl.tran_date) <= (select max(end_dt) from date_lookup where month_num in (202404,202405)) then 1 else 0 end as qtd_q2_flag,
       case when coalesce(dtl.order_date,dtl.tran_date) >= (select min(start_dt) from date_lookup where month_num in (202407,202408))
           and  coalesce(dtl.order_date,dtl.tran_date) <= (select max(end_dt) from date_lookup where month_num in (202407,202408)) then 1 else 0 end as qtd_q3_flag,
       case when coalesce(dtl.order_date,dtl.tran_date) >= (select min(start_dt) from date_lookup where month_num in (202410,202411))
           and  coalesce(dtl.order_date,dtl.tran_date) <= (select max(end_dt) from date_lookup where month_num in (202410,202411)) then 1 else 0 end as qtd_q4_flag,
       case when dtl.tran_date >= (select min(start_dt) from date_lookup where year_num = 2022)
           and  dtl.tran_date <= (select max(end_dt) from date_lookup where year_num = 2022) then 1 else 0 end as ly_flag,
       case when dtl.tran_date >= (select min(start_dt) from date_lookup where halfyear_num = 20241)
           and  dtl.tran_date <= (select max(end_dt) from date_lookup where halfyear_num = 20241) then 1 else 0 end as h1_flag,
       case when dtl.tran_date >= (select min(start_dt) from date_lookup where quarter_num = 20241)
           and  dtl.tran_date <= (select max(end_dt) from date_lookup where quarter_num = 20241) then 1 else 0 end as q1_flag,
       case when dtl.tran_date >= (select min(start_dt) from date_lookup where month_num = 202401)
           and  dtl.tran_date <= (select max(end_dt) from date_lookup where month_num = 202401) then 1 else 0 end as feb_flag
     from prd_nap_usr_vws.retail_tran_detail_fact_vw as dtl
     where dtl.tran_date >= (select min(start_dt) from date_lookup)
       and dtl.tran_date <= (select max(end_dt) from date_lookup)
       and not dtl.acp_id is null
       and dtl.line_net_usd_amt <= 0
--        and dtl.tran_type_code in ('RETN','EXCH')
   ) as tran
   inner join prd_nap_usr_vws.store_dim as str
       on tran.store_num = str.store_num
     and str.business_unit_desc in (
        'FULL LINE',
        'FULL LINE CANADA',
        'N.CA',
        'N.COM',
        'OFFPRICE ONLINE',
        'RACK',
        'RACK CANADA',
        'TRUNK CLUB')
 group by 1,2,3,4,5,6,7,8,9,10,11,12
) with data primary index(acp_id) on commit preserve rows;


-- combine ty
create volatile multiset table ty as (
 select
   coalesce(a.acp_id, b.acp_id) as acp_id,
   coalesce(a.channel, b.channel) as channel,
   coalesce(a.brand, b.brand) as brand,
   coalesce(a.ytd_flag, b.ytd_flag) as ytd_flag,
   coalesce(a.qtd_q1_flag, b.qtd_q1_flag) as qtd_q1_flag,
   coalesce(a.qtd_q2_flag, b.qtd_q2_flag) as qtd_q2_flag,
   coalesce(a.qtd_q3_flag, b.qtd_q3_flag) as qtd_q3_flag,
   coalesce(a.qtd_q4_flag, b.qtd_q4_flag) as qtd_q4_flag,
   coalesce(a.ly_flag, b.ly_flag) as ly_flag,
   coalesce(a.h1_flag, b.h1_flag) as h1_flag,
   coalesce(a.q1_flag, b.q1_flag) as q1_flag,
   coalesce(a.feb_flag, b.feb_flag) as feb_flag,
   coalesce(a.trips_ytd, 0) as trips_ytd,
   coalesce(a.trips_qtd_q1, 0) as trips_qtd_q1,
   coalesce(a.trips_qtd_q2, 0) as trips_qtd_q2,
   coalesce(a.trips_qtd_q3, 0) as trips_qtd_q3,
   coalesce(a.trips_qtd_q4, 0) as trips_qtd_q4,
   coalesce(a.trips_ly, 0) as trips_ly,
   coalesce(a.trips_h1, 0) as trips_h1,
   coalesce(a.trips_q1, 0) as trips_q1,
   coalesce(a.trips_feb, 0) as trips_feb
 from ty_positive as a
 full join ty_negative as b
   on a.acp_id = b.acp_id
   and a.channel = b.channel
   and a.brand = b.brand
   and a.ytd_flag = b.ytd_flag
   and a.qtd_q1_flag = b.qtd_q1_flag
   and a.qtd_q2_flag = b.qtd_q2_flag
   and a.qtd_q3_flag = b.qtd_q3_flag
   and a.qtd_q4_flag = b.qtd_q4_flag
   and a.ly_flag = b.ly_flag
   and a.h1_flag = b.h1_flag
   and a.q1_flag = b.q1_flag
   and a.feb_flag = b.feb_flag
) with data primary index(acp_id) on commit preserve rows;


-- acquired customers
create volatile multiset table new_ty as (
select
 acp_id,
 aare_chnl_code as acquired_channel,
 case when aare_status_date >= (select min(start_dt) from date_lookup where year_num = 2024) then 1 else 0 end as ytd_flag,
 case when aare_status_date >= (select min(start_dt) from date_lookup where month_num in (202401,202402))
       and aare_status_date <= (select max(end_dt) from date_lookup where month_num in (202401,202402)) then 1 else 0 end as qtd_q1_flag,
 case when aare_status_date >= (select min(start_dt) from date_lookup where month_num in (202404,202405))
       and aare_status_date <= (select max(end_dt) from date_lookup where month_num in (202404,202405)) then 1 else 0 end as qtd_q2_flag,
 case when aare_status_date >= (select min(start_dt) from date_lookup where month_num in (202407,202408))
       and aare_status_date <= (select max(end_dt) from date_lookup where month_num in (202407,202408)) then 1 else 0 end as qtd_q3_flag,
 case when aare_status_date >= (select min(start_dt) from date_lookup where month_num in (202410,202411))
       and aare_status_date <= (select max(end_dt) from date_lookup where month_num in (202410,202411)) then 1 else 0 end as qtd_q4_flag,
 case when aare_status_date >= (select min(start_dt) from date_lookup where year_num = 2023)
       and aare_status_date <= (select max(end_dt) from date_lookup where year_num = 2023) then 1 else 0 end as ly_flag,
 case when aare_status_date >= (select min(start_dt) from date_lookup where halfyear_num = 20241)
       and aare_status_date <= (select max(end_dt) from date_lookup where halfyear_num = 20241) then 1 else 0 end as h1_flag,
 case when aare_status_date >= (select min(start_dt) from date_lookup where quarter_num = 20241)
       and aare_status_date <= (select max(end_dt) from date_lookup where quarter_num = 20241) then 1 else 0 end as q1_flag,
 case when aare_status_date >= (select min(start_dt) from date_lookup where month_num = 202401)
       and aare_status_date <= (select max(end_dt) from date_lookup where month_num = 202401) then 1 else 0 end as feb_flag
from PRD_NAP_USR_VWS.CUSTOMER_NTN_STATUS_FACT
where aare_status_date between (select min(start_dt) from date_lookup)
and (select max(end_dt) from date_lookup)
) with data primary index(acp_id) on commit preserve rows;


-- customer level summary
create volatile multiset table customer_summary as (
 select
   ty.channel,
   ty.acp_id,
   ac.country,
   ty.brand,
   ty.trips_ytd,
   ty.trips_qtd_q1,
   ty.trips_qtd_q2,
   ty.trips_qtd_q3,
   ty.trips_qtd_q4,
   ty.trips_ly,
   ty.trips_h1,
   ty.trips_q1,
   ty.trips_feb,
   ty.ytd_flag,
   ty.qtd_q1_flag,
   ty.qtd_q2_flag,
   ty.qtd_q3_flag,
   ty.qtd_q4_flag,
   ty.ly_flag,
   ty.h1_flag,
   ty.q1_flag,
   ty.feb_flag,
   case when acq.acp_id is not null and acq.ytd_flag = 1 then 1 else 0 end as new_flg_ytd,
   case when acq.acp_id is not null and acq.qtd_q1_flag = 1 then 1 else 0 end as new_flg_qtd_q1,
   case when acq.acp_id is not null and acq.qtd_q2_flag = 1 then 1 else 0 end as new_flg_qtd_q2,
   case when acq.acp_id is not null and acq.qtd_q3_flag = 1 then 1 else 0 end as new_flg_qtd_q3,
   case when acq.acp_id is not null and acq.qtd_q4_flag = 1 then 1 else 0 end as new_flg_qtd_q4,
   case when acq.acp_id is not null and acq.ly_flag = 1 then 1 else 0 end as new_flg_ly,
   case when acq.acp_id is not null and acq.h1_flag = 1 then 1 else 0 end as new_flg_h1,
   case when acq.acp_id is not null and acq.q1_flag = 1 then 1 else 0 end as new_flg_q1,
   case when acq.acp_id is not null and acq.feb_flag = 1 then 1 else 0 end as new_flg_feb
   from ty
   left join new_ty acq
     on ty.acp_id = acq.acp_id
       and ty.ytd_flag = acq.ytd_flag
       and ty.qtd_q1_flag = acq.qtd_q1_flag
       and ty.qtd_q2_flag = acq.qtd_q2_flag
       and ty.qtd_q3_flag = acq.qtd_q3_flag
       and ty.qtd_q4_flag = acq.qtd_q4_flag
       and ty.ly_flag = acq.ly_flag
       and ty.h1_flag = acq.h1_flag
       and ty.q1_flag = acq.q1_flag
       and ty.feb_flag = acq.feb_flag
   left join (
         select
         acp_id,
         -- country
         case when us_dma_code = -1 then null else us_dma_desc end as us_dma,
         case
           when us_dma is not null then 'US'
           when ca_dma_code is not null then 'CA'
         else 'unknown' end as country
           from prd_nap_usr_vws.analytical_customer ) as ac
            on ty.acp_id = ac.acp_id
    ) with data primary index (acp_id,channel,brand) on commit preserve rows;

collect statistics on customer_summary column (acp_id,channel,brand);

-- channel level
create volatile multiset table temporary_buyerflowplus_table as (
 select
   '202423_YTD' as report_period,
   cust.channel as channel,
   ebh.total_customers_ly,
   ebh.total_trips_ly,
   ebh.acquired_ly,
   count(distinct(case when ytd_flag = 1 then acp_id else null end)) as total_customers_ytd,
   count(distinct(case when qtd_q1_flag = 1 then acp_id else null end)) as total_customers_qtd_q1,
   count(distinct(case when qtd_q2_flag = 1 then acp_id else null end)) as total_customers_qtd_q2,
   count(distinct(case when qtd_q3_flag = 1 then acp_id else null end)) as total_customers_qtd_q3,
   count(distinct(case when qtd_q4_flag = 1 then acp_id else null end)) as total_customers_qtd_q4,
   --count(distinct(case when ly_flag = 1 then acp_id else null end)) as total_customers_ly,
   count(distinct(case when h1_flag = 1 then acp_id else null end)) as total_customers_h1,
   count(distinct(case when q1_flag = 1 then acp_id else null end)) as total_customers_q1,
   count(distinct(case when feb_flag = 1 then acp_id else null end)) as total_customers_feb,
   sum(trips_ytd) as total_trips_ytd,
   sum(trips_qtd_q1) as total_trips_qtd_q1,
   sum(trips_qtd_q2) as total_trips_qtd_q2,
   sum(trips_qtd_q3) as total_trips_qtd_q3,
   sum(trips_qtd_q4) as total_trips_qtd_q4,
   --sum(trips_ly) as total_trips_ly,
   sum(trips_h1) as total_trips_h1,
   sum(trips_q1) as total_trips_q1,
   sum(trips_feb) as total_trips_feb,
   count(distinct(case when new_flg_ytd= 1 then acp_id else null end)) as acquired_ytd,
   count(distinct(case when new_flg_qtd_q1= 1 then acp_id else null end)) as acquired_qtd_q1,
   count(distinct(case when new_flg_qtd_q2= 1 then acp_id else null end)) as acquired_qtd_q2,
   count(distinct(case when new_flg_qtd_q3= 1 then acp_id else null end)) as acquired_qtd_q3,
   count(distinct(case when new_flg_qtd_q4= 1 then acp_id else null end)) as acquired_qtd_q4,
   --count(distinct(case when new_flg_ly= 1 then acp_id else null end)) as acquired_ly,
   count(distinct(case when new_flg_h1= 1 then acp_id else null end)) as acquired_h1,
   count(distinct(case when new_flg_q1= 1 then acp_id else null end)) as acquired_q1,
   count(distinct(case when new_flg_feb= 1 then acp_id else null end)) as acquired_feb,
   total_trips_ytd/nullif((total_customers_ytd*1.000),0) as trips_per_cust_ytd,
   total_trips_qtd_q1/nullif((total_customers_qtd_q1*1.000),0) as trips_per_cust_qtd_q1,
   total_trips_qtd_q2/nullif((total_customers_qtd_q2*1.000),0) as trips_per_cust_qtd_q2,
   total_trips_qtd_q3/nullif((total_customers_qtd_q3*1.000),0) as trips_per_cust_qtd_q3,
   total_trips_qtd_q4/nullif((total_customers_qtd_q4*1.000),0) as trips_per_cust_qtd_q4,
   total_trips_ly/nullif((total_customers_ly*1.000),0) as trips_per_cust_ly,
   total_trips_h1/nullif((total_customers_h1*1.000),0) as trips_per_cust_h1,
   total_trips_q1/nullif((total_customers_q1*1.000),0) as trips_per_cust_q1,
   total_trips_feb/nullif((total_customers_feb*1.000),0) as trips_per_cust_feb,
   current_timestamp as current_timesta
 from customer_summary as cust
 left join T2DL_DAS_MOTHERSHIP.ets_buyerflow_hist as ebh
   on cust.channel = ebh.channel
 where country <> 'CA'
 group by 1,2,3,4,5)
with data primary index (report_period, channel) on commit preserve rows;

-- brand level
insert into temporary_buyerflowplus_table
 select
   '202423_YTD',
   brand,
   ebh.total_customers_ly,
   ebh.total_trips_ly,
   ebh.acquired_ly,
   count(distinct(case when ytd_flag = 1 then acp_id else null end)) as total_customers_ytd,
   count(distinct(case when qtd_q1_flag = 1 then acp_id else null end)) as total_customers_qtd_q1,
   count(distinct(case when qtd_q2_flag = 1 then acp_id else null end)) as total_customers_qtd_q2,
   count(distinct(case when qtd_q3_flag = 1 then acp_id else null end)) as total_customers_qtd_q3,
   count(distinct(case when qtd_q4_flag = 1 then acp_id else null end)) as total_customers_qtd_q4,
   --count(distinct(case when ly_flag = 1 then acp_id else null end)) as total_customers_ly,
   count(distinct(case when h1_flag = 1 then acp_id else null end)) as total_customers_h1,
   count(distinct(case when q1_flag = 1 then acp_id else null end)) as total_customers_q1,
   count(distinct(case when feb_flag = 1 then acp_id else null end)) as total_customers_feb,
   sum(trips_ytd) as total_trips_ytd,
   sum(trips_qtd_q1) as total_trips_qtd_q1,
   sum(trips_qtd_q2) as total_trips_qtd_q2,
   sum(trips_qtd_q3) as total_trips_qtd_q3,
   sum(trips_qtd_q4) as total_trips_qtd_q4,
   --sum(trips_ly) as total_trips_ly,
   sum(trips_h1) as total_trips_h1,
   sum(trips_q1) as total_trips_q1,
   sum(trips_feb) as total_trips_feb,
   count(distinct(case when new_flg_ytd= 1 then acp_id else null end)) as acquired_ytd,
   count(distinct(case when new_flg_qtd_q1= 1 then acp_id else null end)) as acquired_qtd_q1,
   count(distinct(case when new_flg_qtd_q2= 1 then acp_id else null end)) as acquired_qtd_q2,
   count(distinct(case when new_flg_qtd_q3= 1 then acp_id else null end)) as acquired_qtd_q3,
   count(distinct(case when new_flg_qtd_q4= 1 then acp_id else null end)) as acquired_qtd_q4,
   --count(distinct(case when new_flg_ly= 1 then acp_id else null end)) as acquired_ly,
   count(distinct(case when new_flg_h1= 1 then acp_id else null end)) as acquired_h1,
   count(distinct(case when new_flg_q1= 1 then acp_id else null end)) as acquired_q1,
   count(distinct(case when new_flg_feb= 1 then acp_id else null end)) as acquired_feb,
   total_trips_ytd/nullif((total_customers_ytd*1.000),0) as trips_per_cust_ytd,
   total_trips_qtd_q1/nullif((total_customers_qtd_q1*1.000),0) as trips_per_cust_qtd_q1,
   total_trips_qtd_q2/nullif((total_customers_qtd_q2*1.000),0) as trips_per_cust_qtd_q2,
   total_trips_qtd_q3/nullif((total_customers_qtd_q3*1.000),0) as trips_per_cust_qtd_q3,
   total_trips_qtd_q4/nullif((total_customers_qtd_q4*1.000),0) as trips_per_cust_qtd_q4,
   total_trips_ly/nullif((total_customers_ly*1.000),0) as trips_per_cust_ly,
   total_trips_h1/nullif((total_customers_h1*1.000),0) as trips_per_cust_h1,
   total_trips_q1/nullif((total_customers_q1*1.000),0) as trips_per_cust_q1,
   total_trips_feb/nullif((total_customers_feb*1.000),0) as trips_per_cust_feb,
   current_timestamp
 from customer_summary as cust
 left join T2DL_DAS_MOTHERSHIP.ets_buyerflow_hist as ebh
   on cust.brand = ebh.channel
 where country <> 'CA'
 group by 1,2,3,4,5;


-- JWN level
insert into temporary_buyerflowplus_table
 select
   '202423_YTD',
   'JWN',
   ebh.total_customers_ly,
   ebh.total_trips_ly,
   ebh.acquired_ly,
   count(distinct(case when ytd_flag = 1 then acp_id else null end)) as total_customers_ytd,
   count(distinct(case when qtd_q1_flag = 1 then acp_id else null end)) as total_customers_qtd_q1,
   count(distinct(case when qtd_q2_flag = 1 then acp_id else null end)) as total_customers_qtd_q2,
   count(distinct(case when qtd_q3_flag = 1 then acp_id else null end)) as total_customers_qtd_q3,
   count(distinct(case when qtd_q4_flag = 1 then acp_id else null end)) as total_customers_qtd_q4,
   --count(distinct(case when ly_flag = 1 then acp_id else null end)) as total_customers_ly,
   count(distinct(case when h1_flag = 1 then acp_id else null end)) as total_customers_h1,
   count(distinct(case when q1_flag = 1 then acp_id else null end)) as total_customers_q1,
   count(distinct(case when feb_flag = 1 then acp_id else null end)) as total_customers_feb,
   sum(trips_ytd) as total_trips_ytd,
   sum(trips_qtd_q1) as total_trips_qtd_q1,
   sum(trips_qtd_q2) as total_trips_qtd_q2,
   sum(trips_qtd_q3) as total_trips_qtd_q3,
   sum(trips_qtd_q4) as total_trips_qtd_q4,
   --sum(trips_ly) as total_trips_ly,
   sum(trips_h1) as total_trips_h1,
   sum(trips_q1) as total_trips_q1,
   sum(trips_feb) as total_trips_feb,
   count(distinct(case when new_flg_ytd= 1 then acp_id else null end)) as acquired_ytd,
   count(distinct(case when new_flg_qtd_q1= 1 then acp_id else null end)) as acquired_qtd_q1,
   count(distinct(case when new_flg_qtd_q2= 1 then acp_id else null end)) as acquired_qtd_q2,
   count(distinct(case when new_flg_qtd_q3= 1 then acp_id else null end)) as acquired_qtd_q3,
   count(distinct(case when new_flg_qtd_q4= 1 then acp_id else null end)) as acquired_qtd_q4,
   --count(distinct(case when new_flg_ly= 1 then acp_id else null end)) as acquired_ly,
   count(distinct(case when new_flg_h1= 1 then acp_id else null end)) as acquired_h1,
   count(distinct(case when new_flg_q1= 1 then acp_id else null end)) as acquired_q1,
   count(distinct(case when new_flg_feb= 1 then acp_id else null end)) as acquired_feb,
   total_trips_ytd/nullif((total_customers_ytd*1.000),0) as trips_per_cust_ytd,
   total_trips_qtd_q1/nullif((total_customers_qtd_q1*1.000),0) as trips_per_cust_qtd_q1,
   total_trips_qtd_q2/nullif((total_customers_qtd_q2*1.000),0) as trips_per_cust_qtd_q2,
   total_trips_qtd_q3/nullif((total_customers_qtd_q3*1.000),0) as trips_per_cust_qtd_q3,
   total_trips_qtd_q4/nullif((total_customers_qtd_q4*1.000),0) as trips_per_cust_qtd_q4,
   total_trips_ly/nullif((total_customers_ly*1.000),0) as trips_per_cust_ly,
   total_trips_h1/nullif((total_customers_h1*1.000),0) as trips_per_cust_h1,
   total_trips_q1/nullif((total_customers_q1*1.000),0) as trips_per_cust_q1,
   total_trips_feb/nullif((total_customers_feb*1.000),0) as trips_per_cust_feb,
   current_timestamp
 from customer_summary as cust
 left join T2DL_DAS_MOTHERSHIP.ets_buyerflow_hist as ebh
   on ebh.channel = 'JWN'
 where country <> 'CA'
 group by 1,2,3,4,5;


 create multiset volatile table month_range as (
  select
     distinct month_idnt as month_num,
     month_start_day_date,
     month_end_day_date,
     month_abrv,
     quarter_abrv,
     quarter_idnt,
     fiscal_halfyear_num,
     fiscal_year_num
  from prd_nap_usr_vws.day_cal_454_dim
  where month_idnt between 202301 and 202412
 )with data primary index (month_num) on commit preserve rows;


 create multiset volatile table cust_fm as (
 select
 b.report_period as month_num,
 box,
 case when month_num = 202401 then total_customers_feb else total_customer end as total_customer,
 case when month_num = 202401 then total_trips_feb else total_trips end as total_trips,
 case when month_num = 202401 then acquired_feb else new_customer end as new_customer,
 case when month_num = 202401 then trips_per_cust_feb else total_trips/total_customer end as trips_per_cust
 from dl_cma_cmbr.month_num_buyerflow as b --have week/month/quarter tables
 left join temporary_buyerflowplus_table as tbt
     on b.box = tbt.channel
 where month_num in (select month_num from month_range where fiscal_year_num = 2024)
 and month_num < (select distinct month_num from prd_nap_usr_vws.day_cal where day_date = current_date)
 ) with data primary index (month_num,box) on commit preserve rows;


 create multiset volatile table cust_fq as (
 select
 b.report_period as quarter_num,
 box,
 case when quarter_num = 20241 then total_customers_q1 else total_customer end as total_customer,
 case when quarter_num = 20241 then total_trips_q1 else total_trips end as total_trips,
 case when quarter_num = 20241 then acquired_q1 else new_customer end as new_customer,
 case when quarter_num = 20241 then trips_per_cust_q1 else total_trips/total_customer end as trips_per_cust
 from dl_cma_cmbr.quarter_num_buyerflow as b --have week/month/quarter tables
 left join temporary_buyerflowplus_table as tbt
     on b.box = tbt.channel
 where quarter_num in (select quarter_idnt from month_range where fiscal_year_num = 2024)
 and quarter_num < (select distinct quarter_num from prd_nap_usr_vws.day_cal where day_date = current_date)
     UNION ALL
 --if quarter is not complete then it isn't in the table above
 select
 'qtd_q1' as quarter_num,
 channel as box,
 total_customers_qtd_q1 as total_customer,
 total_trips_qtd_q1 as total_trips,
 acquired_qtd_q1 as new_customer,
 trips_per_cust_qtd_q1 as trips_per_cust
 from temporary_buyerflowplus_table
     UNION ALL
 select
 'qtd_q2' as quarter_num,
 channel as box,
 total_customers_qtd_q2 as total_customer,
 total_trips_qtd_q2 as total_trips,
 acquired_qtd_q2 as new_customer,
 trips_per_cust_qtd_q2 as trips_per_cust
 from temporary_buyerflowplus_table
     UNION ALL
 select
 'qtd_q3' as quarter_num,
 channel as box,
 total_customers_qtd_q3 as total_customer,
 total_trips_qtd_q3 as total_trips,
 acquired_qtd_q3 as new_customer,
 trips_per_cust_qtd_q3 as trips_per_cust
 from temporary_buyerflowplus_table
     UNION ALL
 select
 'qtd_q4' as quarter_num,
 channel as box,
 total_customers_qtd_q4 as total_customer,
 total_trips_qtd_q4 as total_trips,
 acquired_qtd_q4 as new_customer,
 trips_per_cust_qtd_q4 as trips_per_cust
 from temporary_buyerflowplus_table
     UNION ALL
 --for quarters that are further in the future than the current quarter, add these manually so that later joins work properly
 select
 cast(quarter_idnt as varchar(5)) as quarter_num,
 box,
 null as total_customer,
 null as total_trips,
 null as new_customer,
 null as trips_per_cust
 from cust_fm --have week/month/quarter tables
 cross join month_range
 where quarter_idnt > (select max(report_period) from dl_cma_cmbr.quarter_num_buyerflow)
 group by 1,2
 ) with data primary index (quarter_num,box) on commit preserve rows;


 create multiset volatile table cust_fh as (
 select
 b.report_period as half_num,
 box,
 case when half_num = 20241 then total_customers_h1 else total_customer end as total_customer,
 case when half_num = 20241 then total_trips_h1 else total_trips end as total_trips,
 case when half_num = 20241 then acquired_h1 else new_customer end as new_customer,
 case when half_num = 20241 then trips_per_cust_h1 else total_trips/total_customer end as trips_per_cust
 from dl_cma_cmbr.halfyear_num_buyerflow as b --have week/month/quarter tables
 left join temporary_buyerflowplus_table as tbt
     on b.box = tbt.channel
 where half_num in (select fiscal_halfyear_num from month_range where fiscal_year_num = (select max(fiscal_year_num) from month_range))
 and half_num < (select distinct halfyear_num from prd_nap_usr_vws.day_cal where day_date = current_date)
     UNION ALL
 --for halves that are further in the future than the current quarter, add these manually so that later joins work properly
 select
 cast(fiscal_halfyear_num as varchar(5)) as half_num,
 box,
 null as total_customer,
 null as total_trips,
 null as new_customer,
 null as trips_per_cust
 from cust_fm --have week/month/quarter tables
 cross join month_range
 where fiscal_halfyear_num > (select max(report_period) from dl_cma_cmbr.halfyear_num_buyerflow)
 group by 1,2
 ) with data primary index (half_num,box) on commit preserve rows;


 create multiset volatile table cust_fy as (
 select
 '2023' as year_num,
 channel as box,
 cast('ly' as varchar(3)) as period_type,
 total_customers_ly as total_customer,
 total_trips_ly as total_trips,
 acquired_ly as new_customer,
 trips_per_cust_ly as trips_per_cust
 from temporary_buyerflowplus_table
     UNION ALL
 --if year is not complete then it isn't in the table above
 select
 '2024' as year_num,
 channel as box,
 'ytd' as period_type,
 total_customers_ytd as total_customer,
 total_trips_ytd as total_trips,
 acquired_ytd as new_customer,
 trips_per_cust_ytd as trips_per_cust
 from temporary_buyerflowplus_table
     UNION ALL
 --add blank data with ty period_type for join later
 select distinct
 '2024' as year_num,
 box,
 'ty' as period_type,
 null as total_customer,
 null as total_trips,
 null as new_customer,
 null as trips_per_cust
 from dl_cma_cmbr.quarter_num_buyerflow --have week/month/quarter tables
 -- commenting out below line as it was preventing any data from being loaded
 -- where report_period in (select quarter_idnt from month_range where fiscal_year_num >= 2023)
 ) with data primary index (year_num,box) on commit preserve rows;


 --taken from this dashboard:
 --https://tableau.nordstrom.com/#/site/SupplyChain/views/SCOutboundHistoricalReporting/FCC2D?:iid=1
 create multiset volatile table pre_p50 AS (
 SELECT
 OL.ORDER_NUM
 ,OL.CARRIER_TRACKING_NUM
 ,OL.SOURCE_CHANNEL_CODE
 ,OL.ORDER_TMSTP_PACIFIC
 ,OL.ORDER_DATE_PACIFIC
 ,OL.DELIVERY_TMSTP
 ,OL.DELIVERY_DATE
 ,DDD.fiscal_year_num AS FISCAL_YEAR
 ,DDD.fiscal_halfyear_num as half_num
 ,DDD.quarter_idnt
 ,case when DDD.month_idnt in (202401,202402) then 'qtd_q1'
     when DDD.month_idnt in (202404,202405) then 'qtd_q2'
     when DDD.month_idnt in (202407,202408) then 'qtd_q3'
     when DDD.month_idnt in (202410,202411) then 'qtd_q4'
     else null end as qtd_idnt
 ,DDD.month_idnt
 ,CASE WHEN OL.CLICK_TO_DELIVERY_SECONDS IS NULL THEN CAST(NULL AS FLOAT)
 	WHEN CAST(OL.CLICK_TO_DELIVERY_SECONDS AS FLOAT)/86400 > 20 THEN 20
 	WHEN CAST(OL.CLICK_TO_DELIVERY_SECONDS AS FLOAT)/86400 <= 0 THEN  CAST(NULL AS FLOAT)
 	ELSE ROUND(CAST(OL.CLICK_TO_DELIVERY_SECONDS AS FLOAT)/86400, 2)
 END AS CLICK_TO_DELIVERY
 FROM (
 SELECT OLDF.ORDER_NUM
 ,OLDF.CARRIER_TRACKING_NUM
 ,OLDF.SOURCE_CHANNEL_CODE
 ,OLDF.ORDER_TMSTP_PACIFIC
 ,OLDF.ORDER_DATE_PACIFIC
 ,MAX(OLDF.SHIPPED_TMSTP_PACIFIC) AS SHIPPED_TMSTP_PACIFIC
 ,MAX(OLDF.SHIPPED_DATE_PACIFIC) AS SHIPPED_DATE_PACIFIC
 ,MAX(CAST(OLDF.CARRIER_FIRST_ATTEMPTED_DELIVERY_TMSTP_PACIFIC AS DATE)) AS DELIVERY_DATE
 ,MAX(OLDF.CARRIER_FIRST_ATTEMPTED_DELIVERY_TMSTP_PACIFIC) AS DELIVERY_TMSTP
 ,MAX((CAST(((CARRIER_FIRST_ATTEMPTED_DELIVERY_TMSTP_PACIFIC- ORDER_TMSTP_PACIFIC) DAY(4)) AS BIGINT) * 86400)
 	+ ((EXTRACT(HOUR FROM CARRIER_FIRST_ATTEMPTED_DELIVERY_TMSTP_PACIFIC) - EXTRACT(HOUR FROM ORDER_TMSTP_PACIFIC)) * 3600)
 	+ ((EXTRACT(MINUTE FROM CARRIER_FIRST_ATTEMPTED_DELIVERY_TMSTP_PACIFIC) - EXTRACT(MINUTE FROM ORDER_TMSTP_PACIFIC)) * 60)
 	+  (EXTRACT(SECOND FROM CARRIER_FIRST_ATTEMPTED_DELIVERY_TMSTP_PACIFIC) - EXTRACT(SECOND FROM ORDER_TMSTP_PACIFIC))) AS CLICK_TO_DELIVERY_SECONDS
 FROM PRD_NAP_USR_VWS.ORDER_LINE_DETAIL_FACT OLDF
 WHERE SHIPPED_NODE_TYPE_CODE = 'FC'
 AND YEAR(OLDF.CARRIER_FIRST_ATTEMPTED_DELIVERY_TMSTP_PACIFIC) >= 2022
 AND OLDF.CARRIER_FIRST_ATTEMPTED_DELIVERY_TMSTP_PACIFIC > SHIPPED_TMSTP_PACIFIC
 AND OLDF.SHIPPED_TMSTP_PACIFIC > OLDF.ORDER_TMSTP_PACIFIC
 AND OLDF.CARRIER_FIRST_ATTEMPTED_DELIVERY_TMSTP_PACIFIC <= CURRENT_DATE()
 GROUP BY 1, 2, 3, 4, 5
 ) OL
 INNER JOIN T2DL_SCA_VWS.DAY_CAL_454_DIM_VW DDD ON DDD.day_date = OL.DELIVERY_DATE
 INNER JOIN T2DL_SCA_VWS.DAY_CAL_454_DIM_VW AAA ON AAA.day_date = CURRENT_DATE()
 WHERE DDD.fiscal_year_num IN (2023,2024)
 AND DDD.month_idnt < AAA.month_idnt
 )with data primary index (ORDER_NUM, CLICK_TO_DELIVERY, month_idnt) on commit preserve rows;

 create multiset volatile table p50_fm as (
 SELECT 'JWN' AS channel
 , FISCAL_YEAR
 , half_num
 , quarter_idnt AS Quarter
 , month_idnt AS fm
 , 1.00*CAST((1000*PERCENTILE_DISC(.5) WITHIN GROUP (ORDER BY CLICK_TO_DELIVERY ASC)) AS INT)/1000 AS p50_fm
 FROM pre_p50
 group by 1,2,3,4,5
 )with data primary index (fm,channel) on commit preserve rows;

 create multiset volatile table p50_fq as (
 SELECT 'JWN' AS channel
 , FISCAL_YEAR
 , half_num
 , quarter_idnt AS Quarter
 , 'All' AS fm
 , 1.00*CAST((1000*PERCENTILE_DISC(.5) WITHIN GROUP (ORDER BY CLICK_TO_DELIVERY ASC)) AS INT)/1000 AS p50_fq
 FROM pre_p50
 GROUP BY 1,2,3,4,5
 )with data primary index (Quarter,channel) on commit preserve rows;

 create multiset volatile table p50_qtd as (
 SELECT 'JWN' AS channel
 , FISCAL_YEAR
 , half_num
 , qtd_idnt as qtd
 , 'All' AS fm
 , 1.00*CAST((1000*PERCENTILE_DISC(.5) WITHIN GROUP (ORDER BY CLICK_TO_DELIVERY ASC)) AS INT)/1000 AS p50_fq
 FROM pre_p50
 where qtd is not null
 GROUP BY 1,2,3,4,5
 )with data primary index (qtd,channel) on commit preserve rows;

 create multiset volatile table p50_fh as (
 SELECT 'JWN' AS channel
 , FISCAL_YEAR
 , half_num
 , 'All' as Quarter
 , 'All' AS fm
 , 1.00*CAST((1000*PERCENTILE_DISC(.5) WITHIN GROUP (ORDER BY CLICK_TO_DELIVERY ASC)) AS INT)/1000 AS p50_fh
 FROM pre_p50
 GROUP BY 1,2,3,4,5
 )with data primary index (half_num,channel) on commit preserve rows;

 --ytd and ly- use case statement to relabel
 create multiset volatile table p50_fy as (
 SELECT 'JWN' AS channel
 , FISCAL_YEAR
 , 'All' as Quarter
 , 'All' AS fm
 , 1.00*CAST((1000*PERCENTILE_DISC(.5) WITHIN GROUP (ORDER BY CLICK_TO_DELIVERY ASC)) AS INT)/1000 AS p50_fy
 FROM pre_p50
 GROUP BY 1,2,3,4
 )with data primary index (FISCAL_YEAR,channel) on commit preserve rows;


 create multiset volatile table pre_sbrands as (
 select
 case when chnl_label = '210, RACK STORES' then 'RACK' else 'NRHL' end as chnl,
 month_num, quarter_num, halfyear_num as half_num, year_num,
 case when month_num in (202401,202402) then 'qtd_q1'
     when month_num in (202404,202405) then 'qtd_q2'
     when month_num in (202407,202408) then 'qtd_q3'
     when month_num in (202410,202411) then 'qtd_q4'
     else null end as qtd_num,
 sum(case when sb.Brand is not null then sales_dollars else 0 end) as sb_sales,
 sum(sales_dollars) as total_sales_for_sb
 from t2dl_das_in_season_management_reporting.wbr_supplier as ws
 left join t2dl_das_mothership.ets_rsb_lookup as sb
     on ws.brand = sb.Brand --or ws.supplier = sb.Brand
 left join (select distinct week_num, month_num, quarter_num, halfyear_num, year_num from PRD_NAP_USR_VWS.DAY_CAL) as dc
     on ws.week_idnt = dc.week_num
 where month_num in (select month_num from month_range)
 and month_num < (select distinct month_num from prd_nap_usr_vws.day_cal where day_date = current_date)
 and chnl_label in ('210, RACK STORES', '250, OFFPRICE ONLINE')
 group by 1,2,3,4,5,6
 )with data primary index (month_num,chnl) on commit preserve rows;


 create multiset volatile table sbrands_fm as (
 select
 chnl,
 month_num, quarter_num, qtd_num, half_num, year_num,
 sb_sales,
 total_sales_for_sb,
 sb_sales/nullifzero(total_sales_for_sb) as pct_sb_sales
 from pre_sbrands
 UNION ALL
 select
 case when chnl is not null then 'OP' else null end as chnl,
 month_num, quarter_num, qtd_num, half_num, year_num,
 sum(sb_sales) as sb_sales,
 sum(total_sales_for_sb) as total_sales_for_sb,
 sum(sb_sales)/nullifzero(sum(total_sales_for_sb)) as pct_sb_sales
 from pre_sbrands
 group by 1,2,3,4,5,6
 UNION ALL
 select
 case when chnl is not null then 'JWN' else null end as chnl,
 month_num, quarter_num, qtd_num, half_num, year_num,
 sum(sb_sales) as sb_sales,
 sum(total_sales_for_sb) as total_sales_for_sb,
 sum(sb_sales)/nullifzero(sum(total_sales_for_sb)) as pct_sb_sales
 from pre_sbrands
 group by 1,2,3,4,5,6
 )with data primary index (month_num,chnl) on commit preserve rows;

 create multiset volatile table sbrands_fq as (
 select
 chnl,
 quarter_num, half_num, year_num,
 sum(sb_sales) as sb_sales,
 sum(total_sales_for_sb) as total_sales_for_sb,
 sum(sb_sales)/nullifzero(sum(total_sales_for_sb)) as pct_sb_sales
 from sbrands_fm
 group by 1,2,3,4
 )with data primary index (quarter_num,chnl) on commit preserve rows;

 create multiset volatile table sbrands_qtd as (
 select
 chnl,
 qtd_num, half_num, year_num,
 sum(sb_sales) as sb_sales,
 sum(total_sales_for_sb) as total_sales_for_sb,
 sum(sb_sales)/nullifzero(sum(total_sales_for_sb)) as pct_sb_sales
 from sbrands_fm
 group by 1,2,3,4
 )with data primary index (qtd_num,chnl) on commit preserve rows;

 create multiset volatile table sbrands_fh as (
 select
 chnl,
 half_num, year_num,
 sum(sb_sales) as sb_sales,
 sum(total_sales_for_sb) as total_sales_for_sb,
 sum(sb_sales)/nullifzero(sum(total_sales_for_sb)) as pct_sb_sales
 from sbrands_fq
 group by 1,2,3
 )with data primary index (half_num,chnl) on commit preserve rows;

 create multiset volatile table sbrands_fy as (
 select
 chnl,
 year_num,
 sum(sb_sales) as sb_sales,
 sum(sb_sales)/nullifzero(sum(total_sales_for_sb)) as pct_sb_sales
 from sbrands_fh
 group by 1,2
 )with data primary index (year_num,chnl) on commit preserve rows;


 create multiset volatile table pre_merch as (
 --calculations taken from code for "MFP E2E - Combo" tableau datasource and a dashboard
 --https://git.jwn.app/TM01310/APP06792-merch-analytics-insight-framework/-/blob/development/pypeline_sql/mfp_report/pra_pipeline_prod_main_mfp_banner_country_channel.sql
 --https://tableau.nordstrom.com/#/site/AS/views/MFPE2ETemplate/MFPE2ETemplate?:iid=1
 -- for definitions of these metrics see this:
 -- https://nordstrom.sharepoint.com/sites/MFPOnlineGuide/SitePages/Metrics.aspx?OR=Teams-HL&CT=1671121261728&clickparams=eyJBcHBOYW1lIjoiVGVhbXMtRGVza3RvcCIsIkFwcFZlcnNpb24iOiIyNy8yMjExMzAwNDEwMCIsIkhhc0ZlZGVyYXRlZFVzZXIiOmZhbHNlfQ%3D%3D
 select
 'JWN' as banner,
 -- case when banner = 'NORDSTROM RACK' then 'OP'
 --      when banner = 'NORDSTROM' then 'FP'
 --     else null end as banner, --change this to be by box eventually
 dc.week_num,
 dc.month_num,
 case when dc.month_num in (202401,202402) then 'qtd_q1'
     when dc.month_num in (202404,202405) then 'qtd_q2'
     when dc.month_num in (202407,202408) then 'qtd_q3'
     when dc.month_num in (202410,202411) then 'qtd_q4'
     else null end as qtd_num,
 dc.quarter_num,
 dc.halfyear_num,
 dc.year_num,
 case when month_num = 202201 then 0.785546
     when month_num = 202202 then 0.790326
     when month_num = 202203 then 0.791828
     when month_num = 202204 then 0.777424
     when month_num = 202205 then 0.781372
     when month_num = 202206 then 0.772917
     when month_num = 202207 then 0.775675
     when month_num = 202208 then 0.753012
     when month_num = 202209 then 0.728916
     when month_num = 202210 then 0.743329
     when month_num = 202211 then 0.735565
     when month_num = 202212 then 0.744048
     when month_num = 202301 then 0.745101 else null end as CAD_conv,
 case when week_454_num = 1 and month_num = 202301 then 34912771
     when week_454_num = 1 and month_num = 202302 then 9070807
     when week_454_num = 1 and month_num = 202303 then 1763254
     when week_454_num = 1 and month_num = 202304 then 745788
     when week_454_num = 1 and month_num = 202305 then -4141405
     when week_454_num = 1 and month_num = 202306 then -1752272
     else 0 end as merch_margin_topside, --change this to be by box eventually
 --add these manually from NMG dashboard
 --https://nordstrom.sharepoint.com/:x:/r/sites/NMGFinance857/_layouts/15/Doc.aspx?sourcedoc=%7B028F1114-7C8C-4937-9732-6384C25BB39E%7D&file=NMG%20Dashboard.xlsx&wdLOR=c7C61032A-3D88-0A4B-9128-8B1EF040449D&action=default&mobileredirect=true&cid=6095e284-7cbc-4675-881b-68b3d5b81485
 case when month_num < (select distinct month_num from prd_nap_usr_vws.day_cal where day_date = current_date) then 1 else 0 end as ptd_flag, --period to date flag
 --sum(TY_GROSS_MARGIN_RETAIL_AMT) as TY_GROSS_MARGIN_RETAIL_AMT,
 sum(case when country = 'CA' then TY_MERCH_MARGIN_AMT*CAD_conv else TY_MERCH_MARGIN_AMT end) as TY_MERCH_MARGIN_RETAIL_AMT,
 sum(case when country = 'CA' then TY_NET_SALES_RETAIL_AMT*CAD_conv else TY_NET_SALES_RETAIL_AMT end) as TY_NET_SALES_RETAIL_AMT,
 sum(case when fulfill_type_desc <> 'DROPSHIP' and country = 'CA' then TY_NET_SALES_COST_AMT*0.745101
     when fulfill_type_desc <> 'DROPSHIP' then TY_NET_SALES_COST_AMT else 0 end) as TY_NET_SALES_COST_AMT_no_ds,
 --used for turn calculation- the denominator (eop_cost) already excludes DS
 sum(case when country = 'CA' then TY_EOP_TOT_COST_AMT*CAD_conv else TY_EOP_TOT_COST_AMT end) as TY_EOP_TOT_COST_AMT,
 sum(case when week_454_num = 1 and country = 'CA' then TY_BOP_TOT_COST_AMT*CAD_conv
     when week_454_num = 1 then TY_BOP_TOT_COST_AMT else 0 end) as TY_BOP_TOT_COST_AMT_m,
 sum(case when week_454_num = 1 and country = 'CA' and month_454_num in (1,4,7,10) then TY_BOP_TOT_COST_AMT*CAD_conv
     when week_454_num = 1 and month_454_num in (1,4,7,10) then TY_BOP_TOT_COST_AMT else 0 end) as TY_BOP_TOT_COST_AMT_q,
 sum(case when week_454_num = 1 and country = 'CA' and month_454_num in (1,7) then TY_BOP_TOT_COST_AMT*CAD_conv
     when week_454_num = 1 and month_454_num in (1,7) then TY_BOP_TOT_COST_AMT else 0 end) as TY_BOP_TOT_COST_AMT_h,
 sum(case when week_454_num = 1 and country = 'CA' and month_454_num in (1) then TY_BOP_TOT_COST_AMT*CAD_conv
     when week_454_num = 1 and month_454_num in (1) then TY_BOP_TOT_COST_AMT else 0 end) as TY_BOP_TOT_COST_AMT_y,
 --add BOP for only first week of month to average with the complete weekly EOP values
 --this is what the merch team (Megan Smersh, Theresa Lafrombois) do
 -- sum(TY_INV_MVMT_IN_TOT_UNITS) as TY_INV_MVMT_IN_TOT_UNITS,
 -- sum(TY_RCPTS_TOT_UNITS) as TY_RCPTS_TOT_UNITS,
 -- sum(TY_NET_SALES_UNITS) as TY_NET_SALES_UNITS,
 -- sum(TY_BOP_TOT_UNITS) as TY_BOP_TOT_UNITS,
 --sum(OP_GROSS_MARGIN_RETAIL_AMT) as OP_GROSS_MARGIN_RETAIL_AMT,
 sum(case when country = 'CA' then OP_MERCH_MARGIN_AMT*CAD_conv else OP_MERCH_MARGIN_AMT end) as OP_MERCH_MARGIN_RETAIL_AMT,
 sum(case when country = 'CA' then OP_NET_SALES_RETAIL_AMT*CAD_conv else OP_NET_SALES_RETAIL_AMT end) as OP_NET_SALES_RETAIL_AMT,
 sum(case when fulfill_type_desc <> 'DROPSHIP' and country = 'CA' then OP_NET_SALES_COST_AMT*CAD_conv
     when fulfill_type_desc <> 'DROPSHIP' then OP_NET_SALES_COST_AMT else 0 end) as OP_NET_SALES_COST_AMT_no_ds,
 sum(case when country = 'CA' then OP_EOP_TOT_COST_AMT*CAD_conv else OP_EOP_TOT_COST_AMT end) as OP_EOP_TOT_COST_AMT,
 sum(case when week_454_num = 1 and country = 'CA' then OP_BOP_TOT_COST_AMT*CAD_conv
     when week_454_num = 1 then OP_BOP_TOT_COST_AMT else 0 end) as OP_BOP_TOT_COST_AMT_m,
 sum(case when week_454_num = 1 and country = 'CA' and month_454_num in (1,4,7,10) then OP_BOP_TOT_COST_AMT*CAD_conv
     when week_454_num = 1  and month_454_num in (1,4,7,10) then OP_BOP_TOT_COST_AMT else 0 end) as OP_BOP_TOT_COST_AMT_q,
 sum(case when week_454_num = 1 and country = 'CA' and month_454_num in (1,7) then OP_BOP_TOT_COST_AMT*CAD_conv
     when week_454_num = 1  and month_454_num in (1,7) then OP_BOP_TOT_COST_AMT else 0 end) as OP_BOP_TOT_COST_AMT_h,
 sum(case when week_454_num = 1 and country = 'CA' and month_454_num in (1) then OP_BOP_TOT_COST_AMT*CAD_conv
     when week_454_num = 1  and month_454_num in (1) then OP_BOP_TOT_COST_AMT else 0 end) as OP_BOP_TOT_COST_AMT_y
 --see note above
 -- sum(OP_INV_MVMT_IN_TOT_UNITS) as OP_INV_MVMT_IN_TOT_UNITS,
 -- sum(OP_RCPTS_TOT_UNITS) as OP_RCPTS_TOT_UNITS,
 -- sum(OP_NET_SALES_UNITS) as OP_NET_SALES_UNITS,
 -- sum(OP_BOP_TOT_UNITS) as OP_BOP_TOT_UNITS
 from T2DL_DAS_ACE_MFP.MFP_BANNER_COUNTRY_CHANNEL_STG as mfp
 left join (select distinct week_454_num, month_454_num, week_num, month_num, quarter_num, halfyear_num, year_num from PRD_NAP_USR_VWS.DAY_CAL) as dc
     on mfp.week_num = dc.week_num
 where (country = 'US' or (country = 'CA' and mfp.week_num <= 202304))
 and month_num in (select month_num from month_range where year_num >= 2023)
 and division <> '600, ALTERNATE MODELS'
 group by 1,2,3,4,5,6,7,8,9
     UNION ALL
 select
 case when banner = 'NORDSTROM RACK' then 'OP'
      when banner = 'NORDSTROM' then 'FP'
      else null end as banner, --change this to be by box eventually
 dc.week_num,
 dc.month_num,
 case when dc.month_num in (202401,202402) then 'qtd_q1'
     when dc.month_num in (202404,202405) then 'qtd_q2'
     when dc.month_num in (202407,202408) then 'qtd_q3'
     when dc.month_num in (202410,202411) then 'qtd_q4'
     else null end as qtd_num,
 dc.quarter_num,
 dc.halfyear_num,
 dc.year_num,
 case when month_num = 202301 then 0.745101 else null end as CAD_conv,
-- will need to change topsides below for FP/OP banners
 case when week_454_num = 1 and month_num = 202301 then 34912771
     when week_454_num = 1 and month_num = 202302 then 9070807
     when week_454_num = 1 and month_num = 202303 then 1763254
     when week_454_num = 1 and month_num = 202304 then 745788
     when week_454_num = 1 and month_num = 202305 then -4141405
     when week_454_num = 1 and month_num = 202306 then -1752272
     else 0 end as merch_margin_topside, --change this to be by box eventually
 --add these manually from NMG dashboard
 --https://nordstrom.sharepoint.com/:x:/r/sites/NMGFinance857/_layouts/15/Doc.aspx?sourcedoc=%7B028F1114-7C8C-4937-9732-6384C25BB39E%7D&file=NMG%20Dashboard.xlsx&wdLOR=c7C61032A-3D88-0A4B-9128-8B1EF040449D&action=default&mobileredirect=true&cid=6095e284-7cbc-4675-881b-68b3d5b81485
 case when month_num < (select distinct month_num from prd_nap_usr_vws.day_cal where day_date = current_date) then 1 else 0 end as ptd_flag, --period to date flag
 --sum(TY_GROSS_MARGIN_RETAIL_AMT) as TY_GROSS_MARGIN_RETAIL_AMT,
 sum(case when country = 'CA' then TY_MERCH_MARGIN_AMT*CAD_conv else TY_MERCH_MARGIN_AMT end) as TY_MERCH_MARGIN_RETAIL_AMT,
 sum(case when country = 'CA' then TY_NET_SALES_RETAIL_AMT*CAD_conv else TY_NET_SALES_RETAIL_AMT end) as TY_NET_SALES_RETAIL_AMT,
 sum(case when fulfill_type_desc <> 'DROPSHIP' and country = 'CA' then TY_NET_SALES_COST_AMT*0.745101
     when fulfill_type_desc <> 'DROPSHIP' then TY_NET_SALES_COST_AMT else 0 end) as TY_NET_SALES_COST_AMT_no_ds,
 --used for turn calculation- the denominator (eop_cost) already excludes DS
 sum(case when country = 'CA' then TY_EOP_TOT_COST_AMT*CAD_conv else TY_EOP_TOT_COST_AMT end) as TY_EOP_TOT_COST_AMT,
 sum(case when week_454_num = 1 and country = 'CA' then TY_BOP_TOT_COST_AMT*CAD_conv
     when week_454_num = 1 then TY_BOP_TOT_COST_AMT else 0 end) as TY_BOP_TOT_COST_AMT_m,
 sum(case when week_454_num = 1 and country = 'CA' and month_454_num in (1,4,7,10) then TY_BOP_TOT_COST_AMT*CAD_conv
     when week_454_num = 1 and month_454_num in (1,4,7,10) then TY_BOP_TOT_COST_AMT else 0 end) as TY_BOP_TOT_COST_AMT_q,
 sum(case when week_454_num = 1 and country = 'CA' and month_454_num in (1,7) then TY_BOP_TOT_COST_AMT*CAD_conv
     when week_454_num = 1 and month_454_num in (1,7) then TY_BOP_TOT_COST_AMT else 0 end) as TY_BOP_TOT_COST_AMT_h,
 sum(case when week_454_num = 1 and country = 'CA' and month_454_num in (1) then TY_BOP_TOT_COST_AMT*CAD_conv
     when week_454_num = 1 and month_454_num in (1) then TY_BOP_TOT_COST_AMT else 0 end) as TY_BOP_TOT_COST_AMT_y,
 --add BOP for only first week of month to average with the complete weekly EOP values
 --this is what the merch team (Megan Smersh, Theresa Lafrombois) do
 -- sum(TY_INV_MVMT_IN_TOT_UNITS) as TY_INV_MVMT_IN_TOT_UNITS,
 -- sum(TY_RCPTS_TOT_UNITS) as TY_RCPTS_TOT_UNITS,
 -- sum(TY_NET_SALES_UNITS) as TY_NET_SALES_UNITS,
 -- sum(TY_BOP_TOT_UNITS) as TY_BOP_TOT_UNITS,
 --sum(OP_GROSS_MARGIN_RETAIL_AMT) as OP_GROSS_MARGIN_RETAIL_AMT,
 sum(case when country = 'CA' then OP_MERCH_MARGIN_AMT*CAD_conv else OP_MERCH_MARGIN_AMT end) as OP_MERCH_MARGIN_RETAIL_AMT,
 sum(case when country = 'CA' then OP_NET_SALES_RETAIL_AMT*CAD_conv else OP_NET_SALES_RETAIL_AMT end) as OP_NET_SALES_RETAIL_AMT,
 sum(case when fulfill_type_desc <> 'DROPSHIP' and country = 'CA' then OP_NET_SALES_COST_AMT*CAD_conv
     when fulfill_type_desc <> 'DROPSHIP' then OP_NET_SALES_COST_AMT else 0 end) as OP_NET_SALES_COST_AMT_no_ds,
 sum(case when country = 'CA' then OP_EOP_TOT_COST_AMT*CAD_conv else OP_EOP_TOT_COST_AMT end) as OP_EOP_TOT_COST_AMT,
 sum(case when week_454_num = 1 and country = 'CA' then OP_BOP_TOT_COST_AMT*CAD_conv
     when week_454_num = 1 then OP_BOP_TOT_COST_AMT else 0 end) as OP_BOP_TOT_COST_AMT_m,
 sum(case when week_454_num = 1 and country = 'CA' and month_454_num in (1,4,7,10) then OP_BOP_TOT_COST_AMT*CAD_conv
     when week_454_num = 1  and month_454_num in (1,4,7,10) then OP_BOP_TOT_COST_AMT else 0 end) as OP_BOP_TOT_COST_AMT_q,
 sum(case when week_454_num = 1 and country = 'CA' and month_454_num in (1,7) then OP_BOP_TOT_COST_AMT*CAD_conv
     when week_454_num = 1  and month_454_num in (1,7) then OP_BOP_TOT_COST_AMT else 0 end) as OP_BOP_TOT_COST_AMT_h,
 sum(case when week_454_num = 1 and country = 'CA' and month_454_num in (1) then OP_BOP_TOT_COST_AMT*CAD_conv
     when week_454_num = 1  and month_454_num in (1) then OP_BOP_TOT_COST_AMT else 0 end) as OP_BOP_TOT_COST_AMT_y
 --see note above
 -- sum(OP_INV_MVMT_IN_TOT_UNITS) as OP_INV_MVMT_IN_TOT_UNITS,
 -- sum(OP_RCPTS_TOT_UNITS) as OP_RCPTS_TOT_UNITS,
 -- sum(OP_NET_SALES_UNITS) as OP_NET_SALES_UNITS,
 -- sum(OP_BOP_TOT_UNITS) as OP_BOP_TOT_UNITS
 from T2DL_DAS_ACE_MFP.MFP_BANNER_COUNTRY_CHANNEL_STG as mfp
 left join (select distinct week_454_num, month_454_num, week_num, month_num, quarter_num, halfyear_num, year_num from PRD_NAP_USR_VWS.DAY_CAL) as dc
     on mfp.week_num = dc.week_num
 where (country = 'US' or (country = 'CA' and mfp.week_num <= 202304))
 and month_num in (select month_num from month_range where year_num >= 2023)
 and division <> '600, ALTERNATE MODELS'
 group by 1,2,3,4,5,6,7,8,9
 )with data primary index (month_num,banner) on commit preserve rows;


 --aggregating to month/quarter/half has to happen in additional queries to correctly aggregate EOP metric
 create multiset volatile table merch_fm as (
 --validated vs https://tableau.nordstrom.com/#/site/AS/views/MFPE2ETemplate/MFPE2ETemplate?:iid=1
 --validated vs feb 2023 data in et sharepoint
 --special calculations here to get weighted JWN totals
 select
 banner, month_num, quarter_num, halfyear_num, year_num,
 nullif
     (sum(case when ptd_flag = 1 then TY_MERCH_MARGIN_RETAIL_AMT+merch_margin_topside else 0 end)/
     nullif(sum(TY_EOP_TOT_COST_AMT + TY_BOP_TOT_COST_AMT_m)/((count(TY_EOP_TOT_COST_AMT))+1),0)
     ,0) as MMROI,
 sum(case when ptd_flag = 1 then TY_MERCH_MARGIN_RETAIL_AMT+merch_margin_topside else 0 end)/
     nullif(sum(case when ptd_flag = 1 then TY_NET_SALES_RETAIL_AMT else 0 end),0) as merch_margin,
 nullif
     (sum(case when ptd_flag = 1 then TY_NET_SALES_COST_AMT_no_ds else 0 end)/
     nullif(sum(TY_EOP_TOT_COST_AMT + TY_BOP_TOT_COST_AMT_m)/((count(TY_EOP_TOT_COST_AMT))+1),0)
     ,0) as turn,
 sum(OP_NET_SALES_COST_AMT_no_ds)/
     nullif(sum(OP_EOP_TOT_COST_AMT + OP_BOP_TOT_COST_AMT_m)/((count(OP_EOP_TOT_COST_AMT))+1),0) as turn_plan
 from pre_merch
 group by 1,2,3,4,5
 )with data primary index (month_num,banner) on commit preserve rows;


 create multiset volatile table merch_fq as (
 select
 banner, quarter_num, halfyear_num, year_num,
 nullif
     (sum(case when ptd_flag = 1 then TY_MERCH_MARGIN_RETAIL_AMT+merch_margin_topside else 0 end)/
     nullif(sum(case when ptd_flag = 1 then TY_EOP_TOT_COST_AMT + TY_BOP_TOT_COST_AMT_q else null end)/
     ((count(case when ptd_flag = 1 then TY_EOP_TOT_COST_AMT else null end))+1),0)
     ,0) as MMROI,
 sum(case when ptd_flag = 1 then TY_MERCH_MARGIN_RETAIL_AMT+merch_margin_topside else 0 end)/
     nullif(sum(case when ptd_flag = 1 then TY_NET_SALES_RETAIL_AMT else 0 end),0) as merch_margin,
 nullif
     (sum(case when ptd_flag = 1 then TY_NET_SALES_COST_AMT_no_ds else 0 end)/
     nullif(sum(case when ptd_flag = 1 then TY_EOP_TOT_COST_AMT + TY_BOP_TOT_COST_AMT_q else null end)/
     ((count(case when ptd_flag = 1 then TY_EOP_TOT_COST_AMT else null end))+1),0)
     ,0) as turn,
 sum(OP_NET_SALES_COST_AMT_no_ds)/
     nullif(sum(OP_EOP_TOT_COST_AMT + OP_BOP_TOT_COST_AMT_q)/((count(OP_EOP_TOT_COST_AMT))+1),0) as turn_plan
 from pre_merch
 group by 1,2,3,4
 )with data primary index (quarter_num,banner) on commit preserve rows;


 create multiset volatile table merch_qtd as (
 select
 banner, qtd_num, halfyear_num, year_num,
 nullif
     (sum(case when ptd_flag = 1 then TY_MERCH_MARGIN_RETAIL_AMT+merch_margin_topside else 0 end)/
     nullif(sum(case when ptd_flag = 1 then TY_EOP_TOT_COST_AMT + TY_BOP_TOT_COST_AMT_q else null end)/
     ((count(case when ptd_flag = 1 then TY_EOP_TOT_COST_AMT else null end))+1),0)
     ,0) as MMROI,
 sum(case when ptd_flag = 1 then TY_MERCH_MARGIN_RETAIL_AMT+merch_margin_topside else 0 end)/
     nullif(sum(case when ptd_flag = 1 then TY_NET_SALES_RETAIL_AMT else 0 end),0) as merch_margin,
 nullif
     (sum(case when ptd_flag = 1 then TY_NET_SALES_COST_AMT_no_ds else 0 end)/
     nullif(sum(case when ptd_flag = 1 then TY_EOP_TOT_COST_AMT + TY_BOP_TOT_COST_AMT_q else null end)/
     ((count(case when ptd_flag = 1 then TY_EOP_TOT_COST_AMT else null end))+1),0)
     ,0) as turn,
 sum(OP_NET_SALES_COST_AMT_no_ds)/
     nullif(sum(OP_EOP_TOT_COST_AMT + OP_BOP_TOT_COST_AMT_q)/((count(OP_EOP_TOT_COST_AMT))+1),0) as turn_plan
 from pre_merch
 where qtd_num is not null
 group by 1,2,3,4
 )with data primary index (qtd_num,banner) on commit preserve rows;


 create multiset volatile table merch_fh as (
 select
 banner, halfyear_num as half_num, year_num,
 nullif
     (sum(case when ptd_flag = 1 then TY_MERCH_MARGIN_RETAIL_AMT+merch_margin_topside else 0 end)/
     nullif(sum(case when ptd_flag = 1 then TY_EOP_TOT_COST_AMT + TY_BOP_TOT_COST_AMT_h else null end)/
     ((count(case when ptd_flag = 1 then TY_EOP_TOT_COST_AMT else null end))+1),0)
     ,0) as MMROI,
 sum(case when ptd_flag = 1 then TY_MERCH_MARGIN_RETAIL_AMT+merch_margin_topside else 0 end)/
     nullif(sum(case when ptd_flag = 1 then TY_NET_SALES_RETAIL_AMT else 0 end),0) as merch_margin,
 nullif
     (sum(case when ptd_flag = 1 then TY_NET_SALES_COST_AMT_no_ds else 0 end)/
     nullif(sum(case when ptd_flag = 1 then TY_EOP_TOT_COST_AMT + TY_BOP_TOT_COST_AMT_h else null end)/
     ((count(case when ptd_flag = 1 then TY_EOP_TOT_COST_AMT else null end))+1),0)
     ,0) as turn,
 sum(OP_NET_SALES_COST_AMT_no_ds)/
     nullif(sum(OP_EOP_TOT_COST_AMT + OP_BOP_TOT_COST_AMT_h)/((count(OP_EOP_TOT_COST_AMT))+1),0) as turn_plan
 from pre_merch
 group by 1,2,3
 )with data primary index (half_num,banner) on commit preserve rows;


 create multiset volatile table merch_fy as (
 select
 banner, year_num,
 -- adding calc in case statement to pull LY and YTD dynamically
 case when year_num = (select max(fiscal_year_num)-1 from month_range) then 'ly' else 'ytd' end as period_type,
 nullif
     (sum(case when ptd_flag = 1 then TY_MERCH_MARGIN_RETAIL_AMT+merch_margin_topside else 0 end)/
     nullif(sum(case when ptd_flag = 1 then TY_EOP_TOT_COST_AMT + TY_BOP_TOT_COST_AMT_y else null end)/
     ((count(case when ptd_flag = 1 then TY_EOP_TOT_COST_AMT else null end))+1),0)
     ,0) as MMROI,
 sum(case when ptd_flag = 1 then TY_MERCH_MARGIN_RETAIL_AMT+merch_margin_topside else 0 end)/
     nullif(sum(case when ptd_flag = 1 then TY_NET_SALES_RETAIL_AMT else 0 end),0) as merch_margin,
 nullif
     (sum(case when ptd_flag = 1 then TY_NET_SALES_COST_AMT_no_ds else 0 end)/
     nullif(sum(case when ptd_flag = 1 then TY_EOP_TOT_COST_AMT + TY_BOP_TOT_COST_AMT_y else null end)/
     ((count(case when ptd_flag = 1 then TY_EOP_TOT_COST_AMT else null end))+1),0)
     ,0) as turn,
 sum(OP_NET_SALES_COST_AMT_no_ds)/
     nullif(sum(OP_EOP_TOT_COST_AMT + OP_BOP_TOT_COST_AMT_y)/((count(OP_EOP_TOT_COST_AMT))+1),0) as turn_plan
 from pre_merch
 where ptd_flag = 1
 group by 1,2,3
     UNION ALL
 select
 banner, year_num,
 'ty' as period_type,
 null as MMROI,
 null as merch_margin,
 null turn,
 sum(OP_NET_SALES_COST_AMT_no_ds)/
     nullif(sum(OP_EOP_TOT_COST_AMT + OP_BOP_TOT_COST_AMT_y)/((count(OP_EOP_TOT_COST_AMT))+1),0) as turn_plan
 from pre_merch
 --pulling only the most recent fiscal year's ty actuals
 where year_num = (select max(fiscal_year_num) from month_range)
 group by 1,2
 )with data primary index (year_num,banner) on commit preserve rows;


 create multiset volatile table rcpt as (
 select
 mr.month_num,
 quarter_idnt,
 case when mr.month_num in (202401,202402) then 'qtd_q1'
     when mr.month_num in (202404,202405) then 'qtd_q2'
     when mr.month_num in (202407,202408) then 'qtd_q3'
     when mr.month_num in (202410,202411) then 'qtd_q4'
     else null end as qtd_num,
 half_num,
 fiscal_year_num,
 'JWN' as banner,
 sum(case when store_type_code in ('FL', 'RK') then receipts_po_units_ty end) as rcpts_store,
 sum(case when store_type_code in ('FC', 'OF', 'OC') then receipts_po_units_ty end) as rcpts_FC,
 sum(case when store_type_code in ('FC', 'OF', 'OC', 'FL', 'RK') then receipts_po_units_ty end) as rcpts_total
 from PRD_NAP_VWS.MERCH_TRANSACTION_SBCLASS_STORE_WEEK_AGG_FACT_VW as rcpt
 left join PRD_NAP_USR_VWS.STORE_DIM as sd
     on rcpt.store_num = sd.store_num
 inner join month_range as mr
     on rcpt.month_num = mr.month_num
 where 1=1
 --and rcpt.month_num < (select distinct month_num from prd_nap_usr_vws.day_cal where day_date = current_date)
 and rcpt.store_country_code = 'US'
 group by 1,2,3,4,5,5
     UNION ALL
 --add future months as well for later join to plan data
 select
 mr.month_num,
 quarter_idnt,
 case when mr.month_num in (202401,202402) then 'qtd_q1'
     when mr.month_num in (202404,202405) then 'qtd_q2'
     when mr.month_num in (202407,202408) then 'qtd_q3'
     when mr.month_num in (202410,202411) then 'qtd_q4'
     else null end as qtd_num,
 fiscal_halfyear_num as half_num,
 fiscal_year_num,
 'JWN' as banner,
 null as rcpts_store,
 null as rcpts_FC,
 null as rcpts_total
 from T2DL_DAS_APT.DEPT_CHNL_SPLIT_INTENTIONAL as rcpt
 inner join month_range as mr
     on rcpt.mth_idnt = mr.month_num
 where rcpt.mth_idnt > (select distinct month_num from prd_nap_usr_vws.day_cal where day_date = current_date)
 and chnl_idnt in (110,210,120,250)
 group by 1,2,3,4,5,6
 )with data primary index (month_num, banner) on commit preserve rows;


 create multiset volatile table rcpt_plan as (
 select
 mr.month_num,
 quarter_idnt,
 case when mr.month_num in (202401,202402) then 'qtd_q1'
     when mr.month_num in (202404,202405) then 'qtd_q2'
     when mr.month_num in (202407,202408) then 'qtd_q3'
     when mr.month_num in (202410,202411) then 'qtd_q4'
     else null end as qtd_num,
 fiscal_halfyear_num as half_num,
 fiscal_year_num,
 'JWN' as banner,
 1.00*sum(case when chnl_idnt in (110, 210) then plan_rcpt_u end) as rcpts_store_plan,
 1.00*sum(case when chnl_idnt in (120, 250) then plan_rcpt_u end) as rcpts_FC_plan,
 1.00*sum(plan_rcpt_u) as rcpts_total_plan
 from T2DL_DAS_APT.DEPT_CHNL_SPLIT_INTENTIONAL as rcpt
 inner join month_range as mr
     on rcpt.mth_idnt = mr.month_num
 where (rcpt.mth_idnt = rcpt.plan_cycle) or rcpt.mth_idnt >= (select distinct month_num from prd_nap_usr_vws.day_cal where day_date = current_date)
 and chnl_idnt in (110,210,120,250)
 group by 1,2,3,4,5,6
 )with data primary index (month_num, banner) on commit preserve rows;


 create multiset volatile table rcpt_fm as (
 select
 rcpt.month_num,
 'JWN' as banner,
 rcpts_FC/(rcpts_total*1.000) as rcpt_split_FC,
 rcpts_store/(rcpts_total*1.000) as rcpt_split_store,
 rcpts_FC_plan/(rcpts_total_plan*1.000) as rcpt_split_FC_plan,
 rcpts_store_plan/(rcpts_total_plan*1.000) as rcpt_split_store_plan
 from rcpt
 left join rcpt_plan on rcpt.month_num = rcpt_plan.month_num
 )with data primary index (month_num, banner) on commit preserve rows;

 create multiset volatile table rcpt_fq as (
 select
 rcpt.quarter_idnt,
 'JWN' as banner,
 sum(rcpts_FC)/(sum(rcpts_total)*1.000) as rcpt_split_FC,
 sum(rcpts_store)/(sum(rcpts_total)*1.000) as rcpt_split_store,
 sum(rcpts_FC_plan)/(sum(rcpts_total_plan)*1.000) as rcpt_split_FC_plan,
 sum(rcpts_store_plan)/(sum(rcpts_total_plan)*1.000) as rcpt_split_store_plan
 from rcpt
 left join rcpt_plan on rcpt.month_num = rcpt_plan.month_num
 group by 1,2
 )with data primary index (quarter_idnt, banner) on commit preserve rows;

 create multiset volatile table rcpt_qtd as (
 select
 rcpt.qtd_num,
 'JWN' as banner,
 sum(rcpts_FC)/(sum(rcpts_total)*1.000) as rcpt_split_FC,
 sum(rcpts_store)/(sum(rcpts_total)*1.000) as rcpt_split_store,
 sum(rcpts_FC_plan)/(sum(rcpts_total_plan)*1.000) as rcpt_split_FC_plan,
 sum(rcpts_store_plan)/(sum(rcpts_total_plan)*1.000) as rcpt_split_store_plan
 from rcpt
 left join rcpt_plan on rcpt.month_num = rcpt_plan.month_num
 group by 1,2
 )with data primary index (qtd_num, banner) on commit preserve rows;

 create multiset volatile table rcpt_fh as (
 select
 rcpt.half_num,
 'JWN' as banner,
 sum(rcpts_FC)/(sum(rcpts_total)*1.000) as rcpt_split_FC,
 sum(rcpts_store)/(sum(rcpts_total)*1.000) as rcpt_split_store,
 sum(rcpts_FC_plan)/(sum(rcpts_total_plan)*1.000) as rcpt_split_FC_plan,
 sum(rcpts_store_plan)/(sum(rcpts_total_plan)*1.000) as rcpt_split_store_plan
 from rcpt
 left join rcpt_plan on rcpt.month_num = rcpt_plan.month_num
 group by 1,2
 )with data primary index (half_num, banner) on commit preserve rows;

 create multiset volatile table rcpt_fy as (
 select
 rcpt.fiscal_year_num,
 'JWN' as banner,
 case when rcpt.fiscal_year_num = (select max(fiscal_year_num)-1 from month_range) then 'ly' else 'ytd' end as period_type,
 sum(rcpts_FC)/(sum(rcpts_total)*1.000) as rcpt_split_FC,
 sum(rcpts_store)/(sum(rcpts_total)*1.000) as rcpt_split_store,
 sum(rcpts_FC_plan)/(sum(rcpts_total_plan)*1.000) as rcpt_split_FC_plan,
 sum(rcpts_store_plan)/(sum(rcpts_total_plan)*1.000) as rcpt_split_store_plan
 from rcpt
 left join rcpt_plan on rcpt.month_num = rcpt_plan.month_num
 where rcpt.month_num < (select distinct month_num from prd_nap_usr_vws.day_cal where day_date = current_date)
 group by 1,2,3
     UNION ALL
 select
 rcpt.fiscal_year_num,
 'JWN' as banner,
 'ty' as period_type,
 null as rcpt_split_FC,
 null as rcpt_split_store,
 sum(rcpts_FC_plan)/(sum(rcpts_total_plan)*1.000) as rcpt_split_FC_plan,
 sum(rcpts_store_plan)/(sum(rcpts_total_plan)*1.000) as rcpt_split_store_plan
 from rcpt
 left join rcpt_plan on rcpt.month_num = rcpt_plan.month_num
 --pulling only the most recent fiscal year's ty actuals
 where rcpt.fiscal_year_num = (select max(fiscal_year_num) from month_range)
 group by 1,2,3
 )with data primary index (fiscal_year_num, banner) on commit preserve rows;


------------------- Merch Net Sales Addition -------------------
CREATE MULTISET VOLATILE TABLE pre_merch_net_sales AS
(SELECT
   mr.month_num AS month_num,
   mr.quarter_idnt,
   CASE WHEN mr.month_num in (202401,202402) then 'qtd_q1'
        WHEN mr.month_num in (202404,202405) then 'qtd_q2'
        WHEN mr.month_num in (202407,202408) then 'qtd_q3'
        WHEN mr.month_num in (202410,202411) then 'qtd_q4'
        else null
   end as qtd_num,
   mr.fiscal_halfyear_num as half_num,
   mr.fiscal_year_num,
   'JWN' as banner,
   sum(mfp.TY_NET_SALES_RETAIL_AMT) as merch_net_sales,
   sum(mfp.OP_NET_SALES_RETAIL_AMT) as merch_net_sales_plan
FROM PRD_NAP_VWS.MFP_COST_PLAN_ACTUAL_CHANNEL_FACT mfp
JOIN PRD_NAP_VWS.ORG_CHANNEL_DIM ocd
   ON (mfp.CHANNEL_NUM = ocd.CHANNEL_NUM)
JOIN PRD_NAP_VWS.DEPARTMENT_DIM dd
   ON (mfp.DEPT_NUM= dd.DEPT_NUM)
JOIN PRD_NAP_VWS.MERCH_WEEK_CAL_454_VW mwc
   ON (mfp.WEEK_NUM = mwc.week_idnt)
JOIN month_range mr
   ON mr.month_num = mwc.month_idnt
WHERE ocd.BANNER_COUNTRY_NUM IN (1, 3)
   AND dd.DIVISION_NUM IN (10,12,14,20,30,40,45,51,53,54,55,60,70,90,91,92,93,94,95,96,97,99,100,120,200,310,340,345,351,360,365,444,700,900,980,990)
   and month_num in (select distinct month_num from month_range) --
   --< (select distinct month_num from prd_nap_usr_vws.day_cal where day_date = current_date)
GROUP BY 1,2,3,4,5,6
)WITH DATA PRIMARY INDEX (month_num,quarter_idnt,fiscal_year_num,banner) ON COMMIT PRESERVE ROWS;


create multiset volatile table merch_net_sales_fm as (
 select
 pmns.month_num,
 banner,
 merch_net_sales,
 merch_net_sales_plan
 from pre_merch_net_sales pmns
 )with data primary index (month_num, banner) on commit preserve rows;
----- begin below, weird duplication of quarter values?

create multiset volatile table merch_net_sales_fq as (
 select
 pmns.quarter_idnt,
 banner,
 sum(merch_net_sales) as merch_net_sales,
 sum(merch_net_sales_plan) as merch_net_sales_plan
 from pre_merch_net_sales pmns
 group by 1,2
 )with data primary index (quarter_idnt, banner) on commit preserve rows;

 create multiset volatile table merch_net_sales_qtd as (
 select
 pmns.qtd_num,
 banner,
 sum(merch_net_sales) as merch_net_sales,
 sum(merch_net_sales_plan) as merch_net_sales_plan
 from pre_merch_net_sales pmns
 group by 1,2
 )with data primary index (qtd_num, banner) on commit preserve rows;

 create multiset volatile table merch_net_sales_fh as (
 select
 pmns.half_num,
 banner,
 sum(merch_net_sales) as merch_net_sales,
 sum(merch_net_sales_plan) as merch_net_sales_plan
 from pre_merch_net_sales pmns
 group by 1,2
 )with data primary index (half_num, banner) on commit preserve rows;

 create multiset volatile table merch_net_sales_fy as (
 select
 pmns.fiscal_year_num,
 banner,
 case when pmns.fiscal_year_num = (select max(fiscal_year_num)-1 from month_range) then 'ly' else 'ytd' end as period_type,
 sum(merch_net_sales) as merch_net_sales,
 sum(merch_net_sales_plan) as merch_net_sales_plan
 from pre_merch_net_sales pmns
 where pmns.month_num < (select distinct month_num from prd_nap_usr_vws.day_cal where day_date = current_date)
 group by 1,2,3
     UNION ALL
 select
 pmns.fiscal_year_num,
 banner,
 'ty' as period_type,
 sum(merch_net_sales) as merch_net_sales,
 sum(merch_net_sales_plan) as merch_net_sales_plan
 from pre_merch_net_sales pmns
 --pulling only the most recent fiscal year's ty actuals
 where pmns.fiscal_year_num = (select max(fiscal_year_num) from month_range)
 group by 1,2,3
 )with data primary index (fiscal_year_num, banner) on commit preserve rows;

-- eop is a snapshot metric, so data is measured as the last weekly value of a given time period
-- for example, Q3 EOP Units is defined as EOP units during the last week of Q3
CREATE MULTISET VOLATILE TABLE pre_eop AS
(select
     'JWN' as banner,
     -- case when banner = 'NORDSTROM RACK' then 'OP'
     --      when banner = 'NORDSTROM' then 'FP'
     --     else null end as banner, --change this to be by box eventually
     dc.month_idnt month_num,
     dc.fiscal_month_num,
     dc.month_end_week_idnt as month_end_week_idnt,
     case when dc.month_idnt in (202402) then 'qtd_q1'
         when dc.month_idnt in (202405) then 'qtd_q2'
         when dc.month_idnt in (202408) then 'qtd_q3'
         when dc.month_idnt in (202411) then 'qtd_q4'
         else null end as qtd_num,
     dc.week_idnt as week_num,
     dc.quarter_idnt as quarter_num,
     dc.fiscal_quarter_num as fiscal_quarter_num,
     dc.quarter_end_week_idnt as quarter_end_week_idnt,
     dc.fiscal_halfyear_num as halfyear_num,
     dc.fiscal_year_num as fiscal_year_num,
     case when month_idnt < (select distinct month_num from prd_nap_usr_vws.day_cal where day_date = current_date) then 1 else 0 end as ptd_flag,
     sum(case when mfp.week_num = dc.month_end_week_idnt then TY_EOP_TOT_COST_AMT else 0 end) as eop_inv_cost,
     sum(case when mfp.week_num = dc.month_end_week_idnt then TY_EOP_TOT_UNITS else 0 end) as eop_inv_units,
     sum(case when mfp.week_num = dc.month_end_week_idnt then OP_EOP_TOT_COST_AMT else 0 end) as eop_inv_cost_plan,
     sum(case when mfp.week_num = dc.month_end_week_idnt then OP_EOP_TOT_UNITS else 0 end) as eop_inv_units_plan
from T2DL_DAS_ACE_MFP.MFP_BANNER_COUNTRY_CHANNEL_STG as mfp
left join (select distinct fiscal_week_num, fiscal_month_num,
                           month_end_week_idnt, week_idnt, month_idnt,
                           quarter_idnt, fiscal_quarter_num, quarter_end_week_idnt,
                           fiscal_halfyear_num, fiscal_year_num from PRD_NAP_USR_VWS.DAY_CAL_454_DIM) as dc
     on mfp.week_num = dc.week_idnt
 where (country = 'US' or (country = 'CA' and mfp.week_num <= 202304))
 and month_idnt in (select month_num from month_range where fiscal_year_num >= 2023)
 and division <> '600, ALTERNATE MODELS'
 group by 1,2,3,4,5,6,7,8,9,10,11,12
)WITH DATA PRIMARY INDEX (month_num, banner) ON COMMIT PRESERVE ROWS;

create multiset volatile table eop_fm as (
  select
  pe.month_num,
  banner,
  sum(eop_inv_cost) as eop_inv_cost,
  sum(eop_inv_units) as eop_inv_units,
  sum(eop_inv_cost_plan) as eop_inv_cost_plan,
  sum(eop_inv_units_plan) as eop_inv_units_plan
  from pre_eop pe
  group by 1,2
  )with data primary index (month_num, banner) on commit preserve rows;
----- begin below, weird duplication of quarter values?

create multiset volatile table eop_qtd as (
  select distinct
  banner,
  qtd_num,
  --looking for the last weekly EOP value for the 2nd month of each quarter
  sum(case when week_num = month_end_week_idnt and qtd_num is not null then eop_inv_cost else 0 end) as eop_inv_cost,
  sum(case when week_num = month_end_week_idnt and qtd_num is not null then eop_inv_units else 0 end) as eop_inv_units,
  sum(case when week_num = month_end_week_idnt and qtd_num is not null then eop_inv_cost_plan else 0 end) as eop_inv_cost_plan,
  sum(case when week_num = month_end_week_idnt and qtd_num is not null then eop_inv_units_plan else 0 end) as eop_inv_units_plan
  from pre_eop pe
  group by 1,2
)with data primary index (qtd_num, banner) on commit preserve rows;

create multiset volatile table eop_fq as (
  select distinct
  banner,
  quarter_num,
  quarter_end_week_idnt,
  --looking for the last weekly eop value of each quarter
  --summing eop when week_num is the last week of the quarter, else 0
  sum(case when week_num = quarter_end_week_idnt then eop_inv_cost else 0 end) as eop_inv_cost,
  sum(case when week_num = quarter_end_week_idnt then eop_inv_units else 0 end) as eop_inv_units,
  sum(case when week_num = quarter_end_week_idnt then eop_inv_cost_plan else 0 end) as eop_inv_cost_plan,
  sum(case when week_num = quarter_end_week_idnt then eop_inv_units_plan else 0 end) as eop_inv_units_plan
  from pre_eop pe
  group by 1,2,3
)with data primary index (quarter_num, banner) on commit preserve rows;

create multiset volatile table eop_fh as (
  select distinct
  banner,
  halfyear_num as half_num,
  fiscal_year_num,
  --summing EOP when week_num is the last week of the quarter AND it is Q2 or Q4
  --we do this because H1 & H2 EOP is the EOP value during the last week of the half
  --and the last week of Q2 = last week of H1 and last week of Q4 = last week of H2
  sum(case when week_num = quarter_end_week_idnt and fiscal_quarter_num in (2,4)
           then eop_inv_cost else 0 end) as eop_inv_cost,
  sum(case when week_num = quarter_end_week_idnt and fiscal_quarter_num in (2,4)
           then eop_inv_units else 0 end) as eop_inv_units,
  sum(case when week_num = quarter_end_week_idnt and fiscal_quarter_num in (2,4)
           then eop_inv_cost_plan else 0 end) as eop_inv_cost_plan,
  sum(case when week_num = quarter_end_week_idnt and fiscal_quarter_num in (2,4)
           then eop_inv_units_plan else 0 end) as eop_inv_units_plan
  from pre_eop pe
  group by 1,2,3
)with data primary index (half_num, banner) on commit preserve rows;

create multiset volatile table eop_fy as (
select
fiscal_year_num,
banner,
cast('ly' as VARCHAR(10)) as period_type,
--ly EOP is given by the EOP values during the last week_num of the prior year
--taking the last week of the year in our sum statements, and specifying LY in where clause
sum(case when week_num = quarter_end_week_idnt and fiscal_quarter_num = 4
         then eop_inv_cost else 0 end) as eop_inv_cost,
sum(case when week_num = quarter_end_week_idnt and fiscal_quarter_num = 4
         then eop_inv_units else 0 end) as eop_inv_units,
sum(case when week_num = quarter_end_week_idnt and fiscal_quarter_num = 4
         then eop_inv_cost_plan else 0 end) as eop_inv_cost_plan,
sum(case when week_num = quarter_end_week_idnt and fiscal_quarter_num = 4
         then eop_inv_units_plan else 0 end) as eop_inv_units_plan
from pre_eop pe
where pe.month_num = (select max(month_num) from month_range
                      where fiscal_year_num = (select min(fiscal_year_num) from month_range))
group by 1,2,3
  UNION ALL
select
fiscal_year_num,
banner,
cast('ytd' as VARCHAR(10)) as period_type,
--ytd EOP is just EOP from the most recently completed month
sum(case when week_num = month_end_week_idnt then eop_inv_cost else 0 end) as eop_inv_cost,
sum(case when week_num = month_end_week_idnt then eop_inv_units else 0 end) as eop_inv_units,
sum(case when week_num = month_end_week_idnt then eop_inv_cost_plan else 0 end) as eop_inv_cost_plan,
sum(case when week_num = month_end_week_idnt then eop_inv_units_plan else 0 end) as eop_inv_units_plan
from pre_eop pe
where pe.month_num = (select max(month_num) from eop_fm where (eop_inv_cost > 0 and eop_inv_units > 0))
group by 1,2,3
  UNION ALL
select
fiscal_year_num,
banner,
cast('ty' as VARCHAR(10)) as period_type,
--ty EOP PLAN is given by the EOP plan for the last week_num of the current year
--taking the last week of the year in our sum statements, and specifying TY in where clause
--TY EOP actuals should be blank until we reach the last month of the year
sum(case when week_num = quarter_end_week_idnt and fiscal_quarter_num = 4
         then eop_inv_cost else 0 end) as eop_inv_cost,
sum(case when week_num = quarter_end_week_idnt and fiscal_quarter_num = 4
         then eop_inv_units else 0 end) as eop_inv_units,
sum(case when week_num = quarter_end_week_idnt and fiscal_quarter_num = 4
         then eop_inv_cost_plan else 0 end) as eop_inv_cost_plan,
sum(case when week_num = quarter_end_week_idnt and fiscal_quarter_num = 4
         then eop_inv_units_plan else 0 end) as eop_inv_units_plan
from pre_eop pe
where pe.month_num = (select max(month_num) from month_range
                      where fiscal_year_num = (select max(fiscal_year_num) from month_range))
group by 1,2,3
  )with data primary index (fiscal_year_num, banner) on commit preserve rows;

 create multiset volatile table et_fm as (
 select
 cust_fm.box,
 'fm' as period_type,
 cust_fm.month_num as period_value,
 total_trips,
 total_customer,
 new_customer,
 trips_per_cust,
 p50_fm as p50,
 sb_sales,
 pct_sb_sales,
 MMROI,
 merch_margin,
 turn,
 null as sellthru,
 turn_plan,
 null as sellthru_plan,
 rcpt_split_FC,
 rcpt_split_store,
 rcpt_split_FC_plan,
 rcpt_split_store_plan,
 merch_net_sales,
 merch_net_sales_plan,
 eop_inv_cost,
 eop_inv_units,
 eop_inv_cost_plan,
 eop_inv_units_plan
 from cust_fm
 left join p50_fm on cust_fm.month_num = p50_fm.fm and cust_fm.box = p50_fm.channel
 left join sbrands_fm on cust_fm.month_num = sbrands_fm.month_num and cust_fm.box = sbrands_fm.chnl
 left join merch_fm on cust_fm.month_num = merch_fm.month_num and cust_fm.box = merch_fm.banner
 left join rcpt_fm on cust_fm.month_num = rcpt_fm.month_num and cust_fm.box = rcpt_fm.banner
 left join merch_net_sales_fm on cust_fm.month_num = merch_net_sales_fm.month_num and cust_fm.box = merch_net_sales_fm.banner
 left join eop_fm on cust_fm.month_num = eop_fm.month_num and cust_fm.box = eop_fm.banner
 )with data primary index (period_value,box) on commit preserve rows;

 create multiset volatile table et_qtd as (
 select
 cust_fq.box,
 'qtd' as period_type,
 cust_fq.quarter_num as period_value,
 total_trips,
 total_customer,
 new_customer,
 trips_per_cust,
 p50_fq as p50,
 sb_sales,
 pct_sb_sales,
 MMROI,
 merch_margin,
 turn,
 null as sellthru,
 turn_plan,
 null as sellthru_plan,
 rcpt_split_FC,
 rcpt_split_store,
 rcpt_split_FC_plan,
 rcpt_split_store_plan,
 merch_net_sales,
 merch_net_sales_plan,
 eop_inv_cost,
 eop_inv_units,
 eop_inv_cost_plan,
 eop_inv_units_plan
 from cust_fq
 left join p50_qtd on cust_fq.quarter_num = p50_qtd.qtd and cust_fq.box = p50_qtd.channel
 left join sbrands_qtd on cust_fq.quarter_num = sbrands_qtd.qtd_num and cust_fq.box = sbrands_qtd.chnl
 left join merch_qtd on cust_fq.quarter_num = merch_qtd.qtd_num and cust_fq.box = merch_qtd.banner
 left join rcpt_qtd on cust_fq.quarter_num = rcpt_qtd.qtd_num and cust_fq.box = rcpt_qtd.banner
 left join merch_net_sales_qtd on cust_fq.quarter_num = merch_net_sales_qtd.qtd_num and cust_fq.box = merch_net_sales_qtd.banner
 left join eop_qtd on cust_fq.quarter_num = eop_qtd.qtd_num and cust_fq.box = eop_qtd.banner
 where cust_fq.quarter_num like 'qtd%'
 )with data primary index (period_value,box) on commit preserve rows;

 create multiset volatile table et_fq as (
 select
 cust_fq.box,
 'fq' as period_type,
 cust_fq.quarter_num as period_value,
 total_trips,
 total_customer,
 new_customer,
 trips_per_cust,
 p50_fq as p50,
 sb_sales,
 pct_sb_sales,
 MMROI,
 merch_margin,
 turn,
 null as sellthru,
 turn_plan,
 null as sellthru_plan,
 rcpt_split_FC,
 rcpt_split_store,
 rcpt_split_FC_plan,
 rcpt_split_store_plan,
 merch_net_sales,
 merch_net_sales_plan,
 eop_inv_cost,
 eop_inv_units,
 eop_inv_cost_plan,
 eop_inv_units_plan
 from cust_fq
 left join p50_fq on cust_fq.quarter_num = p50_fq.Quarter and cust_fq.box = p50_fq.channel
 left join sbrands_fq on cust_fq.quarter_num = sbrands_fq.quarter_num and cust_fq.box = sbrands_fq.chnl
 left join merch_fq on cust_fq.quarter_num = merch_fq.quarter_num and cust_fq.box = merch_fq.banner
 left join rcpt_fq on cust_fq.quarter_num = rcpt_fq.quarter_idnt and cust_fq.box = rcpt_fq.banner
 left join merch_net_sales_fq on cust_fq.quarter_num = merch_net_sales_fq.quarter_idnt and cust_fq.box = merch_net_sales_fq.banner
 left join eop_fq on cust_fq.quarter_num = eop_fq.quarter_num and cust_fq.box = eop_fq.banner
 where cust_fq.quarter_num not like 'qtd%'
 )with data primary index (period_value,box) on commit preserve rows;


 create multiset volatile table et_fh as (
 select
 cust_fh.box,
 'fh' as period_type,
 cust_fh.half_num as period_value,
 total_trips,
 total_customer,
 new_customer,
 trips_per_cust,
 p50_fh as p50,
 sb_sales,
 pct_sb_sales,
 MMROI,
 merch_margin,
 turn,
 null as sellthru,
 turn_plan,
 null as sellthru_plan,
 rcpt_split_FC,
 rcpt_split_store,
 rcpt_split_FC_plan,
 rcpt_split_store_plan,
 merch_net_sales,
 merch_net_sales_plan,
 eop_inv_cost,
 eop_inv_units,
 eop_inv_cost_plan,
 eop_inv_units_plan
 from cust_fh
 left join p50_fh on cust_fh.half_num = p50_fh.half_num and cust_fh.box = p50_fh.channel
 left join sbrands_fh on cust_fh.half_num = sbrands_fh.half_num and cust_fh.box = sbrands_fh.chnl
 left join merch_fh on cust_fh.half_num = merch_fh.half_num and cust_fh.box = merch_fh.banner
 left join rcpt_fh on cust_fh.half_num = rcpt_fh.half_num and cust_fh.box = rcpt_fh.banner
 left join merch_net_sales_fh on cust_fh.half_num = merch_net_sales_fh.half_num and cust_fh.box = merch_net_sales_fh.banner
 left join eop_fh on cust_fh.half_num = eop_fh.half_num and cust_fh.box = eop_fh.banner
 )with data primary index (period_value,box) on commit preserve rows;


 create multiset volatile table et_fy as (
 select
 cust_fy.box,
 cust_fy.period_type,
 cust_fy.year_num as period_value,
 total_trips,
 total_customer,
 new_customer,
 trips_per_cust,
 p50_fy as p50,
 sb_sales,
 pct_sb_sales,
 MMROI,
 merch_margin,
 turn,
 null as sellthru,
 turn_plan,
 null as sellthru_plan,
 rcpt_split_FC,
 rcpt_split_store,
 rcpt_split_FC_plan,
 rcpt_split_store_plan,
 merch_net_sales,
 merch_net_sales_plan,
 eop_inv_cost,
 eop_inv_units,
 eop_inv_cost_plan,
 eop_inv_units_plan
 from cust_fy
 left join p50_fy on cust_fy.year_num = p50_fy.FISCAL_YEAR and cust_fy.box = p50_fy.channel and cust_fy.period_type <> 'ty'
 left join sbrands_fy on cust_fy.year_num = sbrands_fy.year_num and cust_fy.box = sbrands_fy.chnl and cust_fy.period_type <> 'ty'
 left join merch_fy on cust_fy.year_num = merch_fy.year_num and cust_fy.box = merch_fy.banner and cust_fy.period_type = merch_fy.period_type
 left join rcpt_fy on cust_fy.year_num = rcpt_fy.fiscal_year_num and cust_fy.box = rcpt_fy.banner and cust_fy.period_type = rcpt_fy.period_type
 left join merch_net_sales_fy on cust_fy.year_num = merch_net_sales_fy.fiscal_year_num and cust_fy.box = merch_net_sales_fy.banner and cust_fy.period_type = merch_net_sales_fy.period_type
 left join eop_fy on cust_fy.year_num = eop_fy.fiscal_year_num and cust_fy.box = eop_fy.banner and cust_fy.period_type = eop_fy.period_type
 )with data primary index (period_value,box) on commit preserve rows;


 create multiset volatile table manual_plan as (
 select
 box,
 case when time_period = 'Total Year' then 'ty'
     when time_period = 'YTD' then 'ytd'
     when time_period in ('Q1', 'Q2', 'Q3', 'Q4') then 'fq'
     when time_period in ('QTD Q1 Feb-Mar', 'QTD Q2 May-Jun', 'QTD Q3 Aug-Sep', 'QTD Q4 Nov-Dec') then 'qtd'
     when time_period in ('H1', 'H2') then 'fh'
     else 'fm' end as period_type,
 case when time_period in ('YTD', 'Total Year') then cast(cast(fiscal_year as int) as varchar(20))
     when time_period in ('H1', 'H2') then cast(cast(concat(fiscal_year, oreplace(time_period, 'H', '')) as int) as varchar(20))
     when time_period in ('Q1', 'Q2', 'Q3', 'Q4') then cast(cast(concat(fiscal_year, oreplace(time_period, 'Q', '')) as int) as varchar(20))
     when time_period = 'jan' then cast(cast(concat(fiscal_year,'12') as int) as varchar(20))
     when time_period = 'dec' then cast(cast(concat(fiscal_year,'11') as int) as varchar(20))
     when time_period = 'nov' then cast(cast(concat(fiscal_year,'10') as int) as varchar(20))
     when time_period = 'oct' then cast(cast(concat(fiscal_year,'09') as int) as varchar(20))
     when time_period = 'sep' then cast(cast(concat(fiscal_year,'08') as int) as varchar(20))
     when time_period = 'aug' then cast(cast(concat(fiscal_year,'07') as int) as varchar(20))
     when time_period = 'jul' then cast(cast(concat(fiscal_year,'06') as int) as varchar(20))
     when time_period = 'jun' then cast(cast(concat(fiscal_year,'05') as int) as varchar(20))
     when time_period = 'may' then cast(cast(concat(fiscal_year,'04') as int) as varchar(20))
     when time_period = 'apr' then cast(cast(concat(fiscal_year,'03') as int) as varchar(20))
     when time_period = 'mar' then cast(cast(concat(fiscal_year,'02') as int) as varchar(20))
     when time_period = 'feb' then cast(cast(concat(fiscal_year,'01') as int) as varchar(20))
     when time_period like 'QTD Q1 Feb-%' then 'qtd_q1'
     when time_period like 'QTD Q2 May-%' then 'qtd_q2'
     when time_period like 'QTD Q3 Aug-%' then 'qtd_q3'
     when time_period like 'QTD Q4 Nov-%' then 'qtd_q4'
     else null end as period_value,
 case when metric like 'Variable Fulfillment Rate of Sales%' then 'fulfillment_ros_plan'
     when metric like 'Variable Fulfillment Cost per Unit%' then 'fulfillment_cpu_plan'
     when metric = 'Strategic Brand Sales % (Rack Stores)' then 'sb_sales_plan'
     when metric = 'Strategic Brand Sales % (Rack)' then 'pct_sb_sales_plan'
     when metric = 'Budget Management (Cash excl Bonus)' then 'budget_mgmt_plan'
     when metric = 'Business Value Delivered' then 'bus_value_plan'
     when metric = 'Capitalization Rate (Labor)' then 'cap_rate_plan'
     when metric = 'Liquidity' then 'liquidity_plan'
     when metric = 'Leverage' then 'leverage_plan'
     when metric = 'Corporate OH Labor' then 'oh_labor_plan'
     when metric = 'Compliance (HR & Legal)' then 'compliance_plan'
     when metric = 'Merch Margin % (Cost)' then 'merch_margin_plan'
     when metric = 'MMROI (Cost)' then 'MMROI_plan'
     when metric = 'New Customer Count (Rack)' then 'new_customer_plan'
     when metric like 'Customer Count%' then 'total_customer_plan'
     when metric like 'Customer Trips%' then 'total_trips_plan'
     when metric like 'Customer Spend%' then 'total_spend_plan'
     when metric = 'Headcount' then 'headcount_plan'
     when metric = 'Credit EBIT (excl losses)' then 'credit_ebit_pct_plan'
     when metric = 'Safety Injury Rate' then 'sfty_inj_rate_plan'
     when metric = 'Beauty Selling Cost' then 'beauty_selling_cost_plan'
     when metric like 'Disaster Recovery%' then 'disaster_rec_testing_plan'
     when metric = 'Legal Budget' then 'legal_budget_plan'
     when metric = 'Avg VASN to Store Receive (ex NPG, Nike, HI/AK) Days' then 'avg_vasn_to_store_plan'
     when metric = 'Top Items NMS Coverage' then 'top_items_nms_plan'
     when metric = 'Supply Chain (FC/DC) Labor Productivity' then 'sup_chain_labor_prod_plan'
     when metric = 'Shrinkage %' then 'shrinkage_pct_plan'
     when metric = 'Corporate Overhead Labor $' then 'corp_oh_labor_plan'
     when metric = '% of Stores Executing Weekly RFID Counts' then 'pct_stores_rfid_count_plan'
     when metric = 'Technology-enabled Sales/EBIT Improvements' then 'tech_sales_ebit_plan'
     when metric = 'NAP Data Discoverability ' then 'data_discoverability_plan'
     when metric = 'Store Shrink % (cost)' then 'store_shrink_cost_plan'
     when metric = 'FC/DC Shrink % (cost)' then 'fc_shrink_pct_plan'
     when metric = 'Technology Spend/Budget' then 'tech_budget_plan'
     when metric = 'op_gmv' then 'op_gmv_plan'
     when metric = 'comp_gmv' then 'comp_gmv_plan'
     when metric = 'Marketplace Op GMV' then 'marketplace_opgmv_plan'
     when metric = 'new_store_op_gmv' then 'new_store_op_gmv_plan'
     when metric = 'nyc_op_gmv' then 'nyc_op_gmv_plan'
     when metric = 'Customer Retention Rate' then 'retention_rate_plan'
     when metric = 'Free Cash Flow' then 'cash_flow_plan'
     else metric end as feature_name,
 plan as feature_value
 from {mothership_t2_schema}.ets_manual
 where box is not null
 and feature_value is not null
 )with data primary index (period_type, period_value, feature_name) on commit preserve rows;


 create multiset volatile table pre_insert as (
 SELECT box, cast(period_type as varchar(6)) as period_type, period_value, feature_name, feature_value
 from et_fm UNPIVOT(feature_value FOR feature_name IN (
         total_trips,
         total_customer,
         new_customer,
         trips_per_cust,
         p50,
         sb_sales,
         pct_sb_sales,
         MMROI,
         merch_margin,
         turn,
         turn_plan,
         sellthru_plan,
         rcpt_split_FC,
         rcpt_split_store,
         rcpt_split_FC_plan,
         rcpt_split_store_plan,
         merch_net_sales,
         merch_net_sales_plan,
         eop_inv_cost,
         eop_inv_units,
         eop_inv_cost_plan,
         eop_inv_units_plan
 )) as temp_et_fm_pivot
     UNION ALL
 SELECT box, period_type, period_value, feature_name, feature_value
 from et_fy UNPIVOT(feature_value FOR feature_name IN (
         total_trips,
         total_customer,
         new_customer,
         trips_per_cust,
         p50,
         sb_sales,
         pct_sb_sales,
         MMROI,
         merch_margin,
         turn,
         turn_plan,
         sellthru_plan,
         rcpt_split_FC,
         rcpt_split_store,
         rcpt_split_FC_plan,
         rcpt_split_store_plan,
         merch_net_sales,
         merch_net_sales_plan,
         eop_inv_cost,
         eop_inv_units,
         eop_inv_cost_plan,
         eop_inv_units_plan
 )) as temp_et_fy_pivot
     UNION ALL
 SELECT box, period_type, period_value, feature_name, feature_value
 from et_fh UNPIVOT(feature_value FOR feature_name IN (
         total_trips,
         total_customer,
         new_customer,
         trips_per_cust,
         p50,
         sb_sales,
         pct_sb_sales,
         MMROI,
         merch_margin,
         turn,
         turn_plan,
         sellthru_plan,
         rcpt_split_FC,
         rcpt_split_store,
         rcpt_split_FC_plan,
         rcpt_split_store_plan,
         merch_net_sales,
         merch_net_sales_plan,
         eop_inv_cost,
         eop_inv_units,
         eop_inv_cost_plan,
         eop_inv_units_plan
 )) as temp_et_fh_pivot
     UNION ALL
 SELECT box, period_type, period_value, feature_name, feature_value
 from et_fq UNPIVOT(feature_value FOR feature_name IN (
         total_trips,
         total_customer,
         new_customer,
         trips_per_cust,
         p50,
         sb_sales,
         pct_sb_sales,
         MMROI,
         merch_margin,
         turn,
         turn_plan,
         sellthru_plan,
         rcpt_split_FC,
         rcpt_split_store,
         rcpt_split_FC_plan,
         rcpt_split_store_plan,
         merch_net_sales,
         merch_net_sales_plan,
         eop_inv_cost,
         eop_inv_units,
         eop_inv_cost_plan,
         eop_inv_units_plan
 )) as temp_et_fq_pivot
     UNION ALL
 SELECT box, period_type, period_value, feature_name, feature_value
 from et_qtd UNPIVOT(feature_value FOR feature_name IN (
         total_trips,
         total_customer,
         new_customer,
         trips_per_cust,
         p50,
         sb_sales,
         pct_sb_sales,
         MMROI,
         merch_margin,
         turn,
         turn_plan,
         sellthru_plan,
         rcpt_split_FC,
         rcpt_split_store,
         rcpt_split_FC_plan,
         rcpt_split_store_plan,
         merch_net_sales,
         merch_net_sales_plan,
         eop_inv_cost,
         eop_inv_units,
         eop_inv_cost_plan,
         eop_inv_units_plan
 )) as temp_et_qtd_pivot
     UNION ALL
 select
 box,
 case when time_period = 'Total Year' then 'ly'
     when time_period = 'YTD' then 'ytd'
     when time_period in ('Q1', 'Q2', 'Q3', 'Q4') then 'fq'
     when time_period in ('QTD Q1 Feb-Mar', 'QTD Q2 May-Jun', 'QTD Q3 Aug-Sep', 'QTD Q4 Nov-Dec') then 'qtd'
     when time_period in ('H1', 'H2') then 'fh'
     else 'fm' end as period_type,
 case when time_period in ('YTD', 'Total Year') then cast(cast(fiscal_year as int) as varchar(20))
     when time_period in ('H1', 'H2') then cast(cast(concat(fiscal_year, oreplace(time_period, 'H', '')) as int) as varchar(20))
     when time_period in ('Q1', 'Q2', 'Q3', 'Q4') then cast(cast(concat(fiscal_year, oreplace(time_period, 'Q', '')) as int) as varchar(20))
     when time_period = 'jan' then cast(cast(concat(fiscal_year,'12') as int) as varchar(20))
     when time_period = 'dec' then cast(cast(concat(fiscal_year,'11') as int) as varchar(20))
     when time_period = 'nov' then cast(cast(concat(fiscal_year,'10') as int) as varchar(20))
     when time_period = 'oct' then cast(cast(concat(fiscal_year,'09') as int) as varchar(20))
     when time_period = 'sep' then cast(cast(concat(fiscal_year,'08') as int) as varchar(20))
     when time_period = 'aug' then cast(cast(concat(fiscal_year,'07') as int) as varchar(20))
     when time_period = 'jul' then cast(cast(concat(fiscal_year,'06') as int) as varchar(20))
     when time_period = 'jun' then cast(cast(concat(fiscal_year,'05') as int) as varchar(20))
     when time_period = 'may' then cast(cast(concat(fiscal_year,'04') as int) as varchar(20))
     when time_period = 'apr' then cast(cast(concat(fiscal_year,'03') as int) as varchar(20))
     when time_period = 'mar' then cast(cast(concat(fiscal_year,'02') as int) as varchar(20))
     when time_period = 'feb' then cast(cast(concat(fiscal_year,'01') as int) as varchar(20))
     when time_period like 'QTD Q1 Feb-%' then 'qtd_q1'
     when time_period like 'QTD Q2 May-%' then 'qtd_q2'
     when time_period like 'QTD Q3 Aug-%' then 'qtd_q3'
     when time_period like 'QTD Q4 Nov-%' then 'qtd_q4'
     else null end as period_value,
case when metric like 'Variable Fulfillment Rate of Sales%' then 'fulfillment_ros'
     when metric like 'Variable Fulfillment Cost per Unit%' then 'fulfillment_cpu'
     when metric = 'Budget Management (Cash excl Bonus)' then 'budget_mgmt'
     when metric = 'Business Value Delivered' then 'bus_value'
     when metric = 'Capitalization Rate (Labor)' then 'cap_rate'
     when metric = 'Liquidity' then 'liquidity'
     when metric = 'Leverage' then 'leverage'
     when metric = 'Corporate OH Labor' then 'oh_labor'
     when metric = 'Compliance (HR & Legal)' then 'compliance'
     when metric = 'Reg. Price Sell-thru' then 'sellthru'
     when metric = 'Headcount' then 'headcount'
     when metric = 'Credit EBIT (excl losses)' then 'credit_ebit_pct'
     when metric = 'Safety Injury Rate' then 'sfty_inj_rate'
     when metric = 'Beauty Selling Cost' then 'beauty_selling_cost'
     when metric like 'Disaster Recovery%' then 'disaster_rec_testing'
     when metric = 'Legal Budget' then 'legal_budget'
     when metric = 'Avg VASN to Store Receive (ex NPG, Nike, HI/AK) Days' then 'avg_vasn_to_store'
     when metric = 'Top Items NMS Coverage' then 'top_items_nms'
     when metric = 'Supply Chain (FC/DC) Labor Productivity' then 'sup_chain_labor_prod'
     when metric = 'Shrinkage %' then 'shrinkage_pct'
     when metric = 'Corporate Overhead Labor $' then 'corp_oh_labor'
     when metric = '% of Stores Executing Weekly RFID Counts' then 'pct_stores_rfid_count'
     when metric = 'Technology-enabled Sales/EBIT Improvements' then 'tech_sales_ebit'
     when metric = 'NAP Data Discoverability ' then 'data_discoverability'
     when metric = 'Store Shrink % (cost)' then 'store_shrink_cost'
     when metric = 'FC/DC Shrink % (cost)' then 'fc_shrink_pct'
     when metric = 'Technology Spend/Budget' then 'tech_budget'
     when metric = 'op_gmv' then 'op_gmv'
     when metric = 'comp_gmv' then 'comp_gmv'
     when metric = 'Marketplace Op GMV' then 'marketplace_opgmv'
     when metric = 'new_store_op_gmv' then 'new_store_op_gmv'
     when metric = 'nyc_op_gmv' then 'nyc_op_gmv'
     when metric = 'Customer Retention Rate' then 'retention_rate'
     when metric = 'Free Cash Flow' then 'cash_flow' 
     else metric end as feature_name,
 actuals as feature_value
 from {mothership_t2_schema}.ets_manual
 where box is not null
 and feature_value is not null
     UNION ALL
 select
 box,period_type,period_value,feature_name,feature_value
 from manual_plan
     UNION ALL
 select
 t1.box,
 t1.period_type,
 t1.period_value,
 'trips_per_cust_plan' as feature_name,
 t1.feature_value/t2.feature_value as feature_value
 from manual_plan as t1
 left join manual_plan as t2
     on t1.box = t2.box and t1.period_value = t2.period_value  and t1.period_type = t2.period_type
 where t1.feature_name = 'total_trips_plan' and t2.feature_name = 'total_customer_plan'
     UNION ALL
 select
 t1.box,
 t1.period_type,
 t1.period_value,
 'spend_per_cust_plan' as feature_name,
 t1.feature_value/t2.feature_value as feature_value
 from manual_plan as t1
 left join manual_plan as t2
     on t1.box = t2.box and t1.period_value = t2.period_value  and t1.period_type = t2.period_type
 where t1.feature_name = 'total_spend_plan' and t2.feature_name = 'total_customer_plan'
     UNION ALL
 select
 t1.box,
 t1.period_type,
 t1.period_value,
 'spend_per_trip_plan' as feature_name,
 t1.feature_value/t2.feature_value as feature_value
 from manual_plan as t1
 left join manual_plan as t2
     on t1.box = t2.box and t1.period_value = t2.period_value  and t1.period_type = t2.period_type
 where t1.feature_name = 'total_spend_plan' and t2.feature_name = 'total_trips_plan'
 )with data primary index (period_type, period_value, feature_name) on commit preserve rows;




DELETE FROM {mothership_t2_schema}.ets_primary;


--------------------------------------------------------------------
/* 
final table creation
*/
--T2DL_DAS_MOTHERSHIP.ets_primary

INSERT INTO {mothership_t2_schema}.ets_primary
--create multiset volatile table et_fy as (
SELECT box, period_type, period_value, feature_name, feature_value, CURRENT_TIMESTAMP as dw_sys_load_tmstp
from pre_insert
;

COLLECT STATISTICS COLUMN(box), COLUMN(period_type), COLUMN(period_value), COLUMN(feature_name) ON {mothership_t2_schema}.ets_primary;


SET QUERY_BAND = NONE FOR SESSION;