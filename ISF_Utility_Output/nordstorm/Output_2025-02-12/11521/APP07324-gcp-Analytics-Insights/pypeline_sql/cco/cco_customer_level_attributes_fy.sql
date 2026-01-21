SET QUERY_BAND = 'App_ID=APP08240;
     DAG_ID=cco_cust_chan_yr_attributes_fy_11521_ACE_ENG;
     Task_Name=cco_customer_level_attributes_fy;'
     FOR SESSION VOLATILE;

/************************************************************************************/
/************************************************************************************/
/************************************************************************************
 *
 * Build a Customer/Year-level table for the CCO project   
 *  (intended to be used alongisde t3dl_ace_corp.cco_line_item_table in order to:
 *    a) build any aggregated Tableau Sandboxes (for Strategy to self-serve)
 *    b) answer any more nuanced questions (that Strategy can't do with the Tableau Sandboxes)
 *
 * The Steps involved are:
 * I) Create Customer/Year-Level attributes ()
 *
 *
 ************************************************************************************/
/************************************************************************************/
/************************************************************************************/

/************************************************************************************/
/************************************************************************************
 * PART I) Create Customer/Year-Level attributes
 ************************************************************************************/
/************************************************************************************/


/************************************************************************************
 * PART I-a) Create realigned fiscal calendar (going back far enough to include 4+ years of Cohorts)
 ************************************************************************************/

/********* Step I-a-i: Going back 8 years from today, find all years with a "53rd week" *********/
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

/********* Step I-a-ii: Count the # years with a "53rd week" *********/
--drop table week_53_yr_count;
create MULTISET volatile table week_53_yr_count as (
select count(distinct year_num) year_count
from week_53_yrs x 
) with data primary index(year_count) on commit preserve rows;

/********* Step I-a-iii: Create an empty realigned calendar *********/
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

/********* Step I-a-iv: insert data into realigned calendar 
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
 * PART I-b) Set date parameters
 ************************************************************************************/

/********* Step I-b-i: Create the date-lookup table *********/
create multiset volatile table date_lookup as (
select 
     cast('FY-'||cast((a.year_num mod 2000) as varchar(2)) as varchar(8)) fiscal_year
    ,min(a.day_date) as start_dt
    ,max(a.day_date) as end_dt
    ,min(a.month_num) as start_mo
    ,max(a.month_num) as end_mo 
from realigned_calendar a 
-- where month_num between (select yr1_start_mo from month_lookup) and (select yr4_end_mo from month_lookup)
where day_date between date'{cust_year_start_date}' and date'{cust_year_end_date}' 
                     --date'2019-02-03'             and date'2023-04-29'
group by 1
)with data primary index(fiscal_year) on commit preserve rows;




/************************************************************************************
 * PART I-c) Create driver table (at a customer/year-level)
 ************************************************************************************/

--drop table customer_year_driver;
create multiset volatile table customer_year_driver as (
select a.acp_id
  ,a.fiscal_year_shopped
  ,b.start_dt
  ,b.end_dt
  ,b.start_mo
  ,b.end_mo
  ,max(a.employee_flag) employee_flag
  ,sum(a.net_sales) net_sales
from {cco_t2_schema}.cco_line_items a
  left join date_lookup b on a.fiscal_year_shopped=b.fiscal_year
group by 1,2,3,4,5,6
)with data primary index(acp_id,fiscal_year_shopped) on commit preserve rows;

/************************************************************************************
 * PART I-d) Identify customer's MARKET
 ************************************************************************************/

/********* PART I-d-i) Lookup table assigning nodes to Markets *********/

CREATE MULTISET VOLATILE TABLE consolidated_nms_nodes AS(
select distinct market
  ,cast(case when upper(trim(market))='LOS_ANGELES' then date'2019-06-20'
    when upper(trim(market))='NEW_YORK' then date'2019-10-23'
    when upper(trim(market))='SAN_FRANCISCO' then date'2019-10-29'
    when upper(trim(market))='CHICAGO' then date'2019-11-04'
    when upper(trim(market))='DALLAS' then date'2019-11-04'
    when upper(trim(market))='SEATTLE' then date'2020-09-28'
    when upper(trim(market))='BOSTON' then date'2020-10-05'
    when upper(trim(market))='PHILADELPHIA' then date'2020-10-05'
    when upper(trim(market))='WASHINGTON' then date'2020-10-05'
    when upper(trim(market))='TORONTO' then date'2020-10-28'
    when upper(trim(market))='DENVER' then date'2021-02-15'
    when upper(trim(market))='SAN_DIEGO' then date'2021-02-15'
    when upper(trim(market))='PORTLAND' then date'2021-02-17'
    when upper(trim(market))='AUSTIN' then date'2021-02-22'
    when upper(trim(market))='HOUSTON' then date'2021-02-22'
    when upper(trim(market))='ATLANTA' then date'2021-03-01'
    when upper(trim(market))='DETROIT' then date'2021-03-01'
    when upper(trim(market))='MIAMI' then date'2021-03-01'
    when upper(trim(market))='MINNEAPOLIS' then date'2021-03-01'
    else null end as date) nms_launch_date
  ,node_num
from
  (
  current validtime select local_market as market, cast(node as integer) as node_num
  from PRD_NAP_USR_VWS.LOCAL_MARKET_NODE_DIM
  union
  select market,node_num from t2dl_das_strategy.cco_nms_nodes
  ) x
)WITH DATA UNIQUE PRIMARY INDEX(market, node_num)
              ON COMMIT PRESERVE ROWS;

/********* PART I-d-ii) Create lookup table of customers & their markets *********/

--drop table customer_market;
CREATE MULTISET VOLATILE TABLE customer_market AS(
select distinct a.acp_id
  ,a.market
  ,case when b.market is not null then 1 else 0 end NMS_Market
  ,b.nms_launch_date
from
  (
  select distinct acp_id
    ,COALESCE(bill_zip_market,fls_market,rack_market,ca_dma_desc,us_dma_desc) market
  from
    (
      select distinct acp_id,
        case when b.market is not null then b.market end as bill_zip_market,
        case when c.market is not null then c.market end  as fls_market,
        case when d.market is not null then d.market end as rack_market,
        ca_dma_desc,
        us_dma_desc
      from
        (
         select x.acp_id, billing_postal_code, ca_dma_desc, us_dma_desc, fls_loyalty_store_num, rack_loyalty_store_num,
            case when ca_dma_desc is not null then left(billing_postal_code,3) else billing_postal_code end as join_zip
         from prd_nap_usr_vws.analytical_customer x
            join customer_year_driver y on x.acp_id=y.acp_id
         where x.acp_id is not null
        ) a
        left join (current validtime
                  select distinct local_market as market
                    ,case when local_market ='TORONTO' then left(coarse_postal_code,3) else coarse_postal_code end as join_zip
                  from  PRD_NAP_USR_VWS.LOCAL_MARKET_POSTAL_DIM
                  ) b  on a.join_zip= b.join_zip
        left join consolidated_nms_nodes c on a.fls_loyalty_store_num= c.node_num
        left join consolidated_nms_nodes d on a.rack_loyalty_store_num= d.node_num
      where a.acp_id IS NOT NULL)z
  where market is not null or market not in ('Not Defined', 'Other')
   ) a
left join
    (select distinct market, nms_launch_date
    from consolidated_nms_nodes) b
  on a.market=b.market
)WITH DATA UNIQUE PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;

/********* PART I-d-iii) Create lookup table of customers & their NMS markets *********/
CREATE MULTISET VOLATILE TABLE customer_NMS_market AS(
select distinct acp_id
  ,market
from customer_market
where NMS_Market=1
)WITH DATA UNIQUE PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;

/************************************************************************************
 * PART I-e) Identify customer's DMA (& that DMA's rank in terms of net-sales)
 ************************************************************************************/

/********* PART I-e-i) grab all customers' country & DMA *********/
create multiset volatile table customer_dma_prep as (
select a.acp_id
  ,case when b.ca_dma_desc is not null then 'CA'
        when b.us_dma_desc is not null then 'US' end as cust_country
  ,coalesce(b.ca_dma_desc, b.us_dma_desc) as cust_dma
  ,a.fiscal_year_shopped
  ,a.net_sales
from customer_year_driver a
  left join prd_nap_usr_vws.analytical_customer b on a.acp_id=b.acp_id
)with data primary index(acp_id,fiscal_year_shopped) on commit preserve rows;

/********* PART I-e-ii) rank all DMA's by net-sales *********/
create multiset volatile table dma_ranking as (
select cust_dma
  ,fiscal_year_shopped
  ,sum(net_sales) dma_net_sales
  ,RANK() OVER (PARTITION BY fiscal_year_shopped ORDER BY dma_net_sales DESC) dma_rank
from customer_dma_prep
where coalesce(cust_dma,'Other') <> 'Other'
group by 1,2
)with data primary index(cust_dma,fiscal_year_shopped) on commit preserve rows;

/********* PART I-e-iii) Join DMA-ranking back to the customer DMA & country *********/
--drop table customer_dma;
create multiset volatile table customer_dma as (
select distinct a.acp_id
  ,a.cust_dma
  ,a.cust_country
  ,a.fiscal_year_shopped
  ,case when b.dma_rank is null       then 'DMA missing'
        when b.dma_rank between  1 and  5 then '1) Top 5'
        when b.dma_rank between  6 and 10 then '2) 6-10'
        when b.dma_rank between 11 and 20 then '3) 11-20'
        when b.dma_rank between 21 and 30 then '4) 21-30'
        when b.dma_rank between 31 and 50 then '5) 31-50'
        when b.dma_rank between 51 and 100 then '6) 51-100'
                                          else '7) > 100' end dma_rank
from customer_dma_prep a
  left join dma_ranking b on a.cust_dma=b.cust_dma and a.fiscal_year_shopped=b.fiscal_year_shopped
)with data primary index(acp_id,fiscal_year_shopped) on commit preserve rows;

-- select count(*)
--   ,count(distinct acp_id||fiscal_year_shopped)
-- from customer_dma
-- ;


 /************************************************************************************
 * PART I-f) Grab all attributes DERIVED FROM LINE-ITEM TABLE
 *    (includes channel-combo,
 *              bucketed trips & spend,
 *              1/0 indicators by service, tender, & platform)
 ************************************************************************************/

create multiset volatile table anchor_brands as (
    select
    global_tran_id,
    line_item_seq_num

    from
    prd_nap_usr_vws.RETAIL_TRAN_DETAIL_FACT_VW dtl

    inner join prd_nap_usr_vws.product_sku_dim_vw ps
    on dtl.sku_num = ps.rms_sku_num
    and ps.channel_country = 'US'

    inner join
    (select distinct supplier_idnt
    from T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING.anchor_brands
    where anchor_brand_ind = 'Y') s
    on ps.prmy_supp_num = s.supplier_idnt
      
    where dtl.business_day_date >= date'{lines_start_date}'
) with data primary index(global_tran_id, line_item_seq_num) on commit preserve rows;

create multiset volatile table strategic_brands as (
    select
    global_tran_id,
    line_item_seq_num

    from
      prd_nap_usr_vws.RETAIL_TRAN_DETAIL_FACT_VW dtl

    inner join
      prd_nap_usr_vws.product_sku_dim_vw ps
      on dtl.sku_num = ps.rms_sku_num
      and ps.channel_country = 'US'

    inner join
      prd_nap_usr_vws.vendor_dim sup
      on ps.prmy_supp_num = sup.vendor_num
      
    inner join
      (
      select
      distinct supplier_name
      FROM T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING.rack_strategic_brands
      where RACK_STRATEGIC_BRAND_IND = 'Y'
      ) rsb
      on rsb.supplier_name = sup.vendor_name
      
    where dtl.business_day_date >= date'{lines_start_date}'
) with data primary index(global_tran_id, line_item_seq_num) on commit preserve rows;

COLLECT STATISTICS COLUMN (global_tran_id, line_item_seq_num) ON strategic_brands;
COLLECT STATISTICS COLUMN (global_tran_id, line_item_seq_num) ON anchor_brands;

--derived from
create volatile multiset table derived_cust_attributes as(
select distinct acp_id
  ,fiscal_year_shopped
  ,ntn_this_year
  ,channels_shopped
  ,case when NS_ind=1 and NCOM_ind=0 and RS_ind=0 and RCOM_ind=0 then '01) NordStore-only'
        when NS_ind=0 and NCOM_ind=1 and RS_ind=0 and RCOM_ind=0 then '02) N.com-only'
        when NS_ind=0 and NCOM_ind=0 and RS_ind=1 and RCOM_ind=0 then '03) RackStore-only'
        when NS_ind=0 and NCOM_ind=0 and RS_ind=0 and RCOM_ind=1 then '04) Rack.com-only'
        when NS_ind=1 and NCOM_ind=1 and RS_ind=0 and RCOM_ind=0 then '05) NordStore+N.com'
        when NS_ind=1 and NCOM_ind=0 and RS_ind=1 and RCOM_ind=0 then '06) NordStore+RackStore'
        when NS_ind=1 and NCOM_ind=0 and RS_ind=0 and RCOM_ind=1 then '07) NordStore+Rack.com'
        when NS_ind=0 and NCOM_ind=1 and RS_ind=1 and RCOM_ind=0 then '08) N.com+RackStore'
        when NS_ind=0 and NCOM_ind=1 and RS_ind=0 and RCOM_ind=1 then '09) N.com+Rack.com'
        when NS_ind=0 and NCOM_ind=0 and RS_ind=1 and RCOM_ind=1 then '10) RackStore+Rack.com'
        when NS_ind=1 and NCOM_ind=1 and RS_ind=1 and RCOM_ind=0 then '11) NordStore+N.com+RackStore'
        when NS_ind=1 and NCOM_ind=1 and RS_ind=0 and RCOM_ind=1 then '12) NordStore+N.com+Rack.com'
        when NS_ind=1 and NCOM_ind=0 and RS_ind=1 and RCOM_ind=1 then '13) NordStore+RackStore+Rack.com'
        when NS_ind=0 and NCOM_ind=1 and RS_ind=1 and RCOM_ind=1 then '14) N.com+RackStore+Rack.com'
        when NS_ind=1 and NCOM_ind=1 and RS_ind=1 and RCOM_ind=1 then '15) 4-Box'
        else   '99) Error' end chan_combo
  ,banners_shopped
  ,case when N_ind=1 and R_ind=0 then '1) Nordstrom-only'
        when N_ind=0 and R_ind=1 then '2) Rack-only'
        when N_ind=1 and R_ind=1 then '3) Dual-Banner'
        else '99) Error' end banner_combo
  ,employee_flag
  --vvv need to test this vvvv
  ,case when trips < 10 then '0'||cast(trips as varchar(1))||' trips'
        else '10+ trips' end jwn_trip_bucket
  ,case when net_sales = 0                            then '0) $0'
        when net_sales > 0   and net_sales <= 50      then '1) $0-50'
        when net_sales > 50  and net_sales <= 100     then '2) $50-100'
        when net_sales > 100  and net_sales <= 250    then '3) $100-250'
        when net_sales > 250  and net_sales <= 500    then '4) $250-500'
        when net_sales > 500  and net_sales <= 1000   then '5) $500-1K'
        when net_sales > 1000  and net_sales <= 2000  then '6) 1-2K'
        when net_sales > 2000  and net_sales <= 5000  then '7) 2-5K'
        when net_sales > 5000  and net_sales <= 10000 then '8) 5-10K'
        when net_sales > 10000                        then '9) 10K+'
        else null end jwn_net_spend_bucket
  ,case when ntn_this_year = 1 and coalesce(nonrestaurant_trips,0) <= 5 then 'Acquire & Activate'
        when coalesce(nonrestaurant_trips,0) <= 5  then 'Lightly-Engaged'
        when coalesce(nonrestaurant_trips,0) <= 13 then 'Moderately-Engaged'
        when coalesce(nonrestaurant_trips,0) >= 14 then 'Highly-Engaged'
        else null end engagement_cohort
  ,gross_sales
  ,return_amt
  ,net_sales
  ,net_sales_apparel
  ,trips
  ,gross_items
  ,return_items
  ,net_items
  ,tender_nordstrom cust_tender_nordstrom
  ,tender_nordstrom_note cust_tender_nordstrom_note
  ,tender_3rd_party_credit cust_tender_3rd_party_credit
  ,tender_debit_card cust_tender_debit_card
  ,tender_gift_card cust_tender_gift_card
  ,tender_cash cust_tender_cash
  ,tender_paypal cust_tender_paypal
  ,tender_check cust_tender_check
  --,event_ctr cust_event_ctr
  ,event_holiday cust_event_holiday
  ,event_anniversary cust_event_anniversary
  ,svc_group_exp_delivery cust_svc_group_exp_delivery
  ,svc_group_order_pickup cust_svc_group_order_pickup
  ,svc_group_selling_relation cust_svc_group_selling_relation
  ,svc_group_remote_selling cust_svc_group_remote_selling
  ,svc_group_alterations cust_svc_group_alterations
  ,svc_group_in_store cust_svc_group_in_store
  ,svc_group_restaurant cust_svc_group_restaurant
  ,service_free_exp_delivery cust_service_free_exp_delivery
  ,service_next_day_pickup cust_service_next_day_pickup
  ,service_same_day_bopus cust_service_same_day_bopus
  ,service_curbside_pickup cust_service_curbside_pickup
  ,service_style_boards cust_service_style_boards
  ,service_gift_wrapping cust_service_gift_wrapping
  ,service_pop_in cust_service_pop_in
  ,platform_desktop cust_platform_desktop
  ,platform_MOW cust_platform_MOW
  ,platform_IOS cust_platform_IOS
  ,platform_Android cust_platform_Android
  ,platform_POS cust_platform_POS
  ,anchor_brand cust_anchor_brand
  ,strategic_brand cust_strategic_brand
  ,store_customer cust_store_customer
  ,digital_customer cust_digital_customer
from
  (
  select acp_id
    ,fiscal_year_shopped
    ,count(distinct channel) channels_shopped
    ,count(distinct banner) banners_shopped
    ,max(employee_flag) employee_flag
    ,max(ntn_tran) ntn_this_year
    ,max(case when substring(channel,1,1)='1' then 1 else 0 end) NS_ind
    ,max(case when substring(channel,1,1)='2' then 1 else 0 end) NCOM_ind
    ,max(case when substring(channel,1,1)='3' then 1 else 0 end) RS_ind
    ,max(case when substring(channel,1,1)='4' then 1 else 0 end) RCOM_ind
    ,max(case when substring(channel,1,1) in ('1','2') then 1 else 0 end) N_ind
    ,max(case when substring(channel,1,1) in ('3','4') then 1 else 0 end) R_ind
    ,max(case when substring(channel,1,1) in ('1','3') then 1 else 0 end) store_customer
    ,max(case when substring(channel,1,1) in ('2','4') then 1 else 0 end) digital_customer
    ,sum(gross_sales) gross_sales
    ,sum(return_amt) return_amt
    ,sum(net_sales) net_sales
    ,sum(case when div_num = 351 then net_sales else 0 end) as net_sales_apparel
    ,count(distinct case when gross_incl_gc > 0 then acp_id||store_num||date_shopped else null end) trips
    ,count(distinct case when gross_incl_gc>0 and div_num<>70 then acp_id||store_num||date_shopped else null end) nonrestaurant_trips
    ,sum(gross_items) gross_items
    ,sum(return_items) return_items
    ,sum(gross_items - return_items) net_items
    ,max(tender_nordstrom) tender_nordstrom
    ,max(tender_nordstrom_note) tender_nordstrom_note
    ,max(tender_3rd_party_credit) tender_3rd_party_credit
    ,max(tender_debit_card) tender_debit_card
    ,max(tender_gift_card) tender_gift_card
    ,max(tender_cash) tender_cash
    ,max(tender_paypal) tender_paypal
    ,max(tender_check) tender_check
  --  ,max(event_ctr) event_ctr
    ,max(event_holiday) event_holiday
    ,max(event_anniversary) event_anniversary
    ,max(svc_group_exp_delivery) svc_group_exp_delivery
    ,max(svc_group_order_pickup) svc_group_order_pickup
    ,max(svc_group_selling_relation) svc_group_selling_relation
    ,max(svc_group_remote_selling) svc_group_remote_selling
    ,max(svc_group_alterations) svc_group_alterations
    ,max(svc_group_in_store) svc_group_in_store
    ,max(svc_group_restaurant) svc_group_restaurant
    ,max(service_free_exp_delivery) service_free_exp_delivery
    ,max(service_next_day_pickup) service_next_day_pickup
    ,max(service_same_day_bopus) service_same_day_bopus
    ,max(service_curbside_pickup) service_curbside_pickup
    ,max(service_style_boards) service_style_boards
    ,max(service_gift_wrapping) service_gift_wrapping
    ,max(service_pop_in) service_pop_in
    ,max(case when trim(platform)='WEB' then 1 else 0 end) platform_desktop
    ,max(case when trim(platform)='MOW' then 1 else 0 end) platform_MOW
    ,max(case when trim(platform)='IOS' then 1 else 0 end) platform_IOS
    ,max(case when trim(platform)='ANDROID' then 1 else 0 end) platform_Android
    ,max(case when trim(platform)='POS' then 1 else 0 end) platform_POS
    ,max(case when ab.global_tran_id is not null and substring(channel,1,1) in ('1','2') then 1 else 0 end) as anchor_brand
    ,max(case when sb.global_tran_id is not null and substring(channel,1,1) in ('3','4') then 1 else 0 end) as strategic_brand
    -- ,count(distinct order_num) online_orders
    -- ,count(distinct case when order_num is not null
    --                           and item_delivery_method not like '%BOPUS'
    --                        then global_tran_id else null end) shipments
    -- ,count(distinct case when order_num is not null
    --                           and (item_delivery_method not like '%BOPUS' or item_delivery_method like '%SHIP_TO_STORE')
    --                        then global_tran_id else null end) shipments_to_home
  from {cco_t2_schema}.cco_line_items a
      left join anchor_brands ab on a.global_tran_id = ab.global_tran_id and a.line_item_seq_num = ab.line_item_seq_num
	    left join strategic_brands sb on a.global_tran_id = sb.global_tran_id and a.line_item_seq_num = sb.line_item_seq_num
  group by 1,2
  ) x
)with data primary index(acp_id,fiscal_year_shopped) on commit preserve rows;


/************************************************************************************
 * PART I-g) Identify customer's CONTRIBUTION MARGIN & DECILE
 * 
 *  PAUSE on this for now (until we figure out how to replicate the Merch Margin deciles in CAL
 *    on other time-periods)
 ************************************************************************************/

/*
create multiset volatile table contribution_margin_deciles as (
--explain
select acp_id
  ,fiscal_year_shopped
  ,contribution_margin contribution_margin_amt
  ,1 + (RANK() OVER (PARTITION BY fiscal_year_shopped ORDER BY contribution_margin desc) - 1) * 10
          / COUNT(*) OVER (PARTITION BY fiscal_year_shopped ) contribution_margin_decile
from
  (
  select dr.acp_id
    ,dr.fiscal_year_shopped
    ,sum(a.contribution_margin) contribution_margin
  from  cco_ccm_hist a
    join customer_year_driver dr on a.acp_id=dr.acp_id
        and a.mth_idnt between dr.start_mo and dr.end_mo
  group by 1,2
  ) x
)with data primary index(acp_id,fiscal_year_shopped) on commit preserve rows;
*/



 /************************************************************************************
 * PART I-h) Identify customer's AGE
 ************************************************************************************/

 /********* PART I-h-i) Grab customers' Modeled ages
    (reduce age by the difference between age date & time-period end date)
 *********/

--drop table modeled_ages;
create multiset volatile table modeled_ages as (
select distinct dr.acp_id
  ,dr.fiscal_year_shopped
  ,cast(cast(a.model_age as decimal(6,2))
              - (cast(update_timestamp as date) -  dr.end_dt)/365.25
        as integer) model_age_adjusted
from t2dl_das_age_model.new_age_model_scoring_all a
  join customer_year_driver dr on a.acp_id=dr.acp_id
QUALIFY RANK() OVER (partition by a.acp_id ORDER BY a.update_timestamp DESC) =1
)with data primary index(acp_id,fiscal_year_shopped) on commit preserve rows;

 /********* PART I-h-ii) Join customers' Experian age & gender)
  *     (reduce age by the difference between age date & time-period end date)
 *********/

create multiset volatile table experian_demos as (
select distinct dr.acp_id
  ,dr.fiscal_year_shopped
  ,a.gender
  ,cast(case when age_type = 'Exact Age' and age_value is not null then
      case when length(trim(a.birth_year_and_month))=6 then
        (dr.end_dt - cast(to_date(substring(a.birth_year_and_month,1,4)||'/'||
                                  substring(a.birth_year_and_month,5,2)||'/'||'15'
                      ,'YYYY/MM/DD') as date))/365.25 
      else
        (cast(a.age_value as decimal(6,2)))
              - (cast(a.object_system_time as date) -  dr.end_dt)/365.25
      end
    else null end as integer) experian_age_adjusted
from prd_nap_cust_usr_vws.customer_experian_demographic_prediction_dim a
  join customer_year_driver dr on a.acp_id=dr.acp_id
)with data primary index(acp_id,fiscal_year_shopped) on commit preserve rows;

 /********* PART I-h-iii) Combine the ages: use Experian age if present, Analytics age otherwise
  *   (calculate age-ranges based on Marketing's "lifestage" segments)
 *********/

create multiset volatile table both_ages as (
--explain
select distinct x.acp_id
  ,x.fiscal_year_shopped
  ,x.gender
  ,x.age
  -- ,case when a.age< 18    then '1) < 18 yrs    '
  --       when a.age>=18 and a.age<25 then '2) 18-24 yrs   '
  --       when a.age>=25 and a.age<35 then '3) 25-34 yrs   '
  --       when a.age>=35 and a.age<45 then '4) 35-44 yrs   '
  --       when a.age>=45 and a.age<55 then '5) 45-54 yrs   '
  --       when a.age>=55 and a.age<65 then '6) 55-64 yrs   '
  --       when a.age>=65    then '7) 65+ yrs     '
  --             else 'age not modeled' end age_grp
   ,case when x.age between 14 and 22 then '01) Young Adult'
         when x.age between 23 and 29 then '02) Early Career'
         when x.age between 30 and 44 then '03) Mid Career'
         when x.age between 45 and 64 then '04) Late Career'
         when x.age >= 65 then '05) Retired'
         else 'Unknown'end lifestage
  ,CASE WHEN age < 18 THEN '0) <18 yrs'
        WHEN age >= 18 AND age <= 24 THEN '1) 18-24 yrs' 
        WHEN age > 24  AND age <= 34 THEN '2) 25-34 yrs'
        WHEN age > 34  AND age <= 44 THEN '3) 35-44 yrs'
        WHEN age > 44  AND age <= 54 THEN '4) 45-54 yrs'
        WHEN age > 54  AND age <= 64 THEN '5) 55-64 yrs'
        WHEN age > 64 THEN '6) 65+ yrs'
        ELSE 'Unknown' END as age_group
from
  (
  select distinct a.acp_id
    ,a.fiscal_year_shopped
    ,b.gender
    ,coalesce(b.experian_age_adjusted,c.model_age_adjusted) age
  from customer_year_driver a
    left join experian_demos b on a.acp_id=b.acp_id and a.fiscal_year_shopped=b.fiscal_year_shopped
    left join modeled_ages c on a.acp_id=c.acp_id and a.fiscal_year_shopped=c.fiscal_year_shopped
  ) x
)with data primary index(acp_id,fiscal_year_shopped) on commit preserve rows;

 /************************************************************************************
 * PART I-i) Identify customer's LOYALTY-related attributes: type, level, enrollment date
 ************************************************************************************/

 /********* PART I-i-i) Find Loyalty Cardmembers' highest levels (by year) *********/

/* Cardmembers */
--drop table cust_level_loyalty_cardmember;
create multiset volatile table cust_level_loyalty_cardmember as (
select lmd.acp_id
   ,dr.fiscal_year_shopped
   ,1 as flg_cardmember
   ,max(case when rwd.rewards_level in ('MEMBER') then 1
             when rwd.rewards_level in ('INSIDER','INFLUENCER') then 3
             when rwd.rewards_level in ('AMBASSADOR') then 4
             when rwd.rewards_level in ('ICON') then 5
             else 0
         end) as cardmember_level
   ,min(lmd.cardmember_enroll_date) cardmember_enroll_date
from prd_nap_usr_vws.loyalty_member_dim_vw as lmd
  join customer_year_driver dr on lmd.acp_id=dr.acp_id
  left join (select acp_id, cast(cast(max_close_dt as varchar(8)) as date FORMAT 'yyyymmdd') as max_close_dt
             from t2dl_das_strategy.cco_credit_close_dts) as ccd on lmd.acp_id = ccd.acp_id
  left join prd_nap_usr_vws.loyalty_level_lifecycle_fact_vw as rwd
      on lmd.loyalty_id = rwd.loyalty_id
     and rwd.start_day_date <= dr.end_dt
     and rwd.end_day_date > dr.end_dt
  where coalesce(lmd.cardmember_enroll_date, date'2099-12-31') <  coalesce(ccd.max_close_dt, lmd.cardmember_close_date, date'2099-12-31')
    and coalesce(lmd.cardmember_enroll_date, date'2099-12-31') <= dr.end_dt
    and coalesce(ccd.max_close_dt, lmd.cardmember_close_date, date'2099-12-31') >= dr.end_dt
    and lmd.acp_id is not null
group by 1,2,3
)with data primary index(acp_id,fiscal_year_shopped) on commit preserve rows;


 /********* PART I-i-ii) Find Loyalty Members' highest levels (by year) *********/

/* Members */
--drop table cust_level_loyalty_member;
create multiset volatile table cust_level_loyalty_member as (
select lmd.acp_id
   ,dr.fiscal_year_shopped
   ,1 as flg_member
   ,max(case when rwd.rewards_level in ('MEMBER') then 1
             when rwd.rewards_level in ('INSIDER','INFLUENCER') then 3
             when rwd.rewards_level in ('AMBASSADOR') then 4
             when rwd.rewards_level in ('ICON') then 5
             else 0
         end) as member_level
   ,min(lmd.member_enroll_date) member_enroll_date
from prd_nap_usr_vws.loyalty_member_dim_vw as lmd
  join customer_year_driver dr on lmd.acp_id=dr.acp_id
  -- left join (select acp_id, cast(cast(max_close_dt as varchar(8)) as date FORMAT 'yyyymmdd') as max_close_dt
  --            from t3dl_paymnt_loyalty.credit_close_dts) as ccd on lmd.acp_id = ccd.acp_id
  left join prd_nap_usr_vws.loyalty_level_lifecycle_fact_vw as rwd
      on lmd.loyalty_id = rwd.loyalty_id
     and rwd.start_day_date <= dr.end_dt
     and rwd.end_day_date > dr.end_dt
  where coalesce(lmd.member_enroll_date, date'2099-12-31') <  coalesce(lmd.member_close_date, date'2099-12-31')
    and coalesce(lmd.member_enroll_date, date'2099-12-31') <= dr.end_dt
    and coalesce(lmd.member_close_date, date'2099-12-31') >= dr.end_dt
    and lmd.acp_id is not null
group by 1,2,3
)with data primary index(acp_id,fiscal_year_shopped) on commit preserve rows;


/********* PART I-i-iii) Combine both types of Loyalty customers into 1 lookup table
 *    (prioritize "Cardmember" type & level if present, use "Member" otherwise)
 *  *********/


/* Combine both types of Loyalty customers into 1 lookup table */
--drop table cust_level_loyalty;
create multiset volatile table cust_level_loyalty as
(
select a.acp_id
  ,a.fiscal_year_shopped
  ,case when b.acp_id is not null then 'a) Cardmember'
        when c.acp_id is not null then 'b) Member'
        else 'c) Non-Loyalty' end as loyalty_type

  ,case --when
        when a.employee_flag=1 and b.acp_id is not null and c.acp_id is not null then '1) MEMBER'
        when b.acp_id is not null and b.cardmember_level <= 3 then '2) INFLUENCER'
        when b.acp_id is not null and b.cardmember_level  = 4 then '3) AMBASSADOR'
        when b.acp_id is not null and b.cardmember_level  = 5 then '4) ICON'
        when c.member_level <= 1 then '1) MEMBER'
        when c.member_level = 3 then '2) INFLUENCER'
        when c.member_level >= 4 then '3) AMBASSADOR'
        else null end loyalty_level
  ,c.member_enroll_date loyalty_member_start_dt
  ,b.cardmember_enroll_date loyalty_cardmember_start_dt
from customer_year_driver a
  left join cust_level_loyalty_cardmember b  on a.acp_id=b.acp_id and a.fiscal_year_shopped=b.fiscal_year_shopped
  left join cust_level_loyalty_member c on a.acp_id=c.acp_id and a.fiscal_year_shopped=c.fiscal_year_shopped
)with data primary index(acp_id,fiscal_year_shopped) on commit preserve rows;

-- sel loyalty_type
--   ,loyalty_level
--   ,count(*)
--   ,count(distinct acp_id)
-- from cust_level_loyalty
-- group by 1,2
-- order by 1,2;


/************************************************************************************
 * PART I-j) Identify customer's AQUISITION attributes:
 *    (channel & date of acquisition, bucketed tenure in years)
 ************************************************************************************/

 /********* PART I-j-i) Find Acquisition date, channel, banner, brand & tenure-years *********/
create multiset volatile table customer_acquisition_tenure_prep as (
select a.acp_id
  ,b.fiscal_year_shopped
  ,a.aare_status_date acquisition_date
  ,case when a.aare_chnl_code = 'FLS'  then '1) Nordstrom Stores'
        when a.aare_chnl_code = 'NCOM' then '2) Nordstrom.com'
        when a.aare_chnl_code = 'RACK' then '3) Rack Stores'
        when a.aare_chnl_code = 'NRHL' then '4) Rack.com'
        else null end acquisition_channel
  ,case when a.aare_chnl_code in ('FLS','NCOM')  then 'NORDSTROM'
        when a.aare_chnl_code in ('RACK','NRHL') then 'RACK'
        else null end acquisition_banner
  ,a.aare_brand_name acquisition_brand
  ,cast(floor((b.end_dt - a.aare_status_date)/365.25) as integer) tenure_years
  ,cast(floor(12*(b.end_dt - a.aare_status_date)/365.25) as integer) tenure_months
from prd_nap_usr_vws.customer_ntn_status_fact  a
  join customer_year_driver b on a.acp_id=b.acp_id
where a.aare_status_date <= b.end_dt
QUALIFY RANK() OVER (partition by a.acp_id, b.fiscal_year_shopped ORDER BY a.aare_status_date DESC) =1
) with data primary index (acp_id,fiscal_year_shopped) on commit preserve rows;

 /********* PART I-j-ii) Bucket the tenure-years *********/
--drop table customer_acquisition_tenure;
create multiset volatile table customer_acquisition_tenure as (
select distinct a.acp_id
  ,a.fiscal_year_shopped
  ,a.acquisition_date
  ,d.year_num acquisition_fiscal_year
  ,a.acquisition_channel
  ,a.acquisition_banner
  ,a.acquisition_brand
  ,a.tenure_years
  ,case when a.tenure_months < 4                then '1) 0-3 months'
        when a.tenure_months between 4 and 6    then '2) 4-6 months'
        when a.tenure_months between 7 and 12   then '3) 7-12 months'
        when a.tenure_months between 13 and 24  then '4) 13-24 months'
        when a.tenure_months between 25 and 36  then '5) 25-36 months'
        when a.tenure_months between 37 and 48  then '6) 37-48 months'
        when a.tenure_months between 49 and 60  then '7) 49-60 months'
        when a.tenure_months > 60               then '8) 61+ months'
        else 'unknown' end tenure_bucket_months
  -- ,case when a.tenure_years < 1              then '1) < 1 year'
  --       when a.tenure_years between 1 and 2  then '2) 1-2 years'
  --       when a.tenure_years between 3 and 5  then '3) 3-5 years'
  --       when a.tenure_years between 6 and 9  then '4) 6-9 years'
  --       when a.tenure_years >= 10            then '5) 10+ years'
  --       else 'unknown' end tenure_bucket_years

  ,case when a.tenure_months <= 12              then '1) <= 1 year'
        when a.tenure_months between 13 and 24  then '2) 1-2 years'
        when a.tenure_months between 25 and 60  then '3) 2-5 years'
        when a.tenure_months between 61 and 120  then '4) 5-10 years'
        when a.tenure_months > 120               then '5) 10+ years'
        else 'unknown' end tenure_bucket_years

from customer_acquisition_tenure_prep  a
  join realigned_calendar d on a.acquisition_date=d.day_date
) with data primary index (acp_id,fiscal_year_shopped) on commit preserve rows;


 /************************************************************************************
 * PART I-k) Identify customer's ACTIVATION attributes:
 *    (channel & date of activation)
 ************************************************************************************/

create multiset volatile table customer_activation as (
select a.acp_id
  ,b.fiscal_year_shopped
  ,a.activated_date activation_date
  ,case when a.activated_channel = 'FLS'  then '1) Nordstrom Stores'
        when a.activated_channel = 'NCOM' then '2) Nordstrom.com'
        when a.activated_channel = 'RACK' then '3) Rack Stores'
        when a.activated_channel = 'NRHL' then '4) Rack.com'
        else null end activation_channel
  ,case when a.activated_channel in ('FLS','NCOM')  then 'NORDSTROM'
        when a.activated_channel in ('RACK','NRHL') then 'RACK'
        else null end activation_banner
from dl_cma_cmbr.customer_activation a
  join customer_year_driver b on a.acp_id=b.acp_id
where a.activated_date <= b.end_dt
QUALIFY RANK() OVER (partition by a.acp_id, b.fiscal_year_shopped ORDER BY a.activated_date DESC) =1
) with data primary index (acp_id,fiscal_year_shopped) on commit preserve rows;


 /************************************************************************************
 * PART I-l) Identify Customer's CLV date for all available years
 *    (CLV taken from latest score in the year)
 ************************************************************************************/

create multiset volatile table clv_date as (
SELECT 
fiscal_year as fiscal_year_shopped,
max(scored_date) scored_date

from
	date_lookup dl
left join
	(
	select distinct scored_date
	from
	t2dl_das_customber_model_attribute_productionalization.customer_prediction_clv_hist
	) a
on a.scored_date >= dl.start_dt
and a.scored_date < dl.end_dt

group by 1
) with data primary index (fiscal_year_shopped) on commit preserve rows;

 /************************************************************************************
 * PART I-j) Join all customer/year-level attributes together (& insert into final table)
 ************************************************************************************/

DELETE FROM {cco_t2_schema}.cco_customer_level_attributes_fy ALL;

INSERT INTO {cco_t2_schema}.cco_customer_level_attributes_fy
select distinct a.acp_id
  ,a.fiscal_year_shopped
  ,coalesce(b.gender,'Unknown') cust_gender
  ,b.age cust_age
  ,b.lifestage cust_lifestage
  ,b.age_group cust_age_group
  ,coalesce(trim(c.market),'Z - NON-NMS') cust_NMS_market
  ,d.cust_dma
  ,d.cust_country
  ,coalesce(d.dma_rank,'DMA missing') cust_dma_rank
  ,e.loyalty_type cust_loyalty_type
  ,e.loyalty_level cust_loyalty_level
  ,e.loyalty_member_start_dt cust_loy_member_enroll_dt
  ,e.loyalty_cardmember_start_dt cust_loy_cardmember_enroll_dt
  --,f.contribution_margin_decile cust_contr_margin_decile
  --,f.contribution_margin_amt cust_contr_margin_amt
  --,i.ntn_this_year cust_acquired_this_year
  ,g.acquisition_date cust_acquisition_date
  ,g.acquisition_fiscal_year cust_acquisition_fiscal_year
  ,g.acquisition_channel cust_acquisition_channel
  ,g.acquisition_banner cust_acquisition_banner
  ,g.acquisition_brand cust_acquisition_brand
  ,g.tenure_bucket_months cust_tenure_bucket_months
  ,g.tenure_bucket_years cust_tenure_bucket_years
  ,h.activation_date cust_activation_date
  ,h.activation_channel cust_activation_channel
  ,h.activation_banner cust_activation_banner
  ,i.engagement_cohort cust_engagement_cohort
  ,i.channels_shopped cust_channel_count
  ,i.chan_combo cust_channel_combo
  ,i.banners_shopped cust_banner_count
  ,i.banner_combo cust_banner_combo
  ,i.employee_flag cust_employee_flag
  ,i.jwn_trip_bucket cust_jwn_trip_bucket
  ,i.jwn_net_spend_bucket cust_jwn_net_spend_bucket
  ,i.gross_sales cust_jwn_gross_sales
  ,i.return_amt cust_jwn_return_amt
  ,i.net_sales cust_jwn_net_sales
  ,i.net_sales_apparel cust_jwn_net_sales_apparel
  ,i.trips cust_jwn_trips
  ,i.gross_items cust_jwn_gross_items
  ,i.return_items cust_jwn_return_items
  ,i.net_items cust_jwn_net_items
  ,i.cust_tender_nordstrom
  ,i.cust_tender_nordstrom_note
  ,i.cust_tender_3rd_party_credit
  ,i.cust_tender_debit_card
  ,i.cust_tender_gift_card
  ,i.cust_tender_cash
  ,i.cust_tender_paypal
  ,i.cust_tender_check
  --,i.cust_event_ctr
  ,i.cust_event_holiday
  ,i.cust_event_anniversary
  ,i.cust_svc_group_exp_delivery
  ,i.cust_svc_group_order_pickup
  ,i.cust_svc_group_selling_relation
  ,i.cust_svc_group_remote_selling
  ,i.cust_svc_group_alterations
  ,i.cust_svc_group_in_store
  ,i.cust_svc_group_restaurant
  ,i.cust_service_free_exp_delivery
  ,i.cust_service_next_day_pickup
  ,i.cust_service_same_day_bopus
  ,i.cust_service_curbside_pickup
  ,i.cust_service_style_boards
  ,i.cust_service_gift_wrapping
  ,i.cust_service_pop_in
  ,i.cust_platform_desktop
  ,i.cust_platform_MOW
  ,i.cust_platform_IOS
  ,i.cust_platform_Android
  ,i.cust_platform_POS
  ,i.cust_anchor_brand
  ,i.cust_strategic_brand
  ,i.cust_store_customer
  ,i.cust_digital_customer
  ,clv.clv_jwn as cust_clv_jwn
  ,clv.clv_fp as cust_clv_fp
  ,clv.clv_op as cust_clv_op
from customer_year_driver a
  left join both_ages b on a.acp_id=b.acp_id and a.fiscal_year_shopped=b.fiscal_year_shopped
  left join customer_NMS_market c on a.acp_id=c.acp_id
  left join customer_dma d on a.acp_id=d.acp_id and a.fiscal_year_shopped=d.fiscal_year_shopped
  left join cust_level_loyalty e on a.acp_id=e.acp_id and a.fiscal_year_shopped=e.fiscal_year_shopped
  --left join contribution_margin_deciles f on a.acp_id=f.acp_id and a.fiscal_year_shopped=f.fiscal_year_shopped
  left join customer_acquisition_tenure g on a.acp_id=g.acp_id and a.fiscal_year_shopped=g.fiscal_year_shopped
  left join customer_activation h on a.acp_id=h.acp_id and a.fiscal_year_shopped=h.fiscal_year_shopped
  left join derived_cust_attributes i on a.acp_id=i.acp_id and a.fiscal_year_shopped=i.fiscal_year_shopped
  left join clv_date cld on a.fiscal_year_shopped = cld.fiscal_year_shopped
  left join t2dl_das_customber_model_attribute_productionalization.customer_prediction_clv_hist clv
      on a.acp_id = clv.acp_id and cld.scored_date = clv.scored_date;
collect statistics
column  (acp_id, fiscal_year_shopped)           
on  
{cco_t2_schema}.cco_customer_level_attributes_fy;
 
SET QUERY_BAND = NONE FOR SESSION;