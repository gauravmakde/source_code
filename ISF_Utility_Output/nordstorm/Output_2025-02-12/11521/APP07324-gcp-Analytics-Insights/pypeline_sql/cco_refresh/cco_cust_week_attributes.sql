SET QUERY_BAND = 'App_ID=APP08240;
DAG_ID=cco_tables_week_grain_11521_ACE_ENG;
Task_Name=run_cco_cust_week_attributes;'
FOR SESSION VOLATILE;


/************************************************************************************/
/************************************************************************************/
/************************************************************************************
 *
 * Build a Customer/Week-level attributes table for the CCO project.
 *
 ************************************************************************************/
/************************************************************************************/
/************************************************************************************/

/************************************************************************************/
/************************************************************************************
 * Create Customer/Week-level attributes
 ************************************************************************************/
/************************************************************************************/

/************************************************************************************
 * Dynamic dates for CCO tables
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
) with data primary index (curr_mo) on commit preserve rows;

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
) with data primary index (latest_mo_dt) on commit preserve rows;

/*********** Create Start & End dates lookup table **********************************/
create multiset volatile table date_lookup as (
select dp.fy19_start_dt lines_start_date
     , dp.latest_mo_dt lines_end_date
from date_parameter_lookup dp
) with data primary index (lines_start_date) on commit preserve rows;


/************************************************************************************
 * Create driver table (at a customer/week-level) with CCO_LINES data plus some product
 * identifiers (which get pulled first, then added later to the CCO_LINES data pull
 ************************************************************************************/

create multiset volatile table anchor_brands as (
  select global_tran_id, line_item_seq_num
  from prd_nap_usr_vws.retail_tran_detail_fact dtl
  join prd_nap_usr_vws.product_sku_dim_vw ps
    on dtl.sku_num = ps.rms_sku_num and ps.channel_country = 'US'
  join
  (
    select distinct supplier_idnt
    from t2dl_das_in_season_management_reporting.anchor_brands
    where anchor_brand_ind = 'Y'
  ) s
    on ps.prmy_supp_num = s.supplier_idnt
  where dtl.business_day_date >= (select lines_start_date from date_lookup)
) with data primary index (global_tran_id, line_item_seq_num) on commit preserve rows;

create multiset volatile table strategic_brands as (
  select global_tran_id, line_item_seq_num
  from prd_nap_usr_vws.retail_tran_detail_fact dtl
  join prd_nap_usr_vws.product_sku_dim_vw ps
    on dtl.sku_num = ps.rms_sku_num and ps.channel_country = 'US'
  join prd_nap_usr_vws.vendor_dim sup
    on ps.prmy_supp_num = sup.vendor_num
  join
  (
    select distinct supplier_name
    from t2dl_das_in_season_management_reporting.rack_strategic_brands
    where rack_strategic_brand_ind = 'Y'
  ) rsb
    on rsb.supplier_name = sup.vendor_name
  where dtl.business_day_date >= (select lines_start_date from date_lookup)
) with data primary index (global_tran_id, line_item_seq_num) on commit preserve rows;

collect statistics column (global_tran_id, line_item_seq_num) on anchor_brands;
collect statistics column (global_tran_id, line_item_seq_num) on strategic_brands;


/************************************************************************************
 * Create detail level extract first with more efficient index for later aggregation
 ************************************************************************************/

create multiset volatile table cco_lines_week_extract as (
  select acp_id
    ,dt.week_idnt
    ,dt.week_start_day_date
    ,dt.week_end_day_date
    ,channel
    ,banner
    ,employee_flag
    ,ntn_tran
    ,gross_sales
    ,return_amt
    ,net_sales
    ,div_num
    ,store_num
    ,date_shopped
    ,gross_incl_gc
    ,gross_items
    ,return_items
    ,(gross_items - return_items) net_items
    ,tender_nordstrom
    ,tender_nordstrom_note
    ,tender_3rd_party_credit
    ,tender_debit_card
    ,tender_gift_card
    ,tender_cash
    ,tender_paypal
    ,tender_check
    ,event_holiday
    ,event_anniversary
    ,svc_group_exp_delivery
    ,svc_group_order_pickup
    ,svc_group_selling_relation
    ,svc_group_remote_selling
    ,svc_group_alterations
    ,svc_group_in_store
    ,svc_group_restaurant
    ,service_free_exp_delivery
    ,service_next_day_pickup
    ,service_same_day_bopus
    ,service_curbside_pickup
    ,service_style_boards
    ,service_gift_wrapping
    ,service_pop_in
    ,marketplace_flag
    ,platform
    ,case when ab.global_tran_id is not null and substring(channel,1,1) in ('1','2') then 1 else 0 end as anchor_brand
    ,case when sb.global_tran_id is not null and substring(channel,1,1) in ('3','4') then 1 else 0 end as strategic_brand
  from t2dl_das_strategy.cco_line_items a
  join prd_nap_usr_vws.day_cal_454_dim dt
    on a.date_shopped = dt.day_date
  left join anchor_brands ab
    on a.global_tran_id = ab.global_tran_id
    and a.line_item_seq_num = ab.line_item_seq_num
  left join strategic_brands sb
    on a.global_tran_id = sb.global_tran_id
    and a.line_item_seq_num = sb.line_item_seq_num
) with data primary index (acp_id, week_idnt) on commit preserve rows;

collect statistics column (acp_id, week_idnt) on cco_lines_week_extract;
collect statistics column (acp_id, week_idnt, week_start_day_date, week_end_day_date) on cco_lines_week_extract;
collect statistics column (acp_id, week_idnt, week_start_day_date, week_end_day_date, channel) on cco_lines_week_extract;
collect statistics column (acp_id, week_idnt, week_start_day_date, week_end_day_date, banner ) on cco_lines_week_extract;


create multiset volatile table cco_lines_week_agg as (
  select acp_id
    ,week_idnt
    ,week_start_day_date
    ,week_end_day_date
    ,count(distinct channel) channels_shopped
    ,count(distinct banner) banners_shopped
    ,max(employee_flag) employee_flag
    ,max(ntn_tran) ntn_this_week
    ,max(case when substring(channel,1,1) = '1' then 1 else 0 end) NS_ind
    ,max(case when substring(channel,1,1) = '2' then 1 else 0 end) NCOM_ind
    ,max(case when substring(channel,1,1) = '3' then 1 else 0 end) RS_ind
    ,max(case when substring(channel,1,1) = '4' then 1 else 0 end) RCOM_ind
    ,max(case when substring(channel,1,1) in ('1','2') then 1 else 0 end) N_ind
    ,max(case when substring(channel,1,1) in ('3','4') then 1 else 0 end) R_ind
    ,max(case when substring(channel,1,1) in ('1','3') then 1 else 0 end) store_customer
    ,max(case when substring(channel,1,1) in ('2','4') then 1 else 0 end) digital_customer
    ,sum(gross_sales) gross_sales
    ,sum(return_amt) return_amt
    ,sum(net_sales) net_sales
    ,sum(case when div_num = 351 then net_sales else 0 end) net_sales_apparel
    ,sum(case when div_num = 310 then net_sales else 0 end) net_sales_shoes
    ,sum(case when div_num = 340 then net_sales else 0 end) net_sales_beauty
    ,sum(case when div_num = 341 then net_sales else 0 end) net_sales_designer
    ,sum(case when div_num = 360 then net_sales else 0 end) net_sales_accessories
    ,sum(case when div_num = 365 then net_sales else 0 end) net_sales_home
    ,sum(case when div_num = 370 then net_sales else 0 end) net_sales_merch_projects
    ,sum(case when div_num = 800 then net_sales else 0 end) net_sales_leased_boutiques
    ,sum(case when div_num = 900 then net_sales else 0 end) net_sales_other_non_merch
    ,sum(case when div_num = 70  then net_sales else 0 end) net_sales_restaurant
    ,sum(case when div_num = 351 then 1 else 0 end) transaction_apparel
    ,sum(case when div_num = 310 then 1 else 0 end) transaction_shoes
    ,sum(case when div_num = 340 then 1 else 0 end) transaction_beauty
    ,sum(case when div_num = 341 then 1 else 0 end) transaction_designer
    ,sum(case when div_num = 360 then 1 else 0 end) transaction_accessories
    ,sum(case when div_num = 365 then 1 else 0 end) transaction_home
    ,sum(case when div_num = 370 then 1 else 0 end) transaction_merch_projects
    ,sum(case when div_num = 800 then 1 else 0 end) transaction_leased_boutiques
    ,sum(case when div_num = 900 then 1 else 0 end) transaction_other_non_merch
    ,sum(case when div_num = 70  then 1 else 0 end) transaction_restaurant
    ,count(distinct case when gross_incl_gc > 0
                         then acp_id||store_num||date_shopped
                         else null end) trips
    ,count(distinct case when gross_incl_gc > 0 and div_num <> 70
                         then acp_id||store_num||date_shopped
                         else null end) nonrestaurant_trips
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
    ,max(marketplace_flag) marketplace_flag
    ,max(case when trim(platform) = 'WEB' then 1 else 0 end) platform_desktop
    ,max(case when trim(platform) = 'MOW' then 1 else 0 end) platform_MOW
    ,max(case when trim(platform) = 'IOS' then 1 else 0 end) platform_IOS
    ,max(case when trim(platform) = 'ANDROID' then 1 else 0 end) platform_Android
    ,max(case when trim(platform) = 'POS' then 1 else 0 end) platform_POS
    ,max(anchor_brand) anchor_brand
    ,max(strategic_brand) strategic_brand
  from cco_lines_week_extract
  group by 1,2,3,4
) with data primary index (acp_id, week_idnt) on commit preserve rows;

collect statistics column (acp_id, week_idnt) on cco_lines_week_agg;


/************************************************************************************
 * Create driver tables for filtering the acp_id's and weeks we want from other tables
 * that will append data later along with CCO_LINES data in final table
 ************************************************************************************/

create multiset volatile table customer_acp_id_driver as (
select distinct a.acp_id
from cco_lines_week_agg a
) with data primary index (acp_id) on commit preserve rows;

create multiset volatile table customer_week_driver as (
select distinct a.acp_id
  ,a.week_idnt
  ,a.week_start_day_date
  ,a.week_end_day_date
  ,a.employee_flag
  ,a.net_sales
from cco_lines_week_agg a
) with data primary index (acp_id, week_idnt) on commit preserve rows;

collect statistics column (acp_id, week_idnt) on customer_week_driver;
collect statistics column (week_end_day_date) on customer_week_driver;
collect statistics column (week_idnt) on customer_week_driver;
collect statistics column (acp_id) on customer_week_driver;


/************************************************************************************
 * Identify customer's MARKET
 ************************************************************************************/

/*********** Lookup table assigning nodes to Markets ********************************/
create multiset volatile table consolidated_nms_nodes as (
select distinct market
  ,cast(case when upper(trim(market)) = 'LOS_ANGELES' then date'2019-06-20'
             when upper(trim(market)) = 'NEW_YORK' then date'2019-10-23'
             when upper(trim(market)) = 'SAN_FRANCISCO' then date'2019-10-29'
             when upper(trim(market)) = 'CHICAGO' then date'2019-11-04'
             when upper(trim(market)) = 'DALLAS' then date'2019-11-04'
             when upper(trim(market)) = 'SEATTLE' then date'2020-09-28'
             when upper(trim(market)) = 'BOSTON' then date'2020-10-05'
             when upper(trim(market)) = 'PHILADELPHIA' then date'2020-10-05'
             when upper(trim(market)) = 'WASHINGTON' then date'2020-10-05'
             when upper(trim(market)) = 'TORONTO' then date'2020-10-28'
             when upper(trim(market)) = 'DENVER' then date'2021-02-15'
             when upper(trim(market)) = 'SAN_DIEGO' then date'2021-02-15'
             when upper(trim(market)) = 'PORTLAND' then date'2021-02-17'
             when upper(trim(market)) = 'AUSTIN' then date'2021-02-22'
             when upper(trim(market)) = 'HOUSTON' then date'2021-02-22'
             when upper(trim(market)) = 'ATLANTA' then date'2021-03-01'
             when upper(trim(market)) = 'DETROIT' then date'2021-03-01'
             when upper(trim(market)) = 'MIAMI' then date'2021-03-01'
             when upper(trim(market)) = 'MINNEAPOLIS' then date'2021-03-01'
             else null end as date) nms_launch_date
  ,node_num
from
  (
  current validtime select local_market as market, cast(node as integer) as node_num
  from prd_nap_usr_vws.local_market_node_dim
  union
  select market, node_num
  from t2dl_das_strategy.cco_nms_nodes
  ) x
) with data unique primary index (market, node_num) on commit preserve rows;

/*********** Create lookup table of customers & their markets ***********************/
create multiset volatile table customer_market as (
select distinct a.acp_id
  ,a.market
  ,case when b.market is not null then 1 else 0 end NMS_Market
  ,b.nms_launch_date
from
  (
  select distinct acp_id
    ,coalesce(bill_zip_market,fls_market,rack_market,ca_dma_desc,us_dma_desc) market
  from
    (
      select distinct acp_id,
        case when b.market is not null then b.market end as bill_zip_market,
        case when c.market is not null then c.market end as fls_market,
        case when d.market is not null then d.market end as rack_market,
        ca_dma_desc,
        us_dma_desc
      from
        (
          select x.acp_id, billing_postal_code, ca_dma_desc, us_dma_desc, fls_loyalty_store_num, rack_loyalty_store_num,
            case when ca_dma_desc is not null then left(billing_postal_code,3) else billing_postal_code end as join_zip
          from prd_nap_usr_vws.analytical_customer x
          join customer_week_driver y on x.acp_id = y.acp_id
          where x.acp_id is not null
        ) a
      left join
        ( current validtime
          select distinct local_market as market
            ,case when local_market = 'TORONTO' then left(coarse_postal_code,3) else coarse_postal_code end as join_zip
          from prd_nap_usr_vws.local_market_postal_dim
        ) b on a.join_zip = b.join_zip
      left join consolidated_nms_nodes c on a.fls_loyalty_store_num = c.node_num
      left join consolidated_nms_nodes d on a.rack_loyalty_store_num = d.node_num
      where a.acp_id is not null
    ) z
  where market is not null or market not in ('Not Defined', 'Other')
  ) a
left join
  (
    select distinct market, nms_launch_date
    from consolidated_nms_nodes
  ) b on a.market = b.market
) with data unique primary index (acp_id) on commit preserve rows;

/*********** Create lookup table of customers & their NMS markets ********************/
create multiset volatile table customer_nms_market as (
select distinct acp_id
  ,market
from customer_market
where NMS_Market = 1
) with data unique primary index (acp_id) on commit preserve rows;

collect statistics column (acp_id) on customer_nms_market;


/************************************************************************************
 * Identify customer's DMA (& that DMA's rank in terms of net-sales)
 ************************************************************************************/

/*********** grab all customers' country & DMA **************************************/
create multiset volatile table customer_dma as (
select a.acp_id
  ,case when b.ca_dma_desc is not null then 'CA'
        when b.us_dma_desc is not null then 'US' end as cust_country
  ,coalesce(b.ca_dma_desc, b.us_dma_desc) as cust_dma
from customer_acp_id_driver a
left join prd_nap_usr_vws.analytical_customer b
  on a.acp_id = b.acp_id
) with data primary index (acp_id) on commit preserve rows;

collect statistics column (acp_id) on customer_dma;
collect statistics column (cust_dma) on customer_dma;

/*********** agg the net sales for week/dma *****************************************/
create multiset volatile table customer_week_dma_ns as (
select a.week_idnt
  ,b.cust_dma
  ,sum(net_sales) as dma_net_sales
from customer_week_driver a
left join customer_dma b
  on a.acp_id = b.acp_id
group by 1,2
) with data primary index (week_idnt, cust_dma) on commit preserve rows;

/*********** rank all DMA's by net-sales for each week ******************************/
create multiset volatile table dma_week_ranking as (
select cust_dma
  ,week_idnt
  ,rank() over (partition by week_idnt order by dma_net_sales desc) dma_rank
from customer_week_dma_ns cwns
) with data primary index (cust_dma, week_idnt) on commit preserve rows;

collect statistics column (cust_dma, week_idnt) on dma_week_ranking;
collect statistics column (cust_dma ) on dma_week_ranking;
collect statistics column (week_idnt) on dma_week_ranking;


/*********** customer DMA bucketing *************************************************/
create multiset volatile table customer_dma_week as (
select distinct drv.acp_id
  ,dma.cust_dma
  ,dma.cust_country
  ,drv.week_idnt
  ,case when rnk.dma_rank is null            then 'DMA missing'
        when rnk.dma_rank between  1 and  5  then '1) Top 5'
        when rnk.dma_rank between  6 and 10  then '2) 6-10'
        when rnk.dma_rank between 11 and 20  then '3) 11-20'
        when rnk.dma_rank between 21 and 30  then '4) 21-30'
        when rnk.dma_rank between 31 and 50  then '5) 31-50'
        when rnk.dma_rank between 51 and 100 then '6) 51-100'
                                             else '7) > 100' end dma_rank
from customer_week_driver drv
join customer_dma dma
  on drv.acp_id = dma.acp_id
join dma_week_ranking rnk
  on dma.cust_dma = rnk.cust_dma
  and drv.week_idnt = rnk.week_idnt
) with data primary index (acp_id, week_idnt) on commit preserve rows;

collect statistics column (acp_id, week_idnt) on customer_dma_week;


/************************************************************************************
 * Grab all attributes DERIVED FROM LINE-ITEM TABLE
 *    (includes channel-combo,
 *              bucketed trips & spend,
 *              1/0 indicators by service, tender, & platform)
 *  use fact instead of fact_vw due to missing skus in fact_vw
 ************************************************************************************/
create volatile multiset table derived_cust_attributes as (
select distinct acp_id
  ,week_idnt
  ,ntn_this_week
  ,channels_shopped
  ,case when NS_ind = 1 and NCOM_ind = 0 and RS_ind = 0 and RCOM_ind = 0 then '01) NordStore-only'
        when NS_ind = 0 and NCOM_ind = 1 and RS_ind = 0 and RCOM_ind = 0 then '02) N.com-only'
        when NS_ind = 0 and NCOM_ind = 0 and RS_ind = 1 and RCOM_ind = 0 then '03) RackStore-only'
        when NS_ind = 0 and NCOM_ind = 0 and RS_ind = 0 and RCOM_ind = 1 then '04) Rack.com-only'
        when NS_ind = 1 and NCOM_ind = 1 and RS_ind = 0 and RCOM_ind = 0 then '05) NordStore+N.com'
        when NS_ind = 1 and NCOM_ind = 0 and RS_ind = 1 and RCOM_ind = 0 then '06) NordStore+RackStore'
        when NS_ind = 1 and NCOM_ind = 0 and RS_ind = 0 and RCOM_ind = 1 then '07) NordStore+Rack.com'
        when NS_ind = 0 and NCOM_ind = 1 and RS_ind = 1 and RCOM_ind = 0 then '08) N.com+RackStore'
        when NS_ind = 0 and NCOM_ind = 1 and RS_ind = 0 and RCOM_ind = 1 then '09) N.com+Rack.com'
        when NS_ind = 0 and NCOM_ind = 0 and RS_ind = 1 and RCOM_ind = 1 then '10) RackStore+Rack.com'
        when NS_ind = 1 and NCOM_ind = 1 and RS_ind = 1 and RCOM_ind = 0 then '11) NordStore+N.com+RackStore'
        when NS_ind = 1 and NCOM_ind = 1 and RS_ind = 0 and RCOM_ind = 1 then '12) NordStore+N.com+Rack.com'
        when NS_ind = 1 and NCOM_ind = 0 and RS_ind = 1 and RCOM_ind = 1 then '13) NordStore+RackStore+Rack.com'
        when NS_ind = 0 and NCOM_ind = 1 and RS_ind = 1 and RCOM_ind = 1 then '14) N.com+RackStore+Rack.com'
        when NS_ind = 1 and NCOM_ind = 1 and RS_ind = 1 and RCOM_ind = 1 then '15) 4-Box'
        else '99) Error' end chan_combo
  ,banners_shopped
  ,case when N_ind = 1 and R_ind = 0 then '1) Nordstrom-only'
        when N_ind = 0 and R_ind = 1 then '2) Rack-only'
        when N_ind = 1 and R_ind = 1 then '3) Dual-Banner'
        else '99) Error' end banner_combo
  ,employee_flag
  ,case when trips < 10 then '0'||cast(trips as varchar(1))||' trips'
        else '10+ trips' end jwn_trip_bucket
  ,case when net_sales = 0                            then '0) $0'
        when net_sales > 0     and net_sales <= 50    then '1) $0-50'
        when net_sales > 50    and net_sales <= 100   then '2) $50-100'
        when net_sales > 100   and net_sales <= 250   then '3) $100-250'
        when net_sales > 250   and net_sales <= 500   then '4) $250-500'
        when net_sales > 500   and net_sales <= 1000  then '5) $500-1K'
        when net_sales > 1000  and net_sales <= 2000  then '6) 1-2K'
        when net_sales > 2000  and net_sales <= 5000  then '7) 2-5K'
        when net_sales > 5000  and net_sales <= 10000 then '8) 5-10K'
        when net_sales > 10000                        then '9) 10K+'
        else null end jwn_net_spend_bucket
  ,case when ntn_this_week = 1 and coalesce(nonrestaurant_trips,0) <= 5 then 'Acquire & Activate'
        when coalesce(nonrestaurant_trips,0) <= 5  then 'Lightly-Engaged'
        when coalesce(nonrestaurant_trips,0) <= 13 then 'Moderately-Engaged'
        when coalesce(nonrestaurant_trips,0) >= 14 then 'Highly-Engaged'
        else null end engagement_cohort
  ,gross_sales
  ,return_amt
  ,net_sales
  ,net_sales_apparel
  ,net_sales_shoes
  ,net_sales_beauty
  ,net_sales_designer
  ,net_sales_accessories
  ,net_sales_home
  ,net_sales_merch_projects
  ,net_sales_leased_boutiques
  ,net_sales_other_non_merch
  ,net_sales_restaurant
  ,transaction_apparel
  ,transaction_shoes
  ,transaction_beauty
  ,transaction_designer
  ,transaction_accessories
  ,transaction_home
  ,transaction_merch_projects
  ,transaction_leased_boutiques
  ,transaction_other_non_merch
  ,transaction_restaurant
  ,trips
  ,nonrestaurant_trips
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
  ,marketplace_flag cust_marketplace_flag
  ,platform_desktop cust_platform_desktop
  ,platform_MOW cust_platform_MOW
  ,platform_IOS cust_platform_IOS
  ,platform_Android cust_platform_Android
  ,platform_POS cust_platform_POS
  ,anchor_brand cust_anchor_brand
  ,strategic_brand cust_strategic_brand
  ,store_customer cust_store_customer
  ,digital_customer cust_digital_customer
from cco_lines_week_agg a
) with data primary index (acp_id, week_idnt) on commit preserve rows;

collect statistics column (acp_id, week_idnt) on derived_cust_attributes;


/************************************************************************************
 * Identify customer's AGE
 ************************************************************************************/

/*********** Grab customers' Modeled ages
 *    (reduce age by the difference between age date & time-period end date)
 ***********/
create multiset volatile table modeled_ages as (
select distinct dr.acp_id
  ,dr.week_idnt
  ,cast(cast(a.model_age as decimal(6,2))
          - (cast(update_timestamp as date) - dr.week_end_day_date)/365.25
        as integer) model_age_adjusted
from t2dl_das_age_model.new_age_model_scoring_all a
join customer_week_driver dr
  on a.acp_id = dr.acp_id
qualify rank() over (partition by a.acp_id order by a.update_timestamp desc) = 1
) with data primary index (acp_id, week_idnt) on commit preserve rows;

/*********** Join customers' Experian age & gender)
 *    (reduce age by the difference between age date & time-period end date)
 ***********/
create multiset volatile table experian_demos as (
select distinct dr.acp_id
  ,dr.week_idnt
  ,a.gender
  ,cast(case when age_type = 'Exact Age' and age_value is not null then
               case when length(trim(a.birth_year_and_month)) = 6 then
                      (dr.week_end_day_date
                       - cast(to_date(substring(a.birth_year_and_month,1,4)||'/'||
                                      substring(a.birth_year_and_month,5,2)||'/'||'15'
                                     ,'YYYY/MM/DD') as date))/365.25
                    else (cast(a.age_value as decimal(6,2)))
                       - (cast(a.object_system_time as date) - dr.week_end_day_date)/365.25
               end
             else null end as integer) experian_age_adjusted
from prd_nap_cust_usr_vws.customer_experian_demographic_prediction_dim a
join customer_week_driver dr
  on a.acp_id = dr.acp_id
) with data primary index (acp_id, week_idnt) on commit preserve rows;

/*********** Combine the ages: use Experian age if present, Analytics age otherwise
 *    (calculate age-ranges based on Marketing's "lifestage" segments)
 ***********/
create multiset volatile table both_ages as (
select distinct x.acp_id
  ,x.week_idnt
  ,x.gender
  ,x.age
  ,case when x.age between 14 and 22 then '01) Young Adult'
        when x.age between 23 and 29 then '02) Early Career'
        when x.age between 30 and 44 then '03) Mid Career'
        when x.age between 45 and 64 then '04) Late Career'
        when x.age >= 65 then '05) Retired'
        else 'Unknown' end lifestage
  ,case when age <  18 then '0) <18 yrs'
        when age >= 18 and age <= 24 then '1) 18-24 yrs'
        when age >  24 and age <= 34 then '2) 25-34 yrs'
        when age >  34 and age <= 44 then '3) 35-44 yrs'
        when age >  44 and age <= 54 then '4) 45-54 yrs'
        when age >  54 and age <= 64 then '5) 55-64 yrs'
        when age >  64 then '6) 65+ yrs'
        else 'Unknown' end as age_group
from
  (
  select distinct a.acp_id
    ,a.week_idnt
    ,b.gender
    ,coalesce(b.experian_age_adjusted,c.model_age_adjusted) age
  from customer_week_driver a
  left join experian_demos b
    on a.acp_id = b.acp_id
    and a.week_idnt = b.week_idnt
  left join modeled_ages c
    on a.acp_id = c.acp_id
    and a.week_idnt = c.week_idnt
  ) x
) with data primary index (acp_id, week_idnt) on commit preserve rows;

collect statistics column (acp_id, week_idnt) on both_ages;


/************************************************************************************
 * Identify customer's LOYALTY-related attributes: type, level, enrollment date
 ************************************************************************************/

/*********** Find Loyalty Cardmembers' highest levels (by week) *********************/

/* Cardmembers */

create multiset volatile table cust_level_loyalty_cardmember as (
select lmd.acp_id
   ,dr.week_idnt
   ,1 as flg_cardmember
   ,max(case when rwd.rewards_level in ('MEMBER') then 1
             when rwd.rewards_level in ('INSIDER','INFLUENCER') then 3
             when rwd.rewards_level in ('AMBASSADOR') then 4
             when rwd.rewards_level in ('ICON') then 5
             else 0
         end) as cardmember_level
   ,min(lmd.cardmember_enroll_date) cardmember_enroll_date
from prd_nap_usr_vws.loyalty_member_dim_vw as lmd
join customer_week_driver dr
  on lmd.acp_id = dr.acp_id
left join
(
  select acp_id
    ,cast(cast(max_close_dt as varchar(8)) as date FORMAT 'yyyymmdd') as max_close_dt
  from t2dl_das_strategy.cco_credit_close_dts
) as ccd
  on lmd.acp_id = ccd.acp_id
left join prd_nap_usr_vws.loyalty_level_lifecycle_fact_vw as rwd
  on lmd.loyalty_id = rwd.loyalty_id
  and rwd.start_day_date <= dr.week_end_day_date
  and rwd.end_day_date > dr.week_end_day_date
where coalesce(lmd.cardmember_enroll_date, date'2099-12-31') <  coalesce(ccd.max_close_dt, lmd.cardmember_close_date, date'2099-12-31')
  and coalesce(lmd.cardmember_enroll_date, date'2099-12-31') <= dr.week_end_day_date
  and coalesce(ccd.max_close_dt, lmd.cardmember_close_date, date'2099-12-31') >= dr.week_end_day_date
  and lmd.acp_id is not null
group by 1,2,3
) with data primary index (acp_id, week_idnt) on commit preserve rows;

collect statistics column (acp_id, week_idnt) on cust_level_loyalty_cardmember;


/*********** Find Loyalty Members' highest levels (by week) *************************/

/* Members */

create multiset volatile table cust_level_loyalty_member as (
select lmd.acp_id
   ,dr.week_idnt
   ,1 as flg_member
   ,max(case when rwd.rewards_level in ('MEMBER') then 1
             when rwd.rewards_level in ('INSIDER','INFLUENCER') then 3
             when rwd.rewards_level in ('AMBASSADOR') then 4
             when rwd.rewards_level in ('ICON') then 5
             else 0
         end) as member_level
   ,min(lmd.member_enroll_date) member_enroll_date
from prd_nap_usr_vws.loyalty_member_dim_vw as lmd
join customer_week_driver dr
  on lmd.acp_id = dr.acp_id
left join prd_nap_usr_vws.loyalty_level_lifecycle_fact_vw as rwd
  on lmd.loyalty_id = rwd.loyalty_id
  and rwd.start_day_date <= dr.week_end_day_date
  and rwd.end_day_date > dr.week_end_day_date
where coalesce(lmd.member_enroll_date, date'2099-12-31') <  coalesce(lmd.member_close_date, date'2099-12-31')
  and coalesce(lmd.member_enroll_date, date'2099-12-31') <= dr.week_end_day_date
  and coalesce(lmd.member_close_date,  date'2099-12-31') >= dr.week_end_day_date
  and lmd.acp_id is not null
group by 1,2,3
) with data primary index (acp_id, week_idnt) on commit preserve rows;

collect statistics column (acp_id, week_idnt) on cust_level_loyalty_member;


/*********** Combine both types of Loyalty customers into 1 lookup table
 *    (prioritize "Cardmember" type & level if present, use "Member" otherwise)
 ***********/

/* Combine both types of Loyalty customers into 1 lookup table */

create multiset volatile table cust_level_loyalty as
(
select a.acp_id
  ,a.week_idnt
  ,case when b.acp_id is not null then 'a) Cardmember'
        when c.acp_id is not null then 'b) Member'
        else 'c) Non-Loyalty' end as loyalty_type
  ,case when a.employee_flag = 1 and b.acp_id is not null and c.acp_id is not null then '1) MEMBER'
        when b.acp_id is not null and b.cardmember_level <= 3 then '2) INFLUENCER'
        when b.acp_id is not null and b.cardmember_level  = 4 then '3) AMBASSADOR'
        when b.acp_id is not null and b.cardmember_level  = 5 then '4) ICON'
        when c.member_level <= 1 then '1) MEMBER'
        when c.member_level  = 3 then '2) INFLUENCER'
        when c.member_level >= 4 then '3) AMBASSADOR'
        else null end loyalty_level
  ,c.member_enroll_date loyalty_member_start_dt
  ,b.cardmember_enroll_date loyalty_cardmember_start_dt
from customer_week_driver a
left join cust_level_loyalty_cardmember b
  on a.acp_id = b.acp_id
  and a.week_idnt = b.week_idnt
left join cust_level_loyalty_member c
  on a.acp_id = c.acp_id
  and a.week_idnt = c.week_idnt
) with data primary index (acp_id, week_idnt) on commit preserve rows;

collect statistics column (acp_id, week_idnt) on cust_level_loyalty;


/************************************************************************************
 * Identify customer's AQUISITION attributes:
 *    (channel & date of acquisition, bucketed tenure in years)
 ************************************************************************************/

/*********** Find Acquisition date, channel, banner, brand & tenure-years ***********/
create multiset volatile table customer_acquisition_tenure_prep as (
select a.acp_id
  ,b.week_idnt
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
  ,cast(floor((b.week_end_day_date - a.aare_status_date)/365.25) as integer) tenure_years
  ,cast(floor(12*(b.week_end_day_date - a.aare_status_date)/365.25) as integer) tenure_months
from prd_nap_usr_vws.customer_ntn_status_fact a
join customer_week_driver b
  on a.acp_id = b.acp_id
where a.aare_status_date <= b.week_end_day_date
qualify rank() over (partition by a.acp_id, b.week_idnt order by a.aare_status_date desc) = 1
) with data primary index (acp_id, week_idnt) on commit preserve rows;

collect statistics column (acp_id, week_idnt) on customer_acquisition_tenure_prep;
collect statistics column (acquisition_date ) on customer_acquisition_tenure_prep;

/*********** Bucket the tenure-years ************************************************/
create multiset volatile table customer_acquisition_tenure as (
select distinct a.acp_id
  ,a.week_idnt
  ,a.acquisition_date
  ,dt.fiscal_year_num acquisition_fiscal_year
  ,a.acquisition_channel
  ,a.acquisition_banner
  ,a.acquisition_brand
  ,a.tenure_years
  ,case when a.tenure_months < 4                then '1) 0-3 months'
        when a.tenure_months between 4  and 6   then '2) 4-6 months'
        when a.tenure_months between 7  and 12  then '3) 7-12 months'
        when a.tenure_months between 13 and 24  then '4) 13-24 months'
        when a.tenure_months between 25 and 36  then '5) 25-36 months'
        when a.tenure_months between 37 and 48  then '6) 37-48 months'
        when a.tenure_months between 49 and 60  then '7) 49-60 months'
        when a.tenure_months > 60               then '8) 61+ months'
        else 'Unknown' end tenure_bucket_months
  ,case when a.tenure_months <= 12              then '1) <= 1 year'
        when a.tenure_months between 13 and 24  then '2) 1-2 years'
        when a.tenure_months between 25 and 60  then '3) 2-5 years'
        when a.tenure_months between 61 and 120 then '4) 5-10 years'
        when a.tenure_months > 120              then '5) 10+ years'
        else 'Unknown' end tenure_bucket_years
from customer_acquisition_tenure_prep a
join prd_nap_usr_vws.day_cal_454_dim dt
  on a.acquisition_date = dt.day_date
) with data primary index (acp_id, week_idnt) on commit preserve rows;

collect statistics column (acp_id, week_idnt) on customer_acquisition_tenure;


/************************************************************************************
 * Identify customer's ACTIVATION attributes:
 *    (channel & date of activation)
 ************************************************************************************/

create multiset volatile table customer_activation as (
select a.acp_id
  ,b.week_idnt
  ,a.activated_date activation_date
  ,case when a.activated_chnl_code = 'FLS'  then '1) Nordstrom Stores'
        when a.activated_chnl_code = 'NCOM' then '2) Nordstrom.com'
        when a.activated_chnl_code = 'RACK' then '3) Rack Stores'
        when a.activated_chnl_code = 'NRHL' then '4) Rack.com'
        else null end activation_channel
  ,case when a.activated_chnl_code in ('FLS','NCOM')  then 'NORDSTROM'
        when a.activated_chnl_code in ('RACK','NRHL') then 'RACK'
        else null end activation_banner
from prd_nap_usr_vws.customer_activated_fact a
join customer_week_driver b
  on a.acp_id = b.acp_id
where a.activated_date <= b.week_end_day_date
qualify rank() over (partition by a.acp_id, b.week_idnt order by a.activated_date desc) = 1
) with data primary index (acp_id, week_idnt) on commit preserve rows;

collect statistics column (acp_id, week_idnt) on customer_activation;


/************************************************************************************
 * Identify all scoring dates from clv table
 *    find the scoring date most recent prior to week_idnt being processed
 *    get clv data for that acp_id / scored_date
 ************************************************************************************/

create multiset volatile table clv_scored_dates as (
select count(*) cnt, scored_date
from t2dl_das_customber_model_attribute_productionalization.customer_prediction_clv_hist
group by scored_date
having cnt > 100000
) with data primary index (scored_date) on commit preserve rows;

create multiset volatile table cust_week_scored_dates as (
select drvr.acp_id
  ,drvr.week_idnt
  ,max(sd.scored_date) scored_date
from customer_week_driver drvr
join clv_scored_dates sd
  on sd.scored_date < drvr.week_start_day_date
group by 1,2
) with data primary index (week_idnt) on commit preserve rows;

create multiset volatile table cust_clv_data as (
select distinct cwsd.acp_id
  ,cwsd.week_idnt
  ,clv.clv_jwn
  ,clv.clv_fp
  ,clv.clv_op
from cust_week_scored_dates cwsd
join t2dl_das_customber_model_attribute_productionalization.customer_prediction_clv_hist clv
  on cwsd.acp_id = clv.acp_id
  and cwsd.scored_date = clv.scored_date
) with data primary index (acp_id, week_idnt) on commit preserve rows;

collect statistics column (acp_id, week_idnt) on cust_clv_data;


/************************************************************************************
 * Join all customer/week-level attributes together using NOPI for initial multiple
 * table joins (Teradata recommends). Then insert into final empty production table
 ************************************************************************************/

create multiset volatile table cco_cust_week_attributes_stg,
no fallback, no before journal, no after journal,
checksum = default
(
   acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
  ,week_idnt INTEGER
  ,cust_gender VARCHAR(7) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_age INTEGER
  ,cust_lifestage VARCHAR(16) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_age_group VARCHAR(13) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_nms_market VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_dma VARCHAR(55) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_country VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_dma_rank VARCHAR(11) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_loyalty_type VARCHAR(14) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_loyalty_level VARCHAR(13) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_loy_member_enroll_dt DATE
  ,cust_loy_cardmember_enroll_dt DATE
  ,cust_acquisition_date DATE
  ,cust_acquisition_fiscal_year SMALLINT
  ,cust_acquisition_channel VARCHAR(19) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_acquisition_banner VARCHAR(9) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_acquisition_brand VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_tenure_bucket_months VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_tenure_bucket_years VARCHAR(13) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_activation_date DATE
  ,cust_activation_channel VARCHAR(19) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_activation_banner VARCHAR(9) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_engagement_cohort VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_channel_count INTEGER
  ,cust_channel_combo VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_banner_count INTEGER
  ,cust_banner_combo VARCHAR(17) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_employee_flag INTEGER
  ,cust_jwn_trip_bucket VARCHAR(9) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_jwn_net_spend_bucket VARCHAR(11) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
  ,cust_jwn_gross_sales DECIMAL(38,2)
  ,cust_jwn_return_amt DECIMAL(38,2)
  ,cust_jwn_net_sales DECIMAL(38,2)
  ,cust_jwn_net_sales_apparel DECIMAL(38,2)
  ,cust_jwn_net_sales_shoes DECIMAL(38,2)
  ,cust_jwn_net_sales_beauty DECIMAL(38,2)
  ,cust_jwn_net_sales_designer DECIMAL(38,2)
  ,cust_jwn_net_sales_accessories DECIMAL(38,2)
  ,cust_jwn_net_sales_home DECIMAL(38,2)
  ,cust_jwn_net_sales_merch_projects DECIMAL(38,2)
  ,cust_jwn_net_sales_leased_boutiques DECIMAL(38,2)
  ,cust_jwn_net_sales_other_non_merch DECIMAL(38,2)
  ,cust_jwn_net_sales_restaurant DECIMAL(38,2)
  ,cust_jwn_transaction_apparel_ind INTEGER
  ,cust_jwn_transaction_shoes_ind INTEGER
  ,cust_jwn_transaction_beauty_ind INTEGER
  ,cust_jwn_transaction_designer_ind INTEGER
  ,cust_jwn_transaction_accessories_ind INTEGER
  ,cust_jwn_transaction_home_ind INTEGER
  ,cust_jwn_transaction_merch_projects_ind INTEGER
  ,cust_jwn_transaction_leased_boutiques_ind INTEGER
  ,cust_jwn_transaction_other_non_merch_ind INTEGER
  ,cust_jwn_transaction_restaurant_ind INTEGER
  ,cust_jwn_trips INTEGER
  ,cust_jwn_nonrestaurant_trips INTEGER
  ,cust_jwn_gross_items INTEGER
  ,cust_jwn_return_items INTEGER
  ,cust_jwn_net_items INTEGER
  ,cust_tender_nordstrom INTEGER
  ,cust_tender_nordstrom_note INTEGER
  ,cust_tender_3rd_party_credit INTEGER
  ,cust_tender_debit_card INTEGER
  ,cust_tender_gift_card INTEGER
  ,cust_tender_cash INTEGER
  ,cust_tender_paypal INTEGER
  ,cust_tender_check INTEGER
  ,cust_event_holiday INTEGER
  ,cust_event_anniversary INTEGER
  ,cust_svc_group_exp_delivery INTEGER
  ,cust_svc_group_order_pickup INTEGER
  ,cust_svc_group_selling_relation INTEGER
  ,cust_svc_group_remote_selling INTEGER
  ,cust_svc_group_alterations INTEGER
  ,cust_svc_group_in_store INTEGER
  ,cust_svc_group_restaurant INTEGER
  ,cust_service_free_exp_delivery INTEGER
  ,cust_service_next_day_pickup INTEGER
  ,cust_service_same_day_bopus INTEGER
  ,cust_service_curbside_pickup INTEGER
  ,cust_service_style_boards INTEGER
  ,cust_service_gift_wrapping INTEGER
  ,cust_service_pop_in INTEGER
  ,cust_marketplace_flag INTEGER
  ,cust_platform_desktop INTEGER
  ,cust_platform_MOW INTEGER
  ,cust_platform_IOS INTEGER
  ,cust_platform_Android INTEGER
  ,cust_platform_POS INTEGER
  ,cust_anchor_brand BYTEINT
  ,cust_strategic_brand BYTEINT
  ,cust_store_customer BYTEINT
  ,cust_digital_customer BYTEINT
  ,cust_clv_jwn DECIMAL(14,4) COMPRESS
  ,cust_clv_fp DECIMAL(14,4) COMPRESS
  ,cust_clv_op DECIMAL(14,4) COMPRESS
) no primary index on commit preserve rows;

insert into cco_cust_week_attributes_stg
select distinct a.acp_id
  ,a.week_idnt
  ,coalesce(b.gender,'Unknown') cust_gender
  ,b.age cust_age
  ,b.lifestage cust_lifestage
  ,b.age_group cust_age_group
  ,coalesce(trim(c.market),'Z - NON-NMS') cust_nms_market
  ,d.cust_dma
  ,d.cust_country
  ,coalesce(d.dma_rank,'DMA missing') cust_dma_rank
  ,e.loyalty_type cust_loyalty_type
  ,e.loyalty_level cust_loyalty_level
  ,e.loyalty_member_start_dt cust_loy_member_enroll_dt
  ,e.loyalty_cardmember_start_dt cust_loy_cardmember_enroll_dt
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
  ,i.net_sales_shoes cust_jwn_net_sales_shoes
  ,i.net_sales_beauty cust_jwn_net_sales_beauty
  ,i.net_sales_designer cust_jwn_net_sales_designer
  ,i.net_sales_accessories cust_jwn_net_sales_accessories
  ,i.net_sales_home cust_jwn_net_sales_home
  ,i.net_sales_merch_projects cust_jwn_net_sales_merch_projects
  ,i.net_sales_leased_boutiques cust_jwn_net_sales_leased_boutiques
  ,i.net_sales_other_non_merch cust_jwn_net_sales_other_non_merch
  ,i.net_sales_restaurant cust_jwn_net_sales_restaurant
  ,case when i.transaction_apparel >= 1 then 1 else 0 end cust_jwn_transaction_apparel_ind
  ,case when i.transaction_shoes >= 1 then 1 else 0 end cust_jwn_transaction_shoes_ind
  ,case when i.transaction_beauty >= 1 then 1 else 0 end cust_jwn_transaction_beauty_ind
  ,case when i.transaction_designer >= 1 then 1 else 0 end cust_jwn_transaction_designer_ind
  ,case when i.transaction_accessories >= 1 then 1 else 0 end cust_jwn_transaction_accessories_ind
  ,case when i.transaction_home >= 1 then 1 else 0 end cust_jwn_transaction_home_ind
  ,case when i.transaction_merch_projects >= 1 then 1 else 0 end cust_jwn_transaction_merch_projects_ind
  ,case when i.transaction_leased_boutiques >= 1 then 1 else 0 end cust_jwn_transaction_leased_boutiques_ind
  ,case when i.transaction_other_non_merch >= 1 then 1 else 0 end cust_jwn_transaction_other_non_merch_ind
  ,case when i.transaction_restaurant >= 1 then 1 else 0 end cust_jwn_transaction_restaurant_ind
  ,i.trips cust_jwn_trips
  ,i.nonrestaurant_trips cust_jwn_nonrestaurant_trips
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
  ,i.cust_marketplace_flag
  ,i.cust_platform_desktop
  ,i.cust_platform_MOW
  ,i.cust_platform_IOS
  ,i.cust_platform_Android
  ,i.cust_platform_POS
  ,i.cust_anchor_brand
  ,i.cust_strategic_brand
  ,i.cust_store_customer
  ,i.cust_digital_customer
  ,clvd.clv_jwn as cust_clv_jwn
  ,clvd.clv_fp as cust_clv_fp
  ,clvd.clv_op as cust_clv_op
from customer_week_driver a
left join both_ages b
  on a.acp_id = b.acp_id
  and a.week_idnt = b.week_idnt
left join customer_nms_market c
  on a.acp_id = c.acp_id
left join customer_dma_week d
  on a.acp_id = d.acp_id
  and a.week_idnt = d.week_idnt
left join cust_level_loyalty e
  on a.acp_id = e.acp_id
  and a.week_idnt = e.week_idnt
left join customer_acquisition_tenure g
  on a.acp_id = g.acp_id
  and a.week_idnt = g.week_idnt
left join customer_activation h
  on a.acp_id = h.acp_id
  and a.week_idnt = h.week_idnt
left join derived_cust_attributes i
  on a.acp_id = i.acp_id
  and a.week_idnt = i.week_idnt
left join cust_clv_data clvd
  on a.acp_id = clvd.acp_id
  and a.week_idnt = clvd.week_idnt;


delete from {cco_t2_schema}.cco_cust_week_attributes all;

insert into {cco_t2_schema}.cco_cust_week_attributes
select stg.*
     , current_timestamp(6) as dw_sys_load_tmstp
from cco_cust_week_attributes_stg stg;


SET QUERY_BAND = NONE FOR SESSION;
