SET QUERY_BAND = 'App_ID=APP08737;
DAG_ID=customer_sandbox_fact_daily_11521_ACE_ENG;
Task_Name=customer_sandbox_fact_build;'
FOR SESSION VOLATILE;


/************************************************************************************/
/************************************************************************************/
/************************************************************************************
 *
 * Build Customer Sandbox fact table for microstrategy customer sandbox.
 *
 ************************************************************************************/
/************************************************************************************/
/************************************************************************************/

create multiset volatile table date_range as (
select min(date_shopped) as start_date
     , max(date_shopped) as end_date
     , start_date + ((end_date-start_date) DAY(4))/3 as p2_start_date
     , p2_start_date + ((end_date-start_date) DAY(4))/3 as p2_end_date
from T2DL_DAS_STRATEGY.cco_line_items cli
where reporting_year_shopped is not null
) with data primary index(start_date) on commit preserve rows;


/************************************************************************************
 * PART A-1) Customer/year-level attributes table
 ************************************************************************************/
create multiset volatile table cust_year_level_attributes as (
--explain
select a.acp_id
     , reporting_year_shopped
     , cust_engagement_cohort
     , case when cust_jwn_trips < 10 then '0'||cast(cust_jwn_trips as varchar(1))||' trips'
            when cust_jwn_trips < 30 then cast(cust_jwn_trips as varchar(2))||' trips'
            else '30+ trips' end cust_jwn_trip_bucket
     , cust_jwn_net_spend_bucket
     , cust_channel_combo
     , cust_gender
     , cust_age_group
     , cust_lifestage
     , cust_age
     , cust_tenure_bucket_years
     , cust_dma
     , cust_dma_rank
     , cust_loyalty_type
     , cust_loyalty_level
     , cust_acquisition_date
     , cust_employee_flag
     , cust_chan_holiday
     , cust_chan_anniversary
     , cust_platform_desktop
     , cust_platform_MOW
     , cust_platform_IOS
     , cust_platform_Android
     , cust_platform_POS
     , cust_acquisition_fiscal_year
     , cust_acquisition_channel
     , cust_activation_channel
     , cust_country
     , cust_NMS_market
     , cust_tender_nordstrom
     , cust_tender_nordstrom_note
     , cust_tender_gift_card
     , case when cust_tender_nordstrom = 0 and cust_tender_nordstrom_note = 0 and cust_tender_gift_card = 0 then 1
            else 0 end cust_tender_other
     , cust_svc_group_exp_delivery
     , cust_svc_group_order_pickup
     , cust_svc_group_selling_relation
     , cust_svc_group_remote_selling
     , cust_svc_group_alterations
     , cust_svc_group_in_store
     , cust_svc_group_restaurant
     , cust_service_free_exp_delivery
     , cust_service_next_day_pickup
     , cust_service_same_day_bopus
     , cust_service_curbside_pickup
     , cust_service_style_boards
     , cust_service_gift_wrapping
     , cust_service_pop_in
     , max(case when channel = '1) Nordstrom Stores' then cust_chan_buyer_flow else NULL end) as ns_buyerflow
     , max(case when channel = '2) Nordstrom.com' then cust_chan_buyer_flow else NULL end) as ncom_buyerflow
     , max(case when channel = '3) Rack Stores' then cust_chan_buyer_flow else NULL end) as rs_buyerflow
     , max(case when channel = '4) Rack.com' then cust_chan_buyer_flow else NULL end) as rcom_buyerflow
     , max(case when channel = '5) Nordstrom Banner' then cust_chan_buyer_flow else NULL end) as nord_buyerflow
     , max(case when channel = '6) Rack Banner' then cust_chan_buyer_flow else NULL end) as rack_buyerflow
     , max(case when channel = '7) JWN' then cust_chan_buyer_flow else NULL end) as jwn_buyerflow
     , max(case when channel = '7) JWN' then cust_chan_acquired_aare end) as cust_jwn_acquired_aare
     , max(case when channel = '7) JWN' then cust_chan_activated_aare end) as cust_jwn_activated_aare
     , max(case when channel = '7) JWN' then cust_chan_retained_aare end) as cust_jwn_retained_aare
     , max(case when channel = '7) JWN' then cust_chan_engaged_aare end) as cust_jwn_engaged_aare
     , max(case when substring(channel,1,1)  = '1' then 1 else 0 end ) as shopped_fls
     , max(case when substring(channel,1,1)  = '2' then 1 else 0 end ) as shopped_ncom
     , max(case when substring(channel,1,1)  = '3' then 1 else 0 end ) as shopped_rack
     , max(case when substring(channel,1,1)  = '4' then 1 else 0 end ) as shopped_rcom
     , max(case when substring(channel,1,1)  = '5' then 1 else 0 end ) as shopped_fp
     , max(case when substring(channel,1,1)  = '6' then 1 else 0 end ) as shopped_op
     , max(case when substring(channel,1,1)  = '7' then 1 else 0 end ) as shopped_jwn
     , max(case when cust_chan_return_items > 0 then 1 else 0 end) as cust_made_a_return
from T2DL_DAS_STRATEGY.cco_cust_chan_yr_attributes a
where reporting_year_shopped is not null
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47
) with data primary index(acp_id,reporting_year_shopped) on commit preserve rows;


/************************************************************************************
 * PART A-2) Create Realinged Calendar
 ************************************************************************************/

/********* Step II-1-a: Going back 8 years from today, find all years with a "53rd week" *********/
create multiset volatile table week_53_yrs as (
select year_num
  ,rank() over (order by year_num desc) recency_rank
from
  (
  select distinct year_num
  from PRD_NAP_USR_VWS.DAY_CAL
  where week_of_fyr = 53
    and day_date between date'2017-01-01' and current_date
  ) x 
) with data primary index(year_num) on commit preserve rows;

/********* Step II-1-b: Count the # years with a "53rd week" *********/
create multiset volatile table week_53_yr_count as (
select count(distinct year_num) year_count
from week_53_yrs x 
) with data primary index(year_count) on commit preserve rows;

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

create multiset volatile table month_lookup as (
select distinct month_num as current_mo
  ,case when (month_num mod 100) = 1
        then (month_num - 89)
        else (month_num - 1 )
    end yr4_end_mo
  ,current_mo - 100 yr4_start_mo
  ,yr4_end_mo - 100 yr3_end_mo
  ,current_mo - 200 yr3_start_mo
  ,yr4_end_mo - 200 yr2_end_mo
  ,current_mo - 300 yr2_start_mo
  ,yr4_end_mo - 300 yr1_end_mo
  ,current_mo - 400 yr1_start_mo
from realigned_calendar 
where day_date = CURRENT_DATE()
) with data primary index(current_mo) on commit preserve rows;

create multiset volatile table month_names as (
    select 
       max(case when month_num=(select yr1_end_mo from month_lookup)
              then cast(case when month_short_desc='JAN' then 'FY-'||cast((year_num mod 2000) as varchar(2))
                             else 'YE-'
                                ||substring(month_short_desc,1,1)
                                ||lower(substring(month_short_desc,2,2))
                                ||cast((year_num mod 2000) as varchar(2)) end
                    as varchar(8)) else null end) 
       yr1_name
       ,max(case when month_num=(select yr2_end_mo from month_lookup)
              then cast(case when month_short_desc='JAN' then 'FY-'||cast((year_num mod 2000) as varchar(2))
                             else 'YE-'
                                ||substring(month_short_desc,1,1)
                                ||lower(substring(month_short_desc,2,2))
                                ||cast((year_num mod 2000) as varchar(2)) end
                    as varchar(8)) else null end) 
       yr2_name
       ,max(case when month_num=(select yr3_end_mo from month_lookup)
              then cast(case when month_short_desc='JAN' then 'FY-'||cast((year_num mod 2000) as varchar(2))
                             else 'YE-'
                                ||substring(month_short_desc,1,1)
                                ||lower(substring(month_short_desc,2,2))
                                ||cast((year_num mod 2000) as varchar(2)) end
                    as varchar(8)) else null end) 
       yr3_name
       ,max(case when month_num=(select yr4_end_mo from month_lookup)
              then cast(case when month_short_desc='JAN' then 'FY-'||cast((year_num mod 2000) as varchar(2))
                             else 'YE-'
                                ||substring(month_short_desc,1,1)
                                ||lower(substring(month_short_desc,2,2))
                                ||cast((year_num mod 2000) as varchar(2)) end
                    as varchar(8)) else null end) 
       yr4_name
    from realigned_calendar
    where month_num in ( (select yr1_end_mo from month_lookup)
                       , (select yr2_end_mo from month_lookup)
                       , (select yr3_end_mo from month_lookup)
                       , (select yr4_end_mo from month_lookup) )
) with data primary index(yr1_name) on commit preserve rows;


/************************************************************************************
 * PART A-3) Customer/year-level "cancels flag" table
 ************************************************************************************/
create multiset volatile table cancel_custs as (
select distinct a.acp_id
     , case when b.month_num between (select yr1_start_mo from month_lookup) and (select yr1_end_mo from month_lookup)
                 then (select yr1_name from month_names)
            when b.month_num between (select yr2_start_mo from month_lookup) and (select yr2_end_mo from month_lookup)
                 then (select yr2_name from month_names)
            when b.month_num between (select yr3_start_mo from month_lookup) and (select yr3_end_mo from month_lookup)
                 then (select yr3_name from month_names)
            when b.month_num between (select yr4_start_mo from month_lookup) and (select yr4_end_mo from month_lookup)
                 then (select yr4_name from month_names)
            else null end reporting_year_shopped
     , 1 cancel_flag
from prd_nap_usr_vws.order_line_cancel_fact a
join prd_nap_usr_vws.day_cal b on a.order_date_pacific = b.day_date
where a.order_date_pacific between (select min(day_date)
                                    from prd_nap_usr_vws.day_cal dc
                                    where month_num = (select yr1_start_mo from month_lookup))
                               and (select max(day_date)
                                    from prd_nap_usr_vws.day_cal dc
                                    where month_num = (select yr4_end_mo from month_lookup))
  and a.cancel_reason_code not in ('FRAUD_CHECK_FAILED', 'FRAUD_MANUAL_CANCEL')
  and a.acp_id is not null
) with data primary index(acp_id,reporting_year_shopped) on commit preserve rows;


/************************************************************************************
 * PART A-4) Customer/year-level of Ship-to-Store flags (specific to Autumn Fenz's 20221206 request)
 ************************************************************************************/

/*** PART A-4-i) Identify all Ship-to-Store line-items ***/
create multiset volatile table ship2store_order_activity as
(
select distinct a.global_tran_id
     , a.line_item_seq_num
     , a.item_delivery_method
     , a.source_channel_code
from T2DL_DAS_ITEM_DELIVERY.item_delivery_method_funnel_daily a
left join prd_nap_usr_vws.store_dim st on coalesce(a.picked_up_by_customer_node_num,-1*a.intent_store_num) = st.store_num
where a.business_day_date  between (select min(day_date) from PRD_NAP_USR_VWS.DAY_CAL dc where month_num = (select yr1_start_mo from month_lookup))
                               and (select max(day_date) + 14 from PRD_NAP_USR_VWS.DAY_CAL dc where month_num = (select yr4_end_mo from month_lookup))
  and a.order_date_pacific between (select min(day_date) from PRD_NAP_USR_VWS.DAY_CAL dc where month_num = (select yr1_start_mo from month_lookup))
                               and (select max(day_date) from PRD_NAP_USR_VWS.DAY_CAL dc where month_num = (select yr4_end_mo from month_lookup))
  and a.item_delivery_method like '%SHIP_TO_STORE'
  and a.canceled_date_pacific is null
) with data primary index(global_tran_id,line_item_seq_num) on commit preserve rows;

/*** PART A-4-i) Join-in the acp_id ***/
create multiset volatile table ship2store_joined as 
(
select distinct a.global_tran_id
     , a.line_item_seq_num
     , a.acp_id
     , a.reporting_year_shopped
     , a.channel
     , b.source_channel_code
from T2DL_DAS_STRATEGY.cco_line_items a
join ship2store_order_activity b on a.global_tran_id = b.global_tran_id and a.line_item_seq_num = b.line_item_seq_num
) with data primary index(global_tran_id,line_item_seq_num) on commit preserve rows;

/*** PART A-4-i) At a customer/year-level flag who had an NCOM Ship-to-Store & RCOM ship-to-store ***/

create multiset volatile table ship2store_cust_year as 
(
select acp_id
     , reporting_year_shopped
     , max(case when trim(source_channel_code) = 'FULL_LINE' then 1 else 0 end) ncom_sts
     , max(case when trim(source_channel_code) = 'RACK' then 1 else 0 end) rcom_sts
from ship2store_joined
group by 1,2
) with data primary index(acp_id,reporting_year_shopped) on commit preserve rows;

/************************************************************************************
 * PART A-5) Customer/year-level table of Division- & Price-Type- specific spend
 ************************************************************************************/

create multiset volatile table cust_spend_div_price as (
--explain
select a.acp_id
     , reporting_year_shopped
     , sum(case when substring(channel,1,1) in ('1','2') and div_num = 310 then net_sales else 0 end) cust_nord_Shoes_net_spend_ry
     , sum(case when substring(channel,1,1) in ('1','2') and div_num = 340 then net_sales else 0 end) cust_nord_Beauty_net_spend_ry
     , sum(case when substring(channel,1,1) in ('1','2') and div_num = 345 then net_sales else 0 end) cust_nord_Designer_Apparel_net_spend_ry
     , sum(case when substring(channel,1,1) in ('1','2') and div_num = 351 then net_sales else 0 end) cust_nord_Apparel_net_spend_ry
     , sum(case when substring(channel,1,1) in ('1','2') and div_num = 360 then net_sales else 0 end) cust_nord_Accessories_net_spend_ry
     , sum(case when substring(channel,1,1) in ('1','2') and div_num = 365 then net_sales else 0 end) cust_nord_Home_net_spend_ry
     , sum(case when substring(channel,1,1) in ('1','2') and div_num = 700 then net_sales else 0 end) cust_nord_Merch_Projects_net_spend_ry
     , sum(case when substring(channel,1,1) in ('1','2') and div_num = 800 then net_sales else 0 end) cust_nord_Leased_Boutique_net_spend_ry
     , sum(case when substring(channel,1,1) in ('1','2') and div_num =  70 then net_sales else 0 end) cust_nord_Restaurant_net_spend_ry
     , sum(case when substring(channel,1,1) in ('3','4') and div_num = 310 then net_sales else 0 end) cust_rack_Shoes_net_spend_ry
     , sum(case when substring(channel,1,1) in ('3','4') and div_num = 340 then net_sales else 0 end) cust_rack_Beauty_net_spend_ry
     , sum(case when substring(channel,1,1) in ('3','4') and div_num = 345 then net_sales else 0 end) cust_rack_Designer_Apparel_net_spend_ry
     , sum(case when substring(channel,1,1) in ('3','4') and div_num = 351 then net_sales else 0 end) cust_rack_Apparel_net_spend_ry
     , sum(case when substring(channel,1,1) in ('3','4') and div_num = 360 then net_sales else 0 end) cust_rack_Accessories_net_spend_ry
     , sum(case when substring(channel,1,1) in ('3','4') and div_num = 365 then net_sales else 0 end) cust_rack_Home_net_spend_ry
     , sum(case when substring(channel,1,1) in ('3','4') and div_num = 700 then net_sales else 0 end) cust_rack_Merch_Projects_net_spend_ry
     , sum(case when substring(channel,1,1) in ('3','4') and div_num = 800 then net_sales else 0 end) cust_Rack_Leased_Boutique_net_spend_ry
     , sum(case when substring(channel,1,1) in ('3','4') and div_num =  70 then net_sales else 0 end) cust_Rack_Restaurant_net_spend_ry
     , sum(case when substring(channel,1,1) in ('1','2') and price_type = 'R' then net_sales else 0 end) cust_nord_RegPrice_net_spend_ry
     , sum(case when substring(channel,1,1) in ('1','2') and price_type = 'P' then net_sales else 0 end) cust_nord_Promo_net_spend_ry
     , sum(case when substring(channel,1,1) in ('1','2') and price_type = 'C' then net_sales else 0 end) cust_nord_Clearance_net_spend_ry
     , sum(case when substring(channel,1,1) in ('3','4') and price_type = 'R' then net_sales else 0 end) cust_rack_RegPrice_net_spend_ry
     , sum(case when substring(channel,1,1) in ('3','4') and price_type = 'P' then net_sales else 0 end) cust_rack_Promo_net_spend_ry
     , sum(case when substring(channel,1,1) in ('3','4') and price_type = 'C' then net_sales else 0 end) cust_rack_Clearance_net_spend_ry
     , max(case when div_num = 70 then 1 else 0 end) restaurant_max
     , min(case when div_num = 70 then 1 else 0 end) restaurant_min
     , max(case when sn.store_type_code = 'NL' or rs.store_type_code = 'NL' or ps.store_type_code = 'NL' then 1 else 0 end) shopped_nord_local
     , max(case when substring(channel,1,1) = '2' and substring(return_channel,1,1) = '1' then 1 else 0 end) ncom_ret_to_nstore
     , max(case when substring(channel,1,1) = '2' and substring(return_channel,1,1) = '2' then 1 else 0 end) ncom_ret_by_mail
     , max(case when substring(channel,1,1) = '2' and substring(return_channel,1,1) = '3' then 1 else 0 end) ncom_ret_to_rstore
     , max(case when substring(channel,1,1) = '4' and substring(return_channel,1,1) = '1' then 1 else 0 end) rcom_ret_to_nstore
     , max(case when substring(channel,1,1) = '4' and substring(return_channel,1,1) = '4' then 1 else 0 end) rcom_ret_by_mail
     , max(case when substring(channel,1,1) = '4' and substring(return_channel,1,1) = '3' then 1 else 0 end) rcom_ret_to_rstore
from T2DL_DAS_STRATEGY.cco_line_items a
left join prd_nap_usr_vws.store_dim as sn on a.store_num = sn.store_num
left join prd_nap_usr_vws.store_dim as rs on coalesce(a.return_store,-1*a.store_num) = rs.store_num
left join prd_nap_usr_vws.store_dim as ps on coalesce(a.pickup_store,-1*a.store_num) = ps.store_num
where reporting_year_shopped is not null
group by 1,2
) with data primary index(acp_id,reporting_year_shopped) on commit preserve rows;

/************************************************************************************
 * PART A-6) Customer/year-level table with all the customer attributes
 * for a given reporting year
 ************************************************************************************/

create multiset volatile table cust_year_everything as (
--explain
select distinct a.acp_id
     , a.reporting_year_shopped
     , a.cust_engagement_cohort engagement_cohort
     , a.cust_jwn_trip_bucket
     , a.cust_jwn_net_spend_bucket
     , a.cust_channel_combo
     , a.cust_loyalty_type
     , a.cust_loyalty_level
     , a.cust_gender
     , a.cust_age_group
     , a.cust_employee_flag
     , a.cust_chan_holiday
     , a.cust_chan_anniversary
     , a.cust_tenure_bucket_years
     , a.cust_acquisition_fiscal_year
     , a.cust_acquisition_channel
     , a.cust_activation_channel
     , a.cust_country
     , a.cust_NMS_market
     , a.cust_dma
     , a.cust_tender_nordstrom
     , a.cust_tender_nordstrom_note
     , a.cust_tender_gift_card
     , a.cust_tender_other
     , coalesce(b.cancel_flag,0) cust_had_a_cancel
     , a.cust_svc_group_exp_delivery
     , a.cust_svc_group_order_pickup
     , a.cust_svc_group_selling_relation
     , a.cust_svc_group_remote_selling
     , a.cust_svc_group_alterations
     , a.cust_svc_group_in_store
     , a.cust_svc_group_restaurant
     , a.cust_service_free_exp_delivery
     , a.cust_service_next_day_pickup
     , a.cust_service_same_day_bopus
     , a.cust_service_curbside_pickup
     , a.cust_service_style_boards
     , a.cust_service_gift_wrapping
     , a.cust_service_pop_in
     , a.ns_buyerflow
     , a.ncom_buyerflow
     , a.rs_buyerflow
     , a.rcom_buyerflow
     , a.nord_buyerflow
     , a.rack_buyerflow
     , a.jwn_buyerflow
     , a.cust_jwn_acquired_aare
     , a.cust_jwn_activated_aare
     , a.cust_jwn_retained_aare
     , a.cust_jwn_engaged_aare
     , a.shopped_fls
     , a.shopped_ncom
     , a.shopped_rack
     , a.shopped_rcom
     , a.shopped_fp
     , a.shopped_op
     , a.shopped_jwn
     , a.cust_made_a_return
     , a.cust_platform_desktop
     , a.cust_platform_MOW
     , a.cust_platform_IOS
     , a.cust_platform_Android
     , a.cust_platform_POS
     , coalesce(h.ncom_sts,0) cust_service_ship2store_ncom
     , coalesce(h.rcom_sts,0) cust_service_ship2store_rcom
     , case when d.restaurant_max=1 and d.restaurant_min=1 then 1 else 0 end restaurant_only_flag
     , coalesce(g.closest_store_dist_bucket,'missing') closest_store_dist_bucket
     , g.closest_store_banner
     , coalesce(g.nord_sol_dist_bucket,'missing') nord_sol_dist_bucket
     , coalesce(g.rack_sol_dist_bucket,'missing') rack_sol_dist_bucket
     , d.shopped_nord_local
     --, c.cust_made_a_return
     , coalesce(d.ncom_ret_to_nstore,0) ncom_ret_to_nstore
     , coalesce(d.ncom_ret_by_mail,0) ncom_ret_by_mail
     , coalesce(d.ncom_ret_to_rstore,0) ncom_ret_to_rstore
     , coalesce(d.rcom_ret_to_nstore,0) rcom_ret_to_nstore
     , coalesce(d.rcom_ret_by_mail,0) rcom_ret_by_mail
     , coalesce(d.rcom_ret_to_rstore,0) rcom_ret_to_rstore
     , cust_Nord_Shoes_net_spend_ry
     , cust_Nord_Beauty_net_spend_ry
     , cust_Nord_Designer_Apparel_net_spend_ry
     , cust_Nord_Apparel_net_spend_ry
     , cust_Nord_Accessories_net_spend_ry
     , cust_Nord_Home_net_spend_ry
     , cust_Nord_Merch_Projects_net_spend_ry
     , cust_Nord_Leased_Boutique_net_spend_ry
     , cust_Nord_Restaurant_net_spend_ry
     , cust_Rack_Shoes_net_spend_ry
     , cust_Rack_Beauty_net_spend_ry
     , cust_Rack_Designer_Apparel_net_spend_ry
     , cust_Rack_Apparel_net_spend_ry
     , cust_Rack_Accessories_net_spend_ry
     , cust_Rack_Home_net_spend_ry
     , cust_Rack_Merch_Projects_net_spend_ry
     , cust_Rack_Leased_Boutique_net_spend_ry
     , cust_Rack_Restaurant_net_spend_ry
     , cust_Nord_RegPrice_net_spend_ry
     , cust_Nord_Promo_net_spend_ry
     , cust_Nord_Clearance_net_spend_ry
     , cust_Rack_RegPrice_net_spend_ry
     , cust_Rack_Promo_net_spend_ry
     , cust_Rack_Clearance_net_spend_ry
from cust_year_level_attributes a
left join cancel_custs b on a.acp_id = b.acp_id and a.reporting_year_shopped = b.reporting_year_shopped
left join cust_spend_div_price d on a.acp_id = d.acp_id and a.reporting_year_shopped = d.reporting_year_shopped
left join T2DL_DAS_STRATEGY.customer_store_distance_buckets g on a.acp_id = g.acp_id
left join ship2store_cust_year h on a.acp_id=h.acp_id and a.reporting_year_shopped = h.reporting_year_shopped
) with data primary index(acp_id,reporting_year_shopped) on commit preserve rows;

/************************************************************************************
 * PART A-7) Trip-Dept level table with all transaction,product attributes
 * Also, splitting this into 3 inserts to workaround any potential timeouts
 ************************************************************************************/

create multiset volatile table cust_trx_everything as (
select cli.data_source
     , cli.acp_id
     , cli.banner
     , cli.channel
     , cli.channel_country
     , cli.store_num
     , cli.store_region
     , cli.div_num
     , cli.subdiv_num
     , cli.dept_num
     , cli.npg_flag
     --, cli.price_type
     , cli.brand_name
     , cli.store_country
     , cli.date_shopped
     , cli.fiscal_month_num
     , cli.fiscal_qtr_num
     , cli.fiscal_yr_num
     , cli.item_delivery_method
     , cli.employee_flag
     , cli.reporting_year_shopped
     , case when cli.gross_sales > 0 then cli.acp_id||cli.store_num||cli.date_shopped else null end trip_id
     , sum(cli.gross_sales) as gross_sales
     , sum(cli.return_amt) as return_amt
     , sum(cli.net_sales) as net_sales
     , sum(cli.gross_items) as gross_items
     , sum(cli.return_items) as return_items
     , sum(cli.net_items) as net_items
     , sum(case when price_type = 'R' then cli.net_sales   else 0 end) RegPrice_net_spend
     , sum(case when price_type = 'P' then cli.net_sales   else 0 end) Promo_net_spend
     , sum(case when price_type = 'C' then cli.net_sales   else 0 end) Clearance_net_spend
     , sum(case when price_type = 'R' then cli.gross_sales else 0 end) RegPrice_gross_spend
     , sum(case when price_type = 'P' then cli.gross_sales else 0 end) Promo_gross_spend
     , sum(case when price_type = 'C' then cli.gross_sales else 0 end) Clearance_gross_spend
     , sum(case when price_type = 'R' then cli.return_amt  else 0 end) RegPrice_return_amt
     , sum(case when price_type = 'P' then cli.return_amt  else 0 end) Promo_return_amt
     , sum(case when price_type = 'C' then cli.return_amt  else 0 end) Clearance_return_amt
     , sum(case when price_type = 'R' then cli.net_items   else 0 end) RegPrice_net_items
     , sum(case when price_type = 'P' then cli.net_items   else 0 end) Promo_net_items
     , sum(case when price_type = 'C' then cli.net_items   else 0 end) Clearance_net_items
from T2DL_DAS_STRATEGY.cco_line_items cli
where reporting_year_shopped is not null
  and date_shopped between (select start_date from date_range) and (select p2_start_date from date_range)
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
) with data primary index(acp_id,reporting_year_shopped, trip_id, dept_num) on commit preserve rows;

insert into cust_trx_everything 
select cli.data_source
     , cli.acp_id
     , cli.banner
     , cli.channel
     , cli.channel_country
     , cli.store_num
     , cli.store_region
     , cli.div_num
     , cli.subdiv_num
     , cli.dept_num
     , cli.npg_flag
     --, cli.price_type
     , cli.brand_name
     , cli.store_country
     , cli.date_shopped
     , cli.fiscal_month_num
     , cli.fiscal_qtr_num
     , cli.fiscal_yr_num
     , cli.item_delivery_method
     , cli.employee_flag
     , cli.reporting_year_shopped
     , case when cli.gross_sales > 0 then cli.acp_id||cli.store_num||cli.date_shopped else null end trip_id
     , sum(cli.gross_sales) as gross_sales
     , sum(cli.return_amt) as return_amt
     , sum(cli.net_sales) as net_sales
     , sum(cli.gross_items) as gross_items
     , sum(cli.return_items) as return_items
     , sum(cli.net_items) as net_items
     , sum(case when price_type = 'R' then cli.net_sales   else 0 end) RegPrice_net_spend
     , sum(case when price_type = 'P' then cli.net_sales   else 0 end) Promo_net_spend
     , sum(case when price_type = 'C' then cli.net_sales   else 0 end) Clearance_net_spend
     , sum(case when price_type = 'R' then cli.gross_sales else 0 end) RegPrice_gross_spend
     , sum(case when price_type = 'P' then cli.gross_sales else 0 end) Promo_gross_spend
     , sum(case when price_type = 'C' then cli.gross_sales else 0 end) Clearance_gross_spend
     , sum(case when price_type = 'R' then cli.return_amt  else 0 end) RegPrice_return_amt
     , sum(case when price_type = 'P' then cli.return_amt  else 0 end) Promo_return_amt
     , sum(case when price_type = 'C' then cli.return_amt  else 0 end) Clearance_return_amt
     , sum(case when price_type = 'R' then cli.net_items   else 0 end) RegPrice_net_items
     , sum(case when price_type = 'P' then cli.net_items   else 0 end) Promo_net_items
     , sum(case when price_type = 'C' then cli.net_items   else 0 end) Clearance_net_items
from T2DL_DAS_STRATEGY.cco_line_items cli
where reporting_year_shopped is not null
  and date_shopped between (select p2_start_date + 1 from date_range) and (select p2_end_date from date_range)
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21;

insert into cust_trx_everything 
select cli.data_source
     , cli.acp_id
     , cli.banner
     , cli.channel
     , cli.channel_country
     , cli.store_num
     , cli.store_region
     , cli.div_num
     , cli.subdiv_num
     , cli.dept_num
     , cli.npg_flag
     --, cli.price_type
     , cli.brand_name
     , cli.store_country
     , cli.date_shopped
     , cli.fiscal_month_num
     , cli.fiscal_qtr_num
     , cli.fiscal_yr_num
     , cli.item_delivery_method
     , cli.employee_flag
     , cli.reporting_year_shopped
     , case when cli.gross_sales > 0 then cli.acp_id||cli.store_num||cli.date_shopped else null end trip_id
     , sum(cli.gross_sales) as gross_sales
     , sum(cli.return_amt) as return_amt
     , sum(cli.net_sales) as net_sales
     , sum(cli.gross_items) as gross_items
     , sum(cli.return_items) as return_items
     , sum(cli.net_items) as net_items
     , sum(case when price_type = 'R' then cli.net_sales   else 0 end) RegPrice_net_spend
     , sum(case when price_type = 'P' then cli.net_sales   else 0 end) Promo_net_spend
     , sum(case when price_type = 'C' then cli.net_sales   else 0 end) Clearance_net_spend
     , sum(case when price_type = 'R' then cli.gross_sales else 0 end) RegPrice_gross_spend
     , sum(case when price_type = 'P' then cli.gross_sales else 0 end) Promo_gross_spend
     , sum(case when price_type = 'C' then cli.gross_sales else 0 end) Clearance_gross_spend
     , sum(case when price_type = 'R' then cli.return_amt  else 0 end) RegPrice_return_amt
     , sum(case when price_type = 'P' then cli.return_amt  else 0 end) Promo_return_amt
     , sum(case when price_type = 'C' then cli.return_amt  else 0 end) Clearance_return_amt
     , sum(case when price_type = 'R' then cli.net_items   else 0 end) RegPrice_net_items
     , sum(case when price_type = 'P' then cli.net_items   else 0 end) Promo_net_items
     , sum(case when price_type = 'C' then cli.net_items   else 0 end) Clearance_net_items
from T2DL_DAS_STRATEGY.cco_line_items cli
where reporting_year_shopped is not null
  and date_shopped between (select p2_end_date + 1 from date_range) and (select end_date from date_range)
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21;

/************************************************************************************
 * PART A-8) Delete & insert data into the final table
 * Also, splitting this into 3 inserts to workaround any potential timeouts
 ************************************************************************************/

delete from {str_t2_schema}.customer_sandbox_fact;

-- Temporary table to store the current timestamp
create volatile table current_timestamp_tmp as (
select cast(current_timestamp(6) as timestamp(6))
    as dw_sys_tmstp
) with data on commit preserve rows;

-- Insert part 1
insert into {str_t2_schema}.customer_sandbox_fact
select a.data_source
     , a.acp_id
     , banner_num as store_banner_num
     , channel_num as store_channel_num
     , channel_country_num as store_channel_country
     , a.store_num
     , store_region_num as store_region_num
     , coalesce(a.div_num,-1) as div_num
     , coalesce(a.subdiv_num,-1) as subdiv_num
     , coalesce(a.dept_num,-1) as dept_num
     , a.npg_flag --
     , coalesce(a.brand_name,'Others') as brand_name
     , a.date_shopped
     , a.fiscal_month_num as month_num
     , a.fiscal_qtr_num as quarter_num
     , a.fiscal_yr_num as year_num
     , a.trip_id
     , a.gross_sales
     , a.return_amt
     , a.net_sales
     , a.gross_items
     , a.return_items
     , a.net_items
     , a.RegPrice_net_spend
     , a.Promo_net_spend
     , a.Clearance_net_spend
     , a.RegPrice_gross_spend
     , a.Promo_gross_spend
     , a.Clearance_gross_spend
     , a.RegPrice_return_amt
     , a.Promo_return_amt
     , a.Clearance_return_amt
     , a.RegPrice_net_items
     , a.Promo_net_items
     , a.Clearance_net_items
     , item_delivery_method_num
     , a.employee_flag
     , ry.reporting_year_shopped_num
     , t.audience_engagement_cohort_num
     , cust_jwn_trip_bucket_num
     , cust_jwn_net_spend_bucket_num
     , NULL as cust_jwn_net_spend_bucket_ly_num
     , NULL as cust_jwn_trip_bucket_ly_num
     , NULL as cust_jwn_net_spend_bucket_fy_num
     , NULL as cust_jwn_trip_bucket_num_fy_num
     , NULL as audience_engagement_cohort_ly_num
     , NULL as audience_engagement_cohort_fy_num
     , NULL as cust_rack_trip_bucket_num
     , NULL as cust_rack_net_spend_bucket_num
     , NULL as cust_nord_trip_bucket_num
     , NULL as cust_nord_net_spend_bucket_num
     , cust_channel_combo_num
     , cust_loyalty_type_num
     , cust_loyalty_level_num
     --, b.cust_jwn_buyer_flow
     , b.cust_gender
     , cust_age_group_num
     , b.cust_employee_flag
     , cust_tenure_bucket_years_num
     --, b.cust_jwn_acquired_aare
     --, b.cust_jwn_activated_aare
     --, b.cust_jwn_retained_aare
     --, b.cust_jwn_engaged_aare
     , b.cust_acquisition_fiscal_year
     , f.cust_channel_num as cust_acquisition_channel_num
     , j.cust_channel_num as cust_activation_channel_num
     --, b.cust_platform_WEB
     --, b.cust_platform_APP
     , cust_NMS_market_num
     , cust_NMS_region_num
     --, case when b.reporting_year_shopped not in ('FY-19','FY-20') then b.cust_contr_margin_decile
     --       else null end cust_contr_margin_decile
     --, b.cust_contr_margin_amt
     , m.cust_dma_num
     , m.cust_region_num
     , m.cust_country_num
     , b.cust_tender_nordstrom
     , b.cust_tender_nordstrom_note
     , b.cust_tender_gift_card
     , b.cust_tender_other
     , b.cust_had_a_cancel
     , b.cust_svc_group_exp_delivery
     , b.cust_svc_group_order_pickup
     , b.cust_svc_group_selling_relation
     , b.cust_svc_group_remote_selling
     , b.cust_svc_group_alterations
     , b.cust_svc_group_in_store
     , b.cust_svc_group_restaurant
     , b.cust_service_free_exp_delivery
     , b.cust_service_next_day_pickup
     , b.cust_service_same_day_bopus
     , b.cust_service_curbside_pickup
     , b.cust_service_style_boards
     , b.cust_service_gift_wrapping
     , NULL as cust_styling
     , b.cust_service_pop_in
     , u.cust_chan_buyer_flow_num as cust_ns_buyerflow_num
     , v.cust_chan_buyer_flow_num as cust_ncom_buyerflow_num
     , w.cust_chan_buyer_flow_num as cust_rs_buyerflow_num
     , x.cust_chan_buyer_flow_num as cust_rcom_buyerflow_num
     , y.cust_chan_buyer_flow_num as cust_nord_buyerflow_num
     , z.cust_chan_buyer_flow_num as cust_rack_buyerflow_num
     , aa.cust_chan_buyer_flow_num as cust_jwn_buyerflow_num
     , b.cust_jwn_acquired_aare
     , b.cust_jwn_activated_aare
     , b.cust_jwn_retained_aare
     , b.cust_jwn_engaged_aare
     , b.shopped_fls
     , b.shopped_ncom
     , b.shopped_rack
     , b.shopped_rcom
     , b.shopped_fp
     , b.shopped_op
     , b.shopped_jwn
     , b.cust_made_a_return
     , b.cust_platform_desktop
     , b.cust_platform_MOW
     , b.cust_platform_IOS
     , b.cust_platform_Android
     , b.cust_platform_POS
     , b.cust_service_ship2store_ncom
     , b.cust_service_ship2store_rcom
     , b.restaurant_only_flag
     , closest_sol_dist_bucket_num
     , closest_store_banner_num
     , nord_sol_dist_bucket_num
     , rack_sol_dist_bucket_num
     , b.shopped_nord_local
     , b.ncom_ret_to_nstore
     , b.ncom_ret_by_mail
     , b.ncom_ret_to_rstore
     , b.rcom_ret_to_nstore
     , b.rcom_ret_by_mail
     , b.rcom_ret_to_rstore
     , b.cust_Nord_Shoes_net_spend_ry
     , b.cust_Nord_Beauty_net_spend_ry
     , b.cust_Nord_Designer_Apparel_net_spend_ry
     , b.cust_Nord_Apparel_net_spend_ry
     , b.cust_Nord_Accessories_net_spend_ry
     , b.cust_Nord_Home_net_spend_ry
     , b.cust_Nord_Merch_Projects_net_spend_ry
     , b.cust_Nord_Leased_Boutique_net_spend_ry
     , b.cust_Nord_Restaurant_net_spend_ry
     , b.cust_Rack_Shoes_net_spend_ry
     , b.cust_Rack_Beauty_net_spend_ry
     , b.cust_Rack_Designer_Apparel_net_spend_ry
     , b.cust_Rack_Apparel_net_spend_ry
     , b.cust_Rack_Accessories_net_spend_ry
     , b.cust_Rack_Home_net_spend_ry
     , b.cust_Rack_Merch_Projects_net_spend_ry
     , b.cust_Rack_Leased_Boutique_net_spend_ry
     , b.cust_Rack_Restaurant_net_spend_ry
     , b.cust_Nord_RegPrice_net_spend_ry
     , b.cust_Nord_Promo_net_spend_ry
     , b.cust_Nord_Clearance_net_spend_ry
     , b.cust_Rack_RegPrice_net_spend_ry
     , b.cust_Rack_Promo_net_spend_ry
     , b.cust_Rack_Clearance_net_spend_ry
     , b.cust_chan_holiday
     , b.cust_chan_anniversary
     , NULL as cust_contr_margin_decile
     , NULL as cust_contr_margin_amt
     , (select dw_sys_tmstp from current_timestamp_tmp) as dw_sys_load_tmstp
     , (select dw_sys_tmstp from current_timestamp_tmp) as dw_sys_updt_tmstp
from cust_trx_everything a
join cust_year_everything b on a.acp_id = b.acp_id and a.reporting_year_shopped = b.reporting_year_shopped
left join t2dl_das_strategy.reporting_year_shopped_lkp ry on coalesce(a.reporting_year_shopped, 'Missing') = ry.reporting_year_shopped_desc
left join t2dl_das_strategy.cust_jwn_trip_bucket_lkp c on coalesce(b.cust_jwn_trip_bucket,'Unknown') = c.cust_jwn_trip_bucket_desc
left join t2dl_das_strategy.cust_age_group_lkp d on coalesce(b.cust_age_group,'Unknown') = d.cust_age_group_desc
left join t2dl_das_strategy.cust_tenure_bucket_years_lkp e on coalesce(b.cust_tenure_bucket_years,'Unknown') = e.cust_tenure_bucket_years_desc
left join t2dl_das_strategy.cust_channel_lkp f on coalesce(b.cust_acquisition_channel,'Unknown') = f.cust_channel_desc
left join t2dl_das_strategy.cust_jwn_net_spend_bucket_lkp g on coalesce(b.cust_jwn_net_spend_bucket,'Unknown') = g.cust_jwn_net_spend_bucket_desc
left join t2dl_das_strategy.cust_channel_combo_lkp h on coalesce(b.cust_channel_combo, 'Unknown') = h.cust_channel_combo_desc
left join t2dl_das_strategy.cust_loyalty_type_lkp i on coalesce(b.cust_loyalty_type,'Unknown') = i.cust_loyalty_type_desc
left join t2dl_das_strategy.cust_channel_lkp j on coalesce(b.cust_activation_channel,'Unknown') = j.cust_channel_desc
left join t2dl_das_strategy.cust_NMS_market_lkp k on coalesce(b.cust_NMS_market,'NON-NMS MARKET') = k.cust_NMS_market_desc
left join t2dl_das_strategy.cust_loyalty_level_lkp l on coalesce(b.cust_loyalty_level,'Unknown') = l.cust_loyalty_level_desc
left join t2dl_das_strategy.cust_dma_lkp m on coalesce(b.cust_dma,'Unknown') = m.cust_dma_desc
left join t2dl_das_strategy.rack_sol_dist_bucket_lkp n on coalesce(b.rack_sol_dist_bucket,'missing') = n.rack_sol_dist_bucket_desc
left join t2dl_das_strategy.nord_sol_dist_bucket_lkp o on coalesce(b.nord_sol_dist_bucket,'missing') = o.nord_sol_dist_bucket_desc
left join t2dl_das_strategy.closest_store_banner_lkp p on coalesce(b.closest_store_banner,'missing') = p.closest_store_banner_desc
left join t2dl_das_strategy.closest_sol_dist_bucket_lkp q on coalesce(b.closest_store_dist_bucket,'missing') = q.closest_sol_dist_bucket_desc
left join t2dl_das_strategy.item_delivery_method_lkp r on coalesce(a.item_delivery_method,'NA') = r.item_delivery_method_desc
left join t2dl_das_strategy.store_lkp s on coalesce(a.store_num,'Unknown') = s.store_num and s.channel_country_desc = a.channel_country
left join t2dl_das_strategy.audience_engagement_cohort_lkp t on coalesce(b.engagement_cohort, 'missing') = t.audience_engagement_cohort_desc
left join t2dl_das_strategy.cust_chan_buyer_flow_lkp u on u.cust_chan_buyer_flow_desc = coalesce(b.ns_buyerflow ,'NA')
left join t2dl_das_strategy.cust_chan_buyer_flow_lkp v on v.cust_chan_buyer_flow_desc = coalesce(b.ncom_buyerflow,'NA')
left join t2dl_das_strategy.cust_chan_buyer_flow_lkp w on w.cust_chan_buyer_flow_desc = coalesce(b.rs_buyerflow,'NA')
left join t2dl_das_strategy.cust_chan_buyer_flow_lkp x on x.cust_chan_buyer_flow_desc = coalesce(b.rcom_buyerflow,'NA')
left join t2dl_das_strategy.cust_chan_buyer_flow_lkp y on y.cust_chan_buyer_flow_desc = coalesce(b.nord_buyerflow,'NA')
left join t2dl_das_strategy.cust_chan_buyer_flow_lkp z on z.cust_chan_buyer_flow_desc = coalesce(b.rack_buyerflow,'NA')
left join t2dl_das_strategy.cust_chan_buyer_flow_lkp aa on aa.cust_chan_buyer_flow_desc = coalesce(b.jwn_buyerflow,'NA')
where date_shopped between (select start_date from date_range) and (select p2_start_date from date_range)
;

-- Insert part 2
insert into {str_t2_schema}.customer_sandbox_fact
select a.data_source
     , a.acp_id
     , banner_num as store_banner_num
     , channel_num as store_channel_num
     , channel_country_num as store_channel_country
     , a.store_num
     , store_region_num as store_region_num
     , coalesce(a.div_num,-1) as div_num
     , coalesce(a.subdiv_num,-1) as subdiv_num
     , coalesce(a.dept_num,-1) as dept_num
     , a.npg_flag
     , coalesce(a.brand_name,'Others') as brand_name
     , a.date_shopped
     , a.fiscal_month_num as month_num
     , a.fiscal_qtr_num as quarter_num
     , a.fiscal_yr_num as year_num
     , a.trip_id
     , a.gross_sales
     , a.return_amt
     , a.net_sales
     , a.gross_items
     , a.return_items
     , a.net_items
     , a.RegPrice_net_spend
     , a.Promo_net_spend
     , a.Clearance_net_spend
     , a.RegPrice_gross_spend
     , a.Promo_gross_spend
     , a.Clearance_gross_spend
     , a.RegPrice_return_amt
     , a.Promo_return_amt
     , a.Clearance_return_amt
     , a.RegPrice_net_items
     , a.Promo_net_items
     , a.Clearance_net_items
     , item_delivery_method_num
     , a.employee_flag
     , ry.reporting_year_shopped_num
     , t.audience_engagement_cohort_num
     , cust_jwn_trip_bucket_num
     , cust_jwn_net_spend_bucket_num
     , NULL as cust_jwn_net_spend_bucket_ly_num
     , NULL as cust_jwn_trip_bucket_ly_num
     , NULL as cust_jwn_net_spend_bucket_fy_num
     , NULL as cust_jwn_trip_bucket_num_fy_num
     , NULL as audience_engagement_cohort_ly_num
     , NULL as audience_engagement_cohort_fy_num
     , NULL as cust_rack_trip_bucket_num
     , NULL as cust_rack_net_spend_bucket_num
     , NULL as cust_nord_trip_bucket_num
     , NULL as cust_nord_net_spend_bucket_num
     , cust_channel_combo_num
     , cust_loyalty_type_num
     , cust_loyalty_level_num
     --, b.cust_jwn_buyer_flow
     , b.cust_gender
     , cust_age_group_num
     , b.cust_employee_flag
     , cust_tenure_bucket_years_num
     --, b.cust_jwn_acquired_aare
     --, b.cust_jwn_activated_aare
     --, b.cust_jwn_retained_aare
     --, b.cust_jwn_engaged_aare
     , b.cust_acquisition_fiscal_year
     , f.cust_channel_num as cust_acquisition_channel_num
     , j.cust_channel_num as cust_activation_channel_num
     --, b.cust_platform_WEB
     --, b.cust_platform_APP
     , cust_NMS_market_num
     , cust_NMS_region_num
     --, case when b.reporting_year_shopped not in ('FY-19','FY-20') then b.cust_contr_margin_decile
     --       else null end cust_contr_margin_decile
     --, b.cust_contr_margin_amt
     , m.cust_dma_num
     , m.cust_region_num
     , m.cust_country_num
     , b.cust_tender_nordstrom
     , b.cust_tender_nordstrom_note
     , b.cust_tender_gift_card
     , b.cust_tender_other
     , b.cust_had_a_cancel
     , b.cust_svc_group_exp_delivery
     , b.cust_svc_group_order_pickup
     , b.cust_svc_group_selling_relation
     , b.cust_svc_group_remote_selling
     , b.cust_svc_group_alterations
     , b.cust_svc_group_in_store
     , b.cust_svc_group_restaurant
     , b.cust_service_free_exp_delivery
     , b.cust_service_next_day_pickup
     , b.cust_service_same_day_bopus
     , b.cust_service_curbside_pickup
     , b.cust_service_style_boards
     , b.cust_service_gift_wrapping
     , NULL as cust_styling
     , b.cust_service_pop_in
     , u.cust_chan_buyer_flow_num as cust_ns_buyerflow_num
     , v.cust_chan_buyer_flow_num as cust_ncom_buyerflow_num
     , w.cust_chan_buyer_flow_num as cust_rs_buyerflow_num
     , x.cust_chan_buyer_flow_num as cust_rcom_buyerflow_num
     , y.cust_chan_buyer_flow_num as cust_nord_buyerflow_num
     , z.cust_chan_buyer_flow_num as cust_rack_buyerflow_num
     , aa.cust_chan_buyer_flow_num as cust_jwn_buyerflow_num
     , b.cust_jwn_acquired_aare
     , b.cust_jwn_activated_aare
     , b.cust_jwn_retained_aare
     , b.cust_jwn_engaged_aare
     , b.shopped_fls
     , b.shopped_ncom
     , b.shopped_rack
     , b.shopped_rcom
     , b.shopped_fp
     , b.shopped_op
     , b.shopped_jwn
     , b.cust_made_a_return
     , b.cust_platform_desktop
     , b.cust_platform_MOW
     , b.cust_platform_IOS
     , b.cust_platform_Android
     , b.cust_platform_POS
     , b.cust_service_ship2store_ncom
     , b.cust_service_ship2store_rcom
     , b.restaurant_only_flag
     , closest_sol_dist_bucket_num
     , closest_store_banner_num
     , nord_sol_dist_bucket_num
     , rack_sol_dist_bucket_num
     , b.shopped_nord_local
     , b.ncom_ret_to_nstore
     , b.ncom_ret_by_mail
     , b.ncom_ret_to_rstore
     , b.rcom_ret_to_nstore
     , b.rcom_ret_by_mail
     , b.rcom_ret_to_rstore
     , b.cust_Nord_Shoes_net_spend_ry
     , b.cust_Nord_Beauty_net_spend_ry
     , b.cust_Nord_Designer_Apparel_net_spend_ry
     , b.cust_Nord_Apparel_net_spend_ry
     , b.cust_Nord_Accessories_net_spend_ry
     , b.cust_Nord_Home_net_spend_ry
     , b.cust_Nord_Merch_Projects_net_spend_ry
     , b.cust_Nord_Leased_Boutique_net_spend_ry
     , b.cust_Nord_Restaurant_net_spend_ry
     , b.cust_Rack_Shoes_net_spend_ry
     , b.cust_Rack_Beauty_net_spend_ry
     , b.cust_Rack_Designer_Apparel_net_spend_ry
     , b.cust_Rack_Apparel_net_spend_ry
     , b.cust_Rack_Accessories_net_spend_ry
     , b.cust_Rack_Home_net_spend_ry
     , b.cust_Rack_Merch_Projects_net_spend_ry
     , b.cust_Rack_Leased_Boutique_net_spend_ry
     , b.cust_Rack_Restaurant_net_spend_ry
     , b.cust_Nord_RegPrice_net_spend_ry
     , b.cust_Nord_Promo_net_spend_ry
     , b.cust_Nord_Clearance_net_spend_ry
     , b.cust_Rack_RegPrice_net_spend_ry
     , b.cust_Rack_Promo_net_spend_ry
     , b.cust_Rack_Clearance_net_spend_ry
     , b.cust_chan_holiday
     , b.cust_chan_anniversary
     , NULL as cust_contr_margin_decile
     , NULL as cust_contr_margin_amt
     , (select dw_sys_tmstp from current_timestamp_tmp) as dw_sys_load_tmstp
     , (select dw_sys_tmstp from current_timestamp_tmp) as dw_sys_updt_tmstp
from cust_trx_everything a
join cust_year_everything b on a.acp_id = b.acp_id and a.reporting_year_shopped = b.reporting_year_shopped
left join t2dl_das_strategy.reporting_year_shopped_lkp ry on coalesce(a.reporting_year_shopped, 'Missing') = ry.reporting_year_shopped_desc
left join t2dl_das_strategy.cust_jwn_trip_bucket_lkp c on coalesce(b.cust_jwn_trip_bucket,'Unknown') = c.cust_jwn_trip_bucket_desc
left join t2dl_das_strategy.cust_age_group_lkp d on coalesce(b.cust_age_group,'Unknown') = d.cust_age_group_desc
left join t2dl_das_strategy.cust_tenure_bucket_years_lkp e on coalesce(b.cust_tenure_bucket_years,'Unknown') = e.cust_tenure_bucket_years_desc
left join t2dl_das_strategy.cust_channel_lkp f on coalesce(b.cust_acquisition_channel,'Unknown') = f.cust_channel_desc
left join t2dl_das_strategy.cust_jwn_net_spend_bucket_lkp g on coalesce(b.cust_jwn_net_spend_bucket,'Unknown') = g.cust_jwn_net_spend_bucket_desc
left join t2dl_das_strategy.cust_channel_combo_lkp h on coalesce(b.cust_channel_combo, 'Unknown') = h.cust_channel_combo_desc
left join t2dl_das_strategy.cust_loyalty_type_lkp i on coalesce(b.cust_loyalty_type,'Unknown') = i.cust_loyalty_type_desc
left join t2dl_das_strategy.cust_channel_lkp j on coalesce(b.cust_activation_channel,'Unknown') = j.cust_channel_desc
left join t2dl_das_strategy.cust_NMS_market_lkp k on coalesce(b.cust_NMS_market,'NON-NMS MARKET') = k.cust_NMS_market_desc
left join t2dl_das_strategy.cust_loyalty_level_lkp l on coalesce(b.cust_loyalty_level,'Unknown') = l.cust_loyalty_level_desc
left join t2dl_das_strategy.cust_dma_lkp m on coalesce(b.cust_dma,'Unknown') = m.cust_dma_desc
left join t2dl_das_strategy.rack_sol_dist_bucket_lkp n on coalesce(b.rack_sol_dist_bucket,'missing') = n.rack_sol_dist_bucket_desc
left join t2dl_das_strategy.nord_sol_dist_bucket_lkp o on coalesce(b.nord_sol_dist_bucket,'missing') = o.nord_sol_dist_bucket_desc
left join t2dl_das_strategy.closest_store_banner_lkp p on coalesce(b.closest_store_banner,'missing') = p.closest_store_banner_desc
left join t2dl_das_strategy.closest_sol_dist_bucket_lkp q on coalesce(b.closest_store_dist_bucket,'missing') = q.closest_sol_dist_bucket_desc
left join t2dl_das_strategy.item_delivery_method_lkp r on coalesce(a.item_delivery_method,'NA') = r.item_delivery_method_desc
left join t2dl_das_strategy.store_lkp s on coalesce(a.store_num,'Unknown') = s.store_num and s.channel_country_desc = a.channel_country
left join t2dl_das_strategy.audience_engagement_cohort_lkp t on coalesce(b.engagement_cohort, 'missing') = t.audience_engagement_cohort_desc
left join t2dl_das_strategy.cust_chan_buyer_flow_lkp u on u.cust_chan_buyer_flow_desc = coalesce(b.ns_buyerflow ,'NA')
left join t2dl_das_strategy.cust_chan_buyer_flow_lkp v on v.cust_chan_buyer_flow_desc = coalesce(b.ncom_buyerflow,'NA')
left join t2dl_das_strategy.cust_chan_buyer_flow_lkp w on w.cust_chan_buyer_flow_desc = coalesce(b.rs_buyerflow,'NA')
left join t2dl_das_strategy.cust_chan_buyer_flow_lkp x on x.cust_chan_buyer_flow_desc = coalesce(b.rcom_buyerflow,'NA')
left join t2dl_das_strategy.cust_chan_buyer_flow_lkp y on y.cust_chan_buyer_flow_desc = coalesce(b.nord_buyerflow,'NA')
left join t2dl_das_strategy.cust_chan_buyer_flow_lkp z on z.cust_chan_buyer_flow_desc = coalesce(b.rack_buyerflow,'NA')
left join t2dl_das_strategy.cust_chan_buyer_flow_lkp aa on aa.cust_chan_buyer_flow_desc = coalesce(b.jwn_buyerflow,'NA')
where date_shopped between (select p2_start_date + 1 from date_range) and (select p2_end_date from date_range)
;

--Insert part 3
insert into {str_t2_schema}.customer_sandbox_fact
select a.data_source
     , a.acp_id
     , banner_num as store_banner_num
     , channel_num as store_channel_num
     , channel_country_num as store_channel_country
     , a.store_num
     , store_region_num as store_region_num
     , coalesce(a.div_num,-1) as div_num
     , coalesce(a.subdiv_num,-1) as subdiv_num
     , coalesce(a.dept_num,-1) as dept_num
     , a.npg_flag
     , coalesce(a.brand_name,'Others') as brand_name
     , a.date_shopped
     , a.fiscal_month_num as month_num
     , a.fiscal_qtr_num as quarter_num
     , a.fiscal_yr_num as year_num
     , a.trip_id
     , a.gross_sales
     , a.return_amt
     , a.net_sales
     , a.gross_items
     , a.return_items
     , a.net_items
     , a.RegPrice_net_spend
     , a.Promo_net_spend
     , a.Clearance_net_spend
     , a.RegPrice_gross_spend
     , a.Promo_gross_spend
     , a.Clearance_gross_spend
     , a.RegPrice_return_amt
     , a.Promo_return_amt
     , a.Clearance_return_amt
     , a.RegPrice_net_items
     , a.Promo_net_items
     , a.Clearance_net_items
     , item_delivery_method_num
     , a.employee_flag
     , ry.reporting_year_shopped_num
     , t.audience_engagement_cohort_num
     , cust_jwn_trip_bucket_num
     , cust_jwn_net_spend_bucket_num
     , NULL as cust_jwn_net_spend_bucket_ly_num
     , NULL as cust_jwn_trip_bucket_ly_num
     , NULL as cust_jwn_net_spend_bucket_fy_num
     , NULL as cust_jwn_trip_bucket_num_fy_num
     , NULL as audience_engagement_cohort_ly_num
     , NULL as audience_engagement_cohort_fy_num
     , NULL as cust_rack_trip_bucket_num
     , NULL as cust_rack_net_spend_bucket_num
     , NULL as cust_nord_trip_bucket_num
     , NULL as cust_nord_net_spend_bucket_num
     , cust_channel_combo_num
     , cust_loyalty_type_num
     , cust_loyalty_level_num
     --, b.cust_jwn_buyer_flow
     , b.cust_gender
     , cust_age_group_num
     , b.cust_employee_flag
     , cust_tenure_bucket_years_num
     --, b.cust_jwn_acquired_aare
     --, b.cust_jwn_activated_aare
     --, b.cust_jwn_retained_aare
     --, b.cust_jwn_engaged_aare
     , b.cust_acquisition_fiscal_year
     , f.cust_channel_num as cust_acquisition_channel_num
     , j.cust_channel_num as cust_activation_channel_num
     --, b.cust_platform_WEB
     --, b.cust_platform_APP
     , cust_NMS_market_num
     , cust_NMS_region_num
     --, case when b.reporting_year_shopped not in ('FY-19','FY-20') then b.cust_contr_margin_decile
     --       else null end cust_contr_margin_decile
     -- ,b.cust_contr_margin_amt
     , m.cust_dma_num
     , m.cust_region_num
     , m.cust_country_num
     , b.cust_tender_nordstrom
     , b.cust_tender_nordstrom_note
     , b.cust_tender_gift_card
     , b.cust_tender_other
     , b.cust_had_a_cancel
     , b.cust_svc_group_exp_delivery
     , b.cust_svc_group_order_pickup
     , b.cust_svc_group_selling_relation
     , b.cust_svc_group_remote_selling
     , b.cust_svc_group_alterations
     , b.cust_svc_group_in_store
     , b.cust_svc_group_restaurant
     , b.cust_service_free_exp_delivery
     , b.cust_service_next_day_pickup
     , b.cust_service_same_day_bopus
     , b.cust_service_curbside_pickup
     , b.cust_service_style_boards
     , b.cust_service_gift_wrapping
     , NULL as cust_styling
     , b.cust_service_pop_in
     , u.cust_chan_buyer_flow_num as cust_ns_buyerflow_num
     , v.cust_chan_buyer_flow_num as cust_ncom_buyerflow_num
     , w.cust_chan_buyer_flow_num as cust_rs_buyerflow_num
     , x.cust_chan_buyer_flow_num as cust_rcom_buyerflow_num
     , y.cust_chan_buyer_flow_num as cust_nord_buyerflow_num
     , z.cust_chan_buyer_flow_num as cust_rack_buyerflow_num
     , aa.cust_chan_buyer_flow_num as cust_jwn_buyerflow_num
     , b.cust_jwn_acquired_aare
     , b.cust_jwn_activated_aare
     , b.cust_jwn_retained_aare
     , b.cust_jwn_engaged_aare
     , b.shopped_fls
     , b.shopped_ncom
     , b.shopped_rack
     , b.shopped_rcom
     , b.shopped_fp
     , b.shopped_op
     , b.shopped_jwn
     , b.cust_made_a_return
     , b.cust_platform_desktop
     , b.cust_platform_MOW
     , b.cust_platform_IOS
     , b.cust_platform_Android
     , b.cust_platform_POS
     , b.cust_service_ship2store_ncom
     , b.cust_service_ship2store_rcom
     , b.restaurant_only_flag
     , closest_sol_dist_bucket_num
     , closest_store_banner_num
     , nord_sol_dist_bucket_num
     , rack_sol_dist_bucket_num
     , b.shopped_nord_local
     , b.ncom_ret_to_nstore
     , b.ncom_ret_by_mail
     , b.ncom_ret_to_rstore
     , b.rcom_ret_to_nstore
     , b.rcom_ret_by_mail
     , b.rcom_ret_to_rstore
     , b.cust_Nord_Shoes_net_spend_ry
     , b.cust_Nord_Beauty_net_spend_ry
     , b.cust_Nord_Designer_Apparel_net_spend_ry
     , b.cust_Nord_Apparel_net_spend_ry
     , b.cust_Nord_Accessories_net_spend_ry
     , b.cust_Nord_Home_net_spend_ry
     , b.cust_Nord_Merch_Projects_net_spend_ry
     , b.cust_Nord_Leased_Boutique_net_spend_ry
     , b.cust_Nord_Restaurant_net_spend_ry
     , b.cust_Rack_Shoes_net_spend_ry
     , b.cust_Rack_Beauty_net_spend_ry
     , b.cust_Rack_Designer_Apparel_net_spend_ry
     , b.cust_Rack_Apparel_net_spend_ry
     , b.cust_Rack_Accessories_net_spend_ry
     , b.cust_Rack_Home_net_spend_ry
     , b.cust_Rack_Merch_Projects_net_spend_ry
     , b.cust_Rack_Leased_Boutique_net_spend_ry
     , b.cust_Rack_Restaurant_net_spend_ry
     , b.cust_Nord_RegPrice_net_spend_ry
     , b.cust_Nord_Promo_net_spend_ry
     , b.cust_Nord_Clearance_net_spend_ry
     , b.cust_Rack_RegPrice_net_spend_ry
     , b.cust_Rack_Promo_net_spend_ry
     , b.cust_Rack_Clearance_net_spend_ry
     , b.cust_chan_holiday
     , b.cust_chan_anniversary
     , NULL as cust_contr_margin_decile
     , NULL as cust_contr_margin_amt
     , (select dw_sys_tmstp from current_timestamp_tmp) as dw_sys_load_tmstp
     , (select dw_sys_tmstp from current_timestamp_tmp) as dw_sys_updt_tmstp
from cust_trx_everything a
join cust_year_everything b on a.acp_id = b.acp_id and a.reporting_year_shopped = b.reporting_year_shopped
left join t2dl_das_strategy.reporting_year_shopped_lkp ry on coalesce(a.reporting_year_shopped, 'Missing') = ry.reporting_year_shopped_desc
left join t2dl_das_strategy.cust_jwn_trip_bucket_lkp c on coalesce(b.cust_jwn_trip_bucket,'Unknown') = c.cust_jwn_trip_bucket_desc
left join t2dl_das_strategy.cust_age_group_lkp d on coalesce(b.cust_age_group,'Unknown') = d.cust_age_group_desc
left join t2dl_das_strategy.cust_tenure_bucket_years_lkp e on coalesce(b.cust_tenure_bucket_years,'Unknown') = e.cust_tenure_bucket_years_desc
left join t2dl_das_strategy.cust_channel_lkp f on coalesce(b.cust_acquisition_channel,'Unknown') = f.cust_channel_desc
left join t2dl_das_strategy.cust_jwn_net_spend_bucket_lkp g on coalesce(b.cust_jwn_net_spend_bucket,'Unknown') = g.cust_jwn_net_spend_bucket_desc
left join t2dl_das_strategy.cust_channel_combo_lkp h on coalesce(b.cust_channel_combo, 'Unknown') = h.cust_channel_combo_desc
left join t2dl_das_strategy.cust_loyalty_type_lkp i on coalesce(b.cust_loyalty_type,'Unknown') = i.cust_loyalty_type_desc
left join t2dl_das_strategy.cust_channel_lkp j on coalesce(b.cust_activation_channel,'Unknown') = j.cust_channel_desc
left join t2dl_das_strategy.cust_NMS_market_lkp k on coalesce(b.cust_NMS_market,'NON-NMS MARKET') = k.cust_NMS_market_desc
left join t2dl_das_strategy.cust_loyalty_level_lkp l on coalesce(b.cust_loyalty_level,'Unknown') = l.cust_loyalty_level_desc
left join t2dl_das_strategy.cust_dma_lkp m on coalesce(b.cust_dma,'Unknown') = m.cust_dma_desc
left join t2dl_das_strategy.rack_sol_dist_bucket_lkp n on coalesce(b.rack_sol_dist_bucket,'missing') = n.rack_sol_dist_bucket_desc
left join t2dl_das_strategy.nord_sol_dist_bucket_lkp o on coalesce(b.nord_sol_dist_bucket,'missing') = o.nord_sol_dist_bucket_desc
left join t2dl_das_strategy.closest_store_banner_lkp p on coalesce(b.closest_store_banner,'missing') = p.closest_store_banner_desc
left join t2dl_das_strategy.closest_sol_dist_bucket_lkp q on coalesce(b.closest_store_dist_bucket,'missing') = q.closest_sol_dist_bucket_desc
left join t2dl_das_strategy.item_delivery_method_lkp r on coalesce(a.item_delivery_method,'NA') = r.item_delivery_method_desc
left join t2dl_das_strategy.store_lkp s on coalesce(a.store_num,'Unknown') = s.store_num and s.channel_country_desc = a.channel_country
left join t2dl_das_strategy.audience_engagement_cohort_lkp t on coalesce(b.engagement_cohort, 'missing') = t.audience_engagement_cohort_desc
left join t2dl_das_strategy.cust_chan_buyer_flow_lkp u on u.cust_chan_buyer_flow_desc = coalesce(b.ns_buyerflow ,'NA')
left join t2dl_das_strategy.cust_chan_buyer_flow_lkp v on v.cust_chan_buyer_flow_desc = coalesce(b.ncom_buyerflow,'NA')
left join t2dl_das_strategy.cust_chan_buyer_flow_lkp w on w.cust_chan_buyer_flow_desc = coalesce(b.rs_buyerflow,'NA')
left join t2dl_das_strategy.cust_chan_buyer_flow_lkp x on x.cust_chan_buyer_flow_desc = coalesce(b.rcom_buyerflow,'NA')
left join t2dl_das_strategy.cust_chan_buyer_flow_lkp y on y.cust_chan_buyer_flow_desc = coalesce(b.nord_buyerflow,'NA')
left join t2dl_das_strategy.cust_chan_buyer_flow_lkp z on z.cust_chan_buyer_flow_desc = coalesce(b.rack_buyerflow,'NA')
left join t2dl_das_strategy.cust_chan_buyer_flow_lkp aa on aa.cust_chan_buyer_flow_desc = coalesce(b.jwn_buyerflow,'NA')
where date_shopped between (select p2_end_date + 1 from date_range) and (select end_date from date_range)
;


--COLLECT STATISTICS COLUMN(date_shopped) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(acp_id) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(store_banner_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(store_channel_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(store_channel_country) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(store_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(div_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(subdiv_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(dept_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(trip_id) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(item_delivery_method_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(employee_flag) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(reporting_year_shopped_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(audience_engagement_cohort_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_jwn_trip_bucket_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_jwn_net_spend_bucket_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_rack_trip_bucket_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_rack_net_spend_bucket_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_nord_trip_bucket_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_nord_net_spend_bucket_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_channel_combo_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_loyalty_type_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_loyalty_level_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_gender) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_age_group_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_employee_flag) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_tenure_bucket_years_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_acquisition_fiscal_year) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_acquisition_channel_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_activation_channel_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_nms_market_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_dma_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_had_a_cancel) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_svc_group_exp_delivery) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_svc_group_order_pickup) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_svc_group_selling_relation) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_svc_group_remote_selling) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_svc_group_alterations) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_svc_group_in_store) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_svc_group_restaurant) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_service_free_exp_delivery) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_service_next_day_pickup) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_service_same_day_bopus) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_service_curbside_pickup) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_service_style_boards) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_service_gift_wrapping) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_styling) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_ns_buyerflow_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_ncom_buyerflow_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_rs_buyerflow_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_rcom_buyerflow_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_nord_buyerflow_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_rack_buyerflow_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_jwn_buyerflow_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_jwn_acquired_aare) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_jwn_activated_aare) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_jwn_retained_aare) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_jwn_engaged_aare) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(shopped_fls) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(shopped_ncom) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(shopped_rack) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(shopped_rcom) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(shopped_fp) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(shopped_op) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(shopped_jwn) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_service_ship2store_ncom) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_service_ship2store_rcom) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(closest_sol_dist_bucket_num) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_event_holiday) ON {str_t2_schema}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_event_anniversary) ON {str_t2_schema}.customer_sandbox_fact;


SET QUERY_BAND = NONE FOR SESSION;
