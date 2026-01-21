-- SET QUERY_BAND = 'App_ID=APP08240;
-- DAG_ID=cco_tables_week_grain_11521_ACE_ENG;
-- Task_Name=run_cco_cust_week_attributes;'
-- FOR SESSION VOLATILE;


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
BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS curr_mo_lkp AS
SELECT DISTINCT month_num AS curr_mo,
  CASE WHEN MOD(month_num, 100) = 1 THEN month_num - 89 ELSE month_num - 1 END AS prior_mo,
 year_num AS curr_year
FROM `{{params.gcp_project_id}}`.t2dl_das_usl.usl_rolling_52wk_calendar
WHERE day_date = CURRENT_DATE('PST8PDT');
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
/*********** Get the dates needed for the CCO tables ********************************/
CREATE TEMPORARY TABLE IF NOT EXISTS date_parameter_lookup AS
SELECT MIN(CASE
   WHEN year_num = 2018
   THEN day_date
   ELSE NULL
   END) AS fy18_start_dt,
 MAX(CASE
   WHEN year_num = 2018
   THEN day_date
   ELSE NULL
   END) AS fy18_end_dt,
 MIN(CASE
   WHEN year_num = 2019
   THEN day_date
   ELSE NULL
   END) AS fy19_start_dt,
 MAX(CASE
   WHEN month_num < (SELECT curr_mo
     FROM curr_mo_lkp)
   THEN day_date
   ELSE NULL
   END) AS latest_mo_dt,
 MIN(CASE
   WHEN month_num = (SELECT curr_mo - 400
     FROM curr_mo_lkp)
   THEN day_date
   ELSE NULL
   END) AS r4yr1_start_dt,
 MIN(CASE
   WHEN month_num = (SELECT curr_mo - 500
     FROM curr_mo_lkp)
   THEN day_date
   ELSE NULL
   END) AS r4yr0_start_dt,
 MAX(CASE
   WHEN month_num = (SELECT prior_mo - 400
     FROM curr_mo_lkp)
   THEN day_date
   ELSE NULL
   END) AS r4yr0_end_dt
FROM `{{params.gcp_project_id}}`.t2dl_das_usl.usl_rolling_52wk_calendar;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
/*********** Create Start & End dates lookup table **********************************/

CREATE TEMPORARY TABLE IF NOT EXISTS date_lookup
AS
SELECT fy19_start_dt AS lines_start_date,
 latest_mo_dt AS lines_end_date
FROM date_parameter_lookup AS dp;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
/************************************************************************************
 * Create driver table (at a customer/week-level) with CCO_LINES data plus some product
 * identifiers (which get pulled first, then added later to the CCO_LINES data pull
 ************************************************************************************/

CREATE TEMPORARY TABLE IF NOT EXISTS anchor_brands AS
SELECT dtl.global_tran_id,
 dtl.line_item_seq_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact AS dtl
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS ps ON LOWER(dtl.sku_num) = LOWER(ps.rms_sku_num) AND LOWER(ps.channel_country
    ) = LOWER('US')
 INNER JOIN (SELECT DISTINCT supplier_idnt
  FROM `{{params.gcp_project_id}}`.t2dl_das_in_season_management_reporting.anchor_brands
  WHERE LOWER(anchor_brand_ind) = LOWER('Y')) AS s ON CAST(ps.prmy_supp_num AS FLOAT64) = s.supplier_idnt
WHERE dtl.business_day_date >= (SELECT lines_start_date
   FROM date_lookup);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS strategic_brands AS
SELECT dtl.global_tran_id,
 dtl.line_item_seq_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact AS dtl
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS ps ON LOWER(dtl.sku_num) = LOWER(ps.rms_sku_num) AND LOWER(ps.channel_country
    ) = LOWER('US')
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS sup ON LOWER(ps.prmy_supp_num) = LOWER(sup.vendor_num)
 INNER JOIN (SELECT DISTINCT supplier_name
  FROM `{{params.gcp_project_id}}`.t2dl_das_in_season_management_reporting.rack_strategic_brands
  WHERE LOWER(rack_strategic_brand_ind) = LOWER('Y')) AS rsb ON LOWER(rsb.supplier_name) = LOWER(sup.vendor_name)
WHERE dtl.business_day_date >= (SELECT lines_start_date
   FROM date_lookup);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (global_tran_id, line_item_seq_num) on anchor_brands;
--collect statistics column (global_tran_id, line_item_seq_num) on strategic_brands;
BEGIN
SET _ERROR_CODE  =  0;


/************************************************************************************
 * Create detail level extract first with more efficient index for later aggregation
 ************************************************************************************/
CREATE TEMPORARY TABLE IF NOT EXISTS cco_lines_week_extract AS
SELECT a.acp_id,
 dt.week_idnt,
 dt.week_start_day_date,
 dt.week_end_day_date,
 a.channel,
 a.banner,
 a.employee_flag,
 a.ntn_tran,
 a.gross_sales,
 a.return_amt,
 a.net_sales,
 a.div_num,
 a.store_num,
 a.date_shopped,
 a.gross_incl_gc,
 a.gross_items,
 a.return_items,
  a.gross_items - a.return_items AS net_items,
 a.tender_nordstrom,
 a.tender_nordstrom_note,
 a.tender_3rd_party_credit,
 a.tender_debit_card,
 a.tender_gift_card,
 a.tender_cash,
 a.tender_paypal,
 a.tender_check,
 a.event_holiday,
 a.event_anniversary,
 a.svc_group_exp_delivery,
 a.svc_group_order_pickup,
 a.svc_group_selling_relation,
 a.svc_group_remote_selling,
 a.svc_group_alterations,
 a.svc_group_in_store,
 a.svc_group_restaurant,
 a.service_free_exp_delivery,
 a.service_next_day_pickup,
 a.service_same_day_bopus,
 a.service_curbside_pickup,
 a.service_style_boards,
 a.service_gift_wrapping,
 a.service_pop_in,
 a.marketplace_flag,
 a.platform,
  CASE WHEN ab.global_tran_id IS NOT NULL AND LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('1'), LOWER('2')) THEN 1 ELSE 0 END AS anchor_brand,
  CASE WHEN sb.global_tran_id IS NOT NULL AND LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('3'), LOWER('4')) THEN 1 ELSE 0 END AS strategic_brand
FROM `{{params.gcp_project_id}}`.t2dl_das_strategy.cco_line_items AS a
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dt 
 ON a.date_shopped = dt.day_date
 LEFT JOIN anchor_brands AS ab 
 ON a.global_tran_id = ab.global_tran_id 
 AND a.line_item_seq_num = ab.line_item_seq_num
 LEFT JOIN strategic_brands AS sb 
 ON a.global_tran_id = sb.global_tran_id 
 AND a.line_item_seq_num = sb.line_item_seq_num;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS cco_lines_week_agg AS
SELECT acp_id,
 week_idnt,
 week_start_day_date,
 week_end_day_date,
 COUNT(DISTINCT channel) AS channels_shopped,
 COUNT(DISTINCT banner) AS banners_shopped,
 MAX(employee_flag) AS employee_flag,
 MAX(ntn_tran) AS ntn_this_week,
 MAX(CASE WHEN LOWER(SUBSTR(channel, 1, 1)) = LOWER('1') THEN 1 ELSE 0 END) AS ns_ind,
 MAX(CASE WHEN LOWER(SUBSTR(channel, 1, 1)) = LOWER('2') THEN 1 ELSE 0 END) AS ncom_ind,
 MAX(CASE WHEN LOWER(SUBSTR(channel, 1, 1)) = LOWER('3') THEN 1 ELSE 0 END) AS rs_ind,
 MAX(CASE WHEN LOWER(SUBSTR(channel, 1, 1)) = LOWER('4') THEN 1 ELSE 0 END) AS rcom_ind,
 MAX(CASE WHEN LOWER(SUBSTR(channel, 1, 1)) IN (LOWER('1'), LOWER('2')) THEN 1 ELSE 0 END) AS n_ind,
 MAX(CASE WHEN LOWER(SUBSTR(channel, 1, 1)) IN (LOWER('3'), LOWER('4')) THEN 1 ELSE 0 END) AS r_ind,
 MAX(CASE WHEN LOWER(SUBSTR(channel, 1, 1)) IN (LOWER('1'), LOWER('3')) THEN 1 ELSE 0 END) AS store_customer,
 MAX(CASE WHEN LOWER(SUBSTR(channel, 1, 1)) IN (LOWER('2'), LOWER('4')) THEN 1 ELSE 0 END) AS digital_customer,
 SUM(gross_sales) AS gross_sales,
 SUM(return_amt) AS return_amt,
 SUM(net_sales) AS net_sales,
 SUM(CASE WHEN div_num = 351 THEN net_sales ELSE 0 END) AS net_sales_apparel,
 SUM(CASE WHEN div_num = 310 THEN net_sales ELSE 0 END) AS net_sales_shoes,
 SUM(CASE WHEN div_num = 340 THEN net_sales ELSE 0 END) AS net_sales_beauty,
 SUM(CASE WHEN div_num = 341 THEN net_sales ELSE 0 END) AS net_sales_designer,
 SUM(CASE WHEN div_num = 360 THEN net_sales ELSE 0 END) AS net_sales_accessories,
 SUM(CASE WHEN div_num = 365 THEN net_sales ELSE 0 END) AS net_sales_home,
 SUM(CASE WHEN div_num = 370 THEN net_sales ELSE 0 END) AS net_sales_merch_projects,
 SUM(CASE WHEN div_num = 800 THEN net_sales ELSE 0 END) AS net_sales_leased_boutiques,
 SUM(CASE
   WHEN div_num = 900
   THEN net_sales
   ELSE 0
   END) AS net_sales_other_non_merch,
 SUM(CASE
   WHEN div_num = 70
   THEN net_sales
   ELSE 0
   END) AS net_sales_restaurant,
 SUM(CASE
   WHEN div_num = 351
   THEN 1
   ELSE 0
   END) AS transaction_apparel,
 SUM(CASE
   WHEN div_num = 310
   THEN 1
   ELSE 0
   END) AS transaction_shoes,
 SUM(CASE
   WHEN div_num = 340
   THEN 1
   ELSE 0
   END) AS transaction_beauty,
 SUM(CASE
   WHEN div_num = 341
   THEN 1
   ELSE 0
   END) AS transaction_designer,
 SUM(CASE
   WHEN div_num = 360
   THEN 1
   ELSE 0
   END) AS transaction_accessories,
 SUM(CASE
   WHEN div_num = 365
   THEN 1
   ELSE 0
   END) AS transaction_home,
 SUM(CASE
   WHEN div_num = 370
   THEN 1
   ELSE 0
   END) AS transaction_merch_projects,
 SUM(CASE
   WHEN div_num = 800
   THEN 1
   ELSE 0
   END) AS transaction_leased_boutiques,
 SUM(CASE
   WHEN div_num = 900
   THEN 1
   ELSE 0
   END) AS transaction_other_non_merch,
 SUM(CASE
   WHEN div_num = 70
   THEN 1
   ELSE 0
   END) AS transaction_restaurant,
 COUNT(DISTINCT CASE
   WHEN gross_incl_gc > 0
   THEN acp_id || FORMAT('%11d', store_num) || CAST(date_shopped AS STRING)
   ELSE NULL
   END) AS trips,
 COUNT(DISTINCT CASE
   WHEN gross_incl_gc > 0 AND div_num <> 70
   THEN acp_id || FORMAT('%11d', store_num) || CAST(date_shopped AS STRING)
   ELSE NULL
   END) AS nonrestaurant_trips,
 SUM(gross_items) AS gross_items,
 SUM(return_items) AS return_items,
 SUM(gross_items - return_items) AS net_items,
 MAX(tender_nordstrom) AS tender_nordstrom,
 MAX(tender_nordstrom_note) AS tender_nordstrom_note,
 MAX(tender_3rd_party_credit) AS tender_3rd_party_credit,
 MAX(tender_debit_card) AS tender_debit_card,
 MAX(tender_gift_card) AS tender_gift_card,
 MAX(tender_cash) AS tender_cash,
 MAX(tender_paypal) AS tender_paypal,
 MAX(tender_check) AS tender_check,
 MAX(event_holiday) AS event_holiday,
 MAX(event_anniversary) AS event_anniversary,
 MAX(svc_group_exp_delivery) AS svc_group_exp_delivery,
 MAX(svc_group_order_pickup) AS svc_group_order_pickup,
 MAX(svc_group_selling_relation) AS svc_group_selling_relation,
 MAX(svc_group_remote_selling) AS svc_group_remote_selling,
 MAX(svc_group_alterations) AS svc_group_alterations,
 MAX(svc_group_in_store) AS svc_group_in_store,
 MAX(svc_group_restaurant) AS svc_group_restaurant,
 MAX(service_free_exp_delivery) AS service_free_exp_delivery,
 MAX(service_next_day_pickup) AS service_next_day_pickup,
 MAX(service_same_day_bopus) AS service_same_day_bopus,
 MAX(service_curbside_pickup) AS service_curbside_pickup,
 MAX(service_style_boards) AS service_style_boards,
 MAX(service_gift_wrapping) AS service_gift_wrapping,
 MAX(service_pop_in) AS service_pop_in,
 MAX(marketplace_flag) AS marketplace_flag,
 MAX(CASE
   WHEN LOWER(TRIM(platform)) = LOWER('WEB')
   THEN 1
   ELSE 0
   END) AS platform_desktop,
 MAX(CASE
   WHEN LOWER(TRIM(platform)) = LOWER('MOW')
   THEN 1
   ELSE 0
   END) AS platform_mow,
 MAX(CASE
   WHEN LOWER(TRIM(platform)) = LOWER('IOS')
   THEN 1
   ELSE 0
   END) AS platform_ios,
 MAX(CASE
   WHEN LOWER(TRIM(platform)) = LOWER('ANDROID')
   THEN 1
   ELSE 0
   END) AS platform_android,
 MAX(CASE
   WHEN LOWER(TRIM(platform)) = LOWER('POS')
   THEN 1
   ELSE 0
   END) AS platform_pos,
 MAX(anchor_brand) AS anchor_brand,
 MAX(strategic_brand) AS strategic_brand
FROM cco_lines_week_extract
GROUP BY acp_id,
 week_idnt,
 week_start_day_date,
 week_end_day_date;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (acp_id, week_idnt) on cco_lines_week_agg;
BEGIN
SET _ERROR_CODE  =  0;
/************************************************************************************
 * Create driver tables for filtering the acp_id's and weeks we want from other tables
 * that will append data later along with CCO_LINES data in final table
 ************************************************************************************/

CREATE TEMPORARY TABLE IF NOT EXISTS customer_acp_id_driver AS
SELECT DISTINCT acp_id
FROM cco_lines_week_agg AS a;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS customer_week_driver AS
SELECT DISTINCT acp_id,
 week_idnt,
 week_start_day_date,
 week_end_day_date,
 employee_flag,
 net_sales
FROM cco_lines_week_agg AS a;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
/************************************************************************************
 * Identify customer's MARKET
 ************************************************************************************/

/*********** Lookup table assigning nodes to Markets ********************************/
CREATE TEMPORARY TABLE IF NOT EXISTS consolidated_nms_nodes as (
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
  select local_market as market, cast(trunc(cast(node as float64)) as integer) as node_num
  from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.local_market_node_dim
  where  current_datetime('PST8PDT') between eff_begin_date and eff_end_date
  union DISTINCT
  select market, node_num
  from `{{params.gcp_project_id}}`.t2dl_das_strategy.cco_nms_nodes
    --where  current_datetime('PST8PDT') between eff_begin_date and eff_end_date
  ) x
) ;

/*********** Create lookup table of customers & their markets ***********************/
CREATE TEMPORARY TABLE IF NOT EXISTS customer_market as (
select distinct a.acp_id
  ,a.market
  ,case when b.market is not null then 1 else 0 end NMS_Market
  ,b.nms_launch_date
from
  (
  select distinct acp_id
    ,coalesce(bill_zip_market,fls_market,rack_market,ca_dma_desc,us_dma_desc) AS market
  from
    (
      select distinct acp_id,
       CASE  WHEN LOWER(b.market) IS NOT NULL THEN LOWER(b.market) END AS bill_zip_market,
       CASE  WHEN LOWER(c.market) IS NOT NULL THEN LOWER(c.market)  END AS fls_market,
       CASE  WHEN LOWER(d.market) IS NOT NULL THEN LOWER(d.market)  END AS rack_market,
        ca_dma_desc,
        us_dma_desc
      from
        (
          select x.acp_id, billing_postal_code, ca_dma_desc, us_dma_desc, fls_loyalty_store_num, rack_loyalty_store_num,
            CASE  WHEN LOWER(ca_dma_desc) IS NOT NULL THEN LOWER(LEFT(billing_postal_code, 3))  ELSE LOWER(billing_postal_code) END AS join_zip
          from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.analytical_customer x
          join customer_week_driver y on x.acp_id = y.acp_id
          where x.acp_id is not null
        ) a
      left join
        (
          select distinct local_market as market
            ,case when LOWER(local_market) = LOWER('TORONTO') then LOWER(left(coarse_postal_code,3)) else LOWER(coarse_postal_code) end as join_zip
          from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.local_market_postal_dim
          where  current_datetime('PST8PDT') between eff_begin_date and eff_end_date
        ) b on a.join_zip = b.join_zip
      left join consolidated_nms_nodes c on a.fls_loyalty_store_num = c.node_num
      left join consolidated_nms_nodes d on a.rack_loyalty_store_num = d.node_num
      where a.acp_id is not null
    ) z
WHERE 
  LOWER(COALESCE(bill_zip_market, fls_market, rack_market, ca_dma_desc, us_dma_desc)) IS NOT NULL 
  OR 
  LOWER(COALESCE(bill_zip_market, fls_market, rack_market, ca_dma_desc, us_dma_desc)) NOT IN (LOWER('Not Defined'), LOWER('Other'))
  ) a
left join
  (
    select distinct market, nms_launch_date
    from consolidated_nms_nodes
  ) b on a.market = b.market
) ;

/*********** Create lookup table of customers & their NMS markets ********************/


CREATE TEMPORARY TABLE IF NOT EXISTS customer_nms_market as (
select distinct acp_id
,market
from customer_market
where NMS_Market = 1
) 
;



BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS customer_dma AS
SELECT a.acp_id,
CASE
  WHEN LOWER(b.ca_dma_desc) IS NOT NULL
  THEN 'ca'
  WHEN LOWER(b.us_dma_desc) IS NOT NULL
  THEN 'us'
  ELSE NULL
END AS cust_country,
 COALESCE(b.ca_dma_desc, b.us_dma_desc) AS cust_dma
FROM customer_acp_id_driver AS a
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.analytical_customer AS b ON LOWER(a.acp_id) = LOWER(b.acp_id);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (acp_id) on customer_dma;
--collect statistics column (cust_dma) on customer_dma;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS customer_week_dma_ns AS
SELECT a.week_idnt,
 b.cust_dma,
 SUM(a.net_sales) AS dma_net_sales
FROM customer_week_driver AS a
 LEFT JOIN customer_dma AS b ON LOWER(a.acp_id) = LOWER(b.acp_id)
GROUP BY a.week_idnt,
 b.cust_dma;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS dma_week_ranking
AS
SELECT cust_dma,
 week_idnt,
 RANK() OVER (PARTITION BY week_idnt ORDER BY dma_net_sales DESC) AS dma_rank
FROM customer_week_dma_ns AS cwns;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS customer_dma_week
AS
SELECT DISTINCT drv.acp_id,
 dma.cust_dma,
 dma.cust_country,
 drv.week_idnt,
  CASE
  WHEN rnk.dma_rank BETWEEN 1 AND 5
  THEN '1) Top 5'
  WHEN rnk.dma_rank BETWEEN 6 AND 10
  THEN '2) 6-10'
  WHEN rnk.dma_rank BETWEEN 11 AND 20
  THEN '3) 11-20'
  WHEN rnk.dma_rank BETWEEN 21 AND 30
  THEN '4) 21-30'
  WHEN rnk.dma_rank BETWEEN 31 AND 50
  THEN '5) 31-50'
  WHEN rnk.dma_rank BETWEEN 51 AND 100
  THEN '6) 51-100'
  ELSE '7) > 100'
  END AS dma_rank
FROM customer_week_driver AS drv
 INNER JOIN customer_dma AS dma ON LOWER(drv.acp_id) = LOWER(dma.acp_id)
 INNER JOIN dma_week_ranking AS rnk ON LOWER(dma.cust_dma) = LOWER(rnk.cust_dma) AND drv.week_idnt = rnk.week_idnt;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (acp_id, week_idnt) on customer_dma_week;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS derived_cust_attributes
AS
SELECT DISTINCT acp_id,
 week_idnt,
 ntn_this_week,
 channels_shopped,
  CASE
  WHEN ns_ind = 1 AND ncom_ind = 0 AND rs_ind = 0 AND rcom_ind = 0
  THEN '01) NordStore-only'
  WHEN ns_ind = 0 AND ncom_ind = 1 AND rs_ind = 0 AND rcom_ind = 0
  THEN '02) N.com-only'
  WHEN ns_ind = 0 AND ncom_ind = 0 AND rs_ind = 1 AND rcom_ind = 0
  THEN '03) RackStore-only'
  WHEN ns_ind = 0 AND ncom_ind = 0 AND rs_ind = 0 AND rcom_ind = 1
  THEN '04) Rack.com-only'
  WHEN ns_ind = 1 AND ncom_ind = 1 AND rs_ind = 0 AND rcom_ind = 0
  THEN '05) NordStore+N.com'
  WHEN ns_ind = 1 AND ncom_ind = 0 AND rs_ind = 1 AND rcom_ind = 0
  THEN '06) NordStore+RackStore'
  WHEN ns_ind = 1 AND ncom_ind = 0 AND rs_ind = 0 AND rcom_ind = 1
  THEN '07) NordStore+Rack.com'
  WHEN ns_ind = 0 AND ncom_ind = 1 AND rs_ind = 1 AND rcom_ind = 0
  THEN '08) N.com+RackStore'
  WHEN ns_ind = 0 AND ncom_ind = 1 AND rs_ind = 0 AND rcom_ind = 1
  THEN '09) N.com+Rack.com'
  WHEN ns_ind = 0 AND ncom_ind = 0 AND rs_ind = 1 AND rcom_ind = 1
  THEN '10) RackStore+Rack.com'
  WHEN ns_ind = 1 AND ncom_ind = 1 AND rs_ind = 1 AND rcom_ind = 0
  THEN '11) NordStore+N.com+RackStore'
  WHEN ns_ind = 1 AND ncom_ind = 1 AND rs_ind = 0 AND rcom_ind = 1
  THEN '12) NordStore+N.com+Rack.com'
  WHEN ns_ind = 1 AND ncom_ind = 0 AND rs_ind = 1 AND rcom_ind = 1
  THEN '13) NordStore+RackStore+Rack.com'
  WHEN ns_ind = 0 AND ncom_ind = 1 AND rs_ind = 1 AND rcom_ind = 1
  THEN '14) N.com+RackStore+Rack.com'
  WHEN ns_ind = 1 AND ncom_ind = 1 AND rs_ind = 1 AND rcom_ind = 1
  THEN '15) 4-Box'
  ELSE '99) Error'
  END AS chan_combo,
 banners_shopped,
  CASE
  WHEN n_ind = 1 AND r_ind = 0
  THEN '1) Nordstrom-only'
  WHEN n_ind = 0 AND r_ind = 1
  THEN '2) Rack-only'
  WHEN n_ind = 1 AND r_ind = 1
  THEN '3) Dual-Banner'
  ELSE '99) Error'
  END AS banner_combo,
 employee_flag,
  CASE
  WHEN trips < 10
  THEN '0' || SUBSTR(CAST(trips AS STRING), 1, 1) || ' trips'
  ELSE '10+ trips'
  END AS jwn_trip_bucket,
  CASE
  WHEN net_sales = 0
  THEN '0) $0'
  WHEN net_sales > 0 AND net_sales <= 50
  THEN '1) $0-50'
  WHEN net_sales > 50 AND net_sales <= 100
  THEN '2) $50-100'
  WHEN net_sales > 100 AND net_sales <= 250
  THEN '3) $100-250'
  WHEN net_sales > 250 AND net_sales <= 500
  THEN '4) $250-500'
  WHEN net_sales > 500 AND net_sales <= 1000
  THEN '5) $500-1K'
  WHEN net_sales > 1000 AND net_sales <= 2000
  THEN '6) 1-2K'
  WHEN net_sales > 2000 AND net_sales <= 5000
  THEN '7) 2-5K'
  WHEN net_sales > 5000 AND net_sales <= 10000
  THEN '8) 5-10K'
  WHEN net_sales > 10000
  THEN '9) 10K+'
  ELSE NULL
  END AS jwn_net_spend_bucket,
  CASE
  WHEN ntn_this_week = 1 AND nonrestaurant_trips <= 5
  THEN 'Acquire & Activate'
  WHEN nonrestaurant_trips <= 5
  THEN 'Lightly-Engaged'
  WHEN nonrestaurant_trips <= 13
  THEN 'Moderately-Engaged'
  WHEN nonrestaurant_trips >= 14
  THEN 'Highly-Engaged'
  ELSE NULL
  END AS engagement_cohort,
 gross_sales,
 return_amt,
 net_sales,
 net_sales_apparel,
 net_sales_shoes,
 net_sales_beauty,
 net_sales_designer,
 net_sales_accessories,
 net_sales_home,
 net_sales_merch_projects,
 net_sales_leased_boutiques,
 net_sales_other_non_merch,
 net_sales_restaurant,
 transaction_apparel,
 transaction_shoes,
 transaction_beauty,
 transaction_designer,
 transaction_accessories,
 transaction_home,
 transaction_merch_projects,
 transaction_leased_boutiques,
 transaction_other_non_merch,
 transaction_restaurant,
 trips,
 nonrestaurant_trips,
 gross_items,
 return_items,
 net_items,
 tender_nordstrom AS cust_tender_nordstrom,
 tender_nordstrom_note AS cust_tender_nordstrom_note,
 tender_3rd_party_credit AS cust_tender_3rd_party_credit,
 tender_debit_card AS cust_tender_debit_card,
 tender_gift_card AS cust_tender_gift_card,
 tender_cash AS cust_tender_cash,
 tender_paypal AS cust_tender_paypal,
 tender_check AS cust_tender_check,
 event_holiday AS cust_event_holiday,
 event_anniversary AS cust_event_anniversary,
 svc_group_exp_delivery AS cust_svc_group_exp_delivery,
 svc_group_order_pickup AS cust_svc_group_order_pickup,
 svc_group_selling_relation AS cust_svc_group_selling_relation,
 svc_group_remote_selling AS cust_svc_group_remote_selling,
 svc_group_alterations AS cust_svc_group_alterations,
 svc_group_in_store AS cust_svc_group_in_store,
 svc_group_restaurant AS cust_svc_group_restaurant,
 service_free_exp_delivery AS cust_service_free_exp_delivery,
 service_next_day_pickup AS cust_service_next_day_pickup,
 service_same_day_bopus AS cust_service_same_day_bopus,
 service_curbside_pickup AS cust_service_curbside_pickup,
 service_style_boards AS cust_service_style_boards,
 service_gift_wrapping AS cust_service_gift_wrapping,
 service_pop_in AS cust_service_pop_in,
 marketplace_flag AS cust_marketplace_flag,
 platform_desktop AS cust_platform_desktop,
 platform_mow AS cust_platform_mow,
 platform_ios AS cust_platform_ios,
 platform_android AS cust_platform_android,
 platform_pos AS cust_platform_pos,
 anchor_brand AS cust_anchor_brand,
 strategic_brand AS cust_strategic_brand,
 store_customer AS cust_store_customer,
 digital_customer AS cust_digital_customer
FROM cco_lines_week_agg AS a;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (acp_id, week_idnt) on derived_cust_attributes;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS modeled_ages
AS
SELECT t1.acp_id,
 t1.week_idnt,
 t1.model_age_adjusted
FROM (SELECT DISTINCT dr.acp_id,
   dr.week_idnt,
    CAST(TRUNC(CAST(ROUND(CAST(a.model_age AS NUMERIC), 2) - DATE_DIFF(CAST(a.update_timestamp AS DATE), dr.week_end_day_date, DAY) / 365.25 AS FLOAT64)) AS INT64)
   AS model_age_adjusted,
   a.acp_id AS acp_id0,
   a.update_timestamp
  FROM `{{params.gcp_project_id}}`.t2dl_das_age_model.new_age_model_scoring_all AS a
   INNER JOIN customer_week_driver AS dr ON LOWER(a.acp_id) = LOWER(dr.acp_id)
  QUALIFY (RANK() OVER (PARTITION BY acp_id0 ORDER BY a.update_timestamp DESC)) = 1) AS t1;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS experian_demos
AS
SELECT DISTINCT dr.acp_id,
 dr.week_idnt,
 a.gender,
 CAST(TRUNC(CAST(CASE
   WHEN LOWER(a.age_type) = LOWER('Exact Age') AND a.age_value IS NOT NULL
   THEN CASE
    WHEN LENGTH(TRIM(a.birth_year_and_month)) = 6
    THEN DATE_DIFF(dr.week_end_day_date, CAST(PARSE_DATE('%Y/%m/%d', SUBSTR(a.birth_year_and_month, 1, 4) || '/' || SUBSTR(a.birth_year_and_month, 5, 2) || '/' || '15') AS DATE), DAY) / 365.25
    ELSE CAST(a.age_value AS NUMERIC) - DATE_DIFF(CAST(a.object_system_time AS DATE), dr.week_end_day_date, DAY) / 365.25
    END
   ELSE NULL
   END AS FLOAT64)) AS INT64) AS experian_age_adjusted
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_cust_usr_vws.customer_experian_demographic_prediction_dim AS a
 INNER JOIN customer_week_driver AS dr ON LOWER(a.acp_id) = LOWER(dr.acp_id);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS both_ages
AS
SELECT acp_id,
 week_idnt,
 gender,
 age,
  CASE
  WHEN age BETWEEN 14 AND 22
  THEN '01) Young Adult'
  WHEN age BETWEEN 23 AND 29
  THEN '02) Early Career'
  WHEN age BETWEEN 30 AND 44
  THEN '03) Mid Career'
  WHEN age BETWEEN 45 AND 64
  THEN '04) Late Career'
  WHEN age >= 65
  THEN '05) Retired'
  ELSE 'Unknown'
  END AS lifestage,
  CASE
  WHEN age < 18
  THEN '0) <18 yrs'
  WHEN age >= 18 AND age <= 24
  THEN '1) 18-24 yrs'
  WHEN age > 24 AND age <= 34
  THEN '2) 25-34 yrs'
  WHEN age > 34 AND age <= 44
  THEN '3) 35-44 yrs'
  WHEN age > 44 AND age <= 54
  THEN '4) 45-54 yrs'
  WHEN age > 54 AND age <= 64
  THEN '5) 55-64 yrs'
  WHEN age > 64
  THEN '6) 65+ yrs'
  ELSE 'Unknown'
  END AS age_group
FROM (SELECT DISTINCT a.acp_id,
   a.week_idnt,
   b.gender,
   COALESCE(b.experian_age_adjusted, c.model_age_adjusted) AS age
  FROM customer_week_driver AS a
   LEFT JOIN experian_demos AS b ON LOWER(a.acp_id) = LOWER(b.acp_id) AND a.week_idnt = b.week_idnt
   LEFT JOIN modeled_ages AS c ON LOWER(a.acp_id) = LOWER(c.acp_id) AND a.week_idnt = c.week_idnt) AS x;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (acp_id, week_idnt) on both_ages;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cust_level_loyalty_cardmember
AS
SELECT lmd.acp_id,
 dr.week_idnt,
 1 AS flg_cardmember,
 MAX(CASE
   WHEN LOWER(rwd.rewards_level) IN (LOWER('MEMBER'))
   THEN 1
   WHEN LOWER(rwd.rewards_level) IN (LOWER('INSIDER'), LOWER('INFLUENCER'))
   THEN 3
   WHEN LOWER(rwd.rewards_level) IN (LOWER('AMBASSADOR'))
   THEN 4
   WHEN LOWER(rwd.rewards_level) IN (LOWER('ICON'))
   THEN 5
   ELSE 0
   END) AS cardmember_level,
 MIN(lmd.cardmember_enroll_date) AS cardmember_enroll_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.loyalty_member_dim_vw AS lmd
 INNER JOIN customer_week_driver AS dr ON LOWER(lmd.acp_id) = LOWER(dr.acp_id)
 LEFT JOIN (SELECT acp_id,
   PARSE_DATE('%Y%m%d', SUBSTR(CAST(max_close_dt AS STRING), 1, 8)) AS max_close_dt
  FROM `{{params.gcp_project_id}}`.t2dl_das_strategy.cco_credit_close_dts) AS ccd ON LOWER(lmd.acp_id) = LOWER(ccd.acp_id)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.loyalty_level_lifecycle_fact_vw AS rwd ON LOWER(lmd.loyalty_id) = LOWER(rwd.loyalty_id) AND
    dr.week_end_day_date >= rwd.start_day_date AND dr.week_end_day_date < rwd.end_day_date
WHERE COALESCE(lmd.cardmember_enroll_date, DATE '2099-12-31') < COALESCE(ccd.max_close_dt, lmd.cardmember_close_date,
   DATE '2099-12-31')
 AND COALESCE(lmd.cardmember_enroll_date, DATE '2099-12-31') <= dr.week_end_day_date
 AND COALESCE(ccd.max_close_dt, lmd.cardmember_close_date, DATE '2099-12-31') >= dr.week_end_day_date
 AND lmd.acp_id IS NOT NULL
GROUP BY lmd.acp_id,
 dr.week_idnt,
 flg_cardmember;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (acp_id, week_idnt) on cust_level_loyalty_cardmember;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cust_level_loyalty_member
AS
SELECT lmd.acp_id,
 dr.week_idnt,
 1 AS flg_member,
 MAX(CASE
   WHEN LOWER(rwd.rewards_level) IN (LOWER('MEMBER'))
   THEN 1
   WHEN LOWER(rwd.rewards_level) IN (LOWER('INSIDER'), LOWER('INFLUENCER'))
   THEN 3
   WHEN LOWER(rwd.rewards_level) IN (LOWER('AMBASSADOR'))
   THEN 4
   WHEN LOWER(rwd.rewards_level) IN (LOWER('ICON'))
   THEN 5
   ELSE 0
   END) AS member_level,
 MIN(lmd.member_enroll_date) AS member_enroll_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.loyalty_member_dim_vw AS lmd
 INNER JOIN customer_week_driver AS dr ON LOWER(lmd.acp_id) = LOWER(dr.acp_id)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.loyalty_level_lifecycle_fact_vw AS rwd ON LOWER(lmd.loyalty_id) = LOWER(rwd.loyalty_id) AND
    dr.week_end_day_date >= rwd.start_day_date AND dr.week_end_day_date < rwd.end_day_date
WHERE COALESCE(lmd.member_enroll_date, DATE '2099-12-31') < COALESCE(lmd.member_close_date, DATE '2099-12-31')
 AND COALESCE(lmd.member_enroll_date, DATE '2099-12-31') <= dr.week_end_day_date
 AND COALESCE(lmd.member_close_date, DATE '2099-12-31') >= dr.week_end_day_date
 AND lmd.acp_id IS NOT NULL
GROUP BY lmd.acp_id,
 dr.week_idnt,
 flg_member;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (acp_id, week_idnt) on cust_level_loyalty_member;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cust_level_loyalty
AS
SELECT a.acp_id,
 a.week_idnt,
CASE
  WHEN LOWER(b.acp_id) IS NOT NULL
  THEN LOWER('a) Cardmember')
  WHEN LOWER(c.acp_id) IS NOT NULL
  THEN LOWER('b) Member')
  ELSE LOWER('c) Non-Loyalty')
END AS loyalty_type,
  CASE
  WHEN a.employee_flag = 1 AND b.acp_id IS NOT NULL AND c.acp_id IS NOT NULL
  THEN '1) MEMBER'
  WHEN b.acp_id IS NOT NULL AND b.cardmember_level <= 3
  THEN '2) INFLUENCER'
  WHEN b.acp_id IS NOT NULL AND b.cardmember_level = 4
  THEN '3) AMBASSADOR'
  WHEN b.acp_id IS NOT NULL AND b.cardmember_level = 5
  THEN '4) ICON'
  WHEN c.member_level <= 1
  THEN '1) MEMBER'
  WHEN c.member_level = 3
  THEN '2) INFLUENCER'
  WHEN c.member_level >= 4
  THEN '3) AMBASSADOR'
  ELSE NULL
  END AS loyalty_level,
 c.member_enroll_date AS loyalty_member_start_dt,
 b.cardmember_enroll_date AS loyalty_cardmember_start_dt
FROM customer_week_driver AS a
 LEFT JOIN cust_level_loyalty_cardmember AS b ON LOWER(a.acp_id) = LOWER(b.acp_id) AND a.week_idnt = b.week_idnt
 LEFT JOIN cust_level_loyalty_member AS c ON LOWER(a.acp_id) = LOWER(c.acp_id) AND a.week_idnt = c.week_idnt;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (acp_id, week_idnt) on cust_level_loyalty;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS customer_acquisition_tenure_prep
AS
SELECT a.acp_id,
 b.week_idnt,
 a.aare_status_date AS acquisition_date,
  CASE
  WHEN LOWER(a.aare_chnl_code) = LOWER('FLS')
  THEN '1) Nordstrom Stores'
  WHEN LOWER(a.aare_chnl_code) = LOWER('NCOM')
  THEN '2) Nordstrom.com'
  WHEN LOWER(a.aare_chnl_code) = LOWER('RACK')
  THEN '3) Rack Stores'
  WHEN LOWER(a.aare_chnl_code) = LOWER('NRHL')
  THEN '4) Rack.com'
  ELSE NULL
  END AS acquisition_channel,
  CASE
  WHEN LOWER(a.aare_chnl_code) IN (LOWER('FLS'), LOWER('NCOM'))
  THEN 'NORDSTROM'
  WHEN LOWER(a.aare_chnl_code) IN (LOWER('RACK'), LOWER('NRHL'))
  THEN 'RACK'
  ELSE NULL
  END AS acquisition_banner,
 a.aare_brand_name AS acquisition_brand,
 CAST(TRUNC(CAST(FLOOR(DATE_DIFF(b.week_end_day_date, a.aare_status_date, DAY) / 365.25) AS FLOAT64)) AS INT64) AS tenure_years,
 CAST(TRUNC(CAST(FLOOR(12 * DATE_DIFF(b.week_end_day_date, a.aare_status_date, DAY) / 365.25) AS FLOAT64)) AS INT64) AS tenure_months
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_ntn_status_fact AS a
 INNER JOIN customer_week_driver AS b ON LOWER(a.acp_id) = LOWER(b.acp_id)
WHERE a.aare_status_date <= b.week_end_day_date
QUALIFY (RANK() OVER (PARTITION BY a.acp_id, b.week_idnt ORDER BY a.aare_status_date DESC)) = 1;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS customer_acquisition_tenure
AS
SELECT DISTINCT a.acp_id,
 a.week_idnt,
 a.acquisition_date,
 dt.fiscal_year_num AS acquisition_fiscal_year,
 a.acquisition_channel,
 a.acquisition_banner,
 a.acquisition_brand,
 a.tenure_years,
  CASE
  WHEN a.tenure_months < 4
  THEN '1) 0-3 months'
  WHEN a.tenure_months BETWEEN 4 AND 6
  THEN '2) 4-6 months'
  WHEN a.tenure_months BETWEEN 7 AND 12
  THEN '3) 7-12 months'
  WHEN a.tenure_months BETWEEN 13 AND 24
  THEN '4) 13-24 months'
  WHEN a.tenure_months BETWEEN 25 AND 36
  THEN '5) 25-36 months'
  WHEN a.tenure_months BETWEEN 37 AND 48
  THEN '6) 37-48 months'
  WHEN a.tenure_months BETWEEN 49 AND 60
  THEN '7) 49-60 months'
  WHEN a.tenure_months > 60
  THEN '8) 61+ months'
  ELSE 'Unknown'
  END AS tenure_bucket_months,
  CASE
  WHEN a.tenure_months <= 12
  THEN '1) <= 1 year'
  WHEN a.tenure_months BETWEEN 13 AND 24
  THEN '2) 1-2 years'
  WHEN a.tenure_months BETWEEN 25 AND 60
  THEN '3) 2-5 years'
  WHEN a.tenure_months BETWEEN 61 AND 120
  THEN '4) 5-10 years'
  WHEN a.tenure_months > 120
  THEN '5) 10+ years'
  ELSE 'Unknown'
  END AS tenure_bucket_years
FROM customer_acquisition_tenure_prep AS a
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dt ON a.acquisition_date = dt.day_date;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (acp_id, week_idnt) on customer_acquisition_tenure;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS customer_activation
AS
SELECT a.acp_id,
 b.week_idnt,
 a.activated_date AS activation_date,
  CASE
  WHEN LOWER(a.activated_chnl_code) = LOWER('FLS')
  THEN '1) Nordstrom Stores'
  WHEN LOWER(a.activated_chnl_code) = LOWER('NCOM')
  THEN '2) Nordstrom.com'
  WHEN LOWER(a.activated_chnl_code) = LOWER('RACK')
  THEN '3) Rack Stores'
  WHEN LOWER(a.activated_chnl_code) = LOWER('NRHL')
  THEN '4) Rack.com'
  ELSE NULL
  END AS activation_channel,
  CASE
  WHEN LOWER(a.activated_chnl_code) IN (LOWER('FLS'), LOWER('NCOM'))
  THEN 'NORDSTROM'
  WHEN LOWER(a.activated_chnl_code) IN (LOWER('RACK'), LOWER('NRHL'))
  THEN 'RACK'
  ELSE NULL
  END AS activation_banner
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_activated_fact AS a
 INNER JOIN customer_week_driver AS b ON LOWER(a.acp_id) = LOWER(b.acp_id)
WHERE a.activated_date <= b.week_end_day_date
QUALIFY (RANK() OVER (PARTITION BY a.acp_id, b.week_idnt ORDER BY a.activated_date DESC)) = 1;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (acp_id, week_idnt) on customer_activation;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS clv_scored_dates
AS
SELECT COUNT(*) AS cnt,
 scored_date
FROM `{{params.gcp_project_id}}`.t2dl_das_customber_model_attribute_productionalization.customer_prediction_clv_hist
GROUP BY scored_date
HAVING cnt > 100000;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cust_week_scored_dates
AS
SELECT drvr.acp_id,
 drvr.week_idnt,
 MAX(sd.scored_date) AS scored_date
FROM customer_week_driver AS drvr
 INNER JOIN clv_scored_dates AS sd ON drvr.week_start_day_date > sd.scored_date
GROUP BY drvr.acp_id,
 drvr.week_idnt;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cust_clv_data
AS
SELECT DISTINCT cwsd.acp_id,
 cwsd.week_idnt,
 clv.clv_jwn,
 clv.clv_fp,
 clv.clv_op
FROM cust_week_scored_dates AS cwsd
 INNER JOIN `{{params.gcp_project_id}}`.t2dl_das_customber_model_attribute_productionalization.customer_prediction_clv_hist AS clv ON LOWER(cwsd.acp_id
    ) = LOWER(clv.acp_id) AND cwsd.scored_date = clv.scored_date;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (acp_id, week_idnt) on cust_clv_data;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cco_cust_week_attributes_stg (
acp_id STRING,
week_idnt INTEGER,
cust_gender STRING,
cust_age INTEGER,
cust_lifestage STRING,
cust_age_group STRING,
cust_nms_market STRING,
cust_dma STRING,
cust_country STRING,
cust_dma_rank STRING,
cust_loyalty_type STRING,
cust_loyalty_level STRING,
cust_loy_member_enroll_dt DATE,
cust_loy_cardmember_enroll_dt DATE,
cust_acquisition_date DATE,
cust_acquisition_fiscal_year SMALLINT,
cust_acquisition_channel STRING,
cust_acquisition_banner STRING,
cust_acquisition_brand STRING,
cust_tenure_bucket_months STRING,
cust_tenure_bucket_years STRING,
cust_activation_date DATE,
cust_activation_channel STRING,
cust_activation_banner STRING,
cust_engagement_cohort STRING,
cust_channel_count INTEGER,
cust_channel_combo STRING,
cust_banner_count INTEGER,
cust_banner_combo STRING,
cust_employee_flag INTEGER,
cust_jwn_trip_bucket STRING,
cust_jwn_net_spend_bucket STRING,
cust_jwn_gross_sales BIGNUMERIC(38,2),
cust_jwn_return_amt BIGNUMERIC(38,2),
cust_jwn_net_sales BIGNUMERIC(38,2),
cust_jwn_net_sales_apparel BIGNUMERIC(38,2),
cust_jwn_net_sales_shoes BIGNUMERIC(38,2),
cust_jwn_net_sales_beauty BIGNUMERIC(38,2),
cust_jwn_net_sales_designer BIGNUMERIC(38,2),
cust_jwn_net_sales_accessories BIGNUMERIC(38,2),
cust_jwn_net_sales_home BIGNUMERIC(38,2),
cust_jwn_net_sales_merch_projects BIGNUMERIC(38,2),
cust_jwn_net_sales_leased_boutiques BIGNUMERIC(38,2),
cust_jwn_net_sales_other_non_merch BIGNUMERIC(38,2),
cust_jwn_net_sales_restaurant BIGNUMERIC(38,2),
cust_jwn_transaction_apparel_ind INTEGER,
cust_jwn_transaction_shoes_ind INTEGER,
cust_jwn_transaction_beauty_ind INTEGER,
cust_jwn_transaction_designer_ind INTEGER,
cust_jwn_transaction_accessories_ind INTEGER,
cust_jwn_transaction_home_ind INTEGER,
cust_jwn_transaction_merch_projects_ind INTEGER,
cust_jwn_transaction_leased_boutiques_ind INTEGER,
cust_jwn_transaction_other_non_merch_ind INTEGER,
cust_jwn_transaction_restaurant_ind INTEGER,
cust_jwn_trips INTEGER,
cust_jwn_nonrestaurant_trips INTEGER,
cust_jwn_gross_items INTEGER,
cust_jwn_return_items INTEGER,
cust_jwn_net_items INTEGER,
cust_tender_nordstrom INTEGER,
cust_tender_nordstrom_note INTEGER,
cust_tender_3rd_party_credit INTEGER,
cust_tender_debit_card INTEGER,
cust_tender_gift_card INTEGER,
cust_tender_cash INTEGER,
cust_tender_paypal INTEGER,
cust_tender_check INTEGER,
cust_event_holiday INTEGER,
cust_event_anniversary INTEGER,
cust_svc_group_exp_delivery INTEGER,
cust_svc_group_order_pickup INTEGER,
cust_svc_group_selling_relation INTEGER,
cust_svc_group_remote_selling INTEGER,
cust_svc_group_alterations INTEGER,
cust_svc_group_in_store INTEGER,
cust_svc_group_restaurant INTEGER,
cust_service_free_exp_delivery INTEGER,
cust_service_next_day_pickup INTEGER,
cust_service_same_day_bopus INTEGER,
cust_service_curbside_pickup INTEGER,
cust_service_style_boards INTEGER,
cust_service_gift_wrapping INTEGER,
cust_service_pop_in INTEGER,
cust_marketplace_flag INTEGER,
cust_platform_desktop INTEGER,
cust_platform_mow INTEGER,
cust_platform_ios INTEGER,
cust_platform_android INTEGER,
cust_platform_pos INTEGER,
cust_anchor_brand BYTEINT,
cust_strategic_brand BYTEINT,
cust_store_customer BYTEINT,
cust_digital_customer BYTEINT,
cust_clv_jwn NUMERIC(14,4),
cust_clv_fp NUMERIC(14,4),
cust_clv_op NUMERIC(14,4)
);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

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

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_cust_week_attributes;

INSERT INTO `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_cust_week_attributes
(SELECT stg.*,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM cco_cust_week_attributes_stg AS stg);

END;

