BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP08737;
DAG_ID=customer_sandbox_fact_daily_11521_ACE_ENG;
---Task_Name=customer_sandbox_fact_build;'*/
BEGIN
SET _ERROR_CODE  =  0;




CREATE TEMPORARY TABLE IF NOT EXISTS date_range
AS
SELECT MIN(date_shopped) AS start_date,
 MAX(date_shopped) AS end_date,
 DATE_ADD(MIN(date_shopped), INTERVAL CAST(trunc(DATE_DIFF(MAX(date_shopped), MIN(date_shopped), DAY) / 3) AS INT64)
  DAY) AS p2_start_date,
 DATE_ADD(DATE_ADD(MIN(date_shopped), INTERVAL CAST(trunc(DATE_DIFF(MAX(date_shopped), MIN(date_shopped), DAY) / 3) AS
    INT64) DAY), INTERVAL CAST(trunc(DATE_DIFF(MAX(date_shopped), MIN(date_shopped), DAY) / 3) AS INT64) DAY) AS
 p2_end_date
FROM `{{params.gcp_project_id}}`.t2dl_das_strategy.cco_line_items AS cli
WHERE reporting_year_shopped IS NOT NULL;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;




CREATE TEMPORARY TABLE IF NOT EXISTS cust_year_level_attributes
AS
SELECT acp_id,
 reporting_year_shopped,
 cust_engagement_cohort,
  CASE
  WHEN cust_jwn_trips < 10
  THEN '0' || SUBSTR(CAST(cust_jwn_trips AS STRING), 1, 1) || ' trips'
  WHEN cust_jwn_trips < 30
  THEN SUBSTR(CAST(cust_jwn_trips AS STRING), 1, 2) || ' trips'
  ELSE '30+ trips'
  END AS cust_jwn_trip_bucket,
 cust_jwn_net_spend_bucket,
 cust_channel_combo,
 cust_gender,
 cust_age_group,
 cust_lifestage,
 cust_age,
 cust_tenure_bucket_years,
 cust_dma,
 cust_dma_rank,
 cust_loyalty_type,
 cust_loyalty_level,
 cust_acquisition_date,
 cust_employee_flag,
 cust_chan_holiday,
 cust_chan_anniversary,
 cust_platform_desktop,
 cust_platform_mow,
 cust_platform_ios,
 cust_platform_android,
 cust_platform_pos,
 cust_acquisition_fiscal_year,
 cust_acquisition_channel,
 cust_activation_channel,
 cust_country,
 cust_nms_market,
 cust_tender_nordstrom,
 cust_tender_nordstrom_note,
 cust_tender_gift_card,
  CASE
  WHEN cust_tender_nordstrom = 0 AND cust_tender_nordstrom_note = 0 AND cust_tender_gift_card = 0
  THEN 1
  ELSE 0
  END AS cust_tender_other,
 cust_svc_group_exp_delivery,
 cust_svc_group_order_pickup,
 cust_svc_group_selling_relation,
 cust_svc_group_remote_selling,
 cust_svc_group_alterations,
 cust_svc_group_in_store,
 cust_svc_group_restaurant,
 cust_service_free_exp_delivery,
 cust_service_next_day_pickup,
 cust_service_same_day_bopus,
 cust_service_curbside_pickup,
 cust_service_style_boards,
 cust_service_gift_wrapping,
 cust_service_pop_in,
 MAX(CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN cust_chan_buyer_flow
   ELSE NULL
   END) AS ns_buyerflow,
 MAX(CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN cust_chan_buyer_flow
   ELSE NULL
   END) AS ncom_buyerflow,
 MAX(CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN cust_chan_buyer_flow
   ELSE NULL
   END) AS rs_buyerflow,
 MAX(CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN cust_chan_buyer_flow
   ELSE NULL
   END) AS rcom_buyerflow,
 MAX(CASE
   WHEN LOWER(channel) = LOWER('5) Nordstrom Banner')
   THEN cust_chan_buyer_flow
   ELSE NULL
   END) AS nord_buyerflow,
 MAX(CASE
   WHEN LOWER(channel) = LOWER('6) Rack Banner')
   THEN cust_chan_buyer_flow
   ELSE NULL
   END) AS rack_buyerflow,
 MAX(CASE
   WHEN LOWER(channel) = LOWER('7) JWN')
   THEN cust_chan_buyer_flow
   ELSE NULL
   END) AS jwn_buyerflow,
 MAX(CASE
   WHEN LOWER(channel) = LOWER('7) JWN')
   THEN cust_chan_acquired_aare
   ELSE NULL
   END) AS cust_jwn_acquired_aare,
 MAX(CASE
   WHEN LOWER(channel) = LOWER('7) JWN')
   THEN cust_chan_activated_aare
   ELSE NULL
   END) AS cust_jwn_activated_aare,
 MAX(CASE
   WHEN LOWER(channel) = LOWER('7) JWN')
   THEN cust_chan_retained_aare
   ELSE NULL
   END) AS cust_jwn_retained_aare,
 MAX(CASE
   WHEN LOWER(channel) = LOWER('7) JWN')
   THEN cust_chan_engaged_aare
   ELSE NULL
   END) AS cust_jwn_engaged_aare,
 MAX(CASE
   WHEN LOWER(SUBSTR(channel, 1, 1)) = LOWER('1')
   THEN 1
   ELSE 0
   END) AS shopped_fls,
 MAX(CASE
   WHEN LOWER(SUBSTR(channel, 1, 1)) = LOWER('2')
   THEN 1
   ELSE 0
   END) AS shopped_ncom,
 MAX(CASE
   WHEN LOWER(SUBSTR(channel, 1, 1)) = LOWER('3')
   THEN 1
   ELSE 0
   END) AS shopped_rack,
 MAX(CASE
   WHEN LOWER(SUBSTR(channel, 1, 1)) = LOWER('4')
   THEN 1
   ELSE 0
   END) AS shopped_rcom,
 MAX(CASE
   WHEN LOWER(SUBSTR(channel, 1, 1)) = LOWER('5')
   THEN 1
   ELSE 0
   END) AS shopped_fp,
 MAX(CASE
   WHEN LOWER(SUBSTR(channel, 1, 1)) = LOWER('6')
   THEN 1
   ELSE 0
   END) AS shopped_op,
 MAX(CASE
   WHEN LOWER(SUBSTR(channel, 1, 1)) = LOWER('7')
   THEN 1
   ELSE 0
   END) AS shopped_jwn,
 MAX(CASE
   WHEN cust_chan_return_items > 0
   THEN 1
   ELSE 0
   END) AS cust_made_a_return
FROM `{{params.gcp_project_id}}`.t2dl_das_strategy.cco_cust_chan_yr_attributes AS a
WHERE reporting_year_shopped IS NOT NULL
GROUP BY acp_id,
 reporting_year_shopped,
 cust_engagement_cohort,
 cust_jwn_trip_bucket,
 cust_jwn_net_spend_bucket,
 cust_channel_combo,
 cust_gender,
 cust_age_group,
 cust_lifestage,
 cust_age,
 cust_tenure_bucket_years,
 cust_dma,
 cust_dma_rank,
 cust_loyalty_type,
 cust_loyalty_level,
 cust_acquisition_date,
 cust_employee_flag,
 cust_chan_holiday,
 cust_chan_anniversary,
 cust_platform_desktop,
 cust_platform_mow,
 cust_platform_ios,
 cust_platform_android,
 cust_platform_pos,
 cust_acquisition_fiscal_year,
 cust_acquisition_channel,
 cust_activation_channel,
 cust_country,
 cust_nms_market,
 cust_tender_nordstrom,
 cust_tender_nordstrom_note,
 cust_tender_gift_card,
 cust_tender_other,
 cust_svc_group_exp_delivery,
 cust_svc_group_order_pickup,
 cust_svc_group_selling_relation,
 cust_svc_group_remote_selling,
 cust_svc_group_alterations,
 cust_svc_group_in_store,
 cust_svc_group_restaurant,
 cust_service_free_exp_delivery,
 cust_service_next_day_pickup,
 cust_service_same_day_bopus,
 cust_service_curbside_pickup,
 cust_service_style_boards,
 cust_service_gift_wrapping,
 cust_service_pop_in;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;




CREATE TEMPORARY TABLE IF NOT EXISTS week_53_yrs
AS
SELECT year_num,
 RANK() OVER (ORDER BY year_num DESC) AS recency_rank
FROM (SELECT DISTINCT year_num
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
  WHERE week_of_fyr = 53
   AND day_date BETWEEN DATE '2017-01-01' AND (CURRENT_DATE('PST8PDT')
)) AS x;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;




CREATE TEMPORARY TABLE IF NOT EXISTS week_53_yr_count
AS
SELECT COUNT(DISTINCT year_num) AS year_count
FROM week_53_yrs AS x;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS realigned_calendar (
day_date DATE,
day_num INTEGER,
day_desc STRING,
week_num INTEGER,
week_desc STRING,
month_num INTEGER,
month_short_desc STRING,
quarter_num INTEGER,
halfyear_num INTEGER,
year_num INTEGER,
month_454_num INTEGER,
year_454_num INTEGER
) ;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;





INSERT INTO realigned_calendar
(SELECT CASE
   WHEN year_num > (SELECT year_num
     FROM week_53_yrs
     WHERE recency_rank = 1)
   THEN day_date
   WHEN (SELECT *
      FROM week_53_yr_count) = 1 AND year_num <= (SELECT year_num
      FROM week_53_yrs
      WHERE recency_rank = 1)
   THEN DATE_ADD(day_date, INTERVAL 7 DAY)
   WHEN (SELECT *
      FROM week_53_yr_count) = 2 AND year_num > (SELECT year_num
      FROM week_53_yrs
      WHERE recency_rank = 2)
   THEN DATE_ADD(day_date, INTERVAL 7 DAY)
   WHEN (SELECT *
      FROM week_53_yr_count) = 2 AND year_num <= (SELECT year_num
      FROM week_53_yrs
      WHERE recency_rank = 2)
   THEN DATE_ADD(day_date, INTERVAL 14 DAY)
   WHEN (SELECT *
      FROM week_53_yr_count) = 3 AND year_num > (SELECT year_num
      FROM week_53_yrs
      WHERE recency_rank = 2)
   THEN DATE_ADD(day_date, INTERVAL 7 DAY)
   WHEN (SELECT *
      FROM week_53_yr_count) = 3 AND year_num > (SELECT year_num
      FROM week_53_yrs
      WHERE recency_rank = 3)
   THEN DATE_ADD(day_date, INTERVAL 14 DAY)
   WHEN (SELECT *
      FROM week_53_yr_count) = 3 AND year_num <= (SELECT year_num
      FROM week_53_yrs
      WHERE recency_rank = 3)
   THEN DATE_ADD(day_date, INTERVAL 21 DAY)
   WHEN (SELECT *
      FROM week_53_yr_count) = 4 AND year_num > (SELECT year_num
      FROM week_53_yrs
      WHERE recency_rank = 2)
   THEN DATE_ADD(day_date, INTERVAL 7 DAY)
   WHEN (SELECT *
      FROM week_53_yr_count) = 4 AND year_num > (SELECT year_num
      FROM week_53_yrs
      WHERE recency_rank = 3)
   THEN DATE_ADD(day_date, INTERVAL 14 DAY)
   WHEN (SELECT *
      FROM week_53_yr_count) = 4 AND year_num > (SELECT year_num
      FROM week_53_yrs
      WHERE recency_rank = 4)
   THEN DATE_ADD(day_date, INTERVAL 21 DAY)
   WHEN (SELECT *
      FROM week_53_yr_count) = 4 AND year_num <= (SELECT year_num
      FROM week_53_yrs
      WHERE recency_rank = 4)
   THEN DATE_ADD(day_date, INTERVAL 28 DAY)
   ELSE NULL
   END AS day_date,
  day_num,
  day_desc,
  week_num,
  week_desc,
  month_num,
  month_short_desc,
  quarter_num,
  halfyear_num,
  year_num,
  month_454_num,
  year_num AS year_454_num
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
 WHERE day_date BETWEEN DATE '2009-01-01' AND (CURRENT_DATE('PST8PDT')
)
  AND week_of_fyr <> 53);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;





CREATE TEMPORARY TABLE IF NOT EXISTS month_lookup
AS
SELECT DISTINCT month_num AS current_mo,
  CASE
  WHEN MOD(month_num, 100) = 1
  THEN month_num - 89
  ELSE month_num - 1
  END AS yr4_end_mo,
  month_num - 100 AS yr4_start_mo,
   CASE
   WHEN MOD(month_num, 100) = 1
   THEN month_num - 89
   ELSE month_num - 1
   END - 100 AS yr3_end_mo,
  month_num - 200 AS yr3_start_mo,
   CASE
   WHEN MOD(month_num, 100) = 1
   THEN month_num - 89
   ELSE month_num - 1
   END - 200 AS yr2_end_mo,
  month_num - 300 AS yr2_start_mo,
   CASE
   WHEN MOD(month_num, 100) = 1
   THEN month_num - 89
   ELSE month_num - 1
   END - 300 AS yr1_end_mo,
  month_num - 400 AS yr1_start_mo
FROM realigned_calendar
WHERE day_date = CURRENT_DATE('PST8PDT')
;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS month_names
AS
SELECT MAX(CASE
   WHEN month_num = (SELECT yr1_end_mo
     FROM month_lookup)
   THEN SUBSTR(CASE
     WHEN LOWER(month_short_desc) = LOWER('JAN')
     THEN 'FY-' || SUBSTR(CAST(MOD(year_num, 2000) AS STRING), 1, 2)
     ELSE 'YE-' || SUBSTR(month_short_desc, 1, 1) || LOWER(SUBSTR(month_short_desc, 2, 2)) || SUBSTR(CAST(MOD(year_num, 2000) AS STRING)
       , 1, 2)
     END, 1, 8)
   ELSE NULL
   END) AS yr1_name,
 MAX(CASE
   WHEN month_num = (SELECT yr2_end_mo
     FROM month_lookup)
   THEN SUBSTR(CASE
     WHEN LOWER(month_short_desc) = LOWER('JAN')
     THEN 'FY-' || SUBSTR(CAST(MOD(year_num, 2000) AS STRING), 1, 2)
     ELSE 'YE-' || SUBSTR(month_short_desc, 1, 1) || LOWER(SUBSTR(month_short_desc, 2, 2)) || SUBSTR(CAST(MOD(year_num, 2000) AS STRING)
       , 1, 2)
     END, 1, 8)
   ELSE NULL
   END) AS yr2_name,
 MAX(CASE
   WHEN month_num = (SELECT yr3_end_mo
     FROM month_lookup)
   THEN SUBSTR(CASE
     WHEN LOWER(month_short_desc) = LOWER('JAN')
     THEN 'FY-' || SUBSTR(CAST(MOD(year_num, 2000) AS STRING), 1, 2)
     ELSE 'YE-' || SUBSTR(month_short_desc, 1, 1) || LOWER(SUBSTR(month_short_desc, 2, 2)) || SUBSTR(CAST(MOD(year_num, 2000) AS STRING)
       , 1, 2)
     END, 1, 8)
   ELSE NULL
   END) AS yr3_name,
 MAX(CASE
   WHEN month_num = (SELECT yr4_end_mo
     FROM month_lookup)
   THEN SUBSTR(CASE
     WHEN LOWER(month_short_desc) = LOWER('JAN')
     THEN 'FY-' || SUBSTR(CAST(MOD(year_num, 2000) AS STRING), 1, 2)
     ELSE 'YE-' || SUBSTR(month_short_desc, 1, 1) || LOWER(SUBSTR(month_short_desc, 2, 2)) || SUBSTR(CAST(MOD(year_num, 2000) AS STRING)
       , 1, 2)
     END, 1, 8)
   ELSE NULL
   END) AS yr4_name
FROM realigned_calendar
WHERE month_num IN ((SELECT yr1_end_mo
    FROM month_lookup), (SELECT yr2_end_mo
    FROM month_lookup), (SELECT yr3_end_mo
    FROM month_lookup), (SELECT yr4_end_mo
    FROM month_lookup));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;




CREATE TEMPORARY TABLE IF NOT EXISTS cancel_custs
AS
SELECT DISTINCT a.acp_id,
  CASE
  WHEN b.month_num BETWEEN (SELECT yr1_start_mo
    FROM month_lookup) AND (SELECT yr1_end_mo
    FROM month_lookup)
  THEN (SELECT yr1_name
   FROM month_names)
  WHEN b.month_num BETWEEN (SELECT yr2_start_mo
    FROM month_lookup) AND (SELECT yr2_end_mo
    FROM month_lookup)
  THEN (SELECT yr2_name
   FROM month_names)
  WHEN b.month_num BETWEEN (SELECT yr3_start_mo
    FROM month_lookup) AND (SELECT yr3_end_mo
    FROM month_lookup)
  THEN (SELECT yr3_name
   FROM month_names)
  WHEN b.month_num BETWEEN (SELECT yr4_start_mo
    FROM month_lookup) AND (SELECT yr4_end_mo
    FROM month_lookup)
  THEN (SELECT yr4_name
   FROM month_names)
  ELSE NULL
  END AS reporting_year_shopped,
 1 AS cancel_flag
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.order_line_cancel_fact AS a
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal AS b ON a.order_date_pacific = b.day_date
WHERE a.order_date_pacific BETWEEN (SELECT MIN(day_date)
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal AS dc
   WHERE month_num = (SELECT yr1_start_mo
      FROM month_lookup)) AND (SELECT MAX(day_date)
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal AS dc
   WHERE month_num = (SELECT yr4_end_mo
      FROM month_lookup))
 AND LOWER(a.cancel_reason_code) NOT IN (LOWER('FRAUD_CHECK_FAILED'), LOWER('FRAUD_MANUAL_CANCEL'))
 AND a.acp_id IS NOT NULL;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;




CREATE TEMPORARY TABLE IF NOT EXISTS ship2store_order_activity
AS
SELECT DISTINCT a.global_tran_id,
 a.line_item_seq_num,
 a.item_delivery_method,
 a.source_channel_code
FROM `{{params.gcp_project_id}}`.t2dl_das_item_delivery.item_delivery_method_funnel_daily AS a
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS st ON COALESCE(a.picked_up_by_customer_node_num, - 1 * a.intent_store_num) = st
  .store_num
WHERE a.business_day_date BETWEEN (SELECT MIN(day_date)
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal AS dc
   WHERE month_num = (SELECT yr1_start_mo
      FROM month_lookup)) AND (SELECT DATE_ADD(MAX(day_date), INTERVAL 14 DAY)
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal AS dc
   WHERE month_num = (SELECT yr4_end_mo
      FROM month_lookup))
 AND a.order_date_pacific BETWEEN (SELECT MIN(day_date)
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal AS dc
   WHERE month_num = (SELECT yr1_start_mo
      FROM month_lookup)) AND (SELECT MAX(day_date)
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal AS dc
   WHERE month_num = (SELECT yr4_end_mo
      FROM month_lookup))
 AND LOWER(a.item_delivery_method) LIKE LOWER('%SHIP_TO_STORE')
 AND a.canceled_date_pacific IS NULL;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;





CREATE TEMPORARY TABLE IF NOT EXISTS ship2store_joined
AS
SELECT DISTINCT a.global_tran_id,
 a.line_item_seq_num,
 a.acp_id,
 a.reporting_year_shopped,
 a.channel,
 b.source_channel_code
FROM `{{params.gcp_project_id}}`.`{{params.gcp_project_id}}`.t2dl_das_strategy.cco_line_items AS a
 INNER JOIN ship2store_order_activity AS b ON a.global_tran_id = b.global_tran_id AND a.line_item_seq_num = b.line_item_seq_num;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;





CREATE TEMPORARY TABLE IF NOT EXISTS ship2store_cust_year
AS
SELECT acp_id,
 reporting_year_shopped,
 MAX(CASE
   WHEN LOWER(TRIM(source_channel_code)) = LOWER('FULL_LINE')
   THEN 1
   ELSE 0
   END) AS ncom_sts,
 MAX(CASE
   WHEN LOWER(TRIM(source_channel_code)) = LOWER('RACK')
   THEN 1
   ELSE 0
   END) AS rcom_sts
FROM ship2store_joined
GROUP BY acp_id,
 reporting_year_shopped;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;




CREATE TEMPORARY TABLE IF NOT EXISTS cust_spend_div_price
AS
SELECT a.acp_id,
 a.reporting_year_shopped,
 SUM(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('1'), LOWER('2')) AND a.div_num = 310
   THEN a.net_sales
   ELSE 0
   END) AS cust_nord_shoes_net_spend_ry,
 SUM(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('1'), LOWER('2')) AND a.div_num = 340
   THEN a.net_sales
   ELSE 0
   END) AS cust_nord_beauty_net_spend_ry,
 SUM(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('1'), LOWER('2')) AND a.div_num = 345
   THEN a.net_sales
   ELSE 0
   END) AS cust_nord_designer_apparel_net_spend_ry,
 SUM(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('1'), LOWER('2')) AND a.div_num = 351
   THEN a.net_sales
   ELSE 0
   END) AS cust_nord_apparel_net_spend_ry,
 SUM(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('1'), LOWER('2')) AND a.div_num = 360
   THEN a.net_sales
   ELSE 0
   END) AS cust_nord_accessories_net_spend_ry,
 SUM(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('1'), LOWER('2')) AND a.div_num = 365
   THEN a.net_sales
   ELSE 0
   END) AS cust_nord_home_net_spend_ry,
 SUM(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('1'), LOWER('2')) AND a.div_num = 700
   THEN a.net_sales
   ELSE 0
   END) AS cust_nord_merch_projects_net_spend_ry,
 SUM(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('1'), LOWER('2')) AND a.div_num = 800
   THEN a.net_sales
   ELSE 0
   END) AS cust_nord_leased_boutique_net_spend_ry,
 SUM(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('1'), LOWER('2')) AND a.div_num = 70
   THEN a.net_sales
   ELSE 0
   END) AS cust_nord_restaurant_net_spend_ry,
 SUM(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('3'), LOWER('4')) AND a.div_num = 310
   THEN a.net_sales
   ELSE 0
   END) AS cust_rack_shoes_net_spend_ry,
 SUM(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('3'), LOWER('4')) AND a.div_num = 340
   THEN a.net_sales
   ELSE 0
   END) AS cust_rack_beauty_net_spend_ry,
 SUM(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('3'), LOWER('4')) AND a.div_num = 345
   THEN a.net_sales
   ELSE 0
   END) AS cust_rack_designer_apparel_net_spend_ry,
 SUM(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('3'), LOWER('4')) AND a.div_num = 351
   THEN a.net_sales
   ELSE 0
   END) AS cust_rack_apparel_net_spend_ry,
 SUM(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('3'), LOWER('4')) AND a.div_num = 360
   THEN a.net_sales
   ELSE 0
   END) AS cust_rack_accessories_net_spend_ry,
 SUM(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('3'), LOWER('4')) AND a.div_num = 365
   THEN a.net_sales
   ELSE 0
   END) AS cust_rack_home_net_spend_ry,
 SUM(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('3'), LOWER('4')) AND a.div_num = 700
   THEN a.net_sales
   ELSE 0
   END) AS cust_rack_merch_projects_net_spend_ry,
 SUM(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('3'), LOWER('4')) AND a.div_num = 800
   THEN a.net_sales
   ELSE 0
   END) AS cust_rack_leased_boutique_net_spend_ry,
 SUM(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('3'), LOWER('4')) AND a.div_num = 70
   THEN a.net_sales
   ELSE 0
   END) AS cust_rack_restaurant_net_spend_ry,
 SUM(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('1'), LOWER('2')) AND LOWER(a.price_type) = LOWER('R')
   THEN a.net_sales
   ELSE 0
   END) AS cust_nord_regprice_net_spend_ry,
 SUM(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('1'), LOWER('2')) AND LOWER(a.price_type) = LOWER('P')
   THEN a.net_sales
   ELSE 0
   END) AS cust_nord_promo_net_spend_ry,
 SUM(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('1'), LOWER('2')) AND LOWER(a.price_type) = LOWER('C')
   THEN a.net_sales
   ELSE 0
   END) AS cust_nord_clearance_net_spend_ry,
 SUM(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('3'), LOWER('4')) AND LOWER(a.price_type) = LOWER('R')
   THEN a.net_sales
   ELSE 0
   END) AS cust_rack_regprice_net_spend_ry,
 SUM(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('3'), LOWER('4')) AND LOWER(a.price_type) = LOWER('P')
   THEN a.net_sales
   ELSE 0
   END) AS cust_rack_promo_net_spend_ry,
 SUM(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('3'), LOWER('4')) AND LOWER(a.price_type) = LOWER('C')
   THEN a.net_sales
   ELSE 0
   END) AS cust_rack_clearance_net_spend_ry,
 MAX(CASE
   WHEN a.div_num = 70
   THEN 1
   ELSE 0
   END) AS restaurant_max,
 MIN(CASE
   WHEN a.div_num = 70
   THEN 1
   ELSE 0
   END) AS restaurant_min,
 MAX(CASE
   WHEN LOWER(sn.store_type_code) = LOWER('NL') OR LOWER(rs.store_type_code) = LOWER('NL') OR LOWER(ps.store_type_code)
     = LOWER('NL')
   THEN 1
   ELSE 0
   END) AS shopped_nord_local,
 MAX(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('2') AND LOWER(SUBSTR(a.return_channel, 1, 1)) = LOWER('1')
   THEN 1
   ELSE 0
   END) AS ncom_ret_to_nstore,
 MAX(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('2') AND LOWER(SUBSTR(a.return_channel, 1, 1)) = LOWER('2')
   THEN 1
   ELSE 0
   END) AS ncom_ret_by_mail,
 MAX(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('2') AND LOWER(SUBSTR(a.return_channel, 1, 1)) = LOWER('3')
   THEN 1
   ELSE 0
   END) AS ncom_ret_to_rstore,
 MAX(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('4') AND LOWER(SUBSTR(a.return_channel, 1, 1)) = LOWER('1')
   THEN 1
   ELSE 0
   END) AS rcom_ret_to_nstore,
 MAX(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('4') AND LOWER(SUBSTR(a.return_channel, 1, 1)) = LOWER('4')
   THEN 1
   ELSE 0
   END) AS rcom_ret_by_mail,
 MAX(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('4') AND LOWER(SUBSTR(a.return_channel, 1, 1)) = LOWER('3')
   THEN 1
   ELSE 0
   END) AS rcom_ret_to_rstore
FROM `{{params.gcp_project_id}}`.`{{params.gcp_project_id}}`.t2dl_das_strategy.cco_line_items AS a
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS sn ON a.store_num = sn.store_num
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS rs ON COALESCE(a.return_store, - 1 * a.store_num) = rs.store_num
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS ps ON COALESCE(a.pickup_store, - 1 * a.store_num) = ps.store_num
WHERE a.reporting_year_shopped IS NOT NULL
GROUP BY a.acp_id,
 a.reporting_year_shopped;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;








CREATE TEMPORARY TABLE IF NOT EXISTS cust_year_everything
AS
SELECT DISTINCT a.acp_id,
 a.reporting_year_shopped,
 a.cust_engagement_cohort AS engagement_cohort,
 a.cust_jwn_trip_bucket,
 a.cust_jwn_net_spend_bucket,
 a.cust_channel_combo,
 a.cust_loyalty_type,
 a.cust_loyalty_level,
 a.cust_gender,
 a.cust_age_group,
 a.cust_employee_flag,
 a.cust_chan_holiday,
 a.cust_chan_anniversary,
 a.cust_tenure_bucket_years,
 a.cust_acquisition_fiscal_year,
 a.cust_acquisition_channel,
 a.cust_activation_channel,
 a.cust_country,
 a.cust_nms_market,
 a.cust_dma,
 a.cust_tender_nordstrom,
 a.cust_tender_nordstrom_note,
 a.cust_tender_gift_card,
 a.cust_tender_other,
 COALESCE(b.cancel_flag, 0) AS cust_had_a_cancel,
 a.cust_svc_group_exp_delivery,
 a.cust_svc_group_order_pickup,
 a.cust_svc_group_selling_relation,
 a.cust_svc_group_remote_selling,
 a.cust_svc_group_alterations,
 a.cust_svc_group_in_store,
 a.cust_svc_group_restaurant,
 a.cust_service_free_exp_delivery,
 a.cust_service_next_day_pickup,
 a.cust_service_same_day_bopus,
 a.cust_service_curbside_pickup,
 a.cust_service_style_boards,
 a.cust_service_gift_wrapping,
 a.cust_service_pop_in,
 a.ns_buyerflow,
 a.ncom_buyerflow,
 a.rs_buyerflow,
 a.rcom_buyerflow,
 a.nord_buyerflow,
 a.rack_buyerflow,
 a.jwn_buyerflow,
 a.cust_jwn_acquired_aare,
 a.cust_jwn_activated_aare,
 a.cust_jwn_retained_aare,
 a.cust_jwn_engaged_aare,
 a.shopped_fls,
 a.shopped_ncom,
 a.shopped_rack,
 a.shopped_rcom,
 a.shopped_fp,
 a.shopped_op,
 a.shopped_jwn,
 a.cust_made_a_return,
 a.cust_platform_desktop,
 a.cust_platform_mow,
 a.cust_platform_ios,
 a.cust_platform_android,
 a.cust_platform_pos,
 COALESCE(h.ncom_sts, 0) AS cust_service_ship2store_ncom,
 COALESCE(h.rcom_sts, 0) AS cust_service_ship2store_rcom,
  CASE
  WHEN d.restaurant_max = 1 AND d.restaurant_min = 1
  THEN 1
  ELSE 0
  END AS restaurant_only_flag,
 COALESCE(g.closest_store_dist_bucket, 'missing') AS closest_store_dist_bucket,
 g.closest_store_banner,
 COALESCE(g.nord_sol_dist_bucket, 'missing') AS nord_sol_dist_bucket,
 COALESCE(g.rack_sol_dist_bucket, 'missing') AS rack_sol_dist_bucket,
 d.shopped_nord_local,
 COALESCE(d.ncom_ret_to_nstore, 0) AS ncom_ret_to_nstore,
 COALESCE(d.ncom_ret_by_mail, 0) AS ncom_ret_by_mail,
 COALESCE(d.ncom_ret_to_rstore, 0) AS ncom_ret_to_rstore,
 COALESCE(d.rcom_ret_to_nstore, 0) AS rcom_ret_to_nstore,
 COALESCE(d.rcom_ret_by_mail, 0) AS rcom_ret_by_mail,
 COALESCE(d.rcom_ret_to_rstore, 0) AS rcom_ret_to_rstore,
 d.cust_nord_shoes_net_spend_ry,
 d.cust_nord_beauty_net_spend_ry,
 d.cust_nord_designer_apparel_net_spend_ry,
 d.cust_nord_apparel_net_spend_ry,
 d.cust_nord_accessories_net_spend_ry,
 d.cust_nord_home_net_spend_ry,
 d.cust_nord_merch_projects_net_spend_ry,
 d.cust_nord_leased_boutique_net_spend_ry,
 d.cust_nord_restaurant_net_spend_ry,
 d.cust_rack_shoes_net_spend_ry,
 d.cust_rack_beauty_net_spend_ry,
 d.cust_rack_designer_apparel_net_spend_ry,
 d.cust_rack_apparel_net_spend_ry,
 d.cust_rack_accessories_net_spend_ry,
 d.cust_rack_home_net_spend_ry,
 d.cust_rack_merch_projects_net_spend_ry,
 d.cust_rack_leased_boutique_net_spend_ry,
 d.cust_rack_restaurant_net_spend_ry,
 d.cust_nord_regprice_net_spend_ry,
 d.cust_nord_promo_net_spend_ry,
 d.cust_nord_clearance_net_spend_ry,
 d.cust_rack_regprice_net_spend_ry,
 d.cust_rack_promo_net_spend_ry,
 d.cust_rack_clearance_net_spend_ry
FROM cust_year_level_attributes AS a
 LEFT JOIN cancel_custs AS b ON LOWER(a.acp_id) = LOWER(b.acp_id) AND LOWER(a.reporting_year_shopped) = LOWER(b.reporting_year_shopped
    )
 LEFT JOIN cust_spend_div_price AS d ON LOWER(a.acp_id) = LOWER(d.acp_id) AND LOWER(a.reporting_year_shopped) = LOWER(d
    .reporting_year_shopped)
 LEFT JOIN `{{params.gcp_project_id}}`.`{{params.gcp_project_id}}`.t2dl_das_strategy.customer_store_distance_buckets AS g ON LOWER(a.acp_id) = LOWER(g.acp_id)
 LEFT JOIN ship2store_cust_year AS h ON LOWER(a.acp_id) = LOWER(h.acp_id) AND LOWER(a.reporting_year_shopped) = LOWER(h
    .reporting_year_shopped);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;






CREATE TEMPORARY TABLE IF NOT EXISTS cust_trx_everything
AS
SELECT data_source,
 acp_id,
 banner,
 channel,
 channel_country,
 store_num,
 store_region,
 div_num,
 subdiv_num,
 dept_num,
 npg_flag,
 brand_name,
 store_country,
 date_shopped,
 fiscal_month_num,
 fiscal_qtr_num,
 fiscal_yr_num,
 item_delivery_method,
 employee_flag,
 reporting_year_shopped,
  CASE
  WHEN gross_sales > 0
  THEN acp_id || FORMAT('%11d', store_num) || CAST(date_shopped AS STRING)
  ELSE NULL
  END AS trip_id,
 SUM(gross_sales) AS gross_sales,
 SUM(return_amt) AS return_amt,
 SUM(net_sales) AS net_sales,
 SUM(gross_items) AS gross_items,
 SUM(return_items) AS return_items,
 SUM(net_items) AS net_items,
 SUM(CASE
   WHEN LOWER(price_type) = LOWER('R')
   THEN net_sales
   ELSE 0
   END) AS regprice_net_spend,
 SUM(CASE
   WHEN LOWER(price_type) = LOWER('P')
   THEN net_sales
   ELSE 0
   END) AS promo_net_spend,
 SUM(CASE
   WHEN LOWER(price_type) = LOWER('C')
   THEN net_sales
   ELSE 0
   END) AS clearance_net_spend,
 SUM(CASE
   WHEN LOWER(price_type) = LOWER('R')
   THEN gross_sales
   ELSE 0
   END) AS regprice_gross_spend,
 SUM(CASE
   WHEN LOWER(price_type) = LOWER('P')
   THEN gross_sales
   ELSE 0
   END) AS promo_gross_spend,
 SUM(CASE
   WHEN LOWER(price_type) = LOWER('C')
   THEN gross_sales
   ELSE 0
   END) AS clearance_gross_spend,
 SUM(CASE
   WHEN LOWER(price_type) = LOWER('R')
   THEN return_amt
   ELSE 0
   END) AS regprice_return_amt,
 SUM(CASE
   WHEN LOWER(price_type) = LOWER('P')
   THEN return_amt
   ELSE 0
   END) AS promo_return_amt,
 SUM(CASE
   WHEN LOWER(price_type) = LOWER('C')
   THEN return_amt
   ELSE 0
   END) AS clearance_return_amt,
 SUM(CASE
   WHEN LOWER(price_type) = LOWER('R')
   THEN net_items
   ELSE 0
   END) AS regprice_net_items,
 SUM(CASE
   WHEN LOWER(price_type) = LOWER('P')
   THEN net_items
   ELSE 0
   END) AS promo_net_items,
 SUM(CASE
   WHEN LOWER(price_type) = LOWER('C')
   THEN net_items
   ELSE 0
   END) AS clearance_net_items
FROM `{{params.gcp_project_id}}`.`{{params.gcp_project_id}}`.t2dl_das_strategy.cco_line_items AS cli
WHERE date_shopped BETWEEN (SELECT start_date
   FROM date_range) AND (SELECT p2_start_date
   FROM date_range)
 AND reporting_year_shopped IS NOT NULL
GROUP BY data_source,
 acp_id,
 banner,
 channel,
 channel_country,
 store_num,
 store_region,
 div_num,
 subdiv_num,
 dept_num,
 npg_flag,
 brand_name,
 store_country,
 date_shopped,
 fiscal_month_num,
 fiscal_qtr_num,
 fiscal_yr_num,
 item_delivery_method,
 employee_flag,
 reporting_year_shopped,
 trip_id;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO cust_trx_everything
(SELECT data_source,
  acp_id,
  banner,
  channel,
  channel_country,
  store_num,
  store_region,
  div_num,
  subdiv_num,
  dept_num,
  npg_flag,
  brand_name,
  store_country,
  date_shopped,
  fiscal_month_num,
  fiscal_qtr_num,
  fiscal_yr_num,
  item_delivery_method,
  employee_flag,
  reporting_year_shopped,
   CASE
   WHEN gross_sales > 0
   THEN acp_id || FORMAT('%11d', store_num) || CAST(date_shopped AS STRING)
   ELSE NULL
   END AS trip_id,
  SUM(gross_sales) AS gross_sales,
  SUM(return_amt) AS return_amt,
  SUM(net_sales) AS net_sales,
  SUM(gross_items) AS gross_items,
  SUM(return_items) AS return_items,
  SUM(net_items) AS net_items,
  SUM(CASE
    WHEN LOWER(price_type) = LOWER('R')
    THEN net_sales
    ELSE 0
    END) AS regprice_net_spend,
  SUM(CASE
    WHEN LOWER(price_type) = LOWER('P')
    THEN net_sales
    ELSE 0
    END) AS promo_net_spend,
  SUM(CASE
    WHEN LOWER(price_type) = LOWER('C')
    THEN net_sales
    ELSE 0
    END) AS clearance_net_spend,
  SUM(CASE
    WHEN LOWER(price_type) = LOWER('R')
    THEN gross_sales
    ELSE 0
    END) AS regprice_gross_spend,
  SUM(CASE
    WHEN LOWER(price_type) = LOWER('P')
    THEN gross_sales
    ELSE 0
    END) AS promo_gross_spend,
  SUM(CASE
    WHEN LOWER(price_type) = LOWER('C')
    THEN gross_sales
    ELSE 0
    END) AS clearance_gross_spend,
  SUM(CASE
    WHEN LOWER(price_type) = LOWER('R')
    THEN return_amt
    ELSE 0
    END) AS regprice_return_amt,
  SUM(CASE
    WHEN LOWER(price_type) = LOWER('P')
    THEN return_amt
    ELSE 0
    END) AS promo_return_amt,
  SUM(CASE
    WHEN LOWER(price_type) = LOWER('C')
    THEN return_amt
    ELSE 0
    END) AS clearance_return_amt,
  SUM(CASE
    WHEN LOWER(price_type) = LOWER('R')
    THEN net_items
    ELSE 0
    END) AS regprice_net_items,
  SUM(CASE
    WHEN LOWER(price_type) = LOWER('P')
    THEN net_items
    ELSE 0
    END) AS promo_net_items,
  SUM(CASE
    WHEN LOWER(price_type) = LOWER('C')
    THEN net_items
    ELSE 0
    END) AS clearance_net_items
 FROM `{{params.gcp_project_id}}`.`{{params.gcp_project_id}}`.t2dl_das_strategy.cco_line_items AS cli
 WHERE date_shopped BETWEEN (SELECT DATE_ADD(p2_start_date, INTERVAL 1 DAY)
    FROM date_range) AND (SELECT p2_end_date
    FROM date_range)
  AND reporting_year_shopped IS NOT NULL
 GROUP BY data_source,
  acp_id,
  banner,
  channel,
  channel_country,
  store_num,
  store_region,
  div_num,
  subdiv_num,
  dept_num,
  npg_flag,
  brand_name,
  store_country,
  date_shopped,
  fiscal_month_num,
  fiscal_qtr_num,
  fiscal_yr_num,
  item_delivery_method,
  employee_flag,
  reporting_year_shopped,
  trip_id);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO cust_trx_everything
(SELECT data_source,
  acp_id,
  banner,
  channel,
  channel_country,
  store_num,
  store_region,
  div_num,
  subdiv_num,
  dept_num,
  npg_flag,
  brand_name,
  store_country,
  date_shopped,
  fiscal_month_num,
  fiscal_qtr_num,
  fiscal_yr_num,
  item_delivery_method,
  employee_flag,
  reporting_year_shopped,
   CASE
   WHEN gross_sales > 0
   THEN acp_id || FORMAT('%11d', store_num) || CAST(date_shopped AS STRING)
   ELSE NULL
   END AS trip_id,
  SUM(gross_sales) AS gross_sales,
  SUM(return_amt) AS return_amt,
  SUM(net_sales) AS net_sales,
  SUM(gross_items) AS gross_items,
  SUM(return_items) AS return_items,
  SUM(net_items) AS net_items,
  SUM(CASE
    WHEN LOWER(price_type) = LOWER('R')
    THEN net_sales
    ELSE 0
    END) AS regprice_net_spend,
  SUM(CASE
    WHEN LOWER(price_type) = LOWER('P')
    THEN net_sales
    ELSE 0
    END) AS promo_net_spend,
  SUM(CASE
    WHEN LOWER(price_type) = LOWER('C')
    THEN net_sales
    ELSE 0
    END) AS clearance_net_spend,
  SUM(CASE
    WHEN LOWER(price_type) = LOWER('R')
    THEN gross_sales
    ELSE 0
    END) AS regprice_gross_spend,
  SUM(CASE
    WHEN LOWER(price_type) = LOWER('P')
    THEN gross_sales
    ELSE 0
    END) AS promo_gross_spend,
  SUM(CASE
    WHEN LOWER(price_type) = LOWER('C')
    THEN gross_sales
    ELSE 0
    END) AS clearance_gross_spend,
  SUM(CASE
    WHEN LOWER(price_type) = LOWER('R')
    THEN return_amt
    ELSE 0
    END) AS regprice_return_amt,
  SUM(CASE
    WHEN LOWER(price_type) = LOWER('P')
    THEN return_amt
    ELSE 0
    END) AS promo_return_amt,
  SUM(CASE
    WHEN LOWER(price_type) = LOWER('C')
    THEN return_amt
    ELSE 0
    END) AS clearance_return_amt,
  SUM(CASE
    WHEN LOWER(price_type) = LOWER('R')
    THEN net_items
    ELSE 0
    END) AS regprice_net_items,
  SUM(CASE
    WHEN LOWER(price_type) = LOWER('P')
    THEN net_items
    ELSE 0
    END) AS promo_net_items,
  SUM(CASE
    WHEN LOWER(price_type) = LOWER('C')
    THEN net_items
    ELSE 0
    END) AS clearance_net_items
 FROM `{{params.gcp_project_id}}`.`{{params.gcp_project_id}}`.t2dl_das_strategy.cco_line_items AS cli
 WHERE date_shopped BETWEEN (SELECT DATE_ADD(p2_end_date, INTERVAL 1 DAY)
    FROM date_range) AND (SELECT end_date
    FROM date_range)
  AND reporting_year_shopped IS NOT NULL
 GROUP BY data_source,
  acp_id,
  banner,
  channel,
  channel_country,
  store_num,
  store_region,
  div_num,
  subdiv_num,
  dept_num,
  npg_flag,
  brand_name,
  store_country,
  date_shopped,
  fiscal_month_num,
  fiscal_qtr_num,
  fiscal_yr_num,
  item_delivery_method,
  employee_flag,
  reporting_year_shopped,
  trip_id);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.customer_sandbox_fact;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS current_timestamp_tmp AS
SELECT CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')
) AS DATETIME) AS dw_sys_tmstp;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.customer_sandbox_fact
(SELECT a.data_source,
  a.acp_id,
  s.banner_num AS store_banner_num,
  s.channel_num AS store_channel_num,
  s.channel_country_num AS store_channel_country,
  a.store_num,
  s.store_region_num,
  COALESCE(a.div_num, - 1) AS div_num,
  COALESCE(a.subdiv_num, - 1) AS subdiv_num,
  COALESCE(a.dept_num, - 1) AS dept_num,
  a.npg_flag,
  COALESCE(a.brand_name, 'Others') AS brand_name,
  a.date_shopped,
  a.fiscal_month_num AS month_num,
  a.fiscal_qtr_num AS quarter_num,
  a.fiscal_yr_num AS year_num,
  a.trip_id,
  a.gross_sales,
  a.return_amt,
  a.net_sales,
  a.gross_items,
  a.return_items,
  a.net_items,
  a.regprice_net_spend,
  a.promo_net_spend,
  a.clearance_net_spend,
  a.regprice_gross_spend,
  a.promo_gross_spend,
  a.clearance_gross_spend,
  a.regprice_return_amt,
  a.promo_return_amt,
  a.clearance_return_amt,
  a.regprice_net_items,
  a.promo_net_items,
  a.clearance_net_items,
  r.item_delivery_method_num,
  a.employee_flag,
  ry.reporting_year_shopped_num,
  t.audience_engagement_cohort_num,
  c.cust_jwn_trip_bucket_num,
  g.cust_jwn_net_spend_bucket_num,
  CAST(TRUNC(CAST(NULL AS FLOAT64))AS INTEGER) AS cust_jwn_net_spend_bucket_ly_num,
  CAST(TRUNC(CAST(NULL AS FLOAT64))AS INTEGER) AS cust_jwn_trip_bucket_ly_num,
  CAST(TRUNC(CAST(NULL AS FLOAT64))AS INTEGER) AS cust_jwn_net_spend_bucket_fy_num,
  CAST(TRUNC(CAST(NULL AS FLOAT64))AS INTEGER) AS cust_jwn_trip_bucket_num_fy_num,
  CAST(TRUNC(CAST(NULL AS FLOAT64))AS INTEGER) AS audience_engagement_cohort_ly_num,
  CAST(TRUNC(CAST(NULL AS FLOAT64))AS INTEGER) AS audience_engagement_cohort_fy_num,
  CAST(TRUNC(CAST(NULL AS FLOAT64))AS INTEGER) AS cust_rack_trip_bucket_num,
  CAST(TRUNC(CAST(NULL AS FLOAT64))AS INTEGER) AS cust_rack_net_spend_bucket_num,
  CAST(TRUNC(CAST(NULL AS FLOAT64))AS INTEGER) AS cust_nord_trip_bucket_num,
  CAST(TRUNC(CAST(NULL AS FLOAT64))AS INTEGER) AS cust_nord_net_spend_bucket_num,
  h.cust_channel_combo_num,
  i.cust_loyalty_type_num,
  l.cust_loyalty_level_num,
  b.cust_gender,
  d.cust_age_group_num,
  b.cust_employee_flag,
  e.cust_tenure_bucket_years_num,
  b.cust_acquisition_fiscal_year,
  f.cust_channel_num AS cust_acquisition_channel_num,
  j.cust_channel_num AS cust_activation_channel_num,
  k.cust_nms_market_num,
  k.cust_nms_region_num,
  m.cust_dma_num,
  m.cust_region_num,
  m.cust_country_num,
  b.cust_tender_nordstrom,
  b.cust_tender_nordstrom_note,
  b.cust_tender_gift_card,
  b.cust_tender_other,
  b.cust_had_a_cancel,
  b.cust_svc_group_exp_delivery,
  b.cust_svc_group_order_pickup,
  b.cust_svc_group_selling_relation,
  b.cust_svc_group_remote_selling,
  b.cust_svc_group_alterations,
  b.cust_svc_group_in_store,
  b.cust_svc_group_restaurant,
  b.cust_service_free_exp_delivery,
  b.cust_service_next_day_pickup,
  b.cust_service_same_day_bopus,
  b.cust_service_curbside_pickup,
  b.cust_service_style_boards,
  b.cust_service_gift_wrapping,
  CAST(NULL AS INTEGER) AS cust_styling,
  b.cust_service_pop_in,
  u.cust_chan_buyer_flow_num AS cust_ns_buyerflow_num,
  v.cust_chan_buyer_flow_num AS cust_ncom_buyerflow_num,
  w.cust_chan_buyer_flow_num AS cust_rs_buyerflow_num,
  x.cust_chan_buyer_flow_num AS cust_rcom_buyerflow_num,
  y.cust_chan_buyer_flow_num AS cust_nord_buyerflow_num,
  z.cust_chan_buyer_flow_num AS cust_rack_buyerflow_num,
  aa.cust_chan_buyer_flow_num AS cust_jwn_buyerflow_num,
  b.cust_jwn_acquired_aare,
  b.cust_jwn_activated_aare,
  b.cust_jwn_retained_aare,
  b.cust_jwn_engaged_aare,
  b.shopped_fls,
  b.shopped_ncom,
  b.shopped_rack,
  b.shopped_rcom,
  b.shopped_fp,
  b.shopped_op,
  b.shopped_jwn,
  b.cust_made_a_return,
  b.cust_platform_desktop,
  b.cust_platform_mow,
  b.cust_platform_ios,
  b.cust_platform_android,
  b.cust_platform_pos,
  b.cust_service_ship2store_ncom,
  b.cust_service_ship2store_rcom,
  b.restaurant_only_flag,
  q.closest_sol_dist_bucket_num,
  p.closest_store_banner_num,
  o.nord_sol_dist_bucket_num,
  n.rack_sol_dist_bucket_num,
  b.shopped_nord_local,
  b.ncom_ret_to_nstore,
  b.ncom_ret_by_mail,
  b.ncom_ret_to_rstore,
  b.rcom_ret_to_nstore,
  b.rcom_ret_by_mail,
  b.rcom_ret_to_rstore,
  b.cust_nord_shoes_net_spend_ry,
  b.cust_nord_beauty_net_spend_ry,
  b.cust_nord_designer_apparel_net_spend_ry,
  b.cust_nord_apparel_net_spend_ry,
  b.cust_nord_accessories_net_spend_ry,
  b.cust_nord_home_net_spend_ry,
  b.cust_nord_merch_projects_net_spend_ry,
  b.cust_nord_leased_boutique_net_spend_ry,
  b.cust_nord_restaurant_net_spend_ry,
  b.cust_rack_shoes_net_spend_ry,
  b.cust_rack_beauty_net_spend_ry,
  b.cust_rack_designer_apparel_net_spend_ry,
  b.cust_rack_apparel_net_spend_ry,
  b.cust_rack_accessories_net_spend_ry,
  b.cust_rack_home_net_spend_ry,
  b.cust_rack_merch_projects_net_spend_ry,
  b.cust_rack_leased_boutique_net_spend_ry,
  b.cust_rack_restaurant_net_spend_ry,
  b.cust_nord_regprice_net_spend_ry,
  b.cust_nord_promo_net_spend_ry,
  b.cust_nord_clearance_net_spend_ry,
  b.cust_rack_regprice_net_spend_ry,
  b.cust_rack_promo_net_spend_ry,
  b.cust_rack_clearance_net_spend_ry,
  b.cust_chan_holiday,
  b.cust_chan_anniversary,
  CAST(TRUNC(CAST(NULL AS FLOAT64))AS INTEGER) AS cust_contr_margin_decile,
  CAST(NULL AS NUMERIC) AS cust_contr_margin_amt,
   (SELECT *
   FROM current_timestamp_tmp) AS dw_sys_load_tmstp,
   (SELECT *
   FROM current_timestamp_tmp) AS dw_sys_updt_tmstp
 FROM cust_trx_everything AS a
  INNER JOIN cust_year_everything AS b ON LOWER(a.acp_id) = LOWER(b.acp_id) AND LOWER(a.reporting_year_shopped) = LOWER(b
     .reporting_year_shopped)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.reporting_year_shopped_lkp AS ry ON LOWER(COALESCE(a.reporting_year_shopped, 'Missing')) =
   LOWER(ry.reporting_year_shopped_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_jwn_trip_bucket_lkp AS c ON LOWER(COALESCE(b.cust_jwn_trip_bucket, 'Unknown')) =
   LOWER(c.cust_jwn_trip_bucket_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_age_group_lkp AS d ON LOWER(COALESCE(b.cust_age_group, 'Unknown')) = LOWER(d.cust_age_group_desc
    )
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_tenure_bucket_years_lkp AS e ON LOWER(COALESCE(b.cust_tenure_bucket_years, 'Unknown'
     )) = LOWER(e.cust_tenure_bucket_years_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_channel_lkp AS f ON LOWER(COALESCE(b.cust_acquisition_channel, 'Unknown')) = LOWER(f
    .cust_channel_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_jwn_net_spend_bucket_lkp AS g ON LOWER(COALESCE(b.cust_jwn_net_spend_bucket,
     'Unknown')) = LOWER(g.cust_jwn_net_spend_bucket_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_channel_combo_lkp AS h ON LOWER(COALESCE(b.cust_channel_combo, 'Unknown')) = LOWER(h
    .cust_channel_combo_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_loyalty_type_lkp AS i ON LOWER(COALESCE(b.cust_loyalty_type, 'Unknown')) = LOWER(i.cust_loyalty_type_desc
    )
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_channel_lkp AS j ON LOWER(COALESCE(b.cust_activation_channel, 'Unknown')) = LOWER(j.cust_channel_desc
    )
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_nms_market_lkp AS k ON LOWER(COALESCE(b.cust_nms_market, 'NON-NMS MARKET')) = LOWER(k
    .cust_nms_market_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_loyalty_level_lkp AS l ON LOWER(COALESCE(b.cust_loyalty_level, 'Unknown')) = LOWER(l
    .cust_loyalty_level_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_dma_lkp AS m ON LOWER(COALESCE(b.cust_dma, 'Unknown')) = LOWER(m.cust_dma_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.rack_sol_dist_bucket_lkp AS n ON LOWER(COALESCE(b.rack_sol_dist_bucket, 'missing')) =
   LOWER(n.rack_sol_dist_bucket_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.nord_sol_dist_bucket_lkp AS o ON LOWER(COALESCE(b.nord_sol_dist_bucket, 'missing')) =
   LOWER(o.nord_sol_dist_bucket_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.closest_store_banner_lkp AS p ON LOWER(COALESCE(b.closest_store_banner, 'missing')) =
   LOWER(p.closest_store_banner_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.closest_sol_dist_bucket_lkp AS q ON LOWER(COALESCE(b.closest_store_dist_bucket, 'missing'
     )) = LOWER(q.closest_sol_dist_bucket_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.item_delivery_method_lkp AS r ON LOWER(COALESCE(a.item_delivery_method, 'NA')) = LOWER(r.item_delivery_method_desc
    )
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.store_lkp AS s ON CAST(COALESCE(FORMAT('%11d', a.store_num), 'Unknown') AS FLOAT64) = s.store_num
     AND LOWER(s.channel_country_desc) = LOWER(a.channel_country)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.audience_engagement_cohort_lkp AS t ON LOWER(COALESCE(b.engagement_cohort, 'missing')) =
   LOWER(t.audience_engagement_cohort_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_chan_buyer_flow_lkp AS u ON LOWER(u.cust_chan_buyer_flow_desc) = LOWER(COALESCE(b.ns_buyerflow
     , 'NA'))
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_chan_buyer_flow_lkp AS v ON LOWER(v.cust_chan_buyer_flow_desc) = LOWER(COALESCE(b.ncom_buyerflow
     , 'NA'))
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_chan_buyer_flow_lkp AS w ON LOWER(w.cust_chan_buyer_flow_desc) = LOWER(COALESCE(b.rs_buyerflow
     , 'NA'))
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_chan_buyer_flow_lkp AS x ON LOWER(x.cust_chan_buyer_flow_desc) = LOWER(COALESCE(b.rcom_buyerflow
     , 'NA'))
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_chan_buyer_flow_lkp AS y ON LOWER(y.cust_chan_buyer_flow_desc) = LOWER(COALESCE(b.nord_buyerflow
     , 'NA'))
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_chan_buyer_flow_lkp AS z ON LOWER(z.cust_chan_buyer_flow_desc) = LOWER(COALESCE(b.rack_buyerflow
     , 'NA'))
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_chan_buyer_flow_lkp AS aa ON LOWER(aa.cust_chan_buyer_flow_desc) = LOWER(COALESCE(b.jwn_buyerflow
     , 'NA'))
 WHERE a.date_shopped BETWEEN (SELECT start_date
    FROM date_range) AND (SELECT p2_start_date
    FROM date_range));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;







INSERT INTO `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.customer_sandbox_fact
(SELECT a.data_source,
  a.acp_id,
  s.banner_num AS store_banner_num,
  s.channel_num AS store_channel_num,
  s.channel_country_num AS store_channel_country,
  a.store_num,
  s.store_region_num,
  COALESCE(a.div_num, - 1) AS div_num,
  COALESCE(a.subdiv_num, - 1) AS subdiv_num,
  COALESCE(a.dept_num, - 1) AS dept_num,
  a.npg_flag,
  COALESCE(a.brand_name, 'Others') AS brand_name,
  a.date_shopped,
  a.fiscal_month_num AS month_num,
  a.fiscal_qtr_num AS quarter_num,
  a.fiscal_yr_num AS year_num,
  a.trip_id,
  a.gross_sales,
  a.return_amt,
  a.net_sales,
  a.gross_items,
  a.return_items,
  a.net_items,
  a.regprice_net_spend,
  a.promo_net_spend,
  a.clearance_net_spend,
  a.regprice_gross_spend,
  a.promo_gross_spend,
  a.clearance_gross_spend,
  a.regprice_return_amt,
  a.promo_return_amt,
  a.clearance_return_amt,
  a.regprice_net_items,
  a.promo_net_items,
  a.clearance_net_items,
  r.item_delivery_method_num,
  a.employee_flag,
  ry.reporting_year_shopped_num,
  t.audience_engagement_cohort_num,
  c.cust_jwn_trip_bucket_num,
  g.cust_jwn_net_spend_bucket_num,
  CAST(NULL AS INTEGER) AS cust_jwn_net_spend_bucket_ly_num,
  CAST(NULL AS INTEGER) AS cust_jwn_trip_bucket_ly_num,
  CAST(NULL AS INTEGER) AS cust_jwn_net_spend_bucket_fy_num,
  CAST(NULL AS INTEGER) AS cust_jwn_trip_bucket_num_fy_num,
  CAST(NULL AS INTEGER) AS audience_engagement_cohort_ly_num,
  CAST(NULL AS INTEGER) AS audience_engagement_cohort_fy_num,
  CAST(NULL AS INTEGER) AS cust_rack_trip_bucket_num,
  CAST(NULL AS INTEGER) AS cust_rack_net_spend_bucket_num,
  CAST(NULL AS INTEGER) AS cust_nord_trip_bucket_num,
  CAST(NULL AS INTEGER) AS cust_nord_net_spend_bucket_num,
  h.cust_channel_combo_num,
  i.cust_loyalty_type_num,
  l.cust_loyalty_level_num,
  b.cust_gender,
  d.cust_age_group_num,
  b.cust_employee_flag,
  e.cust_tenure_bucket_years_num,
  b.cust_acquisition_fiscal_year,
  f.cust_channel_num AS cust_acquisition_channel_num,
  j.cust_channel_num AS cust_activation_channel_num,
  k.cust_nms_market_num,
  k.cust_nms_region_num,
  m.cust_dma_num,
  m.cust_region_num,
  m.cust_country_num,
  b.cust_tender_nordstrom,
  b.cust_tender_nordstrom_note,
  b.cust_tender_gift_card,
  b.cust_tender_other,
  b.cust_had_a_cancel,
  b.cust_svc_group_exp_delivery,
  b.cust_svc_group_order_pickup,
  b.cust_svc_group_selling_relation,
  b.cust_svc_group_remote_selling,
  b.cust_svc_group_alterations,
  b.cust_svc_group_in_store,
  b.cust_svc_group_restaurant,
  b.cust_service_free_exp_delivery,
  b.cust_service_next_day_pickup,
  b.cust_service_same_day_bopus,
  b.cust_service_curbside_pickup,
  b.cust_service_style_boards,
  b.cust_service_gift_wrapping,
  CAST(NULL AS INTEGER) AS cust_styling,
  b.cust_service_pop_in,
  u.cust_chan_buyer_flow_num AS cust_ns_buyerflow_num,
  v.cust_chan_buyer_flow_num AS cust_ncom_buyerflow_num,
  w.cust_chan_buyer_flow_num AS cust_rs_buyerflow_num,
  x.cust_chan_buyer_flow_num AS cust_rcom_buyerflow_num,
  y.cust_chan_buyer_flow_num AS cust_nord_buyerflow_num,
  z.cust_chan_buyer_flow_num AS cust_rack_buyerflow_num,
  aa.cust_chan_buyer_flow_num AS cust_jwn_buyerflow_num,
  b.cust_jwn_acquired_aare,
  b.cust_jwn_activated_aare,
  b.cust_jwn_retained_aare,
  b.cust_jwn_engaged_aare,
  b.shopped_fls,
  b.shopped_ncom,
  b.shopped_rack,
  b.shopped_rcom,
  b.shopped_fp,
  b.shopped_op,
  b.shopped_jwn,
  b.cust_made_a_return,
  b.cust_platform_desktop,
  b.cust_platform_mow,
  b.cust_platform_ios,
  b.cust_platform_android,
  b.cust_platform_pos,
  b.cust_service_ship2store_ncom,
  b.cust_service_ship2store_rcom,
  b.restaurant_only_flag,
  q.closest_sol_dist_bucket_num,
  p.closest_store_banner_num,
  o.nord_sol_dist_bucket_num,
  n.rack_sol_dist_bucket_num,
  b.shopped_nord_local,
  b.ncom_ret_to_nstore,
  b.ncom_ret_by_mail,
  b.ncom_ret_to_rstore,
  b.rcom_ret_to_nstore,
  b.rcom_ret_by_mail,
  b.rcom_ret_to_rstore,
  b.cust_nord_shoes_net_spend_ry,
  b.cust_nord_beauty_net_spend_ry,
  b.cust_nord_designer_apparel_net_spend_ry,
  b.cust_nord_apparel_net_spend_ry,
  b.cust_nord_accessories_net_spend_ry,
  b.cust_nord_home_net_spend_ry,
  b.cust_nord_merch_projects_net_spend_ry,
  b.cust_nord_leased_boutique_net_spend_ry,
  b.cust_nord_restaurant_net_spend_ry,
  b.cust_rack_shoes_net_spend_ry,
  b.cust_rack_beauty_net_spend_ry,
  b.cust_rack_designer_apparel_net_spend_ry,
  b.cust_rack_apparel_net_spend_ry,
  b.cust_rack_accessories_net_spend_ry,
  b.cust_rack_home_net_spend_ry,
  b.cust_rack_merch_projects_net_spend_ry,
  b.cust_rack_leased_boutique_net_spend_ry,
  b.cust_rack_restaurant_net_spend_ry,
  b.cust_nord_regprice_net_spend_ry,
  b.cust_nord_promo_net_spend_ry,
  b.cust_nord_clearance_net_spend_ry,
  b.cust_rack_regprice_net_spend_ry,
  b.cust_rack_promo_net_spend_ry,
  b.cust_rack_clearance_net_spend_ry,
  b.cust_chan_holiday,
  b.cust_chan_anniversary,
  CAST(NULL AS INTEGER) AS cust_contr_margin_decile,
  CAST(NULL AS NUMERIC) AS cust_contr_margin_amt,
   (SELECT *
   FROM current_timestamp_tmp) AS dw_sys_load_tmstp,
   (SELECT *
   FROM current_timestamp_tmp) AS dw_sys_updt_tmstp
 FROM cust_trx_everything AS a
  INNER JOIN cust_year_everything AS b ON LOWER(a.acp_id) = LOWER(b.acp_id) AND LOWER(a.reporting_year_shopped) = LOWER(b
     .reporting_year_shopped)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.reporting_year_shopped_lkp AS ry ON LOWER(COALESCE(a.reporting_year_shopped, 'Missing')) =
   LOWER(ry.reporting_year_shopped_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_jwn_trip_bucket_lkp AS c ON LOWER(COALESCE(b.cust_jwn_trip_bucket, 'Unknown')) =
   LOWER(c.cust_jwn_trip_bucket_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_age_group_lkp AS d ON LOWER(COALESCE(b.cust_age_group, 'Unknown')) = LOWER(d.cust_age_group_desc
    )
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_tenure_bucket_years_lkp AS e ON LOWER(COALESCE(b.cust_tenure_bucket_years, 'Unknown'
     )) = LOWER(e.cust_tenure_bucket_years_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_channel_lkp AS f ON LOWER(COALESCE(b.cust_acquisition_channel, 'Unknown')) = LOWER(f
    .cust_channel_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_jwn_net_spend_bucket_lkp AS g ON LOWER(COALESCE(b.cust_jwn_net_spend_bucket,
     'Unknown')) = LOWER(g.cust_jwn_net_spend_bucket_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_channel_combo_lkp AS h ON LOWER(COALESCE(b.cust_channel_combo, 'Unknown')) = LOWER(h
    .cust_channel_combo_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_loyalty_type_lkp AS i ON LOWER(COALESCE(b.cust_loyalty_type, 'Unknown')) = LOWER(i.cust_loyalty_type_desc
    )
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_channel_lkp AS j ON LOWER(COALESCE(b.cust_activation_channel, 'Unknown')) = LOWER(j.cust_channel_desc
    )
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_nms_market_lkp AS k ON LOWER(COALESCE(b.cust_nms_market, 'NON-NMS MARKET')) = LOWER(k
    .cust_nms_market_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_loyalty_level_lkp AS l ON LOWER(COALESCE(b.cust_loyalty_level, 'Unknown')) = LOWER(l
    .cust_loyalty_level_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_dma_lkp AS m ON LOWER(COALESCE(b.cust_dma, 'Unknown')) = LOWER(m.cust_dma_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.rack_sol_dist_bucket_lkp AS n ON LOWER(COALESCE(b.rack_sol_dist_bucket, 'missing')) =
   LOWER(n.rack_sol_dist_bucket_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.nord_sol_dist_bucket_lkp AS o ON LOWER(COALESCE(b.nord_sol_dist_bucket, 'missing')) =
   LOWER(o.nord_sol_dist_bucket_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.closest_store_banner_lkp AS p ON LOWER(COALESCE(b.closest_store_banner, 'missing')) =
   LOWER(p.closest_store_banner_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.closest_sol_dist_bucket_lkp AS q ON LOWER(COALESCE(b.closest_store_dist_bucket, 'missing'
     )) = LOWER(q.closest_sol_dist_bucket_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.item_delivery_method_lkp AS r ON LOWER(COALESCE(a.item_delivery_method, 'NA')) = LOWER(r.item_delivery_method_desc
    )
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.store_lkp AS s ON CAST(COALESCE(FORMAT('%11d', a.store_num), 'Unknown') AS FLOAT64) = s.store_num
     AND LOWER(s.channel_country_desc) = LOWER(a.channel_country)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.audience_engagement_cohort_lkp AS t ON LOWER(COALESCE(b.engagement_cohort, 'missing')) =
   LOWER(t.audience_engagement_cohort_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_chan_buyer_flow_lkp AS u ON LOWER(u.cust_chan_buyer_flow_desc) = LOWER(COALESCE(b.ns_buyerflow
     , 'NA'))
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_chan_buyer_flow_lkp AS v ON LOWER(v.cust_chan_buyer_flow_desc) = LOWER(COALESCE(b.ncom_buyerflow
     , 'NA'))
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_chan_buyer_flow_lkp AS w ON LOWER(w.cust_chan_buyer_flow_desc) = LOWER(COALESCE(b.rs_buyerflow
     , 'NA'))
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_chan_buyer_flow_lkp AS x ON LOWER(x.cust_chan_buyer_flow_desc) = LOWER(COALESCE(b.rcom_buyerflow
     , 'NA'))
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_chan_buyer_flow_lkp AS y ON LOWER(y.cust_chan_buyer_flow_desc) = LOWER(COALESCE(b.nord_buyerflow
     , 'NA'))
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_chan_buyer_flow_lkp AS z ON LOWER(z.cust_chan_buyer_flow_desc) = LOWER(COALESCE(b.rack_buyerflow
     , 'NA'))
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_chan_buyer_flow_lkp AS aa ON LOWER(aa.cust_chan_buyer_flow_desc) = LOWER(COALESCE(b.jwn_buyerflow
     , 'NA'))
 WHERE a.date_shopped BETWEEN (SELECT DATE_ADD(p2_start_date, INTERVAL 1 DAY)
    FROM date_range) AND (SELECT p2_end_date
    FROM date_range));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;






INSERT INTO `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.customer_sandbox_fact
(SELECT a.data_source,
  a.acp_id,
  s.banner_num AS store_banner_num,
  s.channel_num AS store_channel_num,
  s.channel_country_num AS store_channel_country,
  a.store_num,
  s.store_region_num,
  COALESCE(a.div_num, - 1) AS div_num,
  COALESCE(a.subdiv_num, - 1) AS subdiv_num,
  COALESCE(a.dept_num, - 1) AS dept_num,
  a.npg_flag,
  COALESCE(a.brand_name, 'Others') AS brand_name,
  a.date_shopped,
  a.fiscal_month_num AS month_num,
  a.fiscal_qtr_num AS quarter_num,
  a.fiscal_yr_num AS year_num,
  a.trip_id,
  a.gross_sales,
  a.return_amt,
  a.net_sales,
  a.gross_items,
  a.return_items,
  a.net_items,
  a.regprice_net_spend,
  a.promo_net_spend,
  a.clearance_net_spend,
  a.regprice_gross_spend,
  a.promo_gross_spend,
  a.clearance_gross_spend,
  a.regprice_return_amt,
  a.promo_return_amt,
  a.clearance_return_amt,
  a.regprice_net_items,
  a.promo_net_items,
  a.clearance_net_items,
  r.item_delivery_method_num,
  a.employee_flag,
  ry.reporting_year_shopped_num,
  t.audience_engagement_cohort_num,
  c.cust_jwn_trip_bucket_num,
  g.cust_jwn_net_spend_bucket_num,
  CAST(NULL AS INTEGER) AS cust_jwn_net_spend_bucket_ly_num,
  CAST(NULL AS INTEGER) AS cust_jwn_trip_bucket_ly_num,
  CAST(NULL AS INTEGER) AS cust_jwn_net_spend_bucket_fy_num,
  CAST(NULL AS INTEGER) AS cust_jwn_trip_bucket_num_fy_num,
  CAST(NULL AS INTEGER) AS audience_engagement_cohort_ly_num,
  CAST(NULL AS INTEGER) AS audience_engagement_cohort_fy_num,
  CAST(NULL AS INTEGER) AS cust_rack_trip_bucket_num,
  CAST(NULL AS INTEGER) AS cust_rack_net_spend_bucket_num,
  CAST(NULL AS INTEGER) AS cust_nord_trip_bucket_num,
  CAST(NULL AS INTEGER) AS cust_nord_net_spend_bucket_num,
  h.cust_channel_combo_num,
  i.cust_loyalty_type_num,
  l.cust_loyalty_level_num,
  b.cust_gender,
  d.cust_age_group_num,
  b.cust_employee_flag,
  e.cust_tenure_bucket_years_num,
  b.cust_acquisition_fiscal_year,
  f.cust_channel_num AS cust_acquisition_channel_num,
  j.cust_channel_num AS cust_activation_channel_num,
  k.cust_nms_market_num,
  k.cust_nms_region_num,
  m.cust_dma_num,
  m.cust_region_num,
  m.cust_country_num,
  b.cust_tender_nordstrom,
  b.cust_tender_nordstrom_note,
  b.cust_tender_gift_card,
  b.cust_tender_other,
  b.cust_had_a_cancel,
  b.cust_svc_group_exp_delivery,
  b.cust_svc_group_order_pickup,
  b.cust_svc_group_selling_relation,
  b.cust_svc_group_remote_selling,
  b.cust_svc_group_alterations,
  b.cust_svc_group_in_store,
  b.cust_svc_group_restaurant,
  b.cust_service_free_exp_delivery,
  b.cust_service_next_day_pickup,
  b.cust_service_same_day_bopus,
  b.cust_service_curbside_pickup,
  b.cust_service_style_boards,
  b.cust_service_gift_wrapping,
  CAST(NULL AS INTEGER) AS cust_styling,
  b.cust_service_pop_in,
  u.cust_chan_buyer_flow_num AS cust_ns_buyerflow_num,
  v.cust_chan_buyer_flow_num AS cust_ncom_buyerflow_num,
  w.cust_chan_buyer_flow_num AS cust_rs_buyerflow_num,
  x.cust_chan_buyer_flow_num AS cust_rcom_buyerflow_num,
  y.cust_chan_buyer_flow_num AS cust_nord_buyerflow_num,
  z.cust_chan_buyer_flow_num AS cust_rack_buyerflow_num,
  aa.cust_chan_buyer_flow_num AS cust_jwn_buyerflow_num,
  b.cust_jwn_acquired_aare,
  b.cust_jwn_activated_aare,
  b.cust_jwn_retained_aare,
  b.cust_jwn_engaged_aare,
  b.shopped_fls,
  b.shopped_ncom,
  b.shopped_rack,
  b.shopped_rcom,
  b.shopped_fp,
  b.shopped_op,
  b.shopped_jwn,
  b.cust_made_a_return,
  b.cust_platform_desktop,
  b.cust_platform_mow,
  b.cust_platform_ios,
  b.cust_platform_android,
  b.cust_platform_pos,
  b.cust_service_ship2store_ncom,
  b.cust_service_ship2store_rcom,
  b.restaurant_only_flag,
  q.closest_sol_dist_bucket_num,
  p.closest_store_banner_num,
  o.nord_sol_dist_bucket_num,
  n.rack_sol_dist_bucket_num,
  b.shopped_nord_local,
  b.ncom_ret_to_nstore,
  b.ncom_ret_by_mail,
  b.ncom_ret_to_rstore,
  b.rcom_ret_to_nstore,
  b.rcom_ret_by_mail,
  b.rcom_ret_to_rstore,
  b.cust_nord_shoes_net_spend_ry,
  b.cust_nord_beauty_net_spend_ry,
  b.cust_nord_designer_apparel_net_spend_ry,
  b.cust_nord_apparel_net_spend_ry,
  b.cust_nord_accessories_net_spend_ry,
  b.cust_nord_home_net_spend_ry,
  b.cust_nord_merch_projects_net_spend_ry,
  b.cust_nord_leased_boutique_net_spend_ry,
  b.cust_nord_restaurant_net_spend_ry,
  b.cust_rack_shoes_net_spend_ry,
  b.cust_rack_beauty_net_spend_ry,
  b.cust_rack_designer_apparel_net_spend_ry,
  b.cust_rack_apparel_net_spend_ry,
  b.cust_rack_accessories_net_spend_ry,
  b.cust_rack_home_net_spend_ry,
  b.cust_rack_merch_projects_net_spend_ry,
  b.cust_rack_leased_boutique_net_spend_ry,
  b.cust_rack_restaurant_net_spend_ry,
  b.cust_nord_regprice_net_spend_ry,
  b.cust_nord_promo_net_spend_ry,
  b.cust_nord_clearance_net_spend_ry,
  b.cust_rack_regprice_net_spend_ry,
  b.cust_rack_promo_net_spend_ry,
  b.cust_rack_clearance_net_spend_ry,
  b.cust_chan_holiday,
  b.cust_chan_anniversary,
  CAST(NULL AS INTEGER) AS cust_contr_margin_decile,
  CAST(NULL AS NUMERIC) AS cust_contr_margin_amt,
   (SELECT *
   FROM current_timestamp_tmp) AS dw_sys_load_tmstp,
   (SELECT *
   FROM current_timestamp_tmp) AS dw_sys_updt_tmstp
 FROM cust_trx_everything AS a
  INNER JOIN cust_year_everything AS b ON LOWER(a.acp_id) = LOWER(b.acp_id) AND LOWER(a.reporting_year_shopped) = LOWER(b
     .reporting_year_shopped)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.reporting_year_shopped_lkp AS ry ON LOWER(COALESCE(a.reporting_year_shopped, 'Missing')) =
   LOWER(ry.reporting_year_shopped_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_jwn_trip_bucket_lkp AS c ON LOWER(COALESCE(b.cust_jwn_trip_bucket, 'Unknown')) =
   LOWER(c.cust_jwn_trip_bucket_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_age_group_lkp AS d ON LOWER(COALESCE(b.cust_age_group, 'Unknown')) = LOWER(d.cust_age_group_desc
    )
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_tenure_bucket_years_lkp AS e ON LOWER(COALESCE(b.cust_tenure_bucket_years, 'Unknown'
     )) = LOWER(e.cust_tenure_bucket_years_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_channel_lkp AS f ON LOWER(COALESCE(b.cust_acquisition_channel, 'Unknown')) = LOWER(f
    .cust_channel_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_jwn_net_spend_bucket_lkp AS g ON LOWER(COALESCE(b.cust_jwn_net_spend_bucket,
     'Unknown')) = LOWER(g.cust_jwn_net_spend_bucket_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_channel_combo_lkp AS h ON LOWER(COALESCE(b.cust_channel_combo, 'Unknown')) = LOWER(h
    .cust_channel_combo_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_loyalty_type_lkp AS i ON LOWER(COALESCE(b.cust_loyalty_type, 'Unknown')) = LOWER(i.cust_loyalty_type_desc
    )
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_channel_lkp AS j ON LOWER(COALESCE(b.cust_activation_channel, 'Unknown')) = LOWER(j.cust_channel_desc
    )
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_nms_market_lkp AS k ON LOWER(COALESCE(b.cust_nms_market, 'NON-NMS MARKET')) = LOWER(k
    .cust_nms_market_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_loyalty_level_lkp AS l ON LOWER(COALESCE(b.cust_loyalty_level, 'Unknown')) = LOWER(l
    .cust_loyalty_level_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_dma_lkp AS m ON LOWER(COALESCE(b.cust_dma, 'Unknown')) = LOWER(m.cust_dma_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.rack_sol_dist_bucket_lkp AS n ON LOWER(COALESCE(b.rack_sol_dist_bucket, 'missing')) =
   LOWER(n.rack_sol_dist_bucket_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.nord_sol_dist_bucket_lkp AS o ON LOWER(COALESCE(b.nord_sol_dist_bucket, 'missing')) =
   LOWER(o.nord_sol_dist_bucket_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.closest_store_banner_lkp AS p ON LOWER(COALESCE(b.closest_store_banner, 'missing')) =
   LOWER(p.closest_store_banner_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.closest_sol_dist_bucket_lkp AS q ON LOWER(COALESCE(b.closest_store_dist_bucket, 'missing'
     )) = LOWER(q.closest_sol_dist_bucket_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.item_delivery_method_lkp AS r ON LOWER(COALESCE(a.item_delivery_method, 'NA')) = LOWER(r.item_delivery_method_desc
    )
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.store_lkp AS s ON CAST(COALESCE(FORMAT('%11d', a.store_num), 'Unknown') AS FLOAT64) = s.store_num
     AND LOWER(s.channel_country_desc) = LOWER(a.channel_country)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.audience_engagement_cohort_lkp AS t ON LOWER(COALESCE(b.engagement_cohort, 'missing')) =
   LOWER(t.audience_engagement_cohort_desc)
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_chan_buyer_flow_lkp AS u ON LOWER(u.cust_chan_buyer_flow_desc) = LOWER(COALESCE(b.ns_buyerflow
     , 'NA'))
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_chan_buyer_flow_lkp AS v ON LOWER(v.cust_chan_buyer_flow_desc) = LOWER(COALESCE(b.ncom_buyerflow
     , 'NA'))
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_chan_buyer_flow_lkp AS w ON LOWER(w.cust_chan_buyer_flow_desc) = LOWER(COALESCE(b.rs_buyerflow
     , 'NA'))
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_chan_buyer_flow_lkp AS x ON LOWER(x.cust_chan_buyer_flow_desc) = LOWER(COALESCE(b.rcom_buyerflow
     , 'NA'))
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_chan_buyer_flow_lkp AS y ON LOWER(y.cust_chan_buyer_flow_desc) = LOWER(COALESCE(b.nord_buyerflow
     , 'NA'))
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_chan_buyer_flow_lkp AS z ON LOWER(z.cust_chan_buyer_flow_desc) = LOWER(COALESCE(b.rack_buyerflow
     , 'NA'))
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_strategy.cust_chan_buyer_flow_lkp AS aa ON LOWER(aa.cust_chan_buyer_flow_desc) = LOWER(COALESCE(b.jwn_buyerflow
     , 'NA'))
 WHERE a.date_shopped BETWEEN (SELECT DATE_ADD(p2_end_date, INTERVAL 1 DAY)
    FROM date_range) AND (SELECT end_date
    FROM date_range));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

--COLLECT STATISTICS COLUMN(date_shopped) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(acp_id) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(store_banner_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(store_channel_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(store_channel_country) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(store_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(div_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(subdiv_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(dept_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(trip_id) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(item_delivery_method_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(employee_flag) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(reporting_year_shopped_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(audience_engagement_cohort_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_jwn_trip_bucket_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_jwn_net_spend_bucket_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_rack_trip_bucket_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_rack_net_spend_bucket_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_nord_trip_bucket_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_nord_net_spend_bucket_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_channel_combo_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_loyalty_type_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_loyalty_level_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_gender) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_age_group_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_employee_flag) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_tenure_bucket_years_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_acquisition_fiscal_year) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_acquisition_channel_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_activation_channel_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_nms_market_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_dma_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_had_a_cancel) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_svc_group_exp_delivery) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_svc_group_order_pickup) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_svc_group_selling_relation) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_svc_group_remote_selling) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_svc_group_alterations) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_svc_group_in_store) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_svc_group_restaurant) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_service_free_exp_delivery) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_service_next_day_pickup) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_service_same_day_bopus) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_service_curbside_pickup) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_service_style_boards) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_service_gift_wrapping) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_styling) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_ns_buyerflow_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_ncom_buyerflow_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_rs_buyerflow_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_rcom_buyerflow_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_nord_buyerflow_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_rack_buyerflow_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_jwn_buyerflow_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_jwn_acquired_aare) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_jwn_activated_aare) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_jwn_retained_aare) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_jwn_engaged_aare) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(shopped_fls) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(shopped_ncom) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(shopped_rack) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(shopped_rcom) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(shopped_fp) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(shopped_op) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(shopped_jwn) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_service_ship2store_ncom) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_service_ship2store_rcom) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(closest_sol_dist_bucket_num) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_event_holiday) ON {{params.str_t2_schema}}.customer_sandbox_fact;
--COLLECT STATISTICS COLUMN(cust_event_anniversary) ON {{params.str_t2_schema}}.customer_sandbox_fact;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
