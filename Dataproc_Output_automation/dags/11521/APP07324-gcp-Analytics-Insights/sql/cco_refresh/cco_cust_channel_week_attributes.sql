/*SET QUERY_BAND = 'App_ID=APP08240;
DAG_ID=cco_tables_week_grain_11521_ACE_ENG;
---Task_Name=run_cco_cust_channel_week_attributes;'*/
BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
BEGIN
SET _ERROR_CODE  =  0;
/************************************************************************************/
/************************************************************************************/
/************************************************************************************
 *
 * Build a Customer/Week/Channel-level attributes table for the CCO project.
 *
 *
 * The steps involved are:
 *
 * I) Derive Customer/Week/Channel-level attributes from cco_line_items table
 *
 * II) Join everything together:
 *  1) Customer/channel/week-level derived attributes from (I)
 *  2) Customer/week-level attributes from cco_cust_week_attributes table
 *  3) Customer/channel/week-level Buyer-Flow & AARE attributes from
 *     cco_buyer_flow_cust_channel_week table
 *  4) JWN net sales rank
 *
 ************************************************************************************/
/************************************************************************************/
/************************************************************************************/


/************************************************************************************/
/************************************************************************************
 * PART I) Derive Customer/Week/Channel-level attributes from CCO line-items table
 ************************************************************************************/
/************************************************************************************/

CREATE TEMPORARY TABLE IF NOT EXISTS cco_strategy_line_item_extract (
acp_id STRING,
date_shopped DATE,
week_idnt INTEGER,
banner STRING,
channel STRING,
store_num INTEGER,
gross_sales BIGNUMERIC(38,2),
gross_incl_gc BIGNUMERIC(38,2),
return_amt BIGNUMERIC(38,2),
net_sales BIGNUMERIC(38,2),
gross_items INTEGER,
return_items INTEGER,
net_items INTEGER,
event_anniversary INTEGER,
event_holiday INTEGER,
npg_flag INTEGER,
div_num INTEGER,
marketplace_flag INTEGER
) ;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO cco_strategy_line_item_extract
(SELECT cli.acp_id,
  cli.date_shopped,
  cal.week_idnt,
  cli.banner,
  cli.channel,
  cli.store_num,
  cli.gross_sales,
  cli.gross_incl_gc,
  cli.return_amt,
  cli.net_sales,
  cli.gross_items,
  cli.return_items,
  cli.net_items,
  cli.event_anniversary,
  cli.event_holiday,
   CASE WHEN LOWER(cli.npg_flag) = LOWER('Y') THEN 1 ELSE 0 END AS npg_flag,
  cli.div_num,
  cli.marketplace_flag AS cust_marketplace_flag
 FROM `{{params.gcp_project_id}}`.t2dl_das_strategy.cco_line_items AS cli
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS cal
   ON cli.date_shopped = cal.day_date
 WHERE cli.reporting_year_shopped IS NOT NULL);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (acp_id, week_idnt, channel) on cco_strategy_line_item_extract;
BEGIN
SET _ERROR_CODE  =  0;
/************************************************************************************
 * PART I-a) Create a temp Customer/Channel/Week-level table
 ************************************************************************************/

/*********** PART I-a-i) Insert the rows for Channels as "Channels" *****************/
CREATE TEMPORARY TABLE IF NOT EXISTS cust_chan_level_derived_nopi_chan (
acp_id STRING,
week_idnt INTEGER,
channel STRING,
banner STRING,
cust_chan_gross_sales BIGNUMERIC(38,2),
cust_chan_return_amt BIGNUMERIC(38,2),
cust_chan_net_sales BIGNUMERIC(38,2),
cust_chan_trips INTEGER,
cust_chan_gross_items INTEGER,
cust_chan_return_items INTEGER,
cust_chan_net_items INTEGER,
cust_chan_anniversary INTEGER,
cust_chan_holiday INTEGER,
cust_chan_npg INTEGER,
cust_chan_div_count INTEGER,
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
cust_jwn_transaction_restaurant_ind INTEGER
);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO cust_chan_level_derived_nopi_chan
(SELECT acp_id,
  week_idnt,
  channel,
  banner,
  SUM(gross_sales) AS cust_chan_gross_sales,
  SUM(return_amt) AS cust_chan_return_amt,
  SUM(net_sales) AS cust_chan_net_sales,
  COUNT(DISTINCT CASE
    WHEN gross_incl_gc > 0
    THEN acp_id || FORMAT('%11d', store_num) || CAST(date_shopped AS STRING)
    ELSE NULL
    END) AS cust_chan_trips,
  SUM(gross_items) AS cust_chan_gross_items,
  SUM(return_items) AS cust_chan_return_items,
  SUM(gross_items - return_items) AS cust_chan_net_items,
  MAX(event_anniversary) AS cust_chan_anniversary,
  MAX(event_holiday) AS cust_chan_holiday,
  MAX(npg_flag) AS cust_chan_npg,
  COUNT(DISTINCT div_num) AS cust_chan_div_count,
  SUM(CASE
    WHEN div_num = 351
    THEN net_sales
    ELSE 0
    END) AS net_sales_apparel,
  SUM(CASE
    WHEN div_num = 310
    THEN net_sales
    ELSE 0
    END) AS net_sales_shoes,
  SUM(CASE
    WHEN div_num = 340
    THEN net_sales
    ELSE 0
    END) AS net_sales_beauty,
  SUM(CASE
    WHEN div_num = 341
    THEN net_sales
    ELSE 0
    END) AS net_sales_designer,
  SUM(CASE
    WHEN div_num = 360
    THEN net_sales
    ELSE 0
    END) AS net_sales_accessories,
  SUM(CASE
    WHEN div_num = 365
    THEN net_sales
    ELSE 0
    END) AS net_sales_home,
  SUM(CASE
    WHEN div_num = 370
    THEN net_sales
    ELSE 0
    END) AS net_sales_merch_projects,
  SUM(CASE
    WHEN div_num = 800
    THEN net_sales
    ELSE 0
    END) AS net_sales_leased_boutiques,
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
    END) AS transaction_apparel_ind,
  SUM(CASE
    WHEN div_num = 310
    THEN 1
    ELSE 0
    END) AS transaction_shoes_ind,
  SUM(CASE
    WHEN div_num = 340
    THEN 1
    ELSE 0
    END) AS transaction_beauty_ind,
  SUM(CASE
    WHEN div_num = 341
    THEN 1
    ELSE 0
    END) AS transaction_designer_ind,
  SUM(CASE
    WHEN div_num = 360
    THEN 1
    ELSE 0
    END) AS transaction_accessories_ind,
  SUM(CASE
    WHEN div_num = 365
    THEN 1
    ELSE 0
    END) AS transaction_home_ind,
  SUM(CASE
    WHEN div_num = 370
    THEN 1
    ELSE 0
    END) AS transaction_merch_projects_ind,
  SUM(CASE
    WHEN div_num = 800
    THEN 1
    ELSE 0
    END) AS transaction_leased_boutiques_ind,
  SUM(CASE
    WHEN div_num = 900
    THEN 1
    ELSE 0
    END) AS transaction_other_non_merch_ind,
  SUM(CASE
    WHEN div_num = 70
    THEN 1
    ELSE 0
    END) AS transaction_restaurant_ind
 FROM cco_strategy_line_item_extract
 GROUP BY acp_id,
  week_idnt,
  channel,
  banner);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cust_chan_level_derived_nopi_banner (
acp_id STRING,
week_idnt INTEGER,
channel STRING,
cust_chan_gross_sales BIGNUMERIC(38,2),
cust_chan_return_amt BIGNUMERIC(38,2),
cust_chan_net_sales BIGNUMERIC(38,2),
cust_chan_trips INTEGER,
cust_chan_gross_items INTEGER,
cust_chan_return_items INTEGER,
cust_chan_net_items INTEGER,
cust_chan_anniversary INTEGER,
cust_chan_holiday INTEGER,
cust_chan_npg INTEGER,
cust_chan_div_count INTEGER,
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
cust_jwn_transaction_restaurant_ind INTEGER
);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO cust_chan_level_derived_nopi_banner
(SELECT acp_id,
  week_idnt,
   CASE
   WHEN LOWER(banner) = LOWER('NORDSTROM')
   THEN '5) Nordstrom Banner'
   WHEN LOWER(banner) = LOWER('RACK')
   THEN '6) Rack Banner'
   ELSE NULL
   END AS channel,
  SUM(cust_chan_gross_sales) AS cust_chan_gross_sales,
  SUM(cust_chan_return_amt) AS cust_chan_return_amt,
  SUM(cust_chan_net_sales) AS cust_chan_net_sales,
  SUM(cust_chan_trips) AS cust_chan_trips,
  SUM(cust_chan_gross_items) AS cust_chan_gross_items,
  SUM(cust_chan_return_items) AS cust_chan_return_items,
  SUM(cust_chan_net_items) AS cust_chan_net_items,
  MAX(cust_chan_anniversary) AS cust_chan_anniversary,
  MAX(cust_chan_holiday) AS cust_chan_holiday,
  MAX(cust_chan_npg) AS cust_chan_npg,
  SUM(cust_chan_div_count) AS cust_chan_div_count,
  SUM(cust_jwn_net_sales_apparel) AS net_sales_apparel,
  SUM(cust_jwn_net_sales_shoes) AS net_sales_shoes,
  SUM(cust_jwn_net_sales_beauty) AS net_sales_beauty,
  SUM(cust_jwn_net_sales_designer) AS net_sales_designer,
  SUM(cust_jwn_net_sales_accessories) AS net_sales_accessories,
  SUM(cust_jwn_net_sales_home) AS net_sales_home,
  SUM(cust_jwn_net_sales_merch_projects) AS net_sales_merch_projects,
  SUM(cust_jwn_net_sales_leased_boutiques) AS net_sales_leased_boutiques,
  SUM(cust_jwn_net_sales_other_non_merch) AS net_sales_other_non_merch,
  SUM(cust_jwn_net_sales_restaurant) AS net_sales_restaurant,
  MAX(cust_jwn_transaction_apparel_ind) AS transaction_apparel_ind,
  MAX(cust_jwn_transaction_shoes_ind) AS transaction_shoes_ind,
  MAX(cust_jwn_transaction_beauty_ind) AS transaction_beauty_ind,
  MAX(cust_jwn_transaction_designer_ind) AS transaction_designer_ind,
  MAX(cust_jwn_transaction_accessories_ind) AS transaction_accessories_ind,
  MAX(cust_jwn_transaction_home_ind) AS transaction_home_ind,
  MAX(cust_jwn_transaction_merch_projects_ind) AS transaction_merch_projects_ind,
  MAX(cust_jwn_transaction_leased_boutiques_ind) AS transaction_leased_boutiques_ind,
  MAX(cust_jwn_transaction_other_non_merch_ind) AS transaction_other_non_merch_ind,
  MAX(cust_jwn_transaction_restaurant_ind) AS transaction_restaurant_ind
 FROM cust_chan_level_derived_nopi_chan
 GROUP BY acp_id,
  week_idnt,
  channel);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cust_chan_level_derived_nopi_jwn (
acp_id STRING,
week_idnt INTEGER,
channel STRING,
cust_chan_gross_sales BIGNUMERIC(38,2),
cust_chan_return_amt BIGNUMERIC(38,2),
cust_chan_net_sales BIGNUMERIC(38,2),
cust_chan_trips INTEGER,
cust_chan_gross_items INTEGER,
cust_chan_return_items INTEGER,
cust_chan_net_items INTEGER,
cust_chan_anniversary INTEGER,
cust_chan_holiday INTEGER,
cust_chan_npg INTEGER,
cust_chan_div_count INTEGER,
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
cust_jwn_transaction_restaurant_ind INTEGER
);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO cust_chan_level_derived_nopi_jwn
(SELECT acp_id,
  week_idnt,
  '7) JWN' AS channel,
  SUM(cust_chan_gross_sales) AS cust_chan_gross_sales,
  SUM(cust_chan_return_amt) AS cust_chan_return_amt,
  SUM(cust_chan_net_sales) AS cust_chan_net_sales,
  SUM(cust_chan_trips) AS cust_chan_trips,
  SUM(cust_chan_gross_items) AS cust_chan_gross_items,
  SUM(cust_chan_return_items) AS cust_chan_return_items,
  SUM(cust_chan_net_items) AS cust_chan_net_items,
  MAX(cust_chan_anniversary) AS cust_chan_anniversary,
  MAX(cust_chan_holiday) AS cust_chan_holiday,
  MAX(cust_chan_npg) AS cust_chan_npg,
  SUM(cust_chan_div_count) AS cust_chan_div_count,
  SUM(cust_jwn_net_sales_apparel) AS net_sales_apparel,
  SUM(cust_jwn_net_sales_shoes) AS net_sales_shoes,
  SUM(cust_jwn_net_sales_beauty) AS net_sales_beauty,
  SUM(cust_jwn_net_sales_designer) AS net_sales_designer,
  SUM(cust_jwn_net_sales_accessories) AS net_sales_accessories,
  SUM(cust_jwn_net_sales_home) AS net_sales_home,
  SUM(cust_jwn_net_sales_merch_projects) AS net_sales_merch_projects,
  SUM(cust_jwn_net_sales_leased_boutiques) AS net_sales_leased_boutiques,
  SUM(cust_jwn_net_sales_other_non_merch) AS net_sales_other_non_merch,
  SUM(cust_jwn_net_sales_restaurant) AS net_sales_restaurant,
  MAX(cust_jwn_transaction_apparel_ind) AS transaction_apparel_ind,
  MAX(cust_jwn_transaction_shoes_ind) AS transaction_shoes_ind,
  MAX(cust_jwn_transaction_beauty_ind) AS transaction_beauty_ind,
  MAX(cust_jwn_transaction_designer_ind) AS transaction_designer_ind,
  MAX(cust_jwn_transaction_accessories_ind) AS transaction_accessories_ind,
  MAX(cust_jwn_transaction_home_ind) AS transaction_home_ind,
  MAX(cust_jwn_transaction_merch_projects_ind) AS transaction_merch_projects_ind,
  MAX(cust_jwn_transaction_leased_boutiques_ind) AS transaction_leased_boutiques_ind,
  MAX(cust_jwn_transaction_other_non_merch_ind) AS transaction_other_non_merch_ind,
  MAX(cust_jwn_transaction_restaurant_ind) AS transaction_restaurant_ind
 FROM cust_chan_level_derived_nopi_banner
 GROUP BY acp_id,
  week_idnt,
  channel);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cust_chan_level_derived_nopi (
acp_id STRING,
week_idnt INTEGER,
channel STRING,
cust_chan_gross_sales BIGNUMERIC(38,2),
cust_chan_return_amt BIGNUMERIC(38,2),
cust_chan_net_sales BIGNUMERIC(38,2),
cust_chan_trips INTEGER,
cust_chan_gross_items INTEGER,
cust_chan_return_items INTEGER,
cust_chan_net_items INTEGER,
cust_chan_anniversary INTEGER,
cust_chan_holiday INTEGER,
cust_chan_npg INTEGER,
cust_chan_div_count INTEGER,
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
cust_jwn_transaction_restaurant_ind INTEGER
);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO cust_chan_level_derived_nopi
(SELECT *
 FROM (SELECT acp_id,
     week_idnt,
     channel,
     cust_chan_gross_sales,
     cust_chan_return_amt,
     cust_chan_net_sales,
     cust_chan_trips,
     cust_chan_gross_items,
     cust_chan_return_items,
     cust_chan_net_items,
     cust_chan_anniversary,
     cust_chan_holiday,
     cust_chan_npg,
     cust_chan_div_count,
     cust_jwn_net_sales_apparel,
     cust_jwn_net_sales_shoes,
     cust_jwn_net_sales_beauty,
     cust_jwn_net_sales_designer,
     cust_jwn_net_sales_accessories,
     cust_jwn_net_sales_home,
     cust_jwn_net_sales_merch_projects,
     cust_jwn_net_sales_leased_boutiques,
     cust_jwn_net_sales_other_non_merch,
     cust_jwn_net_sales_restaurant,
     cust_jwn_transaction_apparel_ind,
     cust_jwn_transaction_shoes_ind,
     cust_jwn_transaction_beauty_ind,
     cust_jwn_transaction_designer_ind,
     cust_jwn_transaction_accessories_ind,
     cust_jwn_transaction_home_ind,
     cust_jwn_transaction_merch_projects_ind,
     cust_jwn_transaction_leased_boutiques_ind,
     cust_jwn_transaction_other_non_merch_ind,
     cust_jwn_transaction_restaurant_ind
    FROM cust_chan_level_derived_nopi_chan
    UNION ALL
    SELECT *
    FROM cust_chan_level_derived_nopi_banner
    UNION ALL
    SELECT *
    FROM cust_chan_level_derived_nopi_jwn) AS t0);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (week_idnt), column (acp_id, channel), column (acp_id, week_idnt, channel) on cust_chan_level_derived_nopi;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cust_wk_ly_ny
AS
SELECT week_idnt AS curr_week_idnt,
 LAG(week_idnt, 52) OVER (ORDER BY week_idnt) AS prev_52wk_week_idnt,
 LEAD(week_idnt, 52) OVER (ORDER BY week_idnt) AS next_52wk_week_idnt
FROM (SELECT DISTINCT week_idnt
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
  WHERE day_date BETWEEN DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1830 DAY) AND (DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL 730 DAY))) AS
 wks;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (curr_week_idnt), column (prev_52wk_week_idnt), column (next_52wk_week_idnt) on cust_wk_ly_ny;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cust_chan_level_derived (
acp_id STRING,
week_idnt INTEGER,
channel STRING,
cust_chan_gross_sales BIGNUMERIC(38,2),
cust_chan_return_amt BIGNUMERIC(38,2),
cust_chan_net_sales BIGNUMERIC(38,2),
cust_chan_trips INTEGER,
cust_chan_gross_items INTEGER,
cust_chan_return_items INTEGER,
cust_chan_net_items INTEGER,
cust_chan_anniversary INTEGER,
cust_chan_holiday INTEGER,
cust_chan_npg INTEGER,
cust_chan_div_count INTEGER,
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
cust_chan_net_sales_wkly BIGNUMERIC(38,2),
cust_chan_net_sales_wkny BIGNUMERIC(38,2),
cust_chan_anniversary_wkly INTEGER,
cust_chan_holiday_wkly INTEGER
) ;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO cust_chan_level_derived
(SELECT ty.acp_id,
  ty.week_idnt,
  ty.channel,
  ty.cust_chan_gross_sales,
  ty.cust_chan_return_amt,
  ty.cust_chan_net_sales,
  ty.cust_chan_trips,
  ty.cust_chan_gross_items,
  ty.cust_chan_return_items,
  ty.cust_chan_net_items,
  ty.cust_chan_anniversary,
  ty.cust_chan_holiday,
  ty.cust_chan_npg,
  ty.cust_chan_div_count,
  ty.cust_jwn_net_sales_apparel,
  ty.cust_jwn_net_sales_shoes,
  ty.cust_jwn_net_sales_beauty,
  ty.cust_jwn_net_sales_designer,
  ty.cust_jwn_net_sales_accessories,
  ty.cust_jwn_net_sales_home,
  ty.cust_jwn_net_sales_merch_projects,
  ty.cust_jwn_net_sales_leased_boutiques,
  ty.cust_jwn_net_sales_other_non_merch,
  ty.cust_jwn_net_sales_restaurant,
  ty.cust_jwn_transaction_apparel_ind,
  ty.cust_jwn_transaction_shoes_ind,
  ty.cust_jwn_transaction_beauty_ind,
  ty.cust_jwn_transaction_designer_ind,
  ty.cust_jwn_transaction_accessories_ind,
  ty.cust_jwn_transaction_home_ind,
  ty.cust_jwn_transaction_merch_projects_ind,
  ty.cust_jwn_transaction_leased_boutiques_ind,
  ty.cust_jwn_transaction_other_non_merch_ind,
  ty.cust_jwn_transaction_restaurant_ind,
  ly.cust_chan_net_sales AS cust_chan_net_sales_wkly,
  ny.cust_chan_net_sales AS cust_chan_net_sales_wkny,
  ly.cust_chan_anniversary AS cust_chan_anniversary_wkly,
  ly.cust_chan_holiday AS cust_chan_holiday_wkly
 FROM cust_chan_level_derived_nopi AS ty
  INNER JOIN cust_wk_ly_ny AS lynywks ON ty.week_idnt = lynywks.curr_week_idnt
  LEFT JOIN cust_chan_level_derived_nopi AS ly ON LOWER(ty.acp_id) = LOWER(ly.acp_id) AND LOWER(ty.channel) = LOWER(ly.channel
      ) AND lynywks.prev_52wk_week_idnt = ly.week_idnt
  LEFT JOIN cust_chan_level_derived_nopi AS ny ON LOWER(ty.acp_id) = LOWER(ny.acp_id) AND LOWER(ty.channel) = LOWER(ny.channel
      ) AND lynywks.next_52wk_week_idnt = ny.week_idnt);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (acp_id, week_idnt), column (acp_id, week_idnt, channel) on cust_chan_level_derived;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cust_jwn_rank
AS
SELECT acp_id,
 week_idnt,
 cust_chan_net_sales,
 ROW_NUMBER() OVER (PARTITION BY week_idnt ORDER BY cust_chan_net_sales DESC) AS rank_record
FROM cust_chan_level_derived
WHERE LOWER(channel) = LOWER('7) JWN')
QUALIFY (ROW_NUMBER() OVER (PARTITION BY week_idnt ORDER BY cust_chan_net_sales DESC)) <= 500000;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (acp_id, week_idnt) on cust_jwn_rank;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cco_cust_channel_week_attributes_stg (
acp_id STRING,
week_idnt INTEGER,
channel STRING,
cust_chan_gross_sales BIGNUMERIC(38,2),
cust_chan_return_amt BIGNUMERIC(38,2),
cust_chan_net_sales BIGNUMERIC(38,2),
cust_chan_trips INTEGER,
cust_chan_gross_items INTEGER,
cust_chan_return_items INTEGER,
cust_chan_net_items INTEGER,
cust_chan_anniversary BYTEINT,
cust_chan_holiday BYTEINT,
cust_chan_npg BYTEINT,
cust_chan_singledivision BYTEINT,
cust_chan_multidivision BYTEINT,
cust_chan_net_sales_ly BIGNUMERIC(38,2),
cust_chan_net_sales_ny BIGNUMERIC(38,2),
cust_chan_anniversary_ly INTEGER,
cust_chan_holiday_ly INTEGER,
cust_chan_buyer_flow STRING,
cust_chan_acquired_aare BYTEINT,
cust_chan_activated_aare BYTEINT,
cust_chan_retained_aare BYTEINT,
cust_chan_engaged_aare BYTEINT,
cust_gender STRING,
cust_age SMALLINT,
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
cust_acquired_this_year BYTEINT,
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
cust_jwn_gross_items INTEGER,
cust_jwn_return_items INTEGER,
cust_jwn_net_items INTEGER,
cust_tender_nordstrom BYTEINT,
cust_tender_nordstrom_note BYTEINT,
cust_tender_3rd_party_credit BYTEINT,
cust_tender_debit_card BYTEINT,
cust_tender_gift_card BYTEINT,
cust_tender_cash BYTEINT,
cust_tender_paypal BYTEINT,
cust_tender_check BYTEINT,
cust_svc_group_exp_delivery BYTEINT,
cust_svc_group_order_pickup BYTEINT,
cust_svc_group_selling_relation BYTEINT,
cust_svc_group_remote_selling BYTEINT,
cust_svc_group_alterations BYTEINT,
cust_svc_group_in_store BYTEINT,
cust_svc_group_restaurant BYTEINT,
cust_service_free_exp_delivery BYTEINT,
cust_service_next_day_pickup BYTEINT,
cust_service_same_day_bopus BYTEINT,
cust_service_curbside_pickup BYTEINT,
cust_service_style_boards BYTEINT,
cust_service_gift_wrapping BYTEINT,
cust_service_pop_in BYTEINT,
cust_marketplace_flag BYTEINT,
cust_platform_desktop BYTEINT,
cust_platform_mow BYTEINT,
cust_platform_ios BYTEINT,
cust_platform_android BYTEINT,
cust_platform_pos BYTEINT,
cust_anchor_brand BYTEINT,
cust_strategic_brand BYTEINT,
cust_store_customer BYTEINT,
cust_digital_customer BYTEINT,
cust_clv_jwn NUMERIC(14,4),
cust_clv_fp NUMERIC(14,4),
cust_clv_op NUMERIC(14,4),
cust_jwn_top500k BYTEINT,
cust_jwn_top1k BYTEINT
);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO cco_cust_channel_week_attributes_stg
(SELECT a.acp_id,
  a.week_idnt,
  a.channel,
  a.cust_chan_gross_sales,
  a.cust_chan_return_amt,
  a.cust_chan_net_sales,
  a.cust_chan_trips,
  a.cust_chan_gross_items,
  a.cust_chan_return_items,
  a.cust_chan_net_items,
  a.cust_chan_anniversary,
  a.cust_chan_holiday,
  a.cust_chan_npg,
   CASE
   WHEN a.cust_chan_div_count = 1
   THEN 1
   ELSE 0
   END AS cust_chan_singledivision,
   CASE
   WHEN a.cust_chan_div_count > 1
   THEN 1
   ELSE 0
   END AS cust_chan_multidivision,
  a.cust_chan_net_sales_wkly,
  a.cust_chan_net_sales_wkny,
  a.cust_chan_anniversary_wkly,
  a.cust_chan_holiday_wkly,
  b.buyer_flow AS cust_chan_buyer_flow,
  b.aare_acquired AS cust_chan_acquired_aare,
  b.aare_activated AS cust_chan_activated_aare,
  b.aare_retained AS cust_chan_retained_aare,
  b.aare_engaged AS cust_chan_engaged_aare,
  c.cust_gender,
  c.cust_age,
  c.cust_lifestage,
  c.cust_age_group,
  c.cust_nms_market,
  c.cust_dma,
  c.cust_country,
  c.cust_dma_rank,
  c.cust_loyalty_type,
  c.cust_loyalty_level,
  c.cust_loy_member_enroll_dt,
  c.cust_loy_cardmember_enroll_dt,
  b.aare_acquired AS cust_acquired_this_year,
  c.cust_acquisition_date,
  c.cust_acquisition_fiscal_year,
  c.cust_acquisition_channel,
  c.cust_acquisition_banner,
  c.cust_acquisition_brand,
  c.cust_tenure_bucket_months,
  c.cust_tenure_bucket_years,
  c.cust_activation_date,
  c.cust_activation_channel,
  c.cust_activation_banner,
  c.cust_engagement_cohort,
  c.cust_channel_count,
  c.cust_channel_combo,
  c.cust_banner_count,
  c.cust_banner_combo,
  c.cust_employee_flag,
  c.cust_jwn_trip_bucket,
  c.cust_jwn_net_spend_bucket,
  c.cust_jwn_gross_sales,
  c.cust_jwn_return_amt,
  c.cust_jwn_net_sales,
  a.cust_jwn_net_sales_apparel,
  a.cust_jwn_net_sales_shoes,
  a.cust_jwn_net_sales_beauty,
  a.cust_jwn_net_sales_designer,
  a.cust_jwn_net_sales_accessories,
  a.cust_jwn_net_sales_home,
  a.cust_jwn_net_sales_merch_projects,
  a.cust_jwn_net_sales_leased_boutiques,
  a.cust_jwn_net_sales_other_non_merch,
  a.cust_jwn_net_sales_restaurant,
   CASE
   WHEN a.cust_jwn_transaction_apparel_ind > 0
   THEN 1
   ELSE 0
   END AS cust_jwn_transaction_apparel_ind,
   CASE
   WHEN a.cust_jwn_transaction_shoes_ind > 0
   THEN 1
   ELSE 0
   END AS cust_jwn_transaction_shoes_ind,
   CASE
   WHEN a.cust_jwn_transaction_beauty_ind > 0
   THEN 1
   ELSE 0
   END AS cust_jwn_transaction_beauty_ind,
   CASE
   WHEN a.cust_jwn_transaction_designer_ind > 0
   THEN 1
   ELSE 0
   END AS cust_jwn_transaction_designer_ind,
   CASE
   WHEN a.cust_jwn_transaction_accessories_ind > 0
   THEN 1
   ELSE 0
   END AS cust_jwn_transaction_accessories_ind,
   CASE
   WHEN a.cust_jwn_transaction_home_ind > 0
   THEN 1
   ELSE 0
   END AS cust_jwn_transaction_home_ind,
   CASE
   WHEN a.cust_jwn_transaction_merch_projects_ind > 0
   THEN 1
   ELSE 0
   END AS cust_jwn_transaction_merch_projects_ind,
   CASE
   WHEN a.cust_jwn_transaction_leased_boutiques_ind > 0
   THEN 1
   ELSE 0
   END AS cust_jwn_transaction_leased_boutiques_ind,
   CASE
   WHEN a.cust_jwn_transaction_other_non_merch_ind > 0
   THEN 1
   ELSE 0
   END AS cust_jwn_transaction_other_non_merch_ind,
   CASE
   WHEN a.cust_jwn_transaction_restaurant_ind > 0
   THEN 1
   ELSE 0
   END AS cust_jwn_transaction_restaurant_ind,
  c.cust_jwn_trips,
  c.cust_jwn_gross_items,
  c.cust_jwn_return_items,
  c.cust_jwn_net_items,
  c.cust_tender_nordstrom,
  c.cust_tender_nordstrom_note,
  c.cust_tender_3rd_party_credit,
  c.cust_tender_debit_card,
  c.cust_tender_gift_card,
  c.cust_tender_cash,
  c.cust_tender_paypal,
  c.cust_tender_check,
  c.cust_svc_group_exp_delivery,
  c.cust_svc_group_order_pickup,
  c.cust_svc_group_selling_relation,
  c.cust_svc_group_remote_selling,
  c.cust_svc_group_alterations,
  c.cust_svc_group_in_store,
  c.cust_svc_group_restaurant,
  c.cust_service_free_exp_delivery,
  c.cust_service_next_day_pickup,
  c.cust_service_same_day_bopus,
  c.cust_service_curbside_pickup,
  c.cust_service_style_boards,
  c.cust_service_gift_wrapping,
  c.cust_service_pop_in,
  c.cust_marketplace_flag,
  c.cust_platform_desktop,
  c.cust_platform_mow,
  c.cust_platform_ios,
  c.cust_platform_android,
  c.cust_platform_pos,
  c.cust_anchor_brand,
  c.cust_strategic_brand,
  c.cust_store_customer,
  c.cust_digital_customer,
  c.cust_clv_jwn,
  c.cust_clv_fp,
  c.cust_clv_op,
   CASE
   WHEN d.rank_record <= 500000
   THEN 1
   ELSE 0
   END AS cust_jwn_top500k,
   CASE
   WHEN d.rank_record <= 1000
   THEN 1
   ELSE 0
   END AS cust_jwn_top1k
 FROM cust_chan_level_derived AS a
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_buyer_flow_cust_channel_week AS b ON LOWER(a.acp_id) = LOWER(b.acp_id) AND a.week_idnt
      = b.week_idnt AND LOWER(a.channel) = LOWER(b.channel)
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_cust_week_attributes AS c ON LOWER(a.acp_id) = LOWER(c.acp_id) AND a.week_idnt = c.week_idnt
    
  LEFT JOIN cust_jwn_rank AS d ON LOWER(a.acp_id) = LOWER(d.acp_id) AND a.week_idnt = d.week_idnt);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_cust_channel_week_attributes;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_cust_channel_week_attributes
(SELECT stg.*
     ,   CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM cco_cust_channel_week_attributes_stg AS stg);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
