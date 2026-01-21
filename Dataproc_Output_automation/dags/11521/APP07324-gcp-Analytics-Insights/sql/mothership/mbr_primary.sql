BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP08717;
DAG_ID=mothership_mbr_primary_11521_ACE_ENG;
---     Task_Name=mbr_primary;'*/
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS wk_53_realign
AS
SELECT fiscal_year_num + 1 AS fiscal_year_num_ly,
 MAX(fiscal_week_num) AS max_wk_num_ly
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS ty
WHERE fiscal_year_num BETWEEN 2021 AND (SELECT fiscal_year_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_date = CURRENT_DATE('PST8PDT'))
GROUP BY fiscal_year_num_ly;
--EXCEPTION WHEN ERROR THEN
--SET _ERROR_CODE  =  1;
--SET _ERROR_MESSAGE  =  @@error.message;
--END;
--BEGIN
--SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS fm
AS
SELECT DISTINCT ty.fiscal_year_num,
 ty.month_idnt AS month_num,
 ty.month_abrv AS month_desc,
 ty.week_idnt AS week_num,
 ly.max_wk_num_ly,
  CASE
  WHEN ly.max_wk_num_ly = 52 AND ty.fiscal_week_num < 53
  THEN ty.week_idnt - 100
  WHEN ly.max_wk_num_ly = 52 AND ty.fiscal_week_num = 53
  THEN ty.week_idnt - 52
  WHEN ly.max_wk_num_ly = 53
  THEN ty.week_idnt - 99
  ELSE NULL
  END AS ly_week_num,
  ty.month_idnt - 100 AS ly_month_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS ty
 INNER JOIN wk_53_realign AS ly ON ty.fiscal_year_num = ly.fiscal_year_num_ly
WHERE ty.month_idnt BETWEEN 202101 AND CASE
   WHEN (SELECT fiscal_month_num
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
     WHERE day_date = CURRENT_DATE('PST8PDT')) = 1
   THEN (SELECT month_idnt - 89
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
    WHERE day_date = CURRENT_DATE('PST8PDT'))
   WHEN (SELECT fiscal_month_num
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
     WHERE day_date = CURRENT_DATE('PST8PDT')) > 1
   THEN (SELECT month_idnt - 1
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
    WHERE day_date = CURRENT_DATE('PST8PDT'))
   ELSE NULL
   END;
--EXCEPTION WHEN ERROR THEN
--SET _ERROR_CODE  =  1;
--SET _ERROR_MESSAGE  =  @@error.message;
--END;
--BEGIN
--SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS lw_by_mth
AS
SELECT month_num,
 month_desc,
 MAX(week_num) AS last_week_of_mnth
FROM fm
GROUP BY month_num,
 month_desc;
--EXCEPTION WHEN ERROR THEN
--SET _ERROR_CODE  =  1;
--SET _ERROR_MESSAGE  =  @@error.message;
--END;
--BEGIN
--SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS year_cutoff
AS
SELECT DISTINCT fiscal_year_num AS year_num,
 month_idnt AS month_num,
  CASE
  WHEN fiscal_month_num = 1 AND fiscal_year_num = (SELECT MAX(fiscal_year_num)
     FROM fm)
  THEN month_idnt
  WHEN fiscal_month_num = 1 AND fiscal_year_num < (SELECT MAX(fiscal_year_num)
     FROM fm)
  THEN (SELECT MAX(fiscal_year_num) * 100 + 1
   FROM fm)
  WHEN fiscal_month_num > 1 AND fiscal_year_num < (SELECT MAX(fiscal_year_num)
     FROM fm)
  THEN (SELECT MAX(fiscal_year_num) * 100 + 1
   FROM fm)
  ELSE month_idnt
  END AS cutoff
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE month_idnt BETWEEN 202101 AND (SELECT MAX(month_num)
   FROM fm);
--EXCEPTION WHEN ERROR THEN
--SET _ERROR_CODE  =  1;
--SET _ERROR_MESSAGE  =  @@error.message;
--END;
--BEGIN
--SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS bopus
AS
SELECT w.week_num,
 w.month_num,
  CASE
  WHEN LOWER(fsdf.business_unit_desc_bopus_in_store_demand) = LOWER('OFFPRICE ONLINE')
  THEN 'R.COM'
  ELSE fsdf.business_unit_desc_bopus_in_store_demand
  END AS bu,
 SUM(fsdf.gross_merch_sales_amt) AS gross_sales_excl_bopus,
 SUM(fsdf.demand_canceled_amt) AS canceled_demand_excl_bopus
FROM `{{params.gcp_project_id}}`.t2dl_das_mothership.finance_sales_demand_fact AS fsdf
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal AS w ON fsdf.tran_date = w.day_date
WHERE w.month_num IN (SELECT DISTINCT month_num
   FROM fm)
 AND LOWER(fsdf.customer_journey) <> LOWER('BOPUS')
 AND LOWER(fsdf.business_unit_desc_bopus_in_store_demand) IN (LOWER('N.COM'), LOWER('OFFPRICE ONLINE'))
GROUP BY w.week_num,
 w.month_num,
 bu;
--EXCEPTION WHEN ERROR THEN
--SET _ERROR_CODE  =  1;
--SET _ERROR_MESSAGE  =  @@error.message;
--END;
--BEGIN
--SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS sales
AS
SELECT dt.week_num,
 dt.month_num,
 SUBSTR(SUBSTR(CAST(dt.month_num AS STRING), 1, 6), 0, 4) AS fy,
  CASE
  WHEN LOWER(sdotf.business_unit_desc) = LOWER('OFFPRICE ONLINE')
  THEN 'R.COM'
  ELSE sdotf.business_unit_desc
  END AS business_unit_desc,
 SUM(sdotf.demand_amt_excl_bopus + sdotf.bopus_attr_digital_amt) AS demand,
 SUM(sdotf.demand_amt_excl_bopus) AS demand_excl_bopus,
 SUM(sdotf.demand_units_excl_bopus + sdotf.bopus_attr_digital_units) AS units,
 SUM(sdotf.orders) AS orders,
 SUM(sdotf.visitors) AS udv,
 SUM(sdotf.ordering_visitors) AS ordering_udv,
 SUM(sdotf.gross_merch_sales_amt) AS gross_merch_sales_amt,
 SUM(sdotf.merch_returns_amt) AS merch_returns_amt,
 SUM(sdotf.net_merch_sales_amt) AS net_merch_sales_amt
FROM `{{params.gcp_project_id}}`.t2dl_das_mothership.sales_demand_orders_traffic_fact AS sdotf
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal AS dt ON sdotf.tran_date = dt.day_date
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS sd ON sdotf.store_num = sd.store_num
WHERE LOWER(sd.business_unit_desc) = LOWER(sdotf.business_unit_desc)
 AND LOWER(sdotf.business_unit_desc) IN (LOWER('N.COM'), LOWER('OFFPRICE ONLINE'))
 AND (dt.month_num IN (SELECT DISTINCT month_num
     FROM fm) OR dt.month_num IN (SELECT DISTINCT month_num - 100
     FROM fm))
 AND sdotf.store_num NOT IN (141, 173)
GROUP BY dt.week_num,
 dt.month_num,
 fy,
 business_unit_desc;
--EXCEPTION WHEN ERROR THEN
--SET _ERROR_CODE  =  1;
--SET _ERROR_MESSAGE  =  @@error.message;
--END;
--BEGIN
--SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS combo
AS
SELECT sales.week_num,
 sales.month_num,
 SUBSTR(SUBSTR(CAST(sales.month_num AS STRING), 1, 6), 0, 4) AS fy,
 sales.business_unit_desc AS source_site,
 sales.demand,
 sales.demand_excl_bopus,
 sales.units,
 bopus.canceled_demand_excl_bopus,
 sales.orders,
 sales.udv,
 sales.ordering_udv,
 sales.gross_merch_sales_amt,
 bopus.gross_sales_excl_bopus,
 sales.merch_returns_amt,
 sales.net_merch_sales_amt
FROM sales
 LEFT JOIN bopus ON sales.week_num = bopus.week_num AND LOWER(bopus.bu) = LOWER(sales.business_unit_desc);
--EXCEPTION WHEN ERROR THEN
--SET _ERROR_CODE  =  1;
--SET _ERROR_MESSAGE  =  @@error.message;
--END;
--BEGIN
--SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS ly
AS
SELECT fm.week_num,
 fm.month_num,
 combo.source_site,
 combo.demand AS demand_ly,
 combo.demand_excl_bopus AS demand_excl_bopus_ly,
 combo.orders AS orders_ly,
 combo.udv AS udv_ly,
 combo.ordering_udv AS ordering_udv_ly,
 combo.net_merch_sales_amt AS net_merch_sales_amt_ly,
 combo.units AS units_ly
FROM fm
 INNER JOIN combo ON fm.ly_week_num = combo.week_num
WHERE combo.month_num IN (SELECT month_num
   FROM fm)
 OR combo.month_num IN (SELECT ly_month_num
   FROM fm);
--EXCEPTION WHEN ERROR THEN
--SET _ERROR_CODE  =  1;
--SET _ERROR_MESSAGE  =  @@error.message;
--END;
--BEGIN
--SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS grow
AS
SELECT ty.month_num,
 ty.source_site AS box,
 SUM(ty.udv) AS udv,
 SUM(ly.udv_ly) AS udv_ly,
 SUM(ty.ordering_udv) AS ordering_udv,
 SUM(ly.ordering_udv_ly) AS ordering_udv_ly,
 SUM(ty.canceled_demand_excl_bopus) AS canceled_demand_excl_bopus,
 SUM(ty.units) AS units,
 SUM(ly.units_ly) AS units_ly,
 SUM(ty.demand_excl_bopus) AS demand_excl_bopus,
 SUM(ly.demand_excl_bopus_ly) AS demand_excl_bopus_ly,
 SUM(ty.demand) AS demand,
 SUM(ly.demand_ly) AS demand_ly,
 SUM(ty.gross_sales_excl_bopus) AS gross_sales_excl_bopus,
 SUM(ty.gross_merch_sales_amt) AS gross_merch_sales_amt,
 SUM(ty.merch_returns_amt) AS merch_returns_amt,
 SUM(ty.net_merch_sales_amt) AS net_merch_sales_amt,
 SUM(ly.net_merch_sales_amt_ly) AS net_merch_sales_amt_ly,
 SUM(ty.orders) AS orders,
 SUM(ly.orders_ly) AS orders_ly
FROM combo AS ty
 LEFT JOIN ly ON ty.week_num = ly.week_num AND LOWER(ty.source_site) = LOWER(ly.source_site)
WHERE ty.month_num IN (SELECT DISTINCT month_num
   FROM fm)
GROUP BY ty.month_num,
 box;
--EXCEPTION WHEN ERROR THEN
--SET _ERROR_CODE  =  1;
--SET _ERROR_MESSAGE  =  @@error.message;
--END;
--BEGIN
--SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS car
AS
SELECT report_period AS month_num,
  CASE
  WHEN LOWER(box) = LOWER('NCOM')
  THEN 'N.COM'
  ELSE 'R.COM'
  END AS box,
 SUM(new_customer) AS new_customer,
 SUM(retained_customer) AS retained_customer,
 SUM(react_customer) AS react_customer,
 SUM(total_customer) AS total_customer,
 SUM(total_trips) AS total_trips
FROM dl_cma_cmbr.month_num_buyerflow_segment AS a
WHERE LOWER(box) IN (LOWER('NCOM'), LOWER('NRHL'))
 AND LOWER(country) <> LOWER('CA')
 AND CAST(TRUNC(CAST(report_period AS FLOAT64)) AS INTEGER) IN (SELECT DISTINCT month_num
   FROM fm)
GROUP BY month_num,
 box;
--EXCEPTION WHEN ERROR THEN
--SET _ERROR_CODE  =  1;
--SET _ERROR_MESSAGE  =  @@error.message;
--END;
--BEGIN
--SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS nsr_prep
AS
SELECT mta.month_num,
 SUM(CASE
   WHEN LOWER(mta.order_channel) IN (LOWER('NCOM'), LOWER('FLS'))
   THEN mta.attributed_pred_net
   ELSE 0
   END) AS nordstrom_nsr_numerator,
 SUM(CASE
   WHEN LOWER(mta.arrived_channel) IN (LOWER('NCOM')) AND LOWER(mta.funnel_type) = LOWER('low')
   THEN mta.cost
   ELSE 0
   END) AS nordstrom_nsr_denominator,
 SUM(CASE
   WHEN LOWER(mta.order_channel) IN (LOWER('RCOM'), LOWER('RACK'))
   THEN mta.attributed_pred_net
   ELSE 0
   END) AS rack_nsr_numerator,
 SUM(CASE
   WHEN LOWER(mta.arrived_channel) IN (LOWER('RCOM')) AND LOWER(mta.funnel_type) = LOWER('low')
   THEN mta.cost
   ELSE 0
   END) AS rack_nsr_denominator,
  SUM(CASE
    WHEN LOWER(mta.order_channel) IN (LOWER('NCOM'), LOWER('FLS'))
    THEN mta.attributed_pred_net
    ELSE 0
    END) / IF(SUM(CASE
      WHEN LOWER(mta.arrived_channel) IN (LOWER('NCOM')) AND LOWER(mta.funnel_type) = LOWER('low')
      THEN mta.cost
      ELSE 0
      END) = 0, NULL, SUM(CASE
     WHEN LOWER(mta.arrived_channel) IN (LOWER('NCOM')) AND LOWER(mta.funnel_type) = LOWER('low')
     THEN mta.cost
     ELSE 0
     END)) AS nordstrom_nsr,
  SUM(CASE
    WHEN LOWER(mta.order_channel) IN (LOWER('NCOM'), LOWER('FLS'))
    THEN mta.attributed_pred_net
    ELSE 0
    END) / IF(SUM(CASE
      WHEN LOWER(mta.arrived_channel) IN (LOWER('NCOM')) AND LOWER(mta.funnel_type) = LOWER('low')
      THEN mta.cost
      ELSE 0
      END) = 0, NULL, SUM(CASE
     WHEN LOWER(mta.arrived_channel) IN (LOWER('NCOM')) AND LOWER(mta.funnel_type) = LOWER('low')
     THEN mta.cost
     ELSE 0
     END)) AS nordstrom_rack_nsr
FROM `{{params.gcp_project_id}}`.t2dl_das_mta.mta_ssa_cost_agg_vw AS mta
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS w ON mta.activity_date_pacific = w.day_date
WHERE mta.month_num IN (SELECT DISTINCT month_num
   FROM fm)
 AND LOWER(mta.channelcountry) = LOWER('US')
 AND LOWER(mta.marketing_type) = LOWER('paid')
 AND (LOWER(mta.arrived_channel) IN (LOWER('NCOM'), LOWER('RCOM')) OR LOWER(mta.arrived_channel) = LOWER('NULL') AND
     LOWER(mta.order_channel) IN (LOWER('FLS'), LOWER('NCOM'), LOWER('RACK'), LOWER('RCOM')))
GROUP BY mta.month_num;
--EXCEPTION WHEN ERROR THEN
--SET _ERROR_CODE  =  1;
--SET _ERROR_MESSAGE  =  @@error.message;
--END;
--BEGIN
--SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS nsr
AS
SELECT month_num,
 'N.COM' AS box,
 nordstrom_nsr_numerator AS nsr_numerator,
 nordstrom_nsr_denominator AS nsr_denominator,
 nordstrom_nsr AS nsr
FROM nsr_prep
UNION ALL
SELECT month_num,
 'R.COM' AS box,
 rack_nsr_numerator AS nsr_numerator,
 rack_nsr_denominator AS nsr_denominator,
 nordstrom_rack_nsr AS nsr
FROM nsr_prep;
--EXCEPTION WHEN ERROR THEN
--SET _ERROR_CODE  =  1;
--SET _ERROR_MESSAGE  =  @@error.message;
--END;
--BEGIN
--SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS nsr_plan_prep
AS
SELECT w.month_idnt AS month_num,
 SUM(CASE
   WHEN LOWER(mta.order_channel) IN (LOWER('NCOM'), LOWER('FLS'))
   THEN mta.fcst_attributed_pred_net
   ELSE 0
   END) AS nordstrom_nsr_plan_numerator,
 SUM(CASE
   WHEN LOWER(mta.order_channel) IN (LOWER('NCOM'))
   THEN mta.fcst_cost
   ELSE 0
   END) AS nordstrom_nsr_plan_denominator,
 SUM(CASE
   WHEN LOWER(mta.order_channel) IN (LOWER('RCOM'), LOWER('RACK'))
   THEN mta.fcst_attributed_pred_net
   ELSE 0
   END) AS rack_nsr_plan_numerator,
 SUM(CASE
   WHEN LOWER(mta.order_channel) IN (LOWER('RCOM'))
   THEN mta.fcst_cost
   ELSE 0
   END) AS rack_nsr_plan_denominator,
  SUM(CASE
    WHEN LOWER(mta.order_channel) IN (LOWER('NCOM'), LOWER('FLS'))
    THEN mta.fcst_attributed_pred_net
    ELSE 0
    END) / IF(SUM(CASE
      WHEN LOWER(mta.order_channel) IN (LOWER('NCOM'))
      THEN mta.fcst_cost
      ELSE 0
      END) = 0, NULL, SUM(CASE
     WHEN LOWER(mta.order_channel) IN (LOWER('NCOM'))
     THEN mta.fcst_cost
     ELSE 0
     END)) AS nordstrom_nsr_plan,
  SUM(CASE
    WHEN LOWER(mta.order_channel) IN (LOWER('NCOM'), LOWER('FLS'))
    THEN mta.fcst_attributed_pred_net
    ELSE 0
    END) / IF(SUM(CASE
      WHEN LOWER(mta.order_channel) IN (LOWER('NCOM'))
      THEN mta.fcst_cost
      ELSE 0
      END) = 0, NULL, SUM(CASE
     WHEN LOWER(mta.order_channel) IN (LOWER('NCOM'))
     THEN mta.fcst_cost
     ELSE 0
     END)) AS nordstrom_rack_nsr_plan
FROM `{{params.gcp_project_id}}`.t2dl_das_mta.mta_ssa_cost_fcst_agg_vw AS mta
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS w ON mta.activity_date_pacific = w.day_date
WHERE mta.month_num IN (SELECT DISTINCT month_num
   FROM fm)
 AND LOWER(mta.channelcountry) = LOWER('US')
 AND LOWER(mta.marketing_type) = LOWER('paid')
 AND (LOWER(mta.arrived_channel) IN (LOWER('NCOM'), LOWER('RCOM'), LOWER('RACK')) OR LOWER(mta.arrived_channel) = LOWER('NULL'
      ) AND LOWER(mta.order_channel) IN (LOWER('FLS'), LOWER('NCOM'), LOWER('RACK'), LOWER('RCOM')))
GROUP BY month_num;
--EXCEPTION WHEN ERROR THEN
--SET _ERROR_CODE  =  1;
--SET _ERROR_MESSAGE  =  @@error.message;
--END;
--BEGIN
--SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS nsr_plan
AS
SELECT month_num,
 'N.COM' AS box,
 nordstrom_nsr_plan_numerator AS nsr_plan_numerator,
 nordstrom_nsr_plan_denominator AS nsr_plan_denominator,
 nordstrom_nsr_plan AS nsr_plan
FROM nsr_plan_prep
UNION ALL
SELECT month_num,
 'R.COM' AS box,
 rack_nsr_plan_numerator AS nsr_plan_numerator,
 rack_nsr_plan_denominator AS nsr_plan_denominator,
 nordstrom_rack_nsr_plan AS nsr_plan
FROM nsr_plan_prep;
--EXCEPTION WHEN ERROR THEN
--SET _ERROR_CODE  =  1;
--SET _ERROR_MESSAGE  =  @@error.message;
--END;
--BEGIN
--SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS twist
AS
SELECT dc.month_idnt AS month_num,
  CASE
  WHEN LOWER(pf.channel) = LOWER('FULL_LINE')
  THEN 'N.COM'
  ELSE 'R.COM'
  END AS box,
  SUM(pf.product_views * pf.pct_instock) / SUM(CASE
    WHEN pf.pct_instock IS NOT NULL
    THEN pf.product_views
    ELSE NULL
    END) AS twist,
 SUM(pf.product_views * pf.pct_instock) AS twist_numerator,
 SUM(CASE
   WHEN pf.pct_instock IS NOT NULL
   THEN pf.product_views
   ELSE NULL
   END) AS twist_denominator
FROM `{{params.gcp_project_id}}`.t2dl_das_product_funnel.product_price_funnel_daily AS pf
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dc ON pf.event_date_pacific = dc.day_date
WHERE LOWER(pf.channelcountry) IN (LOWER('US'))
 AND dc.month_idnt IN (SELECT DISTINCT month_num
   FROM fm)
 AND LOWER(pf.current_price_type) = LOWER('R')
GROUP BY month_num,
 box;
--EXCEPTION WHEN ERROR THEN
--SET _ERROR_CODE  =  1;
--SET _ERROR_MESSAGE  =  @@error.message;
--END;
--BEGIN
--SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS sff
AS
SELECT x1.month_num,
 x1.box,
 x3.sff_demand_units_bopus_in_digital AS units_sff_shipped,
 x2.units_ds_shipped,
 x1.total_demand_units_bopus_in_digital AS units_shipped,
  CAST(x3.sff_demand_units_bopus_in_digital AS INT64) / x1.total_demand_units_bopus_in_digital AS
 pct_sff_units_shipped,
  CAST(x2.units_ds_shipped AS INT64) / x1.total_demand_units_bopus_in_digital AS pct_ds_units_shipped
FROM (SELECT dc.month_idnt AS month_num,
    CASE
    WHEN LOWER(fsdf.business_unit_desc_bopus_in_digital_demand) = LOWER('OFFPRICE ONLINE')
    THEN 'R.COM'
    ELSE fsdf.business_unit_desc_bopus_in_digital_demand
    END AS box,
   SUM(fsdf.demand_units_bopus_in_digital) AS total_demand_units_bopus_in_digital
  FROM `{{params.gcp_project_id}}`.t2dl_das_mothership.finance_sales_demand_fact AS fsdf
   LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dc ON fsdf.tran_date = dc.day_date
  WHERE LOWER(CASE
      WHEN LOWER(fsdf.business_unit_desc_bopus_in_digital_demand) = LOWER('OFFPRICE ONLINE')
      THEN 'R.COM'
      ELSE fsdf.business_unit_desc_bopus_in_digital_demand
      END) IN (LOWER('N.COM'), LOWER('R.COM'))
  GROUP BY month_num,
   box) AS x1
 LEFT JOIN (SELECT dc0.month_idnt AS month_num,
    CASE
    WHEN LOWER(fsdf0.business_unit_desc_bopus_in_digital_demand) = LOWER('OFFPRICE ONLINE')
    THEN 'R.COM'
    ELSE fsdf0.business_unit_desc_bopus_in_digital_demand
    END AS box,
   SUM(fsdf0.demand_units_bopus_in_digital) AS units_ds_shipped
  FROM `{{params.gcp_project_id}}`.t2dl_das_mothership.finance_sales_demand_fact AS fsdf0
   LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dc0 ON fsdf0.tran_date = dc0.day_date
  WHERE LOWER(fsdf0.customer_journey) IN (LOWER('ShipToStore_from_DropShip'), LOWER('ShipToHome_from_DropShip'))
   AND LOWER(CASE
      WHEN LOWER(fsdf0.business_unit_desc_bopus_in_digital_demand) = LOWER('OFFPRICE ONLINE')
      THEN 'R.COM'
      ELSE fsdf0.business_unit_desc_bopus_in_digital_demand
      END) IN (LOWER('N.COM'), LOWER('R.COM'))
  GROUP BY month_num,
   box) AS x2 ON x1.month_num = x2.month_num AND LOWER(x1.box) = LOWER(x2.box)
 LEFT JOIN (SELECT dc1.month_idnt AS month_num,
    CASE
    WHEN LOWER(fsdf1.business_unit_desc_bopus_in_digital_demand) = LOWER('OFFPRICE ONLINE')
    THEN 'R.COM'
    ELSE fsdf1.business_unit_desc_bopus_in_digital_demand
    END AS box,
   SUM(fsdf1.demand_units_bopus_in_digital) AS sff_demand_units_bopus_in_digital
  FROM `{{params.gcp_project_id}}`.t2dl_das_mothership.finance_sales_demand_fact AS fsdf1
   LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dc1 ON fsdf1.tran_date = dc1.day_date
  WHERE LOWER(fsdf1.customer_journey) IN (LOWER('ShipToStore_from_StoreFulfill'), LOWER('ShipToHome_from_StoreFulfill')
     )
   AND LOWER(CASE
      WHEN LOWER(fsdf1.business_unit_desc_bopus_in_digital_demand) = LOWER('OFFPRICE ONLINE')
      THEN 'R.COM'
      ELSE fsdf1.business_unit_desc_bopus_in_digital_demand
      END) IN (LOWER('N.COM'), LOWER('R.COM'))
  GROUP BY month_num,
   box) AS x3 ON x1.month_num = x3.month_num AND LOWER(x1.box) = LOWER(x3.box)
WHERE LOWER(x1.box) IN (LOWER('N.COM'), LOWER('R.COM'))
 AND x1.month_num IN (SELECT DISTINCT month_num
   FROM fm);
--EXCEPTION WHEN ERROR THEN
--SET _ERROR_CODE  =  1;
--SET _ERROR_MESSAGE  =  @@error.message;
--END;
--BEGIN
--SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS backlog
AS
SELECT fiscal_year * 100 + fiscal_month AS month_num,
   fiscal_year * 100 + fiscal_week AS week_num,
  CASE
  WHEN LOWER(ss_business_unit_21_day_labor) = LOWER('NRHL')
  THEN 'R.COM'
  ELSE 'N.COM'
  END AS box,
 SUM(actual_backlog) AS fc_backlog
FROM `{{params.gcp_project_id}}`.t2dl_sca_vws.daily_outbound_fct_vw AS dof
WHERE LOWER(ss_business_unit_21_day_labor) IN (LOWER('NCOM'), LOWER('NRHL'))
 AND fiscal_year * 100 + fiscal_week IN (SELECT last_week_of_mnth
   FROM lw_by_mth)
 AND LOWER(dow) = LOWER('Sat')
GROUP BY month_num,
 week_num,
 box;
--EXCEPTION WHEN ERROR THEN
--SET _ERROR_CODE  =  1;
--SET _ERROR_MESSAGE  =  @@error.message;
--END;
--BEGIN
--SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS p90
AS
SELECT CASE
  WHEN LOWER(business_unit) = LOWER('NCOM')
  THEN 'N.COM'
  ELSE 'R.COM'
  END AS box,
   cycle_year * 100 + cycle_month AS month_num,
 AVG(p90) AS p90
FROM `{{params.gcp_project_id}}`.t2dl_sca_vws.package_p50p90_daily_node_vw
WHERE LOWER(kpi) = LOWER('Click_to_deliver')
 AND cycle_year * 100 + cycle_month IN (SELECT DISTINCT month_num
   FROM fm)
 AND LOWER(node_type) = LOWER('all_up')
 AND LOWER(node) = LOWER('all_up')
 AND kpi_cohort = 3
GROUP BY box,
 month_num;
--EXCEPTION WHEN ERROR THEN
--SET _ERROR_CODE  =  1;
--SET _ERROR_MESSAGE  =  @@error.message;
--END;
--BEGIN
--SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS mbr_summary_table
AS
SELECT grow.month_num AS fm,
 grow.box,
 car.new_customer,
 car.retained_customer,
 car.react_customer,
 car.total_customer,
 CAST(car.total_trips AS INT64) AS total_trips,
 CAST(nsr.nsr_numerator AS INT64) AS nsr_numerator,
 CAST(nsr.nsr_denominator AS INT64) AS nsr_denominator,
 CAST(nsr.nsr AS INT64) AS nsr,
 CAST(twist.twist_numerator AS INT64) AS twist_numerator,
 CAST(twist.twist_denominator AS INT64) AS twist_denominator,
 CAST(twist.twist AS INT64) AS twist ,
 CAST(sff.units_shipped AS INT64) AS units_shipped,
 CAST(sff.units_sff_shipped AS INT64) AS units_sff_shipped,
 CAST(sff.units_ds_shipped AS INT64) AS units_ds_shipped,
 CAST(p90.p90 AS INT64) AS p90,
 CAST(backlog.fc_backlog AS INT64) AS fc_backlog,
LAG(CAST(car.new_customer AS INT64), 1, NULL) OVER (PARTITION BY car.box ORDER BY car.month_num) AS new_customer_lag,
LAG(CAST(car.retained_customer AS INT64), 1, NULL) OVER (PARTITION BY car.box ORDER BY car.month_num) AS retained_customer_lag,
LAG(CAST(car.react_customer AS INT64), 1, NULL) OVER (PARTITION BY car.box ORDER BY car.month_num) AS react_customer_lag,
LAG(CAST(car.total_customer AS INT64), 1, NULL) OVER (PARTITION BY car.box ORDER BY car.month_num) AS total_customer_lag,
CAST(LAG(CAST(car.total_trips AS INT64), 1, NULL) OVER (PARTITION BY car.box ORDER BY car.month_num) AS INT64) AS total_trips_lag,
LAG(CAST(nsr.nsr AS INT64), 1, NULL) OVER (PARTITION BY nsr.box ORDER BY nsr.month_num) AS nsr_lag,
LAG(CAST(twist.twist AS INT64), 1, NULL) OVER (PARTITION BY twist.box ORDER BY twist.month_num) AS twist_lag,
LAG(CAST(sff.units_shipped AS INT64), 1, NULL) OVER (PARTITION BY sff.box ORDER BY sff.month_num) AS units_shipped_lag,
LAG(CAST(sff.units_sff_shipped AS INT64), 1, NULL) OVER (PARTITION BY sff.box ORDER BY sff.month_num) AS units_sff_shipped_lag,
LAG(CAST(sff.units_ds_shipped AS INT64), 1, NULL) OVER (PARTITION BY sff.box ORDER BY sff.month_num) AS units_ds_shipped_lag,
LAG(CAST(p90.p90 AS INT64), 1, NULL) OVER (PARTITION BY p90.box ORDER BY p90.month_num) AS p90_lag,
LAG(CAST(backlog.fc_backlog AS INT64), 1, NULL) OVER (PARTITION BY backlog.box ORDER BY backlog.month_num) AS fc_backlog_lag,
CAST(grow.udv AS INT64) AS udv,
CAST(grow.udv_ly AS INT64) AS udv_ly,
CAST(grow.ordering_udv AS INT64) AS ordering_udv,
CAST(grow.ordering_udv_ly AS INT64) AS ordering_udv_ly,
CAST(grow.units AS INT64) AS units,
CAST(grow.units_ly AS INT64) AS units_ly,
CAST(grow.demand_excl_bopus AS INT64) AS demand_excl_bopus,
CAST(grow.demand AS INT64) AS demand,
CAST(grow.demand_ly AS INT64) AS demand_ly,
CAST(grow.demand_excl_bopus_ly AS INT64) AS demand_excl_bopus_ly,
CAST(grow.gross_sales_excl_bopus AS INT64) AS gross_sales_excl_bopus,
CAST(grow.gross_merch_sales_amt AS INT64) AS gross_merch_sales_amt,
CAST(grow.merch_returns_amt AS INT64) AS merch_returns_amt,
CAST(grow.net_merch_sales_amt AS INT64) AS net_merch_sales_amt,
CAST(grow.net_merch_sales_amt_ly AS INT64) AS net_merch_sales_amt_ly,
CAST(grow.orders AS INT64) AS orders,
CAST(grow.orders_ly AS INT64) AS orders_ly,
LAG(CAST(grow.udv AS INT64), 1, NULL) OVER (PARTITION BY grow.box ORDER BY grow.month_num) AS udv_lag,
LAG(CAST(grow.udv_ly AS INT64), 1, NULL) OVER (PARTITION BY grow.box ORDER BY grow.month_num) AS udv_ly_lag,
LAG(CAST(grow.ordering_udv AS INT64), 1, NULL) OVER (PARTITION BY grow.box ORDER BY grow.month_num) AS ordering_udv_lag,
LAG(CAST(grow.ordering_udv_ly AS INT64), 1, NULL) OVER (PARTITION BY grow.box ORDER BY grow.month_num) AS ordering_udv_ly_lag,
LAG(CAST(grow.canceled_demand_excl_bopus AS INT64), 1, NULL) OVER (PARTITION BY grow.box ORDER BY grow.month_num) AS canceled_demand_excl_bopus_lag,
LAG(CAST(grow.canceled_demand_excl_bopus AS INT64), 2, NULL) OVER (PARTITION BY grow.box ORDER BY grow.month_num) AS canceled_demand_excl_bopus_lag2,
LAG(CAST(grow.units AS INT64), 1, NULL) OVER (PARTITION BY grow.box ORDER BY grow.month_num) AS units_lag,
LAG(CAST(grow.units_ly AS INT64), 1, NULL) OVER (PARTITION BY grow.box ORDER BY grow.month_num) AS units_ly_lag,
LAG(CAST(grow.demand_excl_bopus AS INT64), 1, NULL) OVER (PARTITION BY grow.box ORDER BY grow.month_num) AS demand_excl_bopus_lag,
LAG(CAST(grow.demand_excl_bopus AS INT64), 2, NULL) OVER (PARTITION BY grow.box ORDER BY grow.month_num) AS demand_excl_bopus_lag2,
LAG(CAST(grow.demand_excl_bopus_ly AS INT64), 1, NULL) OVER (PARTITION BY grow.box ORDER BY grow.month_num) AS demand_excl_bopus_ly_lag,
LAG(CAST(grow.demand AS INT64), 1, NULL) OVER (PARTITION BY grow.box ORDER BY grow.month_num) AS demand_lag,
LAG(CAST(grow.demand_ly AS INT64), 1, NULL) OVER (PARTITION BY grow.box ORDER BY grow.month_num) AS demand_ly_lag,
LAG(CAST(grow.gross_sales_excl_bopus AS INT64), 1, NULL) OVER (PARTITION BY grow.box ORDER BY grow.month_num) AS gross_sales_excl_bopus_lag,
LAG(CAST(grow.gross_merch_sales_amt AS INT64), 1, NULL) OVER (PARTITION BY grow.box ORDER BY grow.month_num) AS gross_merch_sales_amt_lag,
LAG(CAST(grow.merch_returns_amt AS INT64), 1, NULL) OVER (PARTITION BY grow.box ORDER BY grow.month_num) AS merch_returns_amt_lag,
LAG(CAST(grow.net_merch_sales_amt AS INT64), 1, NULL) OVER (PARTITION BY grow.box ORDER BY grow.month_num) AS net_merch_sales_amt_lag,
LAG(CAST(grow.net_merch_sales_amt_ly AS INT64), 1, NULL) OVER (PARTITION BY grow.box ORDER BY grow.month_num) AS net_merch_sales_amt_ly_lag,
LAG(CAST(grow.orders AS INT64), 1, NULL) OVER (PARTITION BY grow.box ORDER BY grow.month_num) AS orders_lag,
LAG(CAST(grow.orders_ly AS INT64), 1, NULL) OVER (PARTITION BY grow.box ORDER BY grow.month_num) AS orders_ly_lag,
CAST(nsr_plan.nsr_plan_numerator AS INT64) AS nsr_plan_numerator,
CAST(nsr_plan.nsr_plan_denominator AS INT64) AS nsr_plan_denominator,
CAST(nsr_plan.nsr_plan AS INT64) AS nsr_plan
FROM grow
 LEFT JOIN car ON grow.month_num = CAST(car.month_num AS FLOAT64) AND LOWER(grow.box) = LOWER(car.box)
 LEFT JOIN nsr AS nsr ON grow.month_num = nsr.month_num AND LOWER(grow.box) = LOWER(nsr.box)
 LEFT JOIN nsr_plan AS nsr_plan ON grow.month_num = nsr_plan.month_num AND LOWER(grow.box) = LOWER(nsr_plan.box)
 LEFT JOIN twist AS twist ON grow.month_num = twist.month_num AND LOWER(grow.box) = LOWER(twist.box)
 LEFT JOIN sff ON grow.month_num = sff.month_num AND LOWER(grow.box) = LOWER(sff.box)
 LEFT JOIN p90 AS p90 ON grow.month_num = p90.month_num AND LOWER(grow.box) = LOWER(p90.box)
 LEFT JOIN backlog ON grow.month_num = backlog.month_num AND LOWER(grow.box) = LOWER(backlog.box);
--EXCEPTION WHEN ERROR THEN
--SET _ERROR_CODE  =  1;
--SET _ERROR_MESSAGE  =  @@error.message;
--END;
--BEGIN
--SET _ERROR_CODE  =  0;



CREATE TEMPORARY TABLE IF NOT EXISTS pre_insert
AS
SELECT fm,
 box,
 feature_name,
 (CAST(feature_value AS FLOAT64)) AS feature_value
FROM (SELECT *
  FROM mbr_summary_table) UNPIVOT EXCLUDE NULLS ((feature_value) FOR feature_name IN (new_customer AS 'null',
   retained_customer AS 'null', react_customer AS 'null', total_customer AS 'null', total_trips AS 'null', nsr_numerator
    AS 'null', nsr_denominator AS 'null', nsr AS 'null', twist_numerator AS 'null', twist_denominator AS 'null', twist
   AS 'null', p90 AS 'null', fc_backlog AS 'null', units_shipped AS 'null', units_sff_shipped AS 'null',
   units_ds_shipped AS 'null', new_customer_lag AS 'null', retained_customer_lag AS 'null', react_customer_lag AS 'null'
   , total_customer_lag AS 'null', total_trips_lag AS 'null', nsr_lag AS 'null', twist_lag AS 'null', p90_lag AS 'null'
   , fc_backlog_lag AS 'null', units_shipped_lag AS 'null', units_sff_shipped_lag AS 'null', units_ds_shipped_lag AS
   'null', udv AS 'null', udv_ly AS 'null', ordering_udv AS 'null', ordering_udv_ly AS 'null', units AS 'null', units_ly
    AS 'null', demand_excl_bopus AS 'null', demand AS 'null', demand_ly AS 'null', demand_excl_bopus_ly AS 'null',
   gross_sales_excl_bopus AS 'null', gross_merch_sales_amt AS 'null', merch_returns_amt AS 'null', net_merch_sales_amt
   AS 'null', net_merch_sales_amt_ly AS 'null', orders AS 'null', orders_ly AS 'null', udv_lag AS 'null', udv_ly_lag AS
   'null', ordering_udv_lag AS 'null', ordering_udv_ly_lag AS 'null', canceled_demand_excl_bopus_lag AS 'null',
   canceled_demand_excl_bopus_lag2 AS 'null', units_lag AS 'null', units_ly_lag AS 'null', demand_excl_bopus_lag AS
   'null', demand_excl_bopus_lag2 AS 'null', demand_excl_bopus_ly_lag AS 'null', demand_lag AS 'null', demand_ly_lag AS
   'null', gross_sales_excl_bopus_lag AS 'null', gross_merch_sales_amt_lag AS 'null', merch_returns_amt_lag AS 'null',
   net_merch_sales_amt_lag AS 'null', net_merch_sales_amt_ly_lag AS 'null', orders_lag AS 'null', orders_ly_lag AS
   'null', nsr_plan AS 'null'));
--EXCEPTION WHEN ERROR THEN
--SET _ERROR_CODE  =  1;
--SET _ERROR_MESSAGE  =  @@error.message;
--END;
--BEGIN
--SET _ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.mothership_t2_schema}}.mbr_primary;
--EXCEPTION WHEN ERROR THEN
--SET _ERROR_CODE  =  1;
--SET _ERROR_MESSAGE  =  @@error.message;
--END;
--BEGIN
--SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.mothership_t2_schema}}.mbr_primary
(SELECT CAST(fm AS NUMERIC) AS month_num,
  box AS business_unit_desc,
  feature_name,
  CAST(feature_value AS INT64) AS feature_value,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM pre_insert);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN(month_num), COLUMN(business_unit_desc), COLUMN(feature_name) ON `{{params.gcp_project_id}}`.t2dl_das_mothership.mbr_primary;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
