BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP08364;
DAG_ID=twist_weekly_11521_ACE_ENG;
---     Task_Name=twist_item_weekly;'*/
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS dt
AS
SELECT DISTINCT day_date AS day_dt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS a
WHERE week_end_day_date <= CURRENT_DATE('PST8PDT')
QUALIFY (ROW_NUMBER() OVER (ORDER BY week_idnt DESC)) <= 52 * 7;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATS PRIMARY INDEX (day_dt) ON dt;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS product_sku
AS
SELECT sku.rms_sku_num,
 TRIM(FORMAT('%11d', sku.div_num) || ', ' || sku.div_desc) AS div_desc,
 TRIM(FORMAT('%11d', sku.grp_num) || ', ' || sku.grp_desc) AS subdiv_desc,
 TRIM(FORMAT('%11d', sku.dept_num) || ', ' || sku.dept_desc) AS dept_desc,
 sku.npg_ind,
 sku.dept_num,
 sku.prmy_supp_num,
  CASE
  WHEN LOWER(sku.npg_ind) = LOWER('Y')
  THEN 'NPG'
  ELSE 'NON-NPG'
  END AS supp_npg_desc,
 UPPER(sku.brand_name) AS brand_name,
 UPPER(supp.vendor_name) AS supplier,
 TRIM(FORMAT('%11d', sku.class_num) || ', ' || sku.class_desc) AS class_desc,
 TRIM(FORMAT('%11d', sku.sbclass_num) || ', ' || sku.sbclass_desc) AS subclass_desc,
   sku.supp_part_num || ', ' || sku.style_desc AS vpn,
 sku.supp_color AS color,
     sku.supp_part_num || ', ' || sku.style_desc || ', ' || sku.supp_color AS cc
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS sku
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS supp 
 ON LOWER(sku.prmy_supp_num) = LOWER(supp.vendor_num)
WHERE LOWER(sku.channel_country) = LOWER('US')
GROUP BY sku.rms_sku_num,
 div_desc,
 subdiv_desc,
 dept_desc,
 sku.npg_ind,
 sku.dept_num,
 sku.prmy_supp_num,
 supp_npg_desc,
 brand_name,
 supplier,
 class_desc,
 subclass_desc,
 vpn,
 color,
 cc;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS shr
AS
SELECT dy.week_idnt AS wk_idnt,
 t8.dept_desc,
  CAST(SUM(CASE
     WHEN loc.channel_num = 110
     THEN t8.hist_items
     ELSE 0
     END) AS NUMERIC) / NULLIF(CAST(SUM(CASE
      WHEN LOWER(t8.banner) = LOWER('NORD')
      THEN t8.hist_items
      ELSE 0
      END) AS NUMERIC), 0) AS share_fp,
  CAST(SUM(CASE
     WHEN loc.channel_num = 210
     THEN t8.hist_items
     ELSE 0
     END) AS NUMERIC) / NULLIF(CAST(SUM(CASE
      WHEN LOWER(t8.banner) = LOWER('RACK')
      THEN t8.hist_items
      ELSE 0
      END) AS NUMERIC), 0) AS share_op
FROM (SELECT td.day_date,
   sku.dept_desc,
   td.store_num,
   td.banner,
   SUM(td.hist_items) AS hist_items
  FROM `{{params.gcp_project_id}}`.{{params.twist_t2_schema}}.twist_daily AS td
   INNER JOIN product_sku AS sku ON LOWER(td.rms_sku_num) = LOWER(sku.rms_sku_num)
  WHERE td.day_date BETWEEN (SELECT MIN(day_date)
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw
     WHERE fiscal_year_num = (SELECT DISTINCT fiscal_year_num - 1
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw
        WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY))) AND (SELECT MAX(day_date)
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
     WHERE week_end_day_date <= CURRENT_DATE('PST8PDT'))
  GROUP BY td.day_date,
   td.banner,
   td.store_num,
   sku.dept_desc) AS t8
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw AS dy ON t8.day_date = dy.day_date
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS loc 
 ON CAST(t8.store_num AS FLOAT64) = loc.store_num
GROUP BY wk_idnt,
 t8.dept_desc;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATS PRIMARY INDEX (wk_idnt) ON shr;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS rp_base
AS
SELECT *
FROM `{{params.gcp_project_id}}`.{{params.twist_t2_schema}}.twist_daily
WHERE day_date >= (SELECT MIN(day_dt)
   FROM dt)
 AND day_date <= (SELECT MAX(day_dt)
   FROM dt)
 AND rp_idnt = 1;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics primary index (rms_sku_num, day_date, store_num) , column(rms_sku_num) , column(day_date) , column(store_num) on rp_base;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS fc_asoh
AS
SELECT snapshot_date AS day_date,
 rms_sku_id AS rms_sku_num,
 join_store AS store_num,
 SUM(CASE
   WHEN CAST(location_id AS FLOAT64) = 489
   THEN asoh
   ELSE 0
   END) AS asoh_489,
 SUM(CASE
   WHEN CAST(location_id AS FLOAT64) = 568
   THEN asoh
   ELSE 0
   END) AS asoh_568,
 SUM(CASE
   WHEN CAST(location_id AS FLOAT64) = 584
   THEN asoh
   ELSE 0
   END) AS asoh_584,
 SUM(CASE
   WHEN CAST(location_id AS FLOAT64) = 599
   THEN asoh
   ELSE 0
   END) AS asoh_599,
 SUM(CASE
   WHEN CAST(location_id AS FLOAT64) = 659
   THEN asoh
   ELSE 0
   END) AS asoh_659,
 SUM(CASE
   WHEN CAST(location_id AS FLOAT64) = 5629
   THEN asoh
   ELSE 0
   END) AS asoh_5629,
 SUM(CASE
   WHEN CAST(location_id AS FLOAT64) = 881
   THEN asoh
   ELSE 0
   END) AS asoh_881
FROM (SELECT snapshot_date,
   rms_sku_id,
   location_id,
    CASE
    WHEN CAST(TRUNC(CAST(location_id AS FLOAT64)) AS INTEGER) IN (489, 568, 584, 599, 659)
    THEN 808
    ELSE 828
    END AS join_store,
   SUM(stock_on_hand_qty) AS eoh,
   SUM(immediately_sellable_qty) AS asoh
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.inventory_stock_quantity_by_day_physical_fact AS a
  WHERE snapshot_date >= (SELECT MIN(day_dt)
     FROM dt)
   AND snapshot_date <= (SELECT MAX(day_dt)
     FROM dt)
   AND CAST(TRUNC(CAST(location_id AS FLOAT64)) AS INTEGER) IN (489, 568, 584, 599, 659, 5629, 881)
  GROUP BY snapshot_date,
   rms_sku_id,
   location_id,
   join_store
  HAVING asoh > 0) AS b
GROUP BY day_date,
 rms_sku_num,
 store_num;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics primary index (day_date, rms_sku_num, store_num) , column(rms_sku_num) , column(day_date) , column(store_num) on fc_asoh;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS rp_base2
AS
SELECT a.day_date,
 a.country,
 a.banner,
 a.business_unit_desc,
 a.dma,
 a.store_num,
 a.web_style_num,
 a.rms_style_num,
 a.rms_sku_num,
 a.rp_idnt,
 a.ownership_price_type,
 a.current_price_type,
 a.current_price_amt,
 a.eoh_mc,
 a.eoh,
 a.asoh_mc,
 a.asoh,
 a.mc_instock_ind,
 a.fc_instock_ind,
 a.demand,
 a.items,
 a.product_views,
 a.traffic,
 a.hist_items,
 a.hist_items_style,
 a.pct_items,
 a.allocated_traffic,
 a.dw_sys_load_tmstp,
 b.asoh_489,
 b.asoh_568,
 b.asoh_584,
 b.asoh_599,
 b.asoh_659,
 b.asoh_5629,
 b.asoh_881
FROM rp_base AS a
 LEFT JOIN fc_asoh AS b ON a.day_date = DATE_ADD(b.day_date, INTERVAL 1 DAY) AND LOWER(a.rms_sku_num) = LOWER(b.rms_sku_num
     ) AND CAST(a.store_num AS FLOAT64) = b.store_num;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics primary index (rms_sku_num, day_date, store_num) , column(rms_sku_num) , column(day_date) , column(store_num) on rp_base2;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS anc
AS
SELECT dept_num,
 supplier_idnt
FROM t2dl_das_in_season_management_reporting.anchor_brands
WHERE LOWER(anchor_brand_ind) = LOWER('Y')
GROUP BY dept_num,
 supplier_idnt;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATS PRIMARY INDEX (dept_num,supplier_idnt) ON anc;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS rsb
AS
SELECT dept_num,
 supplier_idnt
FROM t2dl_das_in_season_management_reporting.rack_strategic_brands
WHERE LOWER(rack_strategic_brand_ind) = LOWER('Y')
GROUP BY dept_num,
 supplier_idnt;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATS PRIMARY INDEX (dept_num,supplier_idnt) ON rsb;
BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.twist_t2_schema}}.twist_item_weekly;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.twist_t2_schema}}.twist_item_weekly
(SELECT tot.wk_num,
  tot.wk_end_day_dt,
  tot.wk_idnt,
  tot.wk_label,
  tot.mth_num,
  tot.fiscal_mth_label,
  tot.qtr_num,
  tot.qtr_label,
  tot.yr_num,
  SUBSTR(tot.banner, 1, 4) AS banner,
  SUBSTR(tot.country, 1, 2) AS country,
  tot.rp_product_ind,
  tot.rp_desc,
  tot.anchor_brand_ind,
  tot.rack_strategic_brand_ind,
  tot.div_desc,
  tot.subdiv_desc,
  tot.dept_desc,
  tot.npg_ind,
  tot.supp_npg_desc,
  tot.brand_name,
  tot.supplier,
  tot.class_desc,
  tot.subclass_desc,
  tot.vpn,
  tot.color,
  tot.cc,
  tot.price_type,
  tot.traffic,
  tot.ncom_traffic,
  tot.ncom_instock_traffic,
  tot.ncom_fc_instock_traffic,
  tot.fls_traffic,
  tot.fls_instock_traffic,
  tot.fls_fc_instock_traffic,
  tot.rack_traffic,
  tot.rack_instock_traffic,
  tot.rack_fc_instock_traffic,
  tot.rcom_traffic,
  tot.rcom_instock_traffic,
  tot.rcom_fc_instock_traffic,
  tot.demand,
  tot.fls_demand,
  tot.ncom_demand,
  tot.rack_demand,
  tot.rcom_demand,
  tot.items,
  tot.fls_items,
  tot.ncom_items,
  tot.rack_items,
  tot.rcom_items,
  tot.eoh_mc,
  tot.eoh_fc,
  tot.asoh_mc,
  tot.asoh_fc,
  tot.fls_eoh,
  tot.fls_asoh,
  tot.ncom_eoh_mc,
  tot.ncom_eoh_fc,
  tot.ncom_asoh_mc,
  tot.ncom_asoh_fc,
  tot.rack_eoh,
  tot.rack_asoh,
  tot.rcom_eoh,
  tot.rcom_asoh,
  tot.asoh_489,
  tot.asoh_568,
  tot.asoh_584,
  tot.asoh_599,
  tot.asoh_659,
  tot.asoh_5629,
  tot.asoh_881,
  tot.instock_489,
  tot.instock_568,
  tot.instock_584,
  tot.instock_599,
  tot.instock_659,
  tot.instock_5629,
  tot.instock_881,
  CAST(TRUNC(CAST(tot.oo_4wk_units AS FLOAT64)) AS INTEGER) AS oo_4wk_units,
  tot.oo_4wk_dollars,
  CAST(TRUNC(CAST(tot.fls_oo_4wk_units AS FLOAT64)) AS INTEGER) AS fls_oo_4wk_units,
  CAST(TRUNC(CAST(tot.ncom_oo_4wk_units AS FLOAT64)) AS INTEGER) AS ncom_oo_4wk_units,
  CAST(TRUNC(CAST(tot.rack_oo_4wk_units AS FLOAT64)) AS INTEGER) AS rack_oo_4wk_units,
  CAST(TRUNC(CAST(tot.rcom_oo_4wk_units AS FLOAT64)) AS INTEGER) AS rcom_oo_4wk_units,
  ROUND(CAST(PERCENT_RANK() OVER (PARTITION BY tot.wk_idnt, tot.banner, tot.dept_desc ORDER BY tot.items DESC, tot.traffic DESC) AS NUMERIC)
   , 4) AS quartile,
  s.share_fp,
  s.share_op,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM (SELECT dy.fiscal_week_num AS wk_num,
    dy.week_end_day_date AS wk_end_day_dt,
    dy.week_idnt AS wk_idnt,
    dy.week_454_label AS wk_label,
    dy.fiscal_month_num AS mth_num,
    dy.month_454_label AS fiscal_mth_label,
    dy.fiscal_quarter_num AS qtr_num,
    dy.quarter_label AS qtr_label,
    dy.fiscal_year_num AS yr_num,
    base.banner,
    base.country,
    base.rp_idnt AS rp_product_ind,
     CASE
     WHEN base.rp_idnt = 1
     THEN 'RP'
     WHEN base.rp_idnt = 0
     THEN 'NRP'
     ELSE 'OTHER'
     END AS rp_desc,
     CASE
     WHEN anc.supplier_idnt IS NOT NULL
     THEN 1
     ELSE 0
     END AS anchor_brand_ind,
     CASE
     WHEN rsb.supplier_idnt IS NOT NULL
     THEN 1
     ELSE 0
     END AS rack_strategic_brand_ind,
    sku.div_desc,
    sku.subdiv_desc,
    sku.dept_desc,
    sku.npg_ind,
    sku.supp_npg_desc,
    sku.brand_name,
    sku.supplier,
    sku.class_desc,
    sku.subclass_desc,
    sku.vpn,
    sku.color,
    sku.cc,
    base.current_price_type AS price_type,
    SUM(base.allocated_traffic) AS traffic,
    SUM(CASE
      WHEN LOWER(base.business_unit_desc) = LOWER('N.COM')
      THEN base.allocated_traffic
      ELSE 0
      END) AS ncom_traffic,
    SUM(CASE
      WHEN LOWER(base.business_unit_desc) = LOWER('N.COM') AND base.mc_instock_ind = 1
      THEN base.allocated_traffic
      ELSE 0
      END) AS ncom_instock_traffic,
    SUM(CASE
      WHEN LOWER(base.business_unit_desc) = LOWER('N.COM') AND base.fc_instock_ind = 1
      THEN base.allocated_traffic
      ELSE 0
      END) AS ncom_fc_instock_traffic,
    SUM(CASE
      WHEN LOWER(base.business_unit_desc) = LOWER('FULL LINE')
      THEN base.allocated_traffic
      ELSE 0
      END) AS fls_traffic,
    SUM(CASE
      WHEN LOWER(base.business_unit_desc) = LOWER('FULL LINE') AND base.mc_instock_ind = 1
      THEN base.allocated_traffic
      ELSE 0
      END) AS fls_instock_traffic,
    SUM(CASE
      WHEN LOWER(base.business_unit_desc) = LOWER('FULL LINE') AND base.fc_instock_ind = 1
      THEN base.allocated_traffic
      ELSE 0
      END) AS fls_fc_instock_traffic,
    SUM(CASE
      WHEN LOWER(base.business_unit_desc) = LOWER('RACK')
      THEN base.allocated_traffic
      ELSE 0
      END) AS rack_traffic,
    SUM(CASE
      WHEN LOWER(base.business_unit_desc) = LOWER('RACK') AND base.mc_instock_ind = 1
      THEN base.allocated_traffic
      ELSE 0
      END) AS rack_instock_traffic,
    SUM(CASE
      WHEN LOWER(base.business_unit_desc) = LOWER('RACK') AND base.fc_instock_ind = 1
      THEN base.allocated_traffic
      ELSE 0
      END) AS rack_fc_instock_traffic,
    SUM(CASE
      WHEN LOWER(base.business_unit_desc) = LOWER('OFFPRICE ONLINE')
      THEN base.allocated_traffic
      ELSE 0
      END) AS rcom_traffic,
    SUM(CASE
      WHEN LOWER(base.business_unit_desc) = LOWER('OFFPRICE ONLINE') AND base.mc_instock_ind = 1
      THEN base.allocated_traffic
      ELSE 0
      END) AS rcom_instock_traffic,
    SUM(CASE
      WHEN LOWER(base.business_unit_desc) = LOWER('OFFPRICE ONLINE') AND base.fc_instock_ind = 1
      THEN base.allocated_traffic
      ELSE 0
      END) AS rcom_fc_instock_traffic,
    SUM(base.demand) AS demand,
    SUM(CASE
      WHEN LOWER(base.business_unit_desc) = LOWER('FULL LINE')
      THEN base.demand
      ELSE 0
      END) AS fls_demand,
    SUM(CASE
      WHEN LOWER(base.business_unit_desc) = LOWER('N.COM')
      THEN base.demand
      ELSE 0
      END) AS ncom_demand,
    SUM(CASE
      WHEN LOWER(base.business_unit_desc) = LOWER('RACK')
      THEN base.demand
      ELSE 0
      END) AS rack_demand,
    SUM(CASE
      WHEN LOWER(base.business_unit_desc) = LOWER('OFFPRICE ONLINE')
      THEN base.demand
      ELSE 0
      END) AS rcom_demand,
    SUM(base.items) AS items,
    SUM(CASE
      WHEN LOWER(base.business_unit_desc) = LOWER('FULL LINE')
      THEN base.items
      ELSE 0
      END) AS fls_items,
    SUM(CASE
      WHEN LOWER(base.business_unit_desc) = LOWER('N.COM')
      THEN base.items
      ELSE 0
      END) AS ncom_items,
    SUM(CASE
      WHEN LOWER(base.business_unit_desc) = LOWER('RACK')
      THEN base.items
      ELSE 0
      END) AS rack_items,
    SUM(CASE
      WHEN LOWER(base.business_unit_desc) = LOWER('OFFPRICE ONLINE')
      THEN base.items
      ELSE 0
      END) AS rcom_items,
    SUM(CASE
      WHEN base.day_date = dy.week_end_day_date
      THEN base.eoh_mc
      ELSE 0
      END) AS eoh_mc,
    SUM(CASE
      WHEN base.day_date = dy.week_end_day_date
      THEN base.eoh
      ELSE 0
      END) AS eoh_fc,
    SUM(CASE
      WHEN base.day_date = dy.week_end_day_date
      THEN base.asoh_mc
      ELSE 0
      END) AS asoh_mc,
    SUM(CASE
      WHEN base.day_date = dy.week_end_day_date
      THEN base.asoh
      ELSE 0
      END) AS asoh_fc,
    SUM(CASE
      WHEN base.day_date = dy.week_end_day_date AND LOWER(base.business_unit_desc) = LOWER('FULL LINE')
      THEN base.eoh
      ELSE 0
      END) AS fls_eoh,
    SUM(CASE
      WHEN base.day_date = dy.week_end_day_date AND LOWER(base.business_unit_desc) = LOWER('FULL LINE')
      THEN base.asoh
      ELSE 0
      END) AS fls_asoh,
    SUM(CASE
      WHEN base.day_date = dy.week_end_day_date AND LOWER(base.business_unit_desc) = LOWER('N.COM')
      THEN base.eoh_mc
      ELSE 0
      END) AS ncom_eoh_mc,
    SUM(CASE
      WHEN base.day_date = dy.week_end_day_date AND LOWER(base.business_unit_desc) = LOWER('N.COM')
      THEN base.eoh
      ELSE 0
      END) AS ncom_eoh_fc,
    SUM(CASE
      WHEN base.day_date = dy.week_end_day_date AND LOWER(base.business_unit_desc) = LOWER('N.COM')
      THEN base.asoh_mc
      ELSE 0
      END) AS ncom_asoh_mc,
    SUM(CASE
      WHEN base.day_date = dy.week_end_day_date AND LOWER(base.business_unit_desc) = LOWER('N.COM')
      THEN base.asoh
      ELSE 0
      END) AS ncom_asoh_fc,
    SUM(CASE
      WHEN base.day_date = dy.week_end_day_date AND LOWER(base.business_unit_desc) = LOWER('RACK')
      THEN base.eoh
      ELSE 0
      END) AS rack_eoh,
    SUM(CASE
      WHEN base.day_date = dy.week_end_day_date AND LOWER(base.business_unit_desc) = LOWER('RACK')
      THEN base.asoh
      ELSE 0
      END) AS rack_asoh,
    SUM(CASE
      WHEN base.day_date = dy.week_end_day_date AND LOWER(base.business_unit_desc) = LOWER('OFFPRICE ONLINE')
      THEN base.eoh
      ELSE 0
      END) AS rcom_eoh,
    SUM(CASE
      WHEN base.day_date = dy.week_end_day_date AND LOWER(base.business_unit_desc) = LOWER('OFFPRICE ONLINE')
      THEN base.asoh
      ELSE 0
      END) AS rcom_asoh,
    SUM(CASE
      WHEN base.day_date = dy.week_end_day_date
      THEN base.asoh_489
      ELSE 0
      END) AS asoh_489,
    SUM(CASE
      WHEN base.day_date = dy.week_end_day_date
      THEN base.asoh_568
      ELSE 0
      END) AS asoh_568,
    SUM(CASE
      WHEN base.day_date = dy.week_end_day_date
      THEN base.asoh_584
      ELSE 0
      END) AS asoh_584,
    SUM(CASE
      WHEN base.day_date = dy.week_end_day_date
      THEN base.asoh_599
      ELSE 0
      END) AS asoh_599,
    SUM(CASE
      WHEN base.day_date = dy.week_end_day_date
      THEN base.asoh_659
      ELSE 0
      END) AS asoh_659,
    SUM(CASE
      WHEN base.day_date = dy.week_end_day_date
      THEN base.asoh_5629
      ELSE 0
      END) AS asoh_5629,
    SUM(CASE
      WHEN base.day_date = dy.week_end_day_date
      THEN base.asoh_881
      ELSE 0
      END) AS asoh_881,
    SUM(CASE
      WHEN LOWER(base.business_unit_desc) = LOWER('N.COM') AND LOWER(sku.div_desc) = LOWER('340, BEAUTY') AND base.fc_instock_ind
          = 1 AND base.asoh_489 > 0
      THEN base.allocated_traffic
      ELSE 0
      END) AS instock_489,
    SUM(CASE
      WHEN LOWER(base.business_unit_desc) = LOWER('N.COM') AND base.fc_instock_ind = 1 AND base.asoh_568 > 0
      THEN base.allocated_traffic
      ELSE 0
      END) AS instock_568,
    SUM(CASE
      WHEN LOWER(base.business_unit_desc) = LOWER('N.COM') AND base.fc_instock_ind = 1 AND base.asoh_584 > 0
      THEN base.allocated_traffic
      ELSE 0
      END) AS instock_584,
    SUM(CASE
      WHEN LOWER(base.business_unit_desc) = LOWER('N.COM') AND base.fc_instock_ind = 1 AND base.asoh_599 > 0
      THEN base.allocated_traffic
      ELSE 0
      END) AS instock_599,
    SUM(CASE
      WHEN LOWER(base.business_unit_desc) = LOWER('N.COM') AND LOWER(sku.div_desc) = LOWER('340, BEAUTY') AND base.fc_instock_ind
          = 1 AND base.asoh_659 > 0
      THEN base.allocated_traffic
      ELSE 0
      END) AS instock_659,
    SUM(CASE
      WHEN LOWER(base.business_unit_desc) = LOWER('OFFPRICE ONLINE') AND base.fc_instock_ind = 1 AND base.asoh_5629 > 0
      THEN base.allocated_traffic
      ELSE 0
      END) AS instock_5629,
    SUM(CASE
      WHEN LOWER(base.business_unit_desc) = LOWER('OFFPRICE ONLINE') AND base.fc_instock_ind = 1 AND base.asoh_881 > 0
      THEN base.allocated_traffic
      ELSE 0
      END) AS instock_881,
    SUM(0) AS oo_4wk_units,
    SUM(0) AS oo_4wk_dollars,
    SUM(0) AS fls_oo_4wk_units,
    SUM(0) AS ncom_oo_4wk_units,
    SUM(0) AS rack_oo_4wk_units,
    SUM(0) AS rcom_oo_4wk_units
   FROM rp_base2 AS base
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw AS dy ON base.day_date = dy.day_date
    INNER JOIN (SELECT DISTINCT store_num
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim
     WHERE LOWER(store_abbrev_name) <> LOWER('CLOSED')
      AND LOWER(selling_store_ind) = LOWER('S')
      AND channel_num NOT IN (220, 240)) AS loc ON CAST(base.store_num AS FLOAT64) = loc.store_num
    INNER JOIN product_sku AS sku ON LOWER(base.rms_sku_num) = LOWER(sku.rms_sku_num)
    LEFT JOIN anc ON sku.dept_num = anc.dept_num AND CAST(sku.prmy_supp_num AS FLOAT64) = anc.supplier_idnt AND LOWER(base
       .banner) = LOWER('NORD')
    LEFT JOIN rsb ON sku.dept_num = rsb.dept_num AND CAST(sku.prmy_supp_num AS FLOAT64) = rsb.supplier_idnt AND LOWER(base
       .banner) = LOWER('RACK')
   GROUP BY wk_num,
    wk_end_day_dt,
    wk_idnt,
    wk_label,
    mth_num,
    fiscal_mth_label,
    qtr_num,
    qtr_label,
    yr_num,
    base.banner,
    base.country,
    rp_product_ind,
    rp_desc,
    anchor_brand_ind,
    rack_strategic_brand_ind,
    sku.div_desc,
    sku.subdiv_desc,
    sku.dept_desc,
    sku.npg_ind,
    sku.supp_npg_desc,
    sku.brand_name,
    sku.supplier,
    sku.class_desc,
    sku.subclass_desc,
    sku.vpn,
    sku.color,
    sku.cc,
    price_type) AS tot
  LEFT JOIN shr AS s ON tot.wk_idnt = s.wk_idnt AND LOWER(tot.dept_desc) = LOWER(s.dept_desc));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats primary index(wk_idnt, cc), column(wk_idnt) , column(cc) on t2dl_das_twist.twist_item_weekly;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
