/*
Name: Size Curves SKU Groupid
APPID-Name: APP08076 Data Driven Size Curves
Purpose: Creates a table with weekly sales and inventory at the sku-location level from 2021 to present
    - size_sku_loc_weekly
Variable(s):    {t2dl_das_size} T2DL_DAS_SIZE
                {} '' or '_dev' tablesuffix for prod testing

DAG: APP08076_size_curves_sku_loc_weekly_main
Author(s): Zisis Daffas & Sara Riker
Date Created: 2/03/2023
Date Last Updated:2/21/2023
*/

-- DROP TABLE week_lkp;



CREATE TEMPORARY TABLE IF NOT EXISTS week_lkp
AS
SELECT day_date,
 week_idnt,
 week_start_day_date,
 day_abrv
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE day_date BETWEEN {{params.start_date}} AND {{params.end_date}};


--COLLECT STATS     COLUMN (day_date)      ON week_lkp


CREATE TEMPORARY TABLE IF NOT EXISTS loc_lkup
AS
SELECT DISTINCT store_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw AS loc
WHERE business_unit_num IN (1000, 6000, 2000, 5000)
 AND LOWER(store_name) NOT LIKE LOWER('CLSD%')
 AND LOWER(store_name) NOT LIKE LOWER('CLOSED%');


--COLLECT STATS     PRIMARY INDEX(store_num)     ON loc_lkup


CREATE TEMPORARY TABLE IF NOT EXISTS daily_loc
AS
SELECT d.sku_idnt,
 d.loc_idnt,
 w.day_date,
 w.week_idnt,
 w.week_start_day_date,
  CASE
  WHEN LOWER(w.day_abrv) = LOWER('SUN')
  THEN d.boh_units
  ELSE 0
  END AS boh_units,
  CASE
  WHEN LOWER(w.day_abrv) = LOWER('SUN')
  THEN d.boh_dollars
  ELSE 0
  END AS boh_dollars,
  CASE
  WHEN LOWER(w.day_abrv) = LOWER('SAT')
  THEN d.eoh_units
  ELSE 0
  END AS eoh_units,
  CASE
  WHEN LOWER(w.day_abrv) = LOWER('SAT')
  THEN d.eoh_dollars
  ELSE 0
  END AS eoh_dollars,
 COALESCE(d.sales_units, 0) AS sales_units,
 COALESCE(d.sales_dollars, 0) AS sales_dollars,
 COALESCE(d.return_units, 0) AS return_units,
 COALESCE(d.return_dollars, 0) AS return_dollars,
 COALESCE(d.receipt_po_units, 0) AS receipt_po_units,
 COALESCE(d.receipt_po_dollars, 0) AS receipt_po_dollars
FROM `{{params.gcp_project_id}}`.t2dl_das_ace_mfp.sku_loc_pricetype_day_vw AS d
 INNER JOIN week_lkp AS w ON d.day_dt = w.day_date
 INNER JOIN loc_lkup AS loc ON d.loc_idnt = loc.store_num;


--COLLECT STATS      PRIMARY INDEX (week_idnt, sku_idnt, loc_idnt)     ,COLUMN(week_idnt)     ,COLUMN(loc_idnt)     ON daily_loc


CREATE TEMPORARY TABLE IF NOT EXISTS week_loc
AS
SELECT sku_idnt,
 loc_idnt,
 week_idnt,
 week_start_day_date,
 SUM(IFNULL(boh_units, 0)) AS boh_units,
 SUM(IFNULL(boh_dollars, 0)) AS boh_dollars,
 SUM(IFNULL(eoh_units, 0)) AS eoh_units,
 SUM(IFNULL(eoh_dollars, 0)) AS eoh_dollars,
 SUM(sales_units) AS sales_units,
 SUM(sales_dollars) AS sales_dollars,
 SUM(return_units) AS return_units,
 SUM(return_dollars) AS return_dollars,
 SUM(receipt_po_units) AS receipt_po_units,
 SUM(receipt_po_dollars) AS receipt_po_dollars,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS update_timestamp,
 `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS update_timestamp_tz
FROM daily_loc
GROUP BY sku_idnt,
 loc_idnt,
 week_idnt,
 week_start_day_date;


DELETE FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_sku_loc_weekly{{params.env_suffix}}
WHERE week_idnt IN (SELECT DISTINCT week_idnt
  FROM week_lkp);


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_sku_loc_weekly{{params.env_suffix}}
(SELECT sku_idnt,
  loc_idnt,
  week_idnt,
  week_start_day_date,
  boh_units,
  CAST(boh_dollars AS NUMERIC) AS boh_dollars,
  eoh_units,
  CAST(eoh_dollars AS NUMERIC) AS eoh_dollars,
  sales_units,
  sales_dollars,
  return_units,
  return_dollars,
  receipt_po_units,
  receipt_po_dollars,
  CAST(update_timestamp AS TIMESTAMP) AS update_timestamp,
  update_timestamp_tz
 FROM week_loc);