BEGIN
DECLARE ERROR_CODE INT64;

DECLARE ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=app08818;
DAG_ID=node_week_sku_inventory_twist_fact_11521_ACE_ENG;
---     Task_Name=node_week_sku_inventory_twist_fact;'*/
---     FOR SESSION VOLATILE;
BEGIN
SET ERROR_CODE  =  0;


CREATE TEMPORARY TABLE IF NOT EXISTS merch_mpg_sku_dim
AS
SELECT DISTINCT psd.rms_sku_num,
 psd.rms_style_num,
 psd.color_num,
 mpg.mpg_category,
 th.nord_role_desc,
 th.rack_role_desc
FROM (SELECT sbclass_num,
   class_num,
   dept_num,
   rms_sku_num,
   rms_style_num,
   color_num
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num ORDER BY channel_country DESC, dw_batch_date DESC)) = 1) AS psd
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_ccs_categories.ccs_merch_themes AS th ON psd.dept_num = th.dept_idnt AND psd.class_num = th.class_idnt
     AND psd.sbclass_num = th.sbclass_idnt
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_cal.customer_merch_mpg_dept_categories AS mpg ON psd.dept_num = mpg.dept_num
WHERE mpg.mpg_category IS NOT NULL
 OR th.nord_role_desc IS NOT NULL
 OR th.rack_role_desc IS NOT NULL;

EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (rms_style_num, color_num, mpg_category, nord_role_desc) ON merch_mpg_sku_dim;
BEGIN
SET ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS twist

AS
SELECT tw.rms_sku_num,
 tw.store_num,
 tw.day_date,
 cal.week_num,
 SUM(CASE
   WHEN tw.mc_instock_ind = 1
   THEN tw.allocated_traffic
   ELSE 0
   END) AS instock_traffic,
 SUM(tw.allocated_traffic) AS allocated_traffic
FROM `{{params.gcp_project_id}}`.t2dl_das_twist.twist_daily AS tw
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal AS cal ON tw.day_date = cal.day_date
WHERE cal.day_date BETWEEN {{params.start_date}} AND {{params.end_date}}
GROUP BY tw.rms_sku_num,
 tw.store_num,
 tw.day_date,
 cal.week_num;

EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (rms_sku_num, store_num, day_date) ON twist;
BEGIN
SET ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS twist_weekly

AS
SELECT rms_sku_num,
 store_num,
 week_num,
 SUM(instock_traffic) AS instock_traffic,
 SUM(allocated_traffic) AS allocated_traffic
FROM twist
GROUP BY rms_sku_num,
 store_num,
 week_num;
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;

--COLLECT STATISTICS COLUMN (rms_sku_num, store_num, week_num) ON twist_weekly;
BEGIN
SET ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS boh_dates

AS
SELECT year_num,
 month_num,
 week_num,
 day_date,
 DATE_SUB(MIN(day_date), INTERVAL 1 DAY) AS prior_wk_end_dt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
WHERE day_date BETWEEN {{params.start_date}} AND {{params.end_date}}
GROUP BY day_date,
 week_num,
 month_num,
 year_num;
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (prior_wk_end_dt, week_num) ON boh_dates;
BEGIN
SET ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS date_range

AS
SELECT MIN(prior_wk_end_dt) AS start_date,
 MAX(prior_wk_end_dt) AS end_date
FROM boh_dates;
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET ERROR_CODE  =  0;


CREATE TEMPORARY TABLE IF NOT EXISTS inventory_twist
AS
SELECT 

CASE
  WHEN LENGTH(TRIM(base.location_id)) >= 4
  THEN 1000
  WHEN LOWER(st.business_unit_desc) IN (LOWER('N.COM'), LOWER('TRUNK CLUB'))
  THEN 808
  WHEN LOWER(st.business_unit_desc) IN (LOWER('N.CA'))
  THEN 867
  WHEN LOWER(st.business_unit_desc) IN (LOWER('OFFPRICE ONLINE'))
  THEN 828
  ELSE CAST(TRUNC(CAST(CASE
    WHEN base.location_id = ''
    THEN '0'
    ELSE base.location_id
    END AS FLOAT64)) AS INTEGER)
  END AS store_num,

  CASE
  WHEN LOWER(st.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'))
  THEN 'nstore'
  WHEN LOWER(st.business_unit_desc) IN (LOWER('N.CA'), LOWER('N.COM'), LOWER('TRUNK CLUB'))
  THEN 'ncom'
  WHEN LOWER(st.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA'))
  THEN 'rstore'
  WHEN LOWER(st.business_unit_desc) IN (LOWER('OFFPRICE ONLINE'))
  THEN 'rcom'
  ELSE NULL
  END AS channel,
 cal.week_num,
 cal.month_num,
 cal.year_num,
 sku.rms_sku_num,
 sku.rms_style_num,
 sku.color_num,
   sku.rms_style_num || '_' || sku.color_num AS selection_cc,
 COALESCE(sku.mpg_category, 'undefined') AS mpg_category,
  CASE
  WHEN LOWER(st.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA'), LOWER('OFFPRICE ONLINE'))
  THEN COALESCE(sku.rack_role_desc, 'undefined')
  ELSE COALESCE(sku.nord_role_desc, 'undefined')
  END AS merch_role,
 SUM(COALESCE(base.stock_on_hand_qty, 0)) AS stock_on_hand_qty,
 SUM(COALESCE(base.in_transit_qty, 0)) AS in_transit_qty,
 SUM(twist0.instock_traffic) AS instock_traffic,
 SUM(twist0.allocated_traffic) AS allocated_traffic,
 SUM(twist0.instock_traffic) / SUM(twist0.allocated_traffic + 0.001) AS twist
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.inventory_stock_quantity_by_day_fact AS base
 INNER JOIN boh_dates AS cal ON base.snapshot_date = cal.prior_wk_end_dt
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS st ON CAST(base.location_id AS FLOAT64) = st.store_num
 LEFT JOIN merch_mpg_sku_dim AS sku ON LOWER(base.rms_sku_id) = LOWER(sku.rms_sku_num)
 INNER JOIN twist_weekly AS twist0 ON LOWER(sku.rms_sku_num) = LOWER(twist0.rms_sku_num) AND CASE
     WHEN LENGTH(TRIM(base.location_id)) >= 4
     THEN 1000
     WHEN LOWER(st.business_unit_desc) IN (LOWER('N.COM'), LOWER('TRUNK CLUB'))
     THEN 808
     WHEN LOWER(st.business_unit_desc) IN (LOWER('N.CA'))
     THEN 867
     WHEN LOWER(st.business_unit_desc) IN (LOWER('OFFPRICE ONLINE'))
     THEN 828
     ELSE CAST(TRUNC(CAST(CASE
    WHEN base.location_id = ''
    THEN '0'
    ELSE base.location_id
    END AS FLOAT64)) AS INTEGER)
     END = CAST(twist0.store_num AS FLOAT64) AND cal.week_num = twist0.week_num
WHERE base.snapshot_date BETWEEN (SELECT start_date
   FROM date_range) AND (SELECT end_date
   FROM date_range)
 AND LOWER(st.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'),
   LOWER('TRUNK CLUB'), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('OFFPRICE ONLINE'))
GROUP BY store_num,
 channel,
 cal.week_num,
 cal.month_num,
 cal.year_num,
 sku.rms_sku_num,
 sku.rms_style_num,
 sku.color_num,
 selection_cc,
 mpg_category,
 merch_role;

 
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET ERROR_CODE  =  0;


DELETE FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.node_week_sku_inventory_twist_fact
WHERE week_num BETWEEN (SELECT week_num
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
        WHERE day_date = {{params.start_date}}) AND (SELECT week_num
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
        WHERE day_date = {{params.end_date}});


EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET ERROR_CODE  =  0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.node_week_sku_inventory_twist_fact
(SELECT store_num AS store_number,
  week_num,
  month_num,
  year_num,
  rms_sku_num,
  color_num,
  selection_cc,
  stock_on_hand_qty,
  in_transit_qty,
  CAST(instock_traffic AS NUMERIC) AS instock_traffic,
  allocated_traffic,
  CAST(twist AS FLOAT64) AS twist,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM inventory_twist);

EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS  COLUMN (store_number), COLUMN (week_num), COLUMN (month_num), COLUMN (year_num), COLUMN (rms_sku_num),  COLUMN (color_num) on t2dl_das_usl.node_week_sku_inventory_twist_fact;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
