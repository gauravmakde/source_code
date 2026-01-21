/* Cross Shopping Monthly : Used to report on basket insights for NMG
  Notes:
    - Hierarchy is as-was and is not fully restated every month for historicals
    - Has bado dependency on cateogry mapping table that needs to be replaced
    - Each run refreshes the prior completed fiscal month to the end date passed through
    - FOR BACKFILLS: Will not run as normal when loops autimatically, need to go month by month
*/


/* Grab most recent completed fiscal month based on run date */



CREATE TEMPORARY TABLE IF NOT EXISTS run_dates AS
SELECT month_start_day_date,
 month_end_day_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw
WHERE day_date <= DATE_SUB(current_date('PST8PDT'), INTERVAL 1 DAY)
 AND month_end_day_date <= DATE_SUB(current_date('PST8PDT'), INTERVAL 1 DAY)
QUALIFY (ROW_NUMBER() OVER (ORDER BY month_end_day_date DESC)) = 1;


CREATE TEMPORARY TABLE IF NOT EXISTS trip_summary
AS
SELECT TRIM(FORMAT('%11d', str.channel_num)) || ', ' || str.channel_desc AS channel,
   hdr.acp_id || CAST(COALESCE(dtl.business_day_date, dtl.tran_date) AS STRING) || (TRIM(FORMAT('%11d', str.channel_num
       )) || ', ' || str.channel_desc) AS trip_id,
 dt.month_num,
 dt.year_num,
   TRIM(FORMAT('%11d', sku.div_num)) || ', ' || sku.div_desc AS division,
   TRIM(FORMAT('%11d', sku.dept_num)) || ', ' || sku.dept_desc AS dept,
 COALESCE(cat.category, 'UNKNOWN') AS category,
 sku.brand_name,
 MAX(CASE
   WHEN COALESCE(dtl.tran_date, dtl.business_day_date) = acq.acq_dt
   THEN 1
   ELSE 0
   END) AS ntn,
 SUM(dtl.line_net_usd_amt) AS sales,
 SUM(dtl.line_item_quantity) AS quantity
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact AS dtl
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_hdr_fact AS hdr ON dtl.global_tran_id = hdr.global_tran_id AND dtl.business_day_date
    = hdr.business_day_date
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS str ON dtl.intent_store_num = str.store_num
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS sku ON LOWER(COALESCE(dtl.sku_num, dtl.hl_sku_num)) = LOWER(sku.rms_sku_num
    ) AND LOWER(str.store_country_code) = LOWER(sku.channel_country)
 LEFT JOIN (SELECT DISTINCT a.dept_num AS dept_idnt,
   a.class_num AS cls_idnt,
    CASE
    WHEN LOWER(a.sbclass_num) = LOWER('-1')
    THEN FORMAT('%11d', b.sbclass_num)
    ELSE a.sbclass_num
    END AS scls_idnt,
   b.sbclass_num,
   a.category
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.catg_subclass_map_dim AS a
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS b ON a.dept_num = b.dept_num AND CAST(a.class_num AS FLOAT64) = b.class_num
     
  WHERE LOWER(a.sbclass_num) = LOWER('-1')
   OR CAST(a.sbclass_num AS FLOAT64) = b.sbclass_num) AS cat ON sku.dept_num = cat.dept_idnt AND sku.class_num = CAST(cat.cls_idnt AS FLOAT64)
     AND sku.sbclass_num = CAST(cat.scls_idnt AS FLOAT64)
 LEFT JOIN (SELECT acp_id,
   MAX(CAST(trans_utc_tmstp AS DATE)) AS acq_dt
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_ntn_fact
  GROUP BY acp_id) AS acq ON LOWER(hdr.acp_id) = LOWER(acq.acp_id)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal AS dt ON COALESCE(dtl.tran_date, dtl.business_day_date) = dt.day_date
WHERE COALESCE(dtl.tran_date, dtl.business_day_date) BETWEEN (SELECT DISTINCT month_start_day_date
   FROM run_dates) AND (SELECT DISTINCT month_end_day_date
   FROM run_dates)
 AND LOWER(dtl.error_flag) = LOWER('N')
 AND LOWER(dtl.tran_latest_version_ind) = LOWER('Y')
 AND dtl.line_net_usd_amt > 0
 AND dtl.line_item_quantity > 0
 AND str.business_unit_desc IS NOT NULL
 AND hdr.acp_id IS NOT NULL
 AND sku.brand_name IS NOT NULL
GROUP BY channel,
 trip_id,
 dt.month_num,
 dt.year_num,
 division,
 dept,
 category,
 sku.brand_name;


DELETE FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.cross_shopping_monthly
WHERE month_num IN (SELECT month_num
  FROM trip_summary);


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.cross_shopping_monthly
(SELECT a.month_num,
  a.year_num,
  a.channel,
   CASE
   WHEN a.ntn = 1
   THEN 'NEW'
   ELSE 'EXISTING'
   END AS ntn,
  a.division AS source_division,
  b.division AS cross_division,
  a.dept AS source_dept,
  b.dept AS cross_dept,
  a.category AS source_category,
  b.category AS cross_category,
  UPPER(a.brand_name) AS source_brand,
  UPPER(b.brand_name) AS cross_brand,
  COUNT(DISTINCT a.trip_id) AS source_trips,
  SUM(a.sales) AS source_spend,
  CAST(SUM(a.quantity) AS BIGINT) AS source_quantity,
  COUNT(DISTINCT b.trip_id) AS cross_trips,
  SUM(b.sales) AS cross_spend,
  CAST(SUM(b.quantity) AS BIGINT) AS cross_quantity
 FROM trip_summary AS a
  LEFT JOIN trip_summary AS b ON LOWER(a.trip_id) = LOWER(b.trip_id)
 GROUP BY a.month_num,
  a.year_num,
  a.channel,
  ntn,
  source_division,
  cross_division,
  source_dept,
  cross_dept,
  source_category,
  cross_category,
  source_brand,
  cross_brand);


--COLLECT STATISTICS COLUMN(month_num), COLUMN(year_num), COLUMN(channel), COLUMN(ntn), COLUMN(source_dept), COLUMN(cross_dept), COLUMN(source_brand), COLUMN(cross_brand)  on t2dl_das_crossshopping.cross_shopping_monthly