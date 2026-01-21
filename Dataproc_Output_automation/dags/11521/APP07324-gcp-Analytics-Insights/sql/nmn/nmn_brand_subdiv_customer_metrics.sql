BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS date_mapping AS
SELECT DISTINCT day_date,
 month_idnt,
  month_idnt - 100 AS month_start,
  CASE
  WHEN CAST(CASE
     WHEN SUBSTR(SUBSTR(CAST(month_idnt AS STRING), 1, 10), 2 * -1) = ''
     THEN '0'
     ELSE SUBSTR(SUBSTR(CAST(month_idnt AS STRING), 1, 10), 2 * -1)
     END AS INTEGER) = 1
  THEN month_idnt - 89
  ELSE month_idnt - 1
  END AS month_end
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS variables AS
SELECT MAX(month_idnt) AS month_idnt,
 MIN(day_date) AS start_date,
 MAX(day_date) AS end_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE month_idnt BETWEEN (SELECT month_start FROM date_mapping
   WHERE day_date = {{params.start_date}}) 
   AND (SELECT month_end FROM date_mapping
   WHERE day_date = {{params.end_date}});

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS shoppers AS
SELECT DISTINCT acp_id
FROM `{{params.gcp_project_id}}`.t2dl_das_sales_returns.sales_and_returns_fact
WHERE business_day_date <= (SELECT end_date  FROM variables)
 AND LOWER(transaction_type) = LOWER('retail')
 AND shipped_sales > 0
 AND LOWER(business_unit_desc) IN (LOWER('N.COM'), LOWER('FULL LINE'))
 AND acp_id IS NOT NULL;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS brand_product AS
SELECT p.rms_sku_num AS sku_num,
 p.grp_desc AS sub_division,
 t1.vendor_brand_name AS brand_name,
 t1.vendor_brand_code AS brand_id,
 SUBSTR('Head', 1, 10) AS brand_tier
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS p
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_style_dim AS q 
 ON p.epm_style_num = q.epm_style_num AND LOWER(p.channel_country) = LOWER(q.channel_country)
 INNER JOIN (SELECT *  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_label_dim AS v
  WHERE vendor_brand_code IN (SELECT DISTINCT cast(brand_id as string)
     FROM `{{params.gcp_project_id}}`.{{params.nmn_t2_schema}}.nmn_head_brands)) AS t1 
  ON CAST(q.vendor_label_code AS FLOAT64) = t1.vendor_label_code
  WHERE LOWER(p.channel_country) = LOWER('US')
UNION ALL
SELECT p0.rms_sku_num AS sku_num,
 p0.grp_desc AS sub_division,
 t6.vendor_brand_name AS brand_name,
 t6.vendor_brand_code AS brand_id,
 SUBSTR('Torso', 1, 10) AS brand_tier
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS p0
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_style_dim AS q0 
 ON p0.epm_style_num = q0.epm_style_num AND LOWER(p0.channel_country ) = LOWER(q0.channel_country)
 INNER JOIN (SELECT * FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_label_dim AS v
  WHERE vendor_brand_code IN (SELECT DISTINCT cast(brand_id as string) FROM `{{params.gcp_project_id}}`.{{params.nmn_t2_schema}}.nmn_torso_brands)) AS t6 
  ON CAST(q0.vendor_label_code AS FLOAT64) = t6.vendor_label_code
WHERE LOWER(p0.channel_country) = LOWER('US');

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS brand_visits AS
SELECT acp_id,
 brand_name,
 business_day_date,
 brand_tier,
 sub_division,
 is_brand_day_0 AS is_brand_day,
 RANK() OVER (PARTITION BY acp_id, brand_name, brand_tier, sub_division ORDER BY business_day_date DESC) AS visit_rank
FROM (SELECT t6.acp_id,
   t6.brand_name,
   t6.business_day_date,
   t6.brand_tier,
   t6.sub_division,
   t6.is_brand_day,
   MAX(t6.is_brand_day) AS is_brand_day_0
  FROM (SELECT s.acp_id,
     np.brand_name,
     np.brand_tier,
     np.sub_division,
      CASE
      WHEN r.business_day_date BETWEEN (SELECT start_date FROM variables) AND (SELECT end_date FROM variables)
      THEN (SELECT start_date FROM variables)
      ELSE r.business_day_date
      END AS business_day_date,
     MAX(CASE
       WHEN np.sku_num IS NOT NULL
       THEN 1
       ELSE 0
       END) AS is_brand_day
    FROM `{{params.gcp_project_id}}`.t2dl_das_sales_returns.sales_and_returns_fact AS r
     INNER JOIN shoppers AS s ON LOWER(r.acp_id) = LOWER(s.acp_id)
     LEFT JOIN brand_product AS np ON LOWER(r.sku_num) = LOWER(np.sku_num)
    WHERE LOWER(r.transaction_type) = LOWER('retail')
     AND r.business_day_date <= (SELECT end_date  FROM variables)
     AND LOWER(r.business_unit_desc) IN (LOWER('N.COM'), LOWER('FULL LINE'))
     AND s.acp_id IS NOT NULL
    GROUP BY s.acp_id,
     np.brand_name,
     np.brand_tier,
     np.sub_division,
     business_day_date
    HAVING is_brand_day = 1) AS t6
  GROUP BY t6.acp_id,
   t6.brand_name,
   t6.brand_tier,
   t6.sub_division,
   t6.business_day_date,
   t6.is_brand_day) AS t8;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS new_visits AS
SELECT acp_id,
 brand_name,
 brand_tier,
 sub_division,
 MAX(business_day_date) AS latest_brand_visit,
 MAX(CASE
   WHEN visit_rank = 2
   THEN business_day_date
   ELSE NULL
   END) AS second_latest_brand_visit,
  CASE
  WHEN MAX(CASE
     WHEN visit_rank = 2
     THEN business_day_date
     ELSE NULL
     END) IS NULL AND MAX(business_day_date) BETWEEN (SELECT start_date
     FROM variables) AND (SELECT end_date
     FROM variables)
  THEN 1
  ELSE 0
  END AS new_to_brand_flag,
  CASE
  WHEN MAX(CASE
    WHEN visit_rank = 2
    THEN business_day_date
    ELSE NULL
    END) IS NULL
  THEN 0
  WHEN MAX(business_day_date) BETWEEN (SELECT start_date FROM variables) AND (SELECT end_date FROM variables) 
  AND DATE_DIFF(MAX(business_day_date), MAX(CASE WHEN visit_rank = 2 THEN business_day_date  ELSE NULL END), DAY) < 365
  THEN 1
  ELSE 0
  END AS retained_flag,
  CASE
  WHEN MAX(CASE
    WHEN visit_rank = 2
    THEN business_day_date
    ELSE NULL
    END) IS NULL
  THEN 0
  WHEN MAX(business_day_date) BETWEEN (SELECT start_date
     FROM variables) AND (SELECT end_date
     FROM variables) AND DATE_DIFF(MAX(business_day_date), MAX(CASE
       WHEN visit_rank = 2
       THEN business_day_date
       ELSE NULL
       END), DAY) > 365
  THEN 1
  ELSE 0
  END AS reactivated_flag,
  CASE
  WHEN MAX(business_day_date) < (SELECT start_date
     FROM variables) AND DATE_DIFF((SELECT start_date
      FROM variables), MAX(business_day_date), DAY) BETWEEN 1 AND 365
  THEN 1
  ELSE 0
  END AS lapsed_flag
FROM brand_visits
GROUP BY acp_id,
 brand_name,
 brand_tier,
 sub_division;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS final_table AS
SELECT brand_name,
 brand_tier,
 sub_division,
 SUM(active) AS active,
 SUM(`new`) AS `new`,
 SUM(retained) AS retained,
 SUM(reactivated) AS reactivated,
 SUM(lapsed) AS lapsed
FROM (SELECT brand_name,
    brand_tier,
    sub_division,
    0 AS active,
    SUM(new_to_brand_flag) AS `new`,
    SUM(retained_flag) AS retained,
    SUM(reactivated_flag) AS reactivated,
    SUM(lapsed_flag) AS lapsed
   FROM new_visits
   GROUP BY brand_name,
    brand_tier,
    sub_division
   UNION ALL
   SELECT brand_name,
    brand_tier,
    sub_division,
    COUNT(DISTINCT acp_id) AS active,
    0 AS `new`,
    0 AS retained,
    0 AS reactivated,
    0 AS lapsed
   FROM brand_visits
   WHERE business_day_date BETWEEN (SELECT start_date  FROM variables) AND (SELECT end_date FROM variables)
   GROUP BY brand_name,
    brand_tier,
    sub_division) AS a
GROUP BY brand_name,
 brand_tier,
 sub_division;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS final_insert AS
SELECT CASE
  WHEN LOWER(nv.brand_name) = LOWER('URBAN OUTFITTERS')
  THEN 'FREE PEOPLE - URBAN OUTFITTERS'
  ELSE nv.brand_name
  END AS brand,
 variables.month_idnt,
 nv.brand_tier,
 nv.sub_division,
 nv.active,
 nv.`new`,
 nv.retained,
 nv.reactivated,
 nv.lapsed
FROM final_table AS nv
 INNER JOIN variables ON TRUE;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM `{{params.gcp_project_id}}`.{{params.nmn_t2_schema}}.nmn_brand_subdiv_customer_metrics
WHERE month_idnt = (SELECT month_idnt FROM variables);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.nmn_t2_schema}}.nmn_brand_subdiv_customer_metrics
(SELECT brand,
  month_idnt,
  brand_tier,
  sub_division,
  CAST(active AS INTEGER) AS active,
  CAST(`new` AS INTEGER) AS `new`,
  CAST(retained AS INTEGER) AS retained,
  CAST(reactivated AS INTEGER) AS reactivated,
  CAST(lapsed AS INTEGER) AS lapsed,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM final_insert AS a);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

END;
