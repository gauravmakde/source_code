BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP07324;
DAG_ID=fls_assortment_pilot_11521_ACE_ENG;
---    Task_Name=fls_assortment_lkps;'*/

/*
T2/Table Names: 
- t2dl_das_ccs_categories.pilot_sku_dim
- t2dl_das_ccs_categories.pilot_store_dim
- t2dl_das_ccs_categories.pilot_day_dim
Team/Owner: Merch Insights / Thomas Peterson
Date Created/Modified: 6/19/2024
*/
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS brand_ind
AS
SELECT p.rms_sku_num,
  CASE
  WHEN LOWER(ab.anchor_brand_ind) = LOWER('Y')
  THEN 'Y'
  ELSE 'N'
  END AS anchor_brand_ind,
  CASE
  WHEN LOWER(rsb.rack_strategic_brand_ind) = LOWER('Y')
  THEN 'Y'
  ELSE 'N'
  END AS rack_strategic_brand_ind
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS p
 LEFT JOIN (SELECT dept_num,
   supplier_idnt,
   brand_name,
   anchor_brand_name,
   anchor_brand_ind,
   anchor_brand_tag,
   field_of_play,
   ROW_NUMBER() OVER (PARTITION BY dept_num, supplier_idnt, brand_name ORDER BY anchor_brand_ind DESC) AS rn
  FROM t2dl_das_in_season_management_reporting.anchor_brands
  WHERE LOWER(banner) = LOWER('NORDSTROM')
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY dept_num, supplier_idnt, brand_name ORDER BY anchor_brand_ind DESC)) = 1) AS
 ab ON p.dept_num = ab.dept_num AND CAST(p.prmy_supp_num AS FLOAT64) = ab.supplier_idnt AND LOWER(p.brand_name) = LOWER(ab
    .brand_name)
 LEFT JOIN (SELECT dept_num,
   supplier_idnt,
   rack_strategic_brand_ind,
   rack_strategy_type,
   ROW_NUMBER() OVER (PARTITION BY dept_num, supplier_idnt ORDER BY rack_strategic_brand_ind DESC) AS rn
  FROM t2dl_das_in_season_management_reporting.rack_strategic_brands
  WHERE LOWER(banner) = LOWER('RACK')
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY dept_num, supplier_idnt ORDER BY rack_strategic_brand_ind DESC)) = 1) AS rsb
 ON p.dept_num = rsb.dept_num AND CAST(p.prmy_supp_num AS FLOAT64) = rsb.supplier_idnt
WHERE LOWER(p.channel_country) = LOWER('US');
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (rms_sku_num) ON brand_ind;
BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_sku_dim;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_sku_dim
(SELECT p.rms_sku_num,
  p.rms_style_num,
  p.style_desc,
  p.style_group_num,
  p.web_style_num,
  p.supp_part_num,
  p.color_num,
  p.color_desc,
  p.size_1_num,
  p.size_1_desc,
  p.size_2_num,
  p.size_2_desc,
              TRIM(FORMAT('%11d', p.dept_num)) || ':' || TRIM(FORMAT('%11d', p.class_num)) || ':' || TRIM(FORMAT('%11d'
             , p.sbclass_num)) || ':' || TRIM(p.prmy_supp_num) || ':' || TRIM(p.supp_part_num) || ':' || TRIM(p.color_num
      ) || ':' || TRIM(p.color_desc) AS cc_num,
  p.div_num,
    TRIM(FORMAT('%11d', p.div_num)) || ', ' || p.div_desc AS div_label,
  p.grp_num AS sdiv_num,
    TRIM(FORMAT('%11d', p.grp_num)) || ', ' || p.grp_desc AS sdiv_label,
  p.dept_num,
    TRIM(FORMAT('%11d', p.dept_num)) || ', ' || p.dept_desc AS dept_label,
  p.class_num,
    TRIM(FORMAT('%11d', p.class_num)) || ', ' || p.class_desc AS class_label,
  p.sbclass_num,
    TRIM(FORMAT('%11d', p.sbclass_num)) || ', ' || p.sbclass_desc AS sbclass_label,
  p.prmy_supp_num AS supplier_num,
  v.vendor_name AS supplier_name,
  UPPER(p.brand_name) AS brand_name,
  b.anchor_brand_ind,
  b.rack_strategic_brand_ind,
  p.npg_ind,
  COALESCE(map1.category, map2.category, 'OTHER') AS quantrix_category,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS p
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.catg_subclass_map_dim AS map1 ON p.dept_num = map1.dept_num AND p.class_num = 
  CAST(TRUNC(CAST(CASE
       WHEN map1.class_num = ''
       THEN '0'
       ELSE map1.class_num
       END AS FLOAT64)) AS INTEGER) AND p.sbclass_num =
  CAST(TRUNC(CAST(CASE
      WHEN map1.sbclass_num = ''
      THEN '0'
      ELSE map1.sbclass_num
      END AS FLOAT64)) AS INTEGER)
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.catg_subclass_map_dim AS map2 ON p.dept_num = map2.dept_num AND p.class_num = 
  CAST(TRUNC(CAST(CASE
       WHEN map2.class_num = ''
       THEN '0'
       ELSE map2.class_num
       END AS FLOAT64)) AS INTEGER) AND LOWER(map2.sbclass_num) = LOWER('-1')
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS v ON LOWER(p.prmy_supp_num) = LOWER(v.vendor_num)
  LEFT JOIN brand_ind AS b ON LOWER(p.rms_sku_num) = LOWER(b.rms_sku_num)
 WHERE LOWER(p.channel_country) = LOWER('US'));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats primary index (rms_sku_num) ,column (cc_num) on `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_sku_dim;
BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_store_dim;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_store_dim
WITH store_dim_base AS (SELECT s.store_num,
   TRIM(FORMAT('%11d', s.store_num)) || ', ' || s.store_short_name AS store_label,
  CASE
  WHEN s.store_num IN (209, 210, 212)
  THEN 210
  ELSE s.store_num
  END AS store_num_nyc,
  CASE
  WHEN s.store_num IN (209, 210, 212)
  THEN '210+, NYC FLAGSHIP+'
  ELSE TRIM(FORMAT('%11d', s.store_num)) || ', ' || s.store_short_name
  END AS store_label_nyc,
 s.channel_num,
   TRIM(FORMAT('%11d', s.channel_num)) || ', ' || s.channel_desc AS channel_label,
 s.selling_channel,
 s.banner,
 s.store_type_code,
 s.store_type_desc,
 s.store_address_state,
 s.store_address_state_name,
 s.store_postal_code,
 s.store_country_code,
 s.store_dma_desc,
 s.store_region,
 s.store_location_latitude,
 s.store_location_longitude,
 s.comp_status_code,
 s.eligibility_types,
  CASE
  WHEN s.channel_num = 110 AND LOWER(s.store_type_code) = LOWER('FL') AND s.store_close_date IS NULL AND s.store_num
    NOT IN (387, 922, 923, 1443, 1446, 8889, 427)
  THEN 1
  WHEN s.channel_num = 210 AND s.store_close_date IS NULL AND LOWER(s.store_name) <> LOWER('UNASSIGNED RK')
  THEN 1
  ELSE 0
  END AS active_store_flag,
 s2.gross_square_footage
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.jwn_store_dim_vw AS s
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw AS s2 ON s.store_num = s2.store_num)
SELECT store_num,
 store_label,
 store_num_nyc,
 store_label_nyc,
 channel_num,
 channel_label,
 selling_channel,
 banner,
 store_type_code,
 store_type_desc,
 store_address_state,
 store_address_state_name,
 store_postal_code,
 store_country_code,
 store_dma_desc,
 store_region,
 store_location_latitude,
 store_location_longitude,
 comp_status_code,
 eligibility_types,
 active_store_flag,
 gross_square_footage,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM store_dim_base AS b
WHERE channel_num = 110
 AND (active_store_flag = 1 OR store_num = 209);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats primary index(store_num) ,column (store_num ,channel_num) on `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_store_dim;
BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_day_dim;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_day_dim
(
day_date,
day_num,
day_abrv,
day_index,
first_day_of_week_flag,
last_day_of_week_flag,
week_num,
week_end_date,
week_label,
week_index,
first_week_of_month_flag,
last_week_of_month_flag,
month_num,
month_abrv,
month_label,
month_index,
quarter_num,
quarter_label,
half_num,
half_label,
year_num,
year_label,
true_week_num,
dw_sys_load_tmstp
)
WITH ty_ly AS (SELECT SUBSTR('TY', 1, 3) AS ty_ly_lly_ind,
 day_date,
 day_idnt AS day_num,
 day_abrv,
 week_idnt AS week_num,
 week_num_of_fiscal_month,
 week_end_day_date AS week_end_date,
 fiscal_week_num,
 month_idnt AS month_num,
 month_abrv,
 month_end_day_date AS month_end_date,
 quarter_idnt AS quarter_num,
 fiscal_quarter_num,
 fiscal_halfyear_num AS half_num,
 fiscal_year_num AS year_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS a
WHERE fiscal_year_num = (SELECT MAX(fiscal_year_num)
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE month_end_day_date <= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY))
UNION ALL
SELECT 'LY' AS ty_ly_lly_ind,
 a0.day_date_last_year_realigned AS day_date,
  a0.day_idnt - 1000 AS day_num,
 b.day_abrv,
  a0.week_idnt - 100 AS week_num,
 a0.week_num_of_fiscal_month,
 b.week_end_day_date AS week_end_date,
 a0.fiscal_week_num,
  a0.month_idnt - 100 AS month_num,
 a0.month_abrv,
 b.month_end_day_date AS month_end_date,
  CAST(TRUNC(CAST(CASE
   WHEN TRIM(TRIM(FORMAT('%11d', a0.fiscal_year_num - 1)) || TRIM(FORMAT('%4d', a0.fiscal_quarter_num))) = ''
   THEN '0'
   ELSE TRIM(TRIM(FORMAT('%11d', a0.fiscal_year_num - 1)) || TRIM(FORMAT('%4d', a0.fiscal_quarter_num)))
   END AS FLOAT64)) AS INTEGER) AS quarter_idnt,
 a0.fiscal_quarter_num,
  a0.fiscal_halfyear_num - 10 AS fiscal_halfyear_num,
  a0.fiscal_year_num - 1 AS fiscal_year_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS a0
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS b ON a0.day_date_last_year_realigned = b.day_date
WHERE a0.fiscal_year_num = (SELECT MAX(fiscal_year_num)
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE month_end_day_date <= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY))
UNION ALL
SELECT 'LLY' AS ty_ly_lly_ind,
 b0.day_date_last_year_realigned AS day_date,
  a1.day_idnt - 2000 AS day_num,
 c.day_abrv,
  a1.week_idnt - 200 AS week_num,
 a1.week_num_of_fiscal_month,
 c.week_end_day_date AS week_end_date,
 a1.fiscal_week_num,
  a1.month_idnt - 200 AS month_idnt,
 a1.month_abrv,
 c.month_end_day_date AS month_end_date,
  CAST(TRUNC(CAST(CASE
   WHEN TRIM(TRIM(FORMAT('%11d', a1.fiscal_year_num - 2)) || TRIM(FORMAT('%4d', a1.fiscal_quarter_num))) = ''
   THEN '0'
   ELSE TRIM(TRIM(FORMAT('%11d', a1.fiscal_year_num - 2)) || TRIM(FORMAT('%4d', a1.fiscal_quarter_num)))
   END AS FLOAT64)) AS INTEGER) AS quarter_idnt,
 a1.fiscal_quarter_num,
  a1.fiscal_halfyear_num - 20 AS fiscal_halfyear_num,
  a1.fiscal_year_num - 2 AS fiscal_year_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS a1
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS b0 ON a1.day_date_last_year_realigned = b0.day_date
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS c ON b0.day_date_last_year_realigned = c.day_date
WHERE a1.fiscal_year_num = (SELECT MAX(fiscal_year_num)
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE week_end_day_date <= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)))
SELECT b.day_date,
 b.day_num,
  SUBSTR(b.day_abrv, 0, 1) || LOWER(SUBSTR(b.day_abrv, 2 * -1)) AS day_abrv,
 b.day_index,
 b.first_day_of_week_flag,
 b.last_day_of_week_flag,
 b.week_num,
 b.week_end_date,
    'Week ' || SUBSTR(CAST(b.fiscal_week_num AS STRING), 1, 2) || ' ' || ('FY\'\'' || SUBSTR(SUBSTR(CAST(b.year_num AS STRING)
      , 1, 4), (2 * -1))) AS week_label,
 b.week_index,
 b.first_week_of_month_flag,
 b.last_week_of_month_flag,
 b.month_num,
  SUBSTR(b.month_abrv, 0, 1) || LOWER(SUBSTR(b.month_abrv, 2 * -1)) AS month_abrv,
    SUBSTR(b.month_abrv, 0, 1) || LOWER(SUBSTR(b.month_abrv, 2 * -1)) || ' ' || ('FY\'\'' || SUBSTR(SUBSTR(CAST(b.year_num AS STRING)
      , 1, 4), (2 * -1))) AS month_label,
 b.month_index,
 b.quarter_num,
    'Q' || SUBSTR(CAST(b.fiscal_quarter_num AS STRING), 1, 1) || ' ' || ('FY\'\'' || SUBSTR(SUBSTR(CAST(b.year_num AS STRING)
      , 1, 4), (2 * -1))) AS quarter_label,
 b.half_num,
    'H' || SUBSTR(CAST(MOD(b.half_num, 10) AS STRING), 1, 1) || ' ' || ('FY\'\'' || SUBSTR(SUBSTR(CAST(b.year_num AS STRING)
      , 1, 4), (2 * -1))) AS half_label,
 b.year_num,
  'FY\'\'' || SUBSTR(SUBSTR(CAST(b.year_num AS STRING), 1, 4), (2 * -1)) AS year_label,
 c.week_idnt AS true_week_num,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM (SELECT ty_ly_lly_ind,
   day_date,
   day_num,
   day_abrv,
   week_num,
   week_num_of_fiscal_month,
   week_end_date,
   fiscal_week_num,
   month_num,
   month_abrv,
   month_end_date,
   quarter_num,
   fiscal_quarter_num,
   half_num,
   year_num,
    CASE
    WHEN day_num = (MIN(day_num) OVER (PARTITION BY week_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
      )
    THEN 1
    ELSE 0
    END AS first_day_of_week_flag,
    CASE
    WHEN day_num = (MAX(day_num) OVER (PARTITION BY week_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
      )
    THEN 1
    ELSE 0
    END AS last_day_of_week_flag,
    CASE
    WHEN week_num = (MIN(week_num) OVER (PARTITION BY month_num RANGE BETWEEN UNBOUNDED PRECEDING AND
       UNBOUNDED FOLLOWING))
    THEN 1
    ELSE 0
    END AS first_week_of_month_flag,
    CASE
    WHEN week_num = (MAX(week_num) OVER (PARTITION BY month_num RANGE BETWEEN UNBOUNDED PRECEDING AND
       UNBOUNDED FOLLOWING))
    THEN 1
    ELSE 0
    END AS last_week_of_month_flag,
    CASE
    WHEN day_date < CURRENT_DATE('PST8PDT')
    THEN 1
    ELSE 0
    END AS day_complete_flag,
    CASE
    WHEN week_end_date < CURRENT_DATE('PST8PDT')
    THEN 1
    ELSE 0
    END AS week_complete_flag,
    CASE
    WHEN month_end_date < CURRENT_DATE('PST8PDT')
    THEN 1
    ELSE 0
    END AS month_complete_flag,
     CASE
     WHEN day_date < CURRENT_DATE('PST8PDT')
     THEN 1
     ELSE 0
     END * (DENSE_RANK() OVER (PARTITION BY CASE
         WHEN day_date < CURRENT_DATE('PST8PDT')
         THEN 1
         ELSE 0
         END ORDER BY day_num DESC)) AS day_index,
     CASE
     WHEN week_end_date < CURRENT_DATE('PST8PDT')
     THEN 1
     ELSE 0
     END * (DENSE_RANK() OVER (PARTITION BY CASE
         WHEN week_end_date < CURRENT_DATE('PST8PDT')
         THEN 1
         ELSE 0
         END ORDER BY week_num DESC)) AS week_index,
     CASE
     WHEN month_end_date < CURRENT_DATE('PST8PDT')
     THEN 1
     ELSE 0
     END * (DENSE_RANK() OVER (PARTITION BY CASE
         WHEN month_end_date < CURRENT_DATE('PST8PDT')
         THEN 1
         ELSE 0
         END ORDER BY month_num DESC)) AS month_index
  FROM ty_ly AS a) AS b
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS c ON b.day_date = c.day_date
WHERE b.week_index >= 1
 AND b.month_index <= 24;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
