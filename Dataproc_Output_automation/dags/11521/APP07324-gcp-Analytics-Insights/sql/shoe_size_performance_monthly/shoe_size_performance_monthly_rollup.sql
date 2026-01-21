/*SET QUERY_BAND = 'App_ID=APP07324;
DAG_ID=shoe_size_performance_monthly_rollup_11521_ACE_ENG;
---     Task_Name=shoe_size_performance_monthly_rollup;'*/
-- drop table date_lkup_1
CREATE TEMPORARY TABLE IF NOT EXISTS date_lkup_1
AS
SELECT DISTINCT re.ty_ly_lly_ind AS ty_ly_ind,
 re.fiscal_year_num AS year_idnt,
 re.quarter_abrv,
 re.quarter_label,
 re.month_idnt,
 re.month_abrv,
 re.month_label,
 re.month_end_day_date AS month_end_date,
 dcd.week_idnt AS week_idnt_true,
 re.week_idnt,
 re.fiscal_week_num,
   TRIM(FORMAT('%11d', re.fiscal_year_num)) || ' ' || re.week_desc AS week_label,
    re.month_abrv || ' ' || 'WK' || TRIM(FORMAT('%11d', re.week_num_of_fiscal_month)) AS week_of_month_abrv,
   SUBSTR(re.week_label, 0, 9) || 'WK' || TRIM(FORMAT('%11d', re.week_num_of_fiscal_month)) AS week_of_month_label,
 re.week_end_day_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw AS re
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dcd ON re.day_date = dcd.day_date
WHERE LOWER(re.ytd_ind) = LOWER('Y')
 AND re.week_end_day_date <= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)
 AND LOWER(re.ty_ly_lly_ind) IN (LOWER('TY'), LOWER('LY'))
 AND re.day_date = re.week_end_day_date;

-- select distinct month_idnt from date_lkup_1
CREATE TEMPORARY TABLE IF NOT EXISTS supp_attr
AS
SELECT dept_num,
 supplier_num,
 preferred_partner_desc AS supplier_attribute,
  CASE
  WHEN LOWER(banner) = LOWER('FP')
  THEN 'N'
  ELSE 'NR'
  END AS banner
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.supp_dept_map_dim AS supp;

--COLLECT STATS PRIMARY INDEX (dept_num, supplier_num, banner) , COLUMN (Supplier_Attribute) ON SUPP_ATTR;

CREATE TEMPORARY TABLE IF NOT EXISTS sku_lkup_1
AS
SELECT p.rms_sku_num,
   TRIM(FORMAT('%11d', p.div_num)) || ', ' || p.div_desc AS division,
   TRIM(FORMAT('%11d', p.grp_num)) || ', ' || p.grp_desc AS subdivision,
 SUBSTR(CAST(p.dept_num AS STRING), 1, 10) AS dept_idnt,
   TRIM(FORMAT('%11d', p.dept_num)) || ', ' || p.dept_desc AS dept_label,
   TRIM(FORMAT('%11d', p.class_num)) || ', ' || p.class_desc AS class_label,
   TRIM(FORMAT('%11d', p.sbclass_num)) || ', ' || p.sbclass_desc AS sbclass_label,
 p.prmy_supp_num AS supp_idnt,
 v.vendor_name AS supplier,
 p.brand_name AS brand,
  CASE
  WHEN LOWER(p.npg_ind) = LOWER('N')
  THEN 0
  WHEN LOWER(p.npg_ind) = LOWER('Y')
  THEN 1
  ELSE NULL
  END AS npg_ind,
 c2.ccs_subcategory,
 COALESCE(map1.category, map2.category, 'OTHER') AS quantrix_category,
 th.merch_themes,
 si.size_1_clean,
 si.left_core_right,
 si.overs_unders,
 si.widths,
 si.wide_calf
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS p
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.item_supplier_size_dim AS sd ON LOWER(p.rms_sku_num) = LOWER(sd.rms_sku_id)
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_ccs_categories.ccs_subcategory_sbclass_map AS c1 ON p.dept_num = CAST(c1.dept_idnt AS FLOAT64) AND p
    .class_num = CAST(c1.class_idnt AS FLOAT64) AND p.sbclass_num = CAST(c1.sbclass_idnt AS FLOAT64)
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_ccs_categories.ccs_subcategories AS c2 ON LOWER(c1.ccs_subcategory) = LOWER(c2.ccs_subcategory)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS v ON LOWER(p.prmy_supp_num) = LOWER(v.vendor_num)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.catg_subclass_map_dim AS map1 ON p.dept_num = map1.dept_num 
 AND p.class_num = CAST(TRUNC(CAST(CASE
      WHEN map1.class_num = ''
      THEN '0'
      ELSE map1.class_num
      END AS FLOAT64)) AS INTEGER) AND p.sbclass_num = CAST(TRUNC(CAST(CASE
     WHEN map1.sbclass_num = ''
     THEN '0'
     ELSE map1.sbclass_num
     END AS FLOAT64)) AS INTEGER)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.catg_subclass_map_dim AS map2 ON p.dept_num = map2.dept_num 
 AND p.class_num = CAST(TRUNC(CAST(CASE
      WHEN map2.class_num = ''
      THEN '0'
      ELSE map2.class_num
      END as FLOAT64)) AS INTEGER) AND LOWER(map2.sbclass_num) = LOWER('-1')
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_ccs_categories.merch_themes_map AS th ON p.dept_num = CAST(th.dept_idnt AS FLOAT64) AND p.class_num
    = CAST(th.class_idnt AS FLOAT64) AND p.sbclass_num = CAST(th.sbclass_idnt AS FLOAT64)
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_ccs_categories.shoe_size_map AS si ON COALESCE(p.dept_num, 0) = CAST(TRUNC(CAST(CASE
        WHEN COALESCE(si.dept_idnt, FORMAT('%4d', 0)) = ''
        THEN '0'
        ELSE COALESCE(si.dept_idnt, FORMAT('%4d', 0))
        END as FLOAT64)) AS INTEGER) 
        AND LOWER(COALESCE(sd.supplier_size_1, 'null')) = LOWER(COALESCE(si.size_1_idnt, 'null')) AND
     LOWER(COALESCE(CASE
        WHEN LOWER(sd.supplier_size_1_desc) = LOWER(' ')
        THEN 'null'
        ELSE sd.supplier_size_1_desc
        END, 'null')) = LOWER(COALESCE(si.size_1_desc, 'null')) AND LOWER(COALESCE(sd.supplier_size_2, 'null')) = LOWER(COALESCE(si
      .size_2_idnt, 'null')) AND LOWER(COALESCE(CASE
      WHEN LOWER(sd.supplier_size_2_desc) = LOWER(' ')
      THEN 'null'
      ELSE sd.supplier_size_2_desc
      END, 'null')) = LOWER(COALESCE(si.size_2_desc, 'null'))
WHERE p.dept_num IN (804, 805, 806, 807, 800, 801, 802, 803, 835, 836)
 AND LOWER(p.channel_country) = LOWER('US')
 AND LOWER(si.size_1_clean) IN (LOWER('4'), LOWER('4.5'), LOWER('5'), LOWER('5.5'), LOWER('6'), LOWER('6.5'), LOWER('7'
    ), LOWER('7.5'), LOWER('8'), LOWER('8.5'), LOWER('9'), LOWER('9.5'), LOWER('10'), LOWER('10.5'), LOWER('11'), LOWER('11.5'
    ), LOWER('12'), LOWER('12.5'), LOWER('13'), LOWER('13.5'), LOWER('14'), LOWER('14.5'), LOWER('15'), LOWER('15.5'),
   LOWER('16'));

--COLLECT STATS PRIMARY INDEX (rms_sku_num) ,COLUMN (rms_sku_num) ,COLUMN (SIZE_1_CLEAN) ,COLUMN (rms_sku_num,SIZE_1_CLEAN) ,COLUMN (DEPT_IDNT,SUPP_IDNT)  ON sku_lkup_1;

CREATE TEMPORARY TABLE IF NOT EXISTS store_lkup_1
AS
SELECT DISTINCT store_num AS loc_idnt,
 TRIM(FORMAT('%11d', store_num) || ', ' || store_name) AS loc_label,
 channel_num AS chnl_idnt,
   TRIM(FORMAT('%11d', channel_num)) || ', ' || channel_desc AS channel,
 store_address_county,
 TRIM(FORMAT('%11d', region_num) || ', ' || region_desc) AS region_label,
 channel_country AS country,
  CASE
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM')
  THEN 'N'
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
  THEN 'NR'
  ELSE NULL
  END AS banner
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw
WHERE LOWER(store_country_code) = LOWER('US')
 AND channel_num IN (110, 111, 120, 121, 130, 140, 210, 211, 220, 221, 250, 260, 261, 310, 311)
 AND LOWER(store_abbrev_name) <> LOWER('CLOSED')
 AND channel_num IS NOT NULL;

--collect statistics unique primary index (LOC_IDNT)  on store_lkup_1;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.shoe_size_performance_monthly_rollup;

INSERT INTO `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.shoe_size_performance_monthly_rollup
(SELECT dt.ty_ly_ind,
  dt.month_end_date,
  dt.quarter_abrv,
  dt.quarter_label,
  dt.month_idnt,
  dt.month_abrv,
  dt.month_label,
  dt.year_idnt,
  b.rp_idnt,
  s.division,
  s.dept_label,
  s.class_label,
  s.subdivision,
  l.banner,
  l.chnl_idnt,
  l.channel,
  l.loc_idnt,
  l.loc_label,
  l.store_address_county,
  l.region_label,
  s.ccs_subcategory,
  s.quantrix_category,
  s.merch_themes,
  s.supplier,
  s.brand,
  sa.supplier_attribute,
  s.npg_ind,
  b.price_type,
  b.ds_ind AS dropship_ind,
  s.size_1_clean,
  s.left_core_right,
  s.overs_unders,
  s.widths,
  s.wide_calf,
  SUM(b.sales_dollars),
  SUM(b.sales_cost),
  SUM(b.sales_units),
  SUM(b.return_dollars),
  SUM(b.return_cost),
  SUM(b.return_units),
  SUM(b.demand_dollars),
  SUM(b.demand_units),
  CAST(SUM(CASE
     WHEN dt.week_end_day_date = dt.month_end_date
     THEN b.eoh_dollars
     ELSE NULL
     END) AS NUMERIC) AS eoh_dollars,
  CAST(SUM(CASE
     WHEN dt.week_end_day_date = dt.month_end_date
     THEN b.eoh_cost
     ELSE NULL
     END) AS NUMERIC) AS eoh_cost,
  CAST(SUM(CASE
     WHEN dt.week_end_day_date = dt.month_end_date
     THEN b.eoh_units
     ELSE NULL
     END) AS NUMERIC) AS eoh_units,
  SUM(b.receipt_dollars),
  SUM(b.receipt_units),
  SUM(b.receipt_cost),
  SUM(b.transfer_pah_dollars),
  SUM(b.transfer_pah_units),
  SUM(b.transfer_pah_cost),
  SUM(b.oo_dollars),
  SUM(b.oo_units),
  SUM(b.oo_cost),
  SUM(b.oo_4weeks_dollars),
  SUM(b.oo_4weeks_units),
  SUM(b.oo_4weeks_cost),
  SUM(b.oo_12weeks_dollars),
  SUM(b.oo_12weeks_units),
  SUM(b.oo_12weeks_cost),
  SUM(b.net_sales_tot_retl_with_cost),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.shoe_size_performance_monthly_base AS b
  INNER JOIN date_lkup_1 AS dt ON b.week_num = dt.week_idnt_true
  INNER JOIN sku_lkup_1 AS s ON LOWER(b.rms_sku_num) = LOWER(s.rms_sku_num)
  INNER JOIN store_lkup_1 AS l ON b.store_num = l.loc_idnt
  LEFT JOIN supp_attr AS sa ON LOWER(s.dept_idnt) = LOWER(sa.dept_num) AND LOWER(s.supp_idnt) = LOWER(sa.supplier_num)
   AND LOWER(l.banner) = LOWER(sa.banner)
 GROUP BY dt.ty_ly_ind,
  dt.month_end_date,
  dt.quarter_abrv,
  dt.quarter_label,
  dt.month_idnt,
  dt.month_abrv,
  dt.month_label,
  dt.year_idnt,
  b.rp_idnt,
  s.division,
  s.dept_label,
  s.class_label,
  s.subdivision,
  l.banner,
  l.chnl_idnt,
  l.channel,
  l.loc_idnt,
  l.loc_label,
  l.store_address_county,
  l.region_label,
  s.ccs_subcategory,
  s.quantrix_category,
  s.merch_themes,
  s.supplier,
  s.brand,
  sa.supplier_attribute,
  s.npg_ind,
  b.price_type,
  dropship_ind,
  s.size_1_clean,
  s.left_core_right,
  s.overs_unders,
  s.widths,
  s.wide_calf);

--COLLECT STATISTICS  COLUMN (PARTITION), COLUMN (month_idnt) on `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.shoe_size_performance_monthly_rollup;
/*SET QUERY_BAND = NONE FOR SESSION;*/
