/*
APT In Season Category Main
Author: Asiyah Fox
Date Created: 1/5/23
Date Last Updated: 6/8/23

Datalab: t2dl_das_apt_cost_reporting
Deletes and Inserts into Table: apt_is_category
*/

/************************************************************************************/
/****************************** 1.DATE LOOKUPS **************************************/
/************************************************************************************/
--purposely includes overlap of LY MTD, LY MTH, and TY for one month--needed for Tableau


CREATE TEMPORARY TABLE IF NOT EXISTS date_lkup
AS
SELECT DISTINCT dt.date_ind,
 dt.week_end_day_date,
 dt.week_idnt AS week_idnt_true,
  CASE
  WHEN LOWER(dt.date_ind) IN (LOWER('LY'), LOWER('LY MTH'))
  THEN ly.week_idnt - 100
  ELSE dt.week_idnt
  END AS week_idnt,
 ty.month_idnt AS month_idnt_true,
  CASE
  WHEN LOWER(dt.date_ind) IN (LOWER('LY'), LOWER('LY MTH'))
  THEN ly.month_idnt - 100
  ELSE ty.month_idnt
  END AS month_idnt,
  CASE
  WHEN LOWER(dt.date_ind) IN (LOWER('LY'), LOWER('LY MTH'))
  THEN ly.month_idnt
  ELSE ty.month_idnt
  END AS month_idnt_aligned,
 ty.month_label AS spend_month_true,
  CASE
  WHEN LOWER(dt.date_ind) IN (LOWER('LY'), LOWER('LY MTH'))
  THEN TRIM(FORMAT('%11d', ly.fiscal_year_num - 1)) || ' ' || TRIM(ly.month_abrv)
  ELSE ty.month_label
  END AS spend_month,
     TRIM(FORMAT('%11d', ty.fiscal_year_num)) || ' ' || TRIM(FORMAT('%11d', ty.fiscal_month_num)) || ' ' || TRIM(ty.month_abrv
   ) AS month_label_true,
  CASE
  WHEN LOWER(dt.date_ind) IN (LOWER('LY'), LOWER('LY MTH'))
  THEN TRIM(FORMAT('%11d', ly.fiscal_year_num - 1)) || ' ' || TRIM(FORMAT('%11d', ly.fiscal_month_num)) || ' ' || TRIM(ly
    .month_abrv)
  ELSE TRIM(FORMAT('%11d', ty.fiscal_year_num)) || ' ' || TRIM(FORMAT('%11d', ty.fiscal_month_num)) || ' ' || TRIM(ty.month_abrv
    )
  END AS month_label,
  CASE
  WHEN LOWER(dt.date_ind) IN (LOWER('LY'), LOWER('LY MTH'))
  THEN TRIM(FORMAT('%11d', ly.fiscal_year_num)) || ' ' || TRIM(FORMAT('%11d', ly.fiscal_month_num)) || ' ' || TRIM(ly.month_abrv
    )
  ELSE TRIM(FORMAT('%11d', ty.fiscal_year_num)) || ' ' || TRIM(FORMAT('%11d', ty.fiscal_month_num)) || ' ' || TRIM(ty.month_abrv
    )
  END AS month_label_aligned,
 ty.month_start_day_date AS month_start_day_date_true,
  CASE
  WHEN LOWER(dt.date_ind) IN (LOWER('LY'), LOWER('LY MTH'))
  THEN MIN(dt.week_start_day_date) OVER (PARTITION BY dt.date_ind, ly.month_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND
   UNBOUNDED FOLLOWING)
  ELSE ty.month_start_day_date
  END AS month_start_day_date,
 ty.month_end_day_date AS month_end_day_date_true,
  CASE
  WHEN LOWER(dt.date_ind) IN (LOWER('LY'), LOWER('LY MTH'))
  THEN MAX(dt.week_end_day_date) OVER (PARTITION BY dt.date_ind, ly.month_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND
   UNBOUNDED FOLLOWING)
  ELSE ty.month_end_day_date
  END AS month_end_day_date,
 MAX(ty.week_end_day_date) OVER (PARTITION BY dt.date_ind, ty.month_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND
  UNBOUNDED FOLLOWING) AS mtd_end_date_true,
  CASE
  WHEN LOWER(dt.date_ind) IN (LOWER('LY'), LOWER('LY MTH'))
  THEN MAX(dt.week_end_day_date) OVER (PARTITION BY dt.date_ind, ly.month_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND
   UNBOUNDED FOLLOWING)
  ELSE MAX(ty.week_end_day_date) OVER (PARTITION BY dt.date_ind, ty.month_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND
   UNBOUNDED FOLLOWING)
  END AS mtd_end_date,
 MAX(ty.week_idnt) OVER (PARTITION BY dt.date_ind, CASE
     WHEN LOWER(dt.date_ind) IN (LOWER('LY'), LOWER('LY MTH'))
     THEN ly.month_idnt
     ELSE ty.month_idnt
     END RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS mtd_week_idnt_true,
  CASE
  WHEN LOWER(dt.date_ind) IN (LOWER('LY'), LOWER('LY MTH'))
  THEN MAX(ly.week_idnt - 100) OVER (PARTITION BY dt.date_ind, CASE
      WHEN LOWER(dt.date_ind) IN (LOWER('LY'), LOWER('LY MTH'))
      THEN ly.month_idnt
      ELSE ty.month_idnt
      END RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
  ELSE MAX(ty.week_idnt) OVER (PARTITION BY dt.date_ind, CASE
      WHEN LOWER(dt.date_ind) IN (LOWER('LY'), LOWER('LY MTH'))
      THEN ly.month_idnt
      ELSE ty.month_idnt
      END RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
  END AS mtd_week_idnt,
 MIN(ty.week_idnt) OVER (PARTITION BY dt.date_ind, CASE
     WHEN LOWER(dt.date_ind) IN (LOWER('LY'), LOWER('LY MTH'))
     THEN ly.month_idnt
     ELSE ty.month_idnt
     END RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS mbeg_week_idnt_true,
  CASE
  WHEN LOWER(dt.date_ind) IN (LOWER('LY'), LOWER('LY MTH'))
  THEN MIN(ly.week_idnt - 100) OVER (PARTITION BY dt.date_ind, CASE
      WHEN LOWER(dt.date_ind) IN (LOWER('LY'), LOWER('LY MTH'))
      THEN ly.month_idnt
      ELSE ty.month_idnt
      END RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
  ELSE MIN(ty.week_idnt) OVER (PARTITION BY dt.date_ind, CASE
      WHEN LOWER(dt.date_ind) IN (LOWER('LY'), LOWER('LY MTH'))
      THEN ly.month_idnt
      ELSE ty.month_idnt
      END RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
  END AS mbeg_week_idnt
FROM (SELECT DISTINCT 'PL MTH' AS date_ind,
    week_idnt,
    week_end_day_date,
    week_start_day_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE month_idnt BETWEEN (SELECT MAX(month_idnt) - 100
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
      WHERE week_end_day_date < CURRENT_DATE('PST8PDT')) AND (SELECT DISTINCT month_idnt + 100
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
      WHERE day_date = (SELECT DATE_SUB(MIN(day_date), INTERVAL 1 DAY)
         FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
         WHERE month_idnt = (SELECT MAX(month_idnt)
            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
            WHERE week_end_day_date < CURRENT_DATE('PST8PDT'))))
    AND month_idnt >= 202211
   UNION ALL
   SELECT DISTINCT 'PL MTD' AS date_ind,
    week_idnt,
    week_end_day_date,
    week_start_day_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE week_idnt BETWEEN (SELECT MIN(week_idnt)
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
      WHERE month_idnt = (SELECT MAX(month_idnt)
         FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
         WHERE week_end_day_date < CURRENT_DATE('PST8PDT'))) AND (SELECT MAX(week_idnt)
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
      WHERE week_end_day_date < CURRENT_DATE('PST8PDT'))
   UNION ALL
   SELECT DISTINCT 'TY' AS date_ind,
    week_idnt,
    week_end_day_date,
    week_start_day_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE week_idnt BETWEEN (SELECT MIN(week_idnt)
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
      WHERE month_idnt = (SELECT MAX(month_idnt) - 100
         FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
         WHERE week_end_day_date < CURRENT_DATE('PST8PDT'))) AND (SELECT MAX(week_idnt)
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
      WHERE week_end_day_date < CURRENT_DATE('PST8PDT'))
   UNION ALL
   SELECT DISTINCT 'LY' AS date_ind,
    week_idnt,
    week_end_day_date,
    week_start_day_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_date BETWEEN (SELECT MIN(day_date_last_year_realigned)
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
      WHERE month_idnt = (SELECT MAX(month_idnt) - 100
         FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
         WHERE week_end_day_date < CURRENT_DATE('PST8PDT'))) AND (SELECT DISTINCT day_date_last_year_realigned
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
      WHERE day_date = (SELECT MAX(week_end_day_date)
         FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
         WHERE week_end_day_date < CURRENT_DATE('PST8PDT')))
   UNION ALL
   SELECT DISTINCT 'LY MTH' AS date_ind,
    week_idnt,
    week_end_day_date,
    week_start_day_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE week_idnt BETWEEN (SELECT DISTINCT week_idnt
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
      WHERE day_date = (SELECT DISTINCT day_date_last_year_realigned
         FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
         WHERE day_date = (SELECT MAX(month_start_day_date)
            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
            WHERE week_end_day_date < CURRENT_DATE('PST8PDT')))) AND (SELECT DISTINCT week_idnt
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
      WHERE day_date = (SELECT MAX(month_end_day_date)
         FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
         WHERE week_end_day_date < CURRENT_DATE('PST8PDT')))) AS dt
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS ty ON dt.week_end_day_date = ty.day_date
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS ly ON ty.day_date = ly.day_date_last_year_realigned;



CREATE TEMPORARY TABLE IF NOT EXISTS date_lkup_acts
AS
SELECT *
FROM date_lkup
WHERE LOWER(date_ind) IN (LOWER('TY'), LOWER('LY'), LOWER('LY MTH'));


CREATE TEMPORARY TABLE IF NOT EXISTS date_lkup_acts_ty
AS
SELECT *
FROM date_lkup
WHERE LOWER(date_ind) IN (LOWER('TY'));


CREATE TEMPORARY TABLE IF NOT EXISTS date_lkup_acts_ly
AS
SELECT *
FROM date_lkup
WHERE LOWER(date_ind) IN (LOWER('LY'), LOWER('LY MTH'));


CREATE TEMPORARY TABLE IF NOT EXISTS dept_lkup
AS
SELECT DISTINCT dept_num,
 division_num,
 subdivision_num,
 TRIM(FORMAT('%11d', dept_num) || ', ' || dept_name) AS department_desc,
 TRIM(FORMAT('%11d', division_num) || ', ' || division_name) AS division_desc,
 TRIM(FORMAT('%11d', subdivision_num) || ', ' || subdivision_name) AS subdivision_desc,
 active_store_ind
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim;



CREATE TEMPORARY TABLE IF NOT EXISTS cat_lkup
AS
SELECT DISTINCT psd.dept_num,
 psd.rms_sku_num,
 psd.channel_country,
 COALESCE(cat1.category, cat2.category, 'OTHER') AS category,
 COALESCE(cat1.category_planner_1, cat2.category_planner_1, 'OTHER') AS category_planner_1,
 COALESCE(cat1.category_planner_2, cat2.category_planner_2, 'OTHER') AS category_planner_2,
 COALESCE(cat1.category_group, cat2.category_group, 'OTHER') AS category_group,
 COALESCE(cat1.seasonal_designation, cat2.seasonal_designation, 'OTHER') AS seasonal_designation,
 COALESCE(cat1.rack_merch_zone, cat2.rack_merch_zone, 'OTHER') AS rack_merch_zone,
 COALESCE(cat1.is_activewear, cat2.is_activewear, 'OTHER') AS is_activewear,
 COALESCE(cat1.channel_category_roles_1, cat2.channel_category_roles_1, 'OTHER') AS channel_category_roles_1,
 COALESCE(cat1.channel_category_roles_2, cat2.channel_category_roles_2, 'OTHER') AS channel_category_roles_2,
 COALESCE(cat1.bargainista_dept_map, cat2.bargainista_dept_map, 'OTHER') AS bargainista_dept_map
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS psd
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.catg_subclass_map_dim AS cat1 ON psd.dept_num = cat1.dept_num AND psd.class_num = CAST(cat1.class_num AS FLOAT64)
     AND psd.sbclass_num = CAST(cat1.sbclass_num AS FLOAT64)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.catg_subclass_map_dim AS cat2 ON psd.dept_num = cat2.dept_num AND psd.class_num = CAST(cat2.class_num AS FLOAT64)
     AND CAST(cat2.sbclass_num AS FLOAT64) = - 1;


CREATE TEMPORARY TABLE IF NOT EXISTS cat_attr
AS
SELECT DISTINCT category,
 dept_num,
 category_planner_1,
 category_planner_2,
 category_group,
 seasonal_designation,
 rack_merch_zone,
 is_activewear,
 channel_category_roles_1,
 channel_category_roles_2,
 bargainista_dept_map
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.catg_subclass_map_dim;


CREATE TEMPORARY TABLE IF NOT EXISTS store_lkup
AS
SELECT DISTINCT store_num,
 channel_country,
 channel_num,
 TRIM(FORMAT('%11d', channel_num) || ', ' || channel_desc) AS channel_label,
 channel_brand AS banner
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw
WHERE channel_num NOT IN (580);



CREATE TEMPORARY TABLE IF NOT EXISTS store_lkup_eop
AS
SELECT *
FROM store_lkup
WHERE channel_num IN (110, 111, 120, 121, 210, 211, 250, 260, 261, 310, 311);



CREATE TEMPORARY TABLE IF NOT EXISTS store_lkup_nord
AS
SELECT DISTINCT *
FROM store_lkup
WHERE LOWER(banner) = LOWER('NORDSTROM');


CREATE TEMPORARY TABLE IF NOT EXISTS store_lkup_rack
AS
SELECT DISTINCT *
FROM store_lkup
WHERE LOWER(banner) = LOWER('NORDSTROM_RACK');


CREATE TEMPORARY TABLE IF NOT EXISTS receipt_price_band_nord
AS
SELECT a.rms_sku_num AS sku_idnt,
 b.channel_country,
 MAX((a.receipts_regular_retail + a.receipts_clearance_retail + a.receipts_crossdock_regular_retail + a.receipts_crossdock_clearance_retail
     ) / (a.receipts_regular_units + a.receipts_clearance_units + a.receipts_crossdock_regular_units + a.receipts_crossdock_clearance_units
     )) AS aur
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_poreceipt_sku_store_week_fact_vw AS a
 INNER JOIN store_lkup_nord AS b ON a.store_num = b.store_num
 INNER JOIN dept_lkup AS dept ON a.department_num = dept.dept_num
WHERE a.receipts_regular_units + a.receipts_clearance_units + a.receipts_crossdock_regular_units + a.receipts_crossdock_clearance_units
    > 0
GROUP BY sku_idnt,
 b.channel_country;


CREATE TEMPORARY TABLE IF NOT EXISTS receipt_price_band_rack
AS
SELECT sku_idnt,
 channel_country,
 MAX(aur) AS aur
FROM (SELECT a.rms_sku_num AS sku_idnt,
    b.channel_country,
    MAX((a.receipts_regular_retail + a.receipts_clearance_retail + a.receipts_crossdock_regular_retail + a.receipts_crossdock_clearance_retail
        ) / (a.receipts_regular_units + a.receipts_clearance_units + a.receipts_crossdock_regular_units + a.receipts_crossdock_clearance_units
        )) AS aur
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_poreceipt_sku_store_week_fact_vw AS a
    INNER JOIN store_lkup_rack AS b ON a.store_num = b.store_num
    INNER JOIN dept_lkup AS dept ON a.department_num = dept.dept_num
   WHERE a.receipts_regular_units + a.receipts_clearance_units + a.receipts_crossdock_regular_units + a.receipts_crossdock_clearance_units
       > 0
   GROUP BY sku_idnt,
    b.channel_country
   UNION ALL
   SELECT tsf.rms_sku_num AS sku_idnt,
    b0.channel_country,
    MAX(tsf.packandhold_transfer_in_retail / tsf.packandhold_transfer_in_units) AS aur
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_transfer_sku_loc_week_agg_fact_vw AS tsf
    INNER JOIN store_lkup_rack AS b0 ON tsf.store_num = b0.store_num
    INNER JOIN dept_lkup AS dept0 ON tsf.department_num = dept0.dept_num
   WHERE tsf.packandhold_transfer_in_units > 0
   GROUP BY sku_idnt,
    b0.channel_country
   UNION ALL
   SELECT tsf0.rms_sku_num AS sku_idnt,
    b1.channel_country,
    MAX(tsf0.racking_transfer_in_retail / tsf0.racking_transfer_in_units) AS aur
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_transfer_sku_loc_week_agg_fact_vw AS tsf0
    INNER JOIN store_lkup_rack AS b1 ON tsf0.store_num = b1.store_num
    INNER JOIN dept_lkup AS dept1 ON tsf0.department_num = dept1.dept_num
   WHERE tsf0.racking_transfer_in_units > 0
   GROUP BY sku_idnt,
    b1.channel_country) AS TBL
GROUP BY sku_idnt,
 channel_country;


CREATE TEMPORARY TABLE IF NOT EXISTS plan
AS
SELECT fct.country AS channel_country,
 fct.banner,
 dt.date_ind,
 dt.month_idnt,
 dt.month_label,
 dt.month_idnt_aligned,
 dt.month_label_aligned,
 dt.month_start_day_date,
 dt.month_end_day_date,
 CAST(TRUNC(CAST(fct.dept_idnt AS FLOAT64)) AS INTEGER) AS department_num,
  CASE
  WHEN fct.fulfill_type_num = 1
  THEN 'Y'
  ELSE 'N'
  END AS dropship_ind,
 fct.category,
 fct.price_band,
 0 AS net_sls_r,
 0 AS net_sls_c,
 0 AS net_sls_units,
 0 AS net_sls_reg_r,
 0 AS net_sls_reg_c,
 0 AS net_sls_reg_units,
 0 AS returns_r,
 0 AS returns_c,
 0 AS returns_u,
 0 AS demand_ttl_r,
 0 AS demand_ttl_units,
 0 AS eop_ttl_c,
 0 AS eop_ttl_units,
 0 AS bop_ttl_c,
 0 AS bop_ttl_units,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_u,
 0 AS ttl_porcpt_rp_c,
 0 AS ttl_porcpt_rp_u,
 0 AS ttl_porcpt_nrp_c,
 0 AS ttl_porcpt_nrp_u,
 0 AS pah_tsfr_in_c,
 0 AS pah_tsfr_in_u,
 0 AS pah_tsfr_out_c,
 0 AS pah_tsfr_out_u,
 0 AS rk_tsfr_in_c,
 0 AS rk_tsfr_in_u,
 0 AS rk_tsfr_out_c,
 0 AS rk_tsfr_out_u,
 0 AS rs_tsfr_in_c,
 0 AS rs_tsfr_in_u,
 0 AS rs_tsfr_out_c,
 0 AS rs_tsfr_out_u,
 SUM(CASE
   WHEN dt.week_idnt = dt.mbeg_week_idnt
   THEN fct.plan_bop_c_dollars
   ELSE 0
   END) AS plan_bop_c,
 SUM(CASE
   WHEN dt.week_idnt = dt.mbeg_week_idnt
   THEN fct.plan_bop_c_units
   ELSE 0
   END) AS plan_bop_u,
 SUM(CASE
   WHEN dt.week_end_day_date = dt.mtd_end_date
   THEN fct.plan_eop_c_dollars
   ELSE 0
   END) AS plan_eop_c,
 SUM(CASE
   WHEN dt.week_end_day_date = dt.mtd_end_date
   THEN fct.plan_eop_c_units
   ELSE 0
   END) AS plan_eop_u,
 SUM(fct.rcpt_need_c) AS plan_receipts_c,
 SUM(fct.rcpt_need_u) AS plan_receipts_u,
 SUM(fct.rcpt_need_lr_c) AS plan_receipts_lr_c,
 SUM(fct.rcpt_need_lr_u) AS plan_receipts_lr_u,
 SUM(fct.net_sls_c_dollars) AS plan_sales_c,
 SUM(fct.net_sls_r_dollars) AS plan_sales_r,
 SUM(fct.net_sls_units) AS plan_sales_u,
 SUM(fct.demand_r_dollars) AS plan_demand_r,
 SUM(fct.demand_units) AS plan_demand_u
FROM `{{params.gcp_project_id}}`.t2dl_das_apt_cost_reporting.category_channel_cost_plans_weekly AS fct
 INNER JOIN date_lkup AS dt ON fct.week_idnt = dt.week_idnt AND LOWER(dt.date_ind) IN (LOWER('PL MTD'), LOWER('PL MTH')
    )
GROUP BY channel_country,
 fct.banner,
 dt.date_ind,
 dt.month_idnt,
 dt.month_label,
 dt.month_idnt_aligned,
 dt.month_label_aligned,
 dt.month_start_day_date,
 dt.month_end_day_date,
 department_num,
 dropship_ind,
 fct.category,
 fct.price_band,
 net_sls_r,
 net_sls_c,
 net_sls_units,
 net_sls_reg_r,
 net_sls_reg_c,
 net_sls_reg_units,
 returns_r,
 returns_c,
 returns_u,
 demand_ttl_r,
 demand_ttl_units,
 eop_ttl_c,
 eop_ttl_units;



CREATE TEMPORARY TABLE IF NOT EXISTS sales
AS
SELECT st.channel_country,
 st.banner,
 dt.date_ind,
 dt.month_idnt,
 dt.month_label,
 dt.month_idnt_aligned,
 dt.month_label_aligned,
 dt.month_start_day_date,
 dt.month_end_day_date,
 fct.department_num,
 fct.dropship_ind,
 cat.category,
  CASE
  WHEN pb.aur <= 0.01
  THEN 'NO RECEIPT FOUND'
  WHEN pb.aur <= 25
  THEN '$0-25'
  WHEN pb.aur <= 50
  THEN '$25-50'
  WHEN pb.aur <= 100
  THEN '$50-100'
  WHEN pb.aur <= 150
  THEN '$100-150'
  WHEN pb.aur <= 200
  THEN '$150-200'
  WHEN pb.aur <= 300
  THEN '$200-300'
  WHEN pb.aur <= 500
  THEN '$300-500'
  WHEN pb.aur <= 700
  THEN '$500-700'
  WHEN pb.aur <= 1000
  THEN '$700-1000'
  WHEN pb.aur <= 1500
  THEN '$1000-1500'
  WHEN pb.aur <= 1000000000
  THEN 'Over $1500'
  ELSE 'NO RECEIPT FOUND'
  END AS price_band,
 SUM(fct.net_sales_tot_retl) AS net_sls_r,
 SUM(fct.net_sales_tot_cost) AS net_sls_c,
 SUM(fct.net_sales_tot_units) AS net_sls_units,
 SUM(fct.net_sales_tot_regular_retl) AS net_sls_reg_r,
 SUM(fct.net_sales_tot_regular_cost) AS net_sls_reg_c,
 SUM(fct.net_sales_tot_regular_units) AS net_sls_reg_units,
 SUM(COALESCE(fct.gross_sales_tot_retl, 0) - COALESCE(fct.net_sales_tot_retl, 0)) AS returns_r,
 SUM(COALESCE(fct.gross_sales_tot_cost, 0) - COALESCE(fct.net_sales_tot_cost, 0)) AS returns_c,
 SUM(COALESCE(fct.gross_sales_tot_units, 0) - COALESCE(fct.net_sales_tot_units, 0)) AS returns_u,
 SUM(CASE
   WHEN st.channel_num IN (110, 111)
   THEN fct.gross_sales_tot_retl
   ELSE 0
   END) AS demand_ttl_r,
 SUM(CASE
   WHEN st.channel_num IN (110, 111)
   THEN fct.gross_sales_tot_units
   ELSE 0
   END) AS demand_ttl_units,
 0 AS eop_ttl_c,
 0 AS eop_ttl_units,
 0 AS bop_ttl_c,
 0 AS bop_ttl_units,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_u,
 0 AS ttl_porcpt_rp_c,
 0 AS ttl_porcpt_rp_u,
 0 AS ttl_porcpt_nrp_c,
 0 AS ttl_porcpt_nrp_u,
 0 AS pah_tsfr_in_c,
 0 AS pah_tsfr_in_u,
 0 AS pah_tsfr_out_c,
 0 AS pah_tsfr_out_u,
 0 AS rk_tsfr_in_c,
 0 AS rk_tsfr_in_u,
 0 AS rk_tsfr_out_c,
 0 AS rk_tsfr_out_u,
 0 AS rs_tsfr_in_c,
 0 AS rs_tsfr_in_u,
 0 AS rs_tsfr_out_c,
 0 AS rs_tsfr_out_u,
 0 AS plan_bop_c,
 0 AS plan_bop_u,
 0 AS plan_eop_c,
 0 AS plan_eop_u,
 0 AS plan_receipts_c,
 0 AS plan_receipts_u,
 0 AS plan_receipts_lr_c,
 0 AS plan_receipts_lr_u,
 0 AS plan_sales_c,
 0 AS plan_sales_r,
 0 AS plan_sales_u,
 0 AS plan_demand_r,
 0 AS plan_demand_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_sale_return_sku_store_week_fact_vw AS fct
 INNER JOIN date_lkup_acts AS dt ON fct.week_num = dt.week_idnt_true
 INNER JOIN store_lkup_nord AS st ON fct.store_num = st.store_num
 LEFT JOIN cat_lkup AS cat ON LOWER(fct.rms_sku_num) = LOWER(cat.rms_sku_num) AND fct.department_num = cat.dept_num AND
   LOWER(st.channel_country) = LOWER(cat.channel_country)
 LEFT JOIN receipt_price_band_nord AS pb ON LOWER(fct.rms_sku_num) = LOWER(pb.sku_idnt) AND LOWER(st.channel_country) =
   LOWER(pb.channel_country)
GROUP BY st.channel_country,
 st.banner,
 dt.date_ind,
 dt.month_idnt,
 dt.month_label,
 dt.month_idnt_aligned,
 dt.month_label_aligned,
 dt.month_start_day_date,
 dt.month_end_day_date,
 fct.department_num,
 fct.dropship_ind,
 cat.category,
 price_band
UNION ALL
SELECT st0.channel_country,
 st0.banner,
 dt0.date_ind,
 dt0.month_idnt,
 dt0.month_label,
 dt0.month_idnt_aligned,
 dt0.month_label_aligned,
 dt0.month_start_day_date,
 dt0.month_end_day_date,
 fct0.department_num,
 fct0.dropship_ind,
 cat0.category,
  CASE
  WHEN pb0.aur <= 0.01
  THEN 'NO RECEIPT FOUND'
  WHEN pb0.aur <= 10
  THEN '$0-10'
  WHEN pb0.aur <= 15
  THEN '$10-15'
  WHEN pb0.aur <= 20
  THEN '$15-20'
  WHEN pb0.aur <= 25
  THEN '$20-25'
  WHEN pb0.aur <= 30
  THEN '$25-30'
  WHEN pb0.aur <= 40
  THEN '$30-40'
  WHEN pb0.aur <= 50
  THEN '$40-50'
  WHEN pb0.aur <= 60
  THEN '$50-60'
  WHEN pb0.aur <= 80
  THEN '$60-80'
  WHEN pb0.aur <= 100
  THEN '$80-100'
  WHEN pb0.aur <= 150
  THEN '$100-150'
  WHEN pb0.aur <= 200
  THEN '$150-200'
  WHEN pb0.aur <= 1000000000
  THEN 'Over $200'
  ELSE 'NO RECEIPT FOUND'
  END AS price_band,
 SUM(fct0.net_sales_tot_retl) AS net_sls_r,
 SUM(fct0.net_sales_tot_cost) AS net_sls_c,
 SUM(fct0.net_sales_tot_units) AS net_sls_units,
 SUM(fct0.net_sales_tot_regular_retl) AS net_sls_reg_r,
 SUM(fct0.net_sales_tot_regular_cost) AS net_sls_reg_c,
 SUM(fct0.net_sales_tot_regular_units) AS net_sls_reg_units,
 SUM(COALESCE(fct0.gross_sales_tot_retl, 0) - COALESCE(fct0.net_sales_tot_retl, 0)) AS returns_r,
 SUM(COALESCE(fct0.gross_sales_tot_cost, 0) - COALESCE(fct0.net_sales_tot_cost, 0)) AS returns_c,
 SUM(COALESCE(fct0.gross_sales_tot_units, 0) - COALESCE(fct0.net_sales_tot_units, 0)) AS returns_u,
 SUM(CASE
   WHEN st0.channel_num IN (210, 211)
   THEN fct0.gross_sales_tot_retl
   ELSE 0
   END) AS demand_ttl_r,
 SUM(CASE
   WHEN st0.channel_num IN (210, 211)
   THEN fct0.gross_sales_tot_units
   ELSE 0
   END) AS demand_ttl_units,
 0 AS eop_ttl_c,
 0 AS eop_ttl_units,
 0 AS bop_ttl_c,
 0 AS bop_ttl_units,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_u,
 0 AS ttl_porcpt_rp_c,
 0 AS ttl_porcpt_rp_u,
 0 AS ttl_porcpt_nrp_c,
 0 AS ttl_porcpt_nrp_u,
 0 AS pah_tsfr_in_c,
 0 AS pah_tsfr_in_u,
 0 AS pah_tsfr_out_c,
 0 AS pah_tsfr_out_u,
 0 AS rk_tsfr_in_c,
 0 AS rk_tsfr_in_u,
 0 AS rk_tsfr_out_c,
 0 AS rk_tsfr_out_u,
 0 AS rs_tsfr_in_c,
 0 AS rs_tsfr_in_u,
 0 AS rs_tsfr_out_c,
 0 AS rs_tsfr_out_u,
 0 AS plan_bop_c,
 0 AS plan_bop_u,
 0 AS plan_eop_c,
 0 AS plan_eop_u,
 0 AS plan_receipts_c,
 0 AS plan_receipts_u,
 0 AS plan_receipts_lr_c,
 0 AS plan_receipts_lr_u,
 0 AS plan_sales_c,
 0 AS plan_sales_r,
 0 AS plan_sales_u,
 0 AS plan_demand_r,
 0 AS plan_demand_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_sale_return_sku_store_week_fact_vw AS fct0
 INNER JOIN date_lkup_acts AS dt0 ON fct0.week_num = dt0.week_idnt_true
 INNER JOIN store_lkup_rack AS st0 ON fct0.store_num = st0.store_num
 LEFT JOIN cat_lkup AS cat0 ON LOWER(fct0.rms_sku_num) = LOWER(cat0.rms_sku_num) AND fct0.department_num = cat0.dept_num
     AND LOWER(st0.channel_country) = LOWER(cat0.channel_country)
 LEFT JOIN receipt_price_band_rack AS pb0 ON LOWER(fct0.rms_sku_num) = LOWER(pb0.sku_idnt) AND LOWER(st0.channel_country
    ) = LOWER(pb0.channel_country)
GROUP BY st0.channel_country,
 st0.banner,
 dt0.date_ind,
 dt0.month_idnt,
 dt0.month_label,
 dt0.month_idnt_aligned,
 dt0.month_label_aligned,
 dt0.month_start_day_date,
 dt0.month_end_day_date,
 fct0.department_num,
 fct0.dropship_ind,
 cat0.category,
 price_band;



CREATE TEMPORARY TABLE IF NOT EXISTS demand
AS
SELECT st.channel_country,
 st.banner,
 dt.date_ind,
 dt.month_idnt,
 dt.month_label,
 dt.month_idnt_aligned,
 dt.month_label_aligned,
 dt.month_start_day_date,
 dt.month_end_day_date,
 fct.department_num,
  CASE
  WHEN LOWER(fct.fulfill_type_code) = LOWER('DS')
  THEN 'Y'
  ELSE 'N'
  END AS dropship_ind,
 cat.category,
  CASE
  WHEN pb.aur <= 0.01
  THEN 'NO RECEIPT FOUND'
  WHEN pb.aur <= 25
  THEN '$0-25'
  WHEN pb.aur <= 50
  THEN '$25-50'
  WHEN pb.aur <= 100
  THEN '$50-100'
  WHEN pb.aur <= 150
  THEN '$100-150'
  WHEN pb.aur <= 200
  THEN '$150-200'
  WHEN pb.aur <= 300
  THEN '$200-300'
  WHEN pb.aur <= 500
  THEN '$300-500'
  WHEN pb.aur <= 700
  THEN '$500-700'
  WHEN pb.aur <= 1000
  THEN '$700-1000'
  WHEN pb.aur <= 1500
  THEN '$1000-1500'
  WHEN pb.aur <= 1000000000
  THEN 'Over $1500'
  ELSE 'NO RECEIPT FOUND'
  END AS price_band,
 0 AS net_sls_r,
 0 AS net_sls_c,
 0 AS net_sls_units,
 0 AS net_sls_reg_r,
 0 AS net_sls_reg_c,
 0 AS net_sls_reg_units,
 0 AS returns_r,
 0 AS returns_c,
 0 AS returns_u,
 SUM(CASE
   WHEN st.channel_num NOT IN (110, 111, 210, 211)
   THEN fct.demand_tot_amt
   ELSE 0
   END) AS demand_ttl_r,
 SUM(CASE
   WHEN st.channel_num NOT IN (110, 111, 210, 211)
   THEN fct.demand_tot_qty
   ELSE 0
   END) AS demand_ttl_units,
 0 AS eop_ttl_c,
 0 AS eop_ttl_units,
 0 AS bop_ttl_c,
 0 AS bop_ttl_units,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_u,
 0 AS ttl_porcpt_rp_c,
 0 AS ttl_porcpt_rp_u,
 0 AS ttl_porcpt_nrp_c,
 0 AS ttl_porcpt_nrp_u,
 0 AS pah_tsfr_in_c,
 0 AS pah_tsfr_in_u,
 0 AS pah_tsfr_out_c,
 0 AS pah_tsfr_out_u,
 0 AS rk_tsfr_in_c,
 0 AS rk_tsfr_in_u,
 0 AS rk_tsfr_out_c,
 0 AS rk_tsfr_out_u,
 0 AS rs_tsfr_in_c,
 0 AS rs_tsfr_in_u,
 0 AS rs_tsfr_out_c,
 0 AS rs_tsfr_out_u,
 0 AS plan_bop_c,
 0 AS plan_bop_u,
 0 AS plan_eop_c,
 0 AS plan_eop_u,
 0 AS plan_receipts_c,
 0 AS plan_receipts_u,
 0 AS plan_receipts_lr_c,
 0 AS plan_receipts_lr_u,
 0 AS plan_sales_c,
 0 AS plan_sales_r,
 0 AS plan_sales_u,
 0 AS plan_demand_r,
 0 AS plan_demand_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_demand_sku_store_week_agg_fact_vw AS fct
 INNER JOIN date_lkup_acts AS dt ON fct.week_num = dt.week_idnt_true
 INNER JOIN store_lkup_nord AS st ON fct.store_num = st.store_num
 LEFT JOIN cat_lkup AS cat ON LOWER(fct.rms_sku_num) = LOWER(cat.rms_sku_num) AND fct.department_num = cat.dept_num AND
   LOWER(st.channel_country) = LOWER(cat.channel_country)
 LEFT JOIN receipt_price_band_nord AS pb ON LOWER(fct.rms_sku_num) = LOWER(pb.sku_idnt) AND LOWER(st.channel_country) =
   LOWER(pb.channel_country)
GROUP BY st.channel_country,
 st.banner,
 dt.date_ind,
 dt.month_idnt,
 dt.month_label,
 dt.month_idnt_aligned,
 dt.month_label_aligned,
 dt.month_start_day_date,
 dt.month_end_day_date,
 fct.department_num,
 dropship_ind,
 cat.category,
 price_band
UNION ALL
SELECT st0.channel_country,
 st0.banner,
 dt0.date_ind,
 dt0.month_idnt,
 dt0.month_label,
 dt0.month_idnt_aligned,
 dt0.month_label_aligned,
 dt0.month_start_day_date,
 dt0.month_end_day_date,
 fct0.department_num,
  CASE
  WHEN LOWER(fct0.fulfill_type_code) = LOWER('DS')
  THEN 'Y'
  ELSE 'N'
  END AS dropship_ind,
 cat0.category,
  CASE
  WHEN pb0.aur <= 0.01
  THEN 'NO RECEIPT FOUND'
  WHEN pb0.aur <= 10
  THEN '$0-10'
  WHEN pb0.aur <= 15
  THEN '$10-15'
  WHEN pb0.aur <= 20
  THEN '$15-20'
  WHEN pb0.aur <= 25
  THEN '$20-25'
  WHEN pb0.aur <= 30
  THEN '$25-30'
  WHEN pb0.aur <= 40
  THEN '$30-40'
  WHEN pb0.aur <= 50
  THEN '$40-50'
  WHEN pb0.aur <= 60
  THEN '$50-60'
  WHEN pb0.aur <= 80
  THEN '$60-80'
  WHEN pb0.aur <= 100
  THEN '$80-100'
  WHEN pb0.aur <= 150
  THEN '$100-150'
  WHEN pb0.aur <= 200
  THEN '$150-200'
  WHEN pb0.aur <= 1000000000
  THEN 'Over $200'
  ELSE 'NO RECEIPT FOUND'
  END AS price_band,
 0 AS net_sls_r,
 0 AS net_sls_c,
 0 AS net_sls_units,
 0 AS net_sls_reg_r,
 0 AS net_sls_reg_c,
 0 AS net_sls_reg_units,
 0 AS returns_r,
 0 AS returns_c,
 0 AS returns_u,
 SUM(CASE
   WHEN st0.channel_num NOT IN (110, 111, 210, 211)
   THEN fct0.demand_tot_amt
   ELSE 0
   END) AS demand_ttl_r,
 SUM(CASE
   WHEN st0.channel_num NOT IN (110, 111, 210, 211)
   THEN fct0.demand_tot_qty
   ELSE 0
   END) AS demand_ttl_units,
 0 AS eop_ttl_c,
 0 AS eop_ttl_units,
 0 AS bop_ttl_c,
 0 AS bop_ttl_units,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_u,
 0 AS ttl_porcpt_rp_c,
 0 AS ttl_porcpt_rp_u,
 0 AS ttl_porcpt_nrp_c,
 0 AS ttl_porcpt_nrp_u,
 0 AS pah_tsfr_in_c,
 0 AS pah_tsfr_in_u,
 0 AS pah_tsfr_out_c,
 0 AS pah_tsfr_out_u,
 0 AS rk_tsfr_in_c,
 0 AS rk_tsfr_in_u,
 0 AS rk_tsfr_out_c,
 0 AS rk_tsfr_out_u,
 0 AS rs_tsfr_in_c,
 0 AS rs_tsfr_in_u,
 0 AS rs_tsfr_out_c,
 0 AS rs_tsfr_out_u,
 0 AS plan_bop_c,
 0 AS plan_bop_u,
 0 AS plan_eop_c,
 0 AS plan_eop_u,
 0 AS plan_receipts_c,
 0 AS plan_receipts_u,
 0 AS plan_receipts_lr_c,
 0 AS plan_receipts_lr_u,
 0 AS plan_sales_c,
 0 AS plan_sales_r,
 0 AS plan_sales_u,
 0 AS plan_demand_r,
 0 AS plan_demand_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_demand_sku_store_week_agg_fact_vw AS fct0
 INNER JOIN date_lkup_acts AS dt0 ON fct0.week_num = dt0.week_idnt_true
 INNER JOIN store_lkup_rack AS st0 ON fct0.store_num = st0.store_num
 LEFT JOIN cat_lkup AS cat0 ON LOWER(fct0.rms_sku_num) = LOWER(cat0.rms_sku_num) AND fct0.department_num = cat0.dept_num
     AND LOWER(st0.channel_country) = LOWER(cat0.channel_country)
 LEFT JOIN receipt_price_band_rack AS pb0 ON LOWER(fct0.rms_sku_num) = LOWER(pb0.sku_idnt) AND LOWER(st0.channel_country
    ) = LOWER(pb0.channel_country)
GROUP BY st0.channel_country,
 st0.banner,
 dt0.date_ind,
 dt0.month_idnt,
 dt0.month_label,
 dt0.month_idnt_aligned,
 dt0.month_label_aligned,
 dt0.month_start_day_date,
 dt0.month_end_day_date,
 fct0.department_num,
 dropship_ind,
 cat0.category,
 price_band;


CREATE TEMPORARY TABLE IF NOT EXISTS receipts
AS
SELECT st.channel_country,
 st.banner,
 dt.date_ind,
 dt.month_idnt,
 dt.month_label,
 dt.month_idnt_aligned,
 dt.month_label_aligned,
 dt.month_start_day_date,
 dt.month_end_day_date,
 fct.department_num,
 fct.dropship_ind,
 cat.category,
  CASE
  WHEN pb.aur <= 0.01
  THEN 'NO RECEIPT FOUND'
  WHEN pb.aur <= 25
  THEN '$0-25'
  WHEN pb.aur <= 50
  THEN '$25-50'
  WHEN pb.aur <= 100
  THEN '$50-100'
  WHEN pb.aur <= 150
  THEN '$100-150'
  WHEN pb.aur <= 200
  THEN '$150-200'
  WHEN pb.aur <= 300
  THEN '$200-300'
  WHEN pb.aur <= 500
  THEN '$300-500'
  WHEN pb.aur <= 700
  THEN '$500-700'
  WHEN pb.aur <= 1000
  THEN '$700-1000'
  WHEN pb.aur <= 1500
  THEN '$1000-1500'
  WHEN pb.aur <= 1000000000
  THEN 'Over $1500'
  ELSE 'NO RECEIPT FOUND'
  END AS price_band,
 0 AS net_sls_r,
 0 AS net_sls_c,
 0 AS net_sls_units,
 0 AS net_sls_reg_r,
 0 AS net_sls_reg_c,
 0 AS net_sls_reg_units,
 0 AS returns_r,
 0 AS returns_c,
 0 AS returns_u,
 0 AS demand_ttl_r,
 0 AS demand_ttl_units,
 0 AS eop_ttl_c,
 0 AS eop_ttl_units,
 0 AS bop_ttl_c,
 0 AS bop_ttl_units,
 SUM(CASE
   WHEN st.channel_num NOT IN (930, 220, 221)
   THEN fct.receipts_regular_cost + fct.receipts_clearance_cost + fct.receipts_crossdock_regular_cost + fct.receipts_crossdock_clearance_cost
    
   ELSE 0
   END) AS ttl_porcpt_c,
 SUM(CASE
   WHEN st.channel_num NOT IN (930, 220, 221)
   THEN fct.receipts_regular_units + fct.receipts_clearance_units + fct.receipts_crossdock_regular_units + fct.receipts_crossdock_clearance_units
    
   ELSE 0
   END) AS ttl_porcpt_u,
 SUM(CASE
   WHEN LOWER(fct.rp_ind) = LOWER('Y') AND st.channel_num NOT IN (930, 220, 221)
   THEN fct.receipts_regular_cost + fct.receipts_clearance_cost + fct.receipts_crossdock_regular_cost + fct.receipts_crossdock_clearance_cost
    
   ELSE 0
   END) AS ttl_porcpt_rp_c,
 SUM(CASE
   WHEN LOWER(fct.rp_ind) = LOWER('Y') AND st.channel_num NOT IN (930, 220, 221)
   THEN fct.receipts_regular_units + fct.receipts_clearance_units + fct.receipts_crossdock_regular_units + fct.receipts_crossdock_clearance_units
    
   ELSE 0
   END) AS ttl_porcpt_rp_u,
 SUM(CASE
   WHEN LOWER(fct.rp_ind) = LOWER('N') AND st.channel_num NOT IN (930, 220, 221)
   THEN fct.receipts_regular_cost + fct.receipts_clearance_cost + fct.receipts_crossdock_regular_cost + fct.receipts_crossdock_clearance_cost
    
   ELSE 0
   END) AS ttl_porcpt_nrp_c,
 SUM(CASE
   WHEN LOWER(fct.rp_ind) = LOWER('N') AND st.channel_num NOT IN (930, 220, 221)
   THEN fct.receipts_regular_units + fct.receipts_clearance_units + fct.receipts_crossdock_regular_units + fct.receipts_crossdock_clearance_units
    
   ELSE 0
   END) AS ttl_porcpt_nrp_u,
 0 AS pah_tsfr_in_c,
 0 AS pah_tsfr_in_u,
 0 AS pah_tsfr_out_c,
 0 AS pah_tsfr_out_u,
 0 AS rk_tsfr_in_c,
 0 AS rk_tsfr_in_u,
 0 AS rk_tsfr_out_c,
 0 AS rk_tsfr_out_u,
 0 AS rs_tsfr_in_c,
 0 AS rs_tsfr_in_u,
 0 AS rs_tsfr_out_c,
 0 AS rs_tsfr_out_u,
 0 AS plan_bop_c,
 0 AS plan_bop_u,
 0 AS plan_eop_c,
 0 AS plan_eop_u,
 0 AS plan_receipts_c,
 0 AS plan_receipts_u,
 0 AS plan_receipts_lr_c,
 0 AS plan_receipts_lr_u,
 0 AS plan_sales_c,
 0 AS plan_sales_r,
 0 AS plan_sales_u,
 0 AS plan_demand_r,
 0 AS plan_demand_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_poreceipt_sku_store_week_fact_vw AS fct
 INNER JOIN date_lkup_acts AS dt ON fct.week_num = dt.week_idnt_true
 INNER JOIN store_lkup_nord AS st ON fct.store_num = st.store_num AND st.channel_num IN (110, 111, 120, 121, 310, 311)
 LEFT JOIN cat_lkup AS cat ON LOWER(fct.rms_sku_num) = LOWER(cat.rms_sku_num) AND fct.department_num = cat.dept_num AND
   LOWER(st.channel_country) = LOWER(cat.channel_country)
 LEFT JOIN receipt_price_band_nord AS pb ON LOWER(fct.rms_sku_num) = LOWER(pb.sku_idnt) AND LOWER(st.channel_country) =
   LOWER(pb.channel_country)
GROUP BY st.channel_country,
 st.banner,
 dt.date_ind,
 dt.month_idnt,
 dt.month_label,
 dt.month_idnt_aligned,
 dt.month_label_aligned,
 dt.month_start_day_date,
 dt.month_end_day_date,
 fct.department_num,
 fct.dropship_ind,
 cat.category,
 price_band
UNION ALL
SELECT st0.channel_country,
 st0.banner,
 dt0.date_ind,
 dt0.month_idnt,
 dt0.month_label,
 dt0.month_idnt_aligned,
 dt0.month_label_aligned,
 dt0.month_start_day_date,
 dt0.month_end_day_date,
 fct0.department_num,
 fct0.dropship_ind,
 cat0.category,
  CASE
  WHEN pb0.aur <= 0.01
  THEN 'NO RECEIPT FOUND'
  WHEN pb0.aur <= 10
  THEN '$0-10'
  WHEN pb0.aur <= 15
  THEN '$10-15'
  WHEN pb0.aur <= 20
  THEN '$15-20'
  WHEN pb0.aur <= 25
  THEN '$20-25'
  WHEN pb0.aur <= 30
  THEN '$25-30'
  WHEN pb0.aur <= 40
  THEN '$30-40'
  WHEN pb0.aur <= 50
  THEN '$40-50'
  WHEN pb0.aur <= 60
  THEN '$50-60'
  WHEN pb0.aur <= 80
  THEN '$60-80'
  WHEN pb0.aur <= 100
  THEN '$80-100'
  WHEN pb0.aur <= 150
  THEN '$100-150'
  WHEN pb0.aur <= 200
  THEN '$150-200'
  WHEN pb0.aur <= 1000000000
  THEN 'Over $200'
  ELSE 'NO RECEIPT FOUND'
  END AS price_band,
 0 AS net_sls_r,
 0 AS net_sls_c,
 0 AS net_sls_units,
 0 AS net_sls_reg_r,
 0 AS net_sls_reg_c,
 0 AS net_sls_reg_units,
 0 AS returns_r,
 0 AS returns_c,
 0 AS returns_u,
 0 AS demand_ttl_r,
 0 AS demand_ttl_units,
 0 AS eop_ttl_c,
 0 AS eop_ttl_units,
 0 AS bop_ttl_c,
 0 AS bop_ttl_units,
 SUM(CASE
   WHEN st0.channel_num NOT IN (930, 220, 221)
   THEN fct0.receipts_regular_cost + fct0.receipts_clearance_cost + fct0.receipts_crossdock_regular_cost + fct0.receipts_crossdock_clearance_cost
    
   ELSE 0
   END) AS ttl_porcpt_c,
 SUM(CASE
   WHEN st0.channel_num NOT IN (930, 220, 221)
   THEN fct0.receipts_regular_units + fct0.receipts_clearance_units + fct0.receipts_crossdock_regular_units + fct0.receipts_crossdock_clearance_units
    
   ELSE 0
   END) AS ttl_porcpt_u,
 SUM(CASE
   WHEN LOWER(fct0.rp_ind) = LOWER('Y') AND st0.channel_num NOT IN (930, 220, 221)
   THEN fct0.receipts_regular_cost + fct0.receipts_clearance_cost + fct0.receipts_crossdock_regular_cost + fct0.receipts_crossdock_clearance_cost
    
   ELSE 0
   END) AS ttl_porcpt_rp_c,
 SUM(CASE
   WHEN LOWER(fct0.rp_ind) = LOWER('Y') AND st0.channel_num NOT IN (930, 220, 221)
   THEN fct0.receipts_regular_units + fct0.receipts_clearance_units + fct0.receipts_crossdock_regular_units + fct0.receipts_crossdock_clearance_units
    
   ELSE 0
   END) AS ttl_porcpt_rp_u,
 SUM(CASE
   WHEN LOWER(fct0.rp_ind) = LOWER('N') AND st0.channel_num NOT IN (930, 220, 221)
   THEN fct0.receipts_regular_cost + fct0.receipts_clearance_cost + fct0.receipts_crossdock_regular_cost + fct0.receipts_crossdock_clearance_cost
    
   ELSE 0
   END) AS ttl_porcpt_nrp_c,
 SUM(CASE
   WHEN LOWER(fct0.rp_ind) = LOWER('N') AND st0.channel_num NOT IN (930, 220, 221)
   THEN fct0.receipts_regular_units + fct0.receipts_clearance_units + fct0.receipts_crossdock_regular_units + fct0.receipts_crossdock_clearance_units
    
   ELSE 0
   END) AS ttl_porcpt_nrp_u,
 0 AS pah_tsfr_in_c,
 0 AS pah_tsfr_in_u,
 0 AS pah_tsfr_out_c,
 0 AS pah_tsfr_out_u,
 0 AS rk_tsfr_in_c,
 0 AS rk_tsfr_in_u,
 0 AS rk_tsfr_out_c,
 0 AS rk_tsfr_out_u,
 0 AS rs_tsfr_in_c,
 0 AS rs_tsfr_in_u,
 0 AS rs_tsfr_out_c,
 0 AS rs_tsfr_out_u,
 0 AS plan_bop_c,
 0 AS plan_bop_u,
 0 AS plan_eop_c,
 0 AS plan_eop_u,
 0 AS plan_receipts_c,
 0 AS plan_receipts_u,
 0 AS plan_receipts_lr_c,
 0 AS plan_receipts_lr_u,
 0 AS plan_sales_c,
 0 AS plan_sales_r,
 0 AS plan_sales_u,
 0 AS plan_demand_r,
 0 AS plan_demand_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_poreceipt_sku_store_week_fact_vw AS fct0
 INNER JOIN date_lkup_acts AS dt0 ON fct0.week_num = dt0.week_idnt_true
 INNER JOIN store_lkup_rack AS st0 ON fct0.store_num = st0.store_num AND st0.channel_num IN (210, 211, 250, 260, 261)
 LEFT JOIN cat_lkup AS cat0 ON LOWER(fct0.rms_sku_num) = LOWER(cat0.rms_sku_num) AND fct0.department_num = cat0.dept_num
     AND LOWER(st0.channel_country) = LOWER(cat0.channel_country)
 LEFT JOIN receipt_price_band_rack AS pb0 ON LOWER(fct0.rms_sku_num) = LOWER(pb0.sku_idnt) AND LOWER(st0.channel_country
    ) = LOWER(pb0.channel_country)
GROUP BY st0.channel_country,
 st0.banner,
 dt0.date_ind,
 dt0.month_idnt,
 dt0.month_label,
 dt0.month_idnt_aligned,
 dt0.month_label_aligned,
 dt0.month_start_day_date,
 dt0.month_end_day_date,
 fct0.department_num,
 fct0.dropship_ind,
 cat0.category,
 price_band;


CREATE TEMPORARY TABLE IF NOT EXISTS transfers
AS
SELECT st.channel_country,
 st.banner,
 dt.date_ind,
 dt.month_idnt,
 dt.month_label,
 dt.month_idnt_aligned,
 dt.month_label_aligned,
 dt.month_start_day_date,
 dt.month_end_day_date,
 fct.department_num,
 RPAD(NULL, 1, ' ') AS dropship_ind,
 cat.category,
  CASE
  WHEN pb.aur <= 0.01
  THEN 'NO RECEIPT FOUND'
  WHEN pb.aur <= 25
  THEN '$0-25'
  WHEN pb.aur <= 50
  THEN '$25-50'
  WHEN pb.aur <= 100
  THEN '$50-100'
  WHEN pb.aur <= 150
  THEN '$100-150'
  WHEN pb.aur <= 200
  THEN '$150-200'
  WHEN pb.aur <= 300
  THEN '$200-300'
  WHEN pb.aur <= 500
  THEN '$300-500'
  WHEN pb.aur <= 700
  THEN '$500-700'
  WHEN pb.aur <= 1000
  THEN '$700-1000'
  WHEN pb.aur <= 1500
  THEN '$1000-1500'
  WHEN pb.aur <= 1000000000
  THEN 'Over $1500'
  ELSE 'NO RECEIPT FOUND'
  END AS price_band,
 0 AS net_sls_r,
 0 AS net_sls_c,
 0 AS net_sls_units,
 0 AS net_sls_reg_r,
 0 AS net_sls_reg_c,
 0 AS net_sls_reg_units,
 0 AS returns_r,
 0 AS returns_c,
 0 AS returns_u,
 0 AS demand_ttl_r,
 0 AS demand_ttl_units,
 0 AS eop_ttl_c,
 0 AS eop_ttl_units,
 0 AS bop_ttl_c,
 0 AS bop_ttl_units,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_u,
 0 AS ttl_porcpt_rp_c,
 0 AS ttl_porcpt_rp_u,
 0 AS ttl_porcpt_nrp_c,
 0 AS ttl_porcpt_nrp_u,
 SUM(fct.packandhold_transfer_in_cost) AS pah_tsfr_in_c,
 SUM(fct.packandhold_transfer_in_units) AS pah_tsfr_in_u,
 SUM(fct.packandhold_transfer_out_cost) AS pah_tsfr_out_c,
 SUM(fct.packandhold_transfer_out_units) AS pah_tsfr_out_u,
 SUM(fct.racking_transfer_in_cost) AS rk_tsfr_in_c,
 SUM(fct.racking_transfer_in_units) AS rk_tsfr_in_u,
 SUM(fct.racking_transfer_out_cost) AS rk_tsfr_out_c,
 SUM(fct.racking_transfer_out_units) AS rk_tsfr_out_u,
 SUM(fct.reservestock_transfer_in_cost) AS rs_tsfr_in_c,
 SUM(fct.reservestock_transfer_in_units) AS rs_tsfr_in_u,
 SUM(fct.reservestock_transfer_out_cost) AS rs_tsfr_out_c,
 SUM(fct.reservestock_transfer_out_units) AS rs_tsfr_out_u,
 0 AS plan_bop_c,
 0 AS plan_bop_u,
 0 AS plan_eop_c,
 0 AS plan_eop_u,
 0 AS plan_receipts_c,
 0 AS plan_receipts_u,
 0 AS plan_receipts_lr_c,
 0 AS plan_receipts_lr_u,
 0 AS plan_sales_c,
 0 AS plan_sales_r,
 0 AS plan_sales_u,
 0 AS plan_demand_r,
 0 AS plan_demand_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_transfer_sku_loc_week_agg_fact_vw AS fct
 INNER JOIN date_lkup_acts AS dt ON fct.week_num = dt.week_idnt_true
 INNER JOIN store_lkup_nord AS st ON fct.store_num = st.store_num AND st.channel_num IN (110, 111, 120, 121, 310, 311)
 LEFT JOIN cat_lkup AS cat ON LOWER(fct.rms_sku_num) = LOWER(cat.rms_sku_num) AND fct.department_num = cat.dept_num AND
   LOWER(st.channel_country) = LOWER(cat.channel_country)
 LEFT JOIN receipt_price_band_nord AS pb ON LOWER(fct.rms_sku_num) = LOWER(pb.sku_idnt) AND LOWER(st.channel_country) =
   LOWER(pb.channel_country)
GROUP BY st.channel_country,
 st.banner,
 dt.date_ind,
 dt.month_idnt,
 dt.month_label,
 dt.month_idnt_aligned,
 dt.month_label_aligned,
 dt.month_start_day_date,
 dt.month_end_day_date,
 fct.department_num,
 dropship_ind,
 cat.category,
 price_band,
 net_sls_r,
 net_sls_c
UNION ALL
SELECT st0.channel_country,
 st0.banner,
 dt0.date_ind,
 dt0.month_idnt,
 dt0.month_label,
 dt0.month_idnt_aligned,
 dt0.month_label_aligned,
 dt0.month_start_day_date,
 dt0.month_end_day_date,
 fct0.department_num,
 RPAD(NULL, 1, ' ') AS dropship_ind,
 cat0.category,
  CASE
  WHEN pb0.aur <= 0.01
  THEN 'NO RECEIPT FOUND'
  WHEN pb0.aur <= 10
  THEN '$0-10'
  WHEN pb0.aur <= 15
  THEN '$10-15'
  WHEN pb0.aur <= 20
  THEN '$15-20'
  WHEN pb0.aur <= 25
  THEN '$20-25'
  WHEN pb0.aur <= 30
  THEN '$25-30'
  WHEN pb0.aur <= 40
  THEN '$30-40'
  WHEN pb0.aur <= 50
  THEN '$40-50'
  WHEN pb0.aur <= 60
  THEN '$50-60'
  WHEN pb0.aur <= 80
  THEN '$60-80'
  WHEN pb0.aur <= 100
  THEN '$80-100'
  WHEN pb0.aur <= 150
  THEN '$100-150'
  WHEN pb0.aur <= 200
  THEN '$150-200'
  WHEN pb0.aur <= 1000000000
  THEN 'Over $200'
  ELSE 'NO RECEIPT FOUND'
  END AS price_band,
 0 AS net_sls_r,
 0 AS net_sls_c,
 0 AS net_sls_units,
 0 AS net_sls_reg_r,
 0 AS net_sls_reg_c,
 0 AS net_sls_reg_units,
 0 AS returns_r,
 0 AS returns_c,
 0 AS returns_u,
 0 AS demand_ttl_r,
 0 AS demand_ttl_units,
 0 AS eop_ttl_c,
 0 AS eop_ttl_units,
 0 AS bop_ttl_c,
 0 AS bop_ttl_units,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_u,
 0 AS ttl_porcpt_rp_c,
 0 AS ttl_porcpt_rp_u,
 0 AS ttl_porcpt_nrp_c,
 0 AS ttl_porcpt_nrp_u,
 SUM(fct0.packandhold_transfer_in_cost) AS pah_tsfr_in_c,
 SUM(fct0.packandhold_transfer_in_units) AS pah_tsfr_in_u,
 SUM(fct0.packandhold_transfer_out_cost) AS pah_tsfr_out_c,
 SUM(fct0.packandhold_transfer_out_units) AS pah_tsfr_out_u,
 SUM(fct0.racking_transfer_in_cost) AS rk_tsfr_in_c,
 SUM(fct0.racking_transfer_in_units) AS rk_tsfr_in_u,
 SUM(fct0.racking_transfer_out_cost) AS rk_tsfr_out_c,
 SUM(fct0.racking_transfer_out_units) AS rk_tsfr_out_u,
 SUM(fct0.reservestock_transfer_in_cost) AS rs_tsfr_in_c,
 SUM(fct0.reservestock_transfer_in_units) AS rs_tsfr_in_u,
 SUM(fct0.reservestock_transfer_out_cost) AS rs_tsfr_out_c,
 SUM(fct0.reservestock_transfer_out_units) AS rs_tsfr_out_u,
 0 AS plan_bop_c,
 0 AS plan_bop_u,
 0 AS plan_eop_c,
 0 AS plan_eop_u,
 0 AS plan_receipts_c,
 0 AS plan_receipts_u,
 0 AS plan_receipts_lr_c,
 0 AS plan_receipts_lr_u,
 0 AS plan_sales_c,
 0 AS plan_sales_r,
 0 AS plan_sales_u,
 0 AS plan_demand_r,
 0 AS plan_demand_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_transfer_sku_loc_week_agg_fact_vw AS fct0
 INNER JOIN date_lkup_acts AS dt0 ON fct0.week_num = dt0.week_idnt_true
 INNER JOIN store_lkup_rack AS st0 ON fct0.store_num = st0.store_num AND st0.channel_num IN (210, 211, 250, 260, 261)
 LEFT JOIN cat_lkup AS cat0 ON LOWER(fct0.rms_sku_num) = LOWER(cat0.rms_sku_num) AND fct0.department_num = cat0.dept_num
     AND LOWER(st0.channel_country) = LOWER(cat0.channel_country)
 LEFT JOIN receipt_price_band_rack AS pb0 ON LOWER(fct0.rms_sku_num) = LOWER(pb0.sku_idnt) AND LOWER(st0.channel_country
    ) = LOWER(pb0.channel_country)
GROUP BY st0.channel_country,
 st0.banner,
 dt0.date_ind,
 dt0.month_idnt,
 dt0.month_label,
 dt0.month_idnt_aligned,
 dt0.month_label_aligned,
 dt0.month_start_day_date,
 dt0.month_end_day_date,
 fct0.department_num,
 dropship_ind,
 cat0.category,
 price_band,
 net_sls_r,
 net_sls_c;


CREATE TEMPORARY TABLE IF NOT EXISTS eop_stage_ty
AS
SELECT fct.rms_sku_num,
 st.channel_country,
 st.banner,
 dt.date_ind,
 dt.month_idnt,
 dt.month_label,
 dt.month_idnt_aligned,
 dt.month_label_aligned,
 dt.month_start_day_date,
 dt.month_end_day_date,
 fct.department_num,
 SUM(CASE
   WHEN dt.mtd_week_idnt_true = fct.week_num
   THEN fct.eoh_total_cost + fct.eoh_in_transit_total_cost
   ELSE NULL
   END) AS eop_ttl_c,
 SUM(CASE
   WHEN dt.mtd_week_idnt_true = fct.week_num
   THEN fct.eoh_total_units + fct.eoh_in_transit_total_units
   ELSE NULL
   END) AS eop_ttl_units,
 SUM(CASE
   WHEN dt.mbeg_week_idnt_true = fct.week_num
   THEN fct.boh_total_cost + fct.boh_in_transit_total_cost
   ELSE NULL
   END) AS bop_ttl_c,
 SUM(CASE
   WHEN dt.mbeg_week_idnt_true = fct.week_num
   THEN fct.boh_total_units + fct.boh_in_transit_total_units
   ELSE NULL
   END) AS bop_ttl_units
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_inventory_sku_store_week_fact_vw AS fct
 INNER JOIN date_lkup_acts_ty AS dt ON fct.week_num = dt.week_idnt_true
 INNER JOIN store_lkup_eop AS st ON CAST(fct.store_num AS FLOAT64) = st.store_num
GROUP BY fct.rms_sku_num,
 st.channel_country,
 st.banner,
 dt.date_ind,
 dt.month_idnt,
 dt.month_label,
 dt.month_idnt_aligned,
 dt.month_label_aligned,
 dt.month_start_day_date,
 dt.month_end_day_date,
 fct.department_num;


CREATE TEMPORARY TABLE IF NOT EXISTS eop_stage_ly
AS
SELECT fct.rms_sku_num,
 st.channel_country,
 st.banner,
 dt.date_ind,
 dt.month_idnt,
 dt.month_label,
 dt.month_idnt_aligned,
 dt.month_label_aligned,
 dt.month_start_day_date,
 dt.month_end_day_date,
 fct.department_num,
 SUM(CASE
   WHEN dt.mtd_week_idnt_true = fct.week_num
   THEN fct.eoh_total_cost + fct.eoh_in_transit_total_cost
   ELSE NULL
   END) AS eop_ttl_c,
 SUM(CASE
   WHEN dt.mtd_week_idnt_true = fct.week_num
   THEN fct.eoh_total_units + fct.eoh_in_transit_total_units
   ELSE NULL
   END) AS eop_ttl_units,
 SUM(CASE
   WHEN dt.mbeg_week_idnt_true = fct.week_num
   THEN fct.boh_total_cost + fct.boh_in_transit_total_cost
   ELSE NULL
   END) AS bop_ttl_c,
 SUM(CASE
   WHEN dt.mbeg_week_idnt_true = fct.week_num
   THEN fct.boh_total_units + fct.boh_in_transit_total_units
   ELSE NULL
   END) AS bop_ttl_units
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_inventory_sku_store_week_fact_vw AS fct
 INNER JOIN date_lkup_acts_ly AS dt ON fct.week_num = dt.week_idnt_true
 INNER JOIN store_lkup_eop AS st ON CAST(fct.store_num AS FLOAT64) = st.store_num
GROUP BY fct.rms_sku_num,
 st.channel_country,
 st.banner,
 dt.date_ind,
 dt.month_idnt,
 dt.month_label,
 dt.month_idnt_aligned,
 dt.month_label_aligned,
 dt.month_start_day_date,
 dt.month_end_day_date,
 fct.department_num;

CREATE TEMPORARY TABLE IF NOT EXISTS eop_ty
AS
SELECT fct.channel_country,
 fct.banner,
 fct.date_ind,
 fct.month_idnt,
 fct.month_label,
 fct.month_idnt_aligned,
 fct.month_label_aligned,
 fct.month_start_day_date,
 fct.month_end_day_date,
 fct.department_num,
 RPAD(NULL, 1, ' ') AS dropship_ind,
 cat.category,
  CASE
  WHEN pb.aur <= 0.01
  THEN 'NO RECEIPT FOUND'
  WHEN pb.aur <= 25
  THEN '$0-25'
  WHEN pb.aur <= 50
  THEN '$25-50'
  WHEN pb.aur <= 100
  THEN '$50-100'
  WHEN pb.aur <= 150
  THEN '$100-150'
  WHEN pb.aur <= 200
  THEN '$150-200'
  WHEN pb.aur <= 300
  THEN '$200-300'
  WHEN pb.aur <= 500
  THEN '$300-500'
  WHEN pb.aur <= 700
  THEN '$500-700'
  WHEN pb.aur <= 1000
  THEN '$700-1000'
  WHEN pb.aur <= 1500
  THEN '$1000-1500'
  WHEN pb.aur <= 1000000000
  THEN 'Over $1500'
  ELSE 'NO RECEIPT FOUND'
  END AS price_band,
 0 AS net_sls_r,
 0 AS net_sls_c,
 0 AS net_sls_units,
 0 AS net_sls_reg_r,
 0 AS net_sls_reg_c,
 0 AS net_sls_reg_units,
 0 AS returns_r,
 0 AS returns_c,
 0 AS returns_u,
 0 AS demand_ttl_r,
 0 AS demand_ttl_units,
 SUM(fct.eop_ttl_c) AS eop_ttl_c,
 SUM(fct.eop_ttl_units) AS eop_ttl_units,
 SUM(fct.bop_ttl_c) AS bop_ttl_c,
 SUM(fct.bop_ttl_units) AS bop_ttl_units,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_u,
 0 AS ttl_porcpt_rp_c,
 0 AS ttl_porcpt_rp_u,
 0 AS ttl_porcpt_nrp_c,
 0 AS ttl_porcpt_nrp_u,
 0 AS pah_tsfr_in_c,
 0 AS pah_tsfr_in_u,
 0 AS pah_tsfr_out_c,
 0 AS pah_tsfr_out_u,
 0 AS rk_tsfr_in_c,
 0 AS rk_tsfr_in_u,
 0 AS rk_tsfr_out_c,
 0 AS rk_tsfr_out_u,
 0 AS rs_tsfr_in_c,
 0 AS rs_tsfr_in_u,
 0 AS rs_tsfr_out_c,
 0 AS rs_tsfr_out_u,
 0 AS plan_bop_c,
 0 AS plan_bop_u,
 0 AS plan_eop_c,
 0 AS plan_eop_u,
 0 AS plan_receipts_c,
 0 AS plan_receipts_u,
 0 AS plan_receipts_lr_c,
 0 AS plan_receipts_lr_u,
 0 AS plan_sales_c,
 0 AS plan_sales_r,
 0 AS plan_sales_u,
 0 AS plan_demand_r,
 0 AS plan_demand_u
FROM eop_stage_ty AS fct
 LEFT JOIN cat_lkup AS cat ON LOWER(fct.rms_sku_num) = LOWER(cat.rms_sku_num) AND fct.department_num = cat.dept_num AND
   LOWER(fct.channel_country) = LOWER(cat.channel_country)
 LEFT JOIN receipt_price_band_nord AS pb ON LOWER(fct.rms_sku_num) = LOWER(pb.sku_idnt) AND LOWER(fct.channel_country) =
   LOWER(pb.channel_country)
WHERE LOWER(fct.banner) = LOWER('NORDSTROM')
GROUP BY fct.channel_country,
 fct.banner,
 fct.date_ind,
 fct.month_idnt,
 fct.month_label,
 fct.month_idnt_aligned,
 fct.month_label_aligned,
 fct.month_start_day_date,
 fct.month_end_day_date,
 fct.department_num,
 dropship_ind,
 cat.category,
 price_band
UNION ALL
SELECT fct0.channel_country,
 fct0.banner,
 fct0.date_ind,
 fct0.month_idnt,
 fct0.month_label,
 fct0.month_idnt_aligned,
 fct0.month_label_aligned,
 fct0.month_start_day_date,
 fct0.month_end_day_date,
 fct0.department_num,
 RPAD(NULL, 1, ' ') AS dropship_ind,
 cat0.category,
  CASE
  WHEN pb0.aur <= 0.01
  THEN 'NO RECEIPT FOUND'
  WHEN pb0.aur <= 10
  THEN '$0-10'
  WHEN pb0.aur <= 15
  THEN '$10-15'
  WHEN pb0.aur <= 20
  THEN '$15-20'
  WHEN pb0.aur <= 25
  THEN '$20-25'
  WHEN pb0.aur <= 30
  THEN '$25-30'
  WHEN pb0.aur <= 40
  THEN '$30-40'
  WHEN pb0.aur <= 50
  THEN '$40-50'
  WHEN pb0.aur <= 60
  THEN '$50-60'
  WHEN pb0.aur <= 80
  THEN '$60-80'
  WHEN pb0.aur <= 100
  THEN '$80-100'
  WHEN pb0.aur <= 150
  THEN '$100-150'
  WHEN pb0.aur <= 200
  THEN '$150-200'
  WHEN pb0.aur <= 1000000000
  THEN 'Over $200'
  ELSE 'NO RECEIPT FOUND'
  END AS price_band,
 0 AS net_sls_r,
 0 AS net_sls_c,
 0 AS net_sls_units,
 0 AS net_sls_reg_r,
 0 AS net_sls_reg_c,
 0 AS net_sls_reg_units,
 0 AS returns_r,
 0 AS returns_c,
 0 AS returns_u,
 0 AS demand_ttl_r,
 0 AS demand_ttl_units,
 SUM(fct0.eop_ttl_c) AS eop_ttl_c,
 SUM(fct0.eop_ttl_units) AS eop_ttl_units,
 SUM(fct0.bop_ttl_c) AS bop_ttl_c,
 SUM(fct0.bop_ttl_units) AS bop_ttl_units,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_u,
 0 AS ttl_porcpt_rp_c,
 0 AS ttl_porcpt_rp_u,
 0 AS ttl_porcpt_nrp_c,
 0 AS ttl_porcpt_nrp_u,
 0 AS pah_tsfr_in_c,
 0 AS pah_tsfr_in_u,
 0 AS pah_tsfr_out_c,
 0 AS pah_tsfr_out_u,
 0 AS rk_tsfr_in_c,
 0 AS rk_tsfr_in_u,
 0 AS rk_tsfr_out_c,
 0 AS rk_tsfr_out_u,
 0 AS rs_tsfr_in_c,
 0 AS rs_tsfr_in_u,
 0 AS rs_tsfr_out_c,
 0 AS rs_tsfr_out_u,
 0 AS plan_bop_c,
 0 AS plan_bop_u,
 0 AS plan_eop_c,
 0 AS plan_eop_u,
 0 AS plan_receipts_c,
 0 AS plan_receipts_u,
 0 AS plan_receipts_lr_c,
 0 AS plan_receipts_lr_u,
 0 AS plan_sales_c,
 0 AS plan_sales_r,
 0 AS plan_sales_u,
 0 AS plan_demand_r,
 0 AS plan_demand_u
FROM eop_stage_ty AS fct0
 LEFT JOIN cat_lkup AS cat0 ON LOWER(fct0.rms_sku_num) = LOWER(cat0.rms_sku_num) AND fct0.department_num = cat0.dept_num
     AND LOWER(fct0.channel_country) = LOWER(cat0.channel_country)
 LEFT JOIN receipt_price_band_rack AS pb0 ON LOWER(fct0.rms_sku_num) = LOWER(pb0.sku_idnt) AND LOWER(fct0.channel_country
    ) = LOWER(pb0.channel_country)
WHERE LOWER(fct0.banner) = LOWER('NORDSTROM_RACK')
GROUP BY fct0.channel_country,
 fct0.banner,
 fct0.date_ind,
 fct0.month_idnt,
 fct0.month_label,
 fct0.month_idnt_aligned,
 fct0.month_label_aligned,
 fct0.month_start_day_date,
 fct0.month_end_day_date,
 fct0.department_num,
 dropship_ind,
 cat0.category,
 price_band;


CREATE TEMPORARY TABLE IF NOT EXISTS eop_ly
AS
SELECT fct.channel_country,
 fct.banner,
 fct.date_ind,
 fct.month_idnt,
 fct.month_label,
 fct.month_idnt_aligned,
 fct.month_label_aligned,
 fct.month_start_day_date,
 fct.month_end_day_date,
 fct.department_num,
 RPAD(NULL, 1, ' ') AS dropship_ind,
 cat.category,
  CASE
  WHEN pb.aur <= 0.01
  THEN 'NO RECEIPT FOUND'
  WHEN pb.aur <= 25
  THEN '$0-25'
  WHEN pb.aur <= 50
  THEN '$25-50'
  WHEN pb.aur <= 100
  THEN '$50-100'
  WHEN pb.aur <= 150
  THEN '$100-150'
  WHEN pb.aur <= 200
  THEN '$150-200'
  WHEN pb.aur <= 300
  THEN '$200-300'
  WHEN pb.aur <= 500
  THEN '$300-500'
  WHEN pb.aur <= 700
  THEN '$500-700'
  WHEN pb.aur <= 1000
  THEN '$700-1000'
  WHEN pb.aur <= 1500
  THEN '$1000-1500'
  WHEN pb.aur <= 1000000000
  THEN 'Over $1500'
  ELSE 'NO RECEIPT FOUND'
  END AS price_band,
 0 AS net_sls_r,
 0 AS net_sls_c,
 0 AS net_sls_units,
 0 AS net_sls_reg_r,
 0 AS net_sls_reg_c,
 0 AS net_sls_reg_units,
 0 AS returns_r,
 0 AS returns_c,
 0 AS returns_u,
 0 AS demand_ttl_r,
 0 AS demand_ttl_units,
 SUM(fct.eop_ttl_c) AS eop_ttl_c,
 SUM(fct.eop_ttl_units) AS eop_ttl_units,
 SUM(fct.bop_ttl_c) AS bop_ttl_c,
 SUM(fct.bop_ttl_units) AS bop_ttl_units,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_u,
 0 AS ttl_porcpt_rp_c,
 0 AS ttl_porcpt_rp_u,
 0 AS ttl_porcpt_nrp_c,
 0 AS ttl_porcpt_nrp_u,
 0 AS pah_tsfr_in_c,
 0 AS pah_tsfr_in_u,
 0 AS pah_tsfr_out_c,
 0 AS pah_tsfr_out_u,
 0 AS rk_tsfr_in_c,
 0 AS rk_tsfr_in_u,
 0 AS rk_tsfr_out_c,
 0 AS rk_tsfr_out_u,
 0 AS rs_tsfr_in_c,
 0 AS rs_tsfr_in_u,
 0 AS rs_tsfr_out_c,
 0 AS rs_tsfr_out_u,
 0 AS plan_bop_c,
 0 AS plan_bop_u,
 0 AS plan_eop_c,
 0 AS plan_eop_u,
 0 AS plan_receipts_c,
 0 AS plan_receipts_u,
 0 AS plan_receipts_lr_c,
 0 AS plan_receipts_lr_u,
 0 AS plan_sales_c,
 0 AS plan_sales_r,
 0 AS plan_sales_u,
 0 AS plan_demand_r,
 0 AS plan_demand_u
FROM eop_stage_ly AS fct
 LEFT JOIN cat_lkup AS cat ON LOWER(fct.rms_sku_num) = LOWER(cat.rms_sku_num) AND fct.department_num = cat.dept_num AND
   LOWER(fct.channel_country) = LOWER(cat.channel_country)
 LEFT JOIN receipt_price_band_nord AS pb ON LOWER(fct.rms_sku_num) = LOWER(pb.sku_idnt) AND LOWER(fct.channel_country) =
   LOWER(pb.channel_country)
WHERE LOWER(fct.banner) = LOWER('NORDSTROM')
GROUP BY fct.channel_country,
 fct.banner,
 fct.date_ind,
 fct.month_idnt,
 fct.month_label,
 fct.month_idnt_aligned,
 fct.month_label_aligned,
 fct.month_start_day_date,
 fct.month_end_day_date,
 fct.department_num,
 dropship_ind,
 cat.category,
 price_band
UNION ALL
SELECT fct0.channel_country,
 fct0.banner,
 fct0.date_ind,
 fct0.month_idnt,
 fct0.month_label,
 fct0.month_idnt_aligned,
 fct0.month_label_aligned,
 fct0.month_start_day_date,
 fct0.month_end_day_date,
 fct0.department_num,
 RPAD(NULL, 1, ' ') AS dropship_ind,
 cat0.category,
  CASE
  WHEN pb0.aur <= 0.01
  THEN 'NO RECEIPT FOUND'
  WHEN pb0.aur <= 10
  THEN '$0-10'
  WHEN pb0.aur <= 15
  THEN '$10-15'
  WHEN pb0.aur <= 20
  THEN '$15-20'
  WHEN pb0.aur <= 25
  THEN '$20-25'
  WHEN pb0.aur <= 30
  THEN '$25-30'
  WHEN pb0.aur <= 40
  THEN '$30-40'
  WHEN pb0.aur <= 50
  THEN '$40-50'
  WHEN pb0.aur <= 60
  THEN '$50-60'
  WHEN pb0.aur <= 80
  THEN '$60-80'
  WHEN pb0.aur <= 100
  THEN '$80-100'
  WHEN pb0.aur <= 150
  THEN '$100-150'
  WHEN pb0.aur <= 200
  THEN '$150-200'
  WHEN pb0.aur <= 1000000000
  THEN 'Over $200'
  ELSE 'NO RECEIPT FOUND'
  END AS price_band,
 0 AS net_sls_r,
 0 AS net_sls_c,
 0 AS net_sls_units,
 0 AS net_sls_reg_r,
 0 AS net_sls_reg_c,
 0 AS net_sls_reg_units,
 0 AS returns_r,
 0 AS returns_c,
 0 AS returns_u,
 0 AS demand_ttl_r,
 0 AS demand_ttl_units,
 SUM(fct0.eop_ttl_c) AS eop_ttl_c,
 SUM(fct0.eop_ttl_units) AS eop_ttl_units,
 SUM(fct0.bop_ttl_c) AS bop_ttl_c,
 SUM(fct0.bop_ttl_units) AS bop_ttl_units,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_u,
 0 AS ttl_porcpt_rp_c,
 0 AS ttl_porcpt_rp_u,
 0 AS ttl_porcpt_nrp_c,
 0 AS ttl_porcpt_nrp_u,
 0 AS pah_tsfr_in_c,
 0 AS pah_tsfr_in_u,
 0 AS pah_tsfr_out_c,
 0 AS pah_tsfr_out_u,
 0 AS rk_tsfr_in_c,
 0 AS rk_tsfr_in_u,
 0 AS rk_tsfr_out_c,
 0 AS rk_tsfr_out_u,
 0 AS rs_tsfr_in_c,
 0 AS rs_tsfr_in_u,
 0 AS rs_tsfr_out_c,
 0 AS rs_tsfr_out_u,
 0 AS plan_bop_c,
 0 AS plan_bop_u,
 0 AS plan_eop_c,
 0 AS plan_eop_u,
 0 AS plan_receipts_c,
 0 AS plan_receipts_u,
 0 AS plan_receipts_lr_c,
 0 AS plan_receipts_lr_u,
 0 AS plan_sales_c,
 0 AS plan_sales_r,
 0 AS plan_sales_u,
 0 AS plan_demand_r,
 0 AS plan_demand_u
FROM eop_stage_ly AS fct0
 LEFT JOIN cat_lkup AS cat0 
 ON LOWER(fct0.rms_sku_num) = LOWER(cat0.rms_sku_num) 
 AND fct0.department_num = cat0.dept_num
 AND LOWER(fct0.channel_country) = LOWER(cat0.channel_country)
 LEFT JOIN receipt_price_band_rack AS pb0 
 ON LOWER(fct0.rms_sku_num) = LOWER(pb0.sku_idnt) 
 AND LOWER(fct0.channel_country) = LOWER(pb0.channel_country)
WHERE LOWER(fct0.banner) = LOWER('NORDSTROM_RACK')
GROUP BY fct0.channel_country,
 fct0.banner,
 fct0.date_ind,
 fct0.month_idnt,
 fct0.month_label,
 fct0.month_idnt_aligned,
 fct0.month_label_aligned,
 fct0.month_start_day_date,
 fct0.month_end_day_date,
 fct0.department_num,
 dropship_ind,
 cat0.category,
 price_band;

CREATE TEMPORARY TABLE IF NOT EXISTS combine
AS
SELECT channel_country,
 banner,
 date_ind,
 month_idnt,
 month_label,
 month_idnt_aligned,
 month_label_aligned,
 month_start_day_date,
 month_end_day_date,
 department_num,
 dropship_ind,
 category,
 price_band,
 SUM(net_sls_r) AS net_sls_r,
 SUM(net_sls_c) AS net_sls_c,
 SUM(net_sls_units) AS net_sls_units,
 SUM(net_sls_reg_r) AS net_sls_reg_r,
 SUM(net_sls_reg_c) AS net_sls_reg_c,
 SUM(net_sls_reg_units) AS net_sls_reg_units,
 SUM(returns_r) AS returns_r,
 SUM(returns_c) AS returns_c,
 SUM(returns_u) AS returns_u,
 SUM(demand_ttl_r) AS demand_ttl_r,
 SUM(demand_ttl_units) AS demand_ttl_units,
 SUM(eop_ttl_c) AS eop_ttl_c,
 SUM(eop_ttl_units) AS eop_ttl_units,
 SUM(bop_ttl_c) AS bop_ttl_c,
 SUM(bop_ttl_units) AS bop_ttl_units,
 SUM(ttl_porcpt_c) AS ttl_porcpt_c,
 SUM(ttl_porcpt_u) AS ttl_porcpt_u,
 SUM(ttl_porcpt_rp_c) AS ttl_porcpt_rp_c,
 SUM(ttl_porcpt_rp_u) AS ttl_porcpt_rp_u,
 SUM(ttl_porcpt_nrp_c) AS ttl_porcpt_nrp_c,
 SUM(ttl_porcpt_nrp_u) AS ttl_porcpt_nrp_u,
 SUM(pah_tsfr_in_c) AS pah_tsfr_in_c,
 SUM(pah_tsfr_in_u) AS pah_tsfr_in_u,
 SUM(pah_tsfr_out_c) AS pah_tsfr_out_c,
 SUM(pah_tsfr_out_u) AS pah_tsfr_out_u,
 SUM(rk_tsfr_in_c) AS rk_tsfr_in_c,
 SUM(rk_tsfr_in_u) AS rk_tsfr_in_u,
 SUM(rk_tsfr_out_c) AS rk_tsfr_out_c,
 SUM(rk_tsfr_out_u) AS rk_tsfr_out_u,
 SUM(rs_tsfr_in_c) AS rs_tsfr_in_c,
 SUM(rs_tsfr_in_u) AS rs_tsfr_in_u,
 SUM(rs_tsfr_out_c) AS rs_tsfr_out_c,
 SUM(rs_tsfr_out_u) AS rs_tsfr_out_u,
 SUM(plan_bop_c) AS plan_bop_c,
 SUM(plan_bop_u) AS plan_bop_u,
 SUM(plan_eop_c) AS plan_eop_c,
 SUM(plan_eop_u) AS plan_eop_u,
 SUM(plan_receipts_c) AS plan_receipts_c,
 SUM(plan_receipts_u) AS plan_receipts_u,
 SUM(plan_receipts_lr_c) AS plan_receipts_lr_c,
 SUM(plan_receipts_lr_u) AS plan_receipts_lr_u,
 SUM(plan_sales_c) AS plan_sales_c,
 SUM(plan_sales_r) AS plan_sales_r,
 SUM(plan_sales_u) AS plan_sales_u,
 SUM(plan_demand_r) AS plan_demand_r,
 SUM(plan_demand_u) AS plan_demand_u
FROM (SELECT *
   FROM sales
   UNION ALL
   SELECT *
   FROM demand
   UNION ALL
   SELECT *
   FROM receipts
   UNION ALL
   SELECT *
   FROM transfers
   UNION ALL
   SELECT *
   FROM eop_ty
   UNION ALL
   SELECT *
   FROM eop_ly
   UNION ALL
   SELECT *
   FROM plan) AS sub
GROUP BY channel_country,
 banner,
 date_ind,
 month_idnt,
 month_label,
 month_idnt_aligned,
 month_label_aligned,
 month_start_day_date,
 month_end_day_date,
 department_num,
 dropship_ind,
 category,
 price_band;


CREATE TEMPORARY TABLE IF NOT EXISTS final_combine
AS
SELECT bs.channel_country,
 bs.banner,
 bs.date_ind,
 bs.month_idnt,
 bs.month_label,
 bs.month_idnt_aligned,
 bs.month_label_aligned,
 bs.month_start_day_date,
 bs.month_end_day_date,
 hr.active_store_ind,
 hr.division_desc,
 hr.subdivision_desc,
 hr.department_desc,
 bs.department_num,
 bs.dropship_ind,
 bs.category,
 ca.category_planner_1,
 ca.category_planner_2,
 ca.category_group,
 ca.seasonal_designation,
 ca.rack_merch_zone,
 ca.is_activewear,
 ca.channel_category_roles_1,
 ca.channel_category_roles_2,
 ca.bargainista_dept_map,
 bs.price_band,
 SUM(bs.net_sls_r) AS net_sls_r,
 SUM(bs.net_sls_c) AS net_sls_c,
 SUM(bs.net_sls_units) AS net_sls_units,
 SUM(bs.net_sls_reg_r) AS net_sls_reg_r,
 SUM(bs.net_sls_reg_c) AS net_sls_reg_c,
 SUM(bs.net_sls_reg_units) AS net_sls_reg_units,
 SUM(bs.returns_r) AS returns_r,
 SUM(bs.returns_c) AS returns_c,
 SUM(bs.returns_u) AS returns_u,
 SUM(bs.demand_ttl_r) AS demand_ttl_r,
 SUM(bs.demand_ttl_units) AS demand_ttl_units,
 SUM(bs.eop_ttl_c) AS eop_ttl_c,
 SUM(bs.eop_ttl_units) AS eop_ttl_units,
 SUM(bs.bop_ttl_c) AS bop_ttl_c,
 SUM(bs.bop_ttl_units) AS bop_ttl_units,
 SUM(bs.ttl_porcpt_c) AS ttl_porcpt_c,
 SUM(bs.ttl_porcpt_u) AS ttl_porcpt_u,
 SUM(bs.ttl_porcpt_rp_c) AS ttl_porcpt_rp_c,
 SUM(bs.ttl_porcpt_rp_u) AS ttl_porcpt_rp_u,
 SUM(bs.ttl_porcpt_nrp_c) AS ttl_porcpt_nrp_c,
 SUM(bs.ttl_porcpt_nrp_u) AS ttl_porcpt_nrp_u,
 SUM(bs.pah_tsfr_in_c) AS pah_tsfr_in_c,
 SUM(bs.pah_tsfr_in_u) AS pah_tsfr_in_u,
 SUM(bs.pah_tsfr_out_c) AS pah_tsfr_out_c,
 SUM(bs.pah_tsfr_out_u) AS pah_tsfr_out_u,
 SUM(bs.rk_tsfr_in_c) AS rk_tsfr_in_c,
 SUM(bs.rk_tsfr_in_u) AS rk_tsfr_in_u,
 SUM(bs.rk_tsfr_out_c) AS rk_tsfr_out_c,
 SUM(bs.rk_tsfr_out_u) AS rk_tsfr_out_u,
 SUM(bs.rs_tsfr_in_c) AS rs_tsfr_in_c,
 SUM(bs.rs_tsfr_in_u) AS rs_tsfr_in_u,
 SUM(bs.rs_tsfr_out_c) AS rs_tsfr_out_c,
 SUM(bs.rs_tsfr_out_u) AS rs_tsfr_out_u,
 SUM(bs.plan_bop_c) AS plan_bop_c,
 SUM(bs.plan_bop_u) AS plan_bop_u,
 SUM(bs.plan_eop_c) AS plan_eop_c,
 SUM(bs.plan_eop_u) AS plan_eop_u,
 SUM(bs.plan_receipts_c) AS plan_receipts_c,
 SUM(bs.plan_receipts_u) AS plan_receipts_u,
 SUM(bs.plan_receipts_lr_c) AS plan_receipts_lr_c,
 SUM(bs.plan_receipts_lr_u) AS plan_receipts_lr_u,
 SUM(bs.plan_sales_c) AS plan_sales_c,
 SUM(bs.plan_sales_r) AS plan_sales_r,
 SUM(bs.plan_sales_u) AS plan_sales_u,
 SUM(bs.plan_demand_r) AS plan_demand_r,
 SUM(bs.plan_demand_u) AS plan_demand_u,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS update_timestamp
FROM combine AS bs
 LEFT JOIN cat_attr AS ca ON bs.department_num = ca.dept_num AND LOWER(bs.category) = LOWER(ca.category)
 LEFT JOIN dept_lkup AS hr ON bs.department_num = hr.dept_num
GROUP BY bs.channel_country,
 bs.banner,
 bs.date_ind,
 bs.month_idnt,
 bs.month_label,
 bs.month_idnt_aligned,
 bs.month_label_aligned,
 bs.month_start_day_date,
 bs.month_end_day_date,
 bs.department_num,
 bs.dropship_ind,
 bs.category,
 bs.price_band,
 ca.category_planner_1,
 ca.category_planner_2,
 ca.category_group,
 ca.seasonal_designation,
 ca.rack_merch_zone,
 ca.is_activewear,
 ca.channel_category_roles_1,
 ca.channel_category_roles_2,
 ca.bargainista_dept_map,
 hr.department_desc,
 hr.division_desc,
 hr.subdivision_desc,
 hr.active_store_ind;


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.apt_is_category;


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.apt_is_category
(SELECT channel_country,
  banner,
  date_ind,
  month_idnt,
  month_label,
  month_idnt_aligned,
  month_label_aligned,
  month_start_day_date,
  month_end_day_date,
  active_store_ind,
  division_desc,
  subdivision_desc,
  department_desc,
  department_num,
  dropship_ind,
  category,
  category_planner_1,
  category_planner_2,
  category_group,
  seasonal_designation,
  rack_merch_zone,
  is_activewear,
  channel_category_roles_1,
  channel_category_roles_2,
  bargainista_dept_map,
  price_band,
  net_sls_r,
  net_sls_c,
  CAST(TRUNC(net_sls_units) AS INTEGER) AS net_sls_units,
  net_sls_reg_r,
  net_sls_reg_c,
  CAST(TRUNC(net_sls_reg_units) AS INTEGER) AS net_sls_reg_units,
  returns_r,
  returns_c,
  CAST(TRUNC(returns_u) AS INTEGER) AS returns_u,
  demand_ttl_r,
  CAST(TRUNC(demand_ttl_units) AS INTEGER) AS demand_ttl_units,
  eop_ttl_c,
  CAST(TRUNC(eop_ttl_units) AS INTEGER) AS eop_ttl_units,
  bop_ttl_c,
  CAST(TRUNC(bop_ttl_units) AS INTEGER) AS bop_ttl_units,
  ttl_porcpt_c,
  CAST(TRUNC(ttl_porcpt_u) AS INTEGER) AS ttl_porcpt_u,
  ttl_porcpt_rp_c,
  CAST(TRUNC(ttl_porcpt_rp_u) AS INTEGER) AS ttl_porcpt_rp_u,
  ttl_porcpt_nrp_c,
  CAST(TRUNC(ttl_porcpt_nrp_u) AS INTEGER) AS ttl_porcpt_nrp_u,
  pah_tsfr_in_c,
  CAST(TRUNC(pah_tsfr_in_u) AS INTEGER) AS pah_tsfr_in_u,
  pah_tsfr_out_c,
  CAST(TRUNC(pah_tsfr_out_u) AS INTEGER) AS pah_tsfr_out_u,
  rk_tsfr_in_c,
  CAST(TRUNC(rk_tsfr_in_u) AS INTEGER) AS rk_tsfr_in_u,
  rk_tsfr_out_c,
  CAST(TRUNC(rk_tsfr_out_u) AS INTEGER) AS rk_tsfr_out_u,
  rs_tsfr_in_c,
  CAST(TRUNC(rs_tsfr_in_u) AS INTEGER) AS rs_tsfr_in_u,
  rs_tsfr_out_c,
  CAST(TRUNC(rs_tsfr_out_u) AS INTEGER) AS rs_tsfr_out_u,
  plan_bop_c,
  CAST(TRUNC(plan_bop_u) AS INTEGER) AS plan_bop_u,
  plan_eop_c,
  CAST(TRUNC(plan_eop_u) AS INTEGER) AS plan_eop_u,
  plan_receipts_c,
  CAST(TRUNC(plan_receipts_u) AS INTEGER) AS plan_receipts_u,
  plan_receipts_lr_c,
  CAST(TRUNC(plan_receipts_lr_u) AS INTEGER) AS plan_receipts_lr_u,
  plan_sales_c,
  plan_sales_r,
  CAST(TRUNC(plan_sales_u) AS INTEGER) AS plan_sales_u,
  plan_demand_r,
  CAST(TRUNC(plan_demand_u) AS INTEGER) AS plan_demand_u,
  CAST(update_timestamp AS TIMESTAMP) AS update_timestamp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(update_timestamp AS STRING)) as update_timestamp_tz
 FROM final_combine);
