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


--COLLECT STATS 	PRIMARY INDEX (date_ind, week_idnt) 		ON date_lkup


CREATE TEMPORARY TABLE IF NOT EXISTS date_lkup_acts
AS 
SELECT *
FROM date_lkup
WHERE LOWER(date_ind) IN (LOWER('TY'), LOWER('LY'), LOWER('LY MTH'));


--COLLECT STATS 	PRIMARY INDEX (week_idnt) 		ON date_lkup_acts


CREATE TEMPORARY TABLE IF NOT EXISTS date_lkup_acts_ty
AS 
SELECT *
FROM date_lkup
WHERE LOWER(date_ind) IN (LOWER('TY'));


--COLLECT STATS 	PRIMARY INDEX (week_idnt) 		ON date_lkup_acts_ty


CREATE TEMPORARY TABLE IF NOT EXISTS date_lkup_acts_ly
AS 
SELECT *
FROM date_lkup
WHERE LOWER(date_ind) IN (LOWER('LY'), LOWER('LY MTH'));


--COLLECT STATS 	PRIMARY INDEX (week_idnt) 		ON date_lkup_acts_ly


CREATE TEMPORARY TABLE IF NOT EXISTS date_lkup_mth
AS 
SELECT DISTINCT date_ind,
 month_idnt,
 month_idnt_aligned,
 month_label,
 month_label_aligned,
 month_start_day_date,
 month_end_day_date,
 mtd_end_date,
 mtd_week_idnt,
 mbeg_week_idnt,
 spend_month
FROM date_lkup;


--COLLECT STATS 	PRIMARY INDEX (date_ind, month_idnt) 	,COLUMN (spend_month) 		ON date_lkup_mth


CREATE TEMPORARY TABLE IF NOT EXISTS date_eop
AS 
SELECT eop.eop_month_idnt,
 dt.date_ind,
 dt.month_idnt,
 dt.month_idnt_aligned,
 dt.month_label,
 dt.month_label_aligned,
 dt.month_start_day_date,
 dt.month_end_day_date,
 dt.mtd_end_date,
 dt.mtd_week_idnt,
 dt.mbeg_week_idnt,
 dt.spend_month
FROM (SELECT month_idnt AS eop_month_idnt,
   LAG(month_idnt, 1) OVER (ORDER BY month_idnt) AS month_idnt
  FROM date_lkup_mth
  WHERE LOWER(date_ind) = LOWER('PL MTH')) AS eop
 LEFT JOIN date_lkup_mth AS dt ON eop.month_idnt = dt.month_idnt AND LOWER(dt.date_ind) = LOWER('PL MTH')
WHERE dt.month_idnt IS NOT NULL;


--COLLECT STATS 	PRIMARY INDEX (eop_month_idnt) 	,COLUMN (month_idnt) 		ON date_eop 


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


--COLLECT STATS 	PRIMARY INDEX (dept_num) 		ON dept_lkup


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


--COLLECT STATS 	PRIMARY INDEX (dept_num, rms_sku_num) 		ON cat_lkup


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


--COLLECT STATS 	PRIMARY INDEX (category, dept_num) 		ON cat_attr


CREATE TEMPORARY TABLE IF NOT EXISTS sup_attr
AS 
SELECT CASE
  WHEN LOWER(sp.banner) = LOWER('FP')
  THEN 'NORDSTROM'
  ELSE 'NORDSTROM_RACK'
  END AS banner,
 sp.dept_num,
 sp.supplier_group,
 MAX(sp.buy_planner) AS buy_planner,
 MAX(sp.preferred_partner_desc) AS preferred_partner_desc,
 MAX(sp.areas_of_responsibility) AS areas_of_responsibility,
 MAX(sp.is_npg) AS is_npg,
 MAX(sp.diversity_group) AS diversity_group,
 MAX(sp.nord_to_rack_transfer_rate) AS nord_to_rack_transfer_rate,
  CASE
  WHEN MAX(CASE
     WHEN LOWER(npg.npg_flag) = LOWER('Y')
     THEN 1
     ELSE 0
     END) = 1
  THEN 'Y'
  ELSE 'N'
  END AS npg_ind
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.supp_dept_map_dim AS sp
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS npg ON LOWER(sp.supplier_num) = LOWER(npg.vendor_num)
GROUP BY banner,
 sp.dept_num,
 sp.supplier_group;


--COLLECT STATS 	PRIMARY INDEX (banner, dept_num, supplier_group) 		ON sup_attr


CREATE TEMPORARY TABLE IF NOT EXISTS store_lkup
AS 
SELECT DISTINCT store_num,
 channel_country,
 channel_num,
 TRIM(TRIM(FORMAT('%11d', channel_num)) || ', ' || TRIM(channel_desc)) AS channel_label,
 channel_brand AS banner
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw
WHERE channel_num NOT IN (580);


--COLLECT STATS 	PRIMARY INDEX (store_num, channel_num) 		ON store_lkup


CREATE TEMPORARY TABLE IF NOT EXISTS store_lkup_eop
AS 
SELECT *
FROM store_lkup
WHERE channel_num IN (110, 120, 111, 121, 210, 310, 311, 260, 261, 211, 250);


--COLLECT STATS 	PRIMARY INDEX (store_num)  		ON store_lkup_eop


CREATE TEMPORARY TABLE IF NOT EXISTS cluster_lkup
AS 
SELECT DISTINCT cluster_name,
  CASE
  WHEN LOWER(cluster_name) = LOWER('NORDSTROM_CANADA_STORES')
  THEN 111
  WHEN LOWER(cluster_name) = LOWER('NORDSTROM_CANADA_ONLINE')
  THEN 121
  WHEN LOWER(cluster_name) = LOWER('NORDSTROM_STORES')
  THEN 110
  WHEN LOWER(cluster_name) = LOWER('NORDSTROM_ONLINE')
  THEN 120
  WHEN LOWER(cluster_name) = LOWER('RACK_ONLINE')
  THEN 250
  WHEN LOWER(cluster_name) = LOWER('RACK_CANADA_STORES')
  THEN 211
  WHEN LOWER(cluster_name) IN (LOWER('RACK STORES'), LOWER('PRICE'), LOWER('HYBRID'), LOWER('BRAND'))
  THEN 210
  WHEN LOWER(cluster_name) = LOWER('NORD CA RSWH')
  THEN 311
  WHEN LOWER(cluster_name) = LOWER('NORD US RSWH')
  THEN 310
  WHEN LOWER(cluster_name) = LOWER('RACK CA RSWH')
  THEN 261
  WHEN LOWER(cluster_name) = LOWER('RACK US RSWH')
  THEN 260
  ELSE NULL
  END AS chnl_idnt
FROM `{{params.gcp_project_id}}`.t2dl_das_apt_cost_reporting.merch_assortment_supplier_cluster_plan_op_fact;


--COLLECT STATS 	PRIMARY INDEX (cluster_name)  		ON cluster_lkup


CREATE TEMPORARY TABLE IF NOT EXISTS rsb_lkup
AS 
SELECT DISTINCT supplier_group,
 CAST(TRUNC(CAST(dept_num AS FLOAT64)) AS INTEGER) AS dept_num
FROM `{{params.gcp_project_id}}`.t2dl_das_in_season_management_reporting.strat_brands_supp_curr_vw
WHERE LOWER(sb_banner) = LOWER('NORDSTROM_RACK');


--COLLECT STATS 	PRIMARY INDEX (supplier_group, dept_num)  		ON rsb_lkup 


CREATE TEMPORARY TABLE IF NOT EXISTS plan
AS 
SELECT fct.country AS channel_country,
 fct.banner,
 fct.chnl_idnt AS channel_num,
 ch.channel_label,
 dt.date_ind,
 dt.month_idnt,
 dt.month_label,
 dt.month_idnt_aligned,
 dt.month_label_aligned,
 dt.month_start_day_date,
 dt.month_end_day_date,
 CAST(TRUNC(CAST(fct.dept_idnt AS FLOAT64)) AS INTEGER) AS department_num,
  CASE
  WHEN LOWER(fct.alternate_inventory_model) = LOWER('DROPSHIP')
  THEN 'Y'
  ELSE 'N'
  END AS dropship_ind,
 fct.category,
 fct.supplier_group,
 RPAD('N', 1, ' ') AS fanatics_ind,
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
 0 AS ttl_porcpt_r,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_u,
 0 AS ttl_porcpt_rp_r,
 0 AS ttl_porcpt_rp_c,
 0 AS ttl_porcpt_rp_u,
 0 AS ttl_porcpt_nrp_r,
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
 0 AS rp_oo_r,
 0 AS rp_oo_c,
 0 AS rp_oo_u,
 0 AS nrp_oo_r,
 0 AS nrp_oo_c,
 0 AS nrp_oo_u,
 0 AS rp_cm_r,
 0 AS rp_cm_c,
 0 AS rp_cm_u,
 0 AS nrp_cm_r,
 0 AS nrp_cm_c,
 0 AS nrp_cm_u,
 0 AS rp_ant_spd_r,
 0 AS rp_ant_spd_c,
 0 AS rp_ant_spd_u,
 SUM(fct.plan_rp_rcpt_lr_c) AS rp_plan_receipts_lr_c,
 SUM(fct.plan_rp_rcpt_lr_u) AS rp_plan_receipts_lr_u,
 SUM(fct.plan_nrp_rcpt_lr_c) AS nrp_plan_receipts_lr_c,
 SUM(fct.plan_nrp_rcpt_lr_u) AS nrp_plan_receipts_lr_u,
 SUM(fct.plan_pah_in_c) AS plan_pah_in_c,
 SUM(fct.plan_pah_in_u) AS plan_pah_in_u,
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
 SUM(fct.demand_units) AS plan_demand_u,
 0 AS plan_op_bop_c,
 0 AS plan_op_bop_u,
 0 AS plan_op_eop_c,
 0 AS plan_op_eop_u,
 0 AS plan_op_receipts_c,
 0 AS plan_op_receipts_u,
 0 AS plan_op_receipts_lr_c,
 0 AS plan_op_receipts_lr_u,
 0 AS plan_op_sales_c,
 0 AS plan_op_sales_r,
 0 AS plan_op_sales_u,
 0 AS plan_op_demand_r,
 0 AS plan_op_demand_u
FROM `{{params.gcp_project_id}}`.t2dl_das_apt_cost_reporting.suppliergroup_channel_cost_plans_weekly AS fct
 INNER JOIN date_lkup AS dt ON fct.week_idnt = dt.week_idnt AND LOWER(dt.date_ind) IN (LOWER('PL MTD'), LOWER('PL MTH')
    )
 INNER JOIN (SELECT DISTINCT channel_country,
   banner,
   channel_num,
   channel_label
  FROM store_lkup) AS ch ON fct.chnl_idnt = ch.channel_num
GROUP BY channel_country,
 fct.banner,
 channel_num,
 ch.channel_label,
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
 fct.supplier_group,
 fanatics_ind,
 net_sls_r,
 net_sls_c;


--COLLECT STATS 	PRIMARY INDEX (date_ind, month_idnt, banner, channel_country, channel_num, department_num, category, supplier_group)  		ON plan


CREATE TEMPORARY TABLE IF NOT EXISTS plan_op
AS 
SELECT fct.selling_country AS channel_country,
 fct.selling_brand AS banner,
 ch.channel_num,
 ch.channel_label,
 dt.date_ind,
 dt.month_idnt,
 dt.month_label,
 dt.month_idnt_aligned,
 dt.month_label_aligned,
 dt.month_start_day_date,
 dt.month_end_day_date,
 fct.dept_idnt AS department_num,
  CASE
  WHEN LOWER(fct.alternate_inventory_model) = LOWER('DROPSHIP')
  THEN 'Y'
  ELSE 'N'
  END AS dropship_ind,
 fct.category,
 fct.supplier_group,
 RPAD('N', 1, ' ') AS fanatics_ind,
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
 0 AS ttl_porcpt_r,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_u,
 0 AS ttl_porcpt_rp_r,
 0 AS ttl_porcpt_rp_c,
 0 AS ttl_porcpt_rp_u,
 0 AS ttl_porcpt_nrp_r,
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
 0 AS rp_oo_r,
 0 AS rp_oo_c,
 0 AS rp_oo_u,
 0 AS nrp_oo_r,
 0 AS nrp_oo_c,
 0 AS nrp_oo_u,
 0 AS rp_cm_r,
 0 AS rp_cm_c,
 0 AS rp_cm_u,
 0 AS nrp_cm_r,
 0 AS nrp_cm_c,
 0 AS nrp_cm_u,
 0 AS rp_ant_spd_r,
 0 AS rp_ant_spd_c,
 0 AS rp_ant_spd_u,
 0 AS rp_plan_receipts_lr_c,
 0 AS rp_plan_receipts_lr_u,
 0 AS nrp_plan_receipts_lr_c,
 0 AS nrp_plan_receipts_lr_u,
 0 AS plan_pah_in_c,
 0 AS plan_pah_in_u,
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
 0 AS plan_demand_u,
 SUM(fct.beginning_of_period_inventory_cost_amount) AS plan_op_bop_c,
 SUM(fct.beginning_of_period_inventory_units) AS plan_op_bop_u,
 0 AS plan_op_eop_c,
 0 AS plan_op_eop_u,
 SUM(fct.plannable_inventory_cost_amount) AS plan_op_receipts_c,
 SUM(fct.plannable_inventory_units) AS plan_op_receipts_u,
 SUM(fct.plannable_inventory_receipt_less_reserve_cost_amount) AS plan_op_receipts_lr_c,
 SUM(fct.plannable_inventory_receipt_less_reserve_units) AS plan_op_receipts_lr_u,
 SUM(fct.net_sales_cost_amount) AS plan_op_sales_c,
 SUM(fct.net_sales_retail_amount) AS plan_op_sales_r,
 SUM(fct.net_sales_units) AS plan_op_sales_u,
 SUM(fct.demand_dollar_amount) AS plan_op_demand_r,
 SUM(fct.demand_units) AS plan_op_demand_u
FROM `{{params.gcp_project_id}}`.t2dl_das_apt_cost_reporting.merch_assortment_supplier_cluster_plan_op_fact AS fct
 INNER JOIN date_lkup_mth AS dt ON fct.fiscal_month_idnt = dt.month_idnt AND LOWER(dt.date_ind) IN (LOWER('PL MTH'))
 INNER JOIN cluster_lkup AS cl ON LOWER(fct.cluster_name) = LOWER(cl.cluster_name)
 INNER JOIN (SELECT DISTINCT channel_country,
   banner,
   channel_num,
   channel_label
  FROM store_lkup) AS ch ON cl.chnl_idnt = ch.channel_num
GROUP BY channel_country,
 banner,
 ch.channel_num,
 ch.channel_label,
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
 fct.supplier_group
UNION ALL
SELECT fct0.selling_country AS channel_country,
 fct0.selling_brand AS banner,
 ch.channel_num,
 ch.channel_label,
 dte.date_ind,
 dte.month_idnt,
 dte.month_label,
 dte.month_idnt_aligned,
 dte.month_label_aligned,
 dte.month_start_day_date,
 dte.month_end_day_date,
 fct0.dept_idnt AS department_num,
  CASE
  WHEN LOWER(fct0.alternate_inventory_model) = LOWER('DROPSHIP')
  THEN 'Y'
  ELSE 'N'
  END AS dropship_ind,
 fct0.category,
 fct0.supplier_group,
 RPAD('N', 1, ' ') AS fanatics_ind,
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
 0 AS ttl_porcpt_r,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_u,
 0 AS ttl_porcpt_rp_r,
 0 AS ttl_porcpt_rp_c,
 0 AS ttl_porcpt_rp_u,
 0 AS ttl_porcpt_nrp_r,
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
 0 AS rp_oo_r,
 0 AS rp_oo_c,
 0 AS rp_oo_u,
 0 AS nrp_oo_r,
 0 AS nrp_oo_c,
 0 AS nrp_oo_u,
 0 AS rp_cm_r,
 0 AS rp_cm_c,
 0 AS rp_cm_u,
 0 AS nrp_cm_r,
 0 AS nrp_cm_c,
 0 AS nrp_cm_u,
 0 AS rp_ant_spd_r,
 0 AS rp_ant_spd_c,
 0 AS rp_ant_spd_u,
 0 AS rp_plan_receipts_lr_c,
 0 AS rp_plan_receipts_lr_u,
 0 AS nrp_plan_receipts_lr_c,
 0 AS nrp_plan_receipts_lr_u,
 0 AS plan_pah_in_c,
 0 AS plan_pah_in_u,
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
 0 AS plan_demand_u,
 0 AS plan_op_bop_c,
 0 AS plan_op_bop_u,
 SUM(fct0.beginning_of_period_inventory_cost_amount) AS plan_op_eop_c,
 SUM(fct0.beginning_of_period_inventory_units) AS plan_op_eop_u,
 0 AS plan_op_receipts_c,
 0 AS plan_op_receipts_u,
 0 AS plan_op_receipts_lr_c,
 0 AS plan_op_receipts_lr_u,
 0 AS plan_op_sales_c,
 0 AS plan_op_sales_r,
 0 AS plan_op_sales_u,
 0 AS plan_op_demand_r,
 0 AS plan_op_demand_u
FROM `{{params.gcp_project_id}}`.t2dl_das_apt_cost_reporting.merch_assortment_supplier_cluster_plan_op_fact AS fct0
 INNER JOIN date_eop AS dte ON fct0.fiscal_month_idnt = dte.eop_month_idnt
 INNER JOIN cluster_lkup AS cl0 ON LOWER(fct0.cluster_name) = LOWER(cl0.cluster_name)
 INNER JOIN (SELECT DISTINCT channel_country,
   banner,
   channel_num,
   channel_label
  FROM store_lkup) AS ch ON cl0.chnl_idnt = ch.channel_num
GROUP BY channel_country,
 banner,
 ch.channel_num,
 ch.channel_label,
 dte.date_ind,
 dte.month_idnt,
 dte.month_label,
 dte.month_idnt_aligned,
 dte.month_label_aligned,
 dte.month_start_day_date,
 dte.month_end_day_date,
 department_num,
 dropship_ind,
 fct0.category,
 fct0.supplier_group;


--COLLECT STATS 	PRIMARY INDEX (date_ind, month_idnt, banner, channel_country, channel_num, department_num, category, supplier_group)  		ON plan_op


CREATE TEMPORARY TABLE IF NOT EXISTS sales
AS 
SELECT st.channel_country,
 st.banner,
 st.channel_num,
 st.channel_label,
 dt.date_ind,
 dt.month_idnt,
 dt.month_label,
 dt.month_idnt_aligned,
 dt.month_label_aligned,
 dt.month_start_day_date,
 dt.month_end_day_date,
 fct.department_num,
 fct.dropship_ind,
  CASE
  WHEN ah.quantrix_category IS NOT NULL
  THEN ah.quantrix_category
  ELSE 'OTHER'
  END AS category,
  CASE
  WHEN LOWER(st.banner) = LOWER('NORDSTROM')
  THEN ah.fp_supplier_group
  WHEN LOWER(st.banner) = LOWER('NORDSTROM_RACK')
  THEN ah.op_supplier_group
  ELSE 'OTHER'
  END AS supplier_group,
  CASE
  WHEN MAX(ah.fanatics_ind) = 1
  THEN 'Y'
  ELSE 'N'
  END AS fanatics_ind,
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
   WHEN st.channel_num IN (110, 111, 210, 211)
   THEN fct.gross_sales_tot_retl
   ELSE 0
   END) AS demand_ttl_r,
 SUM(CASE
   WHEN st.channel_num IN (110, 111, 210, 211)
   THEN fct.gross_sales_tot_units
   ELSE 0
   END) AS demand_ttl_units,
 0 AS eop_ttl_c,
 0 AS eop_ttl_units,
 0 AS bop_ttl_c,
 0 AS bop_ttl_units,
 0 AS ttl_porcpt_r,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_u,
 0 AS ttl_porcpt_rp_r,
 0 AS ttl_porcpt_rp_c,
 0 AS ttl_porcpt_rp_u,
 0 AS ttl_porcpt_nrp_r,
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
 0 AS rp_oo_r,
 0 AS rp_oo_c,
 0 AS rp_oo_u,
 0 AS nrp_oo_r,
 0 AS nrp_oo_c,
 0 AS nrp_oo_u,
 0 AS rp_cm_r,
 0 AS rp_cm_c,
 0 AS rp_cm_u,
 0 AS nrp_cm_r,
 0 AS nrp_cm_c,
 0 AS nrp_cm_u,
 0 AS rp_ant_spd_r,
 0 AS rp_ant_spd_c,
 0 AS rp_ant_spd_u,
 0 AS rp_plan_receipts_lr_c,
 0 AS rp_plan_receipts_lr_u,
 0 AS nrp_plan_receipts_lr_c,
 0 AS nrp_plan_receipts_lr_u,
 0 AS plan_pah_in_c,
 0 AS plan_pah_in_u,
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
 0 AS plan_demand_u,
 0 AS plan_op_bop_c,
 0 AS plan_op_bop_u,
 0 AS plan_op_eop_c,
 0 AS plan_op_eop_u,
 0 AS plan_op_receipts_c,
 0 AS plan_op_receipts_u,
 0 AS plan_op_receipts_lr_c,
 0 AS plan_op_receipts_lr_u,
 0 AS plan_op_sales_c,
 0 AS plan_op_sales_r,
 0 AS plan_op_sales_u,
 0 AS plan_op_demand_r,
 0 AS plan_op_demand_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_sale_return_sku_store_week_fact_vw AS fct
 INNER JOIN date_lkup_acts AS dt ON fct.week_num = dt.week_idnt_true
 INNER JOIN store_lkup AS st ON fct.store_num = st.store_num
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.assortment_hierarchy AS ah ON LOWER(fct.rms_sku_num) = LOWER(ah.sku_idnt) AND LOWER(st
    .channel_country) = LOWER(ah.channel_country)
GROUP BY st.channel_country,
 st.banner,
 st.channel_num,
 st.channel_label,
 dt.date_ind,
 dt.month_idnt,
 dt.month_label,
 dt.month_idnt_aligned,
 dt.month_label_aligned,
 dt.month_start_day_date,
 dt.month_end_day_date,
 fct.department_num,
 fct.dropship_ind,
 category,
 supplier_group;


--COLLECT STATS 	PRIMARY INDEX (date_ind, month_idnt, banner, channel_country, channel_num, department_num, category, supplier_group) 		ON sales


CREATE TEMPORARY TABLE IF NOT EXISTS demand
AS 
SELECT st.channel_country,
 st.banner,
 st.channel_num,
 st.channel_label,
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
  CASE
  WHEN ah.quantrix_category IS NOT NULL
  THEN ah.quantrix_category
  ELSE 'OTHER'
  END AS category,
  CASE
  WHEN LOWER(st.banner) = LOWER('NORDSTROM')
  THEN ah.fp_supplier_group
  WHEN LOWER(st.banner) = LOWER('NORDSTROM_RACK')
  THEN ah.op_supplier_group
  ELSE 'OTHER'
  END AS supplier_group,
  CASE
  WHEN MAX(ah.fanatics_ind) = 1
  THEN 'Y'
  ELSE 'N'
  END AS fanatics_ind,
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
 0 AS ttl_porcpt_r,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_u,
 0 AS ttl_porcpt_rp_r,
 0 AS ttl_porcpt_rp_c,
 0 AS ttl_porcpt_rp_u,
 0 AS ttl_porcpt_nrp_r,
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
 0 AS rp_oo_r,
 0 AS rp_oo_c,
 0 AS rp_oo_u,
 0 AS nrp_oo_r,
 0 AS nrp_oo_c,
 0 AS nrp_oo_u,
 0 AS rp_cm_r,
 0 AS rp_cm_c,
 0 AS rp_cm_u,
 0 AS nrp_cm_r,
 0 AS nrp_cm_c,
 0 AS nrp_cm_u,
 0 AS rp_ant_spd_r,
 0 AS rp_ant_spd_c,
 0 AS rp_ant_spd_u,
 0 AS rp_plan_receipts_lr_c,
 0 AS rp_plan_receipts_lr_u,
 0 AS nrp_plan_receipts_lr_c,
 0 AS nrp_plan_receipts_lr_u,
 0 AS plan_pah_in_c,
 0 AS plan_pah_in_u,
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
 0 AS plan_demand_u,
 0 AS plan_op_bop_c,
 0 AS plan_op_bop_u,
 0 AS plan_op_eop_c,
 0 AS plan_op_eop_u,
 0 AS plan_op_receipts_c,
 0 AS plan_op_receipts_u,
 0 AS plan_op_receipts_lr_c,
 0 AS plan_op_receipts_lr_u,
 0 AS plan_op_sales_c,
 0 AS plan_op_sales_r,
 0 AS plan_op_sales_u,
 0 AS plan_op_demand_r,
 0 AS plan_op_demand_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_demand_sku_store_week_agg_fact_vw AS fct
 INNER JOIN date_lkup_acts AS dt ON fct.week_num = dt.week_idnt_true
 INNER JOIN store_lkup AS st ON fct.store_num = st.store_num
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.assortment_hierarchy AS ah ON LOWER(fct.rms_sku_num) = LOWER(ah.sku_idnt) AND LOWER(st
    .channel_country) = LOWER(ah.channel_country)
GROUP BY st.channel_country,
 st.banner,
 st.channel_num,
 st.channel_label,
 dt.date_ind,
 dt.month_idnt,
 dt.month_label,
 dt.month_idnt_aligned,
 dt.month_label_aligned,
 dt.month_start_day_date,
 dt.month_end_day_date,
 fct.department_num,
 dropship_ind,
 category,
 supplier_group;


--COLLECT STATS 	PRIMARY INDEX (date_ind, month_idnt, banner, channel_country, channel_num, department_num, category, supplier_group) 		ON demand


CREATE TEMPORARY TABLE IF NOT EXISTS receipts
AS 
SELECT st.channel_country,
 st.banner,
 st.channel_num,
 st.channel_label,
 dt.date_ind,
 dt.month_idnt,
 dt.month_label,
 dt.month_idnt_aligned,
 dt.month_label_aligned,
 dt.month_start_day_date,
 dt.month_end_day_date,
 fct.department_num,
 fct.dropship_ind,
  CASE
  WHEN ah.quantrix_category IS NOT NULL
  THEN ah.quantrix_category
  ELSE 'OTHER'
  END AS category,
  CASE
  WHEN LOWER(st.banner) = LOWER('NORDSTROM')
  THEN ah.fp_supplier_group
  WHEN LOWER(st.banner) = LOWER('NORDSTROM_RACK')
  THEN ah.op_supplier_group
  ELSE 'OTHER'
  END AS supplier_group,
  CASE
  WHEN MAX(ah.fanatics_ind) = 1
  THEN 'Y'
  ELSE 'N'
  END AS fanatics_ind,
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
   THEN fct.receipts_regular_retail + fct.receipts_clearance_retail + fct.receipts_crossdock_regular_retail + fct.receipts_crossdock_clearance_retail
    
   ELSE 0
   END) AS ttl_porcpt_r,
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
   THEN fct.receipts_regular_retail + fct.receipts_clearance_retail + fct.receipts_crossdock_regular_retail + fct.receipts_crossdock_clearance_retail
    
   ELSE 0
   END) AS ttl_porcpt_rp_r,
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
   THEN fct.receipts_regular_retail + fct.receipts_clearance_retail + fct.receipts_crossdock_regular_retail + fct.receipts_crossdock_clearance_retail
    
   ELSE 0
   END) AS ttl_porcpt_nrp_r,
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
 0 AS rp_oo_r,
 0 AS rp_oo_c,
 0 AS rp_oo_u,
 0 AS nrp_oo_r,
 0 AS nrp_oo_c,
 0 AS nrp_oo_u,
 0 AS rp_cm_r,
 0 AS rp_cm_c,
 0 AS rp_cm_u,
 0 AS nrp_cm_r,
 0 AS nrp_cm_c,
 0 AS nrp_cm_u,
 0 AS rp_ant_spd_r,
 0 AS rp_ant_spd_c,
 0 AS rp_ant_spd_u,
 0 AS rp_plan_receipts_lr_c,
 0 AS rp_plan_receipts_lr_u,
 0 AS nrp_plan_receipts_lr_c,
 0 AS nrp_plan_receipts_lr_u,
 0 AS plan_pah_in_c,
 0 AS plan_pah_in_u,
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
 0 AS plan_demand_u,
 0 AS plan_op_bop_c,
 0 AS plan_op_bop_u,
 0 AS plan_op_eop_c,
 0 AS plan_op_eop_u,
 0 AS plan_op_receipts_c,
 0 AS plan_op_receipts_u,
 0 AS plan_op_receipts_lr_c,
 0 AS plan_op_receipts_lr_u,
 0 AS plan_op_sales_c,
 0 AS plan_op_sales_r,
 0 AS plan_op_sales_u,
 0 AS plan_op_demand_r,
 0 AS plan_op_demand_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_poreceipt_sku_store_week_fact_vw AS fct
 INNER JOIN date_lkup_acts AS dt ON fct.week_num = dt.week_idnt_true
 INNER JOIN store_lkup AS st ON fct.store_num = st.store_num AND st.channel_num IN (110, 120, 111, 121, 210, 310, 311,
    260, 261, 211, 250)
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.assortment_hierarchy AS ah ON LOWER(fct.rms_sku_num) = LOWER(ah.sku_idnt) AND LOWER(st
    .channel_country) = LOWER(ah.channel_country)
GROUP BY st.channel_country,
 st.banner,
 st.channel_num,
 st.channel_label,
 dt.date_ind,
 dt.month_idnt,
 dt.month_label,
 dt.month_idnt_aligned,
 dt.month_label_aligned,
 dt.month_start_day_date,
 dt.month_end_day_date,
 fct.department_num,
 fct.dropship_ind,
 category,
 supplier_group;


--COLLECT STATS 	PRIMARY INDEX (date_ind, month_idnt, banner, channel_country, channel_num, department_num, category, supplier_group) 		ON receipts


CREATE TEMPORARY TABLE IF NOT EXISTS transfers
AS 
SELECT st.channel_country,
 st.banner,
 st.channel_num,
 st.channel_label,
 dt.date_ind,
 dt.month_idnt,
 dt.month_label,
 dt.month_idnt_aligned,
 dt.month_label_aligned,
 dt.month_start_day_date,
 dt.month_end_day_date,
 fct.department_num,
 RPAD(NULL, 1, ' ') AS dropship_ind,
  CASE
  WHEN ah.quantrix_category IS NOT NULL
  THEN ah.quantrix_category
  ELSE 'OTHER'
  END AS category,
  CASE
  WHEN LOWER(st.banner) = LOWER('NORDSTROM')
  THEN ah.fp_supplier_group
  WHEN LOWER(st.banner) = LOWER('NORDSTROM_RACK')
  THEN ah.op_supplier_group
  ELSE 'OTHER'
  END AS supplier_group,
  CASE
  WHEN MAX(ah.fanatics_ind) = 1
  THEN 'Y'
  ELSE 'N'
  END AS fanatics_ind,
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
 0 AS ttl_porcpt_r,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_u,
 0 AS ttl_porcpt_rp_r,
 0 AS ttl_porcpt_rp_c,
 0 AS ttl_porcpt_rp_u,
 0 AS ttl_porcpt_nrp_r,
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
 0 AS rp_oo_r,
 0 AS rp_oo_c,
 0 AS rp_oo_u,
 0 AS nrp_oo_r,
 0 AS nrp_oo_c,
 0 AS nrp_oo_u,
 0 AS rp_cm_r,
 0 AS rp_cm_c,
 0 AS rp_cm_u,
 0 AS nrp_cm_r,
 0 AS nrp_cm_c,
 0 AS nrp_cm_u,
 0 AS rp_ant_spd_r,
 0 AS rp_ant_spd_c,
 0 AS rp_ant_spd_u,
 0 AS rp_plan_receipts_lr_c,
 0 AS rp_plan_receipts_lr_u,
 0 AS nrp_plan_receipts_lr_c,
 0 AS nrp_plan_receipts_lr_u,
 0 AS plan_pah_in_c,
 0 AS plan_pah_in_u,
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
 0 AS plan_demand_u,
 0 AS plan_op_bop_c,
 0 AS plan_op_bop_u,
 0 AS plan_op_eop_c,
 0 AS plan_op_eop_u,
 0 AS plan_op_receipts_c,
 0 AS plan_op_receipts_u,
 0 AS plan_op_receipts_lr_c,
 0 AS plan_op_receipts_lr_u,
 0 AS plan_op_sales_c,
 0 AS plan_op_sales_r,
 0 AS plan_op_sales_u,
 0 AS plan_op_demand_r,
 0 AS plan_op_demand_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_transfer_sku_loc_week_agg_fact_vw AS fct
 INNER JOIN date_lkup_acts AS dt ON fct.week_num = dt.week_idnt_true
 INNER JOIN store_lkup AS st ON fct.store_num = st.store_num AND st.channel_num IN (110, 120, 111, 121, 210, 310, 311,
    260, 261, 211, 250)
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.assortment_hierarchy AS ah ON LOWER(fct.rms_sku_num) = LOWER(ah.sku_idnt) AND LOWER(st
    .channel_country) = LOWER(ah.channel_country)
GROUP BY st.channel_country,
 st.banner,
 st.channel_num,
 st.channel_label,
 dt.date_ind,
 dt.month_idnt,
 dt.month_label,
 dt.month_idnt_aligned,
 dt.month_label_aligned,
 dt.month_start_day_date,
 dt.month_end_day_date,
 fct.department_num,
 dropship_ind,
 category,
 supplier_group;


--COLLECT STATS 	PRIMARY INDEX (date_ind, month_idnt, banner, channel_country, channel_num, department_num, category, supplier_group) 		ON transfers


CREATE TEMPORARY TABLE IF NOT EXISTS eop_stage_ty
AS 
SELECT fct.rms_sku_num,
 st.channel_country,
 st.banner,
 st.channel_num,
 st.channel_label,
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
 st.channel_num,
 st.channel_label,
 dt.date_ind,
 dt.month_idnt,
 dt.month_label,
 dt.month_idnt_aligned,
 dt.month_label_aligned,
 dt.month_start_day_date,
 dt.month_end_day_date,
 fct.department_num;


--COLLECT STATS 	PRIMARY INDEX (rms_sku_num, channel_country)  		ON eop_stage_ty


CREATE TEMPORARY TABLE IF NOT EXISTS eop_stage_ly
AS 
SELECT fct.rms_sku_num,
 st.channel_country,
 st.banner,
 st.channel_num,
 st.channel_label,
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
 st.channel_num,
 st.channel_label,
 dt.date_ind,
 dt.month_idnt,
 dt.month_label,
 dt.month_idnt_aligned,
 dt.month_label_aligned,
 dt.month_start_day_date,
 dt.month_end_day_date,
 fct.department_num;


--COLLECT STATS 	PRIMARY INDEX (rms_sku_num, channel_country)  		ON eop_stage_ly


CREATE TEMPORARY TABLE IF NOT EXISTS eop_ty
AS 
SELECT fct.channel_country,
 fct.banner,
 fct.channel_num,
 fct.channel_label,
 fct.date_ind,
 fct.month_idnt,
 fct.month_label,
 fct.month_idnt_aligned,
 fct.month_label_aligned,
 fct.month_start_day_date,
 fct.month_end_day_date,
 fct.department_num,
 RPAD(NULL, 1, ' ') AS dropship_ind,
 COALESCE(ah.quantrix_category, 'OTHER') AS category,
  CASE
  WHEN LOWER(fct.banner) = LOWER('NORDSTROM')
  THEN ah.fp_supplier_group
  WHEN LOWER(fct.banner) = LOWER('NORDSTROM_RACK')
  THEN ah.op_supplier_group
  ELSE 'OTHER'
  END AS supplier_group,
  CASE
  WHEN MAX(ah.fanatics_ind) = 1
  THEN 'Y'
  ELSE 'N'
  END AS fanatics_ind,
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
 0 AS ttl_porcpt_r,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_u,
 0 AS ttl_porcpt_rp_r,
 0 AS ttl_porcpt_rp_c,
 0 AS ttl_porcpt_rp_u,
 0 AS ttl_porcpt_nrp_r,
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
 0 AS rp_oo_r,
 0 AS rp_oo_c,
 0 AS rp_oo_u,
 0 AS nrp_oo_r,
 0 AS nrp_oo_c,
 0 AS nrp_oo_u,
 0 AS rp_cm_r,
 0 AS rp_cm_c,
 0 AS rp_cm_u,
 0 AS nrp_cm_r,
 0 AS nrp_cm_c,
 0 AS nrp_cm_u,
 0 AS rp_ant_spd_r,
 0 AS rp_ant_spd_c,
 0 AS rp_ant_spd_u,
 0 AS rp_plan_receipts_lr_c,
 0 AS rp_plan_receipts_lr_u,
 0 AS nrp_plan_receipts_lr_c,
 0 AS nrp_plan_receipts_lr_u,
 0 AS plan_pah_in_c,
 0 AS plan_pah_in_u,
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
 0 AS plan_demand_u,
 0 AS plan_op_bop_c,
 0 AS plan_op_bop_u,
 0 AS plan_op_eop_c,
 0 AS plan_op_eop_u,
 0 AS plan_op_receipts_c,
 0 AS plan_op_receipts_u,
 0 AS plan_op_receipts_lr_c,
 0 AS plan_op_receipts_lr_u,
 0 AS plan_op_sales_c,
 0 AS plan_op_sales_r,
 0 AS plan_op_sales_u,
 0 AS plan_op_demand_r,
 0 AS plan_op_demand_u
FROM eop_stage_ty AS fct
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.assortment_hierarchy AS ah ON LOWER(fct.rms_sku_num) = LOWER(ah.sku_idnt) AND LOWER(fct
    .channel_country) = LOWER(ah.channel_country)
GROUP BY fct.channel_country,
 fct.banner,
 fct.channel_num,
 fct.channel_label,
 fct.date_ind,
 fct.month_idnt,
 fct.month_label,
 fct.month_idnt_aligned,
 fct.month_label_aligned,
 fct.month_start_day_date,
 fct.month_end_day_date,
 fct.department_num,
 dropship_ind,
 category,
 supplier_group;


CREATE TEMPORARY TABLE IF NOT EXISTS eop_ly
AS 
SELECT fct.channel_country,
 fct.banner,
 fct.channel_num,
 fct.channel_label,
 fct.date_ind,
 fct.month_idnt,
 fct.month_label,
 fct.month_idnt_aligned,
 fct.month_label_aligned,
 fct.month_start_day_date,
 fct.month_end_day_date,
 fct.department_num,
 RPAD(NULL, 1, ' ') AS dropship_ind,
 COALESCE(ah.quantrix_category, 'OTHER') AS category,
  CASE
  WHEN LOWER(fct.banner) = LOWER('NORDSTROM')
  THEN ah.fp_supplier_group
  WHEN LOWER(fct.banner) = LOWER('NORDSTROM_RACK')
  THEN ah.op_supplier_group
  ELSE 'OTHER'
  END AS supplier_group,
  CASE
  WHEN MAX(ah.fanatics_ind) = 1
  THEN 'Y'
  ELSE 'N'
  END AS fanatics_ind,
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
 0 AS ttl_porcpt_r,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_u,
 0 AS ttl_porcpt_rp_r,
 0 AS ttl_porcpt_rp_c,
 0 AS ttl_porcpt_rp_u,
 0 AS ttl_porcpt_nrp_r,
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
 0 AS rp_oo_r,
 0 AS rp_oo_c,
 0 AS rp_oo_u,
 0 AS nrp_oo_r,
 0 AS nrp_oo_c,
 0 AS nrp_oo_u,
 0 AS rp_cm_r,
 0 AS rp_cm_c,
 0 AS rp_cm_u,
 0 AS nrp_cm_r,
 0 AS nrp_cm_c,
 0 AS nrp_cm_u,
 0 AS rp_ant_spd_r,
 0 AS rp_ant_spd_c,
 0 AS rp_ant_spd_u,
 0 AS rp_plan_receipts_lr_c,
 0 AS rp_plan_receipts_lr_u,
 0 AS nrp_plan_receipts_lr_c,
 0 AS nrp_plan_receipts_lr_u,
 0 AS plan_pah_in_c,
 0 AS plan_pah_in_u,
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
 0 AS plan_demand_u,
 0 AS plan_op_bop_c,
 0 AS plan_op_bop_u,
 0 AS plan_op_eop_c,
 0 AS plan_op_eop_u,
 0 AS plan_op_receipts_c,
 0 AS plan_op_receipts_u,
 0 AS plan_op_receipts_lr_c,
 0 AS plan_op_receipts_lr_u,
 0 AS plan_op_sales_c,
 0 AS plan_op_sales_r,
 0 AS plan_op_sales_u,
 0 AS plan_op_demand_r,
 0 AS plan_op_demand_u
FROM eop_stage_ly AS fct
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.assortment_hierarchy AS ah ON LOWER(fct.rms_sku_num) = LOWER(ah.sku_idnt) AND LOWER(fct
    .channel_country) = LOWER(ah.channel_country)
GROUP BY fct.channel_country,
 fct.banner,
 fct.channel_num,
 fct.channel_label,
 fct.date_ind,
 fct.month_idnt,
 fct.month_label,
 fct.month_idnt_aligned,
 fct.month_label_aligned,
 fct.month_start_day_date,
 fct.month_end_day_date,
 fct.department_num,
 dropship_ind,
 category,
 supplier_group;


CREATE TEMPORARY TABLE IF NOT EXISTS spend
AS 
SELECT fct.selling_country AS channel_country,
 fct.selling_brand AS banner,
 st.channel_num,
 st.channel_label,
 dt.date_ind,
 dt.month_idnt,
 dt.month_label,
 dt.month_idnt_aligned,
 dt.month_label_aligned,
 dt.month_start_day_date,
 dt.month_end_day_date,
 CAST(TRUNC(CAST(REGEXP_SUBSTR(fct.department, '[^,]+') AS FLOAT64)) AS INTEGER) AS department_num,
 RPAD('N', 1, ' ') AS dropship_ind,
 fct.category,
 fct.supplier_group,
 RPAD('N', 1, ' ') AS fanatics_ind,
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
 0 AS ttl_porcpt_r,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_u,
 0 AS ttl_porcpt_rp_r,
 0 AS ttl_porcpt_rp_c,
 0 AS ttl_porcpt_rp_u,
 0 AS ttl_porcpt_nrp_r,
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
 SUM(fct.rp_oo_r) AS rp_oo_r,
 SUM(fct.rp_oo_c) AS rp_oo_c,
 SUM(fct.rp_oo_u) AS rp_oo_u,
 SUM(fct.nrp_oo_r) AS nrp_oo_r,
 SUM(fct.nrp_oo_c) AS nrp_oo_c,
 SUM(fct.nrp_oo_u) AS nrp_oo_u,
 SUM(fct.rp_cm_r) AS rp_cm_r,
 SUM(fct.rp_cm_c) AS rp_cm_c,
 SUM(fct.rp_cm_u) AS rp_cm_u,
 SUM(fct.nrp_cm_r) AS nrp_cm_r,
 SUM(fct.nrp_cm_c) AS nrp_cm_c,
 SUM(fct.nrp_cm_u) AS nrp_cm_u,
 SUM(fct.rp_ant_spd_r) AS rp_ant_spd_r,
 SUM(fct.rp_ant_spd_c) AS rp_ant_spd_c,
 SUM(fct.rp_ant_spd_u) AS rp_ant_spd_u,
 0 AS rp_plan_receipts_lr_c,
 0 AS rp_plan_receipts_lr_u,
 0 AS nrp_plan_receipts_lr_c,
 0 AS nrp_plan_receipts_lr_u,
 0 AS plan_pah_in_c,
 0 AS plan_pah_in_u,
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
 0 AS plan_demand_u,
 0 AS plan_op_bop_c,
 0 AS plan_op_bop_u,
 0 AS plan_op_eop_c,
 0 AS plan_op_eop_u,
 0 AS plan_op_receipts_c,
 0 AS plan_op_receipts_u,
 0 AS plan_op_receipts_lr_c,
 0 AS plan_op_receipts_lr_u,
 0 AS plan_op_sales_c,
 0 AS plan_op_sales_r,
 0 AS plan_op_sales_u,
 0 AS plan_op_demand_r,
 0 AS plan_op_demand_u
FROM `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.apt_supp_cat_spend_current AS fct
 INNER JOIN date_lkup_mth AS dt ON LOWER(fct.month_label) = LOWER(dt.spend_month) AND LOWER(dt.date_ind) IN (LOWER('PL MTH'
     ))
 INNER JOIN (SELECT DISTINCT channel_num,
   channel_label
  FROM store_lkup) AS st ON CAST(TRUNC(CAST(SUBSTR(fct.channel, 0, 3) AS FLOAT64)) AS INTEGER) = st.channel_num
GROUP BY channel_country,
 banner,
 st.channel_num,
 st.channel_label,
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
 fct.supplier_group;


CREATE TEMPORARY TABLE IF NOT EXISTS combine
AS 
SELECT channel_country,
 banner,
 channel_num,
 channel_label,
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
 supplier_group,
 COALESCE(fanatics_ind, 'N') AS fanatics_ind,
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
 SUM(ttl_porcpt_r) AS ttl_porcpt_r,
 SUM(ttl_porcpt_c) AS ttl_porcpt_c,
 SUM(ttl_porcpt_u) AS ttl_porcpt_u,
 SUM(ttl_porcpt_rp_r) AS ttl_porcpt_rp_r,
 SUM(ttl_porcpt_rp_c) AS ttl_porcpt_rp_c,
 SUM(ttl_porcpt_rp_u) AS ttl_porcpt_rp_u,
 SUM(ttl_porcpt_nrp_r) AS ttl_porcpt_nrp_r,
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
 SUM(rp_oo_r) AS rp_oo_r,
 SUM(rp_oo_c) AS rp_oo_c,
 SUM(rp_oo_u) AS rp_oo_u,
 SUM(nrp_oo_r) AS nrp_oo_r,
 SUM(nrp_oo_c) AS nrp_oo_c,
 SUM(nrp_oo_u) AS nrp_oo_u,
 SUM(rp_cm_r) AS rp_cm_r,
 SUM(rp_cm_c) AS rp_cm_c,
 SUM(rp_cm_u) AS rp_cm_u,
 SUM(nrp_cm_r) AS nrp_cm_r,
 SUM(nrp_cm_c) AS nrp_cm_c,
 SUM(nrp_cm_u) AS nrp_cm_u,
 SUM(rp_ant_spd_r) AS rp_ant_spd_r,
 SUM(rp_ant_spd_c) AS rp_ant_spd_c,
 SUM(rp_ant_spd_u) AS rp_ant_spd_u,
 SUM(rp_plan_receipts_lr_c) AS rp_plan_receipts_lr_c,
 SUM(rp_plan_receipts_lr_u) AS rp_plan_receipts_lr_u,
 SUM(nrp_plan_receipts_lr_c) AS nrp_plan_receipts_lr_c,
 SUM(nrp_plan_receipts_lr_u) AS nrp_plan_receipts_lr_u,
 SUM(plan_pah_in_c) AS plan_pah_in_c,
 SUM(plan_pah_in_u) AS plan_pah_in_u,
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
 SUM(plan_demand_u) AS plan_demand_u,
 SUM(plan_op_bop_c) AS plan_op_bop_c,
 SUM(plan_op_bop_u) AS plan_op_bop_u,
 SUM(plan_op_eop_c) AS plan_op_eop_c,
 SUM(plan_op_eop_u) AS plan_op_eop_u,
 SUM(plan_op_receipts_c) AS plan_op_receipts_c,
 SUM(plan_op_receipts_u) AS plan_op_receipts_u,
 SUM(plan_op_receipts_lr_c) AS plan_op_receipts_lr_c,
 SUM(plan_op_receipts_lr_u) AS plan_op_receipts_lr_u,
 SUM(plan_op_sales_c) AS plan_op_sales_c,
 SUM(plan_op_sales_r) AS plan_op_sales_r,
 SUM(plan_op_sales_u) AS plan_op_sales_u,
 SUM(plan_op_demand_r) AS plan_op_demand_r,
 SUM(plan_op_demand_u) AS plan_op_demand_u
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
   FROM plan
   UNION ALL
   SELECT *
   FROM plan_op
   UNION ALL
   SELECT *
   FROM spend) AS sub
GROUP BY channel_country,
 banner,
 channel_num,
 channel_label,
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
 supplier_group,
 fanatics_ind;


CREATE TEMPORARY TABLE IF NOT EXISTS final_combine
AS 
SELECT bs.channel_country,
 bs.banner,
 bs.channel_num,
 bs.channel_label,
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
 COALESCE(bs.supplier_group, 'OTHER') AS supplier_group,
 COALESCE(bs.fanatics_ind, 'N') AS fanatics_ind,
 sa.buy_planner,
 sa.preferred_partner_desc,
 sa.areas_of_responsibility,
 sa.is_npg,
 sa.diversity_group,
 sa.nord_to_rack_transfer_rate,
 COALESCE(sa.npg_ind, 'N') AS npg_ind,
  CASE
  WHEN rsb.supplier_group IS NOT NULL
  THEN 'Y'
  ELSE 'N'
  END AS rsb_ind,
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
 SUM(bs.ttl_porcpt_r) AS ttl_porcpt_r,
 SUM(bs.ttl_porcpt_c) AS ttl_porcpt_c,
 SUM(bs.ttl_porcpt_u) AS ttl_porcpt_u,
 SUM(bs.ttl_porcpt_rp_r) AS ttl_porcpt_rp_r,
 SUM(bs.ttl_porcpt_rp_c) AS ttl_porcpt_rp_c,
 SUM(bs.ttl_porcpt_rp_u) AS ttl_porcpt_rp_u,
 SUM(bs.ttl_porcpt_nrp_r) AS ttl_porcpt_nrp_r,
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
 SUM(bs.rp_oo_r) AS rp_oo_r,
 SUM(bs.rp_oo_c) AS rp_oo_c,
 SUM(bs.rp_oo_u) AS rp_oo_u,
 SUM(bs.nrp_oo_r) AS nrp_oo_r,
 SUM(bs.nrp_oo_c) AS nrp_oo_c,
 SUM(bs.nrp_oo_u) AS nrp_oo_u,
 SUM(bs.rp_cm_r) AS rp_cm_r,
 SUM(bs.rp_cm_c) AS rp_cm_c,
 SUM(bs.rp_cm_u) AS rp_cm_u,
 SUM(bs.nrp_cm_r) AS nrp_cm_r,
 SUM(bs.nrp_cm_c) AS nrp_cm_c,
 SUM(bs.nrp_cm_u) AS nrp_cm_u,
 SUM(bs.rp_ant_spd_r) AS rp_ant_spd_r,
 SUM(bs.rp_ant_spd_c) AS rp_ant_spd_c,
 SUM(bs.rp_ant_spd_u) AS rp_ant_spd_u,
 SUM(bs.rp_plan_receipts_lr_c) AS rp_plan_receipts_lr_c,
 SUM(bs.rp_plan_receipts_lr_u) AS rp_plan_receipts_lr_u,
 SUM(bs.nrp_plan_receipts_lr_c) AS nrp_plan_receipts_lr_c,
 SUM(bs.nrp_plan_receipts_lr_u) AS nrp_plan_receipts_lr_u,
 SUM(bs.plan_pah_in_c) AS plan_pah_in_c,
 SUM(bs.plan_pah_in_u) AS plan_pah_in_u,
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
 SUM(bs.plan_op_bop_c) AS plan_op_bop_c,
 SUM(bs.plan_op_bop_u) AS plan_op_bop_u,
 SUM(bs.plan_op_eop_c) AS plan_op_eop_c,
 SUM(bs.plan_op_eop_u) AS plan_op_eop_u,
 SUM(bs.plan_op_receipts_c) AS plan_op_receipts_c,
 SUM(bs.plan_op_receipts_u) AS plan_op_receipts_u,
 SUM(bs.plan_op_receipts_lr_c) AS plan_op_receipts_lr_c,
 SUM(bs.plan_op_receipts_lr_u) AS plan_op_receipts_lr_u,
 SUM(bs.plan_op_sales_c) AS plan_op_sales_c,
 SUM(bs.plan_op_sales_r) AS plan_op_sales_r,
 SUM(bs.plan_op_sales_u) AS plan_op_sales_u,
 SUM(bs.plan_op_demand_r) AS plan_op_demand_r,
 SUM(bs.plan_op_demand_u) AS plan_op_demand_u,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS update_timestamp,
  (SELECT MAX(month_idnt)
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
  WHERE week_end_day_date < CURRENT_DATE('PST8PDT')) AS current_month_idnt
FROM combine AS bs
 LEFT JOIN cat_attr AS ca ON bs.department_num = ca.dept_num AND LOWER(bs.category) = LOWER(ca.category)
 LEFT JOIN sup_attr AS sa ON LOWER(bs.banner) = LOWER(sa.banner) AND bs.department_num = CAST(sa.dept_num AS FLOAT64)
  AND LOWER(bs.supplier_group) = LOWER(sa.supplier_group)
 LEFT JOIN dept_lkup AS hr ON bs.department_num = hr.dept_num
 LEFT JOIN rsb_lkup AS rsb ON bs.department_num = rsb.dept_num AND LOWER(bs.supplier_group) = LOWER(rsb.supplier_group)
  AND LOWER(bs.banner) = LOWER('NORDSTROM_RACK')
GROUP BY bs.channel_country,
 bs.banner,
 bs.channel_num,
 bs.channel_label,
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
 supplier_group,
 fanatics_ind,
 sa.buy_planner,
 sa.preferred_partner_desc,
 sa.areas_of_responsibility,
 sa.is_npg,
 sa.diversity_group,
 sa.nord_to_rack_transfer_rate,
 npg_ind,
 rsb_ind;


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.apt_is_supplier;


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.apt_is_supplier
(SELECT channel_country,
  banner,
  channel_num,
  channel_label,
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
  supplier_group,
  fanatics_ind,
  buy_planner,
  preferred_partner_desc,
  areas_of_responsibility,
  is_npg,
  diversity_group,
  nord_to_rack_transfer_rate,
  npg_ind,
  rsb_ind,
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
  ttl_porcpt_r,
  ttl_porcpt_c,
  CAST(TRUNC(ttl_porcpt_u) AS INTEGER) AS ttl_porcpt_u,
  ttl_porcpt_rp_r,
  ttl_porcpt_rp_c,
  CAST(TRUNC(ttl_porcpt_rp_u) AS INTEGER) AS ttl_porcpt_rp_u,
  ttl_porcpt_nrp_r,
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
  rp_oo_r,
  rp_oo_c,
  CAST(TRUNC(rp_oo_u) AS INTEGER) AS rp_oo_u,
  nrp_oo_r,
  nrp_oo_c,
  CAST(TRUNC(nrp_oo_u) AS INTEGER) AS nrp_oo_u,
  rp_cm_r,
  rp_cm_c,
  CAST(TRUNC(rp_cm_u) AS INTEGER) AS rp_cm_u,
  nrp_cm_r,
  nrp_cm_c,
  CAST(TRUNC(nrp_cm_u) AS INTEGER) AS nrp_cm_u,
  rp_ant_spd_r,
  rp_ant_spd_c,
  rp_ant_spd_u,
  rp_plan_receipts_lr_c,
  CAST(TRUNC(rp_plan_receipts_lr_u) AS INTEGER) AS rp_plan_receipts_lr_u,
  nrp_plan_receipts_lr_c,
  CAST(TRUNC(nrp_plan_receipts_lr_u) AS INTEGER) AS nrp_plan_receipts_lr_u,
  plan_pah_in_c,
  CAST(TRUNC(plan_pah_in_u) AS INTEGER) AS plan_pah_in_u,
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
  plan_op_bop_c,
  CAST(TRUNC(plan_op_bop_u) AS INTEGER) AS plan_op_bop_u,
  plan_op_eop_c,
  CAST(TRUNC(plan_op_eop_u) AS INTEGER) AS plan_op_eop_u,
  plan_op_receipts_c,
  CAST(TRUNC(plan_op_receipts_u) AS INTEGER) AS plan_op_receipts_u,
  plan_op_receipts_lr_c,
  CAST(TRUNC(plan_op_receipts_lr_u) AS INTEGER) AS plan_op_receipts_lr_u,
  plan_op_sales_c,
  plan_op_sales_r,
  CAST(TRUNC(plan_op_sales_u) AS INTEGER) AS plan_op_sales_u,
  plan_op_demand_r,
  CAST(TRUNC(plan_op_demand_u) AS INTEGER) AS plan_op_demand_u,
  CAST(update_timestamp AS TIMESTAMP) AS update_timestamp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(update_timestamp AS STRING)) as update_timestamp_tz,
  current_month_idnt
 FROM final_combine);

