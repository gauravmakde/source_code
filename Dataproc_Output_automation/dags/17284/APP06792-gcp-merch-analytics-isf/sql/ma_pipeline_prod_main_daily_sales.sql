-- /*
-- Name: Daily Sales and Profitability
-- Author: Meghan Hickey
-- Modified By: Asiyah Fox, Alli Moore, Tanner Moxcey
-- Date Created:5/3/23
-- Date Last Updated: 6/5/24  -- updated to add D990 for Plan Data
-- */

-- /* 1. Create Date Lookup for rolling 5 fiscal weeks + current week */



CREATE TEMPORARY TABLE IF NOT EXISTS date_filter
AS
SELECT a.day_idnt,
 a.fiscal_year_num,
 a.fiscal_day_num,
 a.day_num_of_fiscal_week,
 a.day_date,
 a.day_desc,
 a.fiscal_week_num,
 a.week_desc,
 a.week_idnt,
 a.fiscal_month_num,
 a.month_idnt,
 a.month_desc,
 a.fiscal_quarter_num,
 a.quarter_idnt,
 a.ty_ly_lly_ind AS ty_ly_ind,
  CASE
  WHEN a.week_idnt = (SELECT week_idnt
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw
     WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) AND CURRENT_DATE('PST8PDT') < a.week_end_day_date
  THEN 0
  ELSE 1
  END AS elapsed_week_ind,
  CASE
  WHEN a.day_date > DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)
  THEN 0
  ELSE 1
  END AS elapsed_day_ind,
  CASE
  WHEN a.fiscal_week_num = (SELECT fiscal_week_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw
    WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY))
  THEN 1
  ELSE 0
  END AS wtd,
  CASE
  WHEN a.fiscal_month_num = (SELECT fiscal_month_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw
    WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY))
  THEN 1
  ELSE 0
  END AS mtd,
  CASE
  WHEN a.fiscal_day_num = (SELECT fiscal_day_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw
    WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY))
  THEN 1
  ELSE 0
  END AS max_day_elapsed,
  CASE
  WHEN a.fiscal_day_num = (SELECT fiscal_day_num
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw
     WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) OR a.day_date = a.week_end_day_date
  THEN 1
  ELSE 0
  END AS max_day_elapsed_week,
  CASE
  WHEN a.fiscal_day_num = (SELECT fiscal_day_num
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw
     WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) OR a.day_date = a.month_end_day_date
  THEN 1
  ELSE 0
  END AS max_day_elapsed_month,
  CASE
  WHEN a.fiscal_day_num = (SELECT fiscal_day_num
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw
     WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) OR a.day_date = a.quarter_end_day_date
  THEN 1
  ELSE 0
  END AS max_day_elapsed_quarter,
 b.day_idnt AS true_day_idnt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw AS a
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS b ON a.day_date = b.day_date
WHERE LOWER(a.ty_ly_lly_ind) IN (LOWER('TY'), LOWER('LY'))
 AND (a.fiscal_month_num = (SELECT DISTINCT fiscal_month_num
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw
     WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 7 DAY)) OR a.fiscal_month_num = (SELECT DISTINCT fiscal_month_num
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw
     WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)))
 AND a.fiscal_day_num <= (SELECT fiscal_day_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw
   WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY));


--collect stats primary index(day_idnt, day_date, week_idnt) on date_filter


CREATE TEMPORARY TABLE IF NOT EXISTS store_lkup
AS
SELECT DISTINCT store_num,
 channel_num,
 channel_desc,
 store_country_code,
  CASE
  WHEN channel_num IN (110, 120, 310)
  THEN 'NORDSTROM'
  WHEN channel_num IN (210, 240, 250, 260)
  THEN 'NORDSTROM RACK'
  ELSE NULL
  END AS banner
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim
WHERE channel_num IN (110, 120, 210, 250, 260, 310);


--collect stats primary index(store_num) on store_lkup


CREATE TEMPORARY TABLE IF NOT EXISTS channel_lkup
AS
SELECT DISTINCT channel_num,
 TRIM(FORMAT('%11d', channel_num) || ', ' || channel_desc) AS channel,
  CASE
  WHEN channel_num IN (110, 120, 310)
  THEN 'NORDSTROM'
  WHEN channel_num IN (210, 240, 250, 260)
  THEN 'NORDSTROM RACK'
  ELSE NULL
  END AS banner,
 store_country_code
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim
WHERE channel_num IN (110, 120, 210, 250, 260, 310);


--collect stats primary index(channel_num) on channel_lkup


CREATE TEMPORARY TABLE IF NOT EXISTS dept_lkup
AS
SELECT DISTINCT dept_num,
 TRIM(FORMAT('%11d', division_num) || ', ' || division_name) AS division,
 TRIM(FORMAT('%11d', subdivision_num) || ', ' || subdivision_name) AS subdivision,
 TRIM(FORMAT('%11d', dept_num) || ', ' || dept_name) AS department
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim;


--collect stats primary index(dept_num) on dept_lkup


CREATE TEMPORARY TABLE IF NOT EXISTS aor
AS
SELECT DISTINCT REGEXP_REPLACE(channel_brand, '_', ' ') AS banner,
 dept_num,
 general_merch_manager_executive_vice_president,
 div_merch_manager_senior_vice_president,
 div_merch_manager_vice_president,
 merch_director,
 buyer,
 merch_planning_executive_vice_president,
 merch_planning_senior_vice_president,
 merch_planning_vice_president,
 merch_planning_director_manager,
 assortment_planner
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.area_of_responsibility_dim
QUALIFY (ROW_NUMBER() OVER (PARTITION BY banner, dept_num ORDER BY general_merch_manager_executive_vice_president,div_merch_manager_senior_vice_president,div_merch_manager_vice_president,merch_director,buyer,merch_planning_executive_vice_president,merch_planning_senior_vice_president,merch_planning_vice_president,merch_planning_director_manager,assortment_planner)) = 1;


--COLLECT STATS 	PRIMARY INDEX (banner,dept_num) 		ON aor 


CREATE TEMPORARY TABLE IF NOT EXISTS eoh
AS
SELECT b.day_idnt AS day_num,
 b.fiscal_day_num,
 b.ty_ly_ind,
 a.sku_idnt AS rms_sku_num,
 a.loc_idnt AS store_num,
 0 AS net_sales_r,
 0 AS net_sales_c,
 0 AS net_sales_u,
 0 AS net_sales_reg_r,
 0 AS net_sales_reg_c,
 0 AS net_sales_reg_u,
 0 AS net_sales_pro_r,
 0 AS net_sales_pro_c,
 0 AS net_sales_pro_u,
 0 AS net_sales_clear_r,
 0 AS net_sales_clear_c,
 0 AS net_sales_clear_u,
 0 AS demand,
 0 AS demand_u,
 SUM(a.eoh_r) AS eoh_r,
 SUM(a.eoh_c) AS eoh_c,
 SUM(a.eoh_u) AS eoh_u,
 0 AS in_transit_r,
 0 AS in_transit_c,
 0 AS in_transit_u
FROM (SELECT day_dt,
   sku_idnt,
   loc_idnt,
   eoh_dollars AS eoh_r,
    eoh_units * weighted_average_cost AS eoh_c,
   eoh_units AS eoh_u
  FROM `{{params.gcp_project_id}}`.t2dl_das_ace_mfp.sku_loc_pricetype_day AS a
  WHERE (day_dt BETWEEN (SELECT MIN(day_date)
       FROM date_filter
       WHERE LOWER(ty_ly_ind) = LOWER('LY')) AND (SELECT MAX(day_date)
       FROM date_filter
       WHERE LOWER(ty_ly_ind) = LOWER('LY')) OR day_dt BETWEEN (SELECT MIN(day_date)
       FROM date_filter
       WHERE LOWER(ty_ly_ind) = LOWER('TY')) AND (SELECT MAX(day_date)
       FROM date_filter
       WHERE LOWER(ty_ly_ind) = LOWER('TY')))
   AND loc_idnt < 1000
   AND eoh_units > 0) AS a
 INNER JOIN date_filter AS b ON a.day_dt = b.day_date
 INNER JOIN store_lkup AS st ON a.loc_idnt = st.store_num AND LOWER(st.store_country_code) = LOWER('US')
GROUP BY day_num,
 b.fiscal_day_num,
 b.ty_ly_ind,
 rms_sku_num,
 store_num;


--collect stats primary index(day_num, rms_sku_num, store_num) on eoh


CREATE TEMPORARY TABLE IF NOT EXISTS it_base
AS
SELECT a.rms_sku_id AS rms_sku_num,
 a.snapshot_date,
 a.location_id AS store_num,
 COALESCE(a.in_transit_qty, 0) AS in_transit_u,
 c.price_store_num,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(a.snapshot_date AS DATETIME)) AS DATETIME) AS snapshot_time
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.inventory_stock_quantity_by_day_fact AS a
 INNER JOIN store_lkup AS b ON CAST(a.location_id AS FLOAT64) = b.store_num
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw AS c ON b.store_num = c.store_num
WHERE (a.snapshot_date BETWEEN (SELECT MIN(day_date)
     FROM date_filter
     WHERE LOWER(ty_ly_ind) = LOWER('LY')) AND (SELECT MAX(day_date)
     FROM date_filter
     WHERE LOWER(ty_ly_ind) = LOWER('LY')) OR a.snapshot_date BETWEEN (SELECT MIN(day_date)
     FROM date_filter
     WHERE LOWER(ty_ly_ind) = LOWER('TY')) AND (SELECT MAX(day_date)
     FROM date_filter
     WHERE LOWER(ty_ly_ind) = LOWER('TY')))
 AND a.in_transit_qty > 0;


--collect stats primary index(snapshot_date, rms_sku_num, store_num, price_store_num, snapshot_time) on it_base


CREATE TEMPORARY TABLE IF NOT EXISTS it_price
AS
SELECT a.rms_sku_num,
 a.snapshot_date,
 a.store_num,
 a.in_transit_u,
 a.price_store_num,
 a.snapshot_time,
  a.in_transit_u * b.ownership_retail_price_amt AS in_transit_r
FROM it_base AS a
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim AS b ON LOWER(a.rms_sku_num) = LOWER(b.rms_sku_num) AND a.price_store_num
     = CAST(b.store_num AS FLOAT64) 
  AND a.snapshot_time BETWEEN CAST(b.eff_begin_tmstp AS DATETIME) AND DATETIME_SUB(b.eff_end_tmstp , INTERVAL 1 MILLISECOND)  --0.001
    ;


--collect stats primary index(snapshot_date, rms_sku_num, store_num) on it_price


CREATE TEMPORARY TABLE IF NOT EXISTS it_cost
AS
SELECT c.day_idnt AS day_num,
 c.fiscal_day_num,
 c.ty_ly_ind,
 a.rms_sku_num,
 CAST(TRUNC(CAST(a.store_num AS FLOAT64)) AS INTEGER) AS store_num,
 0 AS net_sales_r,
 0 AS net_sales_c,
 0 AS net_sales_u,
 0 AS net_sales_reg_r,
 0 AS net_sales_reg_c,
 0 AS net_sales_reg_u,
 0 AS net_sales_pro_r,
 0 AS net_sales_pro_c,
 0 AS net_sales_pro_u,
 0 AS net_sales_clear_r,
 0 AS net_sales_clear_c,
 0 AS net_sales_clear_u,
 0 AS demand,
 0 AS demand_u,
 0 AS eoh_r,
 0 AS eoh_c,
 0 AS eoh_u,
 SUM(a.in_transit_r) AS in_transit_r,
 SUM(a.in_transit_c) AS in_transit_c,
 SUM(a.in_transit_u) AS in_transit_u
FROM (SELECT a.rms_sku_num,
   a.snapshot_date,
   a.store_num,
   a.in_transit_u,
   a.in_transit_r,
    a.in_transit_u * b.weighted_average_cost AS in_transit_c
  FROM it_price AS a
   LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.weighted_average_cost_date_dim AS b ON LOWER(a.rms_sku_num) = LOWER(b.sku_num) AND LOWER(a
       .store_num) = LOWER(b.location_num) AND a.snapshot_date BETWEEN b.eff_begin_dt AND (DATE_SUB(b.eff_end_dt,
       INTERVAL 1 DAY))) AS a
 INNER JOIN date_filter AS c ON a.snapshot_date = c.day_date
GROUP BY day_num,
 c.fiscal_day_num,
 c.ty_ly_ind,
 a.rms_sku_num,
 store_num;


--collect stats primary index(day_num, rms_sku_num, store_num) on it_cost


CREATE TEMPORARY TABLE IF NOT EXISTS sales
AS
SELECT a.day_num,
 b.fiscal_day_num,
 b.ty_ly_ind,
 a.rms_sku_num,
 a.store_num,
 a.jwn_operational_gmv_total_retail_amt AS net_sales_r,
 a.jwn_operational_gmv_total_cost_amt AS net_sales_c,
 a.jwn_operational_gmv_total_units AS net_sales_u,
 a.jwn_operational_gmv_regular_retail_amt AS net_sales_reg_r,
 a.jwn_operational_gmv_regular_cost_amt AS net_sales_reg_c,
 a.jwn_operational_gmv_regular_units AS net_sales_reg_u,
 a.jwn_operational_gmv_promo_retail_amt AS net_sales_pro_r,
 a.jwn_operational_gmv_promo_cost_amt AS net_sales_pro_c,
 a.jwn_operational_gmv_promo_units AS net_sales_pro_u,
 a.jwn_operational_gmv_clearance_retail_amt AS net_sales_clear_r,
 a.jwn_operational_gmv_clearance_cost_amt AS net_sales_clear_c,
 a.jwn_operational_gmv_clearance_units AS net_sales_clear_u,
 0 AS demand,
 0 AS demand_u,
 0 AS eoh_r,
 0 AS eoh_c,
 0 AS eoh_u,
 0 AS in_transit_r,
 0 AS in_transit_c,
 0 AS in_transit_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_jwn_sale_return_sku_store_day_fact_vw AS a
 INNER JOIN date_filter AS b ON a.day_num = b.true_day_idnt
 INNER JOIN store_lkup AS st ON a.store_num = st.store_num AND LOWER(st.store_country_code) = LOWER('US');


--collect stats primary index(day_num, rms_sku_num, store_num) on sales


CREATE TEMPORARY TABLE IF NOT EXISTS demand
AS
SELECT a.day_num,
 b.fiscal_day_num,
 b.ty_ly_ind,
 a.rms_sku_num,
 a.store_num,
 0 AS net_sales_r,
 0 AS net_sales_c,
 0 AS net_sales_u,
 0 AS net_sales_reg_r,
 0 AS net_sales_reg_c,
 0 AS net_sales_reg_u,
 0 AS net_sales_pro_r,
 0 AS net_sales_pro_c,
 0 AS net_sales_pro_u,
 0 AS net_sales_clear_r,
 0 AS net_sales_clear_c,
 0 AS net_sales_clear_u,
 a.jwn_demand_total_retail_amt AS demand,
 a.jwn_demand_total_units AS demand_u,
 0 AS eoh_r,
 0 AS eoh_c,
 0 AS eoh_u,
 0 AS in_transit_r,
 0 AS in_transit_c,
 0 AS in_transit_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_jwn_demand_sku_store_day_fact_vw AS a
 INNER JOIN date_filter AS b ON a.day_num = b.true_day_idnt
 INNER JOIN store_lkup AS st ON a.store_num = st.store_num AND LOWER(st.store_country_code) = LOWER('US');


--collect stats primary index(day_num, rms_sku_num, store_num) on demand


CREATE TEMPORARY TABLE IF NOT EXISTS combine
AS
SELECT *
FROM sales
UNION ALL
SELECT *
FROM demand AS demand
UNION ALL
SELECT *
FROM eoh
UNION ALL
SELECT *
FROM it_cost;


--collect stats primary index(day_num, fiscal_day_num, rms_sku_num, store_num) on combine


CREATE TEMPORARY TABLE IF NOT EXISTS actuals
AS
SELECT st.channel_num,
 sku.dept_num,                                                                                    --sku.class_desc AS dept_num,
 dy.day_date,
 SUM(CASE
   WHEN LOWER(base.ty_ly_ind) = LOWER('TY')
   THEN base.net_sales_r
   ELSE 0
   END) AS net_sales_r_ty,
 SUM(CASE
   WHEN LOWER(base.ty_ly_ind) = LOWER('TY')
   THEN base.net_sales_c
   ELSE 0
   END) AS net_sales_c_ty,
 SUM(CASE
   WHEN LOWER(base.ty_ly_ind) = LOWER('TY')
   THEN base.net_sales_u
   ELSE 0
   END) AS net_sales_u_ty,
 SUM(CASE
   WHEN LOWER(base.ty_ly_ind) = LOWER('TY')
   THEN base.net_sales_reg_r
   ELSE 0
   END) AS net_sales_reg_r_ty,
 SUM(CASE
   WHEN LOWER(base.ty_ly_ind) = LOWER('TY')
   THEN base.net_sales_reg_c
   ELSE 0
   END) AS net_sales_reg_c_ty,
 SUM(CASE
   WHEN LOWER(base.ty_ly_ind) = LOWER('TY')
   THEN base.net_sales_reg_u
   ELSE 0
   END) AS net_sales_reg_u_ty,
 SUM(CASE
   WHEN LOWER(base.ty_ly_ind) = LOWER('TY')
   THEN base.net_sales_pro_r
   ELSE 0
   END) AS net_sales_pro_r_ty,
 SUM(CASE
   WHEN LOWER(base.ty_ly_ind) = LOWER('TY')
   THEN base.net_sales_pro_c
   ELSE 0
   END) AS net_sales_pro_c_ty,
 SUM(CASE
   WHEN LOWER(base.ty_ly_ind) = LOWER('TY')
   THEN base.net_sales_pro_u
   ELSE 0
   END) AS net_sales_pro_u_ty,
 SUM(CASE
   WHEN LOWER(base.ty_ly_ind) = LOWER('TY')
   THEN base.net_sales_clear_r
   ELSE 0
   END) AS net_sales_clear_r_ty,
 SUM(CASE
   WHEN LOWER(base.ty_ly_ind) = LOWER('TY')
   THEN base.net_sales_clear_c
   ELSE 0
   END) AS net_sales_clear_c_ty,
 SUM(CASE
   WHEN LOWER(base.ty_ly_ind) = LOWER('TY')
   THEN base.net_sales_clear_u
   ELSE 0
   END) AS net_sales_clear_u_ty,
 SUM(CASE
   WHEN LOWER(base.ty_ly_ind) = LOWER('TY')
   THEN base.demand
   ELSE 0
   END) AS demand_ty,
 SUM(CASE
   WHEN LOWER(base.ty_ly_ind) = LOWER('TY')
   THEN base.demand_u
   ELSE 0
   END) AS demand_u_ty,
  SUM(CASE
    WHEN LOWER(base.ty_ly_ind) = LOWER('TY')
    THEN base.eoh_r
    ELSE 0
    END) + SUM(CASE
    WHEN LOWER(base.ty_ly_ind) = LOWER('TY')
    THEN base.in_transit_r
    ELSE 0
    END) AS eoh_r_ty,
  SUM(CASE
    WHEN LOWER(base.ty_ly_ind) = LOWER('TY')
    THEN base.eoh_c
    ELSE 0
    END) + SUM(CASE
    WHEN LOWER(base.ty_ly_ind) = LOWER('TY')
    THEN base.in_transit_c
    ELSE 0
    END) AS eoh_c_ty,
  SUM(CASE
    WHEN LOWER(base.ty_ly_ind) = LOWER('TY')
    THEN base.eoh_u
    ELSE 0
    END) + SUM(CASE
    WHEN LOWER(base.ty_ly_ind) = LOWER('TY')
    THEN base.in_transit_u
    ELSE 0
    END) AS eoh_u_ty,
 SUM(CASE
   WHEN LOWER(base.ty_ly_ind) = LOWER('LY')
   THEN base.net_sales_r
   ELSE 0
   END) AS net_sales_r_ly,
 SUM(CASE
   WHEN LOWER(base.ty_ly_ind) = LOWER('LY')
   THEN base.net_sales_c
   ELSE 0
   END) AS net_sales_c_ly,
 SUM(CASE
   WHEN LOWER(base.ty_ly_ind) = LOWER('LY')
   THEN base.net_sales_u
   ELSE 0
   END) AS net_sales_u_ly,
 SUM(CASE
   WHEN LOWER(base.ty_ly_ind) = LOWER('LY')
   THEN base.net_sales_reg_r
   ELSE 0
   END) AS net_sales_reg_r_ly,
 SUM(CASE
   WHEN LOWER(base.ty_ly_ind) = LOWER('LY')
   THEN base.net_sales_reg_c
   ELSE 0
   END) AS net_sales_reg_c_ly,
 SUM(CASE
   WHEN LOWER(base.ty_ly_ind) = LOWER('LY')
   THEN base.net_sales_reg_u
   ELSE 0
   END) AS net_sales_reg_u_ly,
 SUM(CASE
   WHEN LOWER(base.ty_ly_ind) = LOWER('LY')
   THEN base.net_sales_pro_r
   ELSE 0
   END) AS net_sales_pro_r_ly,
 SUM(CASE
   WHEN LOWER(base.ty_ly_ind) = LOWER('LY')
   THEN base.net_sales_pro_c
   ELSE 0
   END) AS net_sales_pro_c_ly,
 SUM(CASE
   WHEN LOWER(base.ty_ly_ind) = LOWER('LY')
   THEN base.net_sales_pro_u
   ELSE 0
   END) AS net_sales_pro_u_ly,
 SUM(CASE
   WHEN LOWER(base.ty_ly_ind) = LOWER('LY')
   THEN base.net_sales_clear_r
   ELSE 0
   END) AS net_sales_clear_r_ly,
 SUM(CASE
   WHEN LOWER(base.ty_ly_ind) = LOWER('LY')
   THEN base.net_sales_clear_c
   ELSE 0
   END) AS net_sales_clear_c_ly,
 SUM(CASE
   WHEN LOWER(base.ty_ly_ind) = LOWER('LY')
   THEN base.net_sales_clear_u
   ELSE 0
   END) AS net_sales_clear_u_ly,
 SUM(CASE
   WHEN LOWER(base.ty_ly_ind) = LOWER('LY')
   THEN base.demand
   ELSE 0
   END) AS demand_ly,
 SUM(CASE
   WHEN LOWER(base.ty_ly_ind) = LOWER('LY')
   THEN base.demand_u
   ELSE 0
   END) AS demand_u_ly,
  SUM(CASE
    WHEN LOWER(base.ty_ly_ind) = LOWER('LY')
    THEN base.eoh_r
    ELSE 0
    END) + SUM(CASE
    WHEN LOWER(base.ty_ly_ind) = LOWER('LY')
    THEN base.in_transit_r
    ELSE 0
    END) AS eoh_r_ly,
  SUM(CASE
    WHEN LOWER(base.ty_ly_ind) = LOWER('LY')
    THEN base.eoh_c
    ELSE 0
    END) + SUM(CASE
    WHEN LOWER(base.ty_ly_ind) = LOWER('LY')
    THEN base.in_transit_c
    ELSE 0
    END) AS eoh_c_ly,
  SUM(CASE
    WHEN LOWER(base.ty_ly_ind) = LOWER('LY')
    THEN base.eoh_u
    ELSE 0
    END) + SUM(CASE
    WHEN LOWER(base.ty_ly_ind) = LOWER('LY')
    THEN base.in_transit_u
    ELSE 0
    END) AS eoh_u_ly
FROM combine AS base
 INNER JOIN store_lkup AS st ON base.store_num = st.store_num AND LOWER(st.store_country_code) = LOWER('US')
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw AS dy 
 ON base.fiscal_day_num = dy.fiscal_day_num
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS sku ON LOWER(base.rms_sku_num) = LOWER(sku.rms_sku_num) AND LOWER(sku.channel_country
    ) = LOWER('US')
WHERE dy.fiscal_year_num = (SELECT fiscal_year_num FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT') ,INTERVAL 1 DAY))
GROUP BY st.channel_num,
 sku.dept_num,
 dy.day_date;


--collect stats primary index(day_date, channel_num, dept_num) on actuals


CREATE TEMPORARY TABLE IF NOT EXISTS sales_demand_plan
AS
SELECT DISTINCT dt.day_date,
 sub.channel_num,
 sub.dept_num,
  COALESCE(sub.net_sales_r_plan_weekly, 0) * dt.max_day_elapsed_week AS net_sales_r_plan_weekly,
  COALESCE(sub.net_sales_u_plan_weekly, 0) * dt.max_day_elapsed_week AS net_sales_u_plan_weekly,
  COALESCE(sub.net_sales_c_plan_weekly, 0) * dt.max_day_elapsed_week AS net_sales_c_plan_weekly,
  COALESCE(sub.net_sales_r_plan_weekly_ly, 0) * dt.max_day_elapsed_week AS net_sales_r_plan_weekly_ly,
  COALESCE(sub.demand_r_plan_weekly, 0) * dt.max_day_elapsed_week AS demand_r_plan_weekly,
  COALESCE(sub.demand_u_plan_weekly, 0) * dt.max_day_elapsed_week AS demand_u_plan_weekly,
  COALESCE(sub.net_sales_r_plan_monthly, 0) * dt.max_day_elapsed_month AS net_sales_r_plan_monthly,
  COALESCE(sub.net_sales_u_plan_monthly, 0) * dt.max_day_elapsed_month AS net_sales_u_plan_monthly,
  COALESCE(sub.net_sales_c_plan_monthly, 0) * dt.max_day_elapsed_month AS net_sales_c_plan_monthly,
  COALESCE(sub.net_sales_r_plan_monthly_ly, 0) * dt.max_day_elapsed_month AS net_sales_r_plan_monthly_ly,
  COALESCE(sub.demand_r_plan_monthly, 0) * dt.max_day_elapsed_month AS demand_r_plan_monthly,
  COALESCE(sub.demand_u_plan_monthly, 0) * dt.max_day_elapsed_month AS demand_u_plan_monthly
FROM (SELECT t9.week_idnt,
   t9.channel_num,
   t9.dept_num,
   t9.month_idnt,
   t9.quarter_idnt,
   t9.net_sales_r_plan_weekly,
   t9.net_sales_u_plan_weekly,
   t9.net_sales_c_plan_weekly,
   t9.net_sales_r_plan_weekly_ly,
   t9.demand_r_plan_weekly,
   t9.demand_u_plan_weekly,
   SUM(t9.net_sales_r_plan_weekly) OVER (PARTITION BY t9.month_idnt, t9.channel_num, t9.dept_num RANGE BETWEEN
    UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS net_sales_r_plan_monthly,
   SUM(t9.net_sales_u_plan_weekly) OVER (PARTITION BY t9.month_idnt, t9.channel_num, t9.dept_num RANGE BETWEEN
    UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS net_sales_u_plan_monthly,
   SUM(t9.net_sales_c_plan_weekly) OVER (PARTITION BY t9.month_idnt, t9.channel_num, t9.dept_num RANGE BETWEEN
    UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS net_sales_c_plan_monthly,
   SUM(t9.net_sales_r_plan_weekly_ly) OVER (PARTITION BY t9.month_idnt, t9.channel_num, t9.dept_num RANGE BETWEEN
    UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS net_sales_r_plan_monthly_ly,
   SUM(t9.demand_r_plan_weekly) OVER (PARTITION BY t9.month_idnt, t9.channel_num, t9.dept_num RANGE BETWEEN
    UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS demand_r_plan_monthly,
   SUM(t9.demand_u_plan_weekly) OVER (PARTITION BY t9.month_idnt, t9.channel_num, t9.dept_num RANGE BETWEEN
    UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS demand_u_plan_monthly
  FROM (SELECT b.week_idnt,
     a.channel_num,
     a.dept_num,
     b.month_idnt,
     b.quarter_idnt,
     a.op_net_sales_retail_amt,
     a.op_net_sales_qty,
     a.op_net_sales_cost_amt,
     a.ly_net_sales_retail_amt,
     a.op_demand_total_retail_amt,
     a.op_gross_sales_retail_amt,
     a.op_demand_total_qty,
     a.op_gross_sales_qty,
     SUM(a.op_net_sales_retail_amt) AS net_sales_r_plan_weekly,
     SUM(a.op_net_sales_qty) AS net_sales_u_plan_weekly,
     SUM(a.op_net_sales_cost_amt) AS net_sales_c_plan_weekly,
     SUM(a.ly_net_sales_retail_amt) AS net_sales_r_plan_weekly_ly,
     SUM(CASE
       WHEN a.channel_num IN (110, 210)
       THEN a.op_gross_sales_retail_amt
       ELSE a.op_demand_total_retail_amt
       END) AS demand_r_plan_weekly,
     SUM(CASE
       WHEN a.channel_num IN (110, 210)
       THEN a.op_gross_sales_qty
       ELSE a.op_demand_total_qty
       END) AS demand_u_plan_weekly
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.mfp_cost_plan_actual_channel_fact AS a
     INNER JOIN (SELECT DISTINCT b.week_idnt AS week_idnt_true,
       a0.week_idnt,
       a0.month_idnt,
       a0.quarter_idnt,
       a0.ty_ly_lly_ind AS ty_ly_ind
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw AS a0
       LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS b ON a0.day_date = b.day_date
      WHERE a0.quarter_idnt IN (SELECT quarter_idnt
         FROM date_filter)) AS b ON a.week_num = b.week_idnt_true AND LOWER(b.ty_ly_ind) = LOWER('TY')
    WHERE a.channel_num IN (SELECT DISTINCT channel_num
       FROM store_lkup)
    GROUP BY b.week_idnt,
     a.channel_num,
     a.dept_num,
     b.month_idnt,
     b.quarter_idnt,
     a.op_net_sales_retail_amt,
     a.op_net_sales_qty,
     a.op_net_sales_cost_amt,
     a.ly_net_sales_retail_amt,
     a.op_demand_total_retail_amt,
     a.op_gross_sales_retail_amt,
     a.op_demand_total_qty,
     a.op_gross_sales_qty
     ) AS t9) AS sub
 LEFT JOIN date_filter AS dt ON sub.week_idnt = dt.week_idnt;


--collect stats primary index(day_date, channel_num, dept_num) on sales_demand_plan


CREATE TEMPORARY TABLE IF NOT EXISTS eop_plan 
AS (
	SELECT DISTINCT
		dt.day_date
		, sub.channel_num
		, sub.dept_num
    , COALESCE(sub.eoh_c_plan_weekly,0) * dt.max_day_elapsed_week AS eoh_c_plan_weekly
    , COALESCE(sub.eoh_u_plan_weekly,0) * dt.max_day_elapsed_week AS eoh_u_plan_weekly
    , COALESCE(sub.eoh_c_plan_monthly,0) * dt.max_day_elapsed_month  AS eoh_c_plan_monthly
    , COALESCE(sub.eoh_u_plan_monthly,0) * dt.max_day_elapsed_month  AS eoh_u_plan_monthly
	FROM
		(
	    SELECT
	        b.week_idnt
	        , CASE WHEN a.banner_country_num = 1 THEN 110
	              WHEN a.banner_country_num = 3 THEN 210
	          END AS channel_num
	        , a.dept_num
	        , b.month_idnt
	        , b.quarter_idnt
	        , b.wk_of_mth
	    		, SUM(op_ending_of_period_active_cost_amt) as eoh_c_plan_weekly
	    		, SUM(op_ending_of_period_active_qty) as eoh_u_plan_weekly
	    		, SUM(CASE WHEN wk_of_mth  = 1 THEN SUM(op_ending_of_period_active_cost_amt) ELSE 0 END) OVER(PARTITION BY month_idnt, (CASE WHEN a.banner_country_num = 1 THEN 110 WHEN a.banner_country_num = 3 THEN 210 END), dept_num) AS eoh_c_plan_monthly
	    		, SUM(CASE WHEN wk_of_mth  = 1 THEN SUM(op_ending_of_period_active_qty) ELSE 0 END) OVER(PARTITION BY month_idnt, (CASE WHEN a.banner_country_num = 1 THEN 110 WHEN a.banner_country_num = 3 THEN 210 END), dept_num) AS eoh_u_plan_monthly
	    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.mfp_cost_plan_actual_banner_country_fact a
	    INNER JOIN (
	        SELECT
	            distinct
	            b.week_idnt as week_idnt_TRUE
	            , a.week_idnt
	            , a.month_idnt
	            , a.quarter_idnt
	            , ty_ly_lly_ind AS ty_ly_ind
	            , DENSE_RANK() OVER(PARTITION BY a.month_idnt ORDER by a.week_idnt desc) as wk_of_mth
	        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw AS a
	        LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS b
	        	ON b.day_date = a.day_date
	        WHERE a.quarter_idnt IN (SELECT quarter_idnt FROM date_filter)
	    ) b
	      ON a.week_num = b.week_idnt_true
	      AND LOWER(b.ty_ly_ind) = LOWER('TY')
	    WHERE banner_country_num IN (1,3) AND dept_num NOT IN (584, 585) --remove beauty sample depts
	    GROUP BY b.week_idnt,channel_num,a.dept_num,b.month_idnt,b.quarter_idnt,b.wk_of_mth,a.banner_country_num
	    ) sub
	LEFT JOIN date_filter dt
		ON sub.week_idnt = dt.week_idnt
) 
;


--collect stats primary index(day_date, channel_num, dept_num) on eop_plan


CREATE TEMPORARY TABLE IF NOT EXISTS combine_all 
AS (
    SELECT
      COALESCE(a.day_date, b.day_date, c.day_date) AS day_date
    , COALESCE(a.channel_num, b.channel_num, c.channel_num) AS channel_num
    , COALESCE(a.dept_num, b.dept_num, c.dept_num) AS dept_num
		, SUM(a.net_sales_r_ty) AS net_sales_r_ty
		, SUM(a.net_sales_c_ty) AS net_sales_c_ty
		, SUM(a.net_sales_u_ty) AS net_sales_u_ty
		, SUM(a.net_sales_reg_r_ty) AS net_sales_reg_r_ty
		, SUM(a.net_sales_reg_c_ty) AS net_sales_reg_c_ty
		, SUM(a.net_sales_reg_u_ty) AS net_sales_reg_u_ty
		, SUM(a.net_sales_pro_r_ty) AS net_sales_pro_r_ty
		, SUM(a.net_sales_pro_c_ty) AS net_sales_pro_c_ty
		, SUM(a.net_sales_pro_u_ty) AS net_sales_pro_u_ty
		, SUM(a.net_sales_clear_r_ty) AS net_sales_clear_r_ty
		, SUM(a.net_sales_clear_c_ty) AS net_sales_clear_c_ty
		, SUM(a.net_sales_clear_u_ty) AS net_sales_clear_u_ty
		, SUM(a.demand_ty) AS demand_ty
		, SUM(a.demand_u_ty) AS demand_u_ty
		, SUM(a.eoh_r_ty) AS eoh_r_ty
		, SUM(a.eoh_c_ty) AS eoh_c_ty
		, SUM(a.eoh_u_ty) AS eoh_u_ty
		, SUM(a.net_sales_r_ly) AS net_sales_r_ly
		, SUM(a.net_sales_c_ly) AS net_sales_c_ly
		, SUM(a.net_sales_u_ly) AS net_sales_u_ly
		, SUM(a.net_sales_reg_r_ly) AS net_sales_reg_r_ly
		, SUM(a.net_sales_reg_c_ly) AS net_sales_reg_c_ly
		, SUM(a.net_sales_reg_u_ly) AS net_sales_reg_u_ly
		, SUM(a.net_sales_pro_r_ly) AS net_sales_pro_r_ly
		, SUM(a.net_sales_pro_c_ly) AS net_sales_pro_c_ly
		, SUM(a.net_sales_pro_u_ly) AS net_sales_pro_u_ly
		, SUM(a.net_sales_clear_r_ly) AS net_sales_clear_r_ly
		, SUM(a.net_sales_clear_c_ly) AS net_sales_clear_c_ly
		, SUM(a.net_sales_clear_u_ly) AS net_sales_clear_u_ly
		, SUM(a.demand_ly) AS demand_ly
		, SUM(a.demand_u_ly) AS demand_u_ly
		, SUM(a.eoh_r_ly) AS eoh_r_ly
		, SUM(a.eoh_c_ly) AS eoh_c_ly
		, SUM(a.eoh_u_ly) AS eoh_u_ly
    , SUM(net_sales_r_plan_weekly) AS net_sales_r_plan_weekly 
    , SUM(net_sales_u_plan_weekly) AS net_sales_u_plan_weekly 
    , SUM(net_sales_c_plan_weekly) AS net_sales_c_plan_weekly 
    , SUM(net_sales_r_plan_weekly_ly) AS net_sales_r_plan_weekly_ly
    , SUM(demand_r_plan_weekly) AS demand_r_plan_weekly
    , SUM(demand_u_plan_weekly) AS demand_u_plan_weekly
    , SUM(eoh_c_plan_weekly) AS eoh_c_plan_weekly
    , SUM(eoh_u_plan_weekly) AS eoh_u_plan_weekly
    , SUM(net_sales_r_plan_monthly) AS net_sales_r_plan_monthly
    , SUM(net_sales_u_plan_monthly) AS net_sales_u_plan_monthly
    , SUM(net_sales_c_plan_monthly) AS net_sales_c_plan_monthly
    , SUM(net_sales_r_plan_monthly_ly) AS net_sales_r_plan_monthly_ly
    , SUM(demand_r_plan_monthly) AS demand_r_plan_monthly
    , SUM(demand_u_plan_monthly) AS demand_u_plan_monthly
    , SUM(eoh_c_plan_monthly) AS eoh_c_plan_monthly
    , SUM(eoh_u_plan_monthly) AS eoh_u_plan_monthly
    FROM actuals a
    FULL OUTER JOIN sales_demand_plan b
      ON a.channel_num = b.channel_num
      AND a.day_date = b.day_date
      AND a.dept_num = b.dept_num
    FULL OUTER JOIN eop_plan c
      ON a.channel_num = c.channel_num
      AND a.day_date = c.day_date
      AND a.dept_num = c.dept_num
GROUP BY 1,2,3) 
;


--collect stats primary index(day_date, channel_num, dept_num) on combine_all


DROP TABLE IF EXISTS `{{params.gcp_project_id}}`.{{params.environment_schema}}.merch_daily_sales;     


CREATE TABLE IF NOT EXISTS `{{params.gcp_project_id}}`.{{params.environment_schema}}.merch_daily_sales    
AS (	
	SELECT
		ch.banner
		,ch.channel_num
		,ch.channel
		,dep.division
		,dep.subdivision
		,dep.department
		,dep.dept_num
		,dt.fiscal_year_num
		,dt.fiscal_day_num
		,dt.day_date
		,dt.day_desc
		,dt.fiscal_week_num
		,dt.week_desc
		,dt.week_idnt
		,dt.fiscal_month_num
		,dt.month_desc
		,dt.wtd
		,dt.mtd
		,dt.elapsed_week_ind
		,dt.elapsed_day_ind
		,dt.max_day_elapsed
		,dt.max_day_elapsed_week
		,dt.max_day_elapsed_month
		,dt.max_day_elapsed_quarter
		,base.net_sales_r_ty
		,base.net_sales_c_ty
		,base.net_sales_u_ty
		,base.net_sales_reg_r_ty
		,base.net_sales_reg_c_ty
		,base.net_sales_reg_u_ty
		,base.net_sales_pro_r_ty
		,base.net_sales_pro_c_ty
		,base.net_sales_pro_u_ty
		,base.net_sales_clear_r_ty
		,base.net_sales_clear_c_ty
		,base.net_sales_clear_u_ty
		,base.demand_ty
		,base.demand_u_ty
		,base.eoh_r_ty
		,base.eoh_c_ty
		,base.eoh_u_ty
		,base.net_sales_r_ly
		,base.net_sales_c_ly
		,base.net_sales_u_ly
		,base.net_sales_reg_r_ly
		,base.net_sales_reg_c_ly
		,base.net_sales_reg_u_ly
		,base.net_sales_pro_r_ly
		,base.net_sales_pro_c_ly
		,base.net_sales_pro_u_ly
		,base.net_sales_clear_r_ly
		,base.net_sales_clear_c_ly
		,base.net_sales_clear_u_ly
		,base.demand_ly
		,base.demand_u_ly
		,base.eoh_r_ly
		,base.eoh_c_ly
		,base.eoh_u_ly
		,aor.general_merch_manager_executive_vice_president
		,aor.div_merch_manager_senior_vice_president
		,aor.div_merch_manager_vice_president
		,aor.merch_director
		,aor.buyer
		,aor.merch_planning_executive_vice_president
		,aor.merch_planning_senior_vice_president
		,aor.merch_planning_vice_president
		,aor.merch_planning_director_manager
		,aor.assortment_planner
		,base.net_sales_r_plan_weekly
		,base.net_sales_u_plan_weekly
		,base.net_sales_c_plan_weekly
		,base.net_sales_r_plan_weekly_ly
		,base.demand_r_plan_weekly
		,base.demand_u_plan_weekly
		,base.eoh_c_plan_weekly
		,base.eoh_u_plan_weekly
		,base.net_sales_r_plan_monthly
		,base.net_sales_u_plan_monthly
		,base.net_sales_c_plan_monthly
		,base.net_sales_r_plan_monthly_ly
		,base.demand_r_plan_monthly
		,base.demand_u_plan_monthly
		,base.eoh_c_plan_monthly
		,base.eoh_u_plan_monthly
		,TIMESTAMP(CURRENT_DATETIME('PST8PDT')) AS load_tmstp
	FROM combine_all base
	INNER JOIN date_filter dt
		ON base.day_date = dt.day_date
    INNER JOIN channel_lkup ch
    	ON base.channel_num = ch.channel_num
    	AND LOWER(ch.store_country_code) = LOWER('US')
    LEFT JOIN dept_lkup dep
    	ON base.dept_num = dep.dept_num
	LEFT JOIN aor
		ON LOWER(ch.banner) = LOWER(aor.banner)
		AND base.dept_num = aor.dept_num
)
;


--collect stats primary index(day_date, channel_num, dept_num) on T2DL_DAS_SELECTION.merch_daily_sales


--grant select on T2DL_DAS_SELECTION.merch_daily_sales to public