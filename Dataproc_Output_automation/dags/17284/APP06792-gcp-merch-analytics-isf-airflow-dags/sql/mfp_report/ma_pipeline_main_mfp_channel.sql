CREATE TEMPORARY TABLE IF NOT EXISTS date_lkup_wk
AS
SELECT dt.week_idnt,
 dt.month_idnt
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dt
 INNER JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_vws.mfp_cost_plan_actual_banner_country_fact AS mfp ON dt.week_idnt = mfp.week_num
 INNER JOIN (SELECT DISTINCT fiscal_month_idnt
  FROM {{params.gcp_project_id}}.t2dl_das_apt_cost_reporting.merch_assortment_supplier_cluster_plan_fact_vw) AS apt ON dt.month_idnt = apt.fiscal_month_idnt
  
GROUP BY dt.week_idnt,
 dt.month_idnt;


--COLLECT STATS 	PRIMARY INDEX (week_idnt) 		ON date_lkup_wk 


CREATE TEMPORARY TABLE IF NOT EXISTS date_lkup_mth
CLUSTER BY month_idnt, month_idnt_eop
AS
SELECT mth.month_idnt,
 mth.month_idnt_eop
FROM date_lkup_wk AS wk
 INNER JOIN (SELECT month_idnt,
   LEAD(month_idnt, 1) OVER (ORDER BY month_idnt) AS month_idnt_eop
  FROM (SELECT DISTINCT month_idnt
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim) AS eop) AS mth ON wk.month_idnt = mth.month_idnt
GROUP BY mth.month_idnt,
 mth.month_idnt_eop;


--COLLECT STATS 	PRIMARY INDEX (month_idnt,month_idnt_eop) 		ON date_lkup_mth 


CREATE TEMPORARY TABLE IF NOT EXISTS apt_stage
AS
SELECT mth.month_idnt,
  CASE
  WHEN LOWER(fct.selling_country) = LOWER('US') AND LOWER(fct.selling_brand) = LOWER('NORDSTROM')
  THEN 1
  WHEN LOWER(fct.selling_country) = LOWER('CA') AND LOWER(fct.selling_brand) = LOWER('NORDSTROM')
  THEN 2
  WHEN LOWER(fct.selling_country) = LOWER('US') AND LOWER(fct.selling_brand) = LOWER('NORDSTROM_RACK')
  THEN 3
  WHEN LOWER(fct.selling_country) = LOWER('CA') AND LOWER(fct.selling_brand) = LOWER('NORDSTROM_RACK')
  THEN 4
  ELSE NULL
  END AS banner_country_num,
  CASE
  WHEN LOWER(fct.alternate_inventory_model) = LOWER('OWN')
  THEN 3
  WHEN LOWER(fct.alternate_inventory_model) = LOWER('DROPSHIP')
  THEN 1
  ELSE NULL
  END AS fulfill_type_num,
  CAST(TRUNC(CAST(fct.dept_idnt AS FLOAT64)) AS INTEGER) AS dept_num,                                                     --check this
  CASE
  WHEN LOWER(fct.cluster_name) = LOWER('NORDSTROM_CANADA_STORES')
  THEN 111
  WHEN LOWER(fct.cluster_name) = LOWER('NORDSTROM_CANADA_ONLINE')
  THEN 121
  WHEN LOWER(fct.cluster_name) = LOWER('NORDSTROM_STORES')
  THEN 110
  WHEN LOWER(fct.cluster_name) = LOWER('NORDSTROM_ONLINE')
  THEN 120
  WHEN LOWER(fct.cluster_name) = LOWER('RACK_ONLINE')
  THEN 250
  WHEN LOWER(fct.cluster_name) = LOWER('RACK_CANADA_STORES')
  THEN 211
  WHEN LOWER(fct.cluster_name) IN (LOWER('RACK STORES'), LOWER('PRICE'), LOWER('HYBRID'), LOWER('BRAND'))
  THEN 210
  WHEN LOWER(fct.cluster_name) = LOWER('NORD CA RSWH')
  THEN 311
  WHEN LOWER(fct.cluster_name) = LOWER('NORD US RSWH')
  THEN 310
  WHEN LOWER(fct.cluster_name) = LOWER('RACK CA RSWH')
  THEN 261
  WHEN LOWER(fct.cluster_name) = LOWER('RACK US RSWH')
  THEN 260
  ELSE NULL
  END AS channel_num,
 SUM(fct.beginning_of_period_inventory_units) AS bop_u,
 SUM(fct.beginning_of_period_inventory_cost_amount) AS bop_c,
 SUM(fct.replenishment_receipts_units + fct.nonreplenishment_receipts_units + fct.dropship_receipt_units) AS
 receipts_act_u,
 SUM(fct.replenishment_receipts_cost_amount + fct.nonreplenishment_receipts_cost_amount + fct.dropship_receipt_cost_amount
   ) AS receipts_act_c,
 SUM(fct.replenishment_receipts_units + fct.nonreplenishment_receipts_units - fct.replenishment_receipts_less_reserve_units
     - fct.nonreplenishment_receipts_less_reserve_units) AS receipts_res_u,
 SUM(fct.replenishment_receipts_cost_amount + fct.nonreplenishment_receipts_cost_amount - fct.replenishment_receipts_less_reserve_cost_amount
     - fct.nonreplenishment_receipts_less_reserve_cost_amount) AS receipts_res_c
FROM {{params.gcp_project_id}}.t2dl_das_apt_cost_reporting.merch_assortment_supplier_cluster_plan_fact_vw AS fct
 INNER JOIN date_lkup_mth AS mth ON fct.fiscal_month_idnt = mth.month_idnt
GROUP BY mth.month_idnt,
 banner_country_num,
 fulfill_type_num,
 dept_num,
 channel_num;


--COLLECT STATS 	PRIMARY INDEX (month_idnt,banner_country_num,fulfill_type_num,dept_num,channel_num)  		ON apt_stage


CREATE TEMPORARY TABLE IF NOT EXISTS apt_pct
AS
SELECT month_idnt,
 banner_country_num,
 fulfill_type_num,
 dept_num,
 channel_num,
 bop_u,
 bop_c,
 receipts_act_u,
 receipts_act_c,
 receipts_res_u,
 receipts_res_c,
  CAST(bop_u AS NUMERIC) / IF((SUM(bop_u) OVER (PARTITION BY month_idnt, banner_country_num, fulfill_type_num, dept_num
      RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) = 0, NULL, SUM(bop_u) OVER (PARTITION BY month_idnt,
      banner_country_num, fulfill_type_num, dept_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) AS
 bop_u_pct,
  CAST(bop_c AS BIGNUMERIC) / IF((SUM(bop_c) OVER (PARTITION BY month_idnt, banner_country_num, fulfill_type_num,
        dept_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) = 0, NULL, SUM(bop_c) OVER (PARTITION BY
      month_idnt, banner_country_num, fulfill_type_num, dept_num RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING)) AS bop_c_pct,
  CAST(receipts_act_u AS NUMERIC) / IF((SUM(receipts_act_u) OVER (PARTITION BY month_idnt, banner_country_num,
        fulfill_type_num, dept_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) = 0, NULL, SUM(receipts_act_u
    ) OVER (PARTITION BY month_idnt, banner_country_num, fulfill_type_num, dept_num RANGE BETWEEN UNBOUNDED PRECEDING
    AND UNBOUNDED FOLLOWING)) AS receipts_act_u_pct,
  CAST(receipts_act_c AS BIGNUMERIC) / IF((SUM(receipts_act_c) OVER (PARTITION BY month_idnt, banner_country_num,
        fulfill_type_num, dept_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) = 0, NULL, SUM(receipts_act_c
    ) OVER (PARTITION BY month_idnt, banner_country_num, fulfill_type_num, dept_num RANGE BETWEEN UNBOUNDED PRECEDING
    AND UNBOUNDED FOLLOWING)) AS receipts_act_c_pct,
  CAST(receipts_res_u AS NUMERIC) / IF((SUM(receipts_res_u) OVER (PARTITION BY month_idnt, banner_country_num,
        fulfill_type_num, dept_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) = 0, NULL, SUM(receipts_res_u
    ) OVER (PARTITION BY month_idnt, banner_country_num, fulfill_type_num, dept_num RANGE BETWEEN UNBOUNDED PRECEDING
    AND UNBOUNDED FOLLOWING)) AS receipts_res_u_pct,
  CAST(receipts_res_c AS BIGNUMERIC) / IF((SUM(receipts_res_c) OVER (PARTITION BY month_idnt, banner_country_num,
        fulfill_type_num, dept_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) = 0, NULL, SUM(receipts_res_c
    ) OVER (PARTITION BY month_idnt, banner_country_num, fulfill_type_num, dept_num RANGE BETWEEN UNBOUNDED PRECEDING
    AND UNBOUNDED FOLLOWING)) AS receipts_res_c_pct
FROM apt_stage AS fct;


--COLLECT STATS 	PRIMARY INDEX (month_idnt,banner_country_num,fulfill_type_num,channel_num,dept_num)  		ON apt_pct


CREATE TEMPORARY TABLE IF NOT EXISTS bop_u
AS
SELECT mfp.week_num,
 COALESCE(wk.month_idnt, apt.month_idnt) AS month_idnt,
 COALESCE(mfp.banner_country_num, apt.banner_country_num) AS banner_country_num,
 COALESCE(mfp.fulfill_type_num, apt.fulfill_type_num) AS fulfill_type_num,
 COALESCE(mfp.dept_num, apt.dept_num) AS dept_num,
 COALESCE(apt.channel_num, 0) AS channel_num,
 ROUND(CAST(mfp.sp_beginning_of_period_active_qty * COALESCE(IF(apt.bop_u_pct = 0, NULL, apt.bop_u_pct), 1) AS NUMERIC)
  , 8) AS sp_beginning_of_period_active_qty,
 ROUND(CAST(mfp.op_beginning_of_period_active_qty * COALESCE(IF(apt.bop_u_pct = 0, NULL, apt.bop_u_pct), 1) AS NUMERIC)
  , 8) AS op_beginning_of_period_active_qty
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_vws.mfp_cost_plan_actual_banner_country_fact AS mfp
 INNER JOIN date_lkup_wk AS wk ON mfp.week_num = wk.week_idnt
 LEFT JOIN apt_pct AS apt ON wk.month_idnt = apt.month_idnt AND mfp.banner_country_num = apt.banner_country_num AND mfp
     .fulfill_type_num = apt.fulfill_type_num AND mfp.dept_num = apt.dept_num AND apt.bop_u > 0;


--COLLECT STATS 	PRIMARY INDEX (week_num,banner_country_num,fulfill_type_num,channel_num,dept_num)  		ON bop_u 


CREATE TEMPORARY TABLE IF NOT EXISTS bop_c
CLUSTER BY week_num, banner_country_num, fulfill_type_num, channel_num
AS
SELECT mfp.week_num,
 COALESCE(wk.month_idnt, apt.month_idnt) AS month_idnt,
 COALESCE(mfp.banner_country_num, apt.banner_country_num) AS banner_country_num,
 COALESCE(mfp.fulfill_type_num, apt.fulfill_type_num) AS fulfill_type_num,
 COALESCE(mfp.dept_num, apt.dept_num) AS dept_num,
 COALESCE(apt.channel_num, 0) AS channel_num,
 CAST(mfp.sp_beginning_of_period_active_cost_amt * COALESCE(IF(apt.bop_c_pct = 0, NULL, apt.bop_c_pct), 1) AS NUMERIC)
 AS sp_beginning_of_period_active_cost_amt,
 CAST(mfp.op_beginning_of_period_active_cost_amt * COALESCE(IF(apt.bop_c_pct = 0, NULL, apt.bop_c_pct), 1) AS NUMERIC)
 AS op_beginning_of_period_active_cost_amt
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_vws.mfp_cost_plan_actual_banner_country_fact AS mfp
 INNER JOIN date_lkup_wk AS wk ON mfp.week_num = wk.week_idnt
 LEFT JOIN apt_pct AS apt ON wk.month_idnt = apt.month_idnt AND mfp.banner_country_num = apt.banner_country_num AND mfp
     .fulfill_type_num = apt.fulfill_type_num AND mfp.dept_num = apt.dept_num AND apt.bop_c > 0;


--COLLECT STATS 	PRIMARY INDEX (week_num,banner_country_num,fulfill_type_num,channel_num,dept_num)  		ON bop_c 


CREATE TEMPORARY TABLE IF NOT EXISTS eop_u
AS
SELECT mfp.week_num,
 COALESCE(wk.month_idnt, mth.month_idnt) AS month_idnt,
 COALESCE(mfp.banner_country_num, apt.banner_country_num) AS banner_country_num,
 COALESCE(mfp.fulfill_type_num, apt.fulfill_type_num) AS fulfill_type_num,
 COALESCE(mfp.dept_num, apt.dept_num) AS dept_num,
 COALESCE(apt.channel_num, 0) AS channel_num,
 ROUND(CAST(mfp.sp_ending_of_period_active_qty * COALESCE(IF(apt.bop_u_pct = 0, NULL, apt.bop_u_pct), 1) AS NUMERIC), 8
  ) AS sp_ending_of_period_active_qty,
 ROUND(CAST(mfp.op_ending_of_period_active_qty * COALESCE(IF(apt.bop_u_pct = 0, NULL, apt.bop_u_pct), 1) AS NUMERIC), 8
  ) AS op_ending_of_period_active_qty
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_vws.mfp_cost_plan_actual_banner_country_fact AS mfp
 INNER JOIN date_lkup_wk AS wk ON mfp.week_num = wk.week_idnt
 INNER JOIN date_lkup_mth AS mth ON wk.month_idnt = mth.month_idnt
 LEFT JOIN apt_pct AS apt ON mth.month_idnt_eop = apt.month_idnt AND mfp.banner_country_num = apt.banner_country_num AND
     mfp.fulfill_type_num = apt.fulfill_type_num AND mfp.dept_num = apt.dept_num AND apt.bop_u > 0;


--COLLECT STATS 	PRIMARY INDEX (week_num,banner_country_num,fulfill_type_num,channel_num,dept_num)  		ON eop_u 


CREATE TEMPORARY TABLE IF NOT EXISTS eop_c
AS
SELECT mfp.week_num,
 COALESCE(wk.month_idnt, mth.month_idnt) AS month_idnt,
 COALESCE(mfp.banner_country_num, apt.banner_country_num) AS banner_country_num,
 COALESCE(mfp.fulfill_type_num, apt.fulfill_type_num) AS fulfill_type_num,
 COALESCE(mfp.dept_num, apt.dept_num) AS dept_num,
 COALESCE(apt.channel_num, 0) AS channel_num,
 CAST(mfp.sp_ending_of_period_active_cost_amt * COALESCE(IF(apt.bop_c_pct = 0, NULL, apt.bop_c_pct), 1) AS NUMERIC) AS
 sp_ending_of_period_active_cost_amt,
 CAST(mfp.op_ending_of_period_active_cost_amt * COALESCE(IF(apt.bop_c_pct = 0, NULL, apt.bop_c_pct), 1) AS NUMERIC) AS
 op_ending_of_period_active_cost_amt
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_vws.mfp_cost_plan_actual_banner_country_fact AS mfp
 INNER JOIN date_lkup_wk AS wk ON mfp.week_num = wk.week_idnt
 INNER JOIN date_lkup_mth AS mth ON wk.month_idnt = mth.month_idnt
 LEFT JOIN apt_pct AS apt ON mth.month_idnt_eop = apt.month_idnt AND mfp.banner_country_num = apt.banner_country_num AND
     mfp.fulfill_type_num = apt.fulfill_type_num AND mfp.dept_num = apt.dept_num AND apt.bop_c > 0;


--COLLECT STATS 	PRIMARY INDEX (week_num,banner_country_num,fulfill_type_num,channel_num,dept_num)  		ON eop_c 


CREATE TEMPORARY TABLE IF NOT EXISTS receipt_u
AS
SELECT mfp.week_num,
 COALESCE(wk.month_idnt, apt.month_idnt) AS month_idnt,
 COALESCE(mfp.banner_country_num, apt.banner_country_num) AS banner_country_num,
 COALESCE(mfp.fulfill_type_num, apt.fulfill_type_num) AS fulfill_type_num,
 COALESCE(mfp.dept_num, apt.dept_num) AS dept_num,
 COALESCE(apt.channel_num, 0) AS channel_num,
 ROUND(CAST(mfp.sp_receipts_active_qty * COALESCE(IF(apt.receipts_act_u_pct = 0, NULL, apt.receipts_act_u_pct), 1) AS NUMERIC)
  , 8) AS sp_receipts_active_qty,
 ROUND(CAST(mfp.op_receipts_active_qty * COALESCE(IF(apt.receipts_act_u_pct = 0, NULL, apt.receipts_act_u_pct), 1) AS NUMERIC)
  , 8) AS op_receipts_active_qty
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_vws.mfp_cost_plan_actual_banner_country_fact AS mfp
 INNER JOIN date_lkup_wk AS wk ON mfp.week_num = wk.week_idnt
 LEFT JOIN apt_pct AS apt ON wk.month_idnt = apt.month_idnt AND mfp.banner_country_num = apt.banner_country_num AND mfp
     .fulfill_type_num = apt.fulfill_type_num AND mfp.dept_num = apt.dept_num AND apt.receipts_act_u > 0;


--COLLECT STATS 	PRIMARY INDEX (week_num,banner_country_num,fulfill_type_num,channel_num,dept_num)  		ON receipt_u 


CREATE TEMPORARY TABLE IF NOT EXISTS receipt_c
AS
SELECT mfp.week_num,
 COALESCE(wk.month_idnt, apt.month_idnt) AS month_idnt,
 COALESCE(mfp.banner_country_num, apt.banner_country_num) AS banner_country_num,
 COALESCE(mfp.fulfill_type_num, apt.fulfill_type_num) AS fulfill_type_num,
 COALESCE(mfp.dept_num, apt.dept_num) AS dept_num,
 COALESCE(apt.channel_num, 0) AS channel_num,
 CAST(mfp.sp_receipts_active_cost_amt * COALESCE(IF(apt.receipts_act_c_pct = 0, NULL, apt.receipts_act_c_pct), 1) AS NUMERIC)
 AS sp_receipts_active_cost_amt,
 CAST(mfp.op_receipts_active_cost_amt * COALESCE(IF(apt.receipts_act_c_pct = 0, NULL, apt.receipts_act_c_pct), 1) AS NUMERIC)
 AS op_receipts_active_cost_amt
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_vws.mfp_cost_plan_actual_banner_country_fact AS mfp
 INNER JOIN date_lkup_wk AS wk ON mfp.week_num = wk.week_idnt
 LEFT JOIN apt_pct AS apt ON wk.month_idnt = apt.month_idnt AND mfp.banner_country_num = apt.banner_country_num AND mfp
     .fulfill_type_num = apt.fulfill_type_num AND mfp.dept_num = apt.dept_num AND apt.receipts_act_c > 0;


--COLLECT STATS 	PRIMARY INDEX (week_num,banner_country_num,fulfill_type_num,channel_num,dept_num)  		ON receipt_c 


CREATE TEMPORARY TABLE IF NOT EXISTS reserve_u
AS
SELECT mfp.week_num,
 COALESCE(wk.month_idnt, apt.month_idnt) AS month_idnt,
 COALESCE(mfp.banner_country_num, apt.banner_country_num) AS banner_country_num,
 COALESCE(mfp.fulfill_type_num, apt.fulfill_type_num) AS fulfill_type_num,
 COALESCE(mfp.dept_num, apt.dept_num) AS dept_num,
 COALESCE(apt.channel_num, 0) AS channel_num,
 ROUND(CAST(mfp.sp_receipts_reserve_qty * COALESCE(IF(apt.receipts_res_u_pct = 0, NULL, apt.receipts_res_u_pct), 1) AS NUMERIC)
  , 8) AS sp_receipts_reserve_qty,
 ROUND(CAST(mfp.op_receipts_reserve_qty * COALESCE(IF(apt.receipts_res_u_pct = 0, NULL, apt.receipts_res_u_pct), 1) AS NUMERIC)
  , 8) AS op_receipts_reserve_qty
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_vws.mfp_cost_plan_actual_banner_country_fact AS mfp
 INNER JOIN date_lkup_wk AS wk ON mfp.week_num = wk.week_idnt
 LEFT JOIN apt_pct AS apt ON wk.month_idnt = apt.month_idnt AND mfp.banner_country_num = apt.banner_country_num AND mfp
     .fulfill_type_num = apt.fulfill_type_num AND mfp.dept_num = apt.dept_num AND apt.receipts_res_u > 0;


--COLLECT STATS 	PRIMARY INDEX (week_num,banner_country_num,fulfill_type_num,channel_num,dept_num)  		ON reserve_u 


CREATE TEMPORARY TABLE IF NOT EXISTS reserve_c
AS
SELECT mfp.week_num,
 COALESCE(wk.month_idnt, apt.month_idnt) AS month_idnt,
 COALESCE(mfp.banner_country_num, apt.banner_country_num) AS banner_country_num,
 COALESCE(mfp.fulfill_type_num, apt.fulfill_type_num) AS fulfill_type_num,
 COALESCE(mfp.dept_num, apt.dept_num) AS dept_num,
 COALESCE(apt.channel_num, 0) AS channel_num,
 CAST(mfp.sp_receipts_reserve_cost_amt * COALESCE(IF(apt.receipts_res_c_pct = 0, NULL, apt.receipts_res_c_pct), 1) AS NUMERIC)
 AS sp_receipts_reserve_cost_amt,
 CAST(mfp.op_receipts_reserve_cost_amt * COALESCE(IF(apt.receipts_res_c_pct = 0, NULL, apt.receipts_res_c_pct), 1) AS NUMERIC)
 AS op_receipts_reserve_cost_amt
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_vws.mfp_cost_plan_actual_banner_country_fact AS mfp
 INNER JOIN date_lkup_wk AS wk ON mfp.week_num = wk.week_idnt
 LEFT JOIN apt_pct AS apt ON wk.month_idnt = apt.month_idnt AND mfp.banner_country_num = apt.banner_country_num AND mfp
     .fulfill_type_num = apt.fulfill_type_num AND mfp.dept_num = apt.dept_num AND apt.receipts_res_c > 0;


--COLLECT STATS 	PRIMARY INDEX (week_num,banner_country_num,fulfill_type_num,channel_num,dept_num)  		ON reserve_u 


CREATE TEMPORARY TABLE IF NOT EXISTS base
AS
(
  SELECT
	 week_num
	,month_idnt
	,banner_country_num
	,fulfill_type_num
	,dept_num
	,channel_num
FROM bop_u
GROUP BY week_num,month_idnt,banner_country_num,fulfill_type_num,dept_num,channel_num
UNION DISTINCT
SELECT
	 week_num
	,month_idnt
	,banner_country_num
	,fulfill_type_num
	,dept_num
	,channel_num
FROM bop_c
GROUP BY week_num,month_idnt,banner_country_num,fulfill_type_num,dept_num,channel_num
UNION DISTINCT
SELECT
	 week_num
	,month_idnt
	,banner_country_num
	,fulfill_type_num
	,dept_num
	,channel_num
FROM eop_u
GROUP BY week_num,month_idnt,banner_country_num,fulfill_type_num,dept_num,channel_num
UNION DISTINCT
SELECT
	 week_num
	,month_idnt
	,banner_country_num
	,fulfill_type_num
	,dept_num
	,channel_num
FROM eop_c
GROUP BY week_num,month_idnt,banner_country_num,fulfill_type_num,dept_num,channel_num
UNION DISTINCT
SELECT
	 week_num
	,month_idnt
	,banner_country_num
	,fulfill_type_num
	,dept_num
	,channel_num
FROM receipt_u
GROUP BY week_num,month_idnt,banner_country_num,fulfill_type_num,dept_num,channel_num
UNION DISTINCT
SELECT
	 week_num
	,month_idnt
	,banner_country_num
	,fulfill_type_num
	,dept_num
	,channel_num
FROM receipt_c
GROUP BY week_num,month_idnt,banner_country_num,fulfill_type_num,dept_num,channel_num
UNION DISTINCT
SELECT
	 week_num
	,month_idnt
	,banner_country_num
	,fulfill_type_num
	,dept_num
	,channel_num
FROM reserve_u
GROUP BY week_num,month_idnt,banner_country_num,fulfill_type_num,dept_num,channel_num
UNION DISTINCT
SELECT
	 week_num
	,month_idnt
	,banner_country_num
	,fulfill_type_num
	,dept_num
	,channel_num
FROM reserve_c
GROUP BY week_num,month_idnt,banner_country_num,fulfill_type_num,dept_num,channel_num
);


--COLLECT STATS 	PRIMARY INDEX (week_num,banner_country_num,fulfill_type_num,channel_num,dept_num)  		ON base 


TRUNCATE TABLE {{params.gcp_project_id}}.{{params.environment_schema}}.mfp_inv_channel_plans{{params.env_suffix}};


INSERT INTO {{params.gcp_project_id}}.{{params.environment_schema}}.mfp_inv_channel_plans{{params.env_suffix}}
(SELECT b.week_num,
  b.month_idnt,
  b.banner_country_num,
  b.fulfill_type_num,
  b.dept_num,
  b.channel_num,
  CAST(TRUNC(CAST(bop_u.sp_beginning_of_period_active_qty AS FLOAT64)) AS BIGINT) AS sp_beginning_of_period_active_qty,
  ROUND(CAST(bop_c.sp_beginning_of_period_active_cost_amt AS NUMERIC), 2) AS sp_beginning_of_period_active_cost_amt,
  CAST(TRUNC(CAST(bop_u.op_beginning_of_period_active_qty AS FLOAT64)) AS BIGINT) AS op_beginning_of_period_active_qty,
  ROUND(CAST(bop_c.op_beginning_of_period_active_cost_amt AS NUMERIC), 2) AS op_beginning_of_period_active_cost_amt,
  CAST(TRUNC(CAST(eop_u.sp_ending_of_period_active_qty AS FLOAT64)) AS BIGINT) AS sp_ending_of_period_active_qty,
  ROUND(CAST(eop_c.sp_ending_of_period_active_cost_amt AS NUMERIC), 2) AS sp_ending_of_period_active_cost_amt,
  CAST(TRUNC(CAST(eop_u.op_ending_of_period_active_qty AS FLOAT64)) AS BIGINT) AS op_ending_of_period_active_qty,
  ROUND(CAST(eop_c.op_ending_of_period_active_cost_amt AS NUMERIC), 2) AS op_ending_of_period_active_cost_amt,
  CAST(TRUNC(CAST(receipt_u.sp_receipts_active_qty AS FLOAT64)) AS BIGINT) AS sp_receipts_active_qty,
  ROUND(CAST(receipt_c.sp_receipts_active_cost_amt AS NUMERIC), 2) AS sp_receipts_active_cost_amt,
  CAST(TRUNC(CAST(receipt_u.op_receipts_active_qty AS FLOAT64)) AS BIGINT) AS op_receipts_active_qty,
  ROUND(CAST(receipt_c.op_receipts_active_cost_amt AS NUMERIC), 2) AS op_receipts_active_cost_amt,
  CAST(TRUNC(CAST(reserve_u.sp_receipts_reserve_qty AS FLOAT64)) AS BIGINT) AS sp_receipts_reserve_qty,
  ROUND(CAST(reserve_c.sp_receipts_reserve_cost_amt AS NUMERIC), 2) AS sp_receipts_reserve_cost_amt,
  CAST(TRUNC(CAST(reserve_u.op_receipts_reserve_qty AS FLOAT64)) AS BIGINT) AS op_receipts_reserve_qty,
  ROUND(CAST(reserve_c.op_receipts_reserve_cost_amt AS NUMERIC), 2) AS op_receipts_reserve_cost_amt,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS update_timestamp,
  `{{params.gcp_project_id}}.JWN_UDF.DEFAULT_TZ_PST`()
 FROM base AS b
  LEFT JOIN bop_u ON b.week_num = bop_u.week_num AND b.month_idnt = bop_u.month_idnt AND b.banner_country_num = bop_u.banner_country_num
        AND b.fulfill_type_num = bop_u.fulfill_type_num AND b.dept_num = bop_u.dept_num AND b.channel_num = bop_u.channel_num
    
  LEFT JOIN bop_c ON b.week_num = bop_c.week_num AND b.month_idnt = bop_c.month_idnt AND b.banner_country_num = bop_c.banner_country_num
        AND b.fulfill_type_num = bop_c.fulfill_type_num AND b.dept_num = bop_c.dept_num AND b.channel_num = bop_c.channel_num
    
  LEFT JOIN eop_u ON b.week_num = eop_u.week_num AND b.month_idnt = eop_u.month_idnt AND b.banner_country_num = eop_u.banner_country_num
        AND b.fulfill_type_num = eop_u.fulfill_type_num AND b.dept_num = eop_u.dept_num AND b.channel_num = eop_u.channel_num
    
  LEFT JOIN eop_c ON b.week_num = eop_c.week_num AND b.month_idnt = eop_c.month_idnt AND b.banner_country_num = eop_c.banner_country_num
        AND b.fulfill_type_num = eop_c.fulfill_type_num AND b.dept_num = eop_c.dept_num AND b.channel_num = eop_c.channel_num
    
  LEFT JOIN receipt_u ON b.week_num = receipt_u.week_num AND b.month_idnt = receipt_u.month_idnt AND b.banner_country_num
        = receipt_u.banner_country_num AND b.fulfill_type_num = receipt_u.fulfill_type_num AND b.dept_num = receipt_u.dept_num
      AND b.channel_num = receipt_u.channel_num
  LEFT JOIN receipt_c ON b.week_num = receipt_c.week_num AND b.month_idnt = receipt_c.month_idnt AND b.banner_country_num
        = receipt_c.banner_country_num AND b.fulfill_type_num = receipt_c.fulfill_type_num AND b.dept_num = receipt_c.dept_num
      AND b.channel_num = receipt_c.channel_num
  LEFT JOIN reserve_u ON b.week_num = reserve_u.week_num AND b.month_idnt = reserve_u.month_idnt AND b.banner_country_num
        = reserve_u.banner_country_num AND b.fulfill_type_num = reserve_u.fulfill_type_num AND b.dept_num = reserve_u.dept_num
      AND b.channel_num = reserve_u.channel_num
  LEFT JOIN reserve_c ON b.week_num = reserve_c.week_num AND b.month_idnt = reserve_c.month_idnt AND b.banner_country_num
        = reserve_c.banner_country_num AND b.fulfill_type_num = reserve_c.fulfill_type_num AND b.dept_num = reserve_c.dept_num
      AND b.channel_num = reserve_c.channel_num);


--COLLECT STATS 	PRIMARY INDEX (week_num,banner_country_num,fulfill_type_num,channel_num,dept_num)  		ON t2dl_das_ace_mfp.mfp_inv_channel_plans 