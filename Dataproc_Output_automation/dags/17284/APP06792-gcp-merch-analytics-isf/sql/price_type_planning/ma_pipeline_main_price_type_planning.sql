/*------------------------------------------------------
 Price Type Planning Summary
 Purpose: Provide department-level in combination with division/subdivision/pricetype/planversion/channel-level price type information at a weekly grain
 
 Last Update: 02/13/24 Rasagnya Avala - Update dates logic to bring in 2023 data
 Contact for logic/code: Rasagnya Avala (Merch Analytics)
 Contact for ETL: Rasagnya Avala (Merch Analytics)
 --------------------------------------------------------*/

-- Table with department level price type details along with ratios to line up with MFP costs
-- Methodology:Obtain MFP actual costs, divide by price type costs to get ratio. Use the ratio to then divide price type costs with and align with each fulfill type, plan version and price type (Use LAST_VALUE())




--,Net_Sales_R_MFP


-- OP 
BEGIN

CREATE TEMPORARY TABLE IF NOT EXISTS base
AS
SELECT department_num,
 week_num,
 channel_num,
 price_type,
 plan_version,
 ownership,
 fulfill_type_num,
  net_sales_r / (LAST_VALUE(net_sales_r_ratio) OVER (PARTITION BY week_num, department_num, channel_num, ownership,
      plan_version ORDER BY price_type DESC, ownership, plan_version RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING)) AS net_sales_r,
  net_sales_c / (LAST_VALUE(net_sales_c_ratio) OVER (PARTITION BY week_num, department_num, channel_num, ownership,
      plan_version ORDER BY price_type DESC, ownership, plan_version RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING)) AS net_sales_c,
  net_sales_u / (LAST_VALUE(net_sales_u_ratio) OVER (PARTITION BY week_num, department_num, channel_num, ownership,
      plan_version ORDER BY price_type DESC, ownership, plan_version RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING)) AS net_sales_u
FROM (SELECT pd.department_num,
    pd.week_num,
    pd.channel_num,
    pd.price_type,
    pd.plan_version,
    pd.ownership,
    mcpacf.fulfill_type_num,
    SUM(CASE
      WHEN LOWER(pd.plan_version) = LOWER('Cp')
      THEN pd.gross_sls_r - pd.rtn_r
      ELSE NULL
      END) AS net_sales_r,
    SUM(CASE
      WHEN LOWER(pd.price_type) = LOWER('Total')
      THEN mcpacf.cp_net_sales_retail_amt
      ELSE NULL
      END) AS net_sales_r_mfp,
    NULLIF(SUM(pd.gross_sls_r - pd.rtn_r) / CAST(IF(SUM(CASE
           WHEN LOWER(pd.price_type) = LOWER('Total')
           THEN mcpacf.cp_net_sales_retail_amt
           ELSE NULL
           END) = 0, NULL, SUM(CASE
          WHEN LOWER(pd.price_type) = LOWER('Total')
          THEN mcpacf.cp_net_sales_retail_amt
          ELSE NULL
          END)) AS BIGNUMERIC), 0) AS net_sales_r_ratio,
    SUM(CASE
      WHEN LOWER(pd.plan_version) = LOWER('Cp')
      THEN pd.gross_sls_c - pd.rtn_c
      ELSE NULL
      END) AS net_sales_c,
    SUM(CASE
      WHEN LOWER(pd.price_type) = LOWER('Total')
      THEN mcpacf.cp_net_sales_cost_amt
      ELSE NULL
      END) AS net_sales_c_mfp,
    NULLIF(SUM(pd.gross_sls_c - pd.rtn_c) / CAST(IF(SUM(CASE
           WHEN LOWER(pd.price_type) = LOWER('Total')
           THEN mcpacf.cp_net_sales_cost_amt
           ELSE NULL
           END) = 0, NULL, SUM(CASE
          WHEN LOWER(pd.price_type) = LOWER('Total')
          THEN mcpacf.cp_net_sales_cost_amt
          ELSE NULL
          END)) AS BIGNUMERIC), 0) AS net_sales_c_ratio,
    SUM(CASE
      WHEN LOWER(pd.plan_version) = LOWER('Cp')
      THEN pd.gross_sls_u - pd.rtn_u
      ELSE NULL
      END) AS net_sales_u,
    SUM(CASE
      WHEN LOWER(pd.price_type) = LOWER('Total')
      THEN mcpacf.cp_net_sales_qty
      ELSE NULL
      END) AS net_sales_u_mfp,
    NULLIF(SUM(pd.gross_sls_u - pd.rtn_u) / CAST(IF(SUM(CASE
           WHEN LOWER(pd.price_type) = LOWER('Total')
           THEN mcpacf.cp_net_sales_qty
           ELSE NULL
           END) = 0, NULL, SUM(CASE
          WHEN LOWER(pd.price_type) = LOWER('Total')
          THEN mcpacf.cp_net_sales_qty
          ELSE NULL
          END)) AS BIGNUMERIC), 0) AS net_sales_u_ratio
   FROM `{{params.gcp_project_id}}`.t2dl_das_price_type_plan_outputs.price_type_plan_data AS pd
    LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.mfp_cost_plan_actual_channel_fact AS mcpacf 
	ON pd.department_num = mcpacf.dept_num 
	AND pd.week_num = mcpacf.week_num 
	AND pd.channel_num = mcpacf.channel_num 
	AND mcpacf.fulfill_type_num = CASE
       WHEN LOWER(pd.ownership) = LOWER('Own')
       THEN 3
       WHEN LOWER(pd.ownership) = LOWER('DropShip')
       THEN 1
       WHEN LOWER(pd.ownership) = LOWER('RevShare')
       THEN 2
       ELSE NULL
       END
   WHERE pd.year_num >= (SELECT DISTINCT fiscal_year_num - 1
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
      WHERE day_date = CURRENT_DATE('PST8PDT'))
    AND pd.channel_num IN (110, 120, 210, 250)
    AND mcpacf.channel_num IN (110, 120, 210, 250)
    AND LOWER(pd.plan_version) = LOWER('Cp')
   GROUP BY pd.department_num,
    pd.week_num,
    pd.channel_num,
    pd.price_type,
    pd.plan_version,
    pd.ownership,
    mcpacf.fulfill_type_num
   UNION ALL
   SELECT pd0.department_num,
    pd0.week_num,
    pd0.channel_num,
    pd0.price_type,
    pd0.plan_version,
    pd0.ownership,
    mcpacf0.fulfill_type_num,
    SUM(CASE
      WHEN LOWER(pd0.plan_version) = LOWER('Sp')
      THEN pd0.gross_sls_r - pd0.rtn_r
      ELSE NULL
      END) AS net_sales_r,
    SUM(CASE
      WHEN LOWER(pd0.price_type) = LOWER('Total')
      THEN mcpacf0.sp_net_sales_retail_amt
      ELSE NULL
      END) AS net_sales_r_mfp,
    NULLIF(SUM(pd0.gross_sls_r - pd0.rtn_r) / CAST(IF(SUM(CASE
           WHEN LOWER(pd0.price_type) = LOWER('Total')
           THEN mcpacf0.sp_net_sales_retail_amt
           ELSE NULL
           END) = 0, NULL, SUM(CASE
          WHEN LOWER(pd0.price_type) = LOWER('Total')
          THEN mcpacf0.sp_net_sales_retail_amt
          ELSE NULL
          END)) AS BIGNUMERIC), 0) AS net_sales_r_ratio,
    SUM(CASE
      WHEN LOWER(pd0.plan_version) = LOWER('Sp')
      THEN pd0.gross_sls_c - pd0.rtn_c
      ELSE NULL
      END) AS net_sales_c,
    SUM(CASE
      WHEN LOWER(pd0.price_type) = LOWER('Total')
      THEN mcpacf0.sp_net_sales_cost_amt
      ELSE NULL
      END) AS net_sales_c_mfp,
    NULLIF(SUM(pd0.gross_sls_c - pd0.rtn_c) / CAST(IF(SUM(CASE
           WHEN LOWER(pd0.price_type) = LOWER('Total')
           THEN mcpacf0.sp_net_sales_cost_amt
           ELSE NULL
           END) = 0, NULL, SUM(CASE
          WHEN LOWER(pd0.price_type) = LOWER('Total')
          THEN mcpacf0.sp_net_sales_cost_amt
          ELSE NULL
          END)) AS BIGNUMERIC), 0) AS net_sales_c_ratio,
    SUM(CASE
      WHEN LOWER(pd0.plan_version) = LOWER('Sp')
      THEN pd0.gross_sls_u - pd0.rtn_u
      ELSE NULL
      END) AS net_sales_u,
    SUM(CASE
      WHEN LOWER(pd0.price_type) = LOWER('Total')
      THEN mcpacf0.sp_net_sales_qty
      ELSE NULL
      END) AS net_sales_u_mfp,
    NULLIF(SUM(pd0.gross_sls_u - pd0.rtn_u) / CAST(IF(SUM(CASE
           WHEN LOWER(pd0.price_type) = LOWER('Total')
           THEN mcpacf0.sp_net_sales_qty
           ELSE NULL
           END) = 0, NULL, SUM(CASE
          WHEN LOWER(pd0.price_type) = LOWER('Total')
          THEN mcpacf0.sp_net_sales_qty
          ELSE NULL
          END)) AS BIGNUMERIC), 0) AS net_sales_u_ratio
   FROM `{{params.gcp_project_id}}`.t2dl_das_price_type_plan_outputs.price_type_plan_data AS pd0
    LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.mfp_cost_plan_actual_channel_fact AS mcpacf0 
	ON pd0.department_num = mcpacf0.dept_num AND
        pd0.week_num = mcpacf0.week_num AND pd0.channel_num = mcpacf0.channel_num AND mcpacf0.fulfill_type_num = CASE
       WHEN LOWER(pd0.ownership) = LOWER('Own')
       THEN 3
       WHEN LOWER(pd0.ownership) = LOWER('DropShip')
       THEN 1
       WHEN LOWER(pd0.ownership) = LOWER('RevShare')
       THEN 2
       ELSE NULL
       END
   WHERE pd0.year_num >= (SELECT DISTINCT fiscal_year_num - 1
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
      WHERE day_date = CURRENT_DATE('PST8PDT'))
    AND pd0.channel_num IN (110, 120, 210, 250)
    AND mcpacf0.channel_num IN (110, 120, 210, 250)
    AND LOWER(pd0.plan_version) = LOWER('Sp')
   GROUP BY pd0.department_num,
    pd0.week_num,
    pd0.channel_num,
    pd0.price_type,
    pd0.plan_version,
    pd0.ownership,
    mcpacf0.fulfill_type_num
   UNION ALL
   SELECT pd1.department_num,
    pd1.week_num,
    pd1.channel_num,
    pd1.price_type,
    pd1.plan_version,
    pd1.ownership,
    mcpacf1.fulfill_type_num,
    SUM(CASE
      WHEN LOWER(pd1.plan_version) = LOWER('Op')
      THEN pd1.gross_sls_r - pd1.rtn_r
      ELSE NULL
      END) AS net_sales_r,
    SUM(CASE
      WHEN LOWER(pd1.price_type) = LOWER('Total')
      THEN mcpacf1.op_net_sales_retail_amt
      ELSE NULL
      END) AS net_sales_r_mfp,
    NULLIF(SUM(pd1.gross_sls_r - pd1.rtn_r) / CAST(IF(SUM(CASE
           WHEN LOWER(pd1.price_type) = LOWER('Total')
           THEN mcpacf1.op_net_sales_retail_amt
           ELSE NULL
           END) = 0, NULL, SUM(CASE
          WHEN LOWER(pd1.price_type) = LOWER('Total')
          THEN mcpacf1.op_net_sales_retail_amt
          ELSE NULL
          END)) AS BIGNUMERIC), 0) AS net_sales_r_ratio,
    SUM(CASE
      WHEN LOWER(pd1.plan_version) = LOWER('Op')
      THEN pd1.gross_sls_c - pd1.rtn_c
      ELSE NULL
      END) AS net_sales_c,
    SUM(CASE
      WHEN LOWER(pd1.price_type) = LOWER('Total')
      THEN mcpacf1.op_net_sales_cost_amt
      ELSE NULL
      END) AS net_sales_c_mfp,
    NULLIF(SUM(pd1.gross_sls_c - pd1.rtn_c) / CAST(IF(SUM(CASE
           WHEN LOWER(pd1.price_type) = LOWER('Total')
           THEN mcpacf1.op_net_sales_cost_amt
           ELSE NULL
           END) = 0, NULL, SUM(CASE
          WHEN LOWER(pd1.price_type) = LOWER('Total')
          THEN mcpacf1.op_net_sales_cost_amt
          ELSE NULL
          END)) AS BIGNUMERIC), 0) AS net_sales_c_ratio,
    SUM(CASE
      WHEN LOWER(pd1.plan_version) = LOWER('Op')
      THEN pd1.gross_sls_u - pd1.rtn_u
      ELSE NULL
      END) AS net_sales_u,
    SUM(CASE
      WHEN LOWER(pd1.price_type) = LOWER('Total')
      THEN mcpacf1.op_net_sales_qty
      ELSE NULL
      END) AS net_sales_u_mfp,
    NULLIF(SUM(pd1.gross_sls_u - pd1.rtn_u) / CAST(IF(SUM(CASE
           WHEN LOWER(pd1.price_type) = LOWER('Total')
           THEN mcpacf1.op_net_sales_qty
           ELSE NULL
           END) = 0, NULL, SUM(CASE
          WHEN LOWER(pd1.price_type) = LOWER('Total')
          THEN mcpacf1.op_net_sales_qty
          ELSE NULL
          END)) AS BIGNUMERIC), 0) AS net_sales_u_ratio
   FROM `{{params.gcp_project_id}}`.t2dl_das_price_type_plan_outputs.price_type_plan_data AS pd1
    LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.mfp_cost_plan_actual_channel_fact AS mcpacf1 ON pd1.department_num = mcpacf1.dept_num AND
        pd1.week_num = mcpacf1.week_num AND pd1.channel_num = mcpacf1.channel_num AND mcpacf1.fulfill_type_num = CASE
       WHEN LOWER(pd1.ownership) = LOWER('Own')
       THEN 3
       WHEN LOWER(pd1.ownership) = LOWER('DropShip')
       THEN 1
       WHEN LOWER(pd1.ownership) = LOWER('RevShare')
       THEN 2
       ELSE NULL
       END
   WHERE pd1.year_num >= (SELECT DISTINCT fiscal_year_num - 1
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
      WHERE day_date = CURRENT_DATE('PST8PDT'))
    AND pd1.channel_num IN (110, 120, 210, 250)
    AND mcpacf1.channel_num IN (110, 120, 210, 250)
    AND LOWER(pd1.plan_version) = LOWER('Op')
   GROUP BY pd1.department_num,
    pd1.week_num,
    pd1.channel_num,
    pd1.price_type,
    pd1.plan_version,
    pd1.ownership,
    mcpacf1.fulfill_type_num) AS j;


--COLLECT STATS PRIMARY INDEX(DEPARTMENT_NUM, CHANNEL_NUM, WEEK_NUM, price_type, plan_version, fulfill_type_num) ON base


-- Department lookup table


CREATE TEMPORARY TABLE IF NOT EXISTS dept_lookup
AS
SELECT DISTINCT dept_num,
 dept_name,
 division_num,
 division_name,
 subdivision_num,
 subdivision_name
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim;


--COLLECT STATS      PRIMARY INDEX(DEPT_NUM, division_num, subdivision_num)      ON dept_lookup


-- Date lookup


CREATE TEMPORARY TABLE IF NOT EXISTS date_lkp
AS
SELECT week_idnt,
 fiscal_year_num AS year_idnt,
 fiscal_halfyear_num AS half_idnt,
 quarter_idnt,
 month_idnt,
   TRIM(FORMAT('%11d', fiscal_year_num)) || ' H' || SUBSTR(RPAD(CAST(fiscal_halfyear_num AS STRING), 5, ' '), -1) AS
 half_label,
 quarter_label,
     TRIM(FORMAT('%11d', fiscal_year_num)) || ' ' || TRIM(FORMAT('%11d', fiscal_month_num)) || ' ' || TRIM(month_abrv)
 AS month_label,
     TRIM(FORMAT('%11d', fiscal_year_num)) || ', ' || TRIM(FORMAT('%11d', fiscal_month_num)) || ', Wk ' || TRIM(FORMAT('%11d', week_num_of_fiscal_month)) AS week_label,
 week_end_day_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS ty
WHERE fiscal_year_num BETWEEN (SELECT MAX(fiscal_year_num - 1)
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE week_end_day_date < CURRENT_DATE('PST8PDT')) AND (SELECT MAX(fiscal_year_num + 1)
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE week_end_day_date < CURRENT_DATE('PST8PDT'));


-- Pull in TY, LY actual costs from MERCH_TRANSACTION_SBCLASS_STORE_WEEK_AGG_FACT_VW and union for each price type and plan version combo


CREATE TEMPORARY TABLE IF NOT EXISTS ty_ly
AS
SELECT dept_num AS department_num,
 week_num,
 channel_num,
 'Promotional' AS price_type,
 'Ty' AS plan_version,
 SUM(jwn_operational_gmv_promo_retail_amt_ty) AS net_sales_r,
 SUM(jwn_operational_gmv_promo_cost_amt_ty) AS net_sales_c,
 SUM(jwn_operational_gmv_promo_units_ty) AS net_sales_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_transaction_sbclass_store_week_agg_fact_vw AS a
WHERE year_num >= (SELECT DISTINCT fiscal_year_num - 1
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_date = CURRENT_DATE('PST8PDT'))
 AND channel_num IN (110, 120, 210, 250)
GROUP BY department_num,
 week_num,
 channel_num,
 price_type,
 plan_version
UNION ALL
SELECT dept_num AS department_num,
 week_num,
 channel_num,
 'Clearance' AS price_type,
 'Ty' AS plan_version,
 SUM(jwn_operational_gmv_clearance_retail_amt_ty) AS net_sales_r,
 SUM(jwn_operational_gmv_clearance_cost_amt_ty) AS net_sales_c,
 SUM(jwn_operational_gmv_clearance_units_ty) AS net_sales_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_transaction_sbclass_store_week_agg_fact_vw AS a
WHERE year_num >= (SELECT DISTINCT fiscal_year_num - 1
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_date = CURRENT_DATE('PST8PDT'))
 AND channel_num IN (110, 120, 210, 250)
GROUP BY department_num,
 week_num,
 channel_num,
 price_type,
 plan_version
UNION ALL
SELECT dept_num AS department_num,
 week_num,
 channel_num,
 'Total' AS price_type,
 'Ty' AS plan_version,
 SUM(jwn_operational_gmv_total_retail_amt_ty) AS net_sales_r,
 SUM(jwn_operational_gmv_total_cost_amt_ty) AS net_sales_c,
 SUM(jwn_operational_gmv_total_units_ty) AS net_sales_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_transaction_sbclass_store_week_agg_fact_vw AS a
WHERE year_num >= (SELECT DISTINCT fiscal_year_num - 1
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_date = CURRENT_DATE('PST8PDT'))
 AND channel_num IN (110, 120, 210, 250)
GROUP BY department_num,
 week_num,
 channel_num,
 price_type,
 plan_version
UNION ALL
SELECT dept_num AS department_num,
 week_num,
 channel_num,
 'Regular' AS price_type,
 'Ty' AS plan_version,
 SUM(jwn_operational_gmv_regular_retail_amt_ty) AS net_sales_r,
 SUM(jwn_operational_gmv_regular_cost_amt_ty) AS net_sales_c,
 SUM(jwn_operational_gmv_regular_units_ty) AS net_sales_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_transaction_sbclass_store_week_agg_fact_vw AS a
WHERE year_num >= (SELECT DISTINCT fiscal_year_num - 1
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_date = CURRENT_DATE('PST8PDT'))
 AND channel_num IN (110, 120, 210, 250)
GROUP BY department_num,
 week_num,
 channel_num,
 price_type,
 plan_version
UNION ALL
SELECT dept_num AS department_num,
 week_num,
 channel_num,
 'Total' AS price_type,
 'Ly' AS plan_version,
 SUM(jwn_operational_gmv_total_retail_amt_ly) AS net_sales_r,
 SUM(jwn_operational_gmv_total_cost_amt_ly) AS net_sales_c,
 SUM(jwn_operational_gmv_total_units_ly) AS net_sales_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_transaction_sbclass_store_week_agg_fact_vw AS a
WHERE year_num >= (SELECT DISTINCT fiscal_year_num - 1
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_date = CURRENT_DATE('PST8PDT'))
 AND channel_num IN (110, 120, 210, 250)
GROUP BY department_num,
 week_num,
 channel_num,
 price_type,
 plan_version
UNION ALL
SELECT dept_num AS department_num,
 week_num,
 channel_num,
 'Regular' AS price_type,
 'Ly' AS plan_version,
 SUM(jwn_operational_gmv_regular_retail_amt_ly) AS net_sales_r,
 SUM(jwn_operational_gmv_regular_cost_amt_ly) AS net_sales_c,
 SUM(jwn_operational_gmv_regular_units_ly) AS net_sales_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_transaction_sbclass_store_week_agg_fact_vw AS a
WHERE year_num >= (SELECT DISTINCT fiscal_year_num - 1
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_date = CURRENT_DATE('PST8PDT'))
 AND channel_num IN (110, 120, 210, 250)
GROUP BY department_num,
 week_num,
 channel_num,
 price_type,
 plan_version
UNION ALL
SELECT dept_num AS department_num,
 week_num,
 channel_num,
 'Promotional' AS price_type,
 'Ly' AS plan_version,
 SUM(jwn_operational_gmv_promo_retail_amt_ly) AS net_sales_r,
 SUM(jwn_operational_gmv_promo_cost_amt_ly) AS net_sales_c,
 SUM(jwn_operational_gmv_promo_units_ly) AS net_sales_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_transaction_sbclass_store_week_agg_fact_vw AS a
WHERE year_num >= (SELECT DISTINCT fiscal_year_num - 1
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_date = CURRENT_DATE('PST8PDT'))
 AND channel_num IN (110, 120, 210, 250)
GROUP BY department_num,
 week_num,
 channel_num,
 price_type,
 plan_version
UNION ALL
SELECT dept_num AS department_num,
 week_num,
 channel_num,
 'Clearance' AS price_type,
 'Ly' AS plan_version,
 SUM(jwn_operational_gmv_clearance_retail_amt_ly) AS net_sales_r,
 SUM(jwn_operational_gmv_clearance_cost_amt_ly) AS net_sales_c,
 SUM(jwn_operational_gmv_clearance_units_ly) AS net_sales_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_transaction_sbclass_store_week_agg_fact_vw AS a
WHERE year_num >= (SELECT DISTINCT fiscal_year_num - 1
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_date = CURRENT_DATE('PST8PDT'))
 AND channel_num IN (110, 120, 210, 250)
GROUP BY department_num,
 week_num,
 channel_num,
 price_type,
 plan_version;


--COLLECT STATS      PRIMARY INDEX(DEPARTMENT_NUM, CHANNEL_NUM, WEEK_NUM, price_type, plan_version)      ON ty_ly


CREATE TEMPORARY TABLE IF NOT EXISTS ty_ly_mapped
AS
SELECT ty_ly.department_num,
 d.dept_name AS department_desc,
 d.division_num,
 d.division_name AS division_desc,
 d.subdivision_num,
 d.subdivision_name AS subdivision_desc,
 e.year_idnt,
 e.half_idnt,
 e.quarter_idnt,
 e.month_idnt,
 e.week_idnt,
 e.half_label,
 e.quarter_label,
 e.month_label,
 e.week_label,
 e.week_end_day_date,
 ty_ly.channel_num,
 ty_ly.price_type,
 ty_ly.plan_version,
 ty_ly.net_sales_r,
 ty_ly.net_sales_c,
 ty_ly.net_sales_u
FROM ty_ly
 INNER JOIN dept_lookup AS d ON ty_ly.department_num = d.dept_num
 INNER JOIN date_lkp AS e ON ty_ly.week_num = e.week_idnt;


--COLLECT STATS      PRIMARY INDEX(DEPARTMENT_NUM, CHANNEL_NUM, week_idnt, price_type, plan_version)      ON ty_ly_mapped


-- Combine SP, OP, CP and TY, LY plans data


-- CP, OP, SP plans


-- TY, LY plans


CREATE TEMPORARY TABLE IF NOT EXISTS plans_base
AS
SELECT base.department_num,
 dept_lookup.dept_name AS department_desc,
 dept_lookup.division_num,
 dept_lookup.division_name AS division_desc,
 dept_lookup.subdivision_num,
 dept_lookup.subdivision_name AS subdivision_desc,
 ty_ly_mapped.year_idnt,
 ty_ly_mapped.half_idnt,
 ty_ly_mapped.quarter_idnt,
 ty_ly_mapped.month_idnt,
 base.week_num AS week_idnt,
 ty_ly_mapped.half_label,
 ty_ly_mapped.quarter_label,
 ty_ly_mapped.month_label,
 ty_ly_mapped.week_label,
 ty_ly_mapped.week_end_day_date,
 base.channel_num,
 base.price_type,
 base.plan_version,
 base.net_sales_r,
 base.net_sales_c,
 base.net_sales_u
FROM base
 LEFT JOIN ty_ly_mapped ON base.department_num = ty_ly_mapped.department_num AND base.week_num = ty_ly_mapped.week_idnt
  AND base.channel_num = ty_ly_mapped.channel_num
 LEFT JOIN dept_lookup ON base.department_num = dept_lookup.dept_num
UNION ALL
SELECT base0.department_num,
 ty_ly_mapped0.department_desc,
 ty_ly_mapped0.division_num,
 ty_ly_mapped0.division_desc,
 ty_ly_mapped0.subdivision_num,
 ty_ly_mapped0.subdivision_desc,
 ty_ly_mapped0.year_idnt,
 ty_ly_mapped0.half_idnt,
 ty_ly_mapped0.quarter_idnt,
 ty_ly_mapped0.month_idnt,
 ty_ly_mapped0.week_idnt,
 ty_ly_mapped0.half_label,
 ty_ly_mapped0.quarter_label,
 ty_ly_mapped0.month_label,
 ty_ly_mapped0.week_label,
 ty_ly_mapped0.week_end_day_date,
 base0.channel_num,
 ty_ly_mapped0.price_type,
 ty_ly_mapped0.plan_version,
 ty_ly_mapped0.net_sales_r,
 ty_ly_mapped0.net_sales_c,
 ty_ly_mapped0.net_sales_u
FROM ty_ly_mapped AS ty_ly_mapped0
 INNER JOIN base AS base0 ON ty_ly_mapped0.department_num = base0.department_num AND ty_ly_mapped0.week_idnt = base0.week_num
     AND ty_ly_mapped0.channel_num = base0.channel_num;


--COLLECT STATS PRIMARY INDEX(DEPARTMENT_NUM, CHANNEL_NUM, week_idnt, price_type, plan_version) ON plans_base


CREATE TEMPORARY TABLE IF NOT EXISTS plans_final
AS
SELECT plans_base.department_num,
 plans_base.department_desc,
 plans_base.division_num,
 plans_base.division_desc,
 plans_base.subdivision_num,
 plans_base.subdivision_desc,
 e.year_idnt,
 e.half_idnt,
 e.quarter_idnt,
 e.month_idnt,
 plans_base.week_idnt,
 e.half_label,
 e.quarter_label,
 e.month_label,
 e.week_label,
 e.week_end_day_date,
 plans_base.channel_num,
 plans_base.price_type,
 plans_base.plan_version,
 plans_base.net_sales_r,
 plans_base.net_sales_c,
 plans_base.net_sales_u
FROM plans_base
 LEFT JOIN date_lkp AS e ON plans_base.week_idnt = e.week_idnt;


--COLLECT STATS PRIMARY INDEX(DEPARTMENT_NUM, CHANNEL_NUM, week_idnt, price_type, plan_version) ON plans_final


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.price_type_planning;


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.price_type_planning
(SELECT DISTINCT department_num,
  department_desc,
  division_num,
  division_desc,
  subdivision_num,
  subdivision_desc,
  year_idnt,
  half_idnt,
  quarter_idnt,
  month_idnt,
  week_idnt,
  half_label,
  quarter_label,
  month_label,
  week_label,
  week_end_day_date,
  channel_num,
  price_type,
  plan_version,
  CAST(net_sales_r AS NUMERIC) AS net_sales_r,
  CAST(net_sales_c AS NUMERIC) AS net_sales_c,
  CAST(net_sales_u AS NUMERIC) AS net_sales_u
 FROM plans_final);

 END