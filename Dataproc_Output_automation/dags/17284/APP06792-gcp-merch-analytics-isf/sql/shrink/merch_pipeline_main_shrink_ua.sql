  /* Pull LY & TY YTD */ --
CREATE TEMPORARY TABLE IF NOT EXISTS dates
 AS
SELECT
  a.day_date,
  a.ty_ly_lly_ind,
  a.week_end_day_date,
  a.week_idnt,
  a.week_454_label,
  a.fiscal_week_num,
  a.month_idnt,
  a.month_454_label,
  a.quarter_label,
  a.half_label,
  a.fiscal_year_num,
  b.week_idnt AS week_idnt_true,
  CASE
    WHEN a.week_end_day_date = a.month_end_day_date THEN 1
    ELSE 0
END
  AS last_week_of_month_ind,
  CASE
    WHEN a.month_end_day_date < CURRENT_DATE('PST8PDT') THEN 1
    ELSE 0
END
  AS completed_month_ind,
  CASE
    WHEN (MAX(a.week_idnt) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) = a.week_idnt THEN 1
    ELSE 0
END
  AS max_week_ind
FROM
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw AS a
LEFT JOIN
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS b
ON
  a.day_date = b.day_date
WHERE
  LOWER(RTRIM(a.ty_ly_lly_ind)) IN (LOWER(RTRIM('TY')),
    LOWER(RTRIM('LY')))
  AND a.day_date < CURRENT_DATE('PST8PDT');
  
  
TRUNCATE TABLE
  `{{params.gcp_project_id}}`.{{params.environment_schema}}.shrink_unit_adj_daily;
  
  
INSERT INTO
  `{{params.gcp_project_id}}`.{{params.environment_schema}}.shrink_unit_adj_daily (
  SELECT
    *
  FROM (
    SELECT
      c.day_date,
      c.week_end_day_date,
      CAST(TRUNC(CAST(a.location_num AS FLOAT64)) AS INTEGER) AS location_num,
      b.channel_num,
      g.pi_count,
      a.general_ledger_reference_number AS gl_ref_no,
      23 AS tran_code,
      e.shrink_category,
      e.reason_description,
      a.department_num,
      a.class_num,
      a.subclass_num,
      UPPER(d.brand_name) AS brand_name,
      UPPER(f.vendor_name) AS supplier,
      a.rms_sku_num,
      SUM(a.total_cost_amount) AS shrink_cost,
      SUM(a.total_retail_amount) AS shrink_retail,
      SUM(a.quantity) AS shrink_units
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_expense_transfer_ledger_fact AS a
    INNER JOIN
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS b
    ON
      a.location_num = b.store_num
      AND LOWER(RTRIM(b.store_country_code)) = LOWER(RTRIM('US'))
    INNER JOIN
      dates AS c
    ON
      a.transaction_date = c.day_date
    LEFT JOIN
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS d
    ON
      LOWER(RTRIM(a.rms_sku_num)) = LOWER(RTRIM(d.rms_sku_num))
      AND LOWER(RTRIM(d.channel_country)) = LOWER(RTRIM('US'))
    INNER JOIN
      `{{params.gcp_project_id}}`.t2dl_das_in_season_management_reporting.shrink_gl_mapping AS e
    ON
      CAST(RTRIM(a.general_ledger_reference_number) AS FLOAT64) = e.reason_code
    LEFT JOIN
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS f
    ON
      LOWER(RTRIM(d.prmy_supp_num)) = LOWER(RTRIM(f.vendor_num))
    LEFT JOIN
      `{{params.gcp_project_id}}`.t2dl_das_in_season_management_reporting.shrink_pi_count_dates AS g
    ON
      a.location_num = g.location
      AND c.day_date = g.day_date
    WHERE
      (a.location_num <> 808
        OR CAST(RTRIM(a.general_ledger_reference_number) AS FLOAT64) <> 376
        OR a.transaction_date < CAST(RTRIM('2024-05-05') AS DATE))
      AND (a.location_num <> 828
        OR CAST(RTRIM(a.general_ledger_reference_number) AS FLOAT64) <> 248
        OR a.transaction_date < CAST(RTRIM('2024-05-05') AS DATE))
      AND a.department_num NOT IN (
      SELECT
        DISTINCT dept_num
      FROM
        `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw
      WHERE
        div_num = 600)
      AND LOWER(b.store_short_name) NOT LIKE LOWER('%CLSD%')
      AND b.channel_num NOT IN (930,
        240)
    GROUP BY
      c.day_date,
      c.week_end_day_date,
      location_num,
      b.channel_num,
      g.pi_count,
      gl_ref_no,
      tran_code,
      e.shrink_category,
      e.reason_description,
      a.department_num,
      a.class_num,
      a.subclass_num,
      brand_name,
      supplier,
      a.rms_sku_num
    UNION ALL
    SELECT
      c0.day_date,
      c0.week_end_day_date,
      CAST(TRUNC(CAST(a0.store_num AS FLOAT64)) AS INTEGER) AS location_num,
      b0.channel_num,
      g0.pi_count,
      a0.general_reference_number AS gl_ref_no,
      22 AS tran_code,
      'Additional Shrink' AS shrink_category,
      e0.reason_description,
      a0.dept_num AS department_num,
      a0.class_num,
      a0.subclass_num,
      UPPER(d0.brand_name) AS brand_name,
      UPPER(f0.vendor_name) AS supplier,
      a0.rms_sku_num,
      SUM(a0.total_cost_amt) AS shrink_cost,
      SUM(a0.total_retail_amt) AS shrink_retail,
      SUM(a0.total_units) AS shrink_units
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_shrink_tc22_sku_store_day_fact AS a0
    INNER JOIN
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS b0
    ON
      a0.store_num = b0.store_num
      AND LOWER(RTRIM(b0.store_country_code)) = LOWER(RTRIM('US'))
    INNER JOIN
      dates AS c0
    ON
      a0.posting_date = c0.day_date
    LEFT JOIN
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS d0
    ON
      LOWER(RTRIM(a0.rms_sku_num)) = LOWER(RTRIM(d0.rms_sku_num))
      AND LOWER(RTRIM(d0.channel_country)) = LOWER(RTRIM('US'))
    LEFT JOIN
      `{{params.gcp_project_id}}`.t2dl_das_in_season_management_reporting.shrink_gl_mapping AS e0
    ON
      CAST(RTRIM(a0.general_reference_number) AS FLOAT64) = e0.reason_code
    LEFT JOIN
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS f0
    ON
      LOWER(RTRIM(d0.prmy_supp_num)) = LOWER(RTRIM(f0.vendor_num))
    LEFT JOIN
      `{{params.gcp_project_id}}`.t2dl_das_in_season_management_reporting.shrink_pi_count_dates AS g0
    ON
      a0.store_num = g0.location
      AND c0 .day_date = g0.day_date
    WHERE
      (b0.channel_num IN (120,
          220,
          250,
          260,
          310,
          920,
          930)
        OR a0.store_num IN (209,
          297,
          697))
      AND a0.dept_num NOT IN (
      SELECT
        DISTINCT dept_num
      FROM
        `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw
      WHERE
        div_num = 600)
      AND LOWER(b0.store_short_name) NOT LIKE LOWER('%CLSD%')
    GROUP BY
      c0.day_date,
      c0.week_end_day_date,
      location_num,
      b0.channel_num,
      g0.pi_count,
      gl_ref_no,
      tran_code,
      shrink_category,
      e0.reason_description,
      department_num,
      a0.class_num,
      a0.subclass_num,
      brand_name,
      supplier,
      a0.rms_sku_num) AS t13);