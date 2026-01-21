INSERT INTO
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_ap_plan_actual_suppgrp_banner_wrk (supplier_group,
    dept_num,
    banner_num,
    channel_country,
    month_num,
    fiscal_year_num,
    actual_transfer_in_total_retail_amount,
    actual_transfer_in_total_cost_amount,
    actual_transfer_in_total_units,
    actual_transfer_out_total_retail_amount,
    actual_transfer_out_total_cost_amount,
    actual_transfer_out_total_units,
    actual_transfer_in_racking_retail_amount,
    actual_transfer_in_racking_cost_amount,
    actual_transfer_in_racking_units,
    actual_transfer_out_racking_retail_amount,
    actual_transfer_out_racking_cost_amount,
    actual_transfer_out_racking_units,
    dw_batch_date,
    dw_sys_load_tmstp) (
  SELECT
    COALESCE(g01.supplier_group, '-1') AS supplier_group,
    COALESCE(dd.dept_num, - 1) AS dept_num,
    COALESCE(b01.banner_id, - 1) AS banner_num,
    COALESCE(b01.channel_country, 'NA') AS channel_country,
    cal.month_idnt AS month_num,
    cal.fiscal_year_num,
    SUM(r01.total_transfer_in_retail),
    SUM(r01.total_transfer_in_cost),
    SUM(r01.total_transfer_in_units),
    SUM(r01.total_transfer_out_retail),
    SUM(r01.total_transfer_out_cost),
    SUM(r01.total_transfer_out_units),
    SUM(r01.racking_transfer_in_retail),
    SUM(r01.racking_transfer_in_cost),
    SUM(r01.racking_transfer_in_units),
    SUM(r01.racking_transfer_out_retail),
    SUM(r01.racking_transfer_out_cost),
    SUM(r01.racking_transfer_out_units),
    CURRENT_DATE,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME)
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_transfer_sku_loc_week_agg_fact AS r01
  LEFT JOIN
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS orgstore
  ON
    r01.store_num = orgstore.store_num
  INNER JOIN
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_week_cal_454_vw AS cal
  ON
    r01.week_num = cal.week_idnt
  LEFT JOIN
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim AS psd
  ON
    LOWER(psd.rms_sku_num) = LOWER(TRIM(r01.rms_sku_num))
    AND LOWER(orgstore .store_country_code) = LOWER(psd.channel_country)
  LEFT JOIN
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.department_dim AS dd
  ON
    psd.dept_num = dd.dept_num
  LEFT JOIN
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.org_channel_country_banner_dim_vw AS b01
  ON
    orgstore.channel_num = b01.channel_num
  LEFT JOIN
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.supp_dept_map_dim AS g01
  ON
    LOWER(psd.prmy_supp_num) = LOWER(g01.supplier_num)
    AND dd.dept_num = CAST(g01.dept_num AS FLOAT64)
    AND LOWER(b01.banner_code) = LOWER(g01.banner)
    AND CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS TIMESTAMP) >= dd.eff_begin_tmstp_utc
    AND CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS TIMESTAMP) < dd.eff_end_tmstp_utc
  WHERE
    r01.week_num >= (
    SELECT
      MIN(week_idnt)
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_week_cal_454_vw
    WHERE
      fiscal_year_num = (
      SELECT
        current_fiscal_year_num - 2
      FROM
        `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
      WHERE
        LOWER(interface_code) = LOWER('MERCH_NAP_SPE_DLY')))
    AND r01.week_num <= (
    SELECT
      last_completed_fiscal_week_num
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
    WHERE
      LOWER(interface_code) = LOWER('MERCH_NAP_SPE_DLY'))
  GROUP BY
    supplier_group,
    dept_num,
    banner_num,
    channel_country,
    month_num,
    cal.fiscal_year_num);