DELETE FROM `{{params.gcp_project_id}}`.{{params.db_env}}_nap_fct.mfp_cost_actual_channel_snapshot_fact AS tgt
WHERE snapshot_week_id = (SELECT week_idnt AS snapshot_week_id
        FROM `{{params.gcp_project_id}}`.{{params.db_env}}_nap_base_vws.day_cal_454_dim
        WHERE day_date < CURRENT_DATE('PST8PDT') AND day_num_of_fiscal_week = 7
        QUALIFY (ROW_NUMBER() OVER (ORDER BY day_date DESC)) = 1);

INSERT INTO `{{params.gcp_project_id}}`.{{params.db_env}}_nap_fct.mfp_cost_actual_channel_snapshot_fact (dept_num, week_num, channel_num, fulfill_type_num,
 snapshot_week_id, ty_demand_flash_retail_amt, ty_demand_persistent_retail_amt, ty_demand_total_qty,
 ty_demand_total_retail_amt, ty_gross_margin_retail_amt, ty_gross_sales_retail_amt, ty_gross_sales_qty,
 ty_net_sales_retail_amt, ty_net_sales_qty, ty_net_sales_cost_amt, ty_net_sales_trunk_club_retail_amt,
 ty_net_sales_trunk_club_qty, ty_returns_retail_amt, ty_returns_qty, ty_shrink_cost_amt, ty_shrink_qty, dw_batch_date,
 dw_sys_load_tmstp)
(SELECT fct.dept_num,
  fct.week_num,
  fct.channel_num,
  fct.fulfill_type_num,
  dcd.snapshot_week_id,
  fct.ty_demand_flash_retail_amt,
  fct.ty_demand_persistent_retail_amt,
  fct.ty_demand_total_qty,
  fct.ty_demand_total_retail_amt,
  fct.ty_gross_margin_retail_amt,
  fct.ty_gross_sales_retail_amt,
  fct.ty_gross_sales_qty,
  fct.ty_net_sales_retail_amt,
  fct.ty_net_sales_qty,
  fct.ty_net_sales_cost_amt,
  fct.ty_net_sales_trunk_club_retail_amt,
  fct.ty_net_sales_trunk_club_qty,
  fct.ty_returns_retail_amt,
  fct.ty_returns_qty,
  fct.ty_shrink_cost_amt,
  fct.ty_shrink_qty,
  fct.dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.db_env}}_nap_base_vws.mfp_cost_plan_actual_channel_fact AS fct
  INNER JOIN (SELECT week_idnt AS snapshot_week_id
   FROM `{{params.gcp_project_id}}`.{{params.db_env}}_nap_base_vws.day_cal_454_dim
   WHERE day_date < CURRENT_DATE('PST8PDT')
    AND day_num_of_fiscal_week = 7
   QUALIFY (ROW_NUMBER() OVER (ORDER BY day_date DESC)) = 1) AS dcd ON TRUE
 WHERE fct.week_num <= (SELECT last_completed_fiscal_week_num
    FROM `{{params.gcp_project_id}}`.{{params.db_env}}_nap_vws.merch_tran_last_completed_month_vw
    WHERE LOWER(interface_code) = LOWER('MFP_CHNL_BLEND_WKLY')));

DELETE FROM `{{params.gcp_project_id}}`.{{params.db_env}}_nap_fct.mfp_cost_actual_channel_snapshot_fact AS sf
WHERE snapshot_week_id <= (SELECT retention_week_end_idnt
        FROM (SELECT month_start_day_date, month_end_day_date, month_end_week_idnt, MIN(month_end_week_idnt) OVER (ORDER BY month_idnt ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) AS retention_week_end_idnt
                FROM `{{params.gcp_project_id}}`.{{params.db_env}}_nap_vws.merch_month_cal_454_vw) AS a
        WHERE CURRENT_DATE('PST8PDT') BETWEEN month_start_day_date AND month_end_day_date);
