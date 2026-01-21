DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_financial_channel_plan_qtr_plan_fct AS tgt
WHERE week_num >= (SELECT first_week_idnt
  FROM (SELECT quarter_idnt,
     MIN(day_date) AS first_quarter_sat,
     MIN(week_idnt) AS first_week_idnt
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.day_cal_454_dim AS c
    WHERE day_num_of_fiscal_week = 7
    GROUP BY quarter_idnt
    HAVING first_quarter_sat = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) AS t1);


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_financial_channel_plan_qtr_plan_fct
(
quarter_idnt,
dept_num,
week_num,
channel_num,
fulfill_type_num,
qp_demand_flash_retail_amt,
qp_demand_persistent_retail_amt,
qp_demand_total_qty,
qp_demand_total_retail_amt,
qp_gross_margin_retail_amt,
qp_gross_sales_retail_amt,
qp_gross_sales_qty,
qp_net_sales_retail_amt,
qp_net_sales_qty,
qp_net_sales_cost_amt,
qp_net_sales_trunk_club_retail_amt,
qp_net_sales_trunk_club_qty,
qp_returns_retail_amt,
qp_returns_qty,
qp_shrink_cost_amt,
qp_shrink_qty,
dw_sys_load_tmstp
)
WITH cal AS (SELECT quarter_idnt,
 MIN(day_date) AS first_quarter_sat,
 MIN(week_idnt) AS first_week_idnt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.day_cal_454_dim AS c
WHERE day_num_of_fiscal_week = 7
GROUP BY quarter_idnt
HAVING first_quarter_sat = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY))
SELECT c.quarter_idnt,
 mfcpf.department_number AS dept_num,
 mfcpf.week_id AS week_num,
 CAST(trunc(cast(CASE
   WHEN mfcpf.channel_id = ''
   THEN '0'
   ELSE mfcpf.channel_id
   END as float64)) AS INTEGER) AS channel_num,
 aimd.fulfill_type_num,
 mfcpf.flash_demand_retail_amount AS qp_demand_flash_retail_amt,
 mfcpf.persistent_demand_retail_amount AS qp_demand_persistent_retail_amt,
 mfcpf.total_demand_units AS qp_demand_total_qty,
 mfcpf.total_demand_dollar_amount AS qp_demand_total_retail_amt,
 mfcpf.gross_margin_retail_amount AS qp_gross_margin_retail_amt,
 mfcpf.gross_sales_dollar_amount AS qp_gross_sales_retail_amt,
 mfcpf.gross_sales_units AS qp_gross_sales_qty,
 mfcpf.net_sales_retail_amount AS qp_net_sales_retail_amt,
 mfcpf.net_sales_units AS qp_net_sales_qty,
 mfcpf.net_sales_dollar_amount AS qp_net_sales_cost_amt,
 mfcpf.net_sales_trunk_club_dollar_amount AS qp_net_sales_trunk_club_retail_amt,
 mfcpf.net_sales_trunk_club_units AS qp_net_sales_trunk_club_qty,
 mfcpf.returns_dollar_amount AS qp_returns_retail_amt,
 mfcpf.returns_units AS qp_returns_qty,
 mfcpf.shrink_dollar_amount AS qp_shrink_cost_amt,
 mfcpf.shrink_units AS qp_shrink_qty,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_financial_channel_plan_fct AS mfcpf
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.alternate_inventory_model_dim AS aimd ON LOWER(mfcpf.alternate_inventory_model) = LOWER(aimd
   .fulfill_type_desc)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.day_cal_454_dim AS c ON mfcpf.week_id = c.week_idnt
 INNER JOIN cal ON mfcpf.week_id >= cal.first_week_idnt
WHERE c.day_num_of_fiscal_week = 1
 AND LOWER(mfcpf.financial_plan_version) = LOWER('SNAP_PLAN');