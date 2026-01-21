

BEGIN
DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=apt_assortment_category_cluster_weekly_snapshot_load;
---Task_Name=load_weekly_snapshot;'*/

BEGIN TRANSACTION;

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_assortment_category_cluster_plan_snapshot_fact AS tgt
WHERE snapshot_week_id = (SELECT week_idnt AS snapshot_week_id
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim
        WHERE day_date < CURRENT_DATE('PST8PDT') AND day_num_of_fiscal_week = 7
        QUALIFY (ROW_NUMBER() OVER (ORDER BY day_date DESC)) = 1);

		

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_assortment_category_cluster_plan_snapshot_fact (snapshot_week_id, event_time,event_time_tz,
 plan_published_date, cluster_name, category, price_band, dept_num, last_updated_time_in_millis, last_updated_time, last_updated_time_tz,
 month_num, month_label, alternate_inventory_model, demand_dollar_currcycd, demand_dollar_amt, demand_units,
 gross_sales_dollar_currcycd, gross_sales_dollar_amt, gross_sales_units, returns_dollar_currcycd, returns_dollar_amt,
 returns_units, net_sales_units, net_sales_retail_currcycd, net_sales_retail_amt, net_sales_cost_currcycd,
 net_sales_cost_amt, gross_margin_retail_currcycd, gross_margin_retail_amt, demand_next_two_month_run_rate,
 sales_next_two_month_run_rate, dw_sys_load_tmstp)
(SELECT dcd.snapshot_week_id,
  CAST(fct.event_time AS TIMESTAMP),
  fct.event_time_tz,
  fct.plan_published_date,
  fct.cluster_name,
  fct.category,
  fct.price_band,
  fct.dept_num,
  fct.last_updated_time_in_millis,
  CAST(fct.last_updated_time AS TIMESTAMP),
  fct.last_updated_time_tz,
  fct.month_num,
  fct.month_label,
  fct.alternate_inventory_model,
  fct.demand_dollar_currcycd,
  fct.demand_dollar_amt,
  fct.demand_units,
  fct.gross_sales_dollar_currcycd,
  fct.gross_sales_dollar_amt,
  fct.gross_sales_units,
  fct.returns_dollar_currcycd,
  fct.returns_dollar_amt,
  fct.returns_units,
  fct.net_sales_units,
  fct.net_sales_retail_currcycd,
  fct.net_sales_retail_amt,
  fct.net_sales_cost_currcycd,
  fct.net_sales_cost_amt,
  fct.gross_margin_retail_currcycd,
  fct.gross_margin_retail_amt,
  fct.demand_next_two_month_run_rate,
  fct.sales_next_two_month_run_rate,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_assortment_category_cluster_plan_fact AS fct
  INNER JOIN (SELECT week_idnt AS snapshot_week_id,
    month_idnt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim
   WHERE day_date < CURRENT_DATE('PST8PDT')
    AND day_num_of_fiscal_week = 7
   QUALIFY (ROW_NUMBER() OVER (ORDER BY day_date DESC)) = 1) AS dcd ON TRUE
 WHERE fct.month_num >= dcd.month_idnt);

 

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_assortment_category_cluster_plan_snapshot_fact AS sf
WHERE snapshot_week_id <= (SELECT retention_week_end_idnt
        FROM (SELECT month_start_day_date, month_end_day_date, month_end_week_idnt, MIN(month_end_week_idnt) OVER (ORDER BY month_idnt ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) AS retention_week_end_idnt
                FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_month_cal_454_vw) AS a
        WHERE CURRENT_DATE('PST8PDT') BETWEEN month_start_day_date AND month_end_day_date);

		
		
COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
ROLLBACK TRANSACTION;
RAISE USING MESSAGE = @@error.message;
END;
