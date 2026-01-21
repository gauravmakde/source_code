

BEGIN
DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=mfp_financial_channel_plan_weekly_snapshot_load;
---Task_Name=mfp_financial_channel_plan_t1_load_last_fiscal_week_snapshot;'*/


BEGIN
SET ERROR_CODE  =  0;


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_financial_channel_plan_snapshot_fact AS tgt
WHERE snapshot_week_id = (SELECT week_idnt AS snapshot_week_id
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim
        WHERE day_date < CURRENT_DATE('PST8PDT') AND day_num_of_fiscal_week = 7
        QUALIFY (ROW_NUMBER() OVER (ORDER BY day_date DESC)) = 1);




INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_financial_channel_plan_snapshot_fact (source_publish_time, source_publish_time_tz, approved_time, approved_time_tz, week_id,
 department_number, channel_id, last_updated_time_in_millis, last_updated_time, last_updated_time_tz, financial_plan_version,
 alternate_inventory_model, snapshot_week_id, flash_demand_retail_currency_code, flash_demand_retail_amount,
 persistent_demand_retail_currency_code, persistent_demand_retail_amount, total_demand_dollar_currency_code,
 total_demand_dollar_amount, total_demand_units, gross_margin_retail_currency_code, gross_margin_retail_amount,
 gross_sales_dollar_currency_code, gross_sales_dollar_amount, gross_sales_units, net_sales_dollar_currency_code,
 net_sales_dollar_amount, net_sales_units, net_sales_retail_currency_code, net_sales_retail_amount,
 net_sales_trunk_club_dollar_currency_code, net_sales_trunk_club_dollar_amount, net_sales_trunk_club_units,
 returns_dollar_currency_code, returns_dollar_amount, returns_units, shrink_dollar_currency_code, shrink_dollar_amount,
 shrink_units, dw_sys_load_tmstp, dw_sys_load_tmstp_tz)
(SELECT CAST(fct.source_publish_time AS TIMESTAMP),
  fct.source_publish_time_tz,
  CAST(fct.approved_time AS TIMESTAMP),
  fct.approved_time_tz,
  fct.week_id,
  fct.department_number,
  fct.channel_id,
  fct.last_updated_time_in_millis,
  CAST(fct.last_updated_time AS TIMESTAMP),
  fct.last_updated_time_tz,
  fct.financial_plan_version,
  fct.alternate_inventory_model,
  dcd.snapshot_week_id,
  fct.flash_demand_retail_currency_code,
  fct.flash_demand_retail_amount,
  fct.persistent_demand_retail_currency_code,
  fct.persistent_demand_retail_amount,
  fct.total_demand_dollar_currency_code,
  fct.total_demand_dollar_amount,
  fct.total_demand_units,
  fct.gross_margin_retail_currency_code,
  fct.gross_margin_retail_amount,
  fct.gross_sales_dollar_currency_code,
  fct.gross_sales_dollar_amount,
  fct.gross_sales_units,
  fct.net_sales_dollar_currency_code,
  fct.net_sales_dollar_amount,
  fct.net_sales_units,
  fct.net_sales_retail_currency_code,
  fct.net_sales_retail_amount,
  fct.net_sales_trunk_club_dollar_currency_code,
  fct.net_sales_trunk_club_dollar_amount,
  fct.net_sales_trunk_club_units,
  fct.returns_dollar_currency_code,
  fct.returns_dollar_amount,
  fct.returns_units,
  fct.shrink_dollar_currency_code,
  fct.shrink_dollar_amount,
  fct.shrink_units,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS
  dw_sys_load_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS dw_sys_load_tmstp_tz
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_financial_channel_plan_fct AS fct
  INNER JOIN (SELECT week_idnt AS snapshot_week_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim
   WHERE day_date < CURRENT_DATE('PST8PDT')
    AND day_num_of_fiscal_week = 7
   QUALIFY (ROW_NUMBER() OVER (ORDER BY day_date DESC)) = 1) AS dcd ON TRUE
 WHERE fct.week_id >= (SELECT DISTINCT month_start_week_idnt
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim
    WHERE day_date < CURRENT_DATE('PST8PDT')
     AND day_num_of_fiscal_week = 7
    QUALIFY (DENSE_RANK() OVER (ORDER BY month_start_week_idnt DESC)) = 3));

	

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_financial_channel_plan_snapshot_fact AS sf
WHERE snapshot_week_id <= (SELECT retention_week_end_idnt
        FROM (SELECT month_start_day_date, month_end_day_date, month_end_week_idnt, MIN(month_end_week_idnt) OVER (ORDER BY month_idnt ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) AS retention_week_end_idnt
                FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_month_cal_454_vw) AS a
        WHERE CURRENT_DATE('PST8PDT') BETWEEN month_start_day_date AND month_end_day_date);

		

EXCEPTION WHEN ERROR THEN
ROLLBACK TRANSACTION;
RAISE USING MESSAGE = @@error.message;
END;
END;