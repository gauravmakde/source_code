TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_on_order_insight_fact_snap_wrk;

       INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_on_order_insight_fact_snap_wrk 
    (week_idnt, day_date, dw_sys_load_dt, dw_sys_load_tmstp, dw_sys_load_tmstp_tz)
SELECT 
    week_idnt,
    day_date,
    CURRENT_DATE() AS dw_sys_load_dt,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME("PST8PDT")) AS TIMESTAMP) AS dw_sys_load_tmstp,
    `{{params.gcp_project_id}}`.jWN_UDF.UDF_TIME_ZONE(CAST(
        CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME("PST8PDT")) AS TIMESTAMP)
        AS STRING)) AS dw_sys_load_tmstp_tz
FROM 
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim
WHERE 
    day_date = (
        SELECT 
            DATE_SUB(day_date, INTERVAL day_num_of_fiscal_week DAY) AS batch_date
        FROM 
            `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim
        WHERE 
            day_date = (
                SELECT 
                    MAX(dw_batch_date)
                FROM 
                    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_on_order_insight_fact
            )
    );



DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_on_order_insight_snapshot_fact AS tgt
WHERE snapshot_week_num = (SELECT week_idnt
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_on_order_insight_fact_snap_wrk);


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_on_order_insight_snapshot_fact (snapshot_week_num, snapshot_week_date, week_num, dept_num
 , banner_num, fulfilment_num, rp_oo_active_cost, non_rp_oo_active_cost, rp_oo_active_units, non_rp_oo_active_units,
 oo_inactive_cost, oo_inactive_units, rp_adjusted_oo_active_cost, non_rp_adjusted_oo_active_cost,
 rp_adjusted_oo_active_units, non_rp_adjusted_oo_active_units, adjusted_oo_inactive_cost, adjusted_oo_inactive_units,
 dw_sys_load_dt, dw_sys_load_tmstp, dw_sys_load_tmstp_tz)
(SELECT (SELECT week_idnt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_on_order_insight_fact_snap_wrk) AS snapshot_week_num,
   (SELECT day_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_on_order_insight_fact_snap_wrk) AS snapshot_week_date,
  week_num,
  dept_num,
  banner_num,
  fulfilment_num,
  rp_oo_active_cost,
  non_rp_oo_active_cost,
  rp_oo_active_units,
  non_rp_oo_active_units,
  oo_inactive_cost,
  oo_inactive_units,
  rp_adjusted_oo_active_cost,
  non_rp_adjusted_oo_active_cost,
  rp_adjusted_oo_active_units,
  non_rp_adjusted_oo_active_units,
  adjusted_oo_inactive_cost,
  adjusted_oo_inactive_units,
  CURRENT_DATE('PST8PDT') AS dw_sys_load_dt,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP) AS dw_sys_load_tmstp,
  `{{params.gcp_project_id}}`.jWN_UDF.UDF_TIME_ZONE(CAST(
        CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME("PST8PDT")) AS TIMESTAMP)
        AS STRING)) AS dw_sys_load_tmstp_tz
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_on_order_insight_fact);
