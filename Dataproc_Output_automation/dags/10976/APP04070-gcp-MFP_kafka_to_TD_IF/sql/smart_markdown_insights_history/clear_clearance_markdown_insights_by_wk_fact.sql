----ET

DELETE
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact
 WHERE SNAPSHOT_DATE = (SELECT DW_BATCH_DT FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
  WHERE LOWER(INTERFACE_CODE) = LOWER('CMD_WKLY')) ;


----ET