DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
WHERE snapshot_date = (
    SELECT dw_batch_dt
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
    WHERE LOWER(INTERFACE_CODE) = LOWER('SMD_INSIGHTS_WKLY')
);