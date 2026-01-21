/***********************************************************************************
-- Update control metrics table with spark job end time
************************************************************************************/
-- -2 mins because approx time of putting task to queue ~2m



UPDATE {{params.gcp_project_id}}.{{params.dbenv}}_nap_utl.isf_dag_control_log SET
 metric_value = CAST(DATETIME_SUB( CURRENT_DATETIME('GMT'), INTERVAL 2 MINUTE) AS STRING),
 metric_tmstp = CAST(CURRENT_DATETIME('GMT') AS TIMESTAMP),
 metric_tmstp_tz = 'GMT'
WHERE LOWER(RTRIM(isf_dag_nm)) = LOWER(RTRIM('{{params.dag_name}}')) 
AND LOWER(RTRIM(metric_nm)) = LOWER(RTRIM('SPARK_JOB_END_TMSTP'))
AND batch_id = (SELECT batch_id
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(RTRIM(subject_area_nm)) = LOWER(RTRIM('{{params.subject_area}}')));
