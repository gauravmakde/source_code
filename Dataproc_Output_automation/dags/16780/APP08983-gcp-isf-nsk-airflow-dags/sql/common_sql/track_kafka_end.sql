UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.isf_dag_control_log SET
 metric_value = CAST(DATETIME_SUB(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S',  CURRENT_DATETIME('GMT')) AS DATETIME), INTERVAL 2 MINUTE) AS STRING),
 metric_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S',CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP),
 metric_tmstp_tz = 'GMT'
WHERE LOWER(isf_dag_nm) = LOWER('{{params.dag_name}}') 
AND LOWER(metric_nm) = LOWER('SPARK_JOB_END_TMSTP')
 AND batch_id = (SELECT
    batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}'));