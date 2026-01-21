UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.isf_dag_control_log
	SET
	metric_value = CAST(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(CURRENT_DATETIME('GMT'))) AS STRING) ,
	metric_tmstp = CAST(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(CURRENT_DATETIME('GMT'))) AS TIMESTAMP),
  metric_tmstp_tz= 'GMT'
WHERE LOWER(isf_dag_nm) = LOWER('{{params.dag_name}}')
	AND LOWER(metric_nm) = LOWER('SPARK_JOB_START_TMSTP')
	AND batch_id = (SELECT batch_id FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}'));
	
	
	
	
