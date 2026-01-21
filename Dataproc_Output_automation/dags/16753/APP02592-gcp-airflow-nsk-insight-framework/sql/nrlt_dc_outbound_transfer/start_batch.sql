/***********************************************************************************
-- Delete log for current batch_id
************************************************************************************/



DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.isf_dag_control_log
WHERE LOWER(isf_dag_nm) = LOWER('{{params.dag_name}}') AND batch_id = (SELECT batch_id + 1
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}'));


/***********************************************************************************
-- Insert new batch into control metrics table
************************************************************************************/


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.isf_dag_control_log
(
isf_dag_nm,
step_nm,
batch_id,
tbl_nm,
metric_nm,
metric_value,
metric_tmstp,
metric_tmstp_tz,
metric_date
)
WITH dummy AS (SELECT 1 AS num)
SELECT '{{params.dag_name}}' AS isf_dag_nm,
 step_nm,
  (SELECT batch_id + 1
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 tbl_nm,
 metric_nm,
 metric_value,
 metric_tmstp,
 metric_tmstp_tz,
 current_date('PST8PDT') AS metric_date
FROM (SELECT SUBSTR('DAG start', 1, 100) AS step_nm,
    SUBSTR(NULL, 1, 100) AS tbl_nm,
    SUBSTR('ISF_DAG_START_TMSTP', 1, 100) AS metric_nm,
    SUBSTR(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS STRING), 1, 60) AS metric_value
    ,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS timestamp) AS metric_tmstp,
    `{{params.gcp_project_id}}`.jwn_udf.default_tz_pst() as metric_tmstp_tz,
   FROM dummy AS d
   UNION ALL
   SELECT SUBSTR('Spark job', 1, 100) AS step_nm,
    SUBSTR(NULL, 1, 100) AS tbl_nm,
    SUBSTR('SPARK_JOB_START_TMSTP', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS timestamp) AS metric_tmstp,
    cast(NULL AS string) as metric_tmstp_tz 
   FROM dummy AS d
   UNION ALL
   SELECT SUBSTR('Spark job', 1, 100) AS step_nm,
    SUBSTR(NULL, 1, 100) AS tbl_nm,
    SUBSTR('SPARK_JOB_END_TMSTP', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS timestamp) AS metric_tmstp,
    cast(NULL AS string) as metric_tmstp_tz 
   FROM dummy AS d
   UNION ALL
   SELECT SUBSTR('DAG end', 1, 100) AS step_nm,
    SUBSTR(NULL, 1, 100) AS tbl_nm,
    SUBSTR('ISF_DAG_END_TMSTP', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS timestamp) AS metric_tmstp,
    cast(NULL AS string) as metric_tmstp_tz 
   FROM dummy AS d
   UNION ALL
   SELECT SUBSTR('Spark job', 1, 100) AS step_nm,
    SUBSTR(NULL, 1, 100) AS tbl_nm,
    SUBSTR('SPARK_PROCESSED_MESSAGE_CNT', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS timestamp) AS metric_tmstp,
    cast(NULL AS string) as metric_tmstp_tz 
   FROM dummy AS d
   UNION ALL
   SELECT SUBSTR('TPT step', 1, 100) AS step_nm,
    SUBSTR('{{params.dbenv}}_NAP_STG.INVENTORY_DISTRIBUTION_CENTER_OUTBOUND_CARTON_EVENTS_LDG', 1, 100) AS tbl_nm,
    SUBSTR('LOADED_TO_LDG_CNT', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS timestamp) AS metric_tmstp,
    cast(NULL AS string) as metric_tmstp_tz 
   FROM dummy AS d
   UNION ALL
   SELECT SUBSTR('Fact table load', 1, 100) AS step_nm,
    SUBSTR('{{params.dbenv}}_NAP_FCT.INVENTORY_DISTRIBUTION_CENTER_OUTBOUND_CARTON_EVENTS_FACT', 1, 100) AS tbl_nm,
    SUBSTR('TERADATA_JOB_END_TMSTP', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS timestamp) AS metric_tmstp,
    cast(NULL AS string) as metric_tmstp_tz 
   FROM dummy AS d
   UNION ALL
   SELECT SUBSTR('Fact table load', 1, 100) AS step_nm,
    SUBSTR('{{params.dbenv}}_NAP_FCT.INVENTORY_DISTRIBUTION_CENTER_OUTBOUND_CARTON_EVENTS_FACT', 1, 100) AS tbl_nm,
    SUBSTR('TERADATA_JOB_START_TMSTP', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS timestamp) AS metric_tmstp,
    cast(NULL AS string) as metric_tmstp_tz 
   FROM dummy AS d
   UNION ALL
   SELECT SUBSTR('TPT step', 1, 100) AS step_nm,
    SUBSTR('{{params.dbenv}}_NAP_STG.WM_OUTBOUND_CARTON_PACKED_ITEMS_LDG', 1, 100) AS tbl_nm,
    SUBSTR('LOADED_TO_LDG_CNT', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS timestamp) AS metric_tmstp,
    cast(NULL AS string) as metric_tmstp_tz 
   FROM dummy AS d
   UNION ALL
   SELECT SUBSTR('Fact table load', 1, 100) AS step_nm,
    SUBSTR('{{params.dbenv}}_NAP_FCT.WM_OUTBOUND_CARTON_PACKED_ITEMS_FACT', 1, 100) AS tbl_nm,
    SUBSTR('TERADATA_JOB_END_TMSTP', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS timestamp) AS metric_tmstp,
    cast(NULL AS string) as metric_tmstp_tz 
   FROM dummy AS d
   UNION ALL
   SELECT SUBSTR('Fact table load', 1, 100) AS step_nm,
    SUBSTR('{{params.dbenv}}_NAP_FCT.WM_OUTBOUND_CARTON_PACKED_ITEMS_FACT', 1, 100) AS tbl_nm,
    SUBSTR('TERADATA_JOB_START_TMSTP', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS timestamp) AS metric_tmstp,
    cast(NULL AS string) as metric_tmstp_tz 
   FROM dummy AS d) AS s;


/***********************************************************************************
-- Delete previous spark log row 
************************************************************************************/


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.processed_data_spark_log
WHERE LOWER(isf_dag_nm) = LOWER('{{params.dag_name}}');


call `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.elt_nrtl_control_start_load('{{params.subject_area}}')