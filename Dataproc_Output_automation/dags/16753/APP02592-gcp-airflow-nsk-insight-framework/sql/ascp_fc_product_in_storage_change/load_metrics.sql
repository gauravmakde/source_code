/***********************************************************************************
-- Delete log for current batch_id
************************************************************************************/



DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.isf_dag_control_log
WHERE LOWER(isf_dag_nm) = LOWER('{{params.dag_name}}') AND batch_id = (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}'));


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
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 tbl_nm,
 metric_nm,
 metric_value,
 metric_tmstp,metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM (SELECT SUBSTR('DAG start', 1, 100) AS step_nm,
    SUBSTR(NULL, 1, 100) AS tbl_nm,
    SUBSTR('ISF_DAG_START_TMSTP', 1, 100) AS metric_nm,
    SUBSTR(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS STRING), 1, 60) AS metric_value
    ,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS timestamp) AS metric_tmstp,
   'GMT' as metric_tmstp_tz
   FROM dummy
   UNION ALL
   SELECT SUBSTR('Spark job', 1, 100) AS step_nm,
    SUBSTR(NULL, 1, 100) AS tbl_nm,
    SUBSTR('SPARK_JOB_START_TMSTP', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS timestamp) AS metric_tmstp,
    `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as metric_tmstp_tz
   FROM dummy
   UNION ALL
   SELECT SUBSTR('Spark job', 1, 100) AS step_nm,
    SUBSTR(NULL, 1, 100) AS tbl_nm,
    SUBSTR('SPARK_JOB_END_TMSTP', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS timestamp) AS metric_tmstp,
    `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as metric_tmstp_tz
   FROM dummy
   UNION ALL
   SELECT SUBSTR('DAG end', 1, 100) AS step_nm,
    SUBSTR(NULL, 1, 100) AS tbl_nm,
    SUBSTR('ISF_DAG_END_TMSTP', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS timestamp) AS metric_tmstp,
    `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as metric_tmstp_tz
   FROM dummy
   UNION ALL
   SELECT SUBSTR('Fact table load', 1, 100) AS step_nm,
    SUBSTR('{{params.dbenv}}_NAP_FCT.FC_PRODUCT_IN_STORAGE_CHANGE_FACT', 1, 100) AS tbl_nm,
    SUBSTR('TERADATA_JOB_START_TMSTP', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS timestamp) AS metric_tmstp,
    `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as metric_tmstp_tz
   FROM dummy
   UNION ALL
   SELECT SUBSTR('Spark job', 1, 100) AS step_nm,
    SUBSTR('{{params.dbenv}}_NAP_STG.FC_PRODUCT_IN_STORAGE_CHANGE_LDG', 1, 100) AS tbl_nm,
    SUBSTR('LOADED_TO_CSV_CNT', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS timestamp) AS metric_tmstp,
    `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as metric_tmstp_tz
   FROM dummy
   UNION ALL
   SELECT SUBSTR('Spark job', 1, 100) AS step_nm,
    SUBSTR(NULL, 1, 100) AS tbl_nm,
    SUBSTR('SPARK_PROCESSED_MESSAGE_CNT', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS timestamp) AS metric_tmstp,
    `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as metric_tmstp_tz
   FROM dummy
   UNION ALL
   SELECT SUBSTR('TPT step', 1, 100) AS step_nm,
    SUBSTR('{{params.dbenv}}_NAP_STG.FC_PRODUCT_IN_STORAGE_CHANGE_LDG', 1, 100) AS tbl_nm,
    SUBSTR('TPT_JOB_START_TMSTP', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS timestamp) AS metric_tmstp,
    `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as metric_tmstp_tz
   FROM dummy
   UNION ALL
   SELECT SUBSTR('TPT step', 1, 100) AS step_nm,
    SUBSTR('{{params.dbenv}}_NAP_STG.FC_PRODUCT_IN_STORAGE_CHANGE_LDG', 1, 100) AS tbl_nm,
    SUBSTR('TPT_JOB_END_TMSTP', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS timestamp) AS metric_tmstp,
    `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as metric_tmstp_tz
   FROM dummy
   UNION ALL
   SELECT SUBSTR('TPT step', 1, 100) AS step_nm,
    SUBSTR('{{params.dbenv}}_NAP_STG.FC_PRODUCT_IN_STORAGE_CHANGE_LDG', 1, 100) AS tbl_nm,
    SUBSTR('LOADED_TO_LDG_CNT', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS timestamp) AS metric_tmstp,
    `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as metric_tmstp_tz
   FROM dummy
   UNION ALL
   SELECT SUBSTR('Fact table load', 1, 100) AS step_nm,
    SUBSTR('{{params.dbenv}}_NAP_FCT.FC_PRODUCT_IN_STORAGE_CHANGE_FACT', 1, 100) AS tbl_nm,
    SUBSTR('TERADATA_JOB_END_TMSTP', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS timestamp) AS metric_tmstp,
    `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as metric_tmstp_tz
   FROM dummy) AS s;


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.processed_data_spark_log
WHERE LOWER(isf_dag_nm) = LOWER('{{params.dag_name}}');