/*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File              : load_metrics2.sql
-- Description       : initial load of metrics table ISF_DAG_CONTROL_LOG for ISF DAGs with 2 landing tables
--*************************************************************************************************************************************
-- Change Log:
-- Date          Author         Description
--*************************************************************************************************************************************
-- 2024-09-23    Andrew Ivchuk  FA-12969: Allocation References ISF Migration
--************************************************************************************************************************************/

/***********************************************************************************
-- Delete log for current batch_id
************************************************************************************/




DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.isf_dag_control_log
WHERE LOWER(isf_dag_nm) = LOWER('{{params.dag_name}}') AND batch_id = (SELECT batch_id
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
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 tbl_nm,
 metric_nm,
 metric_value,
 cast(metric_tmstp as timestamp),
 metric_tmstp_tz,
CURRENT_DATE('GMT') AS metric_date
FROM (SELECT SUBSTR('DAG start', 1, 100) AS step_nm,
    SUBSTR(NULL, 1, 100) AS tbl_nm,
    SUBSTR('ISF_DAG_START_TMSTP', 1, 100) AS metric_nm,
    SUBSTR(CAST(current_datetime('GMT') AS STRING), 1, 60) AS metric_value
    ,
   current_datetime('GMT') AS metric_tmstp,
    'GMT' as metric_tmstp_tz
   FROM dummy
   UNION ALL
   SELECT SUBSTR('Spark job', 1, 100) AS step_nm,
    SUBSTR(NULL, 1, 100) AS tbl_nm,
    SUBSTR('SPARK_JOB_START_TMSTP', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS DATETIME) AS metric_tmstp,
    NULL as metric_tmstp_tz
   FROM dummy
   UNION ALL
   SELECT SUBSTR('Spark job', 1, 100) AS step_nm,
    SUBSTR(NULL, 1, 100) AS tbl_nm,
    SUBSTR('SPARK_JOB_END_TMSTP', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS DATETIME) AS metric_tmstp,
    NULL as metric_tmstp_tz
   FROM dummy
   UNION ALL
   SELECT SUBSTR('DAG end', 1, 100) AS step_nm,
    SUBSTR(NULL, 1, 100) AS tbl_nm,
    SUBSTR('ISF_DAG_END_TMSTP', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS DATETIME) AS metric_tmstp,
    NULL as metric_tmstp_tz
   FROM dummy
   UNION ALL
   SELECT SUBSTR('Fact table load', 1, 100) AS step_nm,
    SUBSTR('{{params.dbenv}}_{{params.fct_table_name}}', 1, 100) AS tbl_nm,
    SUBSTR('TERADATA_JOB_START_TMSTP', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS DATETIME) AS metric_tmstp,
    NULL as metric_tmstp_tz
   FROM dummy
   UNION ALL
   SELECT SUBSTR('Spark job', 1, 100) AS step_nm,
    SUBSTR('{{params.dbenv}}_{{params.ldg_table_name}}', 1, 100) AS tbl_nm,
    SUBSTR('LOADED_TO_CSV_CNT', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS DATETIME) AS metric_tmstp,
    NULL as metric_tmstp_tz
   FROM dummy
   UNION ALL
   SELECT SUBSTR('Spark job', 1, 100) AS step_nm,
    SUBSTR('{{params.dbenv}}_{{params.ldg_table_name2}}', 1, 100) AS tbl_nm,
    SUBSTR('LOADED_TO_CSV_CNT', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS DATETIME) AS metric_tmstp,
    NULL as metric_tmstp_tz
   FROM dummy
   UNION ALL
   SELECT SUBSTR('Spark job', 1, 100) AS step_nm,
    SUBSTR(NULL, 1, 100) AS tbl_nm,
    SUBSTR('SPARK_PROCESSED_MESSAGE_CNT', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS DATETIME) AS metric_tmstp,
    NULL as metric_tmstp_tz
   FROM dummy
   UNION ALL
   SELECT SUBSTR('TPT step', 1, 100) AS step_nm,
    SUBSTR('{{params.dbenv}}_{{params.ldg_table_name}}', 1, 100) AS tbl_nm,
    SUBSTR('TPT_JOB_START_TMSTP', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS DATETIME) AS metric_tmstp,
    NULL as metric_tmstp_tz
   FROM dummy
   UNION ALL
   SELECT SUBSTR('TPT step', 1, 100) AS step_nm,
    SUBSTR('{{params.dbenv}}_{{params.ldg_table_name}}', 1, 100) AS tbl_nm,
    SUBSTR('TPT_JOB_END_TMSTP', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS DATETIME) AS metric_tmstp,
    NULL as metric_tmstp_tz
   FROM dummy
   UNION ALL
   SELECT SUBSTR('TPT step', 1, 100) AS step_nm,
    SUBSTR('{{params.dbenv}}_{{params.ldg_table_name}}', 1, 100) AS tbl_nm,
    SUBSTR('LOADED_TO_LDG_CNT', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS DATETIME) AS metric_tmstp,
    NULL as metric_tmstp_tz
   FROM dummy
   UNION ALL
   SELECT SUBSTR('TPT step', 1, 100) AS step_nm,
    SUBSTR('{{params.dbenv}}_{{params.ldg_table_name2}}', 1, 100) AS tbl_nm,
    SUBSTR('TPT_JOB_START_TMSTP', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS DATETIME) AS metric_tmstp,
    NULL as metric_tmstp_tz
   FROM dummy
   UNION ALL
   SELECT SUBSTR('TPT step', 1, 100) AS step_nm,
    SUBSTR('{{params.dbenv}}_{{params.ldg_table_name2}}', 1, 100) AS tbl_nm,
    SUBSTR('TPT_JOB_END_TMSTP', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS DATETIME) AS metric_tmstp,
    NULL as metric_tmstp_tz
   FROM dummy
   UNION ALL
   SELECT SUBSTR('TPT step', 1, 100) AS step_nm,
    SUBSTR('{{params.dbenv}}_{{params.ldg_table_name2}}', 1, 100) AS tbl_nm,
    SUBSTR('LOADED_TO_LDG_CNT', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS DATETIME) AS metric_tmstp,
    NULL as metric_tmstp_tz
   FROM dummy
   UNION ALL
   SELECT SUBSTR('Fact table load', 1, 100) AS step_nm,
    SUBSTR('{{params.dbenv}}_{{params.fct_table_name}}', 1, 100) AS tbl_nm,
    SUBSTR('TERADATA_JOB_END_TMSTP', 1, 100) AS metric_nm,
    SUBSTR(NULL, 1, 60) AS metric_value,
    CAST(NULL AS DATETIME) AS metric_tmstp,
    NULL as metric_tmstp_tz
   FROM dummy) AS s;

/***********************************************************************************
-- Delete previous spark log row
************************************************************************************/
DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.processed_data_spark_log
WHERE LOWER(isf_dag_nm) = LOWER('{{params.dag_name}}');