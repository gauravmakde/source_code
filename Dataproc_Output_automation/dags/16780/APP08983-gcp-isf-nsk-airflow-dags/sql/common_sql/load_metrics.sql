/*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File              : load_metrics.sql
-- Description       : initial load of metrics table ISF_DAG_CONTROL_LOG for ISF DAGs with 1 landing table
--*************************************************************************************************************************************
-- Change Log:
-- Date          Author          Description
--*************************************************************************************************************************************
-- 2024-08-11    Oleksandr Chaichenko FA-13559: fix deadlocks on ISF_DAG_CONTROL_LOG
--************************************************************************************************************************************/

/***********************************************************************************
-- Delete log for current batch_id
************************************************************************************/




DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.isf_dag_control_log
WHERE isf_dag_nm = '{{params.dag_name}}'
  AND batch_id = (SELECT BATCH_ID FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE SUBJECT_AREA_NM = '{{params.subject_area}}');


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.isf_dag_control_log---change view from table
  select
      '{{params.dag_name}}' as ISF_DAG_NM,
      STEP_NM,
      (SELECT BATCH_ID FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE SUBJECT_AREA_NM = '{{params.subject_area}}') as BATCH_ID,
      TBL_NM,
      METRIC_NM,
      METRIC_VALUE,
      METRIC_TMSTP,
			`{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(metric_value AS STRING)) AS metric_tmstp_tz,
    CURRENT_DATE("GMT")
		
FROM
(
WITH dummy AS (SELECT 1 AS num)
SELECT
	CAST('DAG start' AS STRING) AS step_nm,
	CAST(NULL AS STRING) AS tbl_nm,
	CAST('ISF_DAG_START_TMSTP' AS STRING) AS metric_nm,
	CAST(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', timestamp(current_datetime('PST8PDT')), 'GMT') AS STRING) as 
metric_value,
CAST(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', timestamp(current_datetime('PST8PDT')), 'GMT') as timestamp)  as metric_tmstp

FROM dummy
UNION ALL
SELECT
	CAST('Spark job' AS STRING) AS step_nm,
	CAST(NULL AS STRING) AS tbl_nm,
	CAST('SPARK_JOB_START_TMSTP' AS STRING) AS metric_nm,
	CAST(NULL AS STRING) AS metric_value,
	CAST(NULL AS TIMESTAMP) AS metric_tmstp
FROM dummy
UNION ALL
SELECT
	CAST('Spark job' AS STRING) AS step_nm,
	CAST(NULL AS STRING) AS tbl_nm,
	CAST('SPARK_JOB_END_TMSTP' AS STRING) AS metric_nm,
	CAST(NULL AS STRING) AS metric_value,
	CAST(NULL AS TIMESTAMP) AS metric_tmstp
FROM dummy
UNION ALL
SELECT
	CAST('DAG end' AS STRING) AS step_nm,
	CAST(NULL AS STRING) AS tbl_nm,
	CAST('ISF_DAG_END_TMSTP' AS STRING) AS metric_nm,
	CAST(NULL AS STRING) AS metric_value,
	CAST(NULL AS TIMESTAMP) AS metric_tmstp
FROM dummy
UNION ALL
SELECT
	CAST('Fact table load' AS STRING) AS step_nm,
	CAST('{{params.dbenv}}_{{params.fct_table_name}}' AS STRING) AS tbl_nm,
	CAST('TERADATA_JOB_START_TMSTP' AS STRING) AS metric_nm,
	CAST(NULL AS STRING) AS metric_value,
	CAST(NULL AS TIMESTAMP) AS metric_tmstp
FROM dummy
UNION ALL
SELECT
	CAST('Spark job' AS STRING) AS step_nm,
	CAST('{{params.dbenv}}_{{params.fct_table_name}}' AS STRING) AS tbl_nm,
	CAST('LOADED_TO_CSV_CNT' AS STRING) AS metric_nm,
	CAST(NULL AS STRING) AS metric_value,
	CAST(NULL AS TIMESTAMP) AS metric_tmstp
FROM dummy
UNION ALL
SELECT
	CAST('Spark job' AS STRING) AS step_nm,
	CAST(NULL AS STRING) AS tbl_nm,
	CAST('SPARK_PROCESSED_MESSAGE_CNT' AS STRING) AS metric_nm,
	CAST(NULL AS STRING) AS metric_value,
	CAST(NULL AS TIMESTAMP) AS metric_tmstp
FROM dummy
UNION ALL
SELECT
	CAST('TPT step' AS STRING) AS step_nm,
	CAST('{{params.dbenv}}_{{params.fct_table_name}}' AS STRING) AS tbl_nm,
	CAST('TPT_JOB_START_TMSTP' AS STRING) AS metric_nm,
	CAST(NULL AS STRING) AS metric_value,
	CAST(NULL AS TIMESTAMP) AS metric_tmstp
FROM dummy
UNION ALL
SELECT
	CAST('TPT step' AS STRING) AS step_nm,
	CAST('{{params.dbenv}}_{{params.fct_table_name}}' AS STRING) AS tbl_nm,
	CAST('TPT_JOB_END_TMSTP' AS STRING) AS metric_nm,
	CAST(NULL AS STRING) AS metric_value,
	CAST(NULL AS TIMESTAMP) AS metric_tmstp
FROM dummy
UNION ALL
SELECT
	CAST('TPT step' AS STRING) AS step_nm,
	CAST('{{params.dbenv}}_{{params.fct_table_name}}' AS STRING) AS tbl_nm,
	CAST('LOADED_TO_LDG_CNT' AS STRING) AS metric_nm,
	CAST(NULL AS STRING) AS metric_value,
	CAST(NULL AS TIMESTAMP) AS metric_tmstp
FROM dummy
UNION ALL
SELECT
	CAST('Fact table load' AS STRING) AS step_nm,
	CAST('{{params.dbenv}}_{{params.fct_table_name}}' AS STRING) AS tbl_nm,
	CAST('TERADATA_JOB_END_TMSTP' AS STRING) AS metric_nm,
	CAST(NULL AS STRING) AS metric_value,
	CAST(NULL AS TIMESTAMP) AS metric_tmstp
FROM dummy
) s;




DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.processed_data_spark_log
WHERE LOWER(isf_dag_nm) = LOWER('{{params.dag_name}}');