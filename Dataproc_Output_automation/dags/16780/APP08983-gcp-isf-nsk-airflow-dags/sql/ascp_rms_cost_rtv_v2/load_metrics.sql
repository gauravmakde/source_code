/***********************************************************************************
-- Delete log for current batch_id
************************************************************************************/
DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.isf_dag_control_log
WHERE isf_dag_nm = '{{params.dag_name}}'
      and batch_id = (SELECT batch_id FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(subject_area_nm) =LOWER('{{params.subject_area}}'));

/***********************************************************************************
-- Insert new batch into control metrics table
************************************************************************************/
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.isf_dag_control_log
WITH dummy AS (SELECT 1 AS num)
select
  '{{params.dag_name}}' AS isf_dag_nm,
  CAST('DAG start' AS STRING) AS step_nm,
  (SELECT batch_id FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(subject_area_nm) =LOWER('{{params.subject_area}}')) AS batch_id,
  CAST(NULL AS STRING) AS tbl_nm,
  CAST('ISF_DAG_START_TMSTP' AS STRING) AS metric_nm,
  CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS  TIMESTAMP) AS STRING) AS metric_value,                                                        --AT 'GMT'
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS  TIMESTAMP) AS metric_tmstp,                                                                        --AT 'GMT'
  `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS  TIMESTAMP) AS STRING)) AS metric_tmstp_tz,
  CURRENT_DATE('GMT') AS metric_date                                                                                         --AT 'GMT'
from dummy
union all
select
  '{{params.dag_name}}' AS isf_dag_nm,
  CAST('Spark job' AS STRING) AS step_nm,
  (SELECT batch_id FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(subject_area_nm) =LOWER('{{params.subject_area}}')) AS batch_id,
  CAST(NULL AS STRING) AS tbl_nm,
  CAST('SPARK_JOB_START_TMSTP' AS STRING) AS metric_nm,
  CAST(NULL AS STRING) AS metric_value,
  CAST(NULL AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(CAST(NULL AS TIMESTAMP) AS STRING)) AS metric_tmstp_tz,
  CURRENT_DATE('GMT') AS metric_date                                                                                         --AT 'GMT'
from dummy
union all
select
  '{{params.dag_name}}' AS isf_dag_nm,
  CAST('Spark job' AS STRING) AS step_nm,
  (SELECT batch_id FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(subject_area_nm) =LOWER('{{params.subject_area}}')) AS batch_id,
  CAST(NULL AS STRING) AS tbl_nm,
  CAST('SPARK_JOB_END_TMSTP' AS STRING) AS metric_nm,
  CAST(NULL AS STRING) AS metric_value,
  CAST(NULL AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(CAST(NULL AS TIMESTAMP) AS STRING)) AS metric_tmstp_tz,
  CURRENT_DATE('GMT') AS metric_date                                                                                          --AT 'GMT'
from dummy
union all
select
  '{{params.dag_name}}' AS isf_dag_nm,
  CAST('DAG end' AS STRING) AS step_nm,
  (SELECT batch_id FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(subject_area_nm) =LOWER('{{params.subject_area}}')) AS batch_id,
  CAST(NULL AS STRING) AS tbl_nm,
  CAST('ISF_DAG_END_TMSTP' AS STRING) AS metric_nm,
  CAST(NULL AS STRING) AS metric_value,
  CAST(NULL AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(CAST(NULL AS TIMESTAMP) AS STRING)) AS metric_tmstp_tz,
  CURRENT_DATE('GMT') AS metric_date                                                                                          --AT 'GMT'
from dummy
union all
select
   '{{params.dag_name}}' AS isf_dag_nm,
   CAST('Spark job' AS STRING) AS step_nm,
   (SELECT batch_id FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(subject_area_nm) =LOWER('{{params.subject_area}}')) AS batch_id,
   CAST(NULL AS STRING) AS tbl_nm,
   CAST('SPARK_PROCESSED_MESSAGE_CNT' AS STRING) AS metric_nm,
   CAST(NULL AS STRING) AS metric_value,
   CAST(NULL AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(CAST(NULL AS TIMESTAMP) AS STRING)) AS metric_tmstp_tz,
   CURRENT_DATE('GMT') AS metric_date                                                                                         --AT 'GMT'
from dummy
union all



select
   '{{params.dag_name}}' AS isf_dag_nm,
   CAST('LDG load step' AS STRING) AS step_nm,
   (SELECT batch_id FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(subject_area_nm) =LOWER('{{params.subject_area}}')) AS batch_id,
   CAST('`{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_STG.RMS_COST_RTV_SHIPMENT_DETAILS_V2_LDG' AS STRING) AS tbl_nm,
   CAST('LOADED_TO_LDG_CNT' AS STRING) AS metric_nm,
   CAST(NULL AS STRING) AS metric_value,
   CAST(NULL AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(CAST(NULL AS TIMESTAMP) AS STRING)) AS metric_tmstp_tz,
   CURRENT_DATE('GMT') AS metric_date                                                                                         --AT 'GMT'
from dummy
union all
select
  '{{params.dag_name}}' AS isf_dag_nm,
  CAST('Fact table load' AS STRING) AS step_nm,
  (SELECT batch_id FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(subject_area_nm) =LOWER('{{params.subject_area}}')) AS batch_id,
  CAST('`{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.RMS_COST_RTV_SHIPMENT_DETAILS_V2_FACT' AS STRING) AS tbl_nm,
  CAST('TERADATA_JOB_START_TMSTP' AS STRING) AS metric_nm,
  CAST(NULL AS STRING) AS metric_value,
  CAST(NULL AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(CAST(NULL AS TIMESTAMP) AS STRING)) AS metric_tmstp_tz,
  CURRENT_DATE('GMT') AS metric_date                                                                                          --AT 'GMT'
from dummy
union all
select
  '{{params.dag_name}}' AS isf_dag_nm,
  CAST('Fact table load' AS STRING) AS step_nm,
  (SELECT batch_id FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(subject_area_nm) =LOWER('{{params.subject_area}}')) AS batch_id,
  CAST('`{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.RMS_COST_RTV_SHIPMENT_DETAILS_V2_FACT' AS STRING) AS tbl_nm,
  CAST('TERADATA_JOB_END_TMSTP' AS STRING) AS metric_nm,
  CAST(NULL AS STRING) AS metric_value,
  CAST(NULL AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(CAST(NULL AS TIMESTAMP) AS STRING)) AS metric_tmstp_tz,
  CURRENT_DATE('GMT') AS metric_date                                                                                          --AT 'GMT'
from dummy
union all



select
   '{{params.dag_name}}' AS isf_dag_nm,
   CAST('LDG load step' AS STRING) AS step_nm,
   (SELECT batch_id FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(subject_area_nm) =LOWER('{{params.subject_area}}')) AS batch_id,
   CAST('`{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_STG.RMS_COST_RTV_CANCELED_DETAILS_V2_LDG' AS STRING) AS tbl_nm,
   CAST('LOADED_TO_LDG_CNT' AS STRING) AS metric_nm,
   CAST(NULL AS STRING) AS metric_value,
   CAST(NULL AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(CAST(NULL AS TIMESTAMP) AS STRING)) AS metric_tmstp_tz,
   CURRENT_DATE('GMT') AS metric_date                                                                                         --AT 'GMT'
from dummy
union all
select
  '{{params.dag_name}}' AS isf_dag_nm,
  CAST('Fact table load' AS STRING) AS step_nm,
  (SELECT batch_id FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(subject_area_nm) =LOWER('{{params.subject_area}}')) AS batch_id,
  CAST('`{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.RMS_COST_RTV_CANCELED_DETAILS_V2_FACT' AS STRING) AS tbl_nm,
  CAST('TERADATA_JOB_START_TMSTP' AS STRING) AS metric_nm,
  CAST(NULL AS STRING) AS metric_value,
  CAST(NULL AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(CAST(NULL AS TIMESTAMP) AS STRING)) AS metric_tmstp_tz,
  CURRENT_DATE('GMT') AS metric_date                                                                                          --AT 'GMT'
from dummy
union all
select
  '{{params.dag_name}}' AS isf_dag_nm,
  CAST('Fact table load' AS STRING) AS step_nm,
  (SELECT batch_id FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(subject_area_nm) =LOWER('{{params.subject_area}}')) AS batch_id,
  CAST('`{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.RMS_COST_RTV_CANCELED_DETAILS_V2_FACT' AS STRING) AS tbl_nm,
  CAST('TERADATA_JOB_END_TMSTP' AS STRING) AS metric_nm,
  CAST(NULL AS STRING) AS metric_value,
  CAST(NULL AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(CAST(NULL AS TIMESTAMP) AS STRING)) AS metric_tmstp_tz,
  CURRENT_DATE('GMT') AS metric_date                                                                                          --AT 'GMT'
from dummy
union all



select
   '{{params.dag_name}}' AS isf_dag_nm,
   CAST('LDG load step' AS STRING) AS step_nm,
   (SELECT batch_id FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(subject_area_nm) =LOWER('{{params.subject_area}}')) AS batch_id,
   CAST('`{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_STG.RMS_COST_RTV_V2_LDG' AS STRING) AS tbl_nm,
   CAST('LOADED_TO_LDG_CNT' AS STRING) AS metric_nm,
   CAST(NULL AS STRING) AS metric_value,
   CAST(NULL AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(CAST(NULL AS TIMESTAMP) AS STRING)) AS metric_tmstp_tz,
   CURRENT_DATE('GMT') AS metric_date                                                                                         --AT 'GMT'
from dummy
union all
select
  '{{params.dag_name}}' AS isf_dag_nm,
  CAST('Fact table load' AS STRING) AS step_nm,
  (SELECT batch_id FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(subject_area_nm) =LOWER('{{params.subject_area}}')) AS batch_id,
  CAST('`{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.RMS_COST_RTV_V2_FACT' AS STRING) AS tbl_nm,
  CAST('TERADATA_JOB_START_TMSTP' AS STRING) AS metric_nm,
  CAST(NULL AS STRING) AS metric_value,
  CAST(NULL AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(CAST(NULL AS TIMESTAMP) AS STRING)) AS metric_tmstp_tz,
  CURRENT_DATE('GMT') AS metric_date                                                                                          --AT 'GMT'
from dummy
union all
select
  '{{params.dag_name}}' AS isf_dag_nm,
  CAST('Fact table load' AS STRING) AS step_nm,
  (SELECT batch_id FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(subject_area_nm) =LOWER('{{params.subject_area}}')) AS batch_id,
  CAST('`{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.RMS_COST_RTV_V2_FACT' AS STRING) AS tbl_nm,
  CAST('TERADATA_JOB_END_TMSTP' AS STRING) AS metric_nm,
  CAST(NULL AS STRING) AS metric_value,
  CAST(NULL AS TIMESTAMP) AS metric_tmstp,
  `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(CAST(NULL AS TIMESTAMP) AS STRING)) AS metric_tmstp_tz,
  CURRENT_DATE('GMT') AS metric_date                                                                                          --AT 'GMT'
from dummy
;

/***********************************************************************************
-- Delete previous spark log row 
************************************************************************************/
DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.processed_data_spark_log
WHERE LOWER(isf_dag_nm) = LOWER('{{params.dag_name}}');
