-- mandatory Query Band part
-- SET QUERY_BAND = 'App_ID=app07420; DAG_ID=payment_bin_v2_teradata; Task_Name=teradata_stg_to_teradata_fct_job;' -- noqa
-- FOR SESSION VOLATILE;


-- Create temporary table for selecting distinct records by 'source_id'.

CREATE TEMPORARY TABLE IF NOT EXISTS payment_result_bin_v2_ldg_temp
AS
SELECT DISTINCT *
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_pymnt_fraud_stg.payment_result_bin_v2_ldg
QUALIFY (ROW_NUMBER() OVER (PARTITION BY source_id ORDER BY created_time DESC)) = 1;


DELETE FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_pymnt_fraud_fct.payment_result_bin_v2_fact
WHERE source_id IN (SELECT DISTINCT source_id
  FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_pymnt_fraud_base_vws.payment_result_bin_v2_ldg);


INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_pymnt_fraud_fct.payment_result_bin_v2_fact (dw_sys_load_tmstp, dw_sys_updt_tmstp, source_id, bin,
 event_source, created_time, created_date)
(SELECT DISTINCT CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_updt_tmstp,
  source_id,
  bin,
  event_source,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(created_time AS DATETIME)) AS DATETIME) AS created_time,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(created_time AS DATETIME)) AS DATETIME) AS DATE) AS created_date
 FROM payment_result_bin_v2_ldg_temp);


-- Remove records from temporary tables


TRUNCATE TABLE {{params.gcp_project_id}}.{{params.dbenv}}_nap_pymnt_fraud_stg.payment_result_bin_v2_ldg;


-- SET QUERY_BAND = NONE FOR SESSION; -- noqa