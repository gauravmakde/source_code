/*
SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=hr_outoforder_worker_data_daily_load_2656_napstore_insights;
Task_Name=job_details_stage_load_5_job_details_dim_tables;'
FOR SESSION VOLATILE;
*/

--ET;
-- /* Daily data load */
-- Create temporary table that has the latest data for each worker number and worker type for HR_WORKER_JOB_DETAILS.



CREATE TEMPORARY TABLE IF NOT EXISTS hr_worker_job_details_temp
AS
SELECT DISTINCT worker_number,
 worker_type,
 job_details_last_updated,
 job_details_last_updated_tz,
 job_details_transaction_id,
  CASE
  WHEN LOWER(commission_plan_name) = LOWER('')
  THEN NULL
  ELSE commission_plan_name
  END AS commission_plan_name,
  CASE
  WHEN LOWER(compensation_currency) = LOWER('')
  THEN NULL
  ELSE compensation_currency
  END AS compensation_currency,
  CASE
  WHEN LOWER(is_job_exempt) = LOWER('')
  THEN NULL
  ELSE is_job_exempt
  END AS is_job_exempt,
  CASE
  WHEN LOWER(is_remote_worker) = LOWER('')
  THEN NULL
  ELSE is_remote_worker
  END AS is_remote_worker,
  CASE
  WHEN LOWER(job_profile_id) = LOWER('')
  THEN NULL
  ELSE job_profile_id
  END AS job_profile_id,
  CASE
  WHEN LOWER(location_number) = LOWER('')
  THEN NULL
  ELSE location_number
  END AS location_number,
  CASE
  WHEN LOWER(pay_rate_type) = LOWER('')
  THEN NULL
  ELSE pay_rate_type
  END AS pay_rate_type,
  CASE
  WHEN LOWER(worker_sub_type) = LOWER('')
  THEN NULL
  ELSE worker_sub_type
  END AS worker_sub_type,
  CASE
  WHEN LOWER(manager_worker_number) = LOWER('')
  THEN NULL
  ELSE manager_worker_number
  END AS manager_worker_number,
 job_details_change_effective_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_hr_stg.hr_worker_v1_ldg
WHERE job_details_transaction_id IS NOT NULL
QUALIFY (ROW_NUMBER() OVER (PARTITION BY worker_number, worker_type, job_details_transaction_id ORDER BY
      job_details_last_updated DESC)) = 1;

--ET;
MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_hr_dim.hr_job_details_dim AS tgt
USING hr_worker_job_details_temp AS src
ON LOWER(src.job_details_transaction_id) = LOWER(tgt.transaction_id) AND LOWER(src.worker_number) = LOWER(tgt.worker_number
    ) AND LOWER(src.worker_type) = LOWER(tgt.worker_type)
WHEN MATCHED THEN UPDATE SET
 last_updated = src.job_details_last_updated,
 last_updated_tz = src.job_details_last_updated_tz,
 commission_plan_name = src.commission_plan_name,
 compensation_currency = src.compensation_currency,
 is_job_exempt = src.is_job_exempt,
 is_remote_worker = src.is_remote_worker,
 job_profile_id = src.job_profile_id,
 location_number = src.location_number,
 pay_rate_type = src.pay_rate_type,
 worker_sub_type = src.worker_sub_type,
 manager_worker_number = src.worker_number,
 job_details_change_effective_tmstp = CAST(src.job_details_change_effective_date AS DATETIME),
 dw_batch_date = current_date('PST8PDT'),
 dw_sys_load_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
WHEN NOT MATCHED THEN INSERT VALUES(src.worker_number, src.worker_type, src.job_details_last_updated, src.job_details_last_updated_tz,src.job_details_transaction_id
 , src.commission_plan_name, src.compensation_currency, src.is_job_exempt, src.is_remote_worker, src.job_profile_id, src
 .location_number, src.pay_rate_type, src.worker_sub_type, src.manager_worker_number, CAST(src.job_details_change_effective_date as DATETIME)
 , CURRENT_DATE, CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME));
 
 --ET;

--SET QUERY_BAND = NONE FOR SESSION;

--ET;
