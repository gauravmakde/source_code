/* SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=hr_outoforder_worker_data_daily_load_2656_napstore_insights;
Task_Name=employee_details_stage_load_1_employee_details_dim_tables;'
FOR SESSION VOLATILE; */

--ET;

-- Create temporary table that has the latest data for each worker number and worker type for HR_WORKER_EMPLOYEE_DETAILS.
CREATE TEMPORARY TABLE IF NOT EXISTS hr_worker_employee_details_temp
AS
SELECT DISTINCT worker_number,
 worker_type,
 employment_status_last_updated,
 employment_status_last_updated_tz,
 employment_status_details_transaction_id,
 contingent_worker_projected_end_date,
 contingent_worker_projected_end_date_tz,
 hire_date,
 hire_date_tz,
 original_hire_date,
 original_hire_date_tz,
 termination_date,
 termination_date_tz,
  CASE
  WHEN LOWER(worker_status) = LOWER('')
  THEN NULL
  WHEN LOWER(hire_status) = LOWER('CANCELLED')
  THEN 'UNKNOWN'
  ELSE worker_status
  END AS worker_status,
 CAST(employment_status_change_effective_date AS DATETIME) AS employment_status_change_effective_date,
 employment_status_change_effective_date_tz
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_hr_stg.hr_worker_v1_ldg
WHERE employment_status_details_transaction_id IS NOT NULL
QUALIFY (ROW_NUMBER() OVER (PARTITION BY worker_number, worker_type, employment_status_details_transaction_id ORDER BY
      employment_status_last_updated DESC)) = 1;
      
--ET;

-- Merge and update if worker number and employment_status_details_transaction_id exist.
MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_hr_dim.hr_employment_status_details_dim AS tgt
USING hr_worker_employee_details_temp AS src
ON LOWER(src.employment_status_details_transaction_id) = LOWER(tgt.transaction_id) 
AND LOWER(src.worker_number) = LOWER(tgt.worker_number) 
AND LOWER(src.worker_type) = LOWER(tgt.worker_type)
WHEN MATCHED THEN UPDATE SET
    last_updated = src.employment_status_last_updated,
    last_updated_tz = src.employment_status_last_updated_tz,
    contingent_worker_projected_end_date = CAST(src.contingent_worker_projected_end_date AS DATE),
    hire_date = CAST(src.hire_date AS DATE),
    original_hire_date = CAST(src.original_hire_date AS DATE),
    termination_date = CAST(src.termination_date AS DATE),
    worker_status = src.worker_status,
    employment_status_change_effective_tmstp = src.employment_status_change_effective_date,
    dw_batch_date = CURRENT_DATE('PST8PDT'),
    dw_sys_load_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
WHEN NOT MATCHED THEN INSERT VALUES(src.worker_number, src.worker_type, src.employment_status_last_updated, src.employment_status_last_updated_tz, src.employment_status_details_transaction_id, CAST(src.contingent_worker_projected_end_date AS DATE), CAST(src.hire_date AS DATE), CAST(src.original_hire_date AS DATE), CAST(src.termination_date AS DATE), src.worker_status, src.employment_status_change_effective_date, CURRENT_DATE('PST8PDT'), CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME));

--ET;

--SET QUERY_BAND = NONE FOR SESSION;

--ET;

