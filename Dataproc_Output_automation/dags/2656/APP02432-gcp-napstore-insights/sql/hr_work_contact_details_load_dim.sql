-- SET QUERY_BAND = '
-- App_ID=app02432;
-- DAG_ID=hr_outoforder_worker_data_daily_load_2656_napstore_insights;
-- Task_Name=work_contact_details_stage_load_9_workcontact_details_dim_tables;'
-- FOR SESSION VOLATILE;

-- ET;
-- /* Daily data load */

CREATE TEMPORARY TABLE IF NOT EXISTS hr_work_contact_details_temp
AS
SELECT DISTINCT worker_number,
 worker_type,
 work_contact_details_last_updated,
 work_contact_details_last_updated_tz,
 work_contact_details_transaction_id,
  CASE
  WHEN LOWER(corporate_email) = LOWER('')
  THEN NULL
  ELSE corporate_email
  END AS corporate_email,
  CASE
  WHEN LOWER(corporate_phone_number) = LOWER('')
  THEN NULL
  ELSE corporate_phone_number
  END AS corporate_phone_number,
 CAST(work_contact_change_effective_date AS DATETIME) AS work_contact_change_effective_date,
 work_contact_change_effective_date_tz
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_hr_stg.hr_worker_v1_ldg
WHERE work_contact_details_transaction_id IS NOT NULL
QUALIFY (ROW_NUMBER() OVER (PARTITION BY worker_number, worker_type, work_contact_details_transaction_id ORDER BY
      work_contact_details_last_updated DESC)) = 1;
-- ET;

-- Merge and update if worker number and work_contact_details_transaction_id exist.
MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_hr_dim.hr_work_contact_details_dim AS tgt
USING hr_work_contact_details_temp AS src
ON LOWER(src.work_contact_details_transaction_id) = LOWER(tgt.transaction_id) AND LOWER(src.worker_number) = LOWER(tgt.worker_number) AND LOWER(src.worker_type) = LOWER(tgt.worker_type)
WHEN MATCHED THEN UPDATE SET
    last_updated = src.work_contact_details_last_updated,
    last_updated_tz = src.work_contact_details_last_updated_tz,
    corporate_email = src.corporate_email,
    corporate_phone_number = src.corporate_phone_number,
    work_contact_change_effective_tmstp = src.work_contact_change_effective_date,
    dw_batch_date = CURRENT_DATE('PST8PDT'),
    dw_sys_load_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
WHEN NOT MATCHED THEN INSERT VALUES(src.worker_number, src.worker_type, src.work_contact_details_last_updated, src.work_contact_details_last_updated_tz,src.work_contact_details_transaction_id, src.corporate_email, src.corporate_phone_number, src.work_contact_change_effective_date, CURRENT_DATE('PST8PDT'), CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME));

-- ET;

-- SET QUERY_BAND = NONE FOR SESSION;

-- ET;

