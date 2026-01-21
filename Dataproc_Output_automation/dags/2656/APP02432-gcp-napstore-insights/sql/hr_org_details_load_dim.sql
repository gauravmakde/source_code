-- SET QUERY_BAND = '
-- App_ID=app02432;
-- DAG_ID=hr_outoforder_worker_data_daily_load_2656_napstore_insights;
-- Task_Name=org_details_stage_load_3_org_details_dim_tables;'
-- FOR SESSION VOLATILE;

-- ET;

-- /* Daily data load */


CREATE TEMPORARY TABLE IF NOT EXISTS hr_org_details_temp
AS
SELECT DISTINCT worker_number,
 worker_type,
 org_details_last_updated,
 org_details_last_updated_tz,
 org_details_transaction_id,
  CASE
  WHEN LOWER(business_unit) = LOWER('')
  THEN NULL
  ELSE business_unit
  END AS business_unit,
  CASE
  WHEN LOWER(company_number) = LOWER('')
  THEN NULL
  ELSE company_number
  END AS company_number,
  CASE
  WHEN LOWER(cost_center) = LOWER('')
  THEN NULL
  ELSE cost_center
  END AS cost_center,
  CASE
  WHEN LOWER(company_hierarchy) = LOWER('')
  THEN NULL
  ELSE company_hierarchy
  END AS company_hierarchy,
  CASE
  WHEN LOWER(payroll_department) = LOWER('')
  THEN NULL
  ELSE payroll_department
  END AS payroll_department,
  CASE
  WHEN LOWER(payroll_store) = LOWER('')
  THEN NULL
  ELSE payroll_store
  END AS payroll_store,
  CASE
  WHEN LOWER(region_number) = LOWER('')
  THEN NULL
  ELSE region_number
  END AS region_number,
 cast(org_effective_date as datetime) as org_effective_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_hr_stg.hr_worker_v1_ldg
WHERE org_details_transaction_id IS NOT NULL
QUALIFY (ROW_NUMBER() OVER (PARTITION BY worker_number, worker_type, org_details_transaction_id ORDER BY
      org_details_last_updated DESC)) = 1;
--ET

MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_hr_dim.hr_org_details_dim AS tgt
USING hr_org_details_temp AS src
ON LOWER(src.org_details_transaction_id) = LOWER(tgt.transaction_id) 
AND LOWER(src.worker_number) = LOWER(tgt.worker_number) 
AND LOWER(src.worker_type) = LOWER(tgt.worker_type)
WHEN MATCHED THEN UPDATE SET
    last_updated = src.org_details_last_updated,
    last_updated_tz = src.org_details_last_updated_tz, 
    business_unit = src.business_unit,
    company_number = src.company_number,
    cost_center = src.cost_center,
    company_hierarchy = src.company_hierarchy,
    payroll_department = src.payroll_department,
    payroll_store = src.payroll_store,
    region_number = src.region_number,
    org_details_change_effective_tmstp = src.org_effective_date,
    dw_batch_date = CURRENT_DATE('PST8PDT'),
    dw_sys_load_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
WHEN NOT MATCHED THEN INSERT VALUES(src.worker_number, src.worker_type, src.org_details_last_updated,src.org_details_last_updated_tz, src.org_details_transaction_id, src.business_unit, src.company_number, src.cost_center, src.company_hierarchy, src.payroll_department, src.payroll_store, src.region_number, src.org_effective_date, CURRENT_DATE('PST8PDT'), CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME));

-- ET;

-- SET QUERY_BAND = NONE FOR SESSION;

-- ET;

