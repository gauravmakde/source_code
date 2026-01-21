SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=hr_worker_load_v1_2656_napstore_insights;
Task_Name=hr_worker_v2_load_td_stg_to_dims;'
FOR SESSION VOLATILE;

ET;
-- Create temporary table that has the latest data for each worker number and worker type for HR_WORKER_EMPLOYEE_DETAILS.
CREATE VOLATILE MULTISET TABLE HR_WORKER_EMPLOYEE_DETAILS_TEMP AS (
  	select distinct
    worker_number,
	worker_type,
    employment_status_last_updated,
    employment_status_details_transaction_id,
    contingent_worker_projected_end_date,
    hire_date,
    original_hire_date,
    termination_date,
    case when worker_status='' then NULL
         when hire_status='CANCELLED' then 'UNKNOWN'
         else worker_status
    end as worker_status,
    employment_status_change_effective_date,
    last_updated
    from {db_env}_NAP_HR_STG.HR_WORKER_V1_LDG
       where employment_status_details_transaction_id is not null
       QUALIFY Rank() Over( PARTITION BY worker_number,worker_type,employment_status_details_transaction_id
       ORDER BY last_updated DESC) = 1
) WITH DATA PRIMARY INDEX( worker_number) ON COMMIT PRESERVE ROWS;
ET;

-- Merge and update if worker number and employment_status_details_transaction_id exist.
MERGE INTO {db_env}_NAP_HR_BASE_VWS.HR_EMPLOYMENT_STATUS_DETAILS_DIM tgt
USING  HR_WORKER_EMPLOYEE_DETAILS_TEMP src
	ON (src.employment_status_details_transaction_id = tgt.transaction_id AND src.worker_number = tgt.worker_number AND src.worker_type=tgt.worker_type)
WHEN MATCHED THEN
UPDATE
SET
    last_updated = src.employment_status_last_updated,
    contingent_worker_projected_end_date = src.contingent_worker_projected_end_date,
    hire_date = src.hire_date,
    original_hire_date = src.original_hire_date,
    termination_date = src.termination_date,
    worker_status = src.worker_status,
    employment_status_change_effective_tmstp = src.employment_status_change_effective_date,
	dw_batch_date = CURRENT_DATE,
	dw_sys_load_tmstp = CURRENT_TIMESTAMP(0)
WHEN NOT MATCHED THEN
INSERT (
    worker_number,
	worker_type,
    last_updated,
    transaction_id,
    contingent_worker_projected_end_date,
    hire_date,
    original_hire_date,
    termination_date,
    worker_status,
    employment_status_change_effective_tmstp,
	dw_batch_date,
	dw_sys_load_tmstp
)
VALUES (
    src.worker_number,
	src.worker_type,
    src.employment_status_last_updated,
    src.employment_status_details_transaction_id,
    src.contingent_worker_projected_end_date,
    src.hire_date,
    src.original_hire_date,
    src.termination_date,
    src.worker_status,
    src.employment_status_change_effective_date,
	CURRENT_DATE,
	CURRENT_TIMESTAMP(0)
);
ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;
