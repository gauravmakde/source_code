SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=hr_outoforder_worker_data_daily_load_2656_napstore_insights;
Task_Name=job_details_stage_load_5_job_details_dim_tables;'
FOR SESSION VOLATILE;

ET;
-- /* Daily data load */
-- Create temporary table that has the latest data for each worker number and worker type for HR_WORKER_JOB_DETAILS.
CREATE VOLATILE MULTISET TABLE HR_WORKER_JOB_DETAILS_TEMP AS (
    select distinct
    worker_number,
	worker_type,
    job_details_last_updated,
    job_details_transaction_id,
    case when commission_plan_name='' then NULL else commission_plan_name end as commission_plan_name,
    case when compensation_currency='' then NULL else compensation_currency end as compensation_currency,
    case when is_job_exempt='' then NULL else is_job_exempt end as is_job_exempt,
    case when is_remote_worker='' then NULL else is_remote_worker end as is_remote_worker,
    case when job_profile_id='' then NULL else job_profile_id end as job_profile_id,
    case when location_number='' then NULL else location_number end as location_number,
    case when pay_rate_type='' then NULL else pay_rate_type end as pay_rate_type,
    case when worker_sub_type='' then NULL else worker_sub_type end as worker_sub_type,
    case when manager_worker_number='' then NULL else manager_worker_number end as manager_worker_number,
    job_details_change_effective_date
    from {db_env}_NAP_HR_STG.HR_WORKER_V1_LDG where job_details_transaction_id is not null
    QUALIFY Row_Number() Over( PARTITION BY worker_number,worker_type,job_details_transaction_id
    ORDER BY job_details_last_updated DESC) =1
) WITH DATA PRIMARY INDEX( worker_number) ON COMMIT PRESERVE ROWS;
ET;

-- Merge and update if worker number and job_details_transaction_id exist.
MERGE INTO {db_env}_NAP_HR_BASE_VWS.HR_JOB_DETAILS_DIM tgt
USING  HR_WORKER_JOB_DETAILS_TEMP src
	ON (src.job_details_transaction_id = tgt.transaction_id AND src.worker_number = tgt.worker_number AND src.worker_type=tgt.worker_type)
WHEN MATCHED THEN
UPDATE
SET
    last_updated = src.job_details_last_updated,
    commission_plan_name = src.commission_plan_name,
    compensation_currency = src.compensation_currency,
    is_job_exempt = src.is_job_exempt,
    is_remote_worker = src.is_remote_worker,
    job_profile_id = src.job_profile_id,
    location_number=src.location_number,
    pay_rate_type = src.pay_rate_type,
    worker_sub_type = src.worker_sub_type,
    manager_worker_number = src.manager_worker_number,
    job_details_change_effective_tmstp=src.job_details_change_effective_date,
	dw_batch_date = CURRENT_DATE,
	dw_sys_load_tmstp = CURRENT_TIMESTAMP(0)
WHEN NOT MATCHED THEN
INSERT (
    worker_number,
	worker_type,
    last_updated,
    transaction_id,
    commission_plan_name,
    compensation_currency,
    is_job_exempt,
    is_remote_worker,
    job_profile_id,
    location_number,
    pay_rate_type,
    worker_sub_type,
    manager_worker_number,
    job_details_change_effective_tmstp,
	dw_batch_date,
	dw_sys_load_tmstp
)
VALUES (
    src.worker_number,
	src.worker_type,
    src.job_details_last_updated,
    src.job_details_transaction_id,
    src.commission_plan_name,
    src.compensation_currency,
    src.is_job_exempt,
    src.is_remote_worker,
    src.job_profile_id,
    src.location_number,
    src.pay_rate_type,
    src.worker_sub_type,
    src.manager_worker_number,
    src.job_details_change_effective_date,
	CURRENT_DATE,
	CURRENT_TIMESTAMP(0)
);
ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;
