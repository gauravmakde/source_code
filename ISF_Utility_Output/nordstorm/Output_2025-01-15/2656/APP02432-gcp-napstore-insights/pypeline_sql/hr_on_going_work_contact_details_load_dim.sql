SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=hr_worker_load_v1_2656_napstore_insights;
Task_Name=hr_worker_v2_load_td_stg_to_dims;'
FOR SESSION VOLATILE;

ET;
-- /* Daily data load */

CREATE VOLATILE MULTISET TABLE HR_WORK_CONTACT_DETAILS_TEMP AS (
    select distinct
    worker_number,
	worker_type,
    work_contact_details_last_updated,
    work_contact_details_transaction_id,
    case when corporate_email='' then NULL else corporate_email end as corporate_email,
    case when corporate_phone_number='' then NULL else corporate_phone_number end as corporate_phone_number,
    work_contact_change_effective_date,
    last_updated
    from {db_env}_NAP_HR_STG.HR_WORKER_V1_LDG where work_contact_details_transaction_id is not null
    QUALIFY Rank() Over( PARTITION BY worker_number,worker_type,work_contact_details_transaction_id
    ORDER BY last_updated DESC) = 1
) WITH DATA PRIMARY INDEX( worker_number) ON COMMIT PRESERVE ROWS;
ET;

-- Merge and update if worker number and work_contact_details_transaction_id exist.
MERGE INTO {db_env}_NAP_HR_BASE_VWS.HR_WORK_CONTACT_DETAILS_DIM tgt
USING  HR_WORK_CONTACT_DETAILS_TEMP src
	ON (src.work_contact_details_transaction_id = tgt.transaction_id AND src.worker_number = tgt.worker_number AND src.worker_type=tgt.worker_type)
WHEN MATCHED THEN
UPDATE
SET
    last_updated = src.work_contact_details_last_updated,
    corporate_email = src.corporate_email,
    corporate_phone_number = src.corporate_phone_number,
    work_contact_change_effective_tmstp = src.work_contact_change_effective_date,
	dw_batch_date = CURRENT_DATE,
	dw_sys_load_tmstp = CURRENT_TIMESTAMP(0)
WHEN NOT MATCHED THEN
INSERT (
    worker_number,
	worker_type,
    last_updated,
    transaction_id,
    corporate_email,
    corporate_phone_number,
    work_contact_change_effective_tmstp,
	dw_batch_date,
	dw_sys_load_tmstp
)
VALUES (
    src.worker_number,
	src.worker_type,
    src.work_contact_details_last_updated,
    src.work_contact_details_transaction_id,
    src.corporate_email,
    src.corporate_phone_number,
    src.work_contact_change_effective_date,
	CURRENT_DATE,
	CURRENT_TIMESTAMP(0)
);

ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;
