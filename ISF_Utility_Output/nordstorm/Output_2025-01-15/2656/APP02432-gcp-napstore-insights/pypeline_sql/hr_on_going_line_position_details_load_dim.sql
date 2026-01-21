SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=hr_worker_load_v1_2656_napstore_insights;
Task_Name=hr_worker_v2_load_td_stg_to_dims;'
FOR SESSION VOLATILE;

ET;
-- /* Daily data load */

-- Create temporary table that has the latest data for each worker number and worker type for HR_WORKER_LINE_POSITION_DETAILS.
CREATE VOLATILE MULTISET TABLE HR_WORKER_LINE_POSITION_DETAILS_TEMP AS (
    select distinct
    worker_number,
	worker_type,
    line_position_details_last_updated,
    position_details_transaction_id,
    case when beauty_line_assignment='' then NULL else beauty_line_assignment end as beauty_line_assignment,
    case when beauty_line_assignment_id='' then NULL else beauty_line_assignment_id end as beauty_line_assignment_id,
    case when other_line_assignment='' then NULL else other_line_assignment end as other_line_assignment,
    case when line_assignment='' then NULL else line_assignment end as line_assignment,
    line_assignment_change_effective_date,
    last_updated
    from {db_env}_NAP_HR_STG.HR_WORKER_V1_LDG where position_details_transaction_id is not null
    QUALIFY Rank() Over( PARTITION BY worker_number,worker_type,position_details_transaction_id
    ORDER BY last_updated DESC) = 1
) WITH DATA PRIMARY INDEX( worker_number) ON COMMIT PRESERVE ROWS;
ET;

-- Merge and update if worker number and job_details_transaction_id exist.
MERGE INTO {db_env}_NAP_HR_BASE_VWS.HR_LINE_POSITION_DETAILS_DIM tgt
USING  HR_WORKER_LINE_POSITION_DETAILS_TEMP src
	ON (src.position_details_transaction_id = tgt.transaction_id AND src.worker_number = tgt.worker_number AND src.worker_type=tgt.worker_type)
WHEN MATCHED THEN
UPDATE
SET
    last_updated = src.line_position_details_last_updated,
    beauty_line_assignment = src.beauty_line_assignment,
    beauty_line_assignment_id = src.beauty_line_assignment_id,
    other_line_assignment = src.other_line_assignment,
    line_assignment = src.line_assignment,
    line_assignment_change_effective_tmstp=src.line_assignment_change_effective_date,
	dw_batch_date = CURRENT_DATE,
	dw_sys_load_tmstp = CURRENT_TIMESTAMP(0)
WHEN NOT MATCHED THEN
INSERT (
    worker_number,
	worker_type,
    last_updated,
    transaction_id,
    beauty_line_assignment,
    beauty_line_assignment_id,
    other_line_assignment,
    line_assignment,
    line_assignment_change_effective_tmstp,
	dw_batch_date,
	dw_sys_load_tmstp
)
VALUES (
    src.worker_number,
	src.worker_type,
    src.line_position_details_last_updated,
    src.position_details_transaction_id,
    src.beauty_line_assignment,
    src.beauty_line_assignment_id,
    src.other_line_assignment,
    src.line_assignment,
    src.line_assignment_change_effective_date,
	CURRENT_DATE,
	CURRENT_TIMESTAMP(0)
);

ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;
