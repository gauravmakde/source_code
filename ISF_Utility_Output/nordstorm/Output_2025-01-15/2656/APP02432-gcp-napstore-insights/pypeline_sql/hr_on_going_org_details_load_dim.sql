SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=hr_worker_load_v1_2656_napstore_insights;
Task_Name=hr_worker_v2_load_td_stg_to_dims;'
FOR SESSION VOLATILE;

ET;
-- /* Daily data load */

CREATE VOLATILE MULTISET TABLE HR_ORG_DETAILS_TEMP AS (
    select distinct
    worker_number,
	worker_type,
    org_details_last_updated,
    org_details_transaction_id,
    case when business_unit='' then NULL else business_unit end as business_unit,
    case when company_number='' then NULL else company_number end as company_number,
    case when cost_center='' then NULL else cost_center end as cost_center,
    case when company_hierarchy='' then NULL else company_hierarchy end as company_hierarchy,
    case when payroll_department='' then NULL else payroll_department end as payroll_department,
    case when payroll_store='' then NULL else payroll_store end as payroll_store,
    case when region_number='' then NULL else region_number end as region_number,
    org_effective_date,
    last_updated
    from {db_env}_NAP_HR_STG.HR_WORKER_V1_LDG where org_details_transaction_id is not null
    QUALIFY Rank() Over( PARTITION BY worker_number,worker_type,org_details_transaction_id
    ORDER BY last_updated DESC) =1
) WITH DATA PRIMARY INDEX( worker_number) ON COMMIT PRESERVE ROWS;
ET;

-- Merge and update if worker number and org_details_transaction_id exist.
MERGE INTO {db_env}_NAP_HR_BASE_VWS.HR_ORG_DETAILS_DIM tgt
USING  HR_ORG_DETAILS_TEMP src
	ON (src.org_details_transaction_id = tgt.transaction_id AND src.worker_number = tgt.worker_number AND src.worker_type=tgt.worker_type)
WHEN MATCHED THEN
UPDATE
SET
    last_updated = src.org_details_last_updated,
    business_unit = src.business_unit,
    company_number = src.company_number,
    cost_center = src.cost_center,
    company_hierarchy = src.company_hierarchy,
    payroll_department = src.payroll_department,
    payroll_store = src.payroll_store,
    region_number = src.region_number,
    org_details_change_effective_tmstp = src.org_effective_date,
	dw_batch_date = CURRENT_DATE,
	dw_sys_load_tmstp = CURRENT_TIMESTAMP(0)
WHEN NOT MATCHED THEN
INSERT (
    worker_number,
	worker_type,
    last_updated,
    transaction_id,
    business_unit,
    company_number,
    cost_center,
    company_hierarchy,
    payroll_department,
    payroll_store,
    region_number,
    org_details_change_effective_tmstp,
	dw_batch_date,
	dw_sys_load_tmstp
)
VALUES (
    src.worker_number,
	src.worker_type,
    src.org_details_last_updated,
    src.org_details_transaction_id,
    src.business_unit,
    src.company_number,
    src.cost_center,
    src.company_hierarchy,
    src.payroll_department,
    src.payroll_store,
    src.region_number,
    src.org_effective_date,
	CURRENT_DATE,
	CURRENT_TIMESTAMP(0)
);

ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;
