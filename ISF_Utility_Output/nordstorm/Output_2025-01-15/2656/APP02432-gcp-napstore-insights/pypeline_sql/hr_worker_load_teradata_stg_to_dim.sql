SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=hr_worker_load_v1_2656_napstore_insights;
Task_Name=hr_worker_v2_load_td_stg_to_dims;'
FOR SESSION VOLATILE;

ET;

-- Create temporary table that has the latest data for each worker number and worker type.
CREATE VOLATILE MULTISET TABLE HR_WORKER_DIM_UNIQUE_TEMP AS (
    SELECT DISTINCT
    worker_number,
	worker_type,
    last_updated,

    case when hire_status_details_transaction_id='' then NULL else hire_status_details_transaction_id end as hire_status_details_transaction_id,
    case when hire_status='' then NULL else hire_status end as hire_status,
    hire_status_change_effective_date,

    case when lanId_details_transaction_id='' then NULL else lanId_details_transaction_id end as lanId_details_transaction_id,
    case when lanId='' then NULL else lanId end as lanId,
    lanId_effective_date,

    case when worker_name_details_transaction_id='' then NULL else worker_name_details_transaction_id end as worker_name_details_transaction_id,
    case when first_name='' then NULL else first_name end as first_name,
    case when last_name='' then NULL else last_name end as last_name,
    case when middle_name='' then NULL else middle_name end as middle_name,
    case when preferred_last_name='' then NULL else preferred_last_name end as preferred_last_name,
    case when preferred_first_name='' then NULL else preferred_first_name end as preferred_first_name,
    name_change_effective_date,

    case when employment_status_details_transaction_id='' then NULL else employment_status_details_transaction_id end as employment_status_details_transaction_id,
    contingent_worker_projected_end_date,
    hire_date,
    original_hire_date,
    termination_date,
    case when worker_status='' then NULL else worker_status end as worker_status,
    employment_status_change_effective_date,

    case when job_details_transaction_id='' then NULL else job_details_transaction_id end as job_details_transaction_id,
    case when commission_plan_name='' then NULL else commission_plan_name end as commission_plan_name,
    case when compensation_currency='' then NULL else compensation_currency end as compensation_currency,
    case when is_job_exempt='' then NULL else is_job_exempt end as is_job_exempt,
    case when is_remote_worker='' then NULL else is_remote_worker end as is_remote_worker,
    case when job_profile_id='' then NULL else job_profile_id end as job_profile_id,
    case when location_number='' then NULL else location_number end as location_number,
    case when pay_rate_type='' then NULL else pay_rate_type end as pay_rate_type,
    case when worker_sub_type='' then NULL else worker_sub_type end as worker_sub_type,
    job_details_change_effective_date,

    case when contingent_worker_details_transaction_id='' then NULL else contingent_worker_details_transaction_id end as contingent_worker_details_transaction_id,
    case when is_onsite='' then NULL else is_onsite end as is_onsite,
    case when contingent_worker_vendor_name='' then NULL else contingent_worker_vendor_name end as contingent_worker_vendor_name,
    case when contingent_worker_vendor_number='' then NULL else contingent_worker_vendor_number end as contingent_worker_vendor_number,
    contingent_worker_change_effective_date,

    case when org_details_transaction_id='' then NULL else org_details_transaction_id end as org_details_transaction_id,
    case when business_unit='' then NULL else business_unit end as business_unit,
    case when company_number='' then NULL else company_number end as company_number,
    case when cost_center='' then NULL else cost_center end as cost_center,
    case when company_hierarchy='' then NULL else company_hierarchy end as company_hierarchy,
    case when payroll_department='' then NULL else payroll_department end as payroll_department,
    case when payroll_store='' then NULL else payroll_store end as payroll_store,
    case when region_number='' then NULL else region_number end as region_number,
    org_effective_date,

    case when discount_details_transaction_id='' then NULL else discount_details_transaction_id end as discount_details_transaction_id,
    case when discount_percent='' then NULL else discount_percent end as discount_percent,
    case when discount_status='' then NULL else discount_status end as discount_status,
    discount_change_effective_date,

    case when position_details_transaction_id='' then NULL else position_details_transaction_id end as position_details_transaction_id,
    case when beauty_line_assignment='' then NULL else beauty_line_assignment end as beauty_line_assignment,
    case when beauty_line_assignment_id='' then NULL else beauty_line_assignment_id end as beauty_line_assignment_id,
    case when other_line_assignment='' then NULL else other_line_assignment end as other_line_assignment,
    case when line_assignment='' then NULL else line_assignment end as line_assignment,
    line_assignment_change_effective_date,

    case when work_contact_details_transaction_id='' then NULL else work_contact_details_transaction_id end as work_contact_details_transaction_id,
    case when corporate_email='' then NULL else corporate_email end as corporate_email,
    case when corporate_phone_number='' then NULL else corporate_phone_number end as corporate_phone_number,
    work_contact_change_effective_date,

    case when manager_details_transaction_id='' then NULL else manager_details_transaction_id end as manager_details_transaction_id,
    case when manager_worker_number='' then NULL else manager_worker_number end as manager_worker_number,
    manager_change_effective_date
    FROM {db_env}_NAP_HR_STG.HR_WORKER_V1_LDG
    qualify rank() over (partition BY worker_type, worker_number ORDER BY last_updated DESC) = 1
) WITH DATA PRIMARY INDEX(worker_type, worker_number) ON COMMIT PRESERVE ROWS;
ET;

-- Merge and update if worker number and worker type exist.
MERGE INTO {db_env}_NAP_HR_BASE_VWS.HR_WORKER_V1_DIM tgt
USING  HR_WORKER_DIM_UNIQUE_TEMP src
	ON (src.worker_type = tgt.worker_type AND src.worker_number = tgt.worker_number)
WHEN MATCHED THEN
UPDATE
SET
    last_updated = src.last_updated,

    hire_status_details_transaction_id = src.hire_status_details_transaction_id,
    hire_status = src.hire_status,
    hire_status_change_effective_date = src.hire_status_change_effective_date,

    lanId_details_transaction_id = src.lanId_details_transaction_id,
    lanId = src.lanId,
    lanId_effective_date = src.lanId_effective_date,

    worker_name_details_transaction_id = src.worker_name_details_transaction_id,
    first_name = src.first_name,
    last_name = src.last_name,
    middle_name = src.middle_name,
    preferred_last_name = src.preferred_last_name,
    preferred_first_name = src.preferred_first_name,
    name_change_effective_date = src.name_change_effective_date,

    employment_status_details_transaction_id = src.employment_status_details_transaction_id,
    contingent_worker_projected_end_date = src.contingent_worker_projected_end_date,
    hire_date = src.hire_date,
    original_hire_date = src.original_hire_date,
    termination_date = src.termination_date,
    worker_status = src.worker_status,
    employment_status_change_effective_date = src.employment_status_change_effective_date,

    job_details_transaction_id = src.job_details_transaction_id,
    commission_plan_name = src.commission_plan_name,
    compensation_currency = src.compensation_currency,
    is_job_exempt = src.is_job_exempt,
    is_remote_worker = src.is_remote_worker,
    job_profile_id = src.job_profile_id,
    location_number = src.location_number,
    pay_rate_type = src.pay_rate_type,
    worker_sub_type = src.worker_sub_type,
    job_details_change_effective_date = src.job_details_change_effective_date,

    contingent_worker_details_transaction_id = src.contingent_worker_details_transaction_id,
    is_onsite = src.is_onsite,
    contingent_worker_vendor_name = src.contingent_worker_vendor_name,
    contingent_worker_vendor_number = src.contingent_worker_vendor_number,
    contingent_worker_change_effective_date = src.contingent_worker_change_effective_date,

    org_details_transaction_id = src.org_details_transaction_id,
    business_unit = src.business_unit,
    company_number = src.company_number,
    cost_center = src.cost_center,
    company_hierarchy = src.company_hierarchy,
    payroll_department = src.payroll_department,
    payroll_store = src.payroll_store,
    region_number = src.region_number,
    org_effective_date = src.org_effective_date,

    discount_details_transaction_id = src.discount_details_transaction_id,
    discount_percent = src.discount_percent,
    discount_status = src.discount_status,
    discount_change_effective_date = src.discount_change_effective_date,

    position_details_transaction_id = src.position_details_transaction_id,
    beauty_line_assignment = src.beauty_line_assignment,
    beauty_line_assignment_id = src.beauty_line_assignment_id,
    other_line_assignment = src.other_line_assignment,
    line_assignment = src.line_assignment,
    line_assignment_change_effective_date = src.line_assignment_change_effective_date,

    work_contact_details_transaction_id = src.work_contact_details_transaction_id,
    corporate_email = src.corporate_email,
    corporate_phone_number = src.corporate_phone_number,
    work_contact_change_effective_date = src.work_contact_change_effective_date,

    manager_details_transaction_id = src.manager_details_transaction_id,
    manager_worker_number = src.manager_worker_number,
    manager_change_effective_date = src.manager_change_effective_date,

	dw_batch_date = CURRENT_DATE,
	dw_sys_load_tmstp = CURRENT_TIMESTAMP(0)
WHEN NOT MATCHED THEN
INSERT (
    worker_number,
	worker_type,
    last_updated,

    hire_status_details_transaction_id,
    hire_status,
    hire_status_change_effective_date,

    lanId_details_transaction_id,
    lanId,
    lanId_effective_date,

    worker_name_details_transaction_id,
    first_name,
    last_name,
    middle_name,
    preferred_last_name,
    preferred_first_name,
    name_change_effective_date,

    employment_status_details_transaction_id,
    contingent_worker_projected_end_date,
    hire_date,
    original_hire_date,
    termination_date,
    worker_status,
    employment_status_change_effective_date,

    job_details_transaction_id,
    commission_plan_name,
    compensation_currency,
    is_job_exempt,
    is_remote_worker,
    job_profile_id,
    location_number,
    pay_rate_type,
    worker_sub_type,
    job_details_change_effective_date,

    contingent_worker_details_transaction_id,
    is_onsite,
    contingent_worker_vendor_name,
    contingent_worker_vendor_number,
    contingent_worker_change_effective_date,

    org_details_transaction_id,
    business_unit,
    company_number,
    cost_center,
    company_hierarchy,
    payroll_department,
    payroll_store,
    region_number,
    org_effective_date,

    discount_details_transaction_id,
    discount_percent,
    discount_status,
    discount_change_effective_date,

    position_details_transaction_id,
    beauty_line_assignment,
    beauty_line_assignment_id,
    other_line_assignment,
    line_assignment,
    line_assignment_change_effective_date,

    work_contact_details_transaction_id,
    corporate_email,
    corporate_phone_number,
    work_contact_change_effective_date,

    manager_details_transaction_id,
    manager_worker_number,
    manager_change_effective_date,

	dw_batch_date,
	dw_sys_load_tmstp
)
VALUES (
    src.worker_number,
	src.worker_type,
    src.last_updated,

    src.hire_status_details_transaction_id,
    src.hire_status,
    src.hire_status_change_effective_date,

    src.lanId_details_transaction_id,
    src.lanId,
    src.lanId_effective_date,

    src.worker_name_details_transaction_id,
    src.first_name,
    src.last_name,
    src.middle_name,
    src.preferred_last_name,
    src.preferred_first_name,
    src.name_change_effective_date,

    src.employment_status_details_transaction_id,
    src.contingent_worker_projected_end_date,
    src.hire_date,
    src.original_hire_date,
    src.termination_date,
    src.worker_status,
    src.employment_status_change_effective_date,

    src.job_details_transaction_id,
    src.commission_plan_name,
    src.compensation_currency,
    src.is_job_exempt,
    src.is_remote_worker,
    src.job_profile_id,
    src.location_number,
    src.pay_rate_type,
    src.worker_sub_type,
    src.job_details_change_effective_date,

    src.contingent_worker_details_transaction_id,
    src.is_onsite,
    src.contingent_worker_vendor_name,
    src.contingent_worker_vendor_number,
    src.contingent_worker_change_effective_date,

    src.org_details_transaction_id,
    src.business_unit,
    src.company_number,
    src.cost_center,
    src.company_hierarchy,
    src.payroll_department,
    src.payroll_store,
    src.region_number,
    src.org_effective_date,

    src.discount_details_transaction_id,
    src.discount_percent,
    src.discount_status,
    src.discount_change_effective_date,

    src.position_details_transaction_id,
    src.beauty_line_assignment,
    src.beauty_line_assignment_id,
    src.other_line_assignment,
    src.line_assignment,
    src.line_assignment_change_effective_date,

    src.work_contact_details_transaction_id,
    src.corporate_email,
    src.corporate_phone_number,
    src.work_contact_change_effective_date,

    src.manager_details_transaction_id,
    src.manager_worker_number,
    src.manager_change_effective_date,

	CURRENT_DATE,
	CURRENT_TIMESTAMP(0)
);

ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;
