SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=hr_outoforder_worker_data_daily_load_2656_napstore_insights;
Task_Name=job_details_stage_load_4_job_details_stg_tables;'
FOR SESSION VOLATILE;

-- load out of order worker data from S3 location in AVRO format to Teradata sink:
CREATE OR REPLACE TEMPORARY VIEW hr_job_details_input 
AS select * from s3_avro_input_for_job_details;

-- worker teradata sink:
insert overwrite table hr_worker_v1_ldg
select  employeeId.id as worker_number,
        workerType as worker_type,
        CURRENT_TIMESTAMP as last_updated,
        transactionId as job_details_transaction_id,
        jobInformation.commissionPlanName as commission_plan_name,
        jobInformation.compensationInformation.compensationCurrencyCode as compensation_currency,
        CAST(jobInformation.isJobExempt as VARCHAR(100)) as is_job_exempt,
        CAST(jobInformation.isRemoteWorker as VARCHAR(100)) as is_remote_worker,
        jobInformation.jobProfileId as job_profile_id,
        jobInformation.locationNumber as location_number,
        jobInformation.payRateType as pay_rate_type,
        jobInformation.workerSubtype as worker_sub_type,
        effectiveDate as job_details_change_effective_date,
        jobInformation.managerEmployeeId.id as manager_worker_number,
        eventTime as job_details_last_updated,
        transactionId as org_details_transaction_id,
        organizations.businessUnitNumber as business_unit,
        organizations.companyNumber	as company_number,
        organizations.costCenterNumber as cost_center,
        organizations.companyHierarchy as company_hierarchy,
        organizations.departmentNumber as payroll_department,
        organizations.storeNumber as payroll_store,
        organizations.regionNumber as region_number,
        effectiveDate as org_effective_date,
        eventTime as org_details_last_updated,
        transactionId as position_details_transaction_id,
        additionalPositionInformation.beautyLineAssignment as beauty_line_assignment,
        additionalPositionInformation.beautyLineAssignmentId as beauty_line_assignment_id,
        additionalPositionInformation.otherLineAssignment as other_line_assignment,
        CAST(additionalPositionInformation.lineAssignment as VARCHAR(2000)) as line_assignment,
        effectiveDate as line_assignment_change_effective_date,
        eventTime as line_position_details_last_updated
from hr_job_details_input;
