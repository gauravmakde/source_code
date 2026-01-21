SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=hr_outoforder_worker_data_daily_load_2656_napstore_insights;
Task_Name=hired_details_stage_load_hired_details_a_stg_tables;'
FOR SESSION VOLATILE;

-- load out of order worker data from S3 location in AVRO format to Teradata sink:
CREATE OR REPLACE TEMPORARY VIEW s3_hired_details_input 
AS select * from s3_avro_input_for_hired_details;


-- -- worker teradata sink:
insert overwrite table hr_worker_v1_ldg
select  employeeId.id as worker_number,
        workerType as worker_type,
        CURRENT_TIMESTAMP as last_updated,

        transactionId as lanId_details_transaction_id,
        lanId as lanId,
        effectiveDate as lanId_effective_date,
        eventTime as lanId_last_updated,

        transactionId as worker_name_details_transaction_id,
        name.firstName as first_name,
        name.lastName as last_name,
        name.middleName as middle_name,
        name.preferredLastName as preferred_last_name,
        name.preferredFirstName as preferred_first_name,
        effectiveDate as name_change_effective_date,
        eventTime as name_change_last_updated,

        transactionId as employment_status_details_transaction_id,
        employmentStatus.contingentWorkerProjectedEndDate as contingent_worker_projected_end_date,
        employmentStatus.hireDate as hire_date,
        employmentStatus.originalHireDate as original_hire_date,
        employmentStatus.terminationDate as termination_date,
        employmentStatus.workerStatus as worker_status,
        effectiveDate as employment_status_change_effective_date,
        eventTime as employment_status_last_updated,

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
        eventTime as job_details_last_updated,

        transactionId as contingent_worker_details_transaction_id,
        CAST(contingentWorkerInformation.isOnsite as VARCHAR(100)) as is_onsite,
        contingentWorkerInformation.vendorName as contingent_worker_vendor_name,
        contingentWorkerInformation.vendorNumber as contingent_worker_vendor_number,
        effectiveDate as contingent_worker_change_effective_date,
        eventTime as contingent_worker_last_updated,

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

        transactionId as discount_details_transaction_id,
        discountInformation.discountPercent as discount_percent,
        discountInformation.discountStatus as discount_status,
        effectiveDate as discount_change_effective_date,
        eventTime as discount_details_last_updated,

        transactionId as position_details_transaction_id,
        additionalPositionInformation.beautyLineAssignment as beauty_line_assignment,
        additionalPositionInformation.beautyLineAssignmentId as beauty_line_assignment_id,
        additionalPositionInformation.otherLineAssignment as other_line_assignment,
        CAST(additionalPositionInformation.lineAssignment as VARCHAR(2000)) as line_assignment,
        effectiveDate as line_assignment_change_effective_date,
        eventTime as line_position_details_last_updated,

        transactionId as work_contact_details_transaction_id,
        workContact.corporateEmail as corporate_email,
        workContact.workPhone as corporate_phone_number,
        effectiveDate as work_contact_change_effective_date,
        eventTime as work_contact_details_last_updated,

        jobInformation.managerEmployeeId.id as manager_worker_number

from s3_hired_details_input;
