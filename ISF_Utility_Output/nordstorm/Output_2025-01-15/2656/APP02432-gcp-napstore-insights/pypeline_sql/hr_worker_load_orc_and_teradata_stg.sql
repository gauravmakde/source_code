SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=hr_worker_load_v1_2656_napstore_insights;
Task_Name=hr_worker_v2_load_orc_td_stg;'
FOR SESSION VOLATILE;

--Reading Data from  Source Kafka Topic Name=linear-worker-analytical-avro
create temporary view temp_object_model AS select * from kafka_linear_worker_analytical_avro where employeeId.idType = 'EMPLOYEE_NUMBER';

-- Sink
-- Writing Kafka Data to S3 Path
insert into table hr_worker_orc_output partition(year, month, day)
select *, current_date() as process_date, year(current_date()) as year,month(current_date()) as month,day(current_date()) as day
from temp_object_model;

-- Writing Kafka to Semantic Layer Stage Table
insert overwrite table hr_worker_v1_ldg
select  employeeId.id as worker_number,
        workerType as worker_type,
        lastUpdated as last_updated,

        hireStatusDetails.transactionId as hire_status_details_transaction_id,
        hireStatusDetails.hireStatus as hire_status,
        hireStatusDetails.effectiveDate as hire_status_change_effective_date,
        hireStatusDetails.lastUpdated as hire_status_last_updated,

        lanIdDetails.transactionId as lanId_details_transaction_id,
        lanIdDetails.lanId as lanId,
        lanIdDetails.effectiveDate as lanId_effective_date,
        lanIdDetails.lastUpdated as lanId_last_updated,

        nameDetails.transactionId as worker_name_details_transaction_id,
        cast(decode(encode(nameDetails.name.firstName, 'US-ASCII'), 'US-ASCII') as string) as first_name,
        cast(decode(encode(nameDetails.name.lastName, 'US-ASCII'), 'US-ASCII') as string) as last_name,
        cast(decode(encode(nameDetails.name.middleName, 'US-ASCII'), 'US-ASCII') as string) as middle_name,
        cast(decode(encode(nameDetails.name.preferredLastName, 'US-ASCII'), 'US-ASCII') as string) as preferred_last_name,
        cast(decode(encode(nameDetails.name.preferredFirstName, 'US-ASCII'), 'US-ASCII') as string) as preferred_first_name,
        nameDetails.effectiveDate as name_change_effective_date,
        nameDetails.lastUpdated as name_change_last_updated,

        employmentStatusDetails.transactionId as employment_status_details_transaction_id,
        employmentStatusDetails.employmentStatus.contingentWorkerProjectedEndDate as contingent_worker_projected_end_date,
        employmentStatusDetails.employmentStatus.hireDate as hire_date,
        employmentStatusDetails.employmentStatus.originalHireDate as original_hire_date,
        employmentStatusDetails.employmentStatus.terminationDate as termination_date,
        employmentStatusDetails.employmentStatus.workerStatus as worker_status,
        employmentStatusDetails.effectiveDate as employment_status_change_effective_date,
        employmentStatusDetails.lastUpdated as employment_status_last_updated,

        jobDetails.transactionId as job_details_transaction_id,
        jobDetails.jobInformation.commissionPlanName as commission_plan_name,
        jobDetails.jobInformation.compensationInformation.compensationCurrencyCode as compensation_currency,
        CAST(jobDetails.jobInformation.isJobExempt as VARCHAR(100)) as is_job_exempt,
        CAST(jobDetails.jobInformation.isRemoteWorker as VARCHAR(100)) as is_remote_worker,
        jobDetails.jobInformation.jobProfileId as job_profile_id,
        jobDetails.jobInformation.locationNumber as location_number,
        jobDetails.jobInformation.payRateType as pay_rate_type,
        jobDetails.jobInformation.workerSubtype as worker_sub_type,
        jobDetails.effectiveDate as job_details_change_effective_date,
        jobDetails.lastUpdated as job_details_last_updated,

        contingentWorkerDetails.transactionId as contingent_worker_details_transaction_id,
        CAST(contingentWorkerDetails.contingentWorkerInformation.isOnsite as VARCHAR(100)) as is_onsite,
        contingentWorkerDetails.contingentWorkerInformation.vendorName as contingent_worker_vendor_name,
        contingentWorkerDetails.contingentWorkerInformation.vendorNumber as contingent_worker_vendor_number,
        contingentWorkerDetails.effectiveDate as contingent_worker_change_effective_date,
        contingentWorkerDetails.lastUpdated as contingent_worker_last_updated,

        organizationDetails.transactionId as org_details_transaction_id,
        organizationDetails.organizations.businessUnitNumber as business_unit,
        organizationDetails.organizations.companyNumber	as company_number,
        organizationDetails.organizations.costCenterNumber as cost_center,
        organizationDetails.organizations.companyHierarchy as company_hierarchy,
        organizationDetails.organizations.departmentNumber as payroll_department,
        organizationDetails.organizations.storeNumber as payroll_store,
        organizationDetails.organizations.regionNumber as region_number,
        organizationDetails.effectiveDate as org_effective_date,
        organizationDetails.lastUpdated as org_details_last_updated,

        discountDetails.transactionId as discount_details_transaction_id,
        CAST(discountDetails.discountInformation.discountPercent AS CHAR(3)) as discount_percent,
        discountDetails.discountInformation.discountStatus as discount_status,
        discountDetails.effectiveDate as discount_change_effective_date,
        discountDetails.lastUpdated as discount_details_last_updated,

        positionDetails.transactionId as position_details_transaction_id,
        positionDetails.additionalPositionInformation.beautyLineAssignment as beauty_line_assignment,
        positionDetails.additionalPositionInformation.beautyLineAssignmentId as beauty_line_assignment_id,
        positionDetails.additionalPositionInformation.otherLineAssignment as other_line_assignment,
        CAST(positionDetails.additionalPositionInformation.lineAssignment as VARCHAR(2000)) as line_assignment,
        positionDetails.effectiveDate as line_assignment_change_effective_date,
        positionDetails.lastUpdated as line_position_details_last_updated,

        workContactDetails.transactionId as work_contact_details_transaction_id,
        workContactDetails.workContact.corporateEmail as corporate_email,
        workContactDetails.workContact.workPhone as corporate_phone_number,
        workContactDetails.effectiveDate as work_contact_change_effective_date,
        workContactDetails.lastUpdated as work_contact_details_last_updated,

        managerDetails.transactionId as manager_details_transaction_id,
        jobDetails.jobInformation.managerEmployeeId.id as manager_worker_number,
        managerDetails.effectiveDate as manager_change_effective_date,
        managerDetails.lastUpdated as manager_details_last_updated
from temp_object_model;



