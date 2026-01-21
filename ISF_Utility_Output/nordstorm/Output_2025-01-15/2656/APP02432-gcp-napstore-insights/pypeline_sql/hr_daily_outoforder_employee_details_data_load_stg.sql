SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=hr_outoforder_worker_data_daily_load_2656_napstore_insights;
Task_Name=employee_details_stage_load_0_employee_details_stg_tables;'
FOR SESSION VOLATILE;

-- load out of order worker data from S3 location in AVRO format to Teradata sink:
CREATE OR REPLACE TEMPORARY VIEW hr_employee_details_input 
AS select * from s3_avro_input_for_employee_details;


-- worker teradata sink:
insert overwrite table hr_worker_v1_ldg
select  employeeId.id as worker_number,
        workerType as worker_type,
        CURRENT_TIMESTAMP as last_updated,
        transactionId as employment_status_details_transaction_id,
        employmentStatus.contingentWorkerProjectedEndDate as contingent_worker_projected_end_date,
        employmentStatus.hireDate as hire_date,
        employmentStatus.originalHireDate as original_hire_date,
        employmentStatus.terminationDate as termination_date,
        employmentStatus.workerStatus as worker_status,
        effectiveDate as employment_status_change_effective_date,
        eventTime as employment_status_last_updated
from hr_employee_details_input;

