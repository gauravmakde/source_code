SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=wfm_employee_job_assignment_load_2656_napstore_insights;
Task_Name=wfm_employee_job_assignment_0_load_0_stg_table;'
FOR SESSION VOLATILE;

-- Reading from kafka
-- SOURCE TOPIC NAME: customer-employee-job-assignment-analytical-avro
create
temporary view wfm_employee_job_assignment_input AS
select *
from kafka_wfm_employee_job_assignment_input;

-- Writing Kafka Data to S3 Path
insert into table wfm_employee_job_assignment_orc_output partition(year, month, day)
select *, current_date() as process_date, year(current_date()) as year,month(current_date()) as month,day(current_date()) as day
from wfm_employee_job_assignment_input;


-- Writing Kafka to Semantic Layer:
insert
overwrite table wfm_employee_job_assignment_stg_table
select id ,
       createdAt as created_at,
       lastUpdatedAt as last_updated_at,
       employee.id as employee_id,
       cast(startDate as string) as start_date,
       cast(endDate as string) as end_date,
       jobId as job_id,
       skill as skill,
       departmentId as department_id,
       cast(isPrimary as string) as is_primary,
       cast(isDeleted as string) as is_deleted,
       deletedAt as deleted_at,
       current_timestamp()     as dw_sys_load_tmstp,
       current_timestamp()     as dw_sys_updt_tmstp,
       element_at(headers, 'LastUpdatedTime') as kafka_last_updated_at,
from wfm_employee_job_assignment_input;

