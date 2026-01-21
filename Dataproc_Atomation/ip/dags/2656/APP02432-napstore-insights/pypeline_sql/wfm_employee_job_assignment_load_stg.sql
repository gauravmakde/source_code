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
select id as ID,
       createdAt as CREATED_AT,
       lastUpdatedAt as LAST_UPDATED_AT,
       employee.id as EMPLOYEE_ID,
       startDate as START_DATE,
       endDate as END_DATE,
       jobId as JOB_ID,
       skill as SKILL,
       departmentId as DEPARTMENT_ID,
       isPrimary as IS_PRIMARY,
       isDeleted as IS_DELETED,
       deletedAt as DELETED_AT,
       element_at(headers, 'LastUpdatedTime') as KAFKA_LAST_UPDATED_AT,
       current_timestamp()     as DW_SYS_LOAD_TMSTP,
       current_timestamp()     as DW_SYS_UPDT_TMSTP
from wfm_employee_job_assignment_input;

