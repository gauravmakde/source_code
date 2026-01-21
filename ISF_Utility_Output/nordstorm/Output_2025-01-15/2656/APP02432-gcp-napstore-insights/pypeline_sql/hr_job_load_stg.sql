SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=hr_job_events_load_2656_napstore_insights;
Task_Name=hr_job_events_load_0_stg_table;'
FOR SESSION VOLATILE;

-- Reading from kafka: humanresources-job-changed-avro:
create temporary view hr_job_input AS select * from kafka_hr_job_input;

-- Writing Kafka to Semantic Layer:
insert overwrite table td_job_ldg_table
select 
compensationGrade,
isJobProfileExempt,
isJobProfileInactive,
jobFamily,
jobFamilyGroup,
jobProfileId,
jobTitle,
CAST((lastUpdated / 1000) AS TIMESTAMP) AS lastUpdated,
managementLevel,
workersCompensationCode
from hr_job_input;


