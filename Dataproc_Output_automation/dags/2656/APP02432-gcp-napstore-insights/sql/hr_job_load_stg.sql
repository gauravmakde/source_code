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
CAST(isJobProfileExempt AS STRING),
CAST(isJobProfileInactive AS STRING),
jobFamily,
jobFamilyGroup,
jobProfileId,
jobTitle,
CAST((lastUpdated / 1000) AS TIMESTAMP) AS lastUpdated,
'-0'||extract(hours from to_utc_timestamp(current_timestamp,'UTC' )-from_utc_timestamp(current_timestamp,'America/Los_Angeles' ))||":00" as lastupdated_tz,
managementLevel,
workersCompensationCode
from hr_job_input;