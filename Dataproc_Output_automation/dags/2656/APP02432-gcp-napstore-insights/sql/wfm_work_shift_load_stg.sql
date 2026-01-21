SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=wfm_work_shift_load_2656_napstore_insights;
Task_Name=wfm_work_shift_0_load_0_stg_table;'
FOR SESSION VOLATILE;

create temporary view wfm_work_shift_input as
select *
from kafka_wfm_work_shift_input;

-- Writing Kafka Data to S3 Path
insert into table wfm_work_shift_orc_output partition(year, month, day)
select *, current_date() as process_date, year(current_date()) as year,month(current_date()) as month,day(current_date()) as day
from wfm_work_shift_input;

-- Writing Kafka to Semantic Layer:
insert overwrite table wfm_shift_stg_table
select id                      as ID,
       employee.id             as EMPLOYEE_ID,
       storeNumber             as STORE_NUMBER,
       startDate               as START_DATE,
       localStartTime          as LOCAL_START_TIME,
       endDate                 as END_DATE,
       localEndTime            as LOCAL_END_TIME,
       reason                  as REASON,
       deleted                 as DELETED,
       ifnull(deletedAt, NULL) as DELETED_AT,
       createdAt               as CREATED_AT,
       lastUpdatedAt           as LAST_UPDATED_AT,
       cast(decode(encode(lastModifiedUser, 'US-ASCII'), 'US-ASCII') as string)  as LAST_MODIFIED_USER,
       lastModifiedTimestamp   as LAST_MODIFIED_TIMESTAMP,
       element_at(headers, 'LastUpdatedTime') as KAFKA_LAST_UPDATED_AT,
       current_timestamp()     as DW_SYS_LOAD_TMSTP,
       current_timestamp()     as DW_SYS_UPDT_TMSTP
from wfm_work_shift_input;

create temporary view wfm_work_shift_job_input as
select id as shiftId, element_at(headers, 'LastUpdatedTime') as LastUpdatedTime, explode_outer(shiftJobs) as shiftJobs
from wfm_work_shift_input;

insert overwrite table wfm_shift_job_stg_table
select shiftJobs.id                       as ID,
       shiftId                            as SHIFT_ID,
       shiftJobs.jobId                    as JOB_ID,
       shiftJobs.startDate                as START_DATE,
       shiftJobs.localStartTime           as LOCAL_START_TIME,
       shiftJobs.endDate                  as END_DATE,
       shiftJobs.localEndTime             as LOCAL_END_TIME,
       shiftJobs.reason                   as REASON,
       cast(shiftJobs.deleted as BOOLEAN) as DELETED,
       ifnull(shiftJobs.deletedAt, NULL)  as DELETED_AT,
       shiftJobs.createdAt                as CREATED_AT,
       shiftJobs.lastUpdatedAt            as LAST_UPDATED_AT,
       cast(decode(encode(shiftJobs.lastModifiedUser, 'US-ASCII'), 'US-ASCII') as string)  as LAST_MODIFIED_USER,
       shiftJobs.lastModifiedTimestamp    as LAST_MODIFIED_TIMESTAMP,
       LastUpdatedTime                    as KAFKA_LAST_UPDATED_AT,
       current_timestamp()                as DW_SYS_LOAD_TMSTP,
       current_timestamp()                as DW_SYS_UPDT_TMSTP
from wfm_work_shift_job_input
where shiftJobs.id is not null;

create temporary view wfm_work_shift_job_details_input as
select shiftJobs.id as shiftJobId, shiftJobs.lastUpdatedAt as lastUpdatedAt,
       LastUpdatedTime as LastUpdatedTime, explode_outer(shiftJobs.details) as details
from wfm_work_shift_job_input;

insert overwrite table wfm_shift_job_detail_stg_table
select shiftJobId                 as SHIFT_JOB_ID,
       details.shiftJobDetailType as SHIFT_JOB_DETAIL_TYPE,
       details.startDate          as START_DATE,
       details.localStartTime     as LOCAL_START_TIME,
       details.endDate            as END_DATE,
       details.localEndTime       as LOCAL_END_TIME,
       lastUpdatedAt              as LAST_UPDATED_AT,
       LastUpdatedTime            as KAFKA_LAST_UPDATED_AT,
       current_timestamp()        as DW_SYS_LOAD_TMSTP,
       current_timestamp()        as DW_SYS_UPDT_TMSTP
from wfm_work_shift_job_details_input
where details.shiftJobDetailType is not null;
