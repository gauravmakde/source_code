SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=wfm_time_off_request_load_2656_napstore_insights;
Task_Name=wfm_time_off_request_load_0_stg_table;'
FOR SESSION VOLATILE;

-- Reading from kafka:
create
temporary view wfm_time_off_request_input AS
select *
from kafka_wfm_time_off_request_input;

-- Writing Kafka Data to S3 Path
insert into table wfm_time_off_request_orc_output partition(year, month, day)
select *, current_date() as process_date, year (current_date ()) as year, month (current_date ()) as month, day (current_date ()) as day
from wfm_time_off_request_input;

-- Writing Kafka to Semantic Layer:
insert
overwrite table wfm_time_off_request_stg_table
select id                       as ID,
       employee.id              as EMPLOYEE_ID,
       timeOffRequestStatus     as TIME_OFF_REQUEST_STATUS,
       startDate                as START_DATE,
       endDate                  as END_DATE,
       localStartTime           as LOCAL_START_TIME,
       localEndTime             as LOCAL_END_TIME,
       isCanceled               as IS_CANCELED,
       ifnull(canceledAt, NULL) as CANCELED_AT,
       createdAt                as CREATED_AT,
       lastUpdatedAt            as LAST_UPDATED_AT,
       element_at(headers, 'LastUpdatedTime') as KAFKA_LAST_UPDATED_AT,
       current_timestamp()      as DW_SYS_LOAD_TMSTP,
       current_timestamp()      as DW_SYS_UPDT_TMSTP
from wfm_time_off_request_input;

create
temporary view wfm_time_off_request_details_input AS
select id as timeOffRequestId,lastUpdatedAt as lastUpdatedAt, element_at(headers, 'LastUpdatedTime') as LastUpdatedTime,
       explode_outer(timeOffRequestDetails) as timeOffRequestDetails
from wfm_time_off_request_input;

insert
overwrite table wfm_time_off_request_details_stg_table
select timeOffRequestId                    as TIME_OFF_REQUEST_ID,
       timeOffRequestDetails.minutes       as MINUTES,
       timeOffRequestDetails.effectiveDate as EFFECTIVE_DATE,
       lastUpdatedAt                       as LAST_UPDATED_AT,
       LastUpdatedTime                     as KAFKA_LAST_UPDATED_AT,
       current_timestamp()                 as DW_SYS_LOAD_TMSTP,
       current_timestamp()                 as DW_SYS_UPDT_TMSTP
from wfm_time_off_request_details_input
where timeOffRequestDetails.minutes is not null;
