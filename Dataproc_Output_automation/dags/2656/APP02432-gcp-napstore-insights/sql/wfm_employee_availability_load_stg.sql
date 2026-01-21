SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=wfm_employee_availability_load_2656_napstore_insights;
Task_Name=wfm_employee_availability_load_0_stg_table;'
FOR SESSION VOLATILE;

-- Reading from kafka:
create
temporary view wfm_employee_availability_input AS
select *
from kafka_wfm_employee_availability_input;

-- Writing Kafka Data to S3 Path
-- insert into table wfm_employee_availability_orc_output partition(year, month, day)
-- select *, current_date() as process_date, year (current_date ()) as year, month (current_date ()) as month, day (current_date ()) as day
-- from wfm_employee_availability_input;

-- Writing Kafka to Semantic Layer:
insert
overwrite table wfm_employee_availability_stg_table
select id                       as ID,
       employee.id              as EMPLOYEE_ID,
       startDate                as START_DATE,
       endDate                  as END_DATE,
       localStartTime           as LOCAL_START_TIME,
       localEndTime             as LOCAL_END_TIME,
       numberOfWeeks            as NUMBER_OF_WEEKS,
       cycleBaseDate            as CYCLE_BASE_DATE,
       isCanceled               as IS_CANCELED,
       ifnull(canceledAt, NULL) as CANCELED_AT,
       createdAt                as CREATED_AT,
       lastUpdatedAt            as LAST_UPDATED_AT,
       element_at(headers, 'LastUpdatedTime') as KAFKA_LAST_UPDATED_AT,
       current_timestamp()      as DW_SYS_LOAD_TMSTP,
       current_timestamp()      as DW_SYS_UPDT_TMSTP
from wfm_employee_availability_input;

create
temporary view wfm_employee_availability_details_input AS
select id as employeeAvailabilityId, element_at(headers, 'LastUpdatedTime') as LastUpdatedTime,
       explode_outer(employeeAvailabilityDetails) as employeeAvailabilityDetails
from wfm_employee_availability_input;

insert
overwrite table wfm_employee_availability_details_stg_table
select employeeAvailabilityDetails.id                          as ID,
       employeeAvailabilityId                                  as EMPLOYEE_AVAILABILITY_ID,
       employeeAvailabilityDetails.startTime                   as START_TIME,
       employeeAvailabilityDetails.endTime                     as END_TIME,
       employeeAvailabilityDetails.effectiveDay                as EFFECTIVE_DAY,
       employeeAvailabilityDetails.weekNumber                  as WEEK_NUMBER,
       employeeAvailabilityDetails.availabilityDetailType      as AVAILABILITY_DETAIL_TYPE,
       employeeAvailabilityDetails.createdAt                   as CREATED_AT,
       employeeAvailabilityDetails.lastUpdatedAt               as LAST_UPDATED_AT,
       LastUpdatedTime                                         as KAFKA_LAST_UPDATED_AT,
       cast(employeeAvailabilityDetails.isDeleted as CHAR(15)) as IS_DELETED,
       ifnull(employeeAvailabilityDetails.deletedAt, NULL)     as DELETED_AT,
       current_timestamp()                                     as DW_SYS_LOAD_TMSTP,
       current_timestamp()                                     as DW_SYS_UPDT_TMSTP
from wfm_employee_availability_details_input
where employeeAvailabilityDetails.id is not null;

create
temporary view wfm_employee_fixed_shift_details_input AS
select id as employeeAvailabilityId, element_at(headers, 'LastUpdatedTime') as LastUpdatedTime,
       explode_outer(employeeFixedShiftDetails) as employeeFixedShiftDetails
from wfm_employee_availability_input;

insert
overwrite table wfm_employee_fixed_shift_details_stg_table
select employeeFixedShiftDetails.id                          as ID,
       employeeAvailabilityId                                as EMPLOYEE_AVAILABILITY_ID,
       employeeFixedShiftDetails.effectiveDay                as EFFECTIVE_DAY,
       employeeFixedShiftDetails.weekNumber                  as WEEK_NUMBER,
       employeeFixedShiftDetails.createdAt                   as CREATED_AT,
       employeeFixedShiftDetails.lastUpdatedAt               as LAST_UPDATED_AT,
       LastUpdatedTime                                       as KAFKA_LAST_UPDATED_AT,
       cast(employeeFixedShiftDetails.isDeleted as CHAR(15)) as IS_DELETED,
       ifnull(employeeFixedShiftDetails.deletedAt, NULL)     as DELETED_AT,
       current_timestamp()                                   as DW_SYS_LOAD_TMSTP,
       current_timestamp()                                   as DW_SYS_UPDT_TMSTP
from wfm_employee_fixed_shift_details_input
where employeeFixedShiftDetails.id is not null;

create
temporary view wfm_employee_fixed_shift_job_segment_details_input AS
select employeeFixedShiftDetails.id                                                 as employeeFixedShiftDetailsId,
       LastUpdatedTime,
       explode_outer(employeeFixedShiftDetails.employeeFixedShiftJobSegmentDetails) as employeeFixedShiftJobSegmentDetails
from wfm_employee_fixed_shift_details_input;

insert
overwrite table wfm_employee_fixed_shift_job_segment_details_stg_table
select employeeFixedShiftJobSegmentDetails.id                          as ID,
       employeeFixedShiftDetailsId                                     as EMPLOYEE_FIXED_SHIFT_DETAILS_ID,
       employeeFixedShiftJobSegmentDetails.jobId                       as JOB_ID,
       employeeFixedShiftJobSegmentDetails.startTime                   as START_TIME,
       employeeFixedShiftJobSegmentDetails.endTime                     as END_TIME,
       employeeFixedShiftJobSegmentDetails.createdAt                   as CREATED_AT,
       employeeFixedShiftJobSegmentDetails.lastUpdatedAt               as LAST_UPDATED_AT,
       LastUpdatedTime                                                 as KAFKA_LAST_UPDATED_AT,
       cast(employeeFixedShiftJobSegmentDetails.isDeleted as CHAR(15)) as IS_DELETED,
       ifnull(employeeFixedShiftJobSegmentDetails.deletedAt, NULL)     as DELETED_AT,
       current_timestamp()                                             as DW_SYS_LOAD_TMSTP,
       current_timestamp()                                             as DW_SYS_UPDT_TMSTP
from wfm_employee_fixed_shift_job_segment_details_input
where employeeFixedShiftJobSegmentDetails.id is not null;