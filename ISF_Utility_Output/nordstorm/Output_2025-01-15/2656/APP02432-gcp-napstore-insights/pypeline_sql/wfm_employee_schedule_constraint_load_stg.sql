SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=wfm_employee_schedule_constraint_load_2656_napstore_insights;
Task_Name=wfm_employee_schedule_constraint_load_0_stg_table;'
FOR SESSION VOLATILE;

-- Reading from kafka:
create
temporary view wfm_employee_schedule_constraint_input AS
select *
from kafka_wfm_employee_schedule_constraint_input;

-- Writing Kafka Data to S3 Path
insert into table wfm_employee_schedule_constraint_orc_output partition(year, month, day)
select *,
       current_date() as process_date, year (current_date ()) as year, month (current_date ()) as month, day (current_date ()) as day
from wfm_employee_schedule_constraint_input;

-- Writing Kafka to Semantic Layer:
insert
overwrite table wfm_employee_schedule_constraint_stg_table
select id                                       as ID,
       employee.id                              as EMPLOYEE_ID,
       startDate                                as START_DATE,
       endDate                                  as END_DATE,
       canWorkSplitShift                        as CAN_WORK_SPLIT_SHIFT,
       mustStrictlyEnforceMinHoursBetweenShifts as MUST_SCTRICTLY_ENFORCE_MIN_HOURS_BETWEEN_SHIFTS,
       maxConsecutiveDays                       as MAX_CONSECUTIVE_DAYS,
       maxConsecutiveDaysAcrossWeeks            as MAX_CONSECUTIVE_DAYS_ACROSS_WEEKS,
       maxDaysPerWeek                           as MAX_DAYS_PER_WEEK,
       minimumMinutesBetweenShifts              as MINIMUM_MINUTES_BETWEEN_SHIFTS,
       minimumMinutesPerWeek                    as MINIMUM_MINUTES_PER_WEEK,
       minimumMinutesSunday                     as MINIMUM_MINUTES_SUNDAY,
       minimumMinutesMonday                     as MINIMUM_MINUTES_MONDAY,
       minimumMinutesTuesday                    as MINIMUM_MINUTES_TUESDAY,
       minimumMinutesWednesday                  as MINIMUM_MINUTES_WEDNESDAY,
       minimumMinutesThursday                   as MINIMUM_MINUTES_THURSDAY,
       minimumMinutesFriday                     as MINIMUM_MINUTES_FRIDAY,
       minimumMinutesSaturday                   as MINIMUM_MINUTES_SATURDAY,
       maximumMinutesPerWeek                    as MAXIMUM_MINUTES_PER_WEEK,
       maximumMinutesSunday                     as MAXIMUM_MINUTES_SUNDAY,
       maximumMinutesMonday                     as MAXIMUM_MINUTES_MONDAY,
       maximumMinutesTuesday                    as MAXIMUM_MINUTES_TUESDAY,
       maximumMinutesWednesday                  as MAXIMUM_MINUTES_WEDNESDAY,
       maximumMinutesThursday                   as MAXIMUM_MINUTES_THURSDAY,
       maximumMinutesFriday                     as MAXIMUM_MINUTES_FRIDAY,
       maximumMinutesSaturday                   as MAXIMUM_MINUTES_SATURDAY,
       isDeleted                                as IS_DELETED,
       ifnull(deletedAt, NULL)                  as DELETED_AT,
       createdAt                                as CREATED_AT,
       lastUpdatedAt                            as LAST_UPDATED_AT,
       element_at(headers, 'LastUpdatedTime')   as KAFKA_LAST_UPDATED_AT,
       current_timestamp()                      as DW_SYS_LOAD_TMSTP,
       current_timestamp()                      as DW_SYS_UPDT_TMSTP
from wfm_employee_schedule_constraint_input;