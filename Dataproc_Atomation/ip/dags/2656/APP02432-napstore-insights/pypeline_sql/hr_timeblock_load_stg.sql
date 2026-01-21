SET
QUERY_BAND = '
App_ID=app02432;
DAG_ID=hr_timeblock_load_2656_napstore_insights;
Task_Name=hr_timeblock_load_load_0_stg_table;'
FOR SESSION VOLATILE;

-- Reading from kafka:
create
temporary view hr_timeblock_input AS
select *
from kafka_hr_timeblock_input;

insert
overwrite table hr_timeblock_stg_table
select referenceID                   as referenceID,
       modifiedMoment                as modifiedMoment,
       modifiedMomentUTCOffset       as modifiedMomentUTCOffset,
       workerNumber                  as workerNumber,
       jobFamily.restaurant          as jobFamily_restaurant,
       jobFamily.selling             as jobFamily_selling,
       overrideCodes.store           as overrideCodes_store,
       overrideCodes.department      as overrideCodes_department,
       inTime                        as inTime,
       outTime                       as outTime,
       inOutTimeUTCOffset            as inOutTimeUTCOffset,
       totalHours                    as totalHours,
       sellingStatus                 as sellingStatus,
       cast(punchLocationCode as string)      as punchLocationCode,
       isDeleted                     as isDeleted,
       timeEntryModified             as timeEntryModified,
       mealBreakCertificationBoolean as mealBreakCertificationBoolean,
       inTimeUTCOffset               as inTimeUTCOffset,
       outTimeUTCOffset              as OutTimeUTCOffset,
	   payrollStoreCode              as payrollStore_Code,
	   payrollDepartmentCode         as payrollDepartment_Code,
	   costCenterCode                as costCenter_Code
from hr_timeblock_input;

create
temporary view temp_hr_calculated_timeblock_input as
select referenceID                                     as referenceID,
       modifiedMoment                                  as modifiedMoment,
       modifiedMomentUTCOffset                         as modifiedMomentUTCOffset,
       workerNumber                                    as workerNumber,
       jobFamily.restaurant                            as jobFamily_restaurant,
       jobFamily.selling                               as jobFamily_selling,
       overrideCodes.store                             as overrideCodes_store,
       overrideCodes.department                        as overrideCodes_department,
       inOutTimeUTCOffset                              as inOutTimeUTCOffset,
       sellingStatus                                   as sellingStatus,
       cast(punchLocationCode as string)               as punchLocationCode,
       isDeleted                                       as isDeleted,
       timeEntryModified                               as timeEntryModified,
       mealBreakCertificationBoolean                   as mealBreakCertificationBoolean,
       explode_outer(relatedCalculatedTimeBlocksGroup) as relatedCalculatedTimeBlocksGroup
from hr_timeblock_input;

insert
overwrite table hr_calculated_timeblock_stg_table
select referenceID                                                                                as referenceID,
       modifiedMoment                                                                             as modifiedMoment,
       modifiedMomentUTCOffset                                                                    as modifiedMomentUTCOffset,
       workerNumber                                                                               as workerNumber,
       jobFamily_restaurant                                                                       as jobFamily_restaurant,
       jobFamily_selling                                                                          as jobFamily_selling,
       overrideCodes_store                                                                        as overrideCodes_store,
       overrideCodes_department                                                                   as overrideCodes_department,
       inOutTimeUTCOffset                                                                         as inOutTimeUTCOffset,
       sellingStatus                                                                              as sellingStatus,
       punchLocationCode                                                                          as punchLocationCode,
       isDeleted                                                                                  as isDeleted,
       timeEntryModified                                                                          as timeEntryModified,
       mealBreakCertificationBoolean                                                              as mealBreakCertificationBoolean,
       replace(concat_ws('|', '', relatedCalculatedTimeBlocksGroup.calculationTags, ''), ' ', '') as calculationTags,
       relatedCalculatedTimeBlocksGroup.calculatedTotalHours                                      as calculatedTotalHours,
       relatedCalculatedTimeBlocksGroup.calculatedInTime                                          as calculatedInTime,
       relatedCalculatedTimeBlocksGroup.calculatedOutTime                                         as calculatedOutTime,
       relatedCalculatedTimeBlocksGroup.calculatedInTimeUTCOffset                                 as calculatedInTimeUTCOffset,
       relatedCalculatedTimeBlocksGroup.calculatedOutTimeUTCOffset                                as calculatedOutTimeUTCOffset
from temp_hr_calculated_timeblock_input;