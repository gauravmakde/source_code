SET
QUERY_BAND = '
App_ID=app02432;
DAG_ID=hr_timeblock_load_2656_napstore_insights;
Task_Name=hr_timeblock_load_load_1_fct_table;'
FOR SESSION VOLATILE;

ET;

DELETE
FROM {db_env}_NAP_HR_BASE_VWS.HR_TIMEBLOCK_FACT TGT
WHERE EXISTS
    (SELECT *
    FROM {db_env}_NAP_HR_BASE_VWS.HR_TIMEBLOCK_LDG SRC
    WHERE SRC.referenceID = TGT.reference_id
  AND SRC.modifiedMoment >= TGT.modified_moment );

INSERT INTO {db_env}_NAP_HR_BASE_VWS.HR_TIMEBLOCK_FACT
(reference_id,
 modified_moment,
 modified_moment_utc_offset,
 worker_number,
 job_family_restaurant,
 job_family_selling,
 override_codes_store,
 override_codes_department,
 in_time,
 out_time,
 in_out_time_utc_offset,
 total_hours,
 selling_status,
 punch_location_code,
 is_deleted,
 time_entry_modified,
 meal_break_certification_boolean,
 in_time_utc_offset,
 out_time_utc_offset,
 payroll_store_code,
 payroll_department_code,
 cost_center_code,
 dw_batch_date,
 dw_sys_load_tmstp)
SELECT reference_id,
       modified_moment,
       modified_moment_utc_offset,
       worker_number,
       job_family_restaurant,
       job_family_selling,
       override_codes_store,
       override_codes_department,
       in_time,
       out_time,
       in_out_time_utc_offset,
       total_hours,
       selling_status,
       punch_location_code,
       is_deleted,
       time_entry_modified,
       meal_break_certification_boolean,
       in_time_utc_offset,
       out_time_utc_offset,
	   payroll_store_code,
	   payroll_department_code,
	   cost_center_code,
       dw_batch_date,
       dw_sys_load_tmstp
FROM (SELECT DISTINCT referenceID                                  AS reference_id,
                      modifiedMoment                               AS modified_moment,
                      modifiedMomentUTCOffset                      AS modified_moment_utc_offset,
                      workerNumber                                 AS worker_number,
                      jobFamily_restaurant                         AS job_family_restaurant,
                      jobFamily_selling                            AS job_family_selling,
                      overrideCodes_store                          AS override_codes_store,
                      overrideCodes_department                     AS override_codes_department,
                      inTime                                       AS in_time,
                      outTime                                      AS out_time,
                      inOutTimeUTCOffset                           AS in_out_time_utc_offset,
                      totalHours                                   AS total_hours,
                      sellingStatus                                AS selling_status,
                      punchLocationCode                            AS punch_location_code,
                      TRIM(isDeleted)                              AS is_deleted,
                      timeEntryModified                            AS time_entry_modified,
                      mealBreakCertificationBoolean                AS meal_break_certification_boolean,
                      inTimeUTCOffset                              as in_time_utc_offset,
                      OutTimeUTCOffset                             as out_time_utc_offset,
					  payrollStore_Code              			   as payroll_store_code,
				      payrollDepartment_Code                       as payroll_department_code,
	                  costCenter_Code                              as cost_center_code,
                      CURRENT_DATE                                 AS dw_batch_date, -- (SELECT CURR_BATCH_DATE FROM {db_env}_NAP_HR_BASE_VWS.ELT_CONTROL WHERE Subject_Area_Nm ='@SUBJ_AREA@' ) AS dw_batch_date
                      CURRENT_TIMESTAMP(0)                         AS dw_sys_load_tmstp,
                      MAX(modifiedMoment)                             OVER (partition by referenceID) AS max_modified_moment
      FROM {db_env}_NAP_HR_BASE_VWS.HR_TIMEBLOCK_LDG) WINDOWED
WHERE WINDOWED.modified_moment = WINDOWED.max_modified_moment;

ET;

DELETE
FROM {db_env}_NAP_HR_BASE_VWS.HR_CALCULATED_TIMEBLOCK_FACT TGT
WHERE EXISTS
    (SELECT *
    FROM {db_env}_NAP_HR_BASE_VWS.HR_CALCULATED_TIMEBLOCK_LDG SRC
    WHERE SRC.referenceID = TGT.reference_id
  AND SRC.modifiedMoment >= TGT.modified_moment);

INSERT INTO {db_env}_NAP_HR_BASE_VWS.HR_CALCULATED_TIMEBLOCK_FACT
(reference_id,
 modified_moment,
 worker_number,
 in_out_time_utc_offset,
 is_deleted,
 calculation_tags,
 calculated_total_hours,
 calculated_in_time,
 calculated_out_time,
 calculated_in_time_utc_offset,
 calculated_out_time_utc_offset,
 dw_batch_date,
 dw_sys_load_tmstp)
SELECT reference_id,
       modified_moment,
       worker_number,
       in_out_time_utc_offset,
       is_deleted,
       calculation_tags,
       calculated_total_hours,
       calculated_in_time,
       calculated_out_time,
       calculated_in_time_utc_offset,
       calculated_out_time_utc_offset,
       dw_batch_date,
       dw_sys_load_tmstp
FROM (SELECT DISTINCT referenceID                                  AS reference_id,
                      modifiedMoment                               AS modified_moment,
                      workerNumber                                 AS worker_number,
                      inOutTimeUTCOffset                           AS in_out_time_utc_offset,
                      TRIM(isDeleted)                              AS is_deleted,
                      calculationTags                              AS calculation_tags,
                      calculatedTotalHours                         AS calculated_total_hours,
                      calculatedInTime                             AS calculated_in_time,
                      calculatedOutTime                            AS calculated_out_time,
                      calculatedInTimeUTCOffset                    as calculated_in_time_utc_offset,
                      calculatedOutTimeUTCOffset                   as calculated_out_time_utc_offset,
                      CURRENT_DATE                                 AS dw_batch_date, -- (SELECT CURR_BATCH_DATE FROM {db_env}_NAP_BASE_VWS.ELT_CONTROL WHERE Subject_Area_Nm ='@SUBJ_AREA@' ) AS dw_batch_date
                      CURRENT_TIMESTAMP(0)                         AS dw_sys_load_tmstp,
                      MAX(modifiedMoment)                             OVER (partition by referenceID) AS max_modified_moment
      FROM {db_env}_NAP_HR_BASE_VWS.HR_CALCULATED_TIMEBLOCK_LDG) WINDOWED
WHERE WINDOWED.modified_moment = WINDOWED.max_modified_moment;

ET;

SET
QUERY_BAND = NONE FOR SESSION;

ET;
