CREATE TEMPORARY TABLE IF NOT EXISTS HR_WORKER_JOB_DIM_UNIQUE_TEMP
AS
SELECT DISTINCT jobprofileid,
  CASE
  WHEN LOWER(compensationgrade) = LOWER('')
  THEN NULL
  ELSE compensationgrade
  END AS compensationgrade,
  CASE
  WHEN LOWER(isjobprofileexempt) = LOWER('')
  THEN NULL
  ELSE isjobprofileexempt
  END AS isjobprofileexempt,
  CASE
  WHEN LOWER(isjobprofileinactive) = LOWER('')
  THEN NULL
  ELSE isjobprofileinactive
  END AS isjobprofileinactive,
  CASE
  WHEN LOWER(jobfamily) = LOWER('')
  THEN NULL
  ELSE jobfamily
  END AS jobfamily,
  CASE
  WHEN LOWER(jobfamilygroup) = LOWER('')
  THEN NULL
  ELSE jobfamilygroup
  END AS jobfamilygroup,
  CASE
  WHEN LOWER(jobtitle) = LOWER('')
  THEN NULL
  ELSE jobtitle
  END AS jobtitle,
  CASE
  WHEN LOWER(managementlevel) = LOWER('')
  THEN NULL
  ELSE managementlevel
  END AS managementlevel,
  CASE
  WHEN LOWER(workerscompensationcode) = LOWER('')
  THEN NULL
  ELSE workerscompensationcode
  END AS workerscompensationcode,
 lastupdated,
 JWN_UDF.UDF_TIME_ZONE(lastupdated) AS lastupdated_tz
FROM {{params.dbenv}}_NAP_HR_STG.HR_WORKER_JOB_LDG
QUALIFY (RANK() OVER (PARTITION BY jobprofileid ORDER BY lastupdated DESC)) = 1;




MERGE INTO {{params.dbenv}}_NAP_HR_DIM.HR_WORKER_JOB_DIM AS tgt
USING HR_WORKER_JOB_DIM_UNIQUE_TEMP AS src
ON LOWER(src.jobprofileid) = LOWER(tgt.job_profile_id)
WHEN MATCHED THEN UPDATE SET 
compensation_grade = src.compensationgrade, 
is_job_profile_exempt = src.isjobprofileexempt, 
is_job_profile_inactive = src.isjobprofileinactive, 
job_family = src.jobfamily, 
job_family_group = src.jobfamilygroup, 
job_title = src.jobtitle, 
management_level = src.managementlevel, 
workers_compensation_code = src.workerscompensationcode, 
last_updated = src.lastupdated, 
last_updated_tz = src.lastupdated_tz, 
dw_batch_date = CURRENT_DATE('PST8PDT'), 
dw_sys_load_tmstp = current_datetime('PST8PDT')
WHEN NOT MATCHED THEN INSERT
(compensation_grade
,is_job_profile_exempt
,is_job_profile_inactive
,job_family
,job_family_group
,job_profile_id
,job_title
,last_updated
,last_updated_tz
,management_level
,workers_compensation_code
,dw_batch_date
,dw_sys_load_tmstp)
VALUES
(src.compensationgrade, 
src.isjobprofileexempt, 
src.isjobprofileinactive, 
src.jobfamily, 
src.jobfamilygroup, 
src.jobprofileid, 
src.jobtitle, 
src.lastupdated,
src.lastupdated_tz, 
src.managementlevel, 
src.workerscompensationcode,
CURRENT_DATE('PST8PDT'), 
current_datetime('PST8PDT'));



