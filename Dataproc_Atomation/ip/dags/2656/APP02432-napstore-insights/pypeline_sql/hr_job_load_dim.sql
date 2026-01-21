SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=hr_job_events_load_2656_napstore_insights;
Task_Name=hr_job_events_load_1_dim_table;'
FOR SESSION VOLATILE;

ET;

-- Create temporary table that has the latest unique data for each job:
CREATE VOLATILE MULTISET TABLE HR_WORKER_JOB_DIM_UNIQUE_TEMP AS (
    SELECT DISTINCT
    jobProfileId,
	case when compensationGrade='' then NULL else compensationGrade end as compensationGrade,
	case when isJobProfileExempt='' then NULL else isJobProfileExempt end as isJobProfileExempt,
	case when isJobProfileInactive='' then NULL else isJobProfileInactive end as isJobProfileInactive,
	case when jobFamily='' then NULL else jobFamily end as jobFamily,
	case when jobFamilyGroup='' then NULL else jobFamilyGroup end as jobFamilyGroup,
	case when jobTitle='' then NULL else jobTitle end as jobTitle,
	case when managementLevel='' then NULL else managementLevel end as managementLevel,
	case when workersCompensationCode='' then NULL else workersCompensationCode end as workersCompensationCode,
	lastUpdated
    FROM {db_env}_NAP_HR_STG.HR_WORKER_JOB_LDG
    qualify rank() over (partition BY jobProfileId ORDER BY lastUpdated DESC) = 1
) WITH DATA PRIMARY INDEX(jobProfileId) ON COMMIT PRESERVE ROWS;
ET;

-- Merge and update:
MERGE INTO {db_env}_NAP_HR_BASE_VWS.HR_WORKER_JOB_DIM tgt
USING HR_WORKER_JOB_DIM_UNIQUE_TEMP src
	ON (src.jobProfileId = tgt.job_profile_id)
WHEN MATCHED THEN 
UPDATE
SET
	compensation_grade = src.compensationGrade,
	is_job_profile_exempt = src.isJobProfileExempt,
	is_job_profile_inactive = src.isJobProfileInactive,
	job_family = src.jobFamily,
	job_family_group = src.jobFamilyGroup,
	job_title = src.jobTitle,
	management_level = src.managementLevel,
	workers_compensation_code = src.workersCompensationCode,
	last_updated = src.lastUpdated,
	dw_batch_date = CURRENT_DATE,
	dw_sys_load_tmstp = CURRENT_TIMESTAMP(0)
WHEN NOT MATCHED THEN 
INSERT (
	job_profile_id,
	compensation_grade,
	is_job_profile_exempt,
	is_job_profile_inactive,
	job_family,
	job_family_group,
	job_title,
	management_level,
	workers_compensation_code,
	last_updated, 
	dw_batch_date, 
	dw_sys_load_tmstp
)
VALUES (
	src.jobProfileId,
	src.compensationGrade,
	src.isJobProfileExempt,
	src.isJobProfileInactive,
	src.jobFamily,
	src.jobFamilyGroup,
	src.jobTitle,
	src.managementLevel,
	src.workersCompensationCode,
	src.lastUpdated, 
	CURRENT_DATE, 
	CURRENT_TIMESTAMP(0)
);
ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;
