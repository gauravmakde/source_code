SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=hr_organization_events_load_2656_napstore_insights;
Task_Name=hr_org_events_load_1_dim_tables;'
FOR SESSION VOLATILE;

ET;

-- Create temporary table that has the latest, unique data:
CREATE VOLATILE MULTISET TABLE HR_WORKER_ORG_DIM_UNIQUE_TEMP AS (
    SELECT DISTINCT
    organizationId,
	effectiveDate,
	case when isInactive='' then NULL else isInactive end as isInactive,
	lastUpdated,
	case when organizationCode='' then NULL else organizationCode end as organizationCode,
	case when organizationDescription='' then NULL else organizationDescription end as organizationDescription,
	case when organizationName='' then NULL else organizationName end as organizationName,
	case when organizationType='' then NULL else organizationType end as organizationType
    FROM {db_env}_NAP_HR_STG.HR_WORKER_ORG_LDG
    qualify rank() over (partition BY organizationId ORDER BY lastUpdated DESC) = 1
) WITH DATA PRIMARY INDEX(organizationId) ON COMMIT PRESERVE ROWS;
ET;

-- Merge and update:
MERGE INTO {db_env}_NAP_HR_BASE_VWS.HR_WORKER_ORG_DIM tgt
USING HR_WORKER_ORG_DIM_UNIQUE_TEMP src
	ON (src.organizationId = tgt.organization_id)
WHEN MATCHED THEN 
UPDATE
SET
	effective_date = src.effectiveDate,
	is_inactive = src.isInactive,
	last_updated = src.lastUpdated,
	organization_code = src.organizationCode,
	organization_description = src.organizationDescription,
	organization_name = src.organizationName,
	organization_type = src.organizationType,
	dw_batch_date = CURRENT_DATE,
	dw_sys_load_tmstp = CURRENT_TIMESTAMP(0)
WHEN NOT MATCHED THEN 
INSERT (
	organization_id, 
	effective_date, 
	is_inactive, 
	last_updated, 
	organization_code, 
	organization_description, 
	organization_name, 
	organization_type, 
	dw_batch_date, 
	dw_sys_load_tmstp
)
VALUES (
	src.organizationId,
	src.effectiveDate, 
	src.isInactive, 
	src.lastUpdated, 
	src.organizationCode, 
	src.organizationDescription, 
	src.organizationName, 
	src.organizationType, 
	CURRENT_DATE, 
	CURRENT_TIMESTAMP(0)
);

ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;
