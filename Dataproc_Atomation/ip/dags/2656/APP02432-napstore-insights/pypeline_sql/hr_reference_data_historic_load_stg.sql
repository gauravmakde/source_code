SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=hr_reference_data_historic_load_2656_napstore_insights;
Task_Name=s3_csv_load_0_stg_tables;'
FOR SESSION VOLATILE;

-- HCW0019_Organization csv file headers:
-- ORGANIZATION_ID,PREVIOUS_ORGANIZATION_ID,ORGANIZATION_CODE,ORGANIZATION_TYPE,ORGANIZATION_NAME,ORGANIZATION_DESCRIPTION,ORGANIZATION_SUBTYPE,INACTIVE,MANAGER,LEADERSHIP_ROLE_ASSIGNEE,ORGANIZATION_OWNER,SUPERIOR_ORGANIZATION,PRIMARY_BUSINESS_SITE_REFERENCE,EFFECTIVE_DATE,LAST_UPDATE_DATE_TIME
CREATE TEMPORARY VIEW hr_org_input (
        organizationId STRING,
        previousOrganizationId STRING,
        organizationCode STRING,
        organizationType STRING,
        organizationName STRING,
        organizationDescription STRING,
        organizationSubType STRING,
        isInactive STRING,
        manager STRING,
        leadershipRoleAssignee STRING,
        organizationOwner STRING,
        superiorOrganization STRING,
        primaryBusinessSiteReference STRING,
        effectiveDate TIMESTAMP,
        lastUpdated TIMESTAMP
) USING s3selectCSV
OPTIONS (path "s3://{hr_worker_data_bucket_name}/hr-organization/input", header "true", delimiter ",");

-- organization teradata sink:
insert overwrite table td_org_ldg_table
select effectiveDate, 
        case when lower(isInactive) = 'yes' then '1' else '0' end as isInactive,
        lastUpdated, 
        organizationCode, 
        organizationDescription,
        organizationId,
        organizationName,
        organizationType
from hr_org_input;

-- HCW0021_Location csv file headers:
-- LOCATION_ID,PREV_LOCATION_ID,LOCATION_NAME,LOCATION_NUMBER,INACTIVE,TIME_ZONE,DEFAULT_CURRENCY,COUNTRY_CODE,ADDRESS_LINE_1,ADDRESS_LINE_2,ADDRESS_LINE_3,MUNICIPALITY,COUNTRY_REGION,COUNTRY_REGION_DESCRIPTION,POSTAL_CODE,AREA_PHONE_CODE,PHONE_NUMBER,LAST_UPDATED,LOCATION_TYPE
CREATE TEMPORARY VIEW hr_loc_input (
        locationId STRING,
        prevLocation STRING,
        locationName STRING,
        locationNumber STRING,
        isInactive STRING,
        timeZone STRING,
        defaultCurrency STRING,
        countryCode STRING,
        addressLine1 STRING,
        addressLine2 STRING,
        addressLine3 STRING,
        municipality STRING,
        countryRegion STRING,
        countryRegionDescription STRING,
        postalCode STRING,
        areaPhoneCode STRING,
        phoneNumber STRING,
        lastUpdated TIMESTAMP,
        locationType STRING
) USING s3selectCSV
OPTIONS (path "s3://{hr_worker_data_bucket_name}/hr-location/input", header "true", delimiter ",");

-- location teradata sink:
insert overwrite table td_loc_ldg_table
select addressLine1,
        areaPhoneCode,
        countryCode,
        countryRegion,
        countryRegionDescription,
        defaultCurrency,
        case when lower(isInactive) = 'yes' then '1' else '0' end as isInactive,
        lastUpdated,
        locationId,
        locationName,
        locationNumber,
        locationType,
        municipality,
        phoneNumber,
        postalCode,
        timeZone
from hr_loc_input;

-- HCW0017_Job csv file headers:
-- JOB_PROFILE_ID,JOB_PROFILE_PREV_ID,JOB_PROFILE_EXEMPT,JOB_TITLE,JOB_FAMILY,JOB_FAMILY_GROUP,COMPENSATION_GRADE,MANAGEMENT_LEVEL,JOB_PROFILE_INACTIVE,JOB_PROFILE_SUMMARY,JOB_CATEGORY,JOB_LEVEL,COMPANY_INSIDER_TYPE,WORKERS_COMPENSATION_CODE,LAST_UPDATED,EXTRACT_DATE_TIME
CREATE TEMPORARY VIEW hr_job_input (
        jobProfileId STRING,
        jobProfilePrevId STRING,
        isJobProfileExempt STRING,
        jobTitle STRING,
        jobFamily STRING,
        jobFamilyGroup STRING,
        compensationGrade STRING,
        managementLevel STRING,
        isJobProfileInactive STRING,
        jobProfileSummary STRING,
        jobCategory STRING,
        jobLevel STRING,
        companyInsiderType STRING,
        workersCompensationCode STRING,
        lastUpdated TIMESTAMP,
        extractDateTime TIMESTAMP
) USING s3selectCSV
OPTIONS (path "s3://{hr_worker_data_bucket_name}/hr-job/input", header "true", delimiter ",");

-- job teradata sink:
insert overwrite table td_job_ldg_table
select compensationGrade,
        case when lower(isJobProfileExempt) = 'yes' then '1' else '0' end as isJobProfileExempt,
        case when lower(isJobProfileInactive) = 'yes' then '1' else '0' end as isJobProfileInactive,
        jobFamily,
        jobFamilyGroup,
        jobProfileId,
        jobTitle,
        lastUpdated,
        managementLevel,
        workersCompensationCode
from hr_job_input;


