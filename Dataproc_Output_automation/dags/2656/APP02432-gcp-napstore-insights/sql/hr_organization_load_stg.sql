SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=hr_organization_events_load_2656_napstore_insights;
Task_Name=hr_org_events_load_0_stg_tables;'
FOR SESSION VOLATILE;

-- Reading from kafka:
create temporary view hr_org_input AS select * from kafka_hr_org_input;

--- Writing to Teradata staging table:
insert overwrite table td_org_ldg_table
select  CAST((effectiveDate / 1000) AS TIMESTAMP) AS effectiveDate,
        '-0'||extract(hours from to_utc_timestamp(current_timestamp,'UTC' )-from_utc_timestamp(current_timestamp,'America/Los_Angeles' ))||":00" as effectiveDate_tz,
        CAST((lastUpdated / 1000) AS TIMESTAMP) AS lastUpdated,
		'-0'||extract(hours from to_utc_timestamp(current_timestamp,'UTC' )-from_utc_timestamp(current_timestamp,'America/Los_Angeles' ))||":00" as lastupdated_tz,
        CAST(isInactive AS STRING),
        organizationCode,
        organizationDescription,
        organizationId,
        organizationName,
        organizationType
from hr_org_input;


