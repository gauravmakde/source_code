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
        CAST((lastUpdated / 1000) AS TIMESTAMP) AS lastUpdated,
        isInactive,
        organizationCode,
        organizationDescription,
        organizationId,
        organizationName,
        organizationType
from hr_org_input;


