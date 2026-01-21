SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=hr_outoforder_worker_data_daily_load_2656_napstore_insights;
Task_Name=org_details_stage_load_2_org_details_stg_tables;'
FOR SESSION VOLATILE;

-- load out of order ORG worker data from S3 location in AVRO format to Teradata sink:
CREATE OR REPLACE TEMPORARY VIEW hr_org_details_input 
AS select * from s3_avro_input_for_org_details;


-- worker teradata sink:
insert overwrite table hr_worker_v1_ldg
select  employeeId.id as worker_number,
        workerType as worker_type,
        CURRENT_TIMESTAMP as last_updated,
        transactionId as org_details_transaction_id,
        organizations.businessUnitNumber as business_unit,
        organizations.companyNumber	as company_number,
        organizations.costCenterNumber as cost_center,
        organizations.companyHierarchy as company_hierarchy,
        organizations.departmentNumber as payroll_department,
        organizations.storeNumber as payroll_store,
        organizations.regionNumber as region_number,
        effectiveDate as org_effective_date,
        eventTime as org_details_last_updated
from hr_org_details_input;

