SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=hr_outoforder_worker_data_daily_load_2656_napstore_insights;
Task_Name=hire_cancelled_details_stage_load_10_worker_hire_cancelled_stg_tables;'
FOR SESSION VOLATILE;

-- load out of order worker data from S3 location in AVRO format to Teradata sink:
CREATE OR REPLACE TEMPORARY VIEW hr_hire_cancelled_details_input
AS select * from s3_avro_input_for_hire_cancelled_details;


-- worker teradata sink:
insert overwrite table hr_worker_v1_ldg
select  employeeId.id as worker_number,
        workerType as worker_type,
        CURRENT_TIMESTAMP as last_updated,
        transactionId as employment_status_details_transaction_id,
        effectiveDate as employment_status_change_effective_date,
        eventTime as employment_status_last_updated
from hr_hire_cancelled_details_input;

