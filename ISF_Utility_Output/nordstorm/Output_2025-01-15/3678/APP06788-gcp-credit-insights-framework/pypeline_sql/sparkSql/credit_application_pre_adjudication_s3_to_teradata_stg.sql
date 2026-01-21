SET QUERY_BAND = '
App_ID=app06788;
DAG_ID=credit_application_pre_adjudication;
Task_Name=s3_csv_to_teradata_stg_job;'

-- noqa: disable=all
FOR SESSION VOLATILE;
-- noqa: enable=all

CREATE TEMPORARY TABLE credit_application_pre_adjudication_temp USING CSV
OPTIONS (
    path 's3://{splunk_data_sync_bucket}/csv/', header 'true'
);

INSERT INTO TABLE credit_application_pre_adjudication_stg
SELECT
    AppID AS app_id,
    ZootAppID AS zoot_app_id,
    Status AS status
FROM credit_application_pre_adjudication_temp;
