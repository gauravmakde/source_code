SET QUERY_BAND = '
App_ID=app06788;
DAG_ID=credit_application_pre_adjudication;
Task_Name=teradata_stg_to_teradata_job;'

-- noqa: disable=all
FOR SESSION VOLATILE;
-- noqa: enable=all

END TRANSACTION;

DELETE FROM {splunk_data_sync_db}.credit_application_pre_adjudication_stg
WHERE (
    app_id
)
IN (
    SELECT app_id
    FROM {splunk_data_sync_db}.credit_application_pre_adjudication
);

INSERT INTO {splunk_data_sync_db}.credit_application_pre_adjudication
SELECT *
FROM {splunk_data_sync_db}.credit_application_pre_adjudication_stg;

DELETE FROM {splunk_data_sync_db}.credit_application_pre_adjudication_stg;

SET QUERY_BAND = NONE FOR SESSION; -- noqa

END TRANSACTION;
