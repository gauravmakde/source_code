{teradata_autocommit_on};

SET QUERY_BAND = '
App_ID=app04216; DAG_ID=rsvp_onetime_load_6761_DAS_MARKETING_das_marketing_insights; Task_Name=rsvp_onetime_load_job;'
FOR SESSION VOLATILE;

-- to refresh staging table everyday
DELETE FROM {db_env}_NAP_STG.RSVP_STG ALL;

SET QUERY_BAND = NONE FOR SESSION;
