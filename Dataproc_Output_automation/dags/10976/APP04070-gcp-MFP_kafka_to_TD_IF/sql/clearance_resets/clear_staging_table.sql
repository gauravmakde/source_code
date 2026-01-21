/*
 * Flush staging table in preparation for next loading job.
 * Since this is the first job in the DAG, data is available until next run.
 */

/*SET QUERY_BAND = 'App_ID=app08001;DAG_ID=clearance_reset_10976_tech_nap_merch;Task_Name=job_1_clear_ldg;'
FOR SESSION VOLATILE;*/


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.clearance_markdown_reset_ldg;


--SET QUERY_BAND = NONE FOR SESSION;


