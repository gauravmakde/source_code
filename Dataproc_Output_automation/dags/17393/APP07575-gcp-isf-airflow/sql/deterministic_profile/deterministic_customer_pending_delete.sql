/* SET QUERY_BAND = '
App_ID={app_id};
DAG_ID=isf_deterministic_customer_dim_17393_customer_das_customer;
Task_Name=deterministic_customer_pending_delete;'
FOR SESSION VOLATILE;*/



TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.pending_deterministic_customer_dim{{params.tbl_sfx}};


/* SET QUERY_BAND = NONE FOR SESSION;*/


