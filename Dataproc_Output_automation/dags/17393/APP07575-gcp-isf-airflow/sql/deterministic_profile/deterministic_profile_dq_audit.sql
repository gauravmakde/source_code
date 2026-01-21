/* 
SET QUERY_BAND = '
App_ID={app_id};
DAG_ID=isf_deterministic_profile_ldg_to_dim_17393_customer_das_customer;
Task_Name=deterministic_profile_dq_audit;'
FOR SESSION VOLATILE;
*/


-- IF tgt_metric - src_metric > threshold THEN audit failed


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DQ_PIPELINE_AUDIT_V1
('DETERMINISTIC_CUSTOMER_CLASSIFICATION_LDG_TO_DIM{{params.tbl_sfx}}','DETERMINISTIC_CUSTOMER_PROFILE{{params.tbl_sfx}}','{{params.dbenv}}_NAP_BASE_VWS','DETERMINISTIC_CUSTOMER_CLASSIFICATION_LDG{{params.tbl_sfx}}','{{params.dbenv}}_NAP_BASE_VWS','DETERMINISTIC_CUSTOMER_CLASSIFICATION_DIM{{params.tbl_sfx}}','Count_Distinct',0,'T-S','deterministicprofileid','deterministic_profile_id',NULL,NULL,'Y');

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DQ_PIPELINE_AUDIT_V1
('DETERMINISTIC_CUSTOMER_PROFILE_ASSOCIATION_WRK_TO_DIM{{params.tbl_sfx}}','DETERMINISTIC_CUSTOMER_PROFILE{{params.tbl_sfx}}','{{params.dbenv}}_NAP_BASE_VWS','DETERMINISTIC_CUSTOMER_PROFILE_ASSOCIATION_WRK{{params.tbl_sfx}}','{{params.dbenv}}_NAP_BASE_VWS','DETERMINISTIC_CUSTOMER_PROFILE_ASSOCIATION_DIM{{params.tbl_sfx}}','Count_Distinct',0,'T-S','deterministicprofileid','deterministic_profile_id',NULL,NULL,'Y');


/* SET QUERY_BAND = NONE FOR SESSION; */


