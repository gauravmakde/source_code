CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DQ_PIPELINE_AUDIT_V1
('CUSTOMER_OBJ_PROGRAM_LDG_TO_DIM{{params.tbl_sfx}}','CUSTOMER_OBJ{{params.tbl_sfx}}','{{params.dbenv}}_NAP_BASE_VWS','CUSTOMER_OBJ_PROGRAM_LDG{{params.tbl_sfx}}','{{params.dbenv}}NAP_BASE_VWS','CUSTOMER_OBJ_PROGRAM_DIM{{params.tbl_sfx}}','Count_Distinct',0,'T-S','uniquesourceid','unique_source_id',NULL,NULL,'Y');


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DQ_PIPELINE_AUDIT_V1
('CUSTOMER_OBJ_EMAIL_LDG_TO_DIM{{params.tbl_sfx}}','CUSTOMER_OBJ{{params.tbl_sfx}}','{{params.dbenv}}_NAP_BASE_VWS','CUSTOMER_OBJ_EMAIL_LDG{{params.tbl_sfx}}','{{params.dbenv}}_NAP_BASE_VWS','CUSTOMER_OBJ_EMAIL_DIM{{params.tbl_sfx}}','Count_Distinct',0,'T-S','uniquesourceid','unique_source_id',NULL,NULL,'Y');


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DQ_PIPELINE_AUDIT_V1
('CUSTOMER_OBJ_TELEPHONE_LDG_TO_DIM{{params.tbl_sfx}}','CUSTOMER_OBJ{{params.tbl_sfx}}','{{params.dbenv}}_NAP_BASE_VWS','CUSTOMER_OBJ_TELEPHONE_LDG{{params.tbl_sfx}}','{{params.dbenv}}_NAP_BASE_VWS','CUSTOMER_OBJ_TELEPHONE_DIM{{params.tbl_sfx}}','Count_Distinct',0,'T-S','uniquesourceid','unique_source_id',NULL,NULL,'Y');


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DQ_PIPELINE_AUDIT_V1
('CUSTOMER_OBJ_POSTAL_ADDRESS_LDG_TO_DIM{{params.tbl_sfx}}','CUSTOMER_OBJ{{params.tbl_sfx}}','{{params.dbenv}}_NAP_BASE_VWS','CUSTOMER_OBJ_POSTAL_ADDRESS_LDG{{params.tbl_sfx}}','{{params.dbenv}}_NAP_BASE_VWS','CUSTOMER_OBJ_POSTAL_ADDRESS_DIM{{params.tbl_sfx}}','Count_Distinct',0,'T-S','uniquesourceid','unique_source_id',NULL,NULL,'Y');


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DQ_PIPELINE_AUDIT_V1
('CUSTOMER_OBJ_MERGE_ALIAS_LDG_TO_DIM{{params.tbl_sfx}}','CUSTOMER_OBJ{{params.tbl_sfx}}','{{params.dbenv}}_NAP_BASE_VWS','CUSTOMER_OBJ_MERGE_ALIAS_LDG{{params.tbl_sfx}}','{{params.dbenv}}_NAP_BASE_VWS','CUSTOMER_OBJ_MERGE_ALIAS_DIM{{params.tbl_sfx}}','Count_Distinct',0,'T-S','uniquesourceid','unique_source_id',NULL,NULL,'Y');


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DQ_PIPELINE_AUDIT_V1
('CUSTOMER_OBJ_PAYMENT_METHOD_LDG_TO_DIM{{params.tbl_sfx}}','CUSTOMER_OBJ{{params.tbl_sfx}}','{{params.dbenv}}_NAP_BASE_VWS','CUSTOMER_OBJ_PAYMENT_METHOD_LDG{{params.tbl_sfx}}','{{params.dbenv}}_NAP_BASE_VWS','CUSTOMER_OBJ_PAYMENT_METHOD_DIM{{params.tbl_sfx}}','Count_Distinct',0,'T-S','uniquesourceid','unique_source_id',NULL,NULL,'Y');


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DQ_PIPELINE_AUDIT_V1
('CUSTOMER_OBJ_MASTER_LDG_TO_DIM{{params.tbl_sfx}}','CUSTOMER_OBJ{{params.tbl_sfx}}','{{params.dbenv}}_NAP_BASE_VWS','CUSTOMER_OBJ_MASTER_LDG{{params.tbl_sfx}}','{{params.dbenv}}_NAP_BASE_VWS','CUSTOMER_OBJ_MASTER_DIM{{params.tbl_sfx}}','Count_Distinct',0,'T-S','uniquesourceid','unique_source_id',NULL,NULL,'Y');





