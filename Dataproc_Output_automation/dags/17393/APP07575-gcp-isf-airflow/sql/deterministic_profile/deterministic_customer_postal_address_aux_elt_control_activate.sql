/* 
SET QUERY_BAND = '
App_ID={app_id};
DAG_ID=isf_deterministic_customer_postal_address_aux_dim_17393_customer_das_customer;
Task_Name=customer_postal_address_aux_elt_control_activate;'
FOR SESSION VOLATILE;
*/

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.ELT_CONTROL_START_LOAD('CUSTOMER_OBJ_POSTAL_ADDRESS_AUX_DIM{{params.tbl_sfx}}');


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.elt_control
SET
    CURR_BATCH_DATE = CURRENT_DATE('PST8PDT')
    , EXTRACT_FROM_TMSTP = CAST('1900-01-01 00:00:00' AS TIMESTAMP)
    ,EXTRACT_FROM_TMSTP_tz = '+00:00'
    , EXTRACT_TO_TMSTP = CAST('1900-01-01 00:00:00' AS TIMESTAMP)
    , EXTRACT_TO_TMSTP_tz = '+00:00'
    , BATCH_END_TMSTP = CAST('1900-01-01 00:00:00' AS TIMESTAMP)
    , BATCH_END_TMSTP_tz = '+00:00'
WHERE SUBJECT_AREA_NM = 'CUSTOMER_OBJ_POSTAL_ADDRESS_AUX_DIM{{params.tbl_sfx}}'
;


MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.deterministic_profile_batch_hist_audit{{params.tbl_sfx}} hist
    USING (
            SELECT *
            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
            WHERE SUBJECT_AREA_NM ='CUSTOMER_OBJ_POSTAL_ADDRESS_AUX_DIM{{params.tbl_sfx}}'
            ) control
                ON LOWER(control.SUBJECT_AREA_NM) = LOWER(hist.SUBJECT_AREA_NM)
                AND control.BATCH_ID = hist.DW_BATCH_ID
        WHEN MATCHED
            THEN UPDATE SET
                DW_BATCH_DATE = control.CURR_BATCH_DATE
                , EXTRACT_FROM = control.EXTRACT_FROM_TMSTP_UTC 
				, extract_from_tz = control.extract_from_tmstp_tz
                , EXTRACT_TO = control.EXTRACT_TO_TMSTP_UTC
				, extract_to_tz = control.extract_to_tmstp_tz
                , STATUS_CODE = CASE WHEN control.ACTIVE_LOAD_IND = 'Y' THEN 'RUNNING' ELSE 'FINISHED' END
                , IS_ADHOC_RUN = 'N'
                , DW_SYS_START_TMSTP = control.BATCH_START_TMSTP
                , DW_SYS_END_TMSTP = control.BATCH_END_TMSTP
                , SRC_S3_PATH = NULL
        WHEN NOT MATCHED
            THEN INSERT
            VALUES (
                control.SUBJECT_AREA_NM
               , control.BATCH_ID
               , control.CURR_BATCH_DATE
               , control.EXTRACT_FROM_TMSTP_UTC
			   , control.extract_from_tmstp_tz
               , control.EXTRACT_TO_TMSTP_UTC
               , control.extract_to_tmstp_tz
               , CASE WHEN control.ACTIVE_LOAD_IND = 'Y' THEN 'RUNNING' ELSE 'FINISHED' END
               , 'N'
               , control.BATCH_START_TMSTP
               , control.BATCH_END_TMSTP
               , NULL)
;


