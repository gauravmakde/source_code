
CALL `{{params.gcp_project_id}}`.{{params.db_env}}_NAP_UTL.ELT_CONTROL_END_LOAD ('CUSTOMER_OBJ{{params.tbl_sfx}}');

MERGE INTO `{{params.gcp_project_id}}`.{{params.db_env}}_nap_utl.deterministic_profile_batch_hist_audit{{params.tbl_sfx}} hist
    USING (
            SELECT *
            FROM `{{params.gcp_project_id}}`.{{params.db_env}}_nap_base_vws.elt_controL
            WHERE LOWER(SUBJECT_AREA_NM) = LOWER('CUSTOMER_OBJ{{params.tbl_sfx}}')
            ) control
                ON control.SUBJECT_AREA_NM = hist.SUBJECT_AREA_NM
                AND control.BATCH_ID = hist.DW_BATCH_ID
        WHEN MATCHED
            THEN UPDATE SET
                DW_BATCH_DATE = control.CURR_BATCH_DATE,
                EXTRACT_FROM = CAST(control.EXTRACT_FROM_TMSTP AS TIMESTAMP),
                EXTRACT_TO = CAST(control.EXTRACT_TO_TMSTP AS TIMESTAMP),
                STATUS_CODE = CASE WHEN LOWER(control.ACTIVE_LOAD_IND) = LOWER('Y') THEN LOWER('RUNNING') ELSE LOWER('FINISHED') END,
                IS_ADHOC_RUN = 'N',
                DW_SYS_START_TMSTP = control.BATCH_START_TMSTP,
                DW_SYS_END_TMSTP = control.BATCH_END_TMSTP,
                SRC_S3_PATH = '<src_s3_path>'
        WHEN NOT MATCHED
            THEN INSERT (
                SUBJECT_AREA_NM,
                DW_BATCH_ID,
                DW_BATCH_DATE,
                EXTRACT_FROM,
                EXTRACT_TO,
                STATUS_CODE,
                IS_ADHOC_RUN,
                DW_SYS_START_TMSTP,
                DW_SYS_END_TMSTP,
                SRC_S3_PATH
            )
            VALUES (
                control.SUBJECT_AREA_NM,
                control.BATCH_ID,
                control.CURR_BATCH_DATE,
                CAST(control.EXTRACT_FROM_TMSTP AS TIMESTAMP),
                CAST(control.EXTRACT_TO_TMSTP AS TIMESTAMP),
                CASE WHEN LOWER(control.ACTIVE_LOAD_IND) = LOWER('Y') THEN LOWER('RUNNING') ELSE LOWER('FINISHED') END,
                'N',
                control.BATCH_START_TMSTP,
                control.BATCH_END_TMSTP,
                '<src_s3_path>'
            );

    
