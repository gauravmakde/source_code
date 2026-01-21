{teradata_autocommit_off};
--BT; Not needed as with Auto commit off there is implicit transaction ON
-- Marks ELT as active, creates a new BATCH_ID and sets Batch_Start timestamp at ELT_CONTROL table
EXEC {db_env}_NAP_UTL.ELT_CONTROL_START_LOAD('GIFT_CARD_RPT_ATTRIBUTES_DIM_IMPORT{tbl_sfx}');

-- Sets BATCH_DATE to current date, FROM and TO timestamps - to some very deep past value to show they're yet undefined
UPDATE {db_env}_NAP_BASE_VWS.ELT_CONTROL
    SET CURR_BATCH_DATE = CURRENT_DATE
        , EXTRACT_FROM_TMSTP = '1900-01-01 00:00:00.000000'
        , EXTRACT_TO_TMSTP = '1900-01-01 00:00:00.000000'
        , BATCH_END_TMSTP = '1900-01-01 00:00:00.000000'
    WHERE SUBJECT_AREA_NM = 'GIFT_CARD_RPT_ATTRIBUTES_DIM_IMPORT{tbl_sfx}'
;

--Adding/Updating a record to ETL_BATCH_INFO table from ELT_CONTROL table
MERGE INTO {db_env}_NAP_BASE_VWS.GIFT_CARD_BATCH_HIST_AUDIT{tbl_sfx} hist
    USING (SELECT *
            FROM {db_env}_NAP_BASE_VWS.ELT_CONTROL
            WHERE SUBJECT_AREA_NM = 'GIFT_CARD_RPT_ATTRIBUTES_DIM_IMPORT{tbl_sfx}'
            ) control ON control.SUBJECT_AREA_NM = hist.SUBJECT_AREA_NM
                        AND control.BATCH_ID = hist.DW_BATCH_ID
        WHEN MATCHED
            THEN UPDATE SET
                DW_BATCH_DATE = control.CURR_BATCH_DATE
                , EXTRACT_FROM = control.EXTRACT_FROM_TMSTP
                , EXTRACT_TO = control.EXTRACT_TO_TMSTP
                , STATUS_CODE = CASE WHEN control.ACTIVE_LOAD_IND = 'Y' THEN 'RUNNING' ELSE 'FINISHED' END
                , IS_ADHOC_RUN = 'N'
                , DW_SYS_START_TMSTP = control.BATCH_START_TMSTP
                , DW_SYS_END_TMSTP = control.BATCH_END_TMSTP
                , SRC_S3_PATH = '{gift_card_static_dims_src_s3_path}'
        WHEN NOT MATCHED
            THEN INSERT
            VALUES (
                control.SUBJECT_AREA_NM
               , control.BATCH_ID
               , control.CURR_BATCH_DATE
               , control.EXTRACT_FROM_TMSTP
               , control.EXTRACT_TO_TMSTP
               , CASE WHEN control.ACTIVE_LOAD_IND = 'Y' THEN 'RUNNING' ELSE 'FINISHED' END
               , 'N'
               , control.BATCH_START_TMSTP
               , control.BATCH_END_TMSTP
               , '{gift_card_static_dims_src_s3_path}')
;

ET;
