--=======================================================================================================
--# Project         : GIFT CARD (CUSTECO-8863)
--# Product Version : 1.0.0
--# Filename        : gift_card_static_dims_final.sql
--# File Type       : SQL File
--# Part of         : NORDSTROM ANALYTIC PLATFORM SEMANTIC LAYER
--# Author          : Vartika Parashar
--#  ----------------------------------------------------------------------------------------------------
--# Purpose         : This script populates 3 of 4 the Static Dim tables required for Gift Card Data
--#                 : Other Static Dims will have a separate pipeline
--# -----------------------------------------------------------------------------------------------------
--#======================================================================================================
--# Version | Date       | Name                   | Changes/Comments
--# --------+------------+------------------------+------------------------------------------------------
--# 1.0     | 2022-09-20 | Vartika Parashar       | Script Created
--# 1.1     | 2022-10-07 | Vartika Parashar       | Implement Model Changes as per JIRA CUSTECO-9745
--# 1.2     | 2022-10-07 | Raju Bafna             | Added GIFT_CARD_DISTRIBUTION_DIM as per JIRA CUSTECO-11255
--# 1.3     | 2022-04-12 | Raju Bafna             | Added Static Dim GIFT_CARD_RPT_ATTRIBUTES_DIM - JIRA CUSTECO-11471
--                                                  to consolidate DISTRIBUTION_PARTNER_DIM and ALT_MERCHANT_EXT_DIM
--# 1.4     | 2023-05-15 | Bohdan Verkhohliad     | Move Dynamic DIMs to new subject_area - CUSTECO-11704
--# 1.5     | 2023-06-26 | Svyatoslav Trofimov    | CUSTECO-12070: Moved RPT_ATTRIBUTES_DIM population into
--#                                               | a separate pipeline
--=======================================================================================================

------------------START------------------
SET QUERY_BAND = 'ORG=MARKETING-GC;RUN=DAILYRUN;App_ID=app04216;DAG_ID=gift_card_static_dims_6761_DAS_MARKETING_das_marketing_insights;Task_Name=stage_2_gift_card_static_dims_final;'
FOR SESSION VOLATILE;
ET;

{teradata_autocommit_on};

MERGE INTO {db_env}_NAP_BASE_VWS.GIFT_CARD_TRAN_CODE_DIM{tbl_sfx} TGT
USING (
SELECT
    SRC.tran_code
    , SRC.tran_flow
    , SRC.tran_code_desc
    , SRC.tran_type
    , SRC.event_tmstp
    , SRC.sys_tmstp
    , SRC.dw_batch_date
    , SRC.dw_batch_id
FROM (SELECT
        ldg.tran_code
        , COALESCE(ldg.tran_flow, '') AS tran_flow
        , COALESCE(ldg.tran_code_desc, '') AS tran_code_desc
        , COALESCE(ldg.tran_type, '') AS tran_type
        , CAST(CAST(TO_TIMESTAMP(CAST(ldg.event_tmstp AS BIGINT)/1000) + CAST(ldg.event_tmstp AS BIGINT) MOD 1000 * INTERVAL '0.001' SECOND AS CHAR(26)) || '+00:00' AS TIMESTAMP(6) WITH TIME ZONE) AS event_tmstp
        , CAST(TO_TIMESTAMP(ldg.sys_tmstp/1000) + ldg.sys_tmstp MOD 1000 * INTERVAL '0.001' SECOND as TIMESTAMP(6)) AS sys_tmstp
        , elt.curr_batch_date AS dw_batch_date
        , elt.batch_id AS dw_batch_id
        FROM {db_env}_NAP_BASE_VWS.GIFT_CARD_TRAN_CODE_LDG{tbl_sfx} ldg
        CROSS JOIN (SELECT curr_batch_date, batch_id FROM {db_env}_NAP_UTL.ELT_CONTROL WHERE SUBJECT_AREA_NM = 'GIFT_CARD_STATIC_DIMS{tbl_sfx}') elt
        QUALIFY row_number() OVER( PARTITION BY tran_code order by event_tmstp desc, sys_tmstp desc)=1) SRC
        LEFT JOIN {db_env}_NAP_BASE_VWS.GIFT_CARD_TRAN_CODE_DIM{tbl_sfx} dim ON
            SRC.tran_code = dim.tran_code
        WHERE (SRC.event_tmstp > dim.event_tmstp
              OR (SRC.event_tmstp = dim.event_tmstp AND SRC.sys_tmstp > dim.sys_tmstp)
                OR dim.event_tmstp IS NULL)
) AS SRC
ON SRC.tran_code = TGT.tran_code
WHEN MATCHED THEN UPDATE SET
    tran_flow = SRC.tran_flow
    , tran_code_desc = SRC.tran_code_desc
    , tran_type = SRC.tran_type
    , event_tmstp = SRC.event_tmstp
    , sys_tmstp = SRC.sys_tmstp
    , dw_batch_date = SRC.dw_batch_date
    , dw_sys_updt_tmstp = CURRENT_TIMESTAMP(0)
    , dw_batch_id = SRC.dw_batch_id
WHEN NOT MATCHED THEN INSERT (
    tran_code = SRC.tran_code
    , tran_flow = SRC.tran_flow
    , tran_code_desc = SRC.tran_code_desc
    , tran_type = SRC.tran_type
    , event_tmstp = SRC.event_tmstp
    , sys_tmstp = SRC.sys_tmstp
    , dw_batch_date = SRC.dw_batch_date
    , dw_sys_load_tmstp = CURRENT_TIMESTAMP(0)
    , dw_sys_updt_tmstp = CURRENT_TIMESTAMP(0)
    , dw_batch_id = SRC.dw_batch_id
);

--.IF ERRORCODE > 0 THEN .QUIT 5;

CALL {db_env}_NAP_UTL.DQ_PIPELINE_AUDIT_V1
('GIFT_CARD_TRAN_CODE_DIM_STG_TO_TGT{tbl_sfx}','GIFT_CARD_STATIC_DIMS{tbl_sfx}','{db_env}_NAP_STG','GIFT_CARD_TRAN_CODE_LDG{tbl_sfx}','{db_env}_NAP_BASE_VWS','GIFT_CARD_TRAN_CODE_DIM{tbl_sfx}','Count_Distinct',0,'T-S','tran_code','tran_code',NULL,NULL,'Y');

--.IF ERRORCODE > 0 THEN .QUIT 6;


MERGE INTO {db_env}_NAP_BASE_VWS.GIFT_CARD_CLASS_DIM{tbl_sfx} TGT
USING (
SELECT
    SRC.class_id
    , SRC.class_name
    , SRC.country_code
    , SRC.card_class_type
    , SRC.class_category
    , SRC.event_tmstp
    , SRC.sys_tmstp
    , SRC.dw_batch_date
    , SRC.dw_batch_id
FROM (SELECT
        ldg.class_id
        , COALESCE(ldg.class_name, '') AS class_name
        , COALESCE(ldg.country_code, '') AS country_code
        , COALESCE(ldg.card_class_type, '') AS card_class_type
        , COALESCE(ldg.class_category, '') AS class_category
        , CAST(CAST(TO_TIMESTAMP(CAST(ldg.event_tmstp AS BIGINT)/1000) + CAST(ldg.event_tmstp AS BIGINT) MOD 1000 * INTERVAL '0.001' SECOND AS CHAR(26)) || '+00:00' AS TIMESTAMP(6) WITH TIME ZONE) AS event_tmstp
        , CAST(TO_TIMESTAMP(ldg.sys_tmstp/1000) + ldg.sys_tmstp MOD 1000 * INTERVAL '0.001' SECOND as TIMESTAMP(6)) AS sys_tmstp
        , elt.curr_batch_date AS dw_batch_date
        , elt.batch_id AS dw_batch_id
        FROM {db_env}_NAP_BASE_VWS.GIFT_CARD_CLASS_LDG{tbl_sfx} ldg
        CROSS JOIN (SELECT curr_batch_date, batch_id FROM {db_env}_NAP_UTL.ELT_CONTROL WHERE SUBJECT_AREA_NM = 'GIFT_CARD_STATIC_DIMS{tbl_sfx}') elt
        QUALIFY row_number() OVER( PARTITION BY class_id order by event_tmstp desc, sys_tmstp desc)=1) SRC
        LEFT JOIN {db_env}_NAP_BASE_VWS.GIFT_CARD_CLASS_DIM{tbl_sfx} dim ON
            SRC.class_id = dim.class_id
        WHERE (SRC.event_tmstp > dim.event_tmstp
              OR (SRC.event_tmstp = dim.event_tmstp AND SRC.sys_tmstp > dim.sys_tmstp)
                OR dim.event_tmstp IS NULL)
) AS SRC
ON SRC.class_id = TGT.class_id
WHEN MATCHED THEN UPDATE SET
    class_name = SRC.class_name
    , country_code = SRC.country_code
    , card_class_type = SRC.card_class_type
    , class_category = SRC.class_category
    , event_tmstp = SRC.event_tmstp
    , sys_tmstp = SRC.sys_tmstp
    , dw_batch_date = SRC.dw_batch_date
    , dw_sys_updt_tmstp = CURRENT_TIMESTAMP(0)
    , dw_batch_id = SRC.dw_batch_id
WHEN NOT MATCHED THEN INSERT (
    class_id = SRC.class_id
    , class_name = SRC.class_name
    , country_code = SRC.country_code
    , card_class_type = SRC.card_class_type
    , class_category = SRC.class_category
    , event_tmstp = SRC.event_tmstp
    , sys_tmstp = SRC.sys_tmstp
    , dw_batch_date = SRC.dw_batch_date
    , dw_sys_load_tmstp = CURRENT_TIMESTAMP(0)
    , dw_sys_updt_tmstp = CURRENT_TIMESTAMP(0)
    , dw_batch_id = SRC.dw_batch_id
);

--.IF ERRORCODE > 0 THEN .QUIT 11;

CALL {db_env}_NAP_UTL.DQ_PIPELINE_AUDIT_V1
('GIFT_CARD_CLASS_DIM_STG_TO_TGT{tbl_sfx}','GIFT_CARD_STATIC_DIMS{tbl_sfx}','{db_env}_NAP_STG','GIFT_CARD_CLASS_LDG{tbl_sfx}','{db_env}_NAP_BASE_VWS','GIFT_CARD_CLASS_DIM{tbl_sfx}','Count_Distinct',0,'T-S','class_id','class_id',NULL,NULL,'Y');

--.IF ERRORCODE > 0 THEN .QUIT 12;

MERGE INTO {db_env}_NAP_BASE_VWS.GIFT_CARD_TYPE_DIM{tbl_sfx} TGT
USING (
SELECT
    SRC.activation_tran_code
    , SRC.card_type
    , SRC.event_tmstp
    , SRC.sys_tmstp
    , SRC.dw_batch_date
    , SRC.dw_batch_id
FROM (SELECT
        ldg.activation_tran_code
        , COALESCE(ldg.card_type, '') AS card_type
        , CAST(CAST(TO_TIMESTAMP(CAST(ldg.event_tmstp AS BIGINT)/1000) + CAST(ldg.event_tmstp AS BIGINT) MOD 1000 * INTERVAL '0.001' SECOND AS CHAR(26)) || '+00:00' AS TIMESTAMP(6) WITH TIME ZONE) AS event_tmstp
        , CAST(TO_TIMESTAMP(ldg.sys_tmstp/1000) + ldg.sys_tmstp MOD 1000 * INTERVAL '0.001' SECOND as TIMESTAMP(6)) AS sys_tmstp
        , elt.curr_batch_date AS dw_batch_date
        , elt.batch_id AS dw_batch_id
        FROM {db_env}_NAP_BASE_VWS.GIFT_CARD_TYPE_LDG{tbl_sfx} ldg
        CROSS JOIN (SELECT curr_batch_date, batch_id FROM {db_env}_NAP_UTL.ELT_CONTROL WHERE SUBJECT_AREA_NM = 'GIFT_CARD_STATIC_DIMS{tbl_sfx}') elt
        QUALIFY row_number() OVER( PARTITION BY activation_tran_code order by event_tmstp desc, sys_tmstp desc)=1) SRC
        LEFT JOIN {db_env}_NAP_BASE_VWS.GIFT_CARD_TYPE_DIM{tbl_sfx} dim ON
            SRC.activation_tran_code = dim.activation_tran_code
        WHERE (SRC.event_tmstp > dim.event_tmstp
              OR (SRC.event_tmstp = dim.event_tmstp AND SRC.sys_tmstp > dim.sys_tmstp)
                OR dim.event_tmstp IS NULL)
) AS SRC
ON SRC.activation_tran_code = TGT.activation_tran_code
WHEN MATCHED THEN UPDATE SET
    card_type = SRC.card_type
    , event_tmstp = SRC.event_tmstp
    , sys_tmstp = SRC.sys_tmstp
    , dw_batch_date = SRC.dw_batch_date
    , dw_sys_updt_tmstp = CURRENT_TIMESTAMP(0)
    , dw_batch_id = SRC.dw_batch_id
WHEN NOT MATCHED THEN INSERT (
    activation_tran_code = SRC.activation_tran_code
    , card_type = SRC.card_type
    , event_tmstp = SRC.event_tmstp
    , sys_tmstp = SRC.sys_tmstp
    , dw_batch_date = SRC.dw_batch_date
    , dw_sys_load_tmstp = CURRENT_TIMESTAMP(0)
    , dw_sys_updt_tmstp = CURRENT_TIMESTAMP(0)
    , dw_batch_id = SRC.dw_batch_id
);

CALL {db_env}_NAP_UTL.DQ_PIPELINE_AUDIT_V1
('GIFT_CARD_TYPE_DIM_STG_TO_TGT{tbl_sfx}','GIFT_CARD_STATIC_DIMS{tbl_sfx}','{db_env}_NAP_STG','GIFT_CARD_TYPE_LDG{tbl_sfx}','{db_env}_NAP_BASE_VWS','GIFT_CARD_TYPE_DIM{tbl_sfx}','Count_Distinct',0,'T-S','activation_tran_code','activation_tran_code',NULL,NULL,'Y');

SET QUERY_BAND = NONE FOR SESSION;
------------------END------------------
