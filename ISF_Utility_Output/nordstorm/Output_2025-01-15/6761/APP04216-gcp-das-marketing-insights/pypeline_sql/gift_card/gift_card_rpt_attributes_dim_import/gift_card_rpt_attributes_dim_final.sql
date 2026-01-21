--=======================================================================================================
--# Project         : GIFT CARD (CUSTECO-8863)
--# Product Version : 1.0.0
--# Filename        : gift_card_rpt_attributes_dim_final.sql
--# File Type       : SQL File
--# Part of         : NORDSTROM ANALYTIC PLATFORM SEMANTIC LAYER
--# Author          : Svyatoslav Trofimov
--#  ----------------------------------------------------------------------------------------------------
--# Purpose         : Populates GIFT_CARD_RPT_ATTRIBUTES_DIM table
--# -----------------------------------------------------------------------------------------------------
--#======================================================================================================
--# Version | Date       | Name                   | Changes/Comments
--# --------+------------+------------------------+------------------------------------------------------
--# 1.0     | 2023-06-26 | Svyatoslav Trofimov    | CUSTECO-12070: Moved RPT_ATTRIBUTES_DIM population into
--#                                               | a separate pipeline
--=======================================================================================================

------------------START------------------
SET QUERY_BAND = 'ORG=MARKETING-GC;RUN=DAILYRUN;App_ID=app04216;DAG_ID=gift_card_static_rpt_attributes_dim_import_6761_DAS_MARKETING_das_marketing_insights;Task_Name=stage_2_gift_card_rpt_attributes_dim_final;'
FOR SESSION VOLATILE;
ET;

{teradata_autocommit_on};

MERGE INTO {db_env}_NAP_BASE_VWS.GIFT_CARD_RPT_ATTRIBUTES_DIM{tbl_sfx} TGT
USING (
SELECT
    SRC.alt_merchant_id
    , SRC.merchant_id
    , SRC.class_id
    , SRC.promo_code
    , SRC.rpt_business_unit_name
    , SRC.rpt_location
    , SRC.rpt_geog
    , SRC.rpt_channel
    , SRC.rpt_banner
    , SRC.rpt_distribution_partner_desc
    , SRC.event_tmstp
    , SRC.sys_tmstp
    , SRC.dw_batch_date
    , SRC.dw_batch_id
FROM (SELECT
        ldg.alt_merchant_id
        , ldg.merchant_id
        , ldg.class_id
        , ldg.promo_code
        , COALESCE(ldg.rpt_business_unit_name, '') AS rpt_business_unit_name
        , COALESCE(ldg.rpt_location, '') AS rpt_location
        , COALESCE(ldg.rpt_geog, '') AS rpt_geog
        , COALESCE(ldg.rpt_channel, '') AS rpt_channel
        , COALESCE(ldg.rpt_banner, '') AS rpt_banner
        , COALESCE(ldg.rpt_distribution_partner_desc, '') AS rpt_distribution_partner_desc
        , CAST(CAST(TO_TIMESTAMP(CAST(ldg.event_tmstp AS BIGINT)/1000) + CAST(ldg.event_tmstp AS BIGINT) MOD 1000 * INTERVAL '0.001' SECOND AS CHAR(26)) || '+00:00' AS TIMESTAMP(6) WITH TIME ZONE) AS event_tmstp
        , CAST(TO_TIMESTAMP(ldg.sys_tmstp/1000) + ldg.sys_tmstp MOD 1000 * INTERVAL '0.001' SECOND as TIMESTAMP(6)) AS sys_tmstp
        , elt.curr_batch_date AS dw_batch_date
        , elt.batch_id AS dw_batch_id
        FROM (    SELECT DISTINCT 
                    REGEXP_SUBSTR(alt_merchant_id_desc, '\d+', 1, 1, 'c') as alt_merchant_id
                    , REGEXP_SUBSTR(merchant_id_desc, '\d+', 1, 1, 'c') as merchant_id
                    , REGEXP_SUBSTR(class_id_desc, '\d+', 1, 1, 'c') as class_id
                    , REGEXP_SUBSTR(promo_code_desc, '\d+', 1, 1, 'c') as promo_code
                    , Trim(rpt_business_unit_name) as rpt_business_unit_name
                    , Trim(rpt_location) as rpt_location
                    , Trim(rpt_geog) as rpt_geog
                    , Trim(rpt_channel) as rpt_channel
                    , Trim(rpt_banner) as rpt_banner
                    , Trim(rpt_distribution_partner_desc) as rpt_distribution_partner_desc
                    , event_tmstp
                    , sys_tmstp
                FROM {db_env}_NAP_BASE_VWS.GIFT_CARD_RPT_ATTRIBUTES_LDG{tbl_sfx} 
        ) ldg
        CROSS JOIN (SELECT curr_batch_date, batch_id FROM {db_env}_NAP_UTL.ELT_CONTROL WHERE SUBJECT_AREA_NM = 'GIFT_CARD_RPT_ATTRIBUTES_DIM_IMPORT{tbl_sfx}') elt
        QUALIFY row_number() OVER( PARTITION BY alt_merchant_id, merchant_id, class_id,promo_code order by event_tmstp desc, sys_tmstp desc)=1
      ) SRC
        LEFT JOIN {db_env}_NAP_BASE_VWS.GIFT_CARD_RPT_ATTRIBUTES_DIM{tbl_sfx} dim ON
            SRC.alt_merchant_id = dim.alt_merchant_id
            AND SRC.merchant_id = dim.merchant_id
            AND SRC.class_id = dim.class_id
            AND SRC.promo_code = dim.promo_code            
        WHERE (SRC.event_tmstp > dim.event_tmstp
              OR (SRC.event_tmstp = dim.event_tmstp AND SRC.sys_tmstp > dim.sys_tmstp)
                OR dim.event_tmstp IS NULL)
) AS SRC
ON SRC.alt_merchant_id = TGT.alt_merchant_id
AND SRC.merchant_id = TGT.merchant_id
AND SRC.class_id = TGT.class_id
AND SRC.promo_code = TGT.promo_code            
WHEN MATCHED THEN UPDATE SET
    rpt_business_unit_name = SRC.rpt_business_unit_name
    , rpt_location = SRC.rpt_location
    , rpt_geog = SRC.rpt_geog
    , rpt_channel = SRC.rpt_channel
    , rpt_banner = SRC.rpt_banner
    , rpt_distribution_partner_desc = SRC.rpt_distribution_partner_desc
    , event_tmstp = SRC.event_tmstp
    , sys_tmstp = SRC.sys_tmstp
    , dw_batch_date = SRC.dw_batch_date
    , dw_sys_updt_tmstp = CURRENT_TIMESTAMP(0)
    , dw_batch_id = SRC.dw_batch_id
WHEN NOT MATCHED THEN INSERT (
    alt_merchant_id = SRC.alt_merchant_id
    , merchant_id = SRC.merchant_id
    , class_id = SRC.class_id
    , promo_code = SRC.promo_code
    , rpt_business_unit_name = SRC.rpt_business_unit_name
    , rpt_location = SRC.rpt_location
    , rpt_geog = SRC.rpt_geog
    , rpt_channel = SRC.rpt_channel
    , rpt_banner = SRC.rpt_banner
    , rpt_distribution_partner_desc = SRC.rpt_distribution_partner_desc
    , event_tmstp = SRC.event_tmstp
    , sys_tmstp = SRC.sys_tmstp
    , dw_batch_date = SRC.dw_batch_date
    , dw_sys_load_tmstp = CURRENT_TIMESTAMP(0)
    , dw_sys_updt_tmstp = CURRENT_TIMESTAMP(0)
    , dw_batch_id = SRC.dw_batch_id
);

CALL {db_env}_NAP_UTL.DQ_PIPELINE_AUDIT_V1
('GIFT_CARD_RPT_ATTRIBUTES_DIM_STG_TO_TGT{tbl_sfx}' , 'GIFT_CARD_RPT_ATTRIBUTES_DIM_IMPORT{tbl_sfx}' , '{db_env}_NAP_STG' , 'GIFT_CARD_RPT_ATTRIBUTES_LDG{tbl_sfx}' , '{db_env}_NAP_BASE_VWS' , 'GIFT_CARD_RPT_ATTRIBUTES_DIM{tbl_sfx}' , 'Count_Distinct', 0 ,'T-S'
, 'concat(Cast(REGEXP_SUBSTR(alt_merchant_id_desc,''\d+'', 1, 1,''c'') as varchar(50)) , ''_'' , CAST(REGEXP_SUBSTR(merchant_id_desc,''\d+'',1,1,''c'') as varchar(50)), ''_'' , 
CAST(REGEXP_SUBSTR(class_id_desc,''\d+'',1,1,''c'') as varchar(50)) , ''_'' , 
CAST(REGEXP_SUBSTR(promo_code_desc,''\d+'',1,1,''c'') as varchar(50)))'
, 'concat(alt_merchant_id, ''_'', merchant_id ,''_'' , class_id ,''_'' , promo_code)'
,NULL,NULL,'Y');

SET QUERY_BAND = NONE FOR SESSION;
------------------END------------------
