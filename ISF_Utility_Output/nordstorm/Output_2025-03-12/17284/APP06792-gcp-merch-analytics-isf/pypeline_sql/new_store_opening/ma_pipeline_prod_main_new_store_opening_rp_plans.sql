/*
Purpose:        Creates table for rp plans to support NSO dash 
                    rp_plans
				Joins RP Plan data from the staging table with RMS SKU num (PRODUCT_SKU_DIM_VW) and inserts into rp_plans table
Variable(s):     {{environment_schema}} T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING  (prod) or T3DL_ACE_MCH
                 {{env_suffix}} '' or '_dev' tablesuffix for prod testing
Author(s):       Sara Scott
Date Created/Modified: 10/23/2024 
*/

DELETE FROM {environment_schema}.rp_plans{env_suffix} ALL;
INSERT INTO {environment_schema}.rp_plans{env_suffix}
SELECT 
     WEEK as week_idnt
     ,SUBSTRING(STORE, 2) as STORE_NUM
     ,EPM_SKU as EPM_SKU_IDNT
     ,RMS_SKU_NUM
     ,UNITS
     ,current_timestamp AS LAST_UPDATED_DT
    FROM {environment_schema}.rp_plan_stg{env_suffix} rp
    JOIN PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW sku 
    ON CAST(rp.epm_sku AS INTEGER) = sku.epm_sku_num
    AND sku.channel_country = 'US'
;
--drop staging table to clean up datalab
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'rp_plan_stg{env_suffix}', OUT_RETURN_MSG);