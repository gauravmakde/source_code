/*
Purpose:        Creates staging table for rp plans to support NSO dash 
                    rp_plan_stg
				Loads data from S3 bucket using TPT load and inserts into rp_plan_stg table
Variable(s):     {{environment_schema}} T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING  (prod) or T3DL_ACE_MCH
                 {{env_suffix}} '' or '_dev' tablesuffix for prod testing
Author(s):       Sara Scott
Date Created/Modified: 10/23/2024 
*/

--create DDL for staging table
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'rp_plan_stg{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.rp_plan_stg{env_suffix}, FALLBACK,
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
    WEEK VARCHAR(6),
    STORE VARCHAR(6),
    EPM_SKU VARCHAR(25),
    UNITS DECIMAL(12,4) 
    
) 
PRIMARY INDEX (STORE, EPM_SKU); 
--run TPT load
CALL SYS_MGMT.S3_TPT_LOAD ('{environment_schema}','rp_plan_stg{env_suffix}','us-west-2','rpds-load-op-prod','prod/aip/','mk_proj_inv_sku_dstkweek.csv','2C',OUT_MESSAGE);