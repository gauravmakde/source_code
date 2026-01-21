   /*
Purpose:        Creates empty table in {{environment_schema}} for RP Plans to support NSO dash
                    rp_plans
                    

Variable(s):     {{environment_schema}} T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING (prod) or T3DL_ACE_MCH
                 {{env_suffix}} '' or '_dev' tablesuffix for prod testing
Author(s):       Sara Scott
Date Created/Modified: 10/23/2024 

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'rp_plans{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.rp_plans{env_suffix}, FALLBACK,
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
    WEEK_IDNT VARCHAR(6), 
    STORE_NUM VARCHAR(6),
    EPM_SKU_IDNT VARCHAR(25),
    RMS_SKU_NUM VARCHAR(25),
    UNITS DECIMAL(12,4), 
    LAST_UPDATED_DT TIMESTAMP(6)
    
) 
PRIMARY INDEX (STORE_NUM, RMS_SKU_NUM); 


GRANT SELECT ON {environment_schema}.rp_plans{env_suffix}  TO PUBLIC;