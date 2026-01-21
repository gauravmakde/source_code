/*
DDL for Sku CC Lookup and CC Type
Author: Sara Riker & Christine Buckler

Creates Tables:
    {environment_schema}.sku_cc_lkp{env_suffix}
    {environment_schema}.cc_type{env_suffix}
*/



CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'cc_type_realigned{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.cc_type_realigned{env_suffix}, FALLBACK,
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
     mnth_idnt INTEGER 
    ,mth_start_day_date DATE FORMAT 'YYYY-MM-DD'
    ,channel_country VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('US','CA')
    ,channel_brand VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('NORDSTROM', 'NORDSTROM_RACK')
    ,channel VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('STORE', 'DIGITAL')
    ,customer_choice VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,los_flag INTEGER
    ,cc_type VARCHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('CF', 'NEW')
    ,update_timestamp TIMESTAMP(6) WITH TIME ZONE 
)
PRIMARY INDEX(mnth_idnt, customer_choice)
PARTITION BY RANGE_N(mth_start_day_date BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY , NO RANGE);

GRANT SELECT ON {environment_schema}.cc_type_realigned{env_suffix} TO PUBLIC;