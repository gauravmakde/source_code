/*
Size Curves Adoption Metrics DDL
Author: Sara Riker
12/11/23: Create Script

    {{environment_schema}}: t2dl_das_size
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'adoption_metrics{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.adoption_metrics{env_suffix}
	,fallback
	,NO BEFORE JOURNAL
	,NO AFTER JOURNAL
	,CHECKSUM = DEFAULT
	,DEFAULT MERGEBLOCKRATIO
	,MAP = TD_MAP1 
	(
         fiscal_month INTEGER
        ,plan_seasonid INTEGER
        ,channel_id INTEGER
        ,banner VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
        ,dept_id INTEGER
        ,size_profile VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
        ,rcpt_dollars DECIMAL(12,2)
        ,rcpt_units INTEGER
        ,plan_keys INTEGER
        ,po_keys INTEGER
        ,extract_date DATE
        ,rcd_update_timestamp TIMESTAMP(6) WITH TIME ZONE 
	) 
	PRIMARY INDEX (fiscal_month);