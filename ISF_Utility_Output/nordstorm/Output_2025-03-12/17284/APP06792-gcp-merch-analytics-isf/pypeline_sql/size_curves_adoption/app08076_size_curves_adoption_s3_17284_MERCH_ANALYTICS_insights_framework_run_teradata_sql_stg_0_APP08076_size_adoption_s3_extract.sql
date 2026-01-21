/*
Size Curves Adoption s3 TPT
Author: Sara Riker
12/11/23: Create Script

    {environment_schema}: t2dl_das_size
    pulls adoption csv from s3 into staging table
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_size', 'adoption_staging', OUT_RETURN_MSG);
CREATE MULTISET TABLE t2dl_das_size.adoption_staging
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
	)
	PRIMARY INDEX (fiscal_month);

CALL SYS_MGMT.S3_TPT_LOAD ('t2dl_das_size','adoption_staging','us-west-2','size-curve','adoption/','adoption_extract.csv','2C',OUT_MESSAGE);