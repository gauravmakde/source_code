/*
RP Anticipated Spend for OTB
Author: Sara Riker
Date Created: 8/25/22
Date Last Updated:  9/8/22

Purpose: Pulls RP anticipated spend from s3 bucket into a staging table

Builds table {environment_schema}.rp_anticipated_spend_staging
*/


CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'rp_anticipated_spend_staging', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.rp_anticipated_spend_staging
	,fallback
	,NO BEFORE JOURNAL
	,NO AFTER JOURNAL
	,CHECKSUM = DEFAULT
	,DEFAULT MERGEBLOCKRATIO
	,MAP = TD_MAP1 
	(
         week_idnt VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
        ,dept VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
        ,dept_desc VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
        ,cls_idnt VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
        ,scls_idnt VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
        ,supp_idnt VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
        ,supp_name VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
        ,banner_id VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
        ,ft_id VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC

        ,rp_antspnd_u VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
        ,rp_antspnd_c VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
        ,rp_antspnd_r VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	) 
	PRIMARY INDEX (week_idnt, banner_id, dept, cls_idnt, scls_idnt);

CALL SYS_MGMT.S3_TPT_LOAD ('{environment_schema}','rp_anticipated_spend_staging','us-west-2','rpds-qtrx-extract-prod','prod/apt/','rpspendRetail.csv','7C',OUT_MESSAGE); 