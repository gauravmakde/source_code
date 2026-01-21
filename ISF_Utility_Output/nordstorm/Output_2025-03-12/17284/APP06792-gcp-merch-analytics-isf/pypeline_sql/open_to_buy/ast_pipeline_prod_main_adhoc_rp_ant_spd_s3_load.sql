--This creates a stg table and loads the interim rp file for any corrections to elapsed weeks

DROP TABLE t2dl_das_open_to_buy.rp_anticipated_spend_adhoc;

CREATE MULTISET TABLE t2dl_das_open_to_buy.rp_anticipated_spend_adhoc
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

	GRANT SELECT ON t2dl_das_open_to_buy.rp_anticipated_spend_adhoc TO PUBLIC;

CALL SYS_MGMT.S3_TPT_LOAD ('t2dl_das_open_to_buy','rp_anticipated_spend_adhoc','us-west-2','rpds-qtrx-extract-prod','prod/apt/','rpspendRetailwk.csv','7C',OUT_MESSAGE);
