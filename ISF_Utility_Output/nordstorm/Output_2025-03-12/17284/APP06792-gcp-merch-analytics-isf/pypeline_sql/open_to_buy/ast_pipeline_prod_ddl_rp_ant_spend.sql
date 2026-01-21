/*
RP Anticipated Spend for OTB
Author: Sara Riker
Date Created: 8/25/22
Date Last Updated: 9/8/22

Purpose: DDL for RP anticipated spend 

Creates tables: 
  - {environment_schema}.rp_anticipated_spend
  - {environment_schema}.rp_anticipated_spend_hist
  - {environment_schema}.rp_anticipated_spend_current
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'rp_anticipated_spend', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.rp_anticipated_spend
	,FALLBACK
	,NO BEFORE JOURNAL
	,NO AFTER JOURNAL
	,CHECKSUM = DEFAULT
	,DEFAULT MERGEBLOCKRATIO
	,MAP = TD_MAP1 
	(
         week_idnt INTEGER
        ,dept_idnt INTEGER
        ,dept_desc VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
        ,cls_idnt INTEGER
        ,scls_idnt INTEGER
        ,supp_idnt INTEGER
        ,supp_desc VARCHAR(40)
        ,banner_id INTEGER
        ,ft_id INTEGER

        ,rp_antspnd_u DECIMAL(12,4) 
        ,rp_antspnd_c DECIMAL(12,4) 
        ,rp_antspnd_r DECIMAL(12,4) 
        ,update_timestamp TIMESTAMP(6) WITH TIME ZONE 
)
PRIMARY INDEX(week_idnt, banner_id, dept_idnt, cls_idnt, scls_idnt)
;


CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'rp_anticipated_spend_hist', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.rp_anticipated_spend_hist
	,FALLBACK
	,NO BEFORE JOURNAL
	,NO AFTER JOURNAL
	,CHECKSUM = DEFAULT
	,DEFAULT MERGEBLOCKRATIO
	,MAP = TD_MAP1 
	(
         week_idnt INTEGER
        ,year_454 INTEGER
        ,month_454 INTEGER
        ,banner_id INTEGER
        ,ft_id INTEGER
        ,dept_idnt INTEGER
        ,dept_desc VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
        ,category VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
        ,supp_idnt INTEGER
        ,supp_desc VARCHAR(40)
        ,rp_antspnd_u DECIMAL(12,4) 
        ,rp_antspnd_c DECIMAL(12,4) 
        ,rp_antspnd_r DECIMAL(12,4) 
        ,rcd_load_date DATE
        ,update_timestamp TIMESTAMP(6) WITH TIME ZONE 
)
PRIMARY INDEX(week_idnt, banner_id, dept_idnt, category)
;

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'rp_anticipated_spend_current', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.rp_anticipated_spend_current
	,FALLBACK
	,NO BEFORE JOURNAL
	,NO AFTER JOURNAL
	,CHECKSUM = DEFAULT
	,DEFAULT MERGEBLOCKRATIO
	,MAP = TD_MAP1 
	(
         week_idnt INTEGER
        ,year_454 INTEGER
        ,month_454 INTEGER
        ,banner_id INTEGER
        ,ft_id INTEGER
        ,dept_idnt INTEGER
        ,dept_desc VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
        ,category VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
        ,supp_idnt INTEGER
        ,supp_desc VARCHAR(40)
        ,rp_antspnd_u DECIMAL(12,4) 
        ,rp_antspnd_c DECIMAL(12,4) 
        ,rp_antspnd_r DECIMAL(12,4) 
        ,rcd_load_date DATE
        ,update_timestamp TIMESTAMP(6) WITH TIME ZONE 
)
PRIMARY INDEX(week_idnt, banner_id, dept_idnt, category)
;

