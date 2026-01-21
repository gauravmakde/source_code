/*
APT reporting ly cost DDL
Author: Michelle Du
Date Created: 10/18/22
Date Last Updated: 12/15/22

Datalab: t2dl_das_apt_cost_reporting
Creates Tables:
    - category_priceband_cost_ly
    - category_suppliergroup_cost_ly
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'category_priceband_cost_ly{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.category_priceband_cost_ly{env_suffix} ,FALLBACK ,
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
	channel_num INTEGER
	,cluster_name VARCHAR(23) CHARACTER SET UNICODE NOT CASESPECIFIC
	,channel_country VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
	,banner VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC
	,month_idnt INTEGER NOT NULL
	,week_idnt INTEGER NOT NULL
	,month_start_day_date DATE FORMAT 'YYYY-MM-DD'
    ,month_end_day_date DATE FORMAT 'YYYY-MM-DD'
	,week_start_day_date DATE FORMAT 'YYYY-MM-DD'
	,week_end_day_date DATE FORMAT 'YYYY-MM-DD'
	,department_num INTEGER
	,DROPSHIP_IND CHAR(1)
	,quantrix_category VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC
	,price_band VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	,currency VARCHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('USD','CAD')
	,net_sls_r DECIMAL(38, 4)
	,net_sls_c DECIMAL(38, 4)
	,net_sls_units INTEGER
	,gross_sls_r DECIMAL(38, 4)
	,gross_sls_units INTEGER
	,demand_ttl_r DECIMAL(38, 6)
	,demand_ttl_units INTEGER
	,eoh_ttl_c DECIMAL(38, 2)
	,eoh_ttl_units INTEGER
	,boh_ttl_c DECIMAL(38, 2)
	,boh_ttl_units INTEGER
	,beginofmonth_bop_c DECIMAL(38, 2)
	,beginofmonth_bop_u INTEGER
	,ttl_porcpt_c DECIMAL(38, 4)
	,ttl_porcpt_c_units INTEGER
	,pah_tsfr_in_c DECIMAL(38, 4)
	,pah_tsfr_in_u INTEGER
	,rs_stk_tsfr_in_c DECIMAL(38, 4)
	,rs_stk_tsfr_in_u INTEGER
)
PRIMARY INDEX(week_idnt, week_start_day_date, month_idnt, channel_num, channel_country, banner, department_num, quantrix_category, price_band)
PARTITION BY RANGE_N(week_start_day_date BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY , NO RANGE)
;

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'category_suppliergroup_cost_ly{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.category_suppliergroup_cost_ly{env_suffix} ,FALLBACK ,
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
	channel_num INTEGER
	,cluster_name VARCHAR(23) CHARACTER SET UNICODE NOT CASESPECIFIC
	,channel_country VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
	,banner VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC
	,month_idnt INTEGER NOT NULL
	,week_idnt INTEGER NOT NULL
	,month_start_day_date DATE FORMAT 'YYYY-MM-DD'
    ,month_end_day_date DATE FORMAT 'YYYY-MM-DD'
	,week_start_day_date DATE FORMAT 'YYYY-MM-DD'
	,week_end_day_date DATE FORMAT 'YYYY-MM-DD'
	,department_num INTEGER
	,DROPSHIP_IND CHAR(1)
	,quantrix_category VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC
	,supplier_group VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,currency VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('USD','CAD')
	,net_sls_r DECIMAL(38, 4)
	,net_sls_c DECIMAL(38, 4)
	,net_sls_units INTEGER
	,gross_sls_r DECIMAL(38, 4)
	,gross_sls_units INTEGER
	,demand_ttl_r DECIMAL(38, 6)
	,demand_ttl_units INTEGER
	,eoh_ttl_c DECIMAL(38, 2)
	,eoh_ttl_units INTEGER
	,boh_ttl_c DECIMAL(38, 2)
	,boh_ttl_units INTEGER
	,beginofmonth_bop_c DECIMAL(38, 2)
	,beginofmonth_bop_u INTEGER
	,ttl_porcpt_c DECIMAL(38, 4)
	,ttl_porcpt_c_units INTEGER
	,pah_tsfr_in_c DECIMAL(38, 4)
	,pah_tsfr_in_u INTEGER
	,rs_stk_tsfr_in_c DECIMAL(38, 4)
	,rs_stk_tsfr_in_u INTEGER
)
PRIMARY INDEX(week_idnt, week_start_day_date, month_idnt,channel_num, banner, channel_country, department_num, quantrix_category, supplier_group)
PARTITION BY RANGE_N(week_start_day_date BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY , NO RANGE)
;
