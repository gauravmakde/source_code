/*
APT reporting MFP Cp cost DDL
Author: Michelle Du
Date Created: 10/18/22
Date Last Updated: 10/24/22

Datalab: t2dl_das_apt_cost_reporting
Creates Tables:
    - mfp_cp_cost_channel_weekly
    - mfp_cp_cost_banner_weekly
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'mfp_cp_cost_channel_weekly{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.mfp_cp_cost_channel_weekly{env_suffix} ,FALLBACK ,
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
	month_idnt INTEGER NOT NULL
	,month_start_day_date DATE FORMAT 'YYYY-MM-DD'
    ,month_end_day_date DATE FORMAT 'YYYY-MM-DD'
	,week_idnt INTEGER NOT NULL
    ,week_start_day_date DATE FORMAT 'YYYY-MM-DD'
    ,week_end_day_date DATE FORMAT 'YYYY-MM-DD'
    ,quarter_label VARCHAR(7) CHARACTER SET UNICODE NOT CASESPECIFIC
	,fiscal_year_num INTEGER NOT NULL
	,fulfill_type_num INTEGER NOT NULL
	,channel_country VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC
	,currency VARCHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC
	,chnl_idnt INTEGER
	,dept_idnt INTEGER
	,dept_label VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC
	,division_label VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC
	,subdivision_label VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC
	,cp_demand_r_dollars DECIMAL(38, 4)
	,cp_demand_u DECIMAL(38, 4)
	,cp_gross_sls_r_dollars DECIMAL(38, 4)
	,cp_gross_sls_u DECIMAL(38, 4)
	,cp_return_r_dollars DECIMAL(38, 4)
	,cp_return_u DECIMAL(38, 4)
	,cp_net_sls_r_dollars DECIMAL(38, 4)
	,cp_net_sls_c_dollars DECIMAL(38, 4)
	,cp_net_sls_u DECIMAL(38, 4)
)
PRIMARY INDEX(week_idnt, week_start_day_date, month_idnt, chnl_idnt, channel_country, dept_idnt)
PARTITION BY RANGE_N(week_start_day_date BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY , NO RANGE)
;

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'mfp_cp_cost_banner_weekly{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.mfp_cp_cost_banner_weekly{env_suffix} ,FALLBACK ,
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
	month_idnt INTEGER NOT NULL
	,month_start_day_date DATE FORMAT 'YYYY-MM-DD'
    ,month_end_day_date DATE FORMAT 'YYYY-MM-DD'
	,week_idnt INTEGER NOT NULL
	,week_start_day_date DATE FORMAT 'YYYY-MM-DD'
	,week_end_day_date DATE FORMAT 'YYYY-MM-DD'
    ,quarter_label VARCHAR(7) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,fiscal_year_num INTEGER NOT NULL
    ,fulfill_type_num INTEGER NOT NULL
    ,channel_country VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC
	,banner VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC
	,dept_idnt INTEGER
	,dept_label VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC
	,division_label VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC
	,subdivision_label VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC
	,currency VARCHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC
	,cp_bop_ttl_c_dollars DECIMAL(38, 4)
	,cp_bop_ttl_u DECIMAL(38, 4)
	,cp_beginofmonth_bop_c DECIMAL(38, 4)
	,cp_beginofmonth_bop_u DECIMAL(38, 4)
	,cp_eop_ttl_c_dollars DECIMAL(38, 4)
	,cp_eop_ttl_u DECIMAL(38, 4)
	,cp_eop_eom_c_dollars DECIMAL(38, 4)
	,cp_eop_eom_u DECIMAL(38, 4)
	,cp_rept_need_c_dollars DECIMAL(38, 4)
	,cp_rept_need_u DECIMAL(38, 4)
	,cp_rept_need_lr_c_dollars DECIMAL(38, 4)
	,cp_rept_need_lr_u DECIMAL(38, 4)
)
PRIMARY INDEX(week_idnt, week_start_day_date, month_idnt, banner, channel_country, dept_idnt)
PARTITION BY RANGE_N(week_start_day_date BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY , NO RANGE)
;