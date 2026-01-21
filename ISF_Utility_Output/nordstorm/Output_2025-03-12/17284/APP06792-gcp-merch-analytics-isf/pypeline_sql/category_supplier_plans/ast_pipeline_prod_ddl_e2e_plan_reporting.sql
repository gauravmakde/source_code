/*
Plan & LY/LLY Data DDL
Author: Sara Riker & David Selover
8/15/22: Create DDL for e2e dashboard table

Creates Tables:
    {environment_schema}.e2e_selection_planning 
*/
 
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'e2e_selection_planning', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.e2e_selection_planning ,FALLBACK , 
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
      fiscal_month_num INTEGER 
    , fiscal_year_num INTEGER 
    , month_idnt INTEGER
    , month_start_day_date DATE 
    , plan_month_idnt INTEGER 
	, ly_month_idnt INTEGER 
	, lly_month_idnt INTEGER
	, month_label VARCHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC 
	, quarter_idnt INTEGER
	, quarter_label VARCHAR(7) CHARACTER SET UNICODE NOT CASESPECIFIC 
    , channel_brand VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC 
	, division_num INTEGER
	, division_name VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC 
	, subdivision_num INTEGER
	, subdivision_name VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC 
	, dept_idnt INTEGER
	, dept_name VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC 
	, category VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC 
    -- plan 
	, plan_dmd_r DECIMAL(36,6)
	, plan_dmd_u DECIMAL(12,2)
	, plan_net_sls_r DECIMAL(36,6)
	, plan_net_sls_u DECIMAL(12,2)
	, plan_rcpt_r DECIMAL(36,6)
	, plan_rcpt_u DECIMAL(12,2)
	, model_los_ccs DECIMAL(12,2)
	, plan_los_ccs DECIMAL(12,2)
	, model_new_ccs DECIMAL(12,2)
	, plan_new_ccs DECIMAL(12,2)
	, model_cf_ccs DECIMAL(12,2)
	, plan_cf_ccs DECIMAL(12,2)
	, model_sell_offs_ccs DECIMAL(12,2)
	, plan_sell_off_ccs DECIMAL(12,2)
	, ly_sell_off_ccs DECIMAL(12,2)
	, lly_sell_off_ccs DECIMAL(12,2)
    -- ly/lly
	, ly_demand_$ DECIMAL(12,2)
	, ly_demand_u DECIMAL(12,2)
	, ly_sales_$ DECIMAL(12,2)
	, ly_sales_u DECIMAL(12,2)
	, ly_rcpt_$ DECIMAL(12,2)
	, ly_rcpt_u DECIMAL(12,2)
	, ly_los_ccs DECIMAL(12,2)
	, ly_new_ccs DECIMAL(12,2)
	, ly_cf_ccs DECIMAL(12,2)
	, lly_demand_$ DECIMAL(12,2)
	, lly_demand_u DECIMAL(12,2)
	, lly_sales_$ DECIMAL(12,2)
	, lly_sales_u DECIMAL(12,2)
	, lly_rcpt_$ DECIMAL(12,2)
    , lly_rcpt_u DECIMAL(12,2)
	, lly_los_ccs DECIMAL(12,2)
	, lly_new_ccs DECIMAL(12,2)
	, lly_cf_ccs DECIMAL(12,2)
	-- CM data
	, new_ccs_cm INTEGER
    -- update info
    , ly_lly_update_timestamp TIMESTAMP(6) WITH TIME ZONE
    , plan_update_timestamp TIMESTAMP(6) WITH TIME ZONE
    , cm_data_update_timestamp TIMESTAMP(6) WITH TIME ZONE
    , update_timestamp TIMESTAMP(6) WITH TIME ZONE 
)
PRIMARY INDEX(fiscal_year_num, fiscal_month_num, month_idnt, channel_brand, dept_idnt, category)
PARTITION BY RANGE_N(month_start_day_date BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY , NO RANGE)
;

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'sel_dept_category_plan_history', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.sel_dept_category_plan_history ,FALLBACK , 
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
      snapshot_plan_month_idnt INTEGER 
	, snapshot_start_day_date DATE
    , month_idnt INTEGER
    , channel_brand VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC 
	-- hierarchy
	, dept_idnt INTEGER
	, category VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC 
    -- plan 
	, plan_dmd_r DECIMAL(36,6)
	, plan_dmd_u DECIMAL(12,2)
	, plan_net_sls_r DECIMAL(36,6)
	, plan_net_sls_u DECIMAL(12,2)
	, plan_rcpt_r DECIMAL(36,6)
	, plan_rcpt_u DECIMAL(12,2)
	, model_los_ccs DECIMAL(12,2)
	, plan_los_ccs DECIMAL(12,2)
	, model_new_ccs DECIMAL(12,2)
	, plan_new_ccs DECIMAL(12,2)
	, model_cf_ccs DECIMAL(12,2)
	, plan_cf_ccs DECIMAL(12,2)
	, model_sell_off_ccs DECIMAL(12,2)
	, plan_sell_off_ccs DECIMAL(12,2)
	, ly_sell_off_ccs DECIMAL(12,2)
	, lly_sell_off_ccs DECIMAL(12,2)
	, model_sell_off_ccs_rate DECIMAL(12,2)
	, plan_sell_off_ccs_rate DECIMAL(12,2)
	, ly_sell_off_ccs_rate DECIMAL(12,2)
	, lly_sell_off_ccs_rate DECIMAL(12,2)

	, rcd_load_date DATE 
    -- update info
    , update_timestamp TIMESTAMP(6) WITH TIME ZONE 
)
PRIMARY INDEX(snapshot_plan_month_idnt, month_idnt, channel_brand, dept_idnt, category)
PARTITION BY RANGE_N(snapshot_start_day_date BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY , NO RANGE)
;