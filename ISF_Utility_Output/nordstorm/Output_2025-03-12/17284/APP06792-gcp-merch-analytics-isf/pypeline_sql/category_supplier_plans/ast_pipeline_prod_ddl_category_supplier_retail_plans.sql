/*
Category Supplier Retail Plan DDL
Author: Sara Riker
8/11/22: Create DDL

Creates Tables:
    {environment_schema}.category_level_retail_weekly_percents
    {environment_schema}.category_level_retail_plans
    {environment_schema}.category_level_retail_plans_weekly
    {environment_schema}.supplier_group_retail_plans
    {environment_schema}.supplier_group_retail_plans_weekly
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'category_level_retail_plans{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.category_level_retail_plans{env_suffix} ,FALLBACK , 
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
     month_idnt INTEGER 
    ,month_start_day_date DATE FORMAT 'YYYY-MM-DD'
    ,plan_snapshot_month_idnt INTEGER
    ,chnl_idnt INTEGER
    ,dept_idnt INTEGER
    ,category VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,price_band VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('USD','CAD')
    ,total_sales_units DECIMAL(12,2)
    ,total_sales_retail_dollars DECIMAL(36,6)
    ,total_demand_units DECIMAL(12,2)
    ,total_demand_retail_dollars DECIMAL(36,6)
    ,bop_inventory_units DECIMAL(12,2)
    ,bop_inventory_retail_dollars DECIMAL(36,6)
    ,receipt_need_units DECIMAL(12,2)
    ,receipt_need_retail_dollars DECIMAL(36,6)
    ,next_2months_sales_run_rate DECIMAL(12,2)
    ,receipt_need_less_reserve_units DECIMAL(12,2)
    ,receipt_need_less_reserve_retail_dollars DECIMAL(36,6)
    ,rack_transfer_units DECIMAL(12,2)
    ,rack_transfer_retail_dollars DECIMAL(36,6)
    ,qntrx_update_timestamp TIMESTAMP(6) WITH TIME ZONE 
    ,update_timestamp TIMESTAMP(6) WITH TIME ZONE 
)
PRIMARY INDEX(month_idnt, plan_snapshot_month_idnt, chnl_idnt, dept_idnt, category, price_band)
PARTITION BY RANGE_N(month_start_day_date BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY , NO RANGE)
;

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'supplier_group_retail_plans{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.supplier_group_retail_plans{env_suffix},FALLBACK , 
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
     month_idnt INTEGER 
    ,month_start_day_date DATE FORMAT 'YYYY-MM-DD'
    ,plan_snapshot_month_idnt INTEGER
    ,chnl_idnt INTEGER
    ,dept_idnt INTEGER
    ,supplier_group VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('USD','CAD')
    ,total_sales_units DECIMAL(12,2)
    ,total_sales_retail_dollars DECIMAL(36,6)
    ,total_demand_units DECIMAL(12,2)
    ,total_demand_retail_dollars DECIMAL(36,6)
    ,bop_inventory_units DECIMAL(12,2)
    ,bop_inventory_retail_dollars DECIMAL(36,6)
    ,receipt_need_units DECIMAL(12,2)
    ,receipt_need_retail_dollars DECIMAL(36,6)
    ,next_2months_sales_run_rate DECIMAL(12,2)
    ,receipt_need_less_reserve_units DECIMAL(12,2)
    ,receipt_need_less_reserve_retail_dollars DECIMAL(36,6)
    ,rack_transfer_units DECIMAL(12,2)
    ,rack_transfer_retail_dollars DECIMAL(36,6)
    ,qntrx_update_timestamp TIMESTAMP(6) WITH TIME ZONE 
    ,update_timestamp TIMESTAMP(6) WITH TIME ZONE 
)
PRIMARY INDEX(month_idnt, plan_snapshot_month_idnt, chnl_idnt, dept_idnt, supplier_group)
PARTITION BY RANGE_N(month_start_day_date BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY , NO RANGE)
;

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'mfp_weekly_percents{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.mfp_weekly_percents{env_suffix} ,FALLBACK , 
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
     month_idnt INTEGER 
    ,plan_snapshot_month_idnt INTEGER 
    ,week_idnt INTEGER
    ,week_start_day_date DATE FORMAT 'YYYY-MM-DD'
    ,chnl_idnt INTEGER
    ,dept_idnt INTEGER
    ,sales_plan_sp DECIMAL(36,6)
    ,sales_plan_sp_month DECIMAL(36,6)
    ,sales_plan_sp_month_wk_pct DECIMAL(36,6)
    ,demand_plan_sp DECIMAL(36,6)
    ,demand_plan_sp_month DECIMAL(36,6)
    ,demand_plan_sp_month_wk_pct DECIMAL(36,6)
    ,receipts_plan_sp DECIMAL(36,6)
    ,receipts_plan_sp_month DECIMAL(36,6)
    ,receipts_plan_sp_month_wk_pct DECIMAL(36,6)
    ,eoh_plan_sp DECIMAL(36,6)
    ,eoh_plan_sp_month DECIMAL(36,6)
    ,eoh_plan_sp_month_wk_pct DECIMAL(36,6)
    ,update_timestamp TIMESTAMP(6) WITH TIME ZONE 
)
PRIMARY INDEX(month_idnt, week_idnt, chnl_idnt, dept_idnt)
PARTITION BY RANGE_N(week_start_day_date BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY , NO RANGE)
;

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'category_level_retail_plans_weekly{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.category_level_retail_plans_weekly{env_suffix} ,FALLBACK , 
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
     month_idnt INTEGER
    ,plan_snapshot_month_idnt INTEGER 
    ,week_idnt INTEGER
    ,week_start_day_date DATE FORMAT 'YYYY-MM-DD'
    ,chnl_idnt INTEGER
    ,dept_idnt INTEGER
    ,category VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,price_band VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('USD','CAD')
    ,sales_plan_sp_month_wk_pct DECIMAL(36,6)
    ,demand_plan_sp_month_wk_pct DECIMAL(36,6)
    ,receipts_plan_sp_month_wk_pct DECIMAL(36,6)
    ,eoh_plan_sp_month_wk_pct DECIMAL(36,6)
    ,sales_unit DECIMAL(36,6)
    ,sales_retail_dollars DECIMAL(36,6)
    ,demand_units DECIMAL(12,2)
    ,demand_retail_dollars DECIMAL(36,6)
    ,receipt_units DECIMAL(12,2)
    ,receipt_retail_dollars DECIMAL(36,6)
    ,eoh_units DECIMAL(12,2)
    ,eoh_retail_dollars DECIMAL(36,6)
    ,qntrx_update_timestamp TIMESTAMP(6) WITH TIME ZONE 
    ,update_timestamp TIMESTAMP(6) WITH TIME ZONE 
)
PRIMARY INDEX(week_idnt, chnl_idnt, dept_idnt, category)
PARTITION BY RANGE_N(week_start_day_date BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY , NO RANGE)
;

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'supplier_group_retail_plans_weekly{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.supplier_group_retail_plans_weekly{env_suffix} ,FALLBACK , 
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
     month_idnt INTEGER
    ,plan_snapshot_month_idnt INTEGER 
    ,week_idnt INTEGER
    ,week_start_day_date DATE FORMAT 'YYYY-MM-DD'
    ,chnl_idnt INTEGER
    ,dept_idnt INTEGER
    ,supplier_group VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('USD','CAD')
    ,sales_plan_sp_month_wk_pct DECIMAL(36,6)
    ,demand_plan_sp_month_wk_pct DECIMAL(36,6)
    ,receipts_plan_sp_month_wk_pct DECIMAL(36,6)
    ,eoh_plan_sp_month_wk_pct DECIMAL(36,6)
    ,sales_unit DECIMAL(36,6)
    ,sales_retail_dollars DECIMAL(36,6)
    ,demand_units DECIMAL(12,2)
    ,demand_retail_dollars DECIMAL(36,6)
    ,receipt_units DECIMAL(12,2)
    ,receipt_retail_dollars DECIMAL(36,6)
    ,eoh_units DECIMAL(12,2)
    ,eoh_retail_dollars DECIMAL(36,6)
    ,qntrx_update_timestamp TIMESTAMP(6) WITH TIME ZONE 
    ,update_timestamp TIMESTAMP(6) WITH TIME ZONE 
)
PRIMARY INDEX(week_idnt, chnl_idnt, dept_idnt, supplier_group)
PARTITION BY RANGE_N(week_start_day_date BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY , NO RANGE)
;