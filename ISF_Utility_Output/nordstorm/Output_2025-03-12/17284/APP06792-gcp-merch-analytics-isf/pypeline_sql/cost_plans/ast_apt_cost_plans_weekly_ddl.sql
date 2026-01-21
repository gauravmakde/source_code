/*
APT Cost Plan Weekly DDL
Author: Sara Riker
Date Created: 1/19/22
Date Updated: 7/6/23

Datalab: t2dl_das_apt_cost_reporting
Creates Tables:
    - category_channel_cost_plans_weekly
    - suppliergroup_channel_cost_plans_weekly
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'category_channel_cost_plans_weekly', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.category_channel_cost_plans_weekly ,FALLBACK , 
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
     month_idnt INTEGER
    ,month_start_day_date DATE 
    ,month_end_day_date DATE                                                     
    ,week_idnt INTEGER
    ,week_start_day_date DATE
    ,week_end_day_date DATE
    ,chnl_idnt INTEGER 
    ,banner VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,country VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,channel VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC 
    
    ,dept_idnt VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,alternate_inventory_model VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,fulfill_type_num INTEGER
    ,category VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,price_band	VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,currency_code VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,demand_units DECIMAL(38,8)
    ,demand_r_dollars DECIMAL(38,13)
    ,gross_sls_units DECIMAL(38,8)
    ,gross_sls_r_dollars DECIMAL(38,13)
    ,return_units DECIMAL(38,8)
    ,return_r_dollars DECIMAL(38,13)
    ,net_sls_units DECIMAL(38,8)
    ,net_sls_r_dollars DECIMAL(38,13)
    ,net_sls_c_dollars DECIMAL(38,13)
    ,next_2months_sales_run_rate FLOAT(15)
    ,avg_inv_ttl_c DECIMAL(38,13) 
    ,avg_inv_ttl_u DECIMAL(38,8)
    ,plan_bop_c_dollars DECIMAL(38,13)
	,plan_bop_c_units DECIMAL(38,8)
	,plan_eop_c_dollars DECIMAL(38,13)
	,plan_eop_c_units DECIMAL(38,8)
    ,rcpt_need_r DECIMAL(38,13)
    ,rcpt_need_c DECIMAL(38,8)
    ,rcpt_need_u DECIMAL(38,13)
    ,rcpt_need_lr_r DECIMAL(38,8)
    ,rcpt_need_lr_c DECIMAL(38,8)
    ,rcpt_need_lr_u DECIMAL(38,13)
    ,rcd_update_timestamp TIMESTAMP(6) WITH TIME ZONE
)
PRIMARY INDEX(month_idnt, week_idnt, chnl_idnt, dept_idnt, category)
;

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'suppliergroup_channel_cost_plans_weekly', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.suppliergroup_channel_cost_plans_weekly ,FALLBACK , 
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
     month_idnt INTEGER
    ,month_start_day_date DATE 
    ,month_end_day_date DATE                                                     
    ,week_idnt INTEGER
    ,week_start_day_date DATE
    ,week_end_day_date DATE
    ,chnl_idnt INTEGER 
    ,banner VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,country VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC     
    ,dept_idnt VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,alternate_inventory_model VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,fulfill_type_num INTEGER
    ,category VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,supplier_group	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,currency_code VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,demand_units DECIMAL(38,8)
    ,demand_r_dollars DECIMAL(38,13)
    ,gross_sls_units DECIMAL(38,8)
    ,gross_sls_r_dollars DECIMAL(38,13)
    ,return_units DECIMAL(38,8)
    ,return_r_dollars DECIMAL(38,13)
    ,net_sls_units DECIMAL(38,8)
    ,net_sls_r_dollars DECIMAL(38,13)
    ,net_sls_c_dollars DECIMAL(38,13)
    ,next_2months_sales_run_rate FLOAT(15)
    ,avg_inv_ttl_c DECIMAL(38,13) 
    ,avg_inv_ttl_u DECIMAL(38,8)
    ,plan_bop_c_dollars DECIMAL(38,13)
	,plan_bop_c_units DECIMAL(38,8)
	,plan_eop_c_dollars DECIMAL(38,13)
	,plan_eop_c_units DECIMAL(38,8)
    ,plan_rp_rcpt_r DECIMAL(38,13)
    ,plan_rp_rcpt_c DECIMAL(38,8)
    ,plan_rp_rcpt_u DECIMAL(38,13)
    ,plan_rp_rcpt_lr_r DECIMAL(38,13)
    ,plan_rp_rcpt_lr_c DECIMAL(38,8)
    ,plan_rp_rcpt_lr_u DECIMAL(38,13)
    ,plan_nrp_rcpt_r DECIMAL(38,13)
    ,plan_nrp_rcpt_c DECIMAL(38,8)
    ,plan_nrp_rcpt_u DECIMAL(38,13)
    ,plan_nrp_rcpt_lr_r DECIMAL(38,13)
    ,plan_nrp_rcpt_lr_c DECIMAL(38,8)
    ,plan_nrp_rcpt_lr_u DECIMAL(38,13)
    ,rcpt_need_r DECIMAL(38,13)
    ,rcpt_need_c DECIMAL(38,8)
    ,rcpt_need_u DECIMAL(38,13)
    ,rcpt_need_lr_r DECIMAL(38,8)
    ,rcpt_need_lr_c DECIMAL(38,8)
    ,rcpt_need_lr_u DECIMAL(38,13)
    ,plan_pah_in_r DECIMAL(38,8)
    ,plan_pah_in_c DECIMAL(38,8)
    ,plan_pah_in_u DECIMAL(38,13)
    ,rcd_update_timestamp TIMESTAMP(6) WITH TIME ZONE
)
PRIMARY INDEX(month_idnt, week_idnt, chnl_idnt, dept_idnt, category)
;