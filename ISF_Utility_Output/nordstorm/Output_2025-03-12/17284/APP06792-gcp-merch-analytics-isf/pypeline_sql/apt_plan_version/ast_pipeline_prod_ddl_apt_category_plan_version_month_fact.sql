/*
DDL for APT Plan Version monthly table
Author: Sara Scott

Creates Tables:
    {environment_schema}.apt_plan_version_month_fact{env_suffix}
*/


CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'apt_category_plan_version_month_fact{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.apt_category_plan_version_month_fact{env_suffix}, FALLBACK,
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
	selling_country										VARCHAR(2)
	,selling_brand										VARCHAR(20)
	,category 											VARCHAR(50)	
	,category_group										VARCHAR(50)
	,dept_idnt											INTEGER
	,dept_desc											VARCHAR(40)
	,div_idnt											INTEGER
	,div_desc											VARCHAR(40)
	,subdiv_idnt										INTEGER
	,subdiv_desc										VARCHAR(40)
	,snapshot_plan_month_idnt							INTEGER
	,snapshot_month_id									VARCHAR(40)
	,snapshot_month_label								VARCHAR(40)
	,month_id											VARCHAR(40)
	,month_label										VARCHAR(40)
	,half_label											VARCHAR(40)
	,quarter											VARCHAR(40)
	,fiscal_year_num									INTEGER
	,alternate_inventory_model							VARCHAR(20)
	,net_sales_retail_amount							DECIMAL(38,9)
	,net_sales_cost_amount								DECIMAL(38,9)
	,net_sales_units									DECIMAL(16,4)
	,average_inventory_retail_amount					DECIMAL(38,9)
	,average_inventory_cost_amount						DECIMAL(38,9)
	,average_inventory_units							DECIMAL(16,4)
	,beginning_of_period_inventory_retail_amount		DECIMAL(38,9)
	,beginning_of_period_inventory_cost_amount			DECIMAL(38,9)
	,beginning_of_period_inventory_units				DECIMAL(16,4)
	,rcpt_retail_amount				                    DECIMAL(38,9)
	,rcpt_cost_amount					                DECIMAL(38,9)
	,rcpt_units						                    DECIMAL(16,4)
    ,rcpt_less_reserve_retail_amount				    DECIMAL(38,9)
	,rcpt_less_reserve_cost_amount					    DECIMAL(38,9)
	,rcpt_less_reserve_units						    DECIMAL(16,4)

)
PRIMARY INDEX(dept_idnt,category,month_id)
PARTITION BY RANGE_N(snapshot_plan_month_idnt BETWEEN 201901 AND 202512 EACH 1);

GRANT SELECT ON {environment_schema}.apt_category_plan_version_month_fact{env_suffix} TO PUBLIC;