/*
APT Cost Plan History DDL
Author: Sara Riker
Date Created: 9/29/22
Date Last Updated: 9/29/22

Datalab: t2dl_das_apt_cost_reporting
Creates Tables:
    - merch_assortment_category_cluster_plan_history
    - merch_assortment_category_country_plan_history
    - merch_assortment_supplier_cluster_plan_history
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'merch_assortment_category_cluster_plan_history{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.merch_assortment_category_cluster_plan_history{env_suffix} ,FALLBACK , 
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
     snapshot_plan_month_idnt INTEGER 
    ,snapshot_start_day_date DATE
    ,event_time	TIMESTAMP(6) 
    ,cluster_name VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,banner VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,cahnnel VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,country VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,category VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,price_band	VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,dept_idnt VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,month_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,fiscal_month_idnt INTEGER
    ,alternate_inventory_model VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,demand_dollar_currency_code VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,demand_dollar_amount DECIMAL(38,9)
    ,demand_units DECIMAL(16,4)
    ,gross_sales_dollar_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,gross_sales_dollar_amount DECIMAL(38,9)
    ,gross_sales_units DECIMAL(16,4)
    ,returns_dollar_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,returns_dollar_amount	DECIMAL(38,9)
    ,returns_units DECIMAL(16,4)
    ,net_sales_units DECIMAL(16,4)
    ,net_sales_retail_currency_code	VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,net_sales_retail_amount DECIMAL(38,9)
    ,net_sales_cost_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,net_sales_cost_amount DECIMAL(38,9)
    ,gross_margin_retail_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,gross_margin_retail_amount	DECIMAL(38,9)
    ,demand_next_two_month_run_rate DECIMAL(16,4)
    ,sales_next_two_month_run_rate DECIMAL(16,4)
    ,dw_sys_load_tmstp TIMESTAMP(6)
    ,rcd_update_timestamp TIMESTAMP(6) WITH TIME ZONE
)
PRIMARY INDEX(snapshot_plan_month_idnt, fiscal_month_idnt, cluster_name, dept_idnt, category)
PARTITION BY RANGE_N(snapshot_start_day_date BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY , NO RANGE)
;


CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'merch_assortment_category_country_plan_history{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.merch_assortment_category_country_plan_history{env_suffix} ,FALLBACK , 
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
     snapshot_plan_month_idnt INTEGER 
    ,snapshot_start_day_date DATE
    ,fiscal_month_idnt INTEGER
    ,event_time	TIMESTAMP(6)
    ,selling_country VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,selling_brand VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,category VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,price_band	VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,dept_idnt	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,month_id	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,alternate_inventory_model	VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,average_inventory_units	DECIMAL(16,4)
    ,average_inventory_retail_currency_code	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,average_inventory_retail_amount	DECIMAL(38,9)
    ,average_inventory_cost_currency_code	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,average_inventory_cost_amount	DECIMAL(38,9)
    ,beginning_of_period_inventory_units	DECIMAL(16,4)
    ,beginning_of_period_inventory_retail_currency_code	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,beginning_of_period_inventory_retail_amount	DECIMAL(38,9)
    ,beginning_of_period_inventory_cost_currency_code	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,beginning_of_period_inventory_cost_amount	DECIMAL(38,9)
    ,return_to_vendor_units	DECIMAL(16,4)
    ,return_to_vendor_retail_currency_code	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,return_to_vendor_retail_amount	DECIMAL(38,9)
    ,return_to_vendor_cost_currency_code	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,return_to_vendor_cost_amount	DECIMAL(38,9)
    ,rack_transfer_units	DECIMAL(16,4)
    ,rack_transfer_retail_currency_code	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,rack_transfer_retail_amount	DECIMAL(38,9)
    ,rack_transfer_cost_currency_code	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,rack_transfer_cost_amount	DECIMAL(38,9)
    ,active_inventory_in_units	DECIMAL(16,4)
    ,active_inventory_in_retail_currency_code	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,active_inventory_in_retail_amount	DECIMAL(38,9)
    ,active_inventory_in_cost_currency_code	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,active_inventory_in_cost_amount	DECIMAL(38,9)
    ,active_inventory_out_units	DECIMAL(16,4)
    ,active_inventory_out_retail_currency_code	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,active_inventory_out_retail_amount	DECIMAL(38,9)
    ,active_inventory_out_cost_currency_code	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,active_inventory_out_cost_amount	DECIMAL(38,9)
    ,receipts_units	DECIMAL(16,4)
    ,receipts_retail_currency_code VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,receipts_retail_amount	DECIMAL(38,9)
    ,receipts_cost_currency_code VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,receipts_cost_amount DECIMAL(38,9)
    ,receipts_less_reserve_units DECIMAL(16,4)
    ,receipts_less_reserve_retail_currency_code	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,receipts_less_reserve_retail_amount DECIMAL(38,9)
    ,receipts_less_reserve_cost_currency_code	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,receipts_less_reserve_cost_amount	DECIMAL(38,9)
    ,pack_and_hold_transfer_in_units	DECIMAL(16,4)
    ,pack_and_hold_transfer_in_retail_currency_code	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,pack_and_hold_transfer_in_retail_amount	DECIMAL(38,9)
    ,pack_and_hold_transfer_in_cost_currency_code	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,pack_and_hold_transfer_in_cost_amount	DECIMAL(38,9)
    ,shrink_units DECIMAL(16,4)
    ,shrink_retail_currency_code	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,shrink_retail_amount	DECIMAL(38,9)
    ,shrink_cost_currency_code	VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,shrink_cost_amount	DECIMAL(38,9)
    ,quantrix_update	TIMESTAMP(6)
    ,rcd_update_timestamp TIMESTAMP(6) WITH TIME ZONE
)
PRIMARY INDEX(snapshot_plan_month_idnt, selling_country, selling_brand, dept_idnt, price_band)
PARTITION BY RANGE_N(snapshot_start_day_date BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY , NO RANGE)
;

-- Supplier Clust Plans

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'merch_assortment_supplier_cluster_plan_history{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.merch_assortment_supplier_cluster_plan_history{env_suffix} ,FALLBACK , 
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
     snapshot_plan_month_idnt INTEGER 
    ,snapshot_start_day_date DATE
    ,fiscal_month_idnt INTEGER
    ,event_time TIMESTAMP(6)
    ,selling_country VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC  
    ,selling_brand VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,cluster_name VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('PRICE','BRAND','HYBRID','RACK_CANADA_STORES', 'RACK_STORES', 'RACK_ONLINE','NORDSTROM_STORES','NORDSTROM_ONLINE','NORDSTROM_CANADA_STORES','NORDSTROM_CANADA_ONLINE')
    ,category VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,supplier_group VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,dept_idnt INTEGER
    ,month_id VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,alternate_inventory_model VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,demand_dollar_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,demand_dollar_amount DECIMAL(38,9)
    ,demand_units DECIMAL(16,4)
    ,gross_sales_dollar_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,gross_sales_dollar_amount DECIMAL(38,9)
    ,gross_sales_units DECIMAL(16,4)
    ,returns_dollar_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,returns_dollar_amount DECIMAL(38,9)
    ,returns_units DECIMAL(16,4)
    ,net_sales_units DECIMAL(16,4)
    ,net_sales_retail_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,net_sales_retail_amount DECIMAL(38,9)
    ,net_sales_cost_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,net_sales_cost_amount DECIMAL(38,9)
    ,product_margin_retail_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,product_margin_retail_amount DECIMAL(38,9)
    ,demand_next_two_month_run_rate DECIMAL(16,4)
    ,sales_next_two_month_run_rate DECIMAL(16,4)    	
    ,replenishment_receipts_units DECIMAL(16,4)
    ,replenishment_receipts_retail_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,replenishment_receipts_retail_amount DECIMAL(38,9)
    ,replenishment_receipts_cost_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,replenishment_receipts_cost_amount DECIMAL(38,9)
    ,replenishment_receipts_less_reserve_units DECIMAL(16,4)
    ,replenishment_receipts_less_reserve_retail_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,replenishment_receipts_less_reserve_retail_amount DECIMAL(38,9)
    ,replenishment_receipts_less_reserve_cost_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,replenishment_receipts_less_reserve_cost_amount DECIMAL(38,9)
    ,nonreplenishment_receipts_units DECIMAL(16,4)
    ,nonreplenishment_receipts_retail_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,nonreplenishment_receipts_retail_amount DECIMAL(38,9)
    ,nonreplenishment_receipts_cost_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,nonreplenishment_receipts_cost_amount DECIMAL(38,9)
    ,nonreplenishment_receipts_less_reserve_units DECIMAL(16,4)
    ,nonreplenishment_receipts_less_reserve_retail_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,nonreplenishment_receipts_less_reserve_retail_amount DECIMAL(38,9)
    ,nonreplenishment_receipts_less_reserve_cost_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,nonreplenishment_receipts_less_reserve_cost_amount DECIMAL(38,9)
    ,dropship_receipt_units DECIMAL(16,4)
    ,dropship_receipt_retail_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,dropship_receipt_retail_amount DECIMAL(38,9)
    ,dropship_receipt_cost_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,dropship_receipt_cost_amount DECIMAL(38,9)
    ,average_inventory_units DECIMAL(16,4)
    ,average_inventory_retail_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,average_inventory_retail_amount DECIMAL(38,9)
    ,average_inventory_cost_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,average_inventory_cost_amount DECIMAL(38,9)
    ,beginning_of_period_inventory_units DECIMAL(16,4)
    ,beginning_of_period_inventory_retail_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,beginning_of_period_inventory_retail_amount DECIMAL(38,9)
    ,beginning_of_period_inventory_cost_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,beginning_of_period_inventory_cost_amount DECIMAL(38,9)
    ,beginning_of_period_inventory_target_units DECIMAL(16,4)
    ,beginning_of_period_inventory_target_retail_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,beginning_of_period_inventory_target_retail_amount DECIMAL(38,9)
    ,beginning_of_period_inventory_target_cost_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,beginning_of_period_inventory_target_cost_amount DECIMAL(38,9)
    ,return_to_vendor_units DECIMAL(16,4)
    ,return_to_vendor_retail_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,return_to_vendor_retail_amount DECIMAL(38,9)
    ,return_to_vendor_cost_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,return_to_vendor_cost_amount DECIMAL(38,9)
    ,rack_transfer_units DECIMAL(16,4)
    ,rack_transfer_retail_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,rack_transfer_retail_amount DECIMAL(38,9)
    ,rack_transfer_cost_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,rack_transfer_cost_amount DECIMAL(38,9)
    ,active_inventory_in_units DECIMAL(16,4)
    ,active_inventory_in_retail_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,active_inventory_in_retail_amount DECIMAL(38,9)
    ,active_inventory_in_cost_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,active_inventory_in_cost_amount DECIMAL(38,9)
    ,plannable_inventory_units DECIMAL(16,4)
    ,plannable_inventory_retail_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,plannable_inventory_retail_amount DECIMAL(38,9)
    ,plannable_inventory_cost_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,plannable_inventory_cost_amount DECIMAL(38,9)
    ,plannable_inventory_receipt_less_reserve_units DECIMAL(16,4)
    ,plannable_inventory_receipt_less_reserve_retail_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,plannable_inventory_receipt_less_reserve_retail_amount DECIMAL(38,9)
    ,plannable_inventory_receipt_less_reserve_cost_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,plannable_inventory_receipt_less_reserve_cost_amount DECIMAL(38,9)
    ,shrink_units DECIMAL(16,4)
    ,shrink_retail_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,shrink_retail_amount DECIMAL(38,9)
    ,shrink_cost_currency_code VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,shrink_cost_amount DECIMAL(38,9)
    ,quantrix_update TIMESTAMP(6) WITH TIME ZONE 
    ,rcd_update_timestamp TIMESTAMP(6) WITH TIME ZONE
)
PRIMARY INDEX(snapshot_plan_month_idnt, fiscal_month_idnt, selling_country, selling_brand, dept_idnt, category, supplier_group)
PARTITION BY RANGE_N(snapshot_start_day_date BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY , NO RANGE)
;
