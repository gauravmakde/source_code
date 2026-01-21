/*
Purpose:        Creates empty tables in {{environment_schema}} for Selection Planning
                    selection_planning_daily_base
                    selection_planning_daily
                    selection_planning_monthly
Variable(s):     {{environment_schema}} T2DL_DAS_SELECTION (prod) or T3DL_ACE_ASSORTMENT
                 {{env_suffix}} '' or '_dev' tablesuffix for prod testing
Author(s):       Sara Riker & Christine Buckler
*/


CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'selection_planning_daily_base{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.selection_planning_daily_base{env_suffix}, FALLBACK,
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
     day_date DATE FORMAT 'YYYY-MM-DD'
    ,sku_idnt VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,customer_choice VARCHAR(75) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,wk_idnt INTEGER 
    ,wk_454 SMALLINT COMPRESS(1,2,3,4,5)
    ,week_of_fyr SMALLINT
    ,mnth_454 SMALLINT COMPRESS(1,2,3,4,5,6,7,8,9,10,11,12)
    ,mnth_idnt INTEGER
    ,qtr_454 SMALLINT COMPRESS(1,2,3,4)
    ,yr_454 SMALLINT
    ,channel_country VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,channel_brand VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,cc_type VARCHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('NEW','CF')
    ,los_flag INTEGER
    ,rp_ind BYTEINT 
    ,ds_ind BYTEINT 
    ,cl_ind BYTEINT 
    ,pm_ind BYTEINT 
    ,reg_ind BYTEINT 
    ,flash_ind BYTEINT 
    ,demand_units INTEGER 
    ,demand_dollars DECIMAL(38,6)
    ,order_sessions INTEGER
    ,add_qty INTEGER
    ,add_sessions INTEGER
    ,product_views DECIMAL(38,6)
    ,product_view_sessions DECIMAL(38,6)
    ,instock_views DECIMAL(38,6)
    ,demand_units_reg INTEGER
    ,demand_dollars_reg DECIMAL(38,6)
    ,order_sessions_reg INTEGER
    ,add_qty_reg INTEGER
    ,add_sessions_reg INTEGER
    ,product_views_reg DECIMAL(38,6)
    ,product_view_sessions_reg DECIMAL(38,6)
    ,instock_views_reg DECIMAL(38,6)
    ,return_dollars DECIMAL(38,6)
    ,sales_dollars DECIMAL(38,6)
    ,return_units INTEGER
    ,sales_units INTEGER
    ,sales_net_dollars  DECIMAL(38,6)
    ,sales_net_units INTEGER
    ,update_timestamp TIMESTAMP(6) WITH TIME ZONE 
)
PRIMARY INDEX(day_date, channel_country, channel_brand, sku_idnt, customer_choice)
PARTITION BY RANGE_N(day_date BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY , NO RANGE);

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'selection_planning_daily{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.selection_planning_daily{env_suffix}, FALLBACK,
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
     day_date DATE FORMAT 'YYYY-MM-DD'
    ,wk_idnt INTEGER
    ,wk_454 INTEGER COMPRESS(1,2,3,4,5)
    ,week_of_fyr INTEGER
    ,mnth_454 INTEGER
    ,mnth_idnt INTEGER
    ,qtr_454 INTEGER
    ,yr_454 INTEGER
    ,channel_country VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('US','CA')
    ,channel_brand VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('NORDSTROM', 'NORDSTROM_RACK')
    ,div_idnt INTEGER 
    ,grp_idnt INTEGER 
    ,dept_idnt INTEGER 
    ,cat_idnt INTEGER 
    ,category VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC

    ,customer_choices INTEGER
    ,customer_choices_los INTEGER
    ,customer_choices_new INTEGER
    ,bop_est INTEGER
    ,eop_est INTEGER

    ,update_timestamp TIMESTAMP(6) WITH TIME ZONE 
)
PRIMARY INDEX(day_date, mnth_454, yr_454, dept_idnt, cat_idnt)
PARTITION BY RANGE_N(day_date BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY , NO RANGE);

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'selection_planning_monthly{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.selection_planning_monthly{env_suffix}, FALLBACK,
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
     mnth_454 INTEGER COMPRESS(1,2,3,4,5,6,7,8,9,10,11,12)
    ,mnth_idnt INTEGER
    ,mnth_start_day_date DATE FORMAT 'YYYY-MM-DD'
    ,qtr_454 INTEGER COMPRESS(1,2,3,4)
    ,yr_454 INTEGER
    ,channel_country VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('US','CA')
    ,channel_brand VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('NORDSTROM', 'NORDSTROM_RACK')
    ,div_idnt INTEGER 
    ,grp_idnt INTEGER 
    ,dept_idnt INTEGER 
    ,cat_idnt INTEGER 
    ,category VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC 
    
    ,ccs_daily_avg INTEGER 
    ,ccs_daily_avg_los INTEGER 
    ,ccs_daily_avg_new INTEGER
    ,ccs_mnth_bop INTEGER
    ,ccs_mnth_eop INTEGER
    ,ccs_mnth INTEGER
    ,ccs_mnth_los INTEGER
    ,ccs_mnth_new INTEGER 
    ,ccs_mnth_cf INTEGER
    ,ccs_mnth_cf_rp INTEGER
    ,ccs_mnth_rp INTEGER 
    ,ccs_mnth_ds INTEGER

    ,ccs_mnth_rcpt_new INTEGER
    ,ccs_mnth_rcpt_cf INTEGER
    ,ccs_mnth_rcpt_new_ds INTEGER
    ,ccs_mnth_rcpt_cf_ds INTEGER

    ,order_sessions INTEGER
    ,add_qty INTEGER
    ,add_sessions INTEGER
    ,product_views DECIMAL(12,2)
    ,product_view_sessions DECIMAL(12,2)
    ,instock_views DECIMAL(12,2)
    ,demand_units INTEGER 
    ,demand_dollars DECIMAL(12,2)
    ,demand_units_new INTEGER 
    ,demand_dollars_new DECIMAL(12,2)
    ,conversion DECIMAL(12,2)
    
    ,order_sessions_reg INTEGER
    ,add_qty_reg INTEGER
    ,add_sessions_reg INTEGER
    ,product_views_reg DECIMAL(12,2)
    ,product_view_sessions_reg DECIMAL(12,2)
    ,instock_views_reg DECIMAL(12,2)
    ,demand_units_reg INTEGER
    ,demand_dollars_reg DECIMAL(12,2)
    ,conversion_reg DECIMAL(12,2)

    ,return_dollars DECIMAL(12,2)
    ,return_units INTEGER
    ,sales_dollars DECIMAL(12,2)
    ,sales_units INTEGER
    ,sales_net_dollars  DECIMAL(12,2)
    ,sales_net_units INTEGER
    ,cc_monthly_productivity DECIMAL(12,2)

    ,rcpt_units INTEGER
    ,rcpt_dollars DECIMAL(12,2)
    ,rcpt_bought_units INTEGER
    ,rcpt_bought_dollars DECIMAL(12,2)
    ,rcpt_ds_units INTEGER
    ,rcpt_ds_dollars DECIMAL(12,2)
    ,rcpt_new_units INTEGER
    ,rcpt_new_dollars DECIMAL(12,2)
    ,rcpt_cf_units INTEGER
    ,rcpt_cf_dollars DECIMAL(12,2)

    ,update_timestamp TIMESTAMP(6) WITH TIME ZONE 
)
PRIMARY INDEX(mnth_start_day_date, dept_idnt, cat_idnt)
PARTITION BY RANGE_N(mnth_start_day_date BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY , NO RANGE); 
