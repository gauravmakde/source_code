   /*
Purpose:        Creates empty table in {environment_schema} for Selection Productivity
                   selection_productivity_base

Variable(s):     {environment_schema} T2DL_DAS_SELECTION (prod) or T3DL_ACE_ASSORTMENT
                 {env_suffix} '' or '_dev' tablesuffix for prod testing
Author(s):       Sara Scott
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_selection', 'selection_productivity_base', OUT_RETURN_MSG);
CREATE MULTISET TABLE t2dl_das_selection.selection_productivity_base ,FALLBACK ,
		NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
	(
	 day_date DATE FORMAT 'YYYY-MM-DD'
    ,wk_idnt INTEGER NOT NULL
    ,month_idnt INTEGER NOT NULL
    ,month_abrv VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,quarter_idnt INTEGER NOT NULL
    ,half_idnt INTEGER NOT NULL
    ,year_idnt INTEGER NOT NULL
    ,first_day_of_week BYTEINT NOT NULL
    ,last_day_of_week BYTEINT NOT NULL
    ,first_day_of_month BYTEINT NOT NULL
    ,last_day_of_month BYTEINT NOT NULL
    ,first_day_of_qtr BYTEINT NOT NULL
    ,last_day_of_qtr BYTEINT NOT NULL
    ,current_week BYTEINT NOT NULL
    ,current_month BYTEINT NOT NULL
    ,current_quarter BYTEINT NOT NULL
    ,week_rank INTEGER NOT NULL
    ,month_rank INTEGER NOT NULL
    ,quarter_rank INTEGER NOT NULL
    ,sku_idnt VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,customer_choice VARCHAR(75) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,channel_country CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    ,channel_brand VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,channel VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    ,days_published  DATE FORMAT 'YYYY-MM-DD'
    ,days_live INTEGER NOT NULL
    ,fanatics_flag BYTEINT NOT NULL
    ,new_flag BYTEINT NOT NULL
    ,cf_flag BYTEINT NOT NULL
    ,rp_flag BYTEINT NOT NULL
    ,dropship_flag BYTEINT NOT NULL
    ,anniversary_flag BYTEINT NOT NULL
    ,net_sales_units DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    ,net_sales_dollars DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    ,return_units DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    ,return_dollars DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    ,sales_units DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    ,sales_dollars DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    ,demand_units DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    ,demand_dollars DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    ,demand_cancel_units DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    ,demand_cancel_dollars DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    ,demand_dropship_units DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    ,demand_dropship_dollars DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    ,shipped_units DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    ,shipped_dollars DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    ,store_fulfill_units DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    ,store_fulfill_dollars DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    ,net_sales_reg_units DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    ,net_sales_reg_dollars DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    ,net_sales_clr_units DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    ,net_sales_clr_dollars DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    ,net_sales_pro_units DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    ,net_sales_pro_dollars DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    ,boh_units DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    ,boh_dollars DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    ,eoh_units DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    ,eoh_dollars DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    ,nonsellable_units DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    ,eoh_reg_units DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    ,eoh_reg_dollars DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    ,eoh_clr_units DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    ,eoh_clr_dollars DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    ,eoh_pro_units DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    ,eoh_pro_dollars DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    ,last_receipt_date DATE FORMAT 'YYYY-MM-DD'
    ,order_sessions INTEGER
    ,add_qty INTEGER
    ,add_sessions INTEGER
    ,product_views DECIMAL(38,6)
    ,product_view_sessions DECIMAL(38,6)
    ,instock_views DECIMAL(38,6)
    ,order_sessions_reg INTEGER
    ,add_qty_reg INTEGER
    ,add_sessions_reg INTEGER
    ,product_views_reg DECIMAL(38,6)
    ,product_view_sessions_reg DECIMAL(38,6)
    ,instock_views_reg DECIMAL(38,6)
    ,order_demand DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	,order_quantity INTEGER



)
PRIMARY INDEX (day_date, channel_country, channel_brand, sku_idnt, customer_choice)
PARTITION BY ( RANGE_N(day_date BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY , NO RANGE) ,RANGE_N(month_idnt  BETWEEN 201901  AND 201912  EACH 1 ,
202001  AND 202012  EACH 1 ,
202101  AND 202112  EACH 1 ,
202201  AND 202212  EACH 1 ,
202301  AND 202312  EACH 1 ,
202401  AND 202412  EACH 1 ,
202501  AND 202512  EACH 1 ,
202601  AND 202612  EACH 1 ,
202701  AND 202712  EACH 1 ,
202801  AND 202812  EACH 1 ,
202901  AND 202912  EACH 1 ,
203001  AND 203012  EACH 1 ) );

GRANT SELECT ON t2dl_das_selection.selection_productivity_base TO PUBLIC;



