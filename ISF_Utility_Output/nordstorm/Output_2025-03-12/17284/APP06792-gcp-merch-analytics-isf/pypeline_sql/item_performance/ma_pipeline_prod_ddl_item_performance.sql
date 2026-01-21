   /*
Purpose:        Creates empty tables in {{environment_schema}} for Item Performance
                    item_performance_daily_base
                    item_performance_weekly
                    item_performance_monthly

Variable(s):     {{environment_schema}} T2DL_DAS_SELECTION (prod) or T3DL_ACE_ASSORTMENT
                 {{env_suffix}} '' or '_dev' tablesuffix for prod testing
Author(s):       Sara Scott
Date Created/Modified: 03/05/2023 -- add asoh_units as a column

*/





CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'item_performance_daily_base{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.item_performance_daily_base{env_suffix} ,FALLBACK ,
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
	(
	day_date								DATE FORMAT 'YYYY-MM-DD'
    , week_idnt 							INTEGER NOT NULL
    , week_desc								VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , month_idnt 							INTEGER NOT NULL
    , quarter_idnt 							INTEGER NOT NULL
    , year_idnt 							INTEGER NOT NULL
    , month_end_week_idnt					INTEGER NOT NULL
    , week_end_day_date                     DATE FORMAT 'YYYY-MM-DD'
    , sku_idnt								VARCHAR(100)
    , country 								CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , banner		 						VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
    , channel 								VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , price_type 							CHAR(2)
    , fanatics_flag 						BYTEINT
	  , days_live 							INTEGER
    , days_published  						DATE FORMAT 'YYYY-MM-DD'
	  , rp_flag 								BYTEINT
    , dropship_flag 						BYTEINT
	  , udig									VARCHAR(100)
    , net_sales_units 						DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , net_sales_dollars 					DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , return_units 							DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , return_dollars 						DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , demand_units 							DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , demand_dollars 						DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , demand_cancel_units 					DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , demand_cancel_dollars 				DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , demand_dropship_units 				DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , demand_dropship_dollars 				DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , shipped_units 						DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , shipped_dollars 						DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , store_fulfill_units 					DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , store_fulfill_dollars 				DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , boh_units 							DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , boh_dollars 							DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , boh_cost 							DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , eoh_units 							DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , eoh_dollars 							DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , eoh_cost 							DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , nonsellable_units 					DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , asoh_units 							DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , last_receipt_date 					DATE FORMAT 'YYYY-MM-DD'
    , oo_units								DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , oo_dollars							DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    , add_qty 								INTEGER
  	, product_views 						DECIMAL(38,6)
  	, instock_views 						DECIMAL(38,6)
    , order_demand 							DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	  , order_quantity 						INTEGER
  	, cost_of_goods_sold 					DECIMAL(38,9)
  	, product_margin 						DECIMAL(38,9)
  	, sales_pm               				DECIMAL(38,9)
  	, backorder_units						DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
  	, backorder_dollars						DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
  	, traffic								DECIMAL(38,6)

	, instock_traffic						DECIMAL(38,6)



)
PRIMARY INDEX (day_date, country, channel, sku_idnt)
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



GRANT SELECT ON {environment_schema}.item_performance_daily_base{env_suffix} TO PUBLIC;