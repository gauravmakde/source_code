   /*
Purpose:        Creates empty tables in {{environment_schema}} for Item Performance
                    item_performance_weekly
                    item_performance_monthly

Variable(s):     {{environment_schema}} T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING (prod)
                 {{env_suffix}} '' or '_dev' tablesuffix for prod testing
Author(s):       Sara Scott
Date Created/Modified: 07/21/2024 -- change schema and separate ddl file

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'item_performance_weekly{env_suffix}', OUT_RETURN_MSG);
	CREATE MULTISET TABLE {environment_schema}.item_performance_weekly{env_suffix}  ,FALLBACK ,
		NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
	(

     week_idnt 								INTEGER NOT NULL
    , week_label							VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , month_idnt 							INTEGER NOT NULL
    , month_label    						VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , quarter_idnt 							INTEGER NOT NULL
    , year_idnt 							INTEGER NOT NULL
    , country 								CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , banner		 						VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
    , channel 								VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , price_type 							CHAR(2)
    , VPN 									VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , style_group_num 						VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , style_num								VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , style_desc							VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , color_num								INTEGER
    , supp_color							VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , asset_id 								VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , image_url								VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , supplier_idnt							INTEGER
    , supplier_name							VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , brand_name 							VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , div_idnt 								INTEGER
  	, div_desc 								VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
  	, subdiv_idnt 							INTEGER
  	, subdiv_desc 							VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
  	, dept_idnt 							INTEGER
  	, dept_desc 							VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
  	, class_idnt 							INTEGER
  	, class_desc 							VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
  	, sbclass_idnt 							INTEGER
  	, sbclass_desc 							VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
  	, quantrix_category                     VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
  	, ccs_category                          VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
  	, ccs_subcategory                       VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
  	, nord_role_desc                        VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
  	, rack_role_desc                        VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
  	, parent_group							VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
  	, merch_themes							VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
  	, holiday_theme_ty						VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
  	, gift_ind								VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
  	, stocking_stuffer						VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
  	, assortment_grouping					VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
  	, udig 									VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	  , fanatics_flag 						BYTEINT
    , new_flag 								BYTEINT
  	, cf_flag 								BYTEINT
  	, days_live 							INTEGER
  	, days_published  						DATE FORMAT 'YYYY-MM-DD'
  	, npg_flag 								CHAR(2)
  	, rp_flag 								BYTEINT
    , dropship_flag 						BYTEINT
    , intended_season                       VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , intended_exit_month_year              VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , intended_lifecycle_type               VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , scaled_event                          VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , holiday_or_celebration                VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
  
    , net_sales_units 						DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , net_sales_dollars 					DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , return_units 							DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , return_dollars 						DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00

    , demand_units 						      	DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , demand_dollars 						      DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , demand_cancel_units 					  DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , demand_cancel_dollars 				  DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , demand_dropship_units 				  DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , demand_dropship_dollars 				DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , shipped_units 						      DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , shipped_dollars 						    DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , store_fulfill_units 					  DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , store_fulfill_dollars 				  DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , boh_units 							        DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , boh_dollars 							      DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , boh_cost 							          DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , eoh_units 							        DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , eoh_dollars 							      DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , eoh_cost 							          DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , nonsellable_units 					    DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , asoh_units                      DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , receipt_units 						      DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , receipt_dollars 						    DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , receipt_cost    						    DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
	  , reserve_stock_units 					  DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , reserve_stock_dollars 				  DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , reserve_stock_cost    				  DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , receipt_dropship_units 				  DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , receipt_dropship_dollars 				DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , receipt_dropship_cost 				  DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00

    , last_receipt_date 					DATE FORMAT 'YYYY-MM-DD'
    , oo_units								DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , oo_dollars							DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    , backorder_units						DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	  , backorder_dollars						DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , instock								DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
	  , traffic 								DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , add_qty 								INTEGER
    , product_views 						DECIMAL(38,6)
    , instock_views 						DECIMAL(38,6)
    , order_demand 							DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
  	, order_quantity 						INTEGER
  	, cost_of_goods_sold 					DECIMAL(38,9)
  	, product_margin 						DECIMAL(38,9)
    , sales_pm								DECIMAL(38,9)
    )
PRIMARY INDEX (week_idnt, channel, style_num)
PARTITION BY RANGE_N(week_idnt BETWEEN 201901 AND 202553 EACH 1);

GRANT SELECT ON {environment_schema}.item_performance_weekly{env_suffix} TO PUBLIC;


CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'item_performance_monthly{env_suffix}', OUT_RETURN_MSG);
	CREATE MULTISET TABLE {environment_schema}.item_performance_monthly{env_suffix}  ,FALLBACK ,
		NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
	(

     month_idnt 							INTEGER NOT NULL
    , month_label    						VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , quarter_idnt 							INTEGER NOT NULL
    , year_idnt 							INTEGER NOT NULL
    , country 								CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , banner		 						VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
    , channel 								VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , price_type 							CHAR(2)
    , VPN 									VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , style_group_num 						VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , style_num								VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , style_desc							VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , color_num								INTEGER
    , supp_color							VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , asset_id 								VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , image_url								VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , supplier_idnt							INTEGER
    , supplier_name							VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , brand_name 							VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , div_idnt 								INTEGER
  	, div_desc 								VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
  	, subdiv_idnt 							INTEGER
  	, subdiv_desc 							VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
  	, dept_idnt 							INTEGER
  	, dept_desc 							VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
  	, class_idnt 							INTEGER
  	, class_desc 							VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
  	, sbclass_idnt 							INTEGER
  	, sbclass_desc 							VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
  	, quantrix_category                     VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
  	, ccs_category                          VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
  	, ccs_subcategory                       VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
  	, nord_role_desc                        VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
  	, rack_role_desc                        VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
  	, parent_group							VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
  	, merch_themes							VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
  	, holiday_theme_ty						VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
  	, gift_ind								VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
  	, stocking_stuffer						VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
  	, assortment_grouping					VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , udig 									VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	  , fanatics_flag 						BYTEINT
    , new_flag 								BYTEINT
  	, cf_flag 								        BYTEINT
  	, days_live 							        INTEGER
  	, days_published  						    DATE FORMAT 'YYYY-MM-DD'
  	, npg_flag 								        CHAR(2)
  	, rp_flag 								        BYTEINT
    , dropship_flag 						      BYTEINT

    , net_sales_units 						    DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , net_sales_dollars 					    DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , return_units 							      DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , return_dollars 						      DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00

    , demand_units 							      DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , demand_dollars 						      DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , demand_cancel_units 					  DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , demand_cancel_dollars 				  DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , demand_dropship_units 				  DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , demand_dropship_dollars 				DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , shipped_units 						      DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , shipped_dollars 						    DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , store_fulfill_units 					  DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , store_fulfill_dollars 				  DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , boh_units 							        DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , boh_dollars 							      DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , boh_cost 							          DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , eoh_units 							        DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , eoh_dollars 							      DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , eoh_cost 							          DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , nonsellable_units 					    DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , asoh_units                      DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , receipt_units 						      DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , receipt_dollars 						    DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , receipt_cost    						    DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
	  , reserve_stock_units 					  DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , reserve_stock_dollars 				  DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , reserve_stock_cost    				  DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , receipt_dropship_units 				  DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , receipt_dropship_dollars 				DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , receipt_dropship_cost 				  DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00

    , last_receipt_date 					DATE FORMAT 'YYYY-MM-DD'
    , oo_units								DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , oo_dollars							DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    , backorder_units						DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	  , backorder_dollars						DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , instock								DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
	  , traffic 								DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , add_qty 								INTEGER
    , product_views 						DECIMAL(38,6)
    , instock_views 						DECIMAL(38,6)
    , order_demand 							DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
  	, order_quantity 						INTEGER
  	, cost_of_goods_sold 					DECIMAL(38,9)
  	, product_margin 						DECIMAL(38,9)
    , sales_pm								DECIMAL(38,9)



)
PRIMARY INDEX (month_idnt, channel, style_num)
PARTITION BY RANGE_N(month_idnt BETWEEN 201901 AND 202512 EACH 1);


GRANT SELECT ON {environment_schema}.item_performance_monthly{env_suffix} TO PUBLIC;
