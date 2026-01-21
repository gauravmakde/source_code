CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'wbr_supplier{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.wbr_supplier{env_suffix}
    ,FALLBACK
	,NO BEFORE JOURNAL
	,NO AFTER JOURNAL
	,CHECKSUM = DEFAULT
	,DEFAULT MERGEBLOCKRATIO
	,MAP = TD_MAP1
(
	ty_ly_ind                              CHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('TY', 'LY', 'LLY')
	, ss_ind                               CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('SS', 'DS', 'PL')
	, week_end_date	                       DATE
	, week_idnt                            INTEGER
	, month_idnt                           INTEGER
	, quarter_idnt                         INTEGER
	, year_idnt                            INTEGER
	, week_label                           VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	, month_label                          VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	, quarter_label                        VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	, week_rank                            INTEGER
	, week_idnt_aligned                    INTEGER
	, month_idnt_aligned                   INTEGER
	, quarter_idnt_aligned                 INTEGER
	, year_idnt_aligned                    INTEGER
	, week_label_aligned                   VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	, month_label_aligned                  VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	, quarter_label_aligned                VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	, chnl_idnt                            INTEGER
	, chnl_label                           VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, banner                               CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('FP', 'OP')
	, div_idnt                             INTEGER
	, division                             VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, sdiv_idnt                            INTEGER
	, subdivision                          VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, dept_idnt                            INTEGER
	, department                           VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, class_idnt                           INTEGER
	, "class"                              VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, sbclass_idnt                         INTEGER
	, subclass                             VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, supplier_idnt                        VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
	, supplier                             VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, brand                                VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, quantrix_category                    VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, merch_themes                         VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, assortment_grouping                  VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, holiday_theme_ty                     VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, gift_ind                             VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, stocking_stuffer                     VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, supplier_group                       VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, intended_lifecycle_type              VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	, intended_season                      VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	, intended_exit_month_year             VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	, scaled_event                         VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	, holiday_or_celebration               VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	, price_type                           VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
	, npg_ind                              BYTEINT
	, diverse_brand_ind                    BYTEINT
	, dropship_ind                         BYTEINT
	, rp_ind                               BYTEINT	
	, anchor_brand_ind                     BYTEINT	
	, rack_strategic_brand_ind             BYTEINT	
	, general_merch_manager_executive_vice_president        VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, div_merch_manager_senior_vice_president               VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, div_merch_manager_vice_president                      VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, merch_director                                        VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, buyer                                                 VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, merch_planning_executive_vice_president               VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, merch_planning_senior_vice_president                  VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, merch_planning_vice_president                         VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, merch_planning_director_manager                       VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, assortment_planner                                    VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, sales_units		                   DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, sales_dollars		                   DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, cost_of_goods_sold		           DECIMAL(38,9)
	, product_margin		               DECIMAL(38,9)
	, sales_pm		                       DECIMAL(20,2)
	, return_units		                   DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, return_dollars		               DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, demand_units		                   DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, demand_dollars		               DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, demand_cancel_units		           DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, demand_cancel_dollars		           DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, demand_dropship_units		           DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, demand_dropship_dollars	           DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, shipped_units		                   DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, shipped_dollars		               DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, store_fulfill_units		           DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, store_fulfill_dollars		           DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, receipt_units		                   DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, receipt_dollars		               DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, receipt_cost		                   DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, receipt_dropship_units               DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, receipt_dropship_dollars             DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, receipt_dropship_cost	               DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, rtv_units		                       DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, rtv_dollars		                   DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, rtv_cost  		                   DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, eoh_units		                       DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, eoh_dollars		                   DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, eoh_cost		                       DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, nonsellable_units		               DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, product_views		                   DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, cart_adds		                       DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, web_order_units		               DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, on_order_units		               DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, on_order_retail_dollars              DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, on_order_cost_dollars                DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, on_order_4wk_units		           DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, on_order_4wk_retail_dollars          DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, on_order_4wk_cost_dollars            DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, backorder_units		               DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, backorder_dollars		               DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, apt_demand_u		                   DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, apt_demand_r		                   DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, apt_net_sls_u		                   DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, apt_net_sls_r		                   DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, apt_net_sls_c		                   DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, apt_return_u		                   DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, apt_return_r		                   DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, apt_eop_u		                       DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, apt_eop_c		                       DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, apt_receipt_u		                   DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, apt_receipt_c		                   DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, mfp_op_demand_u		               DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, mfp_op_demand_r		               DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, mfp_op_sales_u		               DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, mfp_op_sales_r		               DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, mfp_op_sales_c		               DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, mfp_op_returns_u		               DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, mfp_op_returns_r		               DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, mfp_sp_demand_u		               DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, mfp_sp_demand_r		               DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, mfp_sp_sales_u		               DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, mfp_sp_sales_r		               DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, mfp_sp_sales_c		               DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, mfp_sp_returns_u		               DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, mfp_sp_returns_r		               DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, mfp_qp_demand_u		               DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, mfp_qp_demand_r		               DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, mfp_qp_sales_u		               DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, mfp_qp_sales_r		               DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, mfp_qp_sales_c		               DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, mfp_qp_returns_u		               DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, mfp_qp_returns_r		               DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, mfp_op_eoh_u		                   DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, mfp_op_eoh_c		                   DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, mfp_op_receipts_u		               DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, mfp_op_receipts_c		               DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, mfp_sp_eoh_u		                   DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, mfp_sp_eoh_c		                   DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, mfp_sp_receipts_u		               DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, mfp_sp_receipts_c		               DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, mfp_qp_eoh_u		                   DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, mfp_qp_eoh_c		                   DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	, mfp_qp_receipts_u		               DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
	, mfp_qp_receipts_c		               DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00	
	, update_timestamp		               TIMESTAMP(6) WITH TIME ZONE	
)
PRIMARY INDEX (week_idnt, chnl_idnt, dept_idnt, quantrix_category, supplier)
PARTITION BY RANGE_N(week_idnt BETWEEN 201901 AND 203553 EACH 1);
GRANT SELECT ON {environment_schema}.wbr_supplier{env_suffix} TO PUBLIC;

COLLECT STATS
	PRIMARY INDEX (week_idnt, chnl_idnt, dept_idnt, quantrix_category, supplier)
	,COLUMN(PARTITION)
	ON {environment_schema}.wbr_supplier{env_suffix}
;