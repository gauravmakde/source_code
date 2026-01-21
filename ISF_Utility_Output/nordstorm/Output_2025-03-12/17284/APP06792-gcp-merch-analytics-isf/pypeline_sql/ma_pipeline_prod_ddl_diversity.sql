
CREATE MULTISET TABLE {environment_schema}.diversity_reporting,
    FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO,
    MAP = TD_MAP1
(
	 run_date                   DATE
     , yr_idnt                  INT
     , mth_idnt                 INT
     , chnl_label 				VARCHAR(100)CHARACTER SET UNICODE NOT CASESPECIFIC
     , country                  VARCHAR(2)CHARACTER SET UNICODE NOT CASESPECIFIC
     , banner                   VARCHAR(2)CHARACTER SET UNICODE NOT CASESPECIFIC
     , div_label                VARCHAR(100)CHARACTER SET UNICODE NOT CASESPECIFIC
     , div_idnt                 VARCHAR(50)CHARACTER SET UNICODE NOT CASESPECIFIC
     , grp_label                VARCHAR(500) CHARACTER SET UNICODE NOT CASESPECIFIC
     , grp_idnt                 VARCHAR(50)CHARACTER SET UNICODE NOT CASESPECIFIC
     , dept_label               VARCHAR(500) CHARACTER SET UNICODE NOT CASESPECIFIC
     , dept_idnt                VARCHAR(50)CHARACTER SET UNICODE NOT CASESPECIFIC
     , class_label             	VARCHAR(500) CHARACTER SET UNICODE NOT CASESPECIFIC
     , class_idnt               VARCHAR(50)CHARACTER SET UNICODE NOT CASESPECIFIC
     , sbclass_label           	VARCHAR(500) CHARACTER SET UNICODE NOT CASESPECIFIC
     , sbclass_idnt             VARCHAR(50)CHARACTER SET UNICODE NOT CASESPECIFIC
     , supp_name                VARCHAR(500) CHARACTER SET UNICODE NOT CASESPECIFIC
     , supp_idnt                VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
     , brand_name               VARCHAR(500) CHARACTER SET UNICODE NOT CASESPECIFIC
     , supp_npg_idnt			INT 
     , cc_idnt 					VARCHAR(500) CHARACTER SET UNICODE NOT CASESPECIFIC
     , black_idnt 				INT 
     , latinx_idnt				INT 
     , asian_idnt				INT 
	 , lgtbq_ind				INT
     , sales_u                  DECIMAL(20,0) 
     , sales_$                  DECIMAL(20,2) 
     --, sales_cost               DECIMAL(20,2) 
     , return_u                 DECIMAL(20,0) 
     , return_$                 DECIMAL(20,2) 
     , demand_u                 DECIMAL(20,0) 
     , demand_$                 DECIMAL(20,2) 
     , eoh_u                 	DECIMAL(20,0) 
     , eoh_$                 	DECIMAL(20,2) 
     --, eoh_cost                 DECIMAL(20,0) 
     , ntn                 	    DECIMAL(20,0) 
     , receipt_u                DECIMAL(20,0) 
     , receipt_$                DECIMAL(20,2) 
     , on_order_u               DECIMAL(20,0) 
     , on_order_$               DECIMAL(20,2) 
     , on_order_4wk_u           DECIMAL(20,0) 
     , on_order_4wk_$           DECIMAL(20,2) 
--     , margin_$           		DECIMAL(20,2)
     , yr_sale_target_$		    DECIMAL(20,2)
     --, mth_sale_target_$		DECIMAL(20,2)
)
PRIMARY INDEX (yr_idnt, mth_idnt, chnl_label, country, banner, cc_idnt)
;

GRANT SELECT ON {environment_schema}.diversity_reporting TO PUBLIC;

