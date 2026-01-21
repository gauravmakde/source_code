/*
Merch Staging DDL
Author: Sara Riker
8/22/22: Create DDL

Creates Tables:
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'wbr_merch_staging{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.wbr_merch_staging{env_suffix},FALLBACK ,
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
      week_idnt                            INTEGER NOT NULL
    , week_end_day_date                    DATE
    , chnl_label                           VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , chnl_idnt                            CHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , country                              CHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('US', 'CA')
    , banner                               CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('FP', 'OP')
    , sku_idnt                             VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
    , price_type                           VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
    , dropship_ind                         BYTEINT NOT NULL
    , rp_ind                               BYTEINT NOT NULL
    , sales_units                          DECIMAL(12,0)
		, sales_dollars                        DECIMAL(20,2)
		, return_units                         DECIMAL(12,0)
		, return_dollars                       DECIMAL(20,2)
		, demand_units                         DECIMAL(12,0)
		, demand_dollars                       DECIMAL(20,2)
		, demand_cancel_units                  DECIMAL(12,0)
		, demand_cancel_dollars                DECIMAL(20,2)
		, demand_dropship_units                DECIMAL(12,0)
		, demand_dropship_dollars              DECIMAL(20,2)
		, shipped_units                        DECIMAL(12,0)
		, shipped_dollars                      DECIMAL(20,2)
		, store_fulfill_units                  DECIMAL(12,0)
		, store_fulfill_dollars                DECIMAL(20,2)
		, eoh_units                            DECIMAL(12,0)
		, eoh_dollars                          DECIMAL(20,2)
		, eoh_cost                          DECIMAL(20,2)
		, nonsellable_units                    DECIMAL(12,0)
		-- added cost metrics 7/21
		, cost_of_goods_sold				   DECIMAL(38,9)
		, product_margin					   DECIMAL(38,9)
		, sales_pm                             DECIMAL(20,2)
    , update_timestamp                     TIMESTAMP(6) WITH TIME ZONE
)
PRIMARY INDEX(week_idnt, chnl_idnt, sku_idnt)
PARTITION BY RANGE_N(week_end_day_date BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY , NO RANGE)
;

GRANT SELECT ON {environment_schema}.wbr_merch_staging{env_suffix} TO PUBLIC;
