/*
Selection Planning DDL
Author: Sara Riker
11/27/21: Create DDL

Creates Tables:
    t2dl_das_selection.trend_score
    t2dl_das_selection.trend_score_dma
    t2dl_das_selection.trend_weekly_data
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_selection', 'trend_score', OUT_RETURN_MSG);
CREATE MULTISET TABLE t2dl_das_selection.trend_score ,FALLBACK , 
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
     wk_idnt INTEGER 
    ,wk_start_day_dt DATE FORMAT 'YYYY-MM-DD'
    ,channel_country VARCHAR(2) COMPRESS ('US','CA')
    ,channel_brand VARCHAR(20) COMPRESS ('NORDSTROM', 'NORDSTROM_RACK')
    ,grp_idnt INTEGER
    ,class_desc VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    ,trend_score FLOAT
    ,update_timestamp TIMESTAMP(6) WITH TIME ZONE 
)
PRIMARY INDEX(wk_idnt, grp_idnt, class_desc)
PARTITION BY RANGE_N(wk_start_day_dt BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY , NO RANGE)
;

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_selection', 'trend_score_dma', OUT_RETURN_MSG);
CREATE MULTISET TABLE t2dl_das_selection.trend_score_dma ,FALLBACK , 
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
     wk_idnt INTEGER 
    ,wk_start_day_dt DATE FORMAT 'YYYY-MM-DD'
    ,channel_country VARCHAR(2) COMPRESS ('US','CA')
    ,channel_brand VARCHAR(20) COMPRESS ('NORDSTROM', 'NORDSTROM_RACK')
    ,dma VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    ,grp_idnt INTEGER
    ,class_desc VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    ,trend_score FLOAT
    ,update_timestamp TIMESTAMP(6) WITH TIME ZONE 
)
PRIMARY INDEX(wk_idnt, grp_idnt, class_desc)
PARTITION BY RANGE_N(wk_start_day_dt BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY , NO RANGE)
;

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_selection', 'trend_score_weekly_data', OUT_RETURN_MSG);
CREATE MULTISET TABLE t2dl_das_selection.trend_score_weekly_data ,FALLBACK ,
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
     wk_idnt INTEGER
    ,wk_start_day_dt DATE FORMAT 'YYYY-MM-DD'
    ,yr_454 INTEGER
    ,channel_country VARCHAR(2) COMPRESS ('US','CA')
    ,channel_brand VARCHAR(20) COMPRESS ('NORDSTROM', 'NORDSTROM_RACK')
    ,div_idnt INTEGER
    ,grp_idnt INTEGER 
    ,grp_desc VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    ,class_idnt INTEGER 
    ,class_desc VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    ,choices INTEGER 
    ,choices_reg INTEGER 
    ,product_views INTEGER
    ,add_qty INTEGER
    ,demand_items INTEGER
    ,demand_dollars DECIMAL(12,2)
    ,product_views_reg INTEGER
    ,add_qty_reg INTEGER
    ,demand_items_reg INTEGER 
    ,demand_dollars_reg DECIMAL(12,2)
    ,update_timestamp TIMESTAMP(6) WITH TIME ZONE
)
PRIMARY INDEX(wk_start_day_dt, grp_idnt, class_desc)
PARTITION BY RANGE_N(wk_start_day_dt BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY , NO RANGE)
;

GRANT SELECT ON t2dl_das_selection.trend_score_weekly_data TO PUBLIC;
GRANT SELECT ON t2dl_das_selection.trend_score TO PUBLIC;
GRANT SELECT ON t2dl_das_selection.trend_score_dma TO PUBLIC;