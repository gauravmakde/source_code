/************** START - INFORMATION *********************/
-- Date Modified: 2023.06.02
-- Purpose of build:
    -- Disaster Recovery, Reference, and DDL Rebuilds

/************** END - INFORMATION *********************/

/******************************************************** START - TABLE DDL CREATION ********************************************************/



/******************************************************************************
** FILE: anniversary_sku.sql
** T2 TABLE: {environment_schema}.ANNIVERSARY_SKU
**
** DESCRIPTION:
**      - Updated Daily to pull in most current list of TY and LY Anniversary SKUs
** UPDATED: 06-10-2024
** AUTHOR:
**      - Alli Moore (alli.moore@nordstrom.com)
******************************************************************************/
  -- UNCOMMENT AFTER DEV
/*
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'ANNIVERSARY_SKU', OUT_RETURN_MSG);

CREATE MULTISET TABLE {environment_schema}.ANNIVERSARY_SKU
     , FALLBACK
     , NO BEFORE JOURNAL
     , NO AFTER JOURNAL
     , CHECKSUM = DEFAULT
     , DEFAULT MERGEBLOCKRATIO
(
    sku_idnt            VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , country 			VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('US')
    , event_year		INTEGER NOT NULL
    , ty_ly_ind			VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('TY', 'LY')
    , process_tmstp    	TIMESTAMP(6) WITH TIME ZONE
)
PRIMARY INDEX (sku_idnt, event_year);
GRANT SELECT ON {environment_schema}.ANNIVERSARY_SKU TO PUBLIC;

COMMENT ON TABLE {environment_schema}.ANNIVERSARY_SKU AS 'A lookup table of the most current TY and LY Anniversary SKUs, updated daily';
COMMENT ON COLUMN {environment_schema}.ANNIVERSARY_SKU.sku_idnt AS 'RMS Sku Number';
COMMENT ON COLUMN {environment_schema}.ANNIVERSARY_SKU.event_year AS 'Anniversary Sale Year';
COMMENT ON COLUMN {environment_schema}.ANNIVERSARY_SKU.ty_ly_ind AS 'TY or LY Event Year Indicator Flag';
COMMENT ON COLUMN {environment_schema}.ANNIVERSARY_SKU.process_tmstp AS 'Table Last Updated Timestamp in PDT/PST';



CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'ANNIV_SKU_HEALTH_LOG', OUT_RETURN_MSG);

CREATE MULTISET TABLE {environment_schema}.ANNIV_SKU_HEALTH_LOG
     , FALLBACK
     , NO BEFORE JOURNAL
     , NO AFTER JOURNAL
     , CHECKSUM = DEFAULT
     , DEFAULT MERGEBLOCKRATIO
(
    process_tmstp       TIMESTAMP(6) WITH TIME ZONE NOT NULL
    , ty_anniv_sku_ct 	INTEGER NOT NULL
    , ly_anniv_sku_ct	INTEGER NOT NULL
)
PRIMARY INDEX (process_tmstp);
GRANT SELECT ON {environment_schema}.ANNIV_SKU_HEALTH_LOG TO PUBLIC;

COMMENT ON TABLE {environment_schema}.ANNIV_SKU_HEALTH_LOG AS 'A daily log of total anniv event sku cts for dq tracking purposes';
*/




/******************************************************************************
** T2 TABLE: {environment_schema}.ANNIVERSARY_SKU_CHNL_DATE
**
** DESCRIPTION:
**      - Updated Daily to pull in most current list of TY and LY Anniversary SKUs/Chnl/Dates
** UPDATED: 05-12-2023
** AUTHOR:
**      - Alli Moore (alli.moore@nordstrom.com)
******************************************************************************/
 /*
 --DROP TABLE {environment_schema}.ANNIVERSARY_SKU_CHNL_DATE;
  CREATE MULTISET TABLE {environment_schema}.ANNIVERSARY_SKU_CHNL_DATE
        , FALLBACK
        , NO BEFORE JOURNAL
        , NO AFTER JOURNAL
        , CHECKSUM = DEFAULT
        , DEFAULT MERGEBLOCKRATIO
 (
     sku_idnt  			VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
     , channel_country 	VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
	 , selling_channel 	VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
	 , store_type_code 	VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
     , day_idnt       	INTEGER NOT NULL
     , day_dt			DATE NOT NULL
     , ty_ly_ind		VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('TY', 'LY')
     , anniv_item_ind   SMALLINT NOT NULL COMPRESS (1)
     , reg_price_amt    DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
     , spcl_price_amt   DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
 )
 PRIMARY INDEX (sku_idnt)
 PARTITION BY RANGE_N(day_idnt  BETWEEN 2022001 AND 2023364 EACH 1);

 GRANT SELECT ON {environment_schema}.ANNIVERSARY_SKU_CHNL_DATE  TO PUBLIC;
*/





/******************************************************************************
** T2 TABLE: {environment_schema}.scaled_event_base
**
** DESCRIPTION:
**      - DDL BUILD FOR SCALED EVENT BASE TABLE
**
** UPDATED: 06-02-2023
** AUTHOR:
**      - Alli Moore (alli.moore@nordstrom.com)
******************************************************************************/
 /*
--RENAME TABLE {environment_schema}.scaled_event_sku_clk_rms_anniv21 as {environment_schema}.scaled_event_sku_clk_rms_anniv22;
--DROP TABLE {environment_schema}.scaled_event_base;
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'scaled_event_base', OUT_RETURN_MSG);

-- 2022.11.01 - DDL build for scaled_event_base

CREATE MULTISET TABLE {environment_schema}.scaled_event_base
    , FALLBACK
    , NO BEFORE JOURNAL
    , NO AFTER JOURNAL
    , CHECKSUM = DEFAULT
    , DEFAULT MERGEBLOCKRATIO
(
       sku_idnt                     VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
     , loc_idnt                     VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
     , day_idnt                     INTEGER NOT NULL
     , day_dt                       DATE NOT NULL
     , price_type                   VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
     , date_event_type              VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
     , chnl_idnt                    VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
     , anniv_ind                    VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
     , rp_ind                       VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
     , sales_units                  DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
     , sales_dollars                DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
     , return_units                 DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
     , return_dollars               DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
     , ntn                          INTEGER NOT NULL
     , demand_units                 DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
     , demand_dollars               DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
     , shipped_units                DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
     , shipped_dollars              DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
     , demand_cancel_units          DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
     , demand_cancel_dollars        DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
     , demand_dropship_units        DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
     , demand_dropship_dollars      DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
     , store_fulfill_units          DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
     , store_fulfill_dollars        DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
     , dropship_units               DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
     , dropship_dollars             DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
     , eoh_units                    DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
     , eoh_dollars                  DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
     , boh_units                    DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
     , boh_dollars                  DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
     , nonsellable_units            DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
     , cogs                         DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
     , receipt_units                DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
     , receipt_dollars              DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
     , receipt_dropship_units       DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
     , receipt_dropship_dollars     DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
     , sales_aur                    DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
     , demand_aur                   DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
     , eoh_aur                      DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
     , receipt_aur                  DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
     , anniv_retail_original        DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
     , anniv_retail_special         DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
     , process_tmstmp               TIMESTAMP(6) WITH TIME ZONE
     , sales_cost 					DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
   	 , return_cost 					DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
	 , shipped_cost 				DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
	 , store_fulfill_cost 			DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
	 , dropship_cost 				DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
	 , eoh_cost 					DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
	 , boh_cost 					DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
	 , receipt_cost 				DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
	 , receipt_dropship_cost 		DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
	 , sales_pm 					DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
)
PRIMARY INDEX (sku_idnt, loc_idnt, day_idnt, price_type)
PARTITION BY RANGE_N(day_dt BETWEEN DATE '2021-09-01' AND DATE '2024-02-01' EACH INTERVAL '1' DAY, NO RANGE, UNKNOWN);

GRANT SELECT ON {environment_schema}.scaled_event_base TO PUBLIC;



-- Update Table with new fields (06.2023)
ALTER TABLE {environment_schema}.scaled_event_base
ADD sales_cost 					DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
 , ADD return_cost 					DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
 , ADD shipped_cost 				DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
 , ADD store_fulfill_cost 			DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
 , ADD dropship_cost 				DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
 , ADD eoh_cost 					DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
 , ADD boh_cost 					DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
 , ADD receipt_cost 				DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
 , ADD receipt_dropship_cost 		DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
 , ADD sales_pm 					DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
 ;
*/

-- Extend Table Partition Range (07.2023)
/*
ALTER TABLE T2DL_DAS_SCALED_EVENTS.scaled_event_base MODIFY PRIMARY INDEX ( sku_idnt ,loc_idnt ,day_idnt ,price_type ) ADD RANGE BETWEEN 2023101 AND 2025001 EACH 1;
*/






/******************************************************************************
** T2 TABLE: {environment_schema}.scaled_event_sku_clk_rms
**
** DESCRIPTION:
**      - DDL BUILD FOR SCALED EVENT SKU CLK RMS TABLE
**
** UPDATED: 07-06-2024 by Manuela Hurtado
** AUTHOR:
**      - Alli Moore (alli.moore@nordstrom.com)
******************************************************************************/

-- 2022.11.01 - DDL Build for
--DROP TABLE {environment_schema}.scaled_event_sku_clk_rms_test
/*
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'scaled_event_sku_clk_rms', OUT_RETURN_MSG);

CREATE MULTISET TABLE {environment_schema}.scaled_event_sku_clk_rms
    , FALLBACK
    , NO BEFORE JOURNAL
    , NO AFTER JOURNAL
    , CHECKSUM = DEFAULT
    , DEFAULT MERGEBLOCKRATIO
    , MAP = TD_MAP1
(
    sku_idnt                        VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , day_idnt                      INTEGER NOT NULL
    , day_dt                        DATE FORMAT 'YY/MM/DD' NOT NULL
    , day_dt_aligned                DATE FORMAT 'YY/MM/DD' NOT NULL
    , date_event_type               VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , event_phase_orig              VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC
    , event_day_orig                INTEGER
    , event_phase                   VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC
    , event_day                     INTEGER
    , month_idnt                    INTEGER NOT NULL
    , month_label                   VARCHAR(20)CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , anniv_ind                     VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('Y', 'N')
    , price_type                    VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('NA', 'Reg', 'Clear', 'Pro')
    , rp_ind                        VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('Y', 'N')
    , ty_ly_ind                     VARCHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('TY', 'LY')
    , channel                       VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , channel_idnt                  VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , location                      VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , banner                        VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('NORDSTROM','RACK')
    , climate_cluster               VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
    , NR_price_cluster              VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
    , country                       VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('US', 'CA')
    , dma_short                     VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
    , dma_proxy_zip                 VARCHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC
    , style_num                     VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
    , style_desc                    VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
    , vpn                           VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC
    , supp_color                    VARCHAR(80) CHARACTER SET UNICODE NOT CASESPECIFIC
    , colr_idnt                     VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
    , style_group_idnt              VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
    , dropship_ind                  VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('Y', 'N')
    , division                      VARCHAR(36) CHARACTER SET UNICODE NOT CASESPECIFIC
    , subdivision                   VARCHAR(36) CHARACTER SET UNICODE NOT CASESPECIFIC
    , department                    VARCHAR(36) CHARACTER SET UNICODE NOT CASESPECIFIC
    , "class"                       VARCHAR(36) CHARACTER SET UNICODE NOT CASESPECIFIC
    , subclass                      VARCHAR(36) CHARACTER SET UNICODE NOT CASESPECIFIC
    , supplier                      VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
    , brand                         VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , npg_ind                       VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('Y', 'N')
    , assortment_grouping           VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , quantrix_category             VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , ccs_category                  VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , ccs_subcategory               VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , nord_role_desc                VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , rack_role_desc                VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , merch_theme                   VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
    , anniversary_theme             VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
    , holiday_theme                 VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
    , gift_ind                      VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS('GIFT', 'NON-GIFT')
    , stocking_stuffer              VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('Y', 'N')
    , price_match_ind               VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('Y', 'N')
    , promo_grouping                VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , bipoc_ind                     VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('Y', 'N')
    , udig                          VARCHAR(80) CHARACTER SET UNICODE NOT CASESPECIFIC
    , price_band_one                VARCHAR(18) CHARACTER SET UNICODE NOT CASESPECIFIC
    , price_band_two                VARCHAR(18) CHARACTER SET UNICODE NOT CASESPECIFIC
    , sales_units                   DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , sales_dollars                 DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , return_units                  DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , return_dollars                DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , ntn                           INTEGER NOT NULL
    , demand_units                  DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , demand_dollars                DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , demand_cancel_units           DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , demand_cancel_dollars         DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , shipped_units                 DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , shipped_dollars               DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , eoh_units                     DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , eoh_dollars                   DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , boh_units                     DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , boh_dollars                   DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , nonsellable_units             DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , cogs                          DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , receipt_units                 DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , receipt_dollars               DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , sales_aur                     DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , demand_aur                    DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , eoh_aur                       DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , store_fulfill_units           DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , store_fulfill_dollars         DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , dropship_units                DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , dropship_dollars              DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.
    , demand_dropship_units         DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , demand_dropship_dollars       DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , receipt_dropship_units        DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , receipt_dropship_dollars      DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , on_order_units                DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , on_order_retail_dollars       DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
   -- , on_order_4wk_units            DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
   -- , on_order_4wk_retail_dollars   DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , product_views                 DECIMAL(14,5) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , cart_adds                     DECIMAL(14,5) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , order_units                   DECIMAL(14,5) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , instock_views                 DECIMAL(14,5) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , demand                        DECIMAL(20,5) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , wishlist_adds                 DECIMAL(20,5) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , retail_original               DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , retail_special                DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , units                         DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , last_receipt_date             DATE
    , process_tmstp                 TIMESTAMP(6) WITH TIME ZONE
    --- INSERT NEW 2023  FIELDS ---
	, sales_cost 					DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
	, return_cost 					DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
 	, shipped_cost 					DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
 	, store_fulfill_cost 			DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
 	, dropship_cost 				DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
 	, eoh_cost 						DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
 	, boh_cost 						DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
 	, receipt_cost 					DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
 	, receipt_dropship_cost 		DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
 	, sales_pm 						DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
 	, on_order_cost_dollars			DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
  , scored_views      DECIMAL(14,5) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
  , general_merch_manager_executive_vice_president VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL DEFAULT 'NA' -- NEW AN 2024
  , div_merch_manager_senior_vice_president VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL DEFAULT 'NA' -- NEW AN 2024
  , div_merch_manager_vice_president VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL DEFAULT 'NA' -- NEW AN 2024
  , merch_director VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL DEFAULT 'NA' -- NEW AN 2024
  , buyer VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL DEFAULT 'NA' -- NEW AN 2024
  , merch_planning_executive_vice_president VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL DEFAULT 'NA' -- NEW AN 2024
  , merch_planning_senior_vice_president VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL DEFAULT 'NA' -- NEW AN 2024
  , merch_planning_vice_president VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL DEFAULT 'NA' -- NEW AN 2024
  , merch_planning_director_manager VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL DEFAULT 'NA' -- NEW AN 2024
  , assortment_planner VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL DEFAULT 'NA' -- NEW AN 2024
  , anchor_brands VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL DEFAULT 'N' -- NEW AN 2024

    ----------------------------------
)
PRIMARY INDEX (sku_idnt, day_idnt, location)
PARTITION BY RANGE_N(day_dt BETWEEN DATE '2021-09-01' AND DATE '2024-02-01' EACH INTERVAL '1' DAY, NO RANGE, UNKNOWN);

GRANT SELECT ON {environment_schema}.scaled_event_sku_clk_rms TO PUBLIC;


-- Update Table with new fields (06.2023)
ALTER TABLE {environment_schema}.scaled_event_sku_clk_rms
DROP on_order_4wk_units
 , DROP on_order_4wk_retail_dollars
 , ADD sales_cost 					DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
 , ADD return_cost 					DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
 , ADD shipped_cost 				DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
 , ADD store_fulfill_cost 			DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
 , ADD dropship_cost 				DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
 , ADD eoh_cost 					DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
 , ADD boh_cost 					DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
 , ADD receipt_cost 				DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
 , ADD receipt_dropship_cost 		DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
 , ADD sales_pm 					DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
 , ADD on_order_cost_dollars		DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW AN 2023
 ;


-- Update Table with new fields (07.2023)
ALTER TABLE {environment_schema}.scaled_event_sku_clk_rms
 ADD scored_views 					DECIMAL(14,5) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ) -- NEW AN 2023
 ;

 -- Extend Table Partition Range (07.2023)

 ALTER TABLE T2DL_DAS_SCALED_EVENTS.scaled_event_sku_clk_rms MODIFY PRIMARY INDEX ( sku_idnt ,day_idnt ,location ) ADD RANGE BETWEEN DATE '2023-02-02' AND DATE '2024-01-01' EACH INTERVAL '1'DAY;



-- Update Table with new fields (06.2024)
ALTER TABLE {environment_schema}.scaled_event_sku_clk_rms
ADD general_merch_manager_executive_vice_president VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL DEFAULT 'NA', -- NEW AN 2024
ADD div_merch_manager_senior_vice_president VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL DEFAULT 'NA', -- NEW AN 2024
ADD div_merch_manager_vice_president VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL DEFAULT 'NA', -- NEW AN 2024
ADD merch_director VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL DEFAULT 'NA', -- NEW AN 2024
ADD buyer VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL DEFAULT 'NA', -- NEW AN 2024
ADD merch_planning_executive_vice_president VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL DEFAULT 'NA', -- NEW AN 2024
ADD merch_planning_senior_vice_president VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL DEFAULT 'NA', -- NEW AN 2024
ADD merch_planning_vice_president VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL DEFAULT 'NA', -- NEW AN 2024
ADD merch_planning_director_manager VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL DEFAULT 'NA', -- NEW AN 2024
ADD assortment_planner VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL DEFAULT 'NA', -- NEW AN 2024
ADD anchor_brands VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL DEFAULT 'N'; -- NEW AN 2024


ALTER TABLE T2DL_DAS_SCALED_EVENTS.scaled_event_sku_clk_rms MODIFY PRIMARY INDEX ( sku_idnt ,day_idnt ,location ) ADD RANGE BETWEEN DATE '2024-02-02' AND DATE '2025-01-01' EACH INTERVAL '1'DAY;



*/









-- 2022.07.08 - DDL creation of an ad hoc method to identify sku's that may have not gone down the happy path of event setup
/*
CREATE MULTISET TABLE {environment_schema}.adhoc_sku_list ,FALLBACK ,
	 NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      sku_idnt VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
      ,country varchar(5) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
      ,process_timestamp TIMESTAMP(6) WITH TIME ZONE
     )
PRIMARY INDEX ( SKU_IDNT, COUNTRY );

GRANT SELECT ON {environment_schema}.adhoc_sku_list TO PUBLIC;
*/

/******************************************************** END - TABLE DDL CREATION ******************************************/
