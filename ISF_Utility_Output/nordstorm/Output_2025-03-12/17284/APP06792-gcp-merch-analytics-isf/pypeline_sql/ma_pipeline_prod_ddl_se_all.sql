/************** START - INFORMATION *********************/
-- Date Created: 2022.06.07
-- Purpose of build:
    -- Disaster Recovery

/************** END - INFORMATION *********************/

/************** START - TABLE DDL CREATION **************/
/* -- UNCOMMENT AFTER DEV
-- 2022.06.07 - DDL build for ANNIVERSARY_SKU_STORE_DATE
-- DROP TABLE {environment_schema}.ANNIVERSARY_SKU_STORE_DATE;
CREATE MULTISET TABLE {environment_schema}.anniversary_sku_store_date ,FALLBACK , 
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      sku_idnt VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      loc_idnt VARCHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      day_idnt INTEGER NOT NULL,
      anniv_item_ind SMALLINT NOT NULL COMPRESS 1 ,
      reg_price_amt DECIMAL(20,2) NOT NULL DEFAULT 0.00  COMPRESS 0.00 ,
      spcl_price_amt DECIMAL(20,2) NOT NULL DEFAULT 0.00  COMPRESS 0.00 )
PRIMARY INDEX ( sku_idnt, loc_idnt )
PARTITION BY RANGE_N(day_idnt  BETWEEN {start_date}  AND {end_date}  EACH 1 );

GRANT SELECT ON {environment_schema}.anniversary_sku_store_date to PUBLIC;

-- 2022.06.08 - DDL build for staging table anniv_sales_plan_22_staging
-- DROP TABLE {environment_schema}.ANNIV_SALES_PLAN_22_STAGING;
CREATE MULTISET TABLE {environment_schema}.anniv_sales_plan_22_staging ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      Country CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      Channel_Type VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      Department VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      Sales_Plan_Dollars INTEGER NOT NULL,
      process_tmstmp TIMESTAMP(6) WITH TIME ZONE)
PRIMARY INDEX ( Country ,Channel_Type ,Department );

GRANT SELECT ON {environment_schema}.ANNIV_SALES_PLAN_22_STAGING TO PUBLIC;
*/

-- 2022.11.01 - DDL build for scaled_event_dates
--DROP TABLE {environment_schema}.SCALED_EVENT_DATES;
/*
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'SCALED_EVENT_DATES', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.scaled_event_dates ,
     FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
     day_dt               DATE FORMAT 'YYYY-MM-DD' NOT NULL
     , day_idnt           INTEGER NOT NULL
     , yr_idnt            INTEGER NOT NULL COMPRESS(2022, 2021, 2020)
     , month_idnt         INTEGER NOT NULL 
     , month_label        VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
     , ty_ly_lly          VARCHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC
     , wk_of_fyr          INTEGER NOT NULL
     , wk_idnt            INTEGER NOT NULL
     , wk_end_dt          DATE FORMAT 'YYYY-MM-DD' NOT NULL
     , event_type         VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
     , event_country      CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS('US', 'CA')
     , day_dt_aligned     DATE FORMAT 'YYYY-MM-DD'
     , event_day          INTEGER NOT NULL
     , event_day_mod      INTEGER
     , event_type_mod     VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
     , anniv_ind          INTEGER NOT NULL
     , cyber_ind          INTEGER NOT NULL
     , process_tmstp      TIMESTAMP(6) WITH TIME ZONE
     )
     PRIMARY INDEX (day_dt)
PARTITION BY RANGE_N(day_idnt  BETWEEN 2020001  AND 2023001  EACH 1 );

GRANT SELECT ON {environment_schema}.SCALED_EVENT_DATES TO PUBLIC;

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
)
PRIMARY INDEX (sku_idnt, loc_idnt, day_idnt, price_type)
PARTITION BY RANGE_N(day_idnt BETWEEN {start_date} AND {end_date} EACH 1);

GRANT SELECT ON {environment_schema}.scaled_event_base TO PUBLIC;
*/
/******************************************************************************
** FILE: anniversary_sku_2022.sql
** T2 TABLE: {environment_schema}.ANNIVERSARY_SKU_2022
**
** DESCRIPTION:
**      - An intermittently updated lookup table for 2022 Anniversary SKUs and Country -- 183,385 SKUs as of 6/13
**
** AUTHOR:
**      - Alli Moore (alli.moore@nordstrom.com)
******************************************************************************/
 /* -- UNCOMMENT AFTER DEV
--DROP TABLE {environment_schema}.ANNIVERSARY_SKU_2022;

CREATE MULTISET TABLE {environment_schema}.ANNIVERSARY_SKU_2022
     , FALLBACK
     , NO BEFORE JOURNAL
     , NO AFTER JOURNAL
     , CHECKSUM = DEFAULT
     , DEFAULT MERGEBLOCKRATIO
(
    sku_idnt             VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , country 			 VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , process_tmstmp     TIMESTAMP(6) WITH TIME ZONE 
)
PRIMARY INDEX (sku_idnt, country);

GRANT SELECT ON {environment_schema}.ANNIVERSARY_SKU_2022 TO PUBLIC;
*/

-- 2022.11.01 - DDL Build for 
--DROP TABLE {environment_schema}.scaled_event_sku_clk_rms
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
    , date_event_type               VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL -- NEW
    , event_phase_orig              VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC 
    , event_day_orig                INTEGER 
    , event_phase                   VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC 
    , event_day                     INTEGER 
    , month_idnt                    INTEGER NOT NULL --NEW
    , month_label                   VARCHAR(20)CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL -- NEW
    , anniv_ind                     VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('Y', 'N')
    , price_type                    VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('NA', 'Reg', 'Clear', 'Pro')
    , rp_ind                        VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('Y', 'N')
    , ty_ly_ind                     VARCHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('TY', 'LY')
    , channel                       VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL 
    , channel_idnt                  VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL 
    , location                      VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , banner                        VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('NORDSTROM','RACK') -- NEW
    , climate_cluster               VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC -- NEW
    , NR_price_cluster              VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC -- NEW 
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
    , assortment_grouping           VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC-- NEW
    , quantrix_category             VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , ccs_category                  VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC  
    , ccs_subcategory               VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC  
    , nord_role_desc                VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , rack_role_desc                VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC  -- NEW
    , merch_theme                   VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
    , anniversary_theme             VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC 
    , holiday_theme                 VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC -- NEW
    , gift_ind                      VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS('GIFT', 'NON-GIFT') -- NEW
    , stocking_stuffer              VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('Y', 'N') -- NEW 
    , price_match_ind               VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('Y', 'N') -- NEW
    , promo_grouping                VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC -- NEW 
    , bipoc_ind                     VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('Y', 'N') -- NEW
    , udig                          VARCHAR(80) CHARACTER SET UNICODE NOT CASESPECIFIC
    , price_band_one                VARCHAR(18) CHARACTER SET UNICODE NOT CASESPECIFIC
    , price_band_two                VARCHAR(18) CHARACTER SET UNICODE NOT CASESPECIFIC
    , sales_units                   DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , sales_dollars                 DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , return_units                  DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , return_dollars                DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , ntn                           INTEGER NOT NULL -- NEW 
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
    , cogs                          DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00 -- NEW 
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
    , on_order_4wk_units            DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , on_order_4wk_retail_dollars   DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
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
)
PRIMARY INDEX (sku_idnt, day_idnt, location)
PARTITION BY RANGE_N(day_dt BETWEEN DATE '2021-09-01' AND DATE '2023-02-01' EACH INTERVAL '1' DAY, NO RANGE, UNKNOWN);
--COMMIT;

GRANT SELECT ON {environment_schema}.scaled_event_sku_clk_rms TO PUBLIC;
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
/************** END - TABLE DDL CREATION **************/

/************** START - VIEW DDL CREATION **************/
-- 2022.06.08 - DDL build for scaled_event_dma_lkp_vw
    -- 2022.10.03 - Updated for Holiday purposes but also for AN
/*
REPLACE VIEW T2DL_DAS_SCALED_EVENTS.scaled_event_dma_lkp_vw 
AS
LOCK ROW FOR ACCESS
SELECT DISTINCT
     dmacd.store_num AS stor_num
    , CASE WHEN top_market_dma = '1' THEN TRIM(dma.dma_code)
        ELSE '0' END AS dma_code --join to clickstream on dma_code
    , CASE WHEN top_market_dma = '1' THEN UPPER(dma.dma_desc)
        ELSE 'OTHER' END AS dma
    , CASE WHEN top_market_dma = '1' THEN UPPER(dma.dma_shrt_desc)
        ELSE 'OTHER' END AS dma_short
    , CASE WHEN top_market_dma = '1' THEN zp.dma_proxy_zip
        ELSE 'OTHER' END dma_proxy_zip
FROM PRD_NAP_USR_VWS.ORG_STORE_US_DMA dmacd
LEFT JOIN PRD_NAP_USR_VWS.ORG_DMA dma
    ON dmacd.us_dma_code = dma.dma_code
LEFT JOIN
    (
    -- US DMA
	SELECT
		us_dma_code AS dma_cd
		, CASE WHEN us_dma_code IN ('803', '501', '602', '807', '819', '825', '623', '511', '753', '528', '504', '820', '751', '506', '613', '618', '635', '505', '862', '539', '524', '659', '512', '770', '548') THEN 1 ELSE 0
		END AS top_market_dma
		, MAX(us_zip_code) AS dma_proxy_zip
	FROM PRD_NAP_USR_VWS.ORG_US_ZIP_DMA oczd 
	GROUP BY 1
    ) zp
        ON dma.dma_code = zp.dma_cd;

--GRANT SELECT ON T2DL_DAS_SCALED_EVENTS.SCALED_EVENT_DMA_LKP_VW TO PUBLIC;

-- 2022.06.08 - DDL build for scaled_event_dma_lkp_vw
REPLACE VIEW {environment_schema}.scaled_event_sku_last_receipt_vw 
AS
LOCK ROW FOR ACCESS
SELECT
    rcpt.sku_idnt
    , MAX(tdl.day_date) AS last_receipt_date
FROM
    prd_ma_bado.rcpt_sku_ld_cvw rcpt
INNER JOIN PRD_NAP_USR_VWS.DAY_CAL tdl
        ON rcpt.day_idnt = tdl.day_num
    WHERE tdl.year_num >= EXTRACT(YEAR FROM CURRENT_DATE())-1 --LY
GROUP BY 1;

--GRANT SELECT ON {environment_schema}.SCALED_EVENT_SKU_LAST_RECEIPT_VW TO PUBLIC;
*/
/******************************************************************************
** FILE: scaled_event_locs_vw.sql
** VIEW: T3DL_ACE_MCH.SCALED_EVENT_LOCS_VW
**
** DESCRIPTION:
**      - Creates a location lookup view for Daily Scaled Event Reporting (Anniversary and Cyber periods)
**
** TO DO:
**		- Productionalize in T2DL_DAS_SCALED_EVENTS 
**
** AUTHOR:
**      - Alli Moore (alli.moore@nordstrom.com)
******************************************************************************/
/*
-- 2022.06.08 - last updated
REPLACE VIEW {environment_schema}.scaled_event_locs_vw 
AS
LOCK ROW FOR ACCESS
SELECT DISTINCT
    store_num AS loc_idnt
    , TRIM(channel_num) || ', ' || TRIM(channel_desc) AS chnl_label
    , channel_num AS chnl_idnt
    , TRIM(store_num) || ', ' || TRIM(store_short_name) AS loc_label 
    , store_country_code AS country
    , CASE 
    	WHEN channel_num IN ('110', '120', '130', '140', '210', '220', '250', '260', '310', '111', '121', '211', '221', '261', '311', '920', '921', '922') THEN 1
    	ELSE 0
    END AS cyber_loc_ind -- FP/OP stores, warehouse, RS, and DC
    , CASE WHEN channel_num IN (110,111,120,121,130,140,310,311,920,921) THEN 1
    ELSE 0
    END AS anniv_loc_ind -- FP stores, RS, and DC (removed selling_store_ind to incorporate RS + DC)
FROM PRD_NAP_USR_VWS.STORE_DIM
WHERE channel_num IN ('110', '120', '130', '140', '210', '220', '250', '260', '310', '111', '121', '211', '221', '261', '311', '920','921','922')
    AND store_close_date IS NULL;

--GRANT SELECT ON {environment_schema}.SCALED_EVENT_LOCS_VW TO PUBLIC;



-- 2022.06.08 - DDL build for anniversary_plan_lkp_vw
REPLACE VIEW {environment_schema}.anniversary_plan_lkp_vw 
AS
LOCK ROW FOR ACCESS
SELECT
	Country
	, CASE 
		WHEN Country = 'US' AND Channel_Type = 'FLS' THEN '110, FULL LINE STORES'
		WHEN Country = 'US' AND Channel_Type = 'N.COM' THEN '120, N.COM'
		WHEN Country = 'CA' AND Channel_Type = 'FLS' THEN '111, FULL LINE STORES CANADA'
		WHEN Country = 'CA' AND Channel_Type = 'N.COM' THEN '121, N.CA'
	END AS channel
	, CASE 
		WHEN Country = 'US' AND Channel_Type = 'FLS' THEN '110'
		WHEN Country = 'US' AND Channel_Type = 'N.COM' THEN '120'
		WHEN Country = 'CA' AND Channel_Type = 'FLS' THEN '111'
		WHEN Country = 'CA' AND Channel_Type = 'N.COM' THEN '121'
	END AS channel_idnt
	, Department AS dept_label
	, STRTOK(Department, ',', 1) AS dept_idnt
	, STRTOK(Department, ',', 2) AS dept_desc
	, Sales_Plan_Dollars
FROM {environment_schema}.ANNIV_SALES_PLAN_22_STAGING;
*/
--GRANT SELECT ON {environment_schema}.ANNIVERSARY_PLAN_LKP_VW TO PUBLIC;

/******************************************************************************
** FILE: ma_pipeline_prod_main_se_cyb_prep_sales_plan.sql
** TABLE: {environment_schema}.cyber_sales_plan_22
**
** DESCRIPTION:
**      - View that contains the financial sales plans. This data is provided by business and is static based upon a T3 Data upload table
**      - PARAMS:   
**          - environment_schema = Schema data will reside in
**
** TO DO:
**      - Productionalize in T2DL_DAS_SCALED_EVENTS 
**
******************************************************************************/

-- 2022.11.01 - Updating to be view instead of table for more dynamic flexibility
REPLACE VIEW {environment_schema}.CYBER_SALES_PLAN_22_VW  
AS
LOCK ROW FOR ACCESS 
SELECT 
    BANNER 
    , DEPT_LABEL
    , STRTOK(dept_label, ',',1) AS DEPT_IDNT 
    , CATEGORY
    , TY_SALES
    , LY_SALES
    ,current_timestamp as process_tmstp
FROM t3dl_ace_mch.CYBER_SALES_PLAN_22_STAGING;

GRANT SELECT ON {environment_schema}.CYBER_SALES_PLAN_22_VW TO PUBLIC;



/******************************************************************************
** FILE: anniversary_item_vw.sql
** TABLE: {environment_schema}.anniversary_item_vw
**
** DESCRIPTION:
**      - Source of Anniversary Item Tableau data source
**
** TO DO:
**      - Productionalize in T2DL_DAS_SCALED_EVENTS 
**
** AUTHOR:
**      - Alli Moore (alli.moore@nordstrom.com)
******************************************************************************/
/*
-- 2022.06.20 - DDL build for anniversary_item_vw
-- Last Updated - 2022.07.05
REPLACE VIEW {environment_schema}.anniversary_item_vw 
AS
LOCK ROW FOR ACCESS
WITH locs AS (
    SELECT 
        ap.vpn
        , ap.supp_color
        , ap.colr_idnt
        , ap.dma_short
        , ap.anniv_ind
        , ap.rp_ind
        , ap.ty_ly_ind
        , ap.channel
        , ap.dropship_ind
        , COUNT(DISTINCT location) AS loc_count
    FROM {environment_schema}.scaled_event_sku_clk_rms ap   
    WHERE ap.day_dt >=
        (
            SELECT MIN(day_dt) AS anniv_start FROM {environment_schema}.scaled_event_dates
            WHERE yr_idnt = (SELECT MAX(yr_idnt) FROM {environment_schema}.scaled_event_dates WHERE anniv_ind = 1)
            AND anniv_ind = 1
        )
        AND date_event_type = 'Anniversary'
        AND ap.receipt_units > 0
        AND ap.dropship_ind = 'N'
    GROUP BY 1,2,3,4,5,6,7,8,9
)
SELECT 
    ap.day_idnt
    , ap.day_dt
--    , ap.day_dt_aligned
    , ap.price_type
    , ap.anniv_ind
    , ap.rp_ind
    , ap.ty_ly_ind
    , ap.country
    , ap.channel
    , ap.dropship_ind
    --, ap.daily_drop_ind
--    , ap.event_phase_orig
--    , ap.event_day_orig
    , ap.event_phase
    , ap.event_day
    , ap.dma_short
    --, ap.division_group
    , ap.division
    , ap.subdivision
    , ap.department
    , ap."class"
    , ap.subclass
    , ap.style_group_idnt
    , ap.style_num
    , ap.style_desc
    , ap.vpn
    , ap.supp_color
    , ap.colr_idnt
    --, sku.supp_idnt AS supplier_num
    , ap.supplier
    --, sup.brand_tier_name AS brand_tier
    , ap.brand
    , ap.npg_ind
    , ap.quantrix_category -- NEW 2022
    , ap.ccs_category -- NEW 2022
    , ap.ccs_subcategory -- NEW 2022
    , ap.nord_role_desc -- NEW 2022
    , ap.merch_theme
    , ap.anniversary_theme -- NEW 2022
    , ap.udig
    --, brand.brand_grp_name AS house
    , ap.parent_group
    , ap.price_band_one
    , ap.price_band_two
    , ZEROIFNULL(MAX(loc_count)) AS loc_count
    , MAX(sd.sold_out_dt_ind) AS sold_out_dt_ind
    , MAX(sd.sold_out_status) AS sold_out_status
    , MAX(sd.sold_out_flip_ds) AS sold_out_flip_ds
    , MAX(ap.retail_original) AS retail_original
    , MIN(ap.retail_special) AS retail_special
    , SUM(ap.sales_units) AS sales_units
    , SUM(ap.sales_dollars) AS sales_dollars
    , SUM(ap.return_units) AS return_units
    , SUM(ap.return_dollars) AS return_dollars
    , SUM(ap.demand_units) AS demand_units
    , SUM(ap.demand_dollars) AS demand_dollars
    , SUM(ap.demand_cancel_units) AS demand_cancel_units
    , SUM(ap.demand_cancel_dollars) AS demand_cancel_dollars
    , SUM(ap.shipped_units) AS shipped_units
    , SUM(ap.shipped_dollars) AS shipped_dollars
    , SUM(ap.eoh_units) AS eoh_units
    , SUM(ap.eoh_dollars) AS eoh_dollars
    , SUM(ap.boh_units) AS boh_units
    , SUM(ap.boh_dollars) AS boh_dollars
    , SUM(ap.nonsellable_units) AS nonsellable_units
    , SUM(ap.receipt_units) AS receipt_units
    , SUM(ap.receipt_dollars) AS receipt_dollars
    --, SUM(ap.dtc_units) AS dtc_units
    --, SUM(ap.dtc_dollars) AS dtc_dollars
    , SUM(ap.store_fulfill_units) AS store_fulfill_units
    , SUM(ap.store_fulfill_dollars) AS store_fulfill_dollars
    , SUM(ap.dropship_units) AS dropship_units
    , SUM(ap.dropship_dollars) AS dropship_dollars
    , SUM(ap.demand_dropship_units) AS demand_dropship_units
    , SUM(ap.demand_dropship_dollars) AS demand_dropship_dollars
    , SUM(ap.receipt_dropship_units) AS receipt_dropship_units
    , SUM(ap.receipt_dropship_dollars) AS receipt_dropship_dollars
    , SUM(ap.on_order_units) AS on_order_units
    , SUM(ap.on_order_retail_dollars) AS on_order_retail_dollars
    , SUM(ap.on_order_4wk_units) AS on_order_4wk_units
    , SUM(ap.on_order_4wk_retail_dollars) AS on_order_4wk_retail_dollars
    , SUM(ap.backorder_units) AS backorder_units
    , SUM(ap.backorder_dollars) AS backorder_dollars
    , SUM(ap.product_views) AS product_views
    , SUM(ap.cart_adds)  AS cart_adds
    , SUM(ap.order_units) AS com_order_units
    , SUM(ap.instock_views) AS instock_views
    , SUM(ap.wishlist_adds) AS wishlist_adds
    , SUM(ap.demand) AS com_demand
    , SUM(ap.units) AS units
    , SUM(ap.units * ap.retail_original) AS retail_original_dollars
    , SUM(ap.units * ap.retail_special) AS retail_special_dollars
    , MAX(ap.last_receipt_date) AS last_receipt_date
FROM {environment_schema}.scaled_event_sku_clk_rms ap
LEFT JOIN (
    SELECT DISTINCT
        rms_sku_num AS sku_idnt
        , dept_num AS dept_idnt
        , prmy_supp_num AS supp_idnt
        , supp_part_num
        , color_num AS colr_idnt
        , supp_color
        , channel_country
    FROM PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY rms_sku_num, channel_country ORDER BY live_date DESC) = 1
)sku 
	ON ap.sku_idnt = sku.sku_idnt
        AND ap.country = sku.channel_country
LEFT JOIN {environment_schema}.anniversary_soldout_vw sd 
    ON sku.dept_idnt = sd.dept_idnt
        AND sku.supp_idnt = sd.supp_idnt
        AND sku.supp_part_num = sd.vpn
        AND sku.colr_idnt = sd.colr_idnt
        AND sku.supp_color = sd.supp_color
        AND ap.day_idnt = sd.day_idnt
        AND ap.country = sd.country
LEFT JOIN locs l 
    ON l.vpn = ap.vpn 
        AND l.supp_color = ap.supp_color 
        AND l.colr_idnt = ap.colr_idnt 
        AND l.dma_short = ap.dma_short 
        AND l.anniv_ind = ap.anniv_ind  
        AND l.rp_ind = ap.rp_ind 
        AND l.ty_ly_ind = ap.ty_ly_ind 
        AND l.channel = ap.channel 
        AND l.dropship_ind = ap.dropship_ind 
WHERE  ap.ty_ly_ind = 'TY' 
    AND ap.anniv_ind = 'Y'
    AND ap.date_event_type = 'Anniversary'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36;

GRANT SELECT ON {environment_schema}.anniversary_item_vw TO PUBLIC;
*/
/******************************************************************************
** FILE: anniversary_location_vw.sql
** TABLE: {environment_schema}.anniversary_location_vw
**
** DESCRIPTION:
**      - Source of Anniversary Location Tableau data source
**
** TO DO:
**      - Productionalize in T2DL_DAS_SCALED_EVENTS 
**
** AUTHOR:
**      - Alli Moore (alli.moore@nordstrom.com)
******************************************************************************/
/*
REPLACE VIEW {environment_schema}.anniversary_location_vw 
AS
LOCK ROW FOR ACCESS
SELECT 
      day_idnt
    , day_dt
    , day_dt_aligned
    , price_type
    , anniv_ind
    , rp_ind
    , ty_ly_ind
    , country
    , channel
    , "location"
    , dropship_ind
    --, daily_drop_ind
    , event_phase_orig
    , event_day_orig
    , event_phase
    , event_day
    , dma_short
    , dma_proxy_zip
    , division
    , subdivision
    , department
    , supplier
    , brand
    , npg_ind
    , quantrix_category -- NEW 2022
    , ccs_category -- NEW 2022
    , ccs_subcategory -- NEW 2022
    , nord_role_desc -- NEW 2022
    , merch_theme
    , anniversary_theme -- NEW 2022
    , udig
    , parent_group
--    , price_band_one
    --, price_band_two
--    , MAX(retail_original) AS retail_original
--    , MIN(retail_special) AS retail_special
    , SUM(sales_units) AS sales_units
    , SUM(sales_dollars) AS sales_dollars
    , SUM(return_units) AS return_units
    , SUM(return_dollars) AS return_dollars
    , SUM(demand_units) AS demand_units
    , SUM(demand_dollars) AS demand_dollars
    , SUM(demand_cancel_units) AS demand_cancel_units
    , SUM(demand_cancel_dollars) AS demand_cancel_dollars
    , SUM(shipped_units) AS shipped_units
    , SUM(shipped_dollars) AS shipped_dollars
    , SUM(eoh_units) AS eoh_units
    , SUM(eoh_dollars) AS eoh_dollars
    , SUM(boh_units) AS boh_units
    , SUM(boh_dollars) AS boh_dollars
    , SUM(nonsellable_units) AS nonsellable_units
    , SUM(receipt_units) AS receipt_units
    , SUM(receipt_dollars) AS receipt_dollars
    --, SUM(dtc_units) AS dtc_units
    --, SUM(dtc_dollars) AS dtc_dollars
    , SUM(store_fulfill_units) AS store_fulfill_units
    , SUM(store_fulfill_dollars) AS store_fulfill_dollars
    , SUM(dropship_units) AS dropship_units
    , SUM(dropship_dollars) AS dropship_dollars
    , SUM(demand_dropship_units) AS demand_dropship_units
    , SUM(demand_dropship_dollars) AS demand_dropship_dollars
    , SUM(receipt_dropship_units) AS receipt_dropship_units
    , SUM(receipt_dropship_dollars) AS receipt_dropship_dollars
    , SUM(on_order_units) AS on_order_units
    , SUM(on_order_retail_dollars) AS on_order_retail_dollars
    , SUM(on_order_4wk_units) AS on_order_4wk_units
    , SUM(on_order_4wk_retail_dollars) AS on_order_4wk_retail_dollars
    , SUM(backorder_units) AS backorder_units
    , SUM(backorder_dollars) AS backorder_dollars
FROM 
	(SELECT *
	 FROM {environment_schema}.scaled_event_sku_clk_rms 
	 WHERE event_phase <> 'Post'
    	AND date_event_type = 'Anniversary'
    	AND day_dt_aligned <= (select max(day_dt) from {environment_schema}.scaled_event_sku_clk_rms)) ap
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31;

GRANT SELECT ON {environment_schema}.anniversary_location_vw TO PUBLIC;
*/
/******************************************************************************
** FILE: anniversary_soldout_vw.sql
** TABLE: {environment_schema}.anniversary_soldout_vw
**
** DESCRIPTION:
**      - Source of sould out indicator for Anniversary Item Views
**
** TO DO:
**      - Productionalize in T2DL_DAS_SCALED_EVENTS 
**
** AUTHOR:
**      - Alli Moore (alli.moore@nordstrom.com)
******************************************************************************/
/*
-- Last Updated: 2022.06.23
REPLACE VIEW {environment_schema}.anniversary_soldout_vw 
AS
LOCK ROW FOR ACCESS
	SELECT
		al.day_idnt
		, al.day_dt
		, al.country
		, al.dept_idnt
		, al.supp_idnt
		, al.vpn
		, al.colr_idnt
		, al.supp_color
		, al.dropship_ind
		, al.dropship_ind_ld
		, al.sales_units
		, al.return_units
		, al.receipt_units
		--, al.sales_units_ld
		, al.cum_sales_units
		, al.cum_sales_units_ld
		--, al.return_units_ld
		, al.eoh_units
		, al.boh_units
		, al.nonsellable_units
		, al.nonsellable_units_ld
		, al.prev_eoh
		, al.prev_ds
		, COALESCE ((CASE WHEN (cum_sales_units_ld+boh_units) > 0
				 THEN (cum_sales_units_ld+nonsellable_units_ld)*1.00/(cum_sales_units_ld+boh_units) END),0) AS sll_through_ld
		, COALESCE ((CASE WHEN (cum_sales_units+eoh_units) > 0
				 THEN (cum_sales_units+nonsellable_units)*1.00/(cum_sales_units+eoh_units) END),0) AS sll_through
		, CASE WHEN prev_eoh >0 THEN 1 ELSE 0 END AS prev_eoh_status  --Has had EOH
		, CASE WHEN eoh_units >0 THEN 1 ELSE 0 END AS curr_eoh_status -- Currently Has EOH
		, CASE WHEN prev_receipt >0 THEN 1 ELSE 0 END AS prev_receipt_status -- Has had receipts
		, CASE WHEN dropship_ind = 0 AND prev_ds = 0 AND prev_eoh_status = 1 AND sll_through_ld < 0.95 AND sll_through >= 0.95 THEN 1 -- (Non DS): Non-DS Today, NEVER DS, Has/had EOH, Yesterday's ST < 95%, Today's ST >= 95%
				 --WHEN dropship_ind = 0 AND dropship_ind_ld = 1 AND curr_eoh_status = 0 AND prev_receipt_status = 1 THEN 1 -- (DS): Non-DS Today, Was DS Yesterday, Currently NO EOH, previously had receipts (AKA DROPSHIP ONLY)
				 WHEN dropship_ind = 0 AND prev_ds = 1 AND curr_eoh_status = 1 AND sll_through_ld < 0.95 AND sll_through >= 0.95 THEN 1 -- Non-DS today, previously was DS, currently has EOH, Yesterday's ST < 95%, Today's ST >= 95%
			ELSE 0 END AS sold_out_dt_ind
		, CASE WHEN dropship_ind = 0 AND prev_ds = 0 AND prev_eoh_status = 1 AND sll_through >= 0.95 THEN 1 -- (Non DS): Non-DS Today, NEVER DS, Has Had EOH, Today's ST >= 95%
				 --WHEN dropship_ind = 0 AND prev_ds = 1 AND curr_eoh_status = 0 AND prev_receipt_status = 1 THEN 1 -- (DS): Non-DS Today, Previously DS, Currently NO EOH, Previously had Receipts
				 WHEN dropship_ind = 0 AND curr_eoh_status = 1 AND sll_through >= 0.95 THEN 1 -- (DS with EOH): Non-DS Today, Previously DS, Currently Has EOH, Today's ST >= 95%
			ELSE 0 END AS sold_out_status
		, CASE WHEN dropship_ind = 1 AND dropship_ind_ld = 0 THEN 1 ELSE 0 END AS flip_ds  -- WHEN currently dropship BUT NOT dropship yesterday
		, CASE WHEN flip_ds = 1 AND sold_out_dt_ind = 1 THEN 1 ELSE 0 END AS sold_out_flip_ds -- WHEN currently dropship BUT NOT dropship yesterday AND is sold OUT (Always going to be 0 for 2022)
	FROM
	(
		SELECT
			base.day_idnt
			, base.day_dt
			, base.country
			, sku.dept_num AS dept_idnt
			, sku.prmy_supp_num AS supp_idnt
			, sku.supp_part_num AS vpn
			, sku.color_num AS colr_idnt
			, sku.supp_color
			, MAX(COALESCE(prev_eoh,0)) AS prev_eoh  --Previously has EOH
			, MAX(COALESCE(ds_status.prev_ds,0)) AS prev_ds -- Prevously was dropship
			, MAX(COALESCE(ds_status.prev_receipt,0)) AS prev_receipt  --Previously had receipts
			, MAX(CASE WHEN base.dropship_ind = 'Y' THEN 1 ELSE 0 END) AS dropship_ind
			, MAX(CASE WHEN base.dropship_ind_ld = 'Y' THEN 1 ELSE 0 END) AS dropship_ind_ld
			, SUM(cum_sales.cum_sales_units) AS cum_sales_units
			, SUM(base.sales_units) AS sales_units
			, SUM(base.return_units) AS return_units
			, SUM(base.receipt_units) AS receipt_units
			, SUM(cum_sales_ld.cum_sales_units) AS cum_sales_units_ld
			--, SUM(COALESCE(sales_ld.return_units_ld,0)) AS return_units_ld
			, SUM(base.eoh_units) AS eoh_units
			, SUM(base.boh_units) AS boh_units
			, SUM(base.nonsellable_units) AS nonsellable_units
			, SUM(unavailable_ld.nonsellable_units_ld) AS nonsellable_units_ld
		FROM  -- PULLS BASE WITH SALES/RETURN/EOH/BOH/NONSELL/RECEIPT UNITS FOR ANNIV ITEMS ONLY SKU/DATE/COUNTRY WITH DS_IND/DS_IND_LD
		(
			SELECT
					ap.day_idnt
					, ap.day_dt
					, ap.country
					, ap.sku_idnt
					, ap.dropship_ind
					, CASE WHEN ds_ld.sku_idnt IS NOT NULL THEN 'Y' ELSE 'N' END AS dropship_ind_ld
					, SUM(ap.sales_units) AS sales_units
					, SUM(ap.return_units) AS return_units
					, SUM(ap.eoh_units) AS eoh_units
					, SUM(ap.boh_units) AS boh_units
					, SUM(ap.nonsellable_units) AS nonsellable_units
					, SUM(ap.receipt_units) AS receipt_units
					--, SUM(ap.receipt_dropship_units) AS receipt_dropship_units
			FROM {environment_schema}.scaled_event_sku_clk_rms ap
			LEFT JOIN prd_ma_bado.dshp_sku_lkup_vw ds_ld  -- UPDATE? Keep in MADM until next year (2023)
					ON ap.sku_idnt = ds_ld.sku_idnt
					AND ap.day_idnt = ds_ld.day_idnt+1
					AND ap.country = 'US'
			WHERE ap.ty_ly_ind = 'TY' 
				AND ap.anniv_ind = 'Y'
				AND ap.date_event_type = 'Anniversary'
			GROUP BY 1,2,3,4,5,6
		) base
		-- PULLS NONSELLABLE UNITS BY SKU/DAY/COUNTRY (For all of time? Don't we want just anniv?)
		LEFT JOIN
		(		
				SELECT
					nsl.sku_idnt 
					, ps.channel_country AS country
					, dt.day_num+1 AS day_idnt
					, SUM(NONSELLABLE_UNITS) AS nonsellable_units_ld
				FROM T2DL_DAS_ACE_MFP.SKU_LOC_PRICETYPE_DAY_VW nsl	
				LEFT JOIN PRD_NAP_USR_VWS.DAY_CAL dt 
					ON dt.day_date = nsl.day_dt
				LEFT JOIN PRD_NAP_USR_VWS.PRICE_STORE_DIM_VW ps
					ON ps.store_num = nsl.loc_idnt
				WHERE nsl.day_dt >=
						(
							SELECT MIN(day_dt) AS anniv_start FROM {environment_schema}.scaled_event_dates
							WHERE yr_idnt = (SELECT MAX(yr_idnt) FROM {environment_schema}.scaled_event_dates WHERE anniv_ind = 1)
							AND anniv_ind = 1
						)
				GROUP BY 1,2,3
		) unavailable_ld
			ON base.sku_idnt = unavailable_ld.sku_idnt
			AND base.day_idnt = unavailable_ld.day_idnt
			ANd base.country = unavailable_ld.country
		LEFT JOIN (
			SELECT DISTINCT
				rms_sku_num 
				, dept_num 
				, prmy_supp_num
				, supp_part_num
				, color_num
				, supp_color
			FROM PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW 
			QUALIFY ROW_NUMBER() OVER (PARTITION BY rms_sku_num ORDER BY live_date DESC) = 1
		)sku 
			ON base.sku_idnt = sku.rms_sku_num
		-- PULLS Maximum Previous EOH BY sku/country/date FOR Anniv Items ONLY during Anniv TY dates
		LEFT JOIN
		( 
			SELECT
				base_eoh.day_idnt
				, base_eoh.day_dt
				, base_eoh.country
				, base_eoh.sku_idnt
				, MAX(COALESCE(eoh_check.eoh_units,0)) AS prev_eoh
			FROM
				( -- PULLS Anniv item only  sales/RETURNS/eoh/boh/receipt/nonsell units BY SKU/DAY/COUNTRY
					SELECT
							ap.day_idnt
							, ap.day_dt
							, ap.country
							, ap.sku_idnt
							, SUM(ap.sales_units) AS sales_units
							, SUM(ap.return_units) AS return_units
							, SUM(ap.eoh_units) AS eoh_units
							, SUM(ap.boh_units) AS boh_units
							, SUM(ap.nonsellable_units) AS nonsellable_units
							, SUM(ap.receipt_units) AS receipt_units

					FROM {environment_schema}.scaled_event_sku_clk_rms ap
					WHERE ap.ty_ly_ind = 'TY' 
						AND ap.anniv_ind = 'Y'
					GROUP BY 1,2,3,4	
				) base_eoh
			LEFT JOIN
				(  --- PULLS EOH BY SKU/COUNTRY/DAY FOR ANNIV TY ALL ITEMS
					SELECT
							ap.day_idnt
							, ap.day_dt
							, ap.country
							, ap.sku_idnt
							, SUM(ap.eoh_units) AS eoh_units
							--, SUM(ap.receipt_units) AS receipt_units
					FROM {environment_schema}.scaled_event_sku_clk_rms ap
					WHERE ap.day_dt >=
						(
							SELECT MIN(day_dt) AS anniv_start FROM {environment_schema}.scaled_event_dates
							WHERE yr_idnt = (SELECT MAX(yr_idnt) FROM {environment_schema}.scaled_event_dates WHERE anniv_ind = 1)
							AND anniv_ind = 1
						)
						AND date_event_type = 'Anniversary'
					GROUP BY 1,2,3,4
				) eoh_check
				ON base_eoh.sku_idnt = eoh_check.sku_idnt
				AND base_eoh.day_dt >= eoh_check.day_dt
				AND base_eoh.country = eoh_check.country
			GROUP BY 1,2,3,4
		) eoh
			ON base.sku_idnt = eoh.sku_idnt
			AND base.day_dt = eoh.day_dt
			AND base.country = eoh.country	
		-- PULLS CUMULATIVE SALES UNITS BY SKU/DAY/COUNTRY FOR ANNIV ITEMS ONLY DURING ANNIV TY PERIOD
		LEFT JOIN
		( 
			SELECT
				base_eoh.day_idnt
				, base_eoh.day_dt
				, base_eoh.country
				, base_eoh.sku_idnt
				, SUM(sales.sales_units) AS cum_sales_units
			FROM
				( -- PULLS Anniv item only sales/RETURNS/eoh/boh/receipt/nonsell units BY SKU/DAY/COUNTRY
					SELECT
							ap.day_idnt
							, ap.day_dt
							, ap.country
							, ap.sku_idnt
							, SUM(ap.sales_units) AS sales_units
							, SUM(ap.return_units) AS return_units
							, SUM(ap.eoh_units) AS eoh_units
							, SUM(ap.boh_units) AS boh_units
							, SUM(ap.nonsellable_units) AS nonsellable_units
							, SUM(ap.receipt_units) AS receipt_units
					FROM {environment_schema}.scaled_event_sku_clk_rms ap
					WHERE ap.ty_ly_ind = 'TY' 
						AND ap.anniv_ind = 'Y'
					GROUP BY 1,2,3,4
				) base_eoh
			LEFT JOIN
				(-- PULLS SALE UNITS BY SKU/COUNTRY/DAY FOR ANNIV TY ALL ITEMS
					SELECT
							ap.day_idnt
							, ap.day_dt
							, ap.country
							, ap.sku_idnt
							, SUM(ap.sales_units) AS sales_units
							--, SUM(ap.receipt_units) AS receipt_units
					FROM {environment_schema}.scaled_event_sku_clk_rms ap
					WHERE ap.day_dt >=
						(
							SELECT MIN(day_dt) AS anniv_start FROM {environment_schema}.scaled_event_dates
							WHERE yr_idnt = (SELECT MAX(yr_idnt) FROM {environment_schema}.scaled_event_dates where anniv_ind = 1)
							AND anniv_ind = 1
						)
						AND date_event_type = 'Anniversary'
					GROUP BY 1,2,3,4
				) sales
				ON base_eoh.sku_idnt = sales.sku_idnt
					AND base_eoh.day_dt >= sales.day_dt
					AND base_eoh.country = sales.country
			GROUP BY 1,2,3,4
		) cum_sales
			ON base.sku_idnt = cum_sales.sku_idnt
				AND base.day_dt = cum_sales.day_dt
				AND base.country = cum_sales.country
		-- PULLS CUMULATIVE SALES UNITS BY SKU/DAY/COUNTRY FOR ANNIV ITEMS ONLY DURING ANNIV TY PERIOD (LAST DATE - i.e. lagging sum, BOH-type sum)
		LEFT JOIN
		( 
			SELECT
				base_eoh.day_idnt
				, base_eoh.day_dt
				, base_eoh.country
				, base_eoh.sku_idnt
				, SUM(sales.sales_units) AS cum_sales_units
			FROM
				( -- PULLS Anniv item only sales/RETURNS/eoh/boh/receipt/nonsell units BY SKU/DAY/COUNTRY FOR ALL EVENT
					SELECT
							ap.day_idnt
							, ap.day_dt
							, ap.country
							, ap.sku_idnt
							, SUM(ap.sales_units) AS sales_units
							, SUM(ap.return_units) AS return_units
							, SUM(ap.eoh_units) AS eoh_units
							, SUM(ap.boh_units) AS boh_units
							, SUM(ap.nonsellable_units) AS nonsellable_units
							, SUM(ap.receipt_units) AS receipt_units
					FROM {environment_schema}.scaled_event_sku_clk_rms ap
					WHERE ap.ty_ly_ind = 'TY' 
						AND ap.anniv_ind = 'Y'  	
					GROUP BY 1,2,3,4
				) base_eoh
			LEFT JOIN
				( -- PULLS SALE UNITS BY SKU/COUNTRY/DAY FOR ANNIV TY ALL ITEMS
					SELECT
							ap.day_idnt
							, ap.day_dt
							, ap.country
							, ap.sku_idnt
							, SUM(ap.sales_units) AS sales_units
							--, SUM(ap.receipt_units) AS receipt_units
					FROM {environment_schema}.scaled_event_sku_clk_rms ap
					WHERE ap.day_dt >=
						(
							SELECT MIN(day_dt) AS anniv_start FROM {environment_schema}.scaled_event_dates
							WHERE yr_idnt = (SELECT MAX(yr_idnt) FROM {environment_schema}.scaled_event_dates where anniv_ind = 1)
							AND anniv_ind = 1
						)
						AND date_event_type = 'Anniversary'
					GROUP BY 1,2,3,4
				) sales
				ON base_eoh.sku_idnt = sales.sku_idnt
				AND base_eoh.day_dt > sales.day_dt
				AND base_eoh.country = sales.country
			GROUP BY 1,2,3,4
		) cum_sales_ld
			ON base.sku_idnt = cum_sales_ld.sku_idnt
			AND base.day_dt = cum_sales_ld.day_dt
			AND base.country = cum_sales_ld.country	
		-- PULLS RECEIPT UNITS AND DS_IND HISTORY BY SKU/DAY/COUNTRY FOR ANNIV ITEMS ONLY DURING ANNIV TY PERIOD (LAST DATE - i.e. lagging sum, BOH-type sum)
		LEFT JOIN
		( 
			SELECT
				base_eoh.day_idnt
				, base_eoh.day_dt
				, base_eoh.country
				, base_eoh.sku_idnt
				, MAX(COALESCE(CASE WHEN ds_status_check.dropship_ind = 'Y' THEN 1 ELSE 0 END,0)) AS prev_ds
				, MAX(COALESCE(ds_status_check.receipt_units,0)) AS prev_receipt
			FROM
			( -- PULLS Anniv item only sales/RETURNS/eoh/boh/receipt/nonsell units BY SKU/DAY/COUNTRY FOR ALL EVENT DATES
				SELECT
						ap.day_idnt
						, ap.day_dt
						, ap.country
						, ap.sku_idnt
						, SUM(ap.sales_units) AS sales_units
						, SUM(ap.return_units) AS return_units
						, SUM(ap.eoh_units) AS eoh_units
						, SUM(ap.boh_units) AS boh_units
						, SUM(ap.nonsellable_units) AS nonsellable_units
						, SUM(ap.receipt_units) AS receipt_units
				FROM {environment_schema}.scaled_event_sku_clk_rms ap
				WHERE ap.ty_ly_ind = 'TY' 
					AND ap.anniv_ind = 'Y'
				GROUP BY 1,2,3,4
			) base_eoh
			LEFT JOIN
			(-- PULLS RECEIPT UNITS AND DS_IND (MAX) BY SKU/COUNTRY/DAY FOR ANNIV TY ALL ITEMS - Checks dropship eligibility OF ALL items
				SELECT
						ap.day_idnt
						, ap.day_dt
						, ap.country
						, ap.sku_idnt
						, MAX(dropship_ind) AS dropship_ind
						, SUM(ap.receipt_units) AS receipt_units

				FROM {environment_schema}.scaled_event_sku_clk_rms ap
				WHERE ap.day_dt >=
					(
						SELECT MIN(day_dt) AS anniv_start FROM {environment_schema}.scaled_event_dates
						WHERE yr_idnt = (SELECT MAX(yr_idnt) FROM {environment_schema}.scaled_event_dates WHERE anniv_ind = 1)
						AND anniv_ind = 1
					)
					AND date_event_type = 'Anniversary'
				GROUP BY 1,2,3,4
			) ds_status_check
				ON base_eoh.sku_idnt = ds_status_check.sku_idnt
				AND base_eoh.day_dt > ds_status_check.day_dt
				AND base_eoh.country = ds_status_check.country
			GROUP BY 1,2,3,4
		) ds_status
			ON base.sku_idnt = ds_status.sku_idnt
			AND base.day_dt = ds_status.day_dt
			AND base.country = ds_status.country
	GROUP BY 1,2,3,4,5,6,7,8
	) al;

GRANT SELECT ON {environment_schema}.anniversary_soldout_vw TO PUBLIC;
*/

/******************************************************************************
** FILE: anniversary_supplier_class_vw.sql
** TABLE: {environment_schema}.anniversary_supplier_class_vw
**
** DESCRIPTION:
**      - Source of Anniversary Supplier Class Tableau data source
**
** TO DO:
**      - Productionalize in T2DL_DAS_SCALED_EVENTS 
**
** AUTHOR:
**      - Alli Moore (alli.moore@nordstrom.com)
******************************************************************************/
/*
-- Last Updated: 2022.08.12
REPLACE VIEW {environment_schema}.anniversary_supplier_class_vw 
AS
LOCK ROW FOR ACCESS
select a.day_idnt
	, a.day_dt
	, a.day_dt_aligned
    , a.event_phase_orig
    , a.event_day_orig
    , a.event_phase
    , a.event_day
    , a.anniv_ind
    , a.price_type
    , a.rp_ind
    , a.ty_ly_ind
    , a.channel
    , a.country
    , a.dropship_ind
    , a.division
    , a.subdivision
    , a.department
    , a."class"
    , a.subclass
    , a.supplier
    , a.brand
    , a.npg_ind
    , a.quantrix_category -- NEW 2022
    , a.ccs_category -- NEW 2022
    , a.ccs_subcategory -- NEW 2022
    , a.nord_role_desc -- NEW 2022
    , a.parent_group 
    , a.merch_theme
    , a.anniversary_theme -- NEW 2022
    , a.udig
    , a.price_band_one
    , a.price_band_two
    , ZEROIFNULL(b.sales_plan_dollars) AS sales_plan
    , a.sales_units
    , a.sales_dollars
    , a.return_units
    , a.return_dollars
    , a.demand_units
    , a.demand_dollars
    , a.demand_cancel_units
    , a.demand_cancel_dollars
    , a.shipped_units
    , a.shipped_dollars
    , a.eoh_units
    , a.eoh_dollars
    , a.boh_units
    , a.boh_dollars
    , a.nonsellable_units
    , a.receipt_units
    , a.receipt_dollars
    , a.store_fulfill_units
    , a.store_fulfill_dollars
    , a.dropship_units
    , a.dropship_dollars
    , a.demand_dropship_units
    , a.demand_dropship_dollars
    , a.receipt_dropship_units
    , a.receipt_dropship_dollars
    , a.on_order_units
    , a.on_order_retail_dollars
    , a.on_order_4wk_units
    , a.on_order_4wk_retail_dollars
    , a.backorder_units
    , a.backorder_dollars
    , a.product_views
    , a.cart_adds
    , a.com_order_units
    , a.instock_views
    , a.com_demand
    , a.units
    , a.retail_original_dollars
    , a.retail_special_dollars
from 
	(SELECT 
	      ap.day_idnt
	    , ap.day_dt
	    , ap.day_dt_aligned
	    , ap.event_phase_orig
	    , ap.event_day_orig
	    , ap.event_phase
	    , ap.event_day
	    , ap.anniv_ind
	    , ap.price_type
	    , ap.rp_ind
	    , ap.ty_ly_ind
	    , ap.channel
	    , ap.country
	    , ap.dropship_ind
	    , ap.division
	    , ap.subdivision
	    , ap.department
	    , ap."class"
	    , ap.subclass
	    , ap.supplier
	    , ap.brand
	    , ap.npg_ind
	    , ap.quantrix_category -- NEW 2022
	    , ap.ccs_category -- NEW 2022
	    , ap.ccs_subcategory -- NEW 2022
	    , ap.nord_role_desc -- NEW 2022
	    , ap.parent_group 
	    , ap.merch_theme
	    , ap.anniversary_theme -- NEW 2022
	    , ap.udig
	    , ap.price_band_one
	    , ap.price_band_two
	--    , ZEROIFNULL(pl.sales_plan_dollars) AS sales_plan
	    , SUM(ap.sales_units) AS sales_units
	    , SUM(ap.sales_dollars) AS sales_dollars
	    , SUM(ap.return_units) AS return_units
	    , SUM(ap.return_dollars) AS return_dollars
	    , SUM(ap.demand_units) AS demand_units
	    , SUM(ap.demand_dollars) AS demand_dollars
	    , SUM(ap.demand_cancel_units) AS demand_cancel_units
	    , SUM(ap.demand_cancel_dollars) AS demand_cancel_dollars
	    , SUM(ap.shipped_units) AS shipped_units
	    , SUM(ap.shipped_dollars) AS shipped_dollars
	    , SUM(ap.eoh_units) AS eoh_units
	    , SUM(ap.eoh_dollars) AS eoh_dollars
	    , SUM(ap.boh_units) AS boh_units
	    , SUM(ap.boh_dollars) AS boh_dollars
	    , SUM(ap.nonsellable_units) AS nonsellable_units
	    , SUM(ap.receipt_units) AS receipt_units
	    , SUM(ap.receipt_dollars) AS receipt_dollars
	    , SUM(ap.store_fulfill_units) AS store_fulfill_units
	    , SUM(ap.store_fulfill_dollars) AS store_fulfill_dollars
	    , SUM(ap.dropship_units) AS dropship_units
	    , SUM(ap.dropship_dollars) AS dropship_dollars
	    , SUM(ap.demand_dropship_units) AS demand_dropship_units
	    , SUM(ap.demand_dropship_dollars) AS demand_dropship_dollars
	    , SUM(ap.receipt_dropship_units) AS receipt_dropship_units
	    , SUM(ap.receipt_dropship_dollars) AS receipt_dropship_dollars
	    , SUM(ap.on_order_units) AS on_order_units
	    , SUM(ap.on_order_retail_dollars) AS on_order_retail_dollars
	    , SUM(ap.on_order_4wk_units) AS on_order_4wk_units
	    , SUM(ap.on_order_4wk_retail_dollars) AS on_order_4wk_retail_dollars
	    , SUM(ap.backorder_units) AS backorder_units
	    , SUM(ap.backorder_dollars) AS backorder_dollars
	    , SUM(ap.product_views) AS product_views
	    , SUM(ap.cart_adds)  AS cart_adds
	    , SUM(ap.order_units) AS com_order_units
	    , SUM(ap.instock_views) AS instock_views
	    , SUM(ap.demand) AS com_demand
	    , SUM(ap.units) AS units
	    , SUM(ap.units * ap.retail_original) AS retail_original_dollars
	    , SUM(ap.units * ap.retail_special) AS retail_special_dollars
	FROM {environment_schema}.scaled_event_sku_clk_rms ap
	--    	AND day_dt_aligned <= (select max(day_dt) from T2DL_DAS_SCALED_EVENTS.scaled_event_sku_clk_rms)) ap
		-- sales plan
	WHERE date_event_type = 'Anniversary'
		and day_dt in (select distinct day_dt 
			from {environment_schema}.scaled_event_sku_clk_rms 
			where day_dt_aligned <= (select max(day_dt) from {environment_schema}.scaled_event_sku_clk_rms))
	GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32) a
		LEFT JOIN {environment_schema}.ANNIVERSARY_PLAN_LKP_VW b  
		    ON a.channel = b.channel
		        AND a.department = b.dept_label
		        AND ty_ly_ind = 'TY';

GRANT SELECT ON {environment_schema}.anniversary_supplier_class_vw TO PUBLIC;

-- 1:1 DDL view of annivessary_sku_2022_vw
-- Last Updated: 2022.06.24
CREATE VIEW {environment_schema}.anniversary_sku_2022_vw
as
lock ROW FOR access 
SELECT sku_idnt
	,country
	,process_tmstmp
FROM t2dl_das_scaled_events.anniversary_sku_2022;

GRANT SELECT ON {environment_schema}.anniversary_sku_2022_vw TO PUBLIC;
*/


/******************************************************************************
** FILE: anniversary_market_vw.sql
** TABLE: {environment_schema}.anniversary_market_vw
**
** DESCRIPTION:
**      - Source of Anniversary Market Tableau dashboard data source
**
** TO DO:
**      - Productionalize in T2DL_DAS_SCALED_EVENTS 
**
** AUTHOR:
**      - Alli Moore (alli.moore@nordstrom.com)
******************************************************************************/
/*
REPLACE VIEW {environment_schema}.anniversary_market_vw 
AS
LOCK ROW FOR ACCESS
SELECT --TOP 1000
      ap.day_idnt
    , ap.day_dt
    , ap.day_dt_aligned
    , ap.event_phase_orig
    , ap.event_day_orig
    , ap.event_phase
    , ap.event_day
    , ap.anniv_ind
    , ap.rp_ind
    , ap.ty_ly_ind
    , ap.channel
    , ap.country
    , ap.dropship_ind
    , ap.division
    , ap.subdivision
    , ap.department
    , ap."class"
    , ap.supplier
    , ap.brand
    , ap.npg_ind
    , ap.quantrix_category -- NEW 2022
    , ap.ccs_category -- NEW 2022
    , ap.ccs_subcategory -- NEW 2022
    , ap.nord_role_desc -- NEW 2022
    , ap.parent_group
    , ap.merch_theme
    , ap.anniversary_theme -- NEW 2022
    , ap.udig
    , ap.dma_short
    , ap.dma_proxy_zip
    , MAX(ap.retail_original) AS retail_original
    , MIN(ap.retail_special) AS retail_special
    , SUM(ap.sales_units) AS sales_units
    , SUM(ap.sales_dollars) AS sales_dollars
    , SUM(ap.return_units) AS return_units
    , SUM(ap.return_dollars) AS return_dollars
    , SUM(ap.demand_units) AS demand_units
    , SUM(ap.demand_dollars) AS demand_dollars
    , SUM(ap.demand_cancel_units) AS demand_cancel_units
    , SUM(ap.demand_cancel_dollars) AS demand_cancel_dollars
    , SUM(ap.shipped_units) AS shipped_units
    , SUM(ap.shipped_dollars) AS shipped_dollars
    , SUM(ap.eoh_units) AS eoh_units
    , SUM(ap.eoh_dollars) AS eoh_dollars
    , SUM(ap.boh_units) AS boh_units
    , SUM(ap.boh_dollars) AS boh_dollars
    , SUM(ap.nonsellable_units) AS nonsellable_units
    , SUM(ap.receipt_units) AS receipt_units
    , SUM(ap.receipt_dollars) AS receipt_dollars
    , SUM(ap.store_fulfill_units) AS store_fulfill_units
    , SUM(ap.store_fulfill_dollars) AS store_fulfill_dollars
    , SUM(ap.dropship_units) AS dropship_units
    , SUM(ap.dropship_dollars) AS dropship_dollars
    , SUM(ap.demand_dropship_units) AS demand_dropship_units
    , SUM(ap.demand_dropship_dollars) AS demand_dropship_dollars
    , SUM(ap.receipt_dropship_units) AS receipt_dropship_units
    , SUM(ap.receipt_dropship_dollars) AS receipt_dropship_dollars
    , SUM(ap.on_order_units) AS on_order_units
    , SUM(ap.on_order_retail_dollars) AS on_order_retail_dollars
    , SUM(ap.on_order_4wk_units) AS on_order_4wk_units
    , SUM(ap.on_order_4wk_retail_dollars) AS on_order_4wk_retail_dollars
    , SUM(ap.backorder_units) AS backorder_units
    , SUM(ap.backorder_dollars) AS backorder_dollars
    , SUM(ap.product_views) AS product_views
    , SUM(ap.cart_adds)  AS cart_adds
    , SUM(ap.order_units) AS com_order_units
    , SUM(ap.instock_views) AS instock_views
    , SUM(ap.demand) AS com_demand
FROM {environment_schema}.scaled_event_sku_clk_rms ap
WHERE date_event_type = 'Anniversary'
--    	AND day_dt_aligned <= (select max(day_dt) from T2DL_DAS_SCALED_EVENTS.scaled_event_sku_clk_rms)
	and day_dt in 
        (select distinct day_dt 
        from {environment_schema}.scaled_event_sku_clk_rms 
        where day_dt_aligned <= (select max(day_dt) from {environment_schema}.scaled_event_sku_clk_rms))
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30;

GRANT SELECT ON {environment_schema}.anniversary_market_vw TO PUBLIC;
*/

-- 2022.07.05 - Data Check View DDL Creation for AN Data Checks
    -- 2022.11.01 - Commented out for Cyber use, re-evaluate need for view for Anniv 23 use
/*
CREATE VIEW {environment_schema}.anniversary_data_check_vw
AS
LOCK ROW FOR ACCESS
WITH sources AS (
	SELECT source_table
		,date_check
		,source_database
		,source_field
		,check_type
	from (
		select 'product_price_funnel_daily' source_table
			,'t2dl_das_product_funnel' source_database
			,'event_date_pacific' source_field
			,'primary' check_type
			,current_date - 1 date_check
		from (select 1 as "dummy") "dummy"
		union all
		select 'sku_loc_pricetype_day_vw' source_table 
			,'t2dl_das_ace_mfp' source_database
			,'day_dt' source_field
			,'primary' check_type
			,current_date date_check
		from (select 1 as "dummy") "dummy"
		union all 
		select 'rcpt_sku_ld_rvw' source_table 
			,'prd_ma_bado' source_database
			,'dm_recd_load_dt' source_field
			,'primary' check_type
			,current_date - 1 date_check
		from (select 1 as "dummy") "dummy"
		) a
)
select CASE 
		WHEN b.date_check = a.date_updated then 'PASS'
		ELSE 'FAIL'
	 END SOURCE_STATUS
	,b.check_type
	,b.source_database
	,b.source_table
	,b.source_field
	,a.date_updated
	,a.ttl_records
from (
	select 'product_price_funnel_daily' source_table
		,event_date_pacific as date_updated
		,count(*) ttl_records
	from t2dl_das_product_funnel.product_price_funnel_daily 
	where event_date_pacific = current_date - 1
	group by 1,2
	union all
	select 'sku_loc_pricetype_day_vw' source_table
		,day_dt as date_updated
		,count(*) ttl_records
	from t2dl_das_ace_mfp.SKU_LOC_PRICETYPE_DAY_VW 
	where day_dt = current_date
	group by 1,2
	union all
	select 'rcpt_sku_ld_rvw' source_table
		,dm_recd_load_dt as date_updated
		,count(*) ttl_records
	from prd_ma_bado.rcpt_sku_ld_rvw
	where dm_recd_load_dt = current_date - 1
	group by 1,2
) a
	right join sources b 
		on b.source_table = a.source_table;

GRANT SELECT ON {environment_schema}.anniversary_data_check_vw TO PUBLIC;
*/



/******************************************************************************
** FILE: cyber_item_daily_vw.sql
** TABLE: T2DL_DAS_SCALED_EVENTS.cyber_item_daily_vw
**
** DESCRIPTION:
**      - Source of Cyber Item Daily Tableau data source - TY Only, and UDIG-captured items only
**
** TO DO:
**      - Productionalize in T2DL_DAS_SCALED_EVENTS 
**
** AUTHOR:
**      - Alli Moore (alli.moore@nordstrom.com)
******************************************************************************/


REPLACE VIEW {environment_schema}.cyber_item_daily_vw -- UPDATE TO REPLACE AFTER DEV
AS
LOCK ROW FOR ACCESS 
SELECT
    day_idnt
    , day_dt
    , event_day
    , bs.month_idnt --NEW
    , bs.month_label --NEW
    , ty_ly_ind
    , price_type
    , channel
    , banner
    , country
    , style_num
    , style_desc
    , vpn
    , supp_color
    , style_group_idnt
    , division
    , subdivision
    , department
    , "class"
    , subclass
    , supplier
    , brand
    , holiday_theme
    , bs.stocking_stuffer -- NEW 
    , quantrix_category 
    , bs.assortment_grouping -- NEW 
    , ccs_category      
    , ccs_subcategory   
    , nord_role_desc    
    , rack_role_desc    
    , promo_grouping    
    , merch_theme       
    , price_band_one                        AS price_band_1
    , price_band_two                        AS price_band_2
    , MAX(rp_ind                         )  AS rp_ind
    , MAX(npg_ind                        )  AS npg_ind
    , MAX(dropship_ind                   )  AS dropship_ind
    , MAX(price_match_ind                )  AS price_match_ind
    , MAX(udig                           )  AS udig_main
    , MAX(gift_ind                       )  AS gift_ind
    , MAX(bipoc_ind                      )  AS bipoc_ind
    , SUM(sales_units                    )  AS sales_units
    , SUM(sales_dollars                  )  AS sales_dollars
    , SUM(return_units                   )  AS return_units
    , SUM(return_dollars                 )  AS return_dollars
    , SUM(ntn                            )  AS ntn
    , SUM(demand_units                   )  AS demand_units
    , SUM(demand_dollars                 )  AS demand_dollars
    , SUM(bs.demand_cancel_units         )  AS demand_cancel_units -- NEW 
    , SUM(bs.demand_cancel_dollars       )  AS demand_cancel_dollars -- NEW 
    , SUM(shipped_units                  )  AS shipped_units
    , SUM(shipped_dollars                )  AS shipped_dollars
    , SUM(eoh_units                      )  AS eoh_units
    , SUM(eoh_dollars                    )  AS eoh_dollars
    , SUM(boh_units                      )  AS boh_units
    , SUM(boh_dollars                    )  AS boh_dollars
    , SUM(nonsellable_units              )  AS nonsellable_units
    , SUM(receipt_units                  )  AS receipt_units
    , SUM(receipt_dollars                )  AS receipt_dollars
    , SUM(store_fulfill_units            )  AS store_fulfill_units
    , SUM(store_fulfill_dollars          )  AS store_fulfill_dollars
    , SUM(dropship_units                 )  AS dropship_units
    , SUM(dropship_dollars               )  AS dropship_dollars
    , SUM(demand_dropship_units          )  AS demand_dropship_units
    , SUM(demand_dropship_dollars        )  AS demand_dropship_dollars
    , SUM(receipt_dropship_units         )  AS receipt_dropship_units
    , SUM(receipt_dropship_dollars       )  AS receipt_dropship_dollars
    , SUM(on_order_units                 )  AS on_order_units
    , SUM(on_order_retail_dollars        )  AS on_order_retail_dollars
    , SUM(on_order_4wk_units             )  AS on_order_4wk_units
    , SUM(on_order_4wk_retail_dollars    )  AS on_order_4wk_retail_dollars
    , SUM(product_views                  )  AS product_views
    , SUM(cart_adds                      )  AS cart_adds
    , SUM(order_units                    )  AS order_units
    , SUM(instock_views                  )  AS instock_views
    , SUM(bs.wishlist_adds               )  AS wishlist_adds -- NEW 
    , SUM(bs.cogs                        )  AS cogs -- NEW
FROM {environment_schema}.scaled_event_sku_clk_rms bs
WHERE date_event_type = 'Cyber'
    AND ty_ly_ind = 'TY'
    AND udig IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34;



GRANT SELECT ON {environment_schema}.cyber_item_daily_vw TO PUBLIC WITH GRANT OPTION;



/******************************************************************************
** FILE: cyber_supplier_class_daily_vw.sql
** TABLE: T2DL_DAS_SCALED_EVENTS.cyber_supplier_class_daily_vw
**
** DESCRIPTION:
**      - Source of Cyber Supplier Class Daily Tableau data source
**
** TO DO:
**      - Productionalize in T2DL_DAS_SCALED_EVENTS 
**
** AUTHOR:
**      - Alli Moore (alli.moore@nordstrom.com)
******************************************************************************/

CREATE VIEW {environment_schema}.cyber_supplier_class_daily_vw -- UPDATE TO REPLACE AFTER DEV
AS
LOCK ROW FOR ACCESS 
SELECT
    bs.day_idnt
    , bs.day_dt
    , bs.event_day
    , bs.day_dt_aligned
    , bs.month_idnt --NEW
    , bs.month_label --NEW
    , bs.ty_ly_ind
    , bs.banner
    , bs.price_type
    , bs.channel
    , bs.country
    , bs.division
    , bs.subdivision
    , bs.department
    , bs."class"
    , bs.subclass
    , bs.brand   
    , bs.supplier
    , bs.holiday_theme
    , bs.stocking_stuffer -- NEW 
    , bs.quantrix_category
    , bs.assortment_grouping -- NEW 
    --, bs.climate_cluster -- NEW 
    --, bs.NR_price_cluster -- NEW 
    , bs.ccs_category      
    , bs.ccs_subcategory   
    , bs.nord_role_desc    
    , bs.rack_role_desc    
    , bs.promo_grouping    
    , bs.merch_theme       
    , bs.rp_ind
    , bs.npg_ind
    , bs.dropship_ind
    , bs.gift_ind
    , bs.bipoc_ind
    , bs.price_band_1
    , bs.price_band_2
    , bs.price_match_ind
    , bs.udig_main
    , ZEROIFNULL(
    CASE 
        WHEN bs.ty_ly_ind = 'TY' THEN pl.ty_sales
        WHEN bs.ty_ly_ind = 'LY' THEN pl.ly_sales
        ELSE NULL
    END) AS sales_plan -- NEW 
    , ntn
    , sales_units
    , sales_dollars
    , return_units
    , return_dollars
    , demand_units
    , demand_dollars
    , demand_cancel_units -- NEW 
    , demand_cancel_dollars -- NEW 
    , receipt_units
    , receipt_dollars
    , shipped_units
    , shipped_dollars
    , store_fulfill_units
    , store_fulfill_dollars
    , dropship_units
    , dropship_dollars
    , demand_dropship_units
    , demand_dropship_dollars
    , receipt_dropship_units
    , receipt_dropship_dollars
    , on_order_units
    , on_order_retail_dollars
    , on_order_4wk_units
    , on_order_4wk_retail_dollars
    , product_views
    , cart_adds
    , order_units
    , instock_views
    , boh_units 
    , boh_dollars
    , eoh_units
    , eoh_dollars
    , nonsellable_units
    , wishlist_adds -- NEW 
    , cogs -- NEW  
FROM (
    SELECT
        bs.day_idnt
        , bs.day_dt
        , bs.event_day
        , bs.day_dt_aligned
        , bs.month_idnt --NEW
        , bs.month_label --NEW
        , bs.ty_ly_ind
        , bs.banner
        , bs.price_type
        , bs.channel
        , bs.country
        , bs.division
        , bs.subdivision
        , bs.department
        , bs."class"
        , bs.subclass
        , bs.brand   
        , bs.supplier
        , bs.holiday_theme
        , bs.stocking_stuffer -- NEW 
        , bs.quantrix_category
        , bs.assortment_grouping -- NEW 
        --, bs.climate_cluster -- NEW 
        --, bs.NR_price_cluster -- NEW 
        , bs.ccs_category      
        , bs.ccs_subcategory   
        , bs.nord_role_desc    
        , bs.rack_role_desc    
        , bs.promo_grouping    
        , bs.merch_theme       
        , bs.rp_ind
        , bs.npg_ind
        , bs.dropship_ind
        , bs.gift_ind
        , bs.bipoc_ind
        , bs.price_band_one as price_band_1
        , bs.price_band_two as price_band_2
        , bs.price_match_ind
        , bs.udig AS udig_main
        , SUM(bs.ntn                            ) AS ntn
        , SUM(bs.sales_units                    ) AS sales_units
        , SUM(bs.sales_dollars                  ) AS sales_dollars
        , SUM(bs.return_units                   ) AS return_units
        , SUM(bs.return_dollars                 ) AS return_dollars
        , SUM(bs.demand_units                   ) AS demand_units
        , SUM(bs.demand_dollars                 ) AS demand_dollars
        , SUM(bs.demand_cancel_units            ) AS demand_cancel_units -- NEW 
        , SUM(bs.demand_cancel_dollars          ) AS demand_cancel_dollars -- NEW 
        , SUM(bs.receipt_units                  ) AS receipt_units
        , SUM(bs.receipt_dollars                ) AS receipt_dollars  
        , SUM(bs.shipped_units                  ) AS shipped_units
        , SUM(bs.shipped_dollars                ) AS shipped_dollars
        , SUM(bs.store_fulfill_units            ) AS store_fulfill_units
        , SUM(bs.store_fulfill_dollars          ) AS store_fulfill_dollars
        , SUM(bs.dropship_units                 ) AS dropship_units
        , SUM(bs.dropship_dollars               ) AS dropship_dollars
        , SUM(bs.demand_dropship_units          ) AS demand_dropship_units
        , SUM(bs.demand_dropship_dollars        ) AS demand_dropship_dollars
        , SUM(bs.receipt_dropship_units         ) AS receipt_dropship_units
        , SUM(bs.receipt_dropship_dollars       ) AS receipt_dropship_dollars
        , SUM(bs.on_order_units                 ) AS on_order_units
        , SUM(bs.on_order_retail_dollars        ) AS on_order_retail_dollars
        , SUM(bs.on_order_4wk_units             ) AS on_order_4wk_units
        , SUM(bs.on_order_4wk_retail_dollars    ) AS on_order_4wk_retail_dollars
        , SUM(bs.product_views                  ) AS product_views
        , SUM(bs.cart_adds                      ) AS cart_adds
        , SUM(bs.order_units                    ) AS order_units
        , SUM(bs.instock_views                  ) AS instock_views
        , SUM(bs.boh_units                      ) AS boh_units 
        , SUM(bs.boh_dollars                    ) AS boh_dollars
        , SUM(bs.eoh_units                      ) AS eoh_units
        , SUM(bs.eoh_dollars                    ) AS eoh_dollars
        , SUM(bs.nonsellable_units              ) AS nonsellable_units
        , SUM(bs.wishlist_adds                  ) AS wishlist_adds -- NEW 
        , SUM(bs.cogs                           ) AS cogs -- NEW  
    FROM 
        (SELECT *
         FROM {environment_schema}.scaled_event_sku_clk_rms bs
         WHERE date_event_type = 'Cyber'
            AND day_dt_aligned <= (select max(day_dt) from {environment_schema}.scaled_event_sku_clk_rms)) bs
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37 
) bs
-- sales plan
LEFT JOIN --T2DL_DAS_SCALED_EVENTS.CYBER_SALES_PLAN_22_VW pl  
(
    SELECT 
        BANNER 
        , DEPT_LABEL
        , STRTOK(dept_label, ',',1) AS DEPT_IDNT 
        , CATEGORY
        , TY_SALES
        , LY_SALES
        ,current_timestamp as process_tmstp
    FROM t3dl_ace_mch.CYBER_SALES_PLAN_22_STAGING
)pl
    ON bs.banner = pl.banner
        AND bs.department = pl.dept_label
        AND bs.quantrix_category = pl.category;

GRANT SELECT ON {environment_schema}.cyber_supplier_class_daily_vw TO PUBLIC WITH GRANT OPTION;




/************** END - VIEW DDL CREATION **************/

