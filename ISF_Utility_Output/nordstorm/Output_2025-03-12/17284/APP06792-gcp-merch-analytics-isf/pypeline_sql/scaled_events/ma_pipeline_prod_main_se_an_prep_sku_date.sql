/******************************************************************************
Name: Scaled Events Anniversary SKU/CHNL/DATE Table
APPID-Name: APP08204 - Scaled Events Reporting
Purpose: Populate the Anniversary SKU/CHNL/DATE lookup table
Variable(s):    {environment_schema} - T2DL_DAS_SCALED_EVENTS
DAG: merch_se_an_prep_sku_date
TABLE NAME: T2DL_DAS_SCALED_EVENTS.ANNIVERSARY_SKU_CHNL_DATE (T3: T3DL_ACE_PRA.ANNIVERSARY_SKU_CHNL_DATE)
Author(s): Alli Moore, Manuela Hurtado
Date Last Updated: 05-13-2024
******************************************************************************/


	CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'ANNIVERSARY_SKU_CHNL_DATE', OUT_RETURN_MSG);
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
 PARTITION BY RANGE_N(day_idnt  BETWEEN 2023001 AND 2024364 EACH 1);

 GRANT SELECT ON {environment_schema}.ANNIVERSARY_SKU_CHNL_DATE  TO PUBLIC;



CREATE MULTISET VOLATILE TABLE anniv_sku
AS (
	SELECT
		pptd.rms_sku_num AS sku_idnt
		, pptd.channel_country
		, pptd.store_num
		, pptd.selling_channel
		, CASE WHEN EXTRACT(YEAR FROM enticement_start_tmstp) = EXTRACT(YEAR FROM CURRENT_DATE) THEN 'TY'
			ELSE 'LY'
            END AS ty_ly_ind
		, AVG(regular_price_amt) AS regular_price_amt
        , AVG(selling_retail_price_amt) AS current_price_amt
	FROM PRD_NAP_USR_VWS.PRODUCT_PROMOTION_TIMELINE_DIM pptd
	LEFT JOIN PRD_NAP_USR_VWS.PRODUCT_PRICE_TIMELINE_DIM price_dim
		ON PRICE_DIM.RMS_SKU_NUM = pptd.RMS_SKU_NUM
		AND PRICE_DIM.CHANNEL_COUNTRY = pptd.CHANNEL_COUNTRY
		AND PRICE_DIM.SELLING_CHANNEL = pptd.SELLING_CHANNEL
		AND PRICE_DIM.CHANNEL_BRAND = pptd.CHANNEL_BRAND
		AND PRICE_DIM.SELLING_RETAIL_RECORD_ID = pptd.PROMO_ID
		AND PERIOD(PRICE_DIM.EFF_BEGIN_TMSTP, PRICE_DIM.EFF_END_TMSTP) OVERLAPS PERIOD(pptd.enticement_start_tmstp, pptd.enticement_end_tmstp)
	WHERE pptd.channel_country = 'US'
	    AND enticement_tags LIKE '%ANNIVERSARY_SALE%'
	    AND EXTRACT(YEAR FROM enticement_start_tmstp) <= EXTRACT(YEAR FROM CURRENT_DATE)
	    AND EXTRACT(YEAR FROM enticement_start_tmstp) >= EXTRACT(YEAR FROM CURRENT_DATE) - 1
	    -- (07.27.2023) EXCLUDE NIKE SKUS THAT WERE ACCIDENTALLY PUT ON ANNIV ENTICEMENT - REMOVE AFTER ANNIV 2023
    	AND pptd.rms_sku_num NOT IN (SELECT DISTINCT rms_sku_num FROM PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW psdv
			WHERE (channel_country = 'US' AND style_group_num = '6133999' AND color_num = '101' AND supp_part_num = 'DA6364')
				OR (channel_country = 'US' AND style_group_num = '6088468' AND color_num = '100' AND supp_part_num = 'BQ6806'))
 	GROUP BY 1,2,3,4,5
) WITH DATA PRIMARY INDEX(sku_idnt, store_num) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS COLUMN(sku_idnt, store_num) ON anniv_sku;


-- Assign all Online Beauty as FLS also
CREATE MULTISET VOLATILE TABLE anniv_sku_final
AS (
	SELECT
		sku_idnt
		, channel_country
		, store_num
		, selling_channel
		, ty_ly_ind
		, MAX(regular_price_amt) AS regular_price_amt
		, MAX(current_price_amt) AS current_price_amt
	FROM (
		SELECT
			*
		FROM anniv_sku
		UNION ALL
		SELECT
			sku_idnt
			, a.channel_country
			, '1' AS store_num
			, 'STORE' AS selling_channel
			, ty_ly_ind
			, regular_price_amt
			, current_price_amt
		FROM anniv_sku a
		INNER JOIN PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW psd
			ON psd.rms_sku_num = a.sku_idnt
			AND psd.channel_country = a.channel_country
		WHERE div_num = '340'
			AND store_num = '808'
	) a
	GROUP BY 1,2,3,4,5
) WITH DATA PRIMARY INDEX(sku_idnt, store_num) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS COLUMN(sku_idnt, store_num) ON anniv_sku_final;

CREATE MULTISET VOLATILE TABLE price_store_lkp
AS (
	SELECT DISTINCT
		price_store_num
		, channel_country
		, selling_channel
		, store_type_code
		, store_type_desc
		, channel_num
		, channel_desc
		, channel_brand
	FROM PRD_NAP_USR_VWS.PRICE_STORE_DIM_VW
	WHERE store_close_date IS NULL
		AND channel_country = 'US'
		AND channel_brand = 'NORDSTROM'
		AND channel_num IS NOT NULL
) WITH DATA PRIMARY INDEX(store_type_code, price_store_num) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS COLUMN(store_type_code, price_store_num)
	,COLUMN (selling_channel, channel_country)
	ON price_store_lkp;

CREATE MULTISET VOLATILE TABLE anniv_sku_chnl
AS (
	SELECT DISTINCT
		sku_idnt
		, ty_ly_ind
		, store_type_code
		, a.channel_country
		, channel_num
		, a.selling_channel
		, regular_price_amt
		, current_price_amt
	FROM anniv_sku_final a
	INNER JOIN price_store_lkp ps
		ON ps.price_store_num = a.store_num
			AND ps.channel_country = a.channel_country
			AND ps.selling_channel = a.selling_channel
) WITH DATA PRIMARY INDEX(sku_idnt, store_type_code) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS COLUMN(sku_idnt, store_type_code)
	,COLUMN (selling_channel, channel_country)
	ON anniv_sku_chnl;


CREATE MULTISET VOLATILE TABLE anniv_dates
AS (
    SELECT DISTINCT
        day_idnt
        , day_dt
        , ty_ly_lly AS ty_ly_ind
        , event_country AS country
    FROM T2DL_DAS_SCALED_EVENTS.SCALED_EVENT_DATES sed
    WHERE anniv_ind = '1'
    AND ty_ly_lly IN ('TY', 'LY')
    AND event_type <> 'Non-Event'
) WITH DATA PRIMARY INDEX(day_dt) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS COLUMN(day_dt) ON anniv_dates;


DELETE FROM {environment_schema}.ANNIVERSARY_SKU_CHNL_DATE ALL;

INSERT INTO {environment_schema}.ANNIVERSARY_SKU_CHNL_DATE
SELECT
    sku.sku_idnt
    , sku.channel_country
	, sku.selling_channel
	, sku.store_type_code
    , dates.day_idnt
    , dates.day_dt
    , dates.ty_ly_ind
    , '1' AS anniv_ind
    , AVG(sku.regular_price_amt) AS reg_price_amt
    , AVG(sku.current_price_amt) AS spcl_price_amt
FROM anniv_sku_chnl sku
INNER JOIN anniv_dates dates
    ON dates.country = sku.channel_country
    AND dates.ty_ly_ind = sku.ty_ly_ind
WHERE store_type_code IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8;

COLLECT STATISTICS
    COLUMN(sku_idnt)
    , COLUMN(partition)
ON {environment_schema}.ANNIVERSARY_SKU_CHNL_DATE ;
