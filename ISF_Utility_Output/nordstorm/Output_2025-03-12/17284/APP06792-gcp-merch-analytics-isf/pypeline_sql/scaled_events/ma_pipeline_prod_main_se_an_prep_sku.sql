/******************************************************************************
Name: Anniversary SKU Lookup Table
APPID-Name: APP08204 - Scaled Events Reporting
Purpose: Populate the backend lookup table that is used org-wide as the source of truth for TY/LY Anniversary SKUs
Variable(s):    {environment_schema} - T2DL_DAS_SCALED_EVENTS
DAG: merch_se_an_prep_an_sku
TABLE NAME: T2DL_DAS_SCALED_EVENTS.ANNIVERSARY_SKU (T3: T3DL_ACE_PRA.ANNIVERSARY_SKU)
Author(s): Alli Moore
Date Last Updated: 07-27-2023
******************************************************************************/

DELETE FROM {environment_schema}.ANNIVERSARY_SKU ALL;

INSERT INTO {environment_schema}.ANNIVERSARY_SKU
SELECT DISTINCT
	rms_sku_num AS sku_idnt
	, 'US' AS country
	, EXTRACT(YEAR FROM enticement_start_tmstp) AS event_year
	, CASE WHEN EXTRACT(YEAR FROM enticement_start_tmstp) = EXTRACT(YEAR FROM CURRENT_DATE) THEN 'TY'
		ELSE 'LY' END AS ty_ly_ind
	, CURRENT_TIMESTAMP AS process_tmstp
FROM PRD_NAP_USR_VWS.PRODUCT_PROMOTION_TIMELINE_DIM pptd
WHERE channel_country = 'US'
    AND enticement_tags LIKE '%ANNIVERSARY_SALE%'
    AND EXTRACT(YEAR FROM enticement_start_tmstp) <= EXTRACT(YEAR FROM CURRENT_DATE)
    AND EXTRACT(YEAR FROM enticement_start_tmstp) >= EXTRACT(YEAR FROM CURRENT_DATE) - 1
    -- (07.27.2023) EXCLUDE NIKE SKUS THAT WERE ACCIDENTALLY PUT ON ANNIV ENTICEMENT - REMOVE AFTER ANNIV 2024
    AND rms_sku_num NOT IN (SELECT DISTINCT rms_sku_num FROM PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW psdv
		WHERE (channel_country = 'US' AND style_group_num = '6133999' AND color_num = '101' AND supp_part_num = 'DA6364')
			OR (channel_country = 'US' AND style_group_num = '6088468' AND color_num = '100' AND supp_part_num = 'BQ6806'))
;

COLLECT STATISTICS
    PRIMARY INDEX (SKU_IDNT, EVENT_YEAR)
    ,COLUMN (SKU_IDNT, EVENT_YEAR)
    ,COLUMN (SKU_IDNT)
    ,COLUMN (EVENT_YEAR)
ON {environment_schema}.ANNIVERSARY_SKU;



-- Data Quality LOG
    -- Keeps Daily LOG of Event SKU Cts to track changes
INSERT INTO {environment_schema}.ANNIV_SKU_HEALTH_LOG
SELECT
	CURRENT_TIMESTAMP AS process_tmstp
	, COUNT(CASE WHEN ty_ly_ind = 'TY' THEN sku_idnt ELSE NULL END) AS ty_anniv_sku_ct
	, COUNT(CASE WHEN ty_ly_ind = 'LY' THEN sku_idnt ELSE NULL END) AS ly_anniv_sku_ct
FROM {environment_schema}.ANNIVERSARY_SKU
GROUP BY 1
;
