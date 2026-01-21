/******************************************************************************
Name: Scaled Events - Anniversary View Definitions
APPID-Name: APP08204 - Scaled Events Reporting
Purpose: Store all view definitions for Anniversary Reporting (both prep and tableau view code)
Variable(s):    {environment_schema} - T2DL_DAS_SCALED_EVENTS
DAG: merch_se_all_views
TABLE NAME: Multiple Views
Author(s): Alli Moore, Manuela Hurtado Gonzalez
Date Last Updated: 06-10-2024
******************************************************************************/

/******************************************************** START - PREP VIEWS ********************************************************/

/******************************************************************************
** VIEW: T2DL_DAS_SCALED_EVENTS.SCALED_EVENT_DMA_LKP_VW
**
** DESCRIPTION: Creates a dma lookup view for Daily Scaled Event Reporting (Anniversary and Cyber periods) of top 25 markets by sales in 2021
** UPDATED: 2022-10-03
******************************************************************************/

/*
-- 2022.06.08 - DDL build for scaled_event_dma_lkp_vw
    -- 2022.10.03 - Updated for Holiday purposes but also for AN

REPLACE VIEW {environment_schema}.scaled_event_dma_lkp_vw
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
*/
/******************************************************************************
** VIEW: T2DL_DAS_SCALED_EVENTS.scaled_event_sku_last_receipt_vw
**
** DESCRIPTION: Creates a location lookup view for Daily Scaled Event Reporting (Anniversary and Cyber periods)
** UPDATED: 2023-05-19
******************************************************************************/
/*
-- 2023.05.19 - last updated
REPLACE VIEW {environment_schema}.scaled_event_sku_last_receipt_vw
AS
LOCK ROW FOR ACCESS
SELECT
    rcpt.rms_sku_num AS sku_idnt
    --, MAX(tdl.day_date) AS last_receipt_date
    , MAX(INVENTORY_IN_LAST_DATE) AS last_receipt_date
FROM PRD_NAP_USR_VWS.MERCH_RECEIPT_DATE_SKU_STORE_FACT rcpt
WHERE EXTRACT(YEAR FROM INVENTORY_IN_LAST_DATE) >= EXTRACT(YEAR FROM CURRENT_DATE())-1 --LY
GROUP BY 1;
*/
/******************************************************************************
** VIEW: T2DL_DAS_SCALED_EVENTS.SCALED_EVENT_LOCS_VW
**
** DESCRIPTION: Creates a US location lookup view for Daily Scaled Event Reporting (Anniversary and Cyber periods)
** UPDATED: 2023-10-02
******************************************************************************/
/*
-- 2023.10.02 - last updated
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
    	WHEN channel_num IN ('110', '120', '130', '140', '210', '220', '250', '260', '310', '920') THEN 1
    	ELSE 0
    END AS cyber_loc_ind -- FP/OP stores, warehouse, RS, and DC
    , CASE WHEN channel_num IN ('110', '120', '130', '140', '310', '920') THEN 1
    ELSE 0
    END AS anniv_loc_ind -- FP stores, RS, and DC (removed selling_store_ind to incorporate RS + DC)
FROM PRD_NAP_USR_VWS.STORE_DIM
WHERE channel_num IN ('110', '120', '130', '140', '210', '220', '250', '260', '310', '920')
    AND store_close_date IS NULL
    AND store_country_code = 'US';
*/
/******************************************************************************
** TABLE: {environment_schema}.anniversary_soldout_vw
**
** DESCRIPTION: Source of sold out indicator for Anniversary Item View
** UPDATED: 2023-06-14
******************************************************************************/
/*
-- Last Updated: 2024.06.10
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
		, al.cum_sales_units
		, al.cum_sales_units_ld
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
					, CASE WHEN ds_ld.rms_sku_id IS NOT NULL THEN 'Y' ELSE 'N' END AS dropship_ind_ld
					, SUM(ap.sales_units) AS sales_units
					, SUM(ap.return_units) AS return_units
					, SUM(ap.eoh_units) AS eoh_units
					, SUM(ap.boh_units) AS boh_units
					, SUM(ap.nonsellable_units) AS nonsellable_units
					, SUM(ap.receipt_units) AS receipt_units
			FROM {environment_schema}.scaled_event_sku_clk_rms ap
			LEFT JOIN PRD_NAP_USR_VWS.INVENTORY_STOCK_QUANTITY_BY_DAY_PHYSICAL_FACT ds_ld
					ON ap.sku_idnt = ds_ld.rms_sku_id
					AND ap.day_dt = ds_ld.snapshot_date + INTERVAL '1' DAY
					AND ap.country = 'US'
					AND ds_ld.location_type IN ('DS')
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
					, ps.store_country_code AS country
					, dt.day_num+1 AS day_idnt
					, SUM(NONSELLABLE_UNITS) AS nonsellable_units_ld
				FROM T2DL_DAS_ACE_MFP.SKU_LOC_PRICETYPE_DAY_VW nsl
				LEFT JOIN PRD_NAP_USR_VWS.DAY_CAL dt
					ON dt.day_date = nsl.day_dt
				LEFT JOIN PRD_NAP_USR_VWS.STORE_DIM ps
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
			WHERE channel_country = 'US'
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
							WHERE yr_idnt = (SELECT MAX(yr_idnt) FROM {environment_schema}.scaled_event_dates where anniv_ind = 1)
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

*/

/******************************************************************************
** TABLE: {environment_schema}.anniversary_data_check_vw
**
** DESCRIPTION: Data check view for AN Data SLA checks
** UPDATED: 2022-07-05
******************************************************************************/
/*
REPLACE VIEW {environment_schema}.anniversary_data_check_vw
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
		UNION ALL
        SELECT
			'sku_item_added' AS source_table
			, 'T2DL_DAS_SCALED_EVENTS' AS source_database
			, 'event_date_pacific' AS source_field
			, 'primary' AS check_type
			, CURRENT_DATE AS date_check
		FROM (SELECT 1 AS "dummy") "dummy"
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
    UNION ALL
	SELECT
		'sku_item_added' source_table
		, CAST(MAX(dw_sys_load_tmstp) AS DATE FORMAT 'YYYY-MM-DD') as date_updated
		, COUNT(*) ttl_records
	FROM T2DL_DAS_SCALED_EVENTS.SKU_ITEM_ADDED
	WHERE event_date_pacific = CURRENT_DATE - 1
	GROUP BY 1
) a
	right join sources b
		on b.source_table = a.source_table;

--GRANT SELECT ON {environment_schema}.anniversary_data_check_vw TO PUBLIC;

*/



/******************************************************** END - PREP VIEWS ********************************************************/











/******************************************************** START - TABLEAU VIEWS ********************************************************/


/******************************************************************************
** VIEW: {environment_schema}.anniversary_item_vw
**
** DESCRIPTION:Source of Anniversary Item Tableau data source
** UPDATED: 2024-06-10
******************************************************************************/

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
    , ap.price_type
    , ap.anniv_ind
    , ap.rp_ind
    , ap.ty_ly_ind
    , ap.country
    , ap.channel
    , ap.dropship_ind
    , ap.event_phase
    , ap.event_day
    , ap.dma_short
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
    , ap.supplier
    , ap.brand
    , ap.npg_ind
    , ap.quantrix_category
    , ap.ccs_category
    , ap.ccs_subcategory
    , ap.nord_role_desc
    , ap.merch_theme
    , ap.anniversary_theme
    , ap.assortment_grouping
    , ap.udig
   -- , ap.parent_group -- removed 2023
    , ap.price_band_one
    , ap.price_band_two
    , ap.bipoc_ind -- NEW 2023
    , ap.anchor_brands -- NEW 2024
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
    --, SUM(ap.on_order_4wk_units) AS on_order_4wk_units -- removed 2023
    --, SUM(ap.on_order_4wk_retail_dollars) AS on_order_4wk_retail_dollars -- removed 2023
    --, SUM(ap.backorder_units) AS backorder_units -- removed 2023
    --, SUM(ap.backorder_dollars) AS backorder_dollars -- removed 2023
    , SUM(ap.product_views) AS product_views
    , SUM(ap.cart_adds)  AS cart_adds
    , SUM(ap.order_units) AS com_order_units
    , SUM(ap.instock_views) AS instock_views
	, SUM(ap.scored_views) as scored_views -- NEW
    , SUM(ap.wishlist_adds) AS wishlist_adds
    , SUM(ap.demand) AS com_demand
    , SUM(ap.units) AS units
    , SUM(ap.units * ap.retail_original) AS retail_original_dollars
    , SUM(ap.units * ap.retail_special) AS retail_special_dollars
    , MAX(ap.last_receipt_date) AS last_receipt_date
    ----- NEW Anniv 2023 -----
    , SUM(ap.sales_cost) AS sales_cost
   	, SUM(ap.return_cost) AS return_cost
	, SUM(ap.shipped_cost) AS shipped_cost
	, SUM(ap.store_fulfill_cost) AS store_fulfill_cost
	, SUM(ap.dropship_cost) AS dropship_cost
	, SUM(ap.eoh_cost) AS eoh_cost
	, SUM(ap.boh_cost) AS boh_cost
	, SUM(ap.receipt_cost) AS receipt_cost
	, SUM(ap.receipt_dropship_cost) AS receipt_dropship_cost
	, SUM(ap.sales_pm) AS sales_pm
    , SUM(ap.on_order_cost_dollars) AS on_order_cost_dollars
	, MAX(process_tmstp) AS last_updated_tmstp
	-------------------------------
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
    WHERE channel_country = 'US'
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
LEFT JOIN locs l --item location count
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
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38;

--GRANT SELECT ON {environment_schema}.anniversary_item_vw TO PUBLIC WITH GRANT OPTION;





















/******************************************************************************
** TABLE: {environment_schema}.anniversary_location_vw
**
** DESCRIPTION: Source of Anniversary Location Tableau data source
** UPDATED: 2024-06-10
******************************************************************************/

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
    , climate_cluster -- NEW 2023
    , dropship_ind
    , bipoc_ind -- NEW 2023
    , anchor_brands -- NEW 2024
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
    , quantrix_category
    , ccs_category
    , ccs_subcategory
    , nord_role_desc
    , merch_theme
    , anniversary_theme
    ,assortment_grouping
    , udig
--    , parent_group
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
--    , SUM(on_order_4wk_units) AS on_order_4wk_units
--    , SUM(on_order_4wk_retail_dollars) AS on_order_4wk_retail_dollars
--    , SUM(backorder_units) AS backorder_units
--    , SUM(backorder_dollars) AS backorder_dollars
    ----- NEW Anniv 2023 -----
    , SUM(ap.sales_cost) AS sales_cost
   	, SUM(ap.return_cost) AS return_cost
	, SUM(ap.shipped_cost) AS shipped_cost
	, SUM(ap.store_fulfill_cost) AS store_fulfill_cost
	, SUM(ap.dropship_cost) AS dropship_cost
	, SUM(ap.eoh_cost) AS eoh_cost
	, SUM(ap.boh_cost) AS boh_cost
	, SUM(ap.receipt_cost) AS receipt_cost
	, SUM(ap.receipt_dropship_cost) AS receipt_dropship_cost
	, SUM(ap.on_order_cost_dollars) AS on_order_cost_dollars
	, SUM(ap.sales_pm) AS sales_pm
	, MAX(process_tmstp) AS last_updated_tmstp
    -----------------------------
FROM {environment_schema}.scaled_event_sku_clk_rms ap
 WHERE event_phase <> 'Post'
	AND date_event_type = 'Anniversary'
	AND day_dt_aligned <= (select max(day_dt) from {environment_schema}.scaled_event_sku_clk_rms)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34;

--GRANT SELECT ON {environment_schema}.anniversary_location_vw TO PUBLIC;















/******************************************************************************
** TABLE: {environment_schema}.anniversary_supplier_class_vw
**
** DESCRIPTION: Source of Anniversary Supplier Class Tableau data source
** UPDATED: 2024-06-10 -- Split TY/LY in attempt to avoid CPU Timeout
******************************************************************************/

----------------------------------- MODIFIED TABLEAU CUSTOM SQL AND INITIAL SQL AS OF 8/18/23 -------------------------------------
	-- Implemented as last resort due to CPU Limit issues
/*
-- Tableau Initial SQL
CREATE MULTISET VOLATILE TABLE date_lkp AS (
	SELECT DISTINCT
		day_dt AS day_dt_lkp
	FROM T2DL_DAS_SCALED_EVENTS.SCALED_EVENT_DATES
	WHERE day_dt_aligned <= (SELECT MAX(day_dt) FROM T2DL_DAS_SCALED_EVENTS.scaled_event_sku_clk_rms WHERE date_event_type = 'Anniversary' AND ty_ly_ind = 'TY')
	AND anniv_ind = 1
) WITH DATA PRIMARY INDEX(day_dt_lkp) ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE cat_plan AS (
	SELECT
		CAST(channel AS VARCHAR(10)) AS cat_channel
		, CAST(dept_idnt AS VARCHAR(10)) AS cat_dept_idnt
		, category
    , PLAN_NET_SALES_R AS cat_plan_net_sales_r
  	, PLAN_NET_SALES_C AS cat_plan_net_sales_c
  	, PLAN_NET_SALES_U AS cat_plan_net_sales_u
  	, PLAN_ANNIV_CC_COUNT AS cat_plan_anniv_cc_count
  	, "PLAN_PRODUCT_MARGIN_$" AS cat_plan_pm_dollars
	FROM T2DL_DAS_SCALED_EVENTS.ANNIVERSARY_COST_CATEGORY_PLANS
) WITH DATA PRIMARY INDEX(cat_channel, cat_dept_idnt, category) ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE ty_base AS (
	SELECT
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
	    , ap.channel_idnt
	    , ap.country
	    , ap.dropship_ind
	    , ap.division
	    , ap.subdivision
	    , ap.department
	    , TRIM(STRTOK(ap.department,', ', 1)) AS dept_idnt
	    , ap."class"
	    , ap.subclass
	    , ap.supplier
	    , ap.brand
      , ap.general_merch_manager_executive_vice_president
      , ap.div_merch_manager_senior_vice_president
      , ap.div_merch_manager_vice_president
      , ap.merch_director
      , ap.buyer
      , ap.merch_planning_executive_vice_president
      , ap.merch_planning_senior_vice_president
      , ap.merch_planning_vice_president
      , ap.merch_planning_director_manager
      , ap.assortment_planner
	    , ap.npg_ind
	    , ap.bipoc_ind
      , ap.anchor_brands
	    , ap.quantrix_category
	    , ap.ccs_category
	    , ap.ccs_subcategory
	    , ap.nord_role_desc
	    , ap.merch_theme
      , ap.assortment_grouping
	    , ap.anniversary_theme
	    , ap.udig
	    , ap.price_band_one
	    , ap.price_band_two
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
	    , SUM(ap.product_views) AS product_views
	    , SUM(ap.cart_adds)  AS cart_adds
	    , SUM(ap.order_units) AS com_order_units
	    , SUM(ap.instock_views) AS instock_views
		, SUM(ap.scored_views) AS scored_views
	    , sum(ap.wishlist_adds) AS wishlist_adds
	    , SUM(ap.demand) AS com_demand
	    , SUM(ap.units) AS units
	    , SUM(ap.units * ap.retail_original) AS retail_original_dollars
	    , SUM(ap.units * ap.retail_special) AS retail_special_dollars
	    , SUM(ap.sales_cost) AS sales_cost
	   	, SUM(ap.return_cost) AS return_cost
		, SUM(ap.shipped_cost) AS shipped_cost
		, SUM(ap.store_fulfill_cost) AS store_fulfill_cost
		, SUM(ap.dropship_cost) AS dropship_cost
		, SUM(ap.eoh_cost) AS eoh_cost
		, SUM(ap.boh_cost) AS boh_cost
		, SUM(ap.receipt_cost) AS receipt_cost
		, SUM(ap.receipt_dropship_cost) AS receipt_dropship_cost
		, SUM(ap.on_order_cost_dollars) AS on_order_cost_dollars
		, SUM(ap.sales_pm) AS sales_pm
		, MAX(process_tmstp) AS last_updated_tmstp
	FROM T2DL_DAS_SCALED_EVENTS.scaled_event_sku_clk_rms ap
	INNER JOIN date_lkp d
		ON d.day_dt_lkp = ap.day_dt
	WHERE date_event_type = 'Anniversary'
		AND ap.ty_ly_ind = 'TY'
	GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35
) WITH DATA PRIMARY INDEX(day_dt, channel_idnt, dept_idnt, quantrix_category, subclass) ON COMMIT PRESERVE ROWS;


-- Tableau Custom SQL
SELECT
	a.day_idnt
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
    , a.general_merch_manager_executive_vice_president
    , a.div_merch_manager_senior_vice_president
    , a.div_merch_manager_vice_president
    , a.merch_director
    , a.buyer
    , a.merch_planning_executive_vice_president
    , a.merch_planning_senior_vice_president
    , a.merch_planning_vice_president
    , a.merch_planning_director_manager
    , a.assortment_planner
    , a.npg_ind
    , a.bipoc_ind
    , a.anchor_brands
    , a.quantrix_category
    , a.ccs_category
    , a.ccs_subcategory
    , a.nord_role_desc
    , a.merch_theme
    , a.assortment_grouping
    , a.anniversary_theme
    , a.udig
    , a.price_band_one
    , a.price_band_two
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
    , a.product_views
    , a.cart_adds
    , a.com_order_units
    , a.instock_views
	, a.scored_views
    , a.wishlist_adds
    , a.com_demand
    , a.units
    , a.retail_original_dollars
    , a.retail_special_dollars
    , sales_cost
   	, return_cost
	, shipped_cost
	, store_fulfill_cost
	, dropship_cost
	, eoh_cost
	, boh_cost
	, receipt_cost
	, receipt_dropship_cost
	, on_order_cost_dollars
	, sales_pm
	, last_updated_tmstp
  , b.PLAN_NET_SALES_R AS cat_plan_net_sales_r
  , b.PLAN_NET_SALES_C AS cat_plan_net_sales_c
  , b.PLAN_NET_SALES_U AS cat_plan_net_sales_u
  , b.PLAN_ANNIV_CC_COUNT AS cat_plan_anniv_cc_count
  , b."PLAN_PRODUCT_MARGIN_$" AS cat_plan_pm_dollars
FROM ty_base a
LEFT JOIN cat_plan b
	ON a.channel_idnt = b.cat_channel
	AND a.dept_idnt = b.cat_dept_idnt
  AND a.ty_ly_ind = 'TY'
	AND a.event_phase <> 'Post'

UNION ALL
SELECT
	a.day_idnt
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
    , a.general_merch_manager_executive_vice_president
    , a.div_merch_manager_senior_vice_president
    , a.div_merch_manager_vice_president
    , a.merch_director
    , a.buyer
    , a.merch_planning_executive_vice_president
    , a.merch_planning_senior_vice_president
    , a.merch_planning_vice_president
    , a.merch_planning_director_manager
    , a.assortment_planner
    , a.npg_ind
    , a.bipoc_ind
    , a.anchor_brands
    , a.quantrix_category
    , a.ccs_category
    , a.ccs_subcategory
    , a.nord_role_desc
    , a.merch_theme
    , a.assortment_grouping
    , a.anniversary_theme
    , a.udig
    , a.price_band_one
    , a.price_band_two
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
    , a.product_views
    , a.cart_adds
    , a.com_order_units
    , a.instock_views
	, a.scored_views
    , a.wishlist_adds
    , a.com_demand
    , a.units
    , a.retail_original_dollars
    , a.retail_special_dollars
    , sales_cost
   	, return_cost
	, shipped_cost
	, store_fulfill_cost
	, dropship_cost
	, eoh_cost
	, boh_cost
	, receipt_cost
	, receipt_dropship_cost
	, on_order_cost_dollars
	, sales_pm
	, last_updated_tmstp
  , b.PLAN_NET_SALES_R AS cat_plan_net_sales_r
  , b.PLAN_NET_SALES_C AS cat_plan_net_sales_c
  , b.PLAN_NET_SALES_U AS cat_plan_net_sales_u
  , b.PLAN_ANNIV_CC_COUNT AS cat_plan_anniv_cc_count
  , b."PLAN_PRODUCT_MARGIN_$" AS cat_plan_pm_dollars
FROM
	(
	SELECT
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
	    , ap.channel_idnt
	    , ap.country
	    , ap.dropship_ind
	    , ap.division
	    , ap.subdivision
	    , ap.department
	    , ap."class"
	    , ap.subclass
	    , ap.supplier
	    , ap.brand
      , ap.general_merch_manager_executive_vice_president
      , ap.div_merch_manager_senior_vice_president
      , ap.div_merch_manager_vice_president
      , ap.merch_director
      , ap.buyer
      , ap.merch_planning_executive_vice_president
      , ap.merch_planning_senior_vice_president
      , ap.merch_planning_vice_president
      , ap.merch_planning_director_manager
      , ap.assortment_planner
	    , ap.npg_ind
	    , ap.bipoc_ind
      , ap.anchor_brands
	    , ap.quantrix_category
	    , ap.ccs_category
	    , ap.ccs_subcategory
	    , ap.nord_role_desc
	    , ap.merch_theme
      , ap.assortment_grouping
	    , ap.anniversary_theme
	    , ap.udig
	    , ap.price_band_one
	    , ap.price_band_two
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
	    , SUM(ap.product_views) AS product_views
	    , SUM(ap.cart_adds)  AS cart_adds
	    , SUM(ap.order_units) AS com_order_units
	    , SUM(ap.instock_views) AS instock_views
		, SUM(ap.scored_views) AS scored_views
	    , sum(ap.wishlist_adds) AS wishlist_adds
	    , SUM(ap.demand) AS com_demand
	    , SUM(ap.units) AS units
	    , SUM(ap.units * ap.retail_original) AS retail_original_dollars
	    , SUM(ap.units * ap.retail_special) AS retail_special_dollars
	    , SUM(ap.sales_cost) AS sales_cost
	   	, SUM(ap.return_cost) AS return_cost
		, SUM(ap.shipped_cost) AS shipped_cost
		, SUM(ap.store_fulfill_cost) AS store_fulfill_cost
		, SUM(ap.dropship_cost) AS dropship_cost
		, SUM(ap.eoh_cost) AS eoh_cost
		, SUM(ap.boh_cost) AS boh_cost
		, SUM(ap.receipt_cost) AS receipt_cost
		, SUM(ap.receipt_dropship_cost) AS receipt_dropship_cost
		, SUM(ap.on_order_cost_dollars) AS on_order_cost_dollars
		, SUM(ap.sales_pm) AS sales_pm
		, MAX(process_tmstp) AS last_updated_tmstp
		, CAST(SUM(0) AS DECIMAL(12,2)) AS cat_plan_net_sales_r
		, CAST(SUM(0) AS DECIMAL(12,2)) AS cat_plan_net_sales_c
		, CAST(SUM(0) AS DECIMAL(12,2)) AS cat_plan_net_sales_u
		, CAST(SUM(0) AS DECIMAL(12,2)) AS cat_plan_anniv_cc_count
		, CAST(SUM(0) AS DECIMAL(12,2)) AS cat_plan_pm_dollars
	FROM T2DL_DAS_SCALED_EVENTS.scaled_event_sku_clk_rms ap
	INNER JOIN date_lkp d
		ON d.day_dt_lkp = ap.day_dt
	WHERE date_event_type = 'Anniversary'
		AND ap.ty_ly_ind = 'LY'
	GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34
) a
*/

-- Actual View Definition (Not Implemented in Reporting -- See Above Note)
REPLACE VIEW {environment_schema}.anniversary_supplier_class_vw
AS
LOCK ROW FOR ACCESS
SELECT a.day_idnt
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
    , a.general_merch_manager_executive_vice_president
    , a.div_merch_manager_senior_vice_president
    , a.div_merch_manager_vice_president
    , a.merch_director
    , a.buyer
    , a.merch_planning_executive_vice_president
    , a.merch_planning_senior_vice_president
    , a.merch_planning_vice_president
    , a.merch_planning_director_manager
    , a.assortment_planner
    , a.npg_ind
    , a.bipoc_ind
    , a.anchor_brands
    , a.quantrix_category
    , a.ccs_category
    , a.ccs_subcategory
    , a.nord_role_desc
    , a.merch_theme
    , a.assortment_grouping
    , a.anniversary_theme
    , a.udig
    , a.price_band_one
    , a.price_band_two
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
    , a.product_views
    , a.cart_adds
    , a.com_order_units
    , a.instock_views
	  , a.scored_views
    , a.wishlist_adds
    , a.com_demand
    , a.units
    , a.retail_original_dollars
    , a.retail_special_dollars
    , sales_cost
   	, return_cost
	, shipped_cost
	, store_fulfill_cost
	, dropship_cost
	, eoh_cost
	, boh_cost
	, receipt_cost
	, receipt_dropship_cost
	, on_order_cost_dollars
	, sales_pm
	, last_updated_tmstp
  , b.PLAN_NET_SALES_R AS cat_plan_net_sales_r
	, b.PLAN_NET_SALES_C AS cat_plan_net_sales_c
	, b.PLAN_NET_SALES_U AS cat_plan_net_sales_u
	, b.PLAN_ANNIV_CC_COUNT AS cat_plan_anniv_cc_count
	, b."PLAN_PRODUCT_MARGIN_$" AS cat_plan_pm_dollars
FROM
	(
	SELECT
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
	    , ap.channel_idnt
	    , ap.country
	    , ap.dropship_ind
	    , ap.division
	    , ap.subdivision
	    , ap.department
	    , TRIM(STRTOK(ap.department,', ', 1)) AS dept_idnt
	    , ap."class"
	    , ap.subclass
	    , ap.supplier
	    , ap.brand
      , ap.general_merch_manager_executive_vice_president
      , ap.div_merch_manager_senior_vice_president
      , ap.div_merch_manager_vice_president
      , ap.merch_director
      , ap.buyer
      , ap.merch_planning_executive_vice_president
      , ap.merch_planning_senior_vice_president
      , ap.merch_planning_vice_president
      , ap.merch_planning_director_manager
      , ap.assortment_planner
	    , ap.npg_ind
	    , ap.bipoc_ind
      , ap.anchor_brands
	    , ap.quantrix_category
	    , ap.ccs_category
	    , ap.ccs_subcategory
	    , ap.nord_role_desc
	    , ap.merch_theme
	    , ap.anniversary_theme
      , ap.assortment_grouping
	    , ap.udig
	    , ap.price_band_one
	    , ap.price_band_two
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
	    , SUM(ap.product_views) AS product_views
	    , SUM(ap.cart_adds)  AS cart_adds
	    , SUM(ap.order_units) AS com_order_units
	    , SUM(ap.instock_views) AS instock_views
		, SUM(ap.scored_views) AS scored_views
	    , sum(ap.wishlist_adds) AS wishlist_adds
	    , SUM(ap.demand) AS com_demand
	    , SUM(ap.units) AS units
	    , SUM(ap.units * ap.retail_original) AS retail_original_dollars
	    , SUM(ap.units * ap.retail_special) AS retail_special_dollars
	    , SUM(ap.sales_cost) AS sales_cost
	   	, SUM(ap.return_cost) AS return_cost
		, SUM(ap.shipped_cost) AS shipped_cost
		, SUM(ap.store_fulfill_cost) AS store_fulfill_cost
		, SUM(ap.dropship_cost) AS dropship_cost
		, SUM(ap.eoh_cost) AS eoh_cost
		, SUM(ap.boh_cost) AS boh_cost
		, SUM(ap.receipt_cost) AS receipt_cost
		, SUM(ap.receipt_dropship_cost) AS receipt_dropship_cost
		, SUM(ap.on_order_cost_dollars) AS on_order_cost_dollars
		, SUM(ap.sales_pm) AS sales_pm
		, MAX(process_tmstp) AS last_updated_tmstp
	FROM {environment_schema}.scaled_event_sku_clk_rms ap
	WHERE date_event_type = 'Anniversary'
		AND day_dt IN (SELECT DISTINCT day_dt FROM {environment_schema}.scaled_event_dates
			WHERE day_dt_aligned <= (SELECT MAX(day_dt) FROM {environment_schema}.scaled_event_sku_clk_rms WHERE date_event_type = 'Anniversary' AND ty_ly_ind = 'TY'))
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46
) a
LEFT JOIN T2DL_DAS_SCALED_EVENTS.ANNIVERSARY_COST_CATEGORY_PLANS b
	ON a.channel_idnt = b.channel
	AND a.dept_idnt = b.dept_idnt
  AND a.ty_ly_ind = 'TY'
	AND a.event_phase <> 'Post';

--GRANT SELECT ON {environment_schema}.anniversary_supplier_class_vw TO PUBLIC;









/******************************************************************************
** TABLE: {environment_schema}.anniversary_market_vw
**
** DESCRIPTION: Source of Anniversary Market Tableau dashboard data source
** UPDATED: 2024-06-10
******************************************************************************/

REPLACE VIEW {environment_schema}.anniversary_market_vw
AS
LOCK ROW FOR ACCESS
WITH date_lkp AS(
 SELECT DISTINCT
 	day_dt
 FROM {environment_schema}.SCALED_EVENT_DATES
 WHERE day_dt_aligned <= (SELECT MAX(day_dt) FROM T2DL_DAS_SCALED_EVENTS.scaled_event_sku_clk_rms WHERE date_event_type = 'Anniversary' AND ty_ly_ind = 'TY')
 	AND anniv_ind = 1
)
SELECT
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
    , ap.bipoc_ind
    , ap.anchor_brands
    , ap.quantrix_category
    , ap.ccs_category
    , ap.ccs_subcategory
    , ap.nord_role_desc
    , ap.merch_theme
    , ap.anniversary_theme
    , ap.assortment_grouping
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
    , SUM(ap.product_views) AS product_views
    , SUM(ap.cart_adds)  AS cart_adds
    , SUM(ap.order_units) AS com_order_units
    , SUM(ap.instock_views) AS instock_views
	, SUM(ap.scored_views) AS scored_views
    , SUM(ap.demand) AS com_demand
    , SUM(ap.sales_cost) AS sales_cost
   	, SUM(ap.return_cost) AS return_cost
	, SUM(ap.shipped_cost) AS shipped_cost
	, SUM(ap.store_fulfill_cost) AS store_fulfill_cost
	, SUM(ap.dropship_cost) AS dropship_cost
	, SUM(ap.eoh_cost) AS eoh_cost
	, SUM(ap.boh_cost) AS boh_cost
	, SUM(ap.receipt_cost) AS receipt_cost
	, SUM(ap.receipt_dropship_cost) AS receipt_dropship_cost
	, SUM(ap.on_order_cost_dollars) AS on_order_cost_dollars
	, SUM(ap.sales_pm) AS sales_pm
	, MAX(process_tmstp) AS last_updated_tmstp
FROM {environment_schema}.scaled_event_sku_clk_rms ap
INNER JOIN date_lkp d
	ON d.day_dt = ap.day_dt
WHERE date_event_type = 'Anniversary'
	AND ap.ty_ly_ind = 'TY'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32
UNION ALL
SELECT
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
    , ap.bipoc_ind
    , ap.anchor_brands
    , ap.quantrix_category
    , ap.ccs_category
    , ap.ccs_subcategory
    , ap.nord_role_desc
    , ap.merch_theme
    , ap.anniversary_theme
    , ap.assortment_grouping
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
    , SUM(ap.product_views) AS product_views
    , SUM(ap.cart_adds)  AS cart_adds
    , SUM(ap.order_units) AS com_order_units
    , SUM(ap.instock_views) AS instock_views
	, SUM(ap.scored_views) AS scored_views
    , SUM(ap.demand) AS com_demand
    , SUM(ap.sales_cost) AS sales_cost
   	, SUM(ap.return_cost) AS return_cost
	, SUM(ap.shipped_cost) AS shipped_cost
	, SUM(ap.store_fulfill_cost) AS store_fulfill_cost
	, SUM(ap.dropship_cost) AS dropship_cost
	, SUM(ap.eoh_cost) AS eoh_cost
	, SUM(ap.boh_cost) AS boh_cost
	, SUM(ap.receipt_cost) AS receipt_cost
	, SUM(ap.receipt_dropship_cost) AS receipt_dropship_cost
	, SUM(ap.on_order_cost_dollars) AS on_order_cost_dollars
	, SUM(ap.sales_pm) AS sales_pm
	, MAX(process_tmstp) AS last_updated_tmstp
FROM {environment_schema}.scaled_event_sku_clk_rms ap
INNER JOIN date_lkp d
	ON d.day_dt = ap.day_dt
WHERE date_event_type = 'Anniversary'
	AND ap.ty_ly_ind = 'LY'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32
;

--GRANT SELECT ON {environment_schema}.anniversary_market_vw TO PUBLIC;


/******************************************************** END - TABLEAU VIEWS ********************************************************/
