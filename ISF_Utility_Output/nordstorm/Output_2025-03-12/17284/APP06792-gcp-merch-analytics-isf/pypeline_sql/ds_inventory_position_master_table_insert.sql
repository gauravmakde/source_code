/******************************************************************************
** FILE: ip_master_table_insert.sql
** PROJECT: inventory-positioning
** TYPE: INSERT
** TABLE: T2DL_DAS_INV_POSITIONING.inventory_position_master_table
**
** DESCRIPTION:
**      This SQL file contains the pipeline that updates the master table for the Inventory Position Demand Forecasting Project.
**      This file generates weekly demand for each online CC at the geographic resolution of DMA and combines it with inventory, pricing, and event data.
**		This query backfills data for the past 3 weeks and inserts into the master table (seen in the frequent "CURRENT DATE - 21" statements)
**
** GRANULARITY:
**      Scope: Online (N.COM, R.COM)
**      Product: CC
**      Time: Weekly
**      Geography: DMA (Designated Marketing Area)
**
** NOTES:
**		The live_date given comes from PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW and may not be wholly reliable.
**		Pricing information at a given time is determined from effective timestamps in PRD_NAP_USR_VWS.PRODUCT_PRICE_FORECAST_DIM.  These can generally change
**			to accommodate NAP data events (such that there is no overlap), though they should be stable for historical events in this table.  Note that, while
**			effective timestamps ensure no overlap between pricing events, they can generally miss pricing information from the beginning of an item's history.
**
** AUTHORS:
**      - Corey Kownacki (corey.kownacki@nordstrom.com)
**      - Anisha Dubashi
**      - Chuan Chen 
******************************************************************************/


-- Exclude anniversary items from forecasts.  Collect them first.  Note different effective flags for different years (2020 vs 2021+).

CREATE MULTISET VOLATILE TABLE anniv AS (
SELECT  
	DISTINCT day_year, 
	rms_sku_num, 
	channel_country, 
	channel_brand, 
	selling_channel
FROM
(
	SELECT  day_date,
			EXTRACT(YEAR from day_date) as day_year
	FROM prd_nap_usr_vws.SCALED_EVENTS_DATES_DIM
	WHERE EXTRACT(YEAR from day_date) >= 2020 AND event_name = 'Anniversary Sale' 
) a
INNER JOIN PRD_NAP_USR_VWS.PRODUCT_PRICE_FORECAST_DIM
ON a.day_date BETWEEN eff_begin_tmstp AND eff_end_tmstp
WHERE ((event_tags LIKE '%ANNIVERSARY_SALE%' AND day_year > 2020) OR (current_price_reason = 'AnniversaryPromotion' and day_year = 2020)) --2020 skus rely on different flag for anniversary
AND selling_channel = 'ONLINE' -- PRODUCT ONLINE
AND channel_brand = 'NORDSTROM' -- N.COM BRAND
)
WITH DATA primary index (day_year, rms_sku_num, channel_country, channel_brand, selling_channel)
ON commit PRESERVE ROWS;
COLLECT STATISTICS COLUMN (DAY_YEAR ,RMS_SKU_NUM ,CHANNEL_COUNTRY) ON anniv;
COLLECT STATISTICS COLUMN (DAY_YEAR) ON anniv;


-- Retrieve SKU/CC level information for products WITH intention to aggregate up to CC (epm_choice_num)

CREATE multiset volatile TABLE sku AS (
SELECT  rms_sku_num
       ,channel_country
       ,rms_style_num
       ,epm_choice_num
       ,style_desc
       ,brand_name
       ,web_style_num
       ,prmy_supp_num
       ,manufacturer_num
       ,sbclass_num
       ,class_num
       ,dept_num
       ,grp_num
       ,div_num
       ,color_num
       ,color_desc
       ,nord_display_color
       ,size_1_num
       ,size_2_num
       ,return_disposition_code
       ,selling_status_desc
       ,CASE WHEN live_date > current_date THEN null 
	   ELSE live_date
		END as live_date
       ,selling_channel_eligibility_list
       ,drop_ship_eligible_ind
FROM PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW 
)
WITH DATA primary index (rms_sku_num, channel_country)
ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS COLUMN (RMS_SKU_NUM ,CHANNEL_COUNTRY,EPM_CHOICE_NUM) ON sku;
COLLECT STATISTICS COLUMN (CHANNEL_COUNTRY ,EPM_CHOICE_NUM ,SIZE_1_NUM) ON sku;
COLLECT STATISTICS COLUMN (CHANNEL_COUNTRY ,EPM_CHOICE_NUM) ON sku;
COLLECT STATISTICS COLUMN (RMS_SKU_NUM ,CHANNEL_COUNTRY) ON sku;
COLLECT STATISTICS COLUMN (EPM_CHOICE_NUM) ON sku;
COLLECT STATISTICS COLUMN (RMS_SKU_NUM) ON sku;
COLLECT STATISTICS COLUMN (CHANNEL_COUNTRY) ON sku;


-- Identify replenishment (RP) items from our online inventory

CREATE MULTISET VOLATILE TABLE RP AS (
   SELECT A.week_num,
          B.epm_choice_num,
          A.channel_country,
          A.channel_brand,
          A.selling_channel,
          MAX(A.rp_ind) AS rp_ind
   FROM (
	   SELECT D.week_num,
	          CAST(R.RMS_SKU_IDNT AS INTEGER) AS rms_sku_num,
	          CASE
		          WHEN S.business_unit_desc IN ('FULL LINE','N.COM', 'OMNI.COM', 'RACK','OFFPRICE ONLINE', 'CORPORATE') THEN 'US'
		          ELSE 'CA'
		          END AS channel_country,
	          CASE
		          WHEN S.channel_num in (120,121) THEN 'NORDSTROM'
		          WHEN S.channel_num in (250) THEN 'NORDSTROM_RACK'
		          END AS channel_brand,
	          CASE
		          WHEN S.channel_num in (120,121,250) THEN 'ONLINE'
		          WHEN S.channel_num in (110,111,210,211) THEN 'STORE'
		          END AS selling_channel,
	          MAX(CASE WHEN R.AIP_RP_FL = 'Y' THEN 1 ELSE 0 END) AS rp_ind
	   FROM PRD_USR_VWS.AIP_RP_SKU_LD_LKUP AS R
	   JOIN PRD_NAP_USR_VWS.STORE_DIM AS S 
	   ON S.STORE_NUM =  CAST(R.LOC_IDNT AS INTEGER)
	   JOIN PRD_NAP_USR_VWS.DAY_CAL AS D 
	   ON R.day_idnt = D.day_num
	   WHERE D.DAY_DATE >= (current_date - 21)
	     AND S.business_unit_desc IN ('N.COM', 'OFFPRICE ONLINE') 
	   GROUP BY 1,2,3,4,5
   ) A
   JOIN sku AS B
   ON A.rms_sku_num = B.rms_sku_num
   AND A.channel_country = B.channel_country
   WHERE B.epm_choice_num IS NOT NULL
   GROUP BY 1,2,3,4,5
) 
WITH DATA PRIMARY INDEX (week_num, epm_choice_num, channel_country, channel_brand, selling_channel) 
ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS COLUMN (WEEK_NUM ,EPM_CHOICE_NUM,CHANNEL_COUNTRY, CHANNEL_BRAND, SELLING_CHANNEL) ON RP;


-- Since project is constrained to the online channel, be sure the products we're collecting are liveonsite (available to buy online)

CREATE multiset volatile TABLE liveonsite_base AS (
SELECT  p.rms_sku_num
       ,p.channel_country 
       ,p.channel_brand
       ,'ONLINE' as selling_channel
       ,a.epm_choice_num
       ,p.eff_begin_tmstp
       ,p.eff_end_tmstp
FROM PRD_NAP_USR_VWS.PRODUCT_ONLINE_PURCHASABLE_ITEM_DIM AS p --Should we change this alias to something more interpretable?
INNER JOIN
(
	SELECT  distinct rms_sku_num
	       ,channel_country
	       ,epm_choice_num
	FROM sku
) AS a
ON p.rms_sku_num = a.rms_sku_num 
AND p.channel_country = a.channel_country
WHERE is_online_purchasable = 'Y'
AND p.channel_brand IN ('NORDSTROM', 'NORDSTROM_RACK')
AND p.channel_country = 'US'
AND epm_choice_num is not null 
) 
WITH DATA primary index (rms_sku_num, channel_country, channel_brand, selling_channel)
ON COMMIT PRESERVE ROWS;


-- Using the liveonsite base table above, expand the liveonsite start/end timestamp to create a daily inventory of which SKUs are liveonsite.

CREATE multiset volatile TABLE liveonsite AS (
SELECT  rms_sku_num
       ,channel_country
       ,channel_brand,
       selling_channel
       ,epm_choice_num
       ,live_date 
FROM
(
	SELECT  pa.rms_sku_num
       ,pa.channel_country 
       ,pa.channel_brand
       ,pa.selling_channel
       ,pa.epm_choice_num
    	,pa.eff_begin_tmstp
    	,pa.eff_end_tmstp
		,CAST(begin (eff_period) AT local AS date ) AS live_date
	FROM liveonsite_base pa 
    EXPAND
	ON period(eff_begin_tmstp, eff_end_tmstp) AS eff_period by anchor day at time '08:00:00' AT local for period(current_timestamp - interval '180' day, current_timestamp) -- we look at a max 180 day history for liveonsite
) a 
)
WITH DATA primary index (rms_sku_num, channel_country, channel_brand, selling_channel)
ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS COLUMN (RMS_SKU_NUM ,CHANNEL_COUNTRY,EPM_CHOICE_NUM ,CHANNEL_BRAND, SELLING_CHANNEL) ON liveonsite;
COLLECT STATISTICS COLUMN (RMS_SKU_NUM ,CHANNEL_COUNTRY) ON liveonsite;
COLLECT STATISTICS COLUMN (LIVE_DATE) ON liveonsite;


-- Collect price information for SKUs

CREATE multiset volatile TABLE price AS (
SELECT p.rms_sku_num
       ,p.store_num
       ,p.channel_country
       ,p.channel_brand
       ,p.selling_channel
       ,p.regular_price_amt
       ,p.current_price_amt
       ,p.current_price_type
       ,p.current_price_event
       ,p.eff_begin_tmstp
       ,p.eff_end_tmstp
       ,p.event_tags
FROM PRD_NAP_USR_VWS.PRODUCT_PRICE_FORECAST_DIM p
INNER JOIN PRD_NAP_USR_VWS.STORE_DIM AS d
ON p.store_num = d.store_num
WHERE business_unit_desc IN ('N.COM', 'OFFPRICE ONLINE')

UNION --Impute period prior to first eff_begin_tmstp with max price seen during period started from pricing_start_tmstp.  Value is not completely reliable, but better than missing pricing information completely.

SELECT p.rms_sku_num
       ,p.store_num
       ,p.channel_country
       ,p.channel_brand
       ,p.selling_channel
       ,MAX(p.regular_price_amt) as regular_price_amt
       ,MIN(p.current_price_amt) as current_price_amt
       ,CASE WHEN MAX(p.regular_price_amt) = MIN(p.current_price_amt) THEN 'R' ELSE 'C' END as current_price_type
       ,MAX(current_price_event) as current_price_event
       ,MIN(p.pricing_start_tmstp) as eff_begin_tmstp
       ,MAX(p.eff_begin_tmstp) as eff_end_tmstp
       ,MAX(p.event_tags) as event_tags
FROM PRD_NAP_USR_VWS.PRODUCT_PRICE_FORECAST_DIM p
INNER JOIN PRD_NAP_USR_VWS.STORE_DIM AS d
ON p.store_num = d.store_num
WHERE business_unit_desc IN ('N.COM', 'OFFPRICE ONLINE')
GROUP BY 1, 2, 3, 4, 5
)
WITH DATA primary index (rms_sku_num, channel_country, channel_brand, selling_channel)
ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS COLUMN (RMS_SKU_NUM ,CHANNEL_COUNTRY, CHANNEL_BRAND, SELLING_CHANNEL) ON price;
COLLECT STATISTICS COLUMN (eff_begin_tmstp) ON price;
COLLECT STATISTICS COLUMN (eff_end_tmstp) ON price;


-- Connect the liveonsite SKUs with their price information during their available-to-buy periods.

CREATE multiset volatile TABLE liveonsite_price AS (
SELECT  g.week_num
       ,liveonsite.channel_country
       ,liveonsite.channel_brand
       ,liveonsite.selling_channel
       ,liveonsite.epm_choice_num
       ,MAX(case WHEN an.rms_sku_num is not null THEN 1 else 0 end)                                                                                 AS anniversary_flag
       ,MIN(g.week_start_date)                                                                                                                      AS week_start_date  --Is this cheaper than including week_start_date in the groupby?
       ,COUNT(distinct liveonsite.rms_sku_num) sku_ct
       ,COUNT(distinct CASE WHEN p.current_price_type = 'R' THEN liveonsite.rms_sku_num else null end) regular_sku_ct
       ,COUNT(distinct liveonsite.rms_sku_num || CAST((CAST(liveonsite.live_date AS FORMAT 'YYYY-MM-DD'))                                           AS char(10))) skuday_ct
       ,COUNT(distinct CASE WHEN p.current_price_type = 'R' THEN liveonsite.rms_sku_num || CAST((CAST(liveonsite.live_date AS FORMAT 'YYYY-MM-DD')) AS char(10)) else null end) regular_skuday_ct
       ,COUNT(distinct CASE WHEN p.current_price_type = 'C' THEN liveonsite.rms_sku_num || CAST((CAST(liveonsite.live_date AS FORMAT 'YYYY-MM-DD')) AS char(10)) else null end) clearance_skuday_ct
       ,COUNT(distinct CASE WHEN p.current_price_type = 'P' THEN liveonsite.rms_sku_num || CAST((CAST(liveonsite.live_date AS FORMAT 'YYYY-MM-DD')) AS char(10)) else null end) promotion_skuday_ct
       ,MAX(regular_price_amt) regular_price_amt
       ,MIN(current_price_amt) current_price_amt
FROM (
	SELECT  *
	FROM liveonsite
	WHERE live_date BETWEEN (current_date - 21) AND current_date
) liveonsite
INNER JOIN (
	SELECT  day_date
	       ,week_num
	       ,MIN(day_date) over (partition by week_num) AS week_start_date
	FROM PRD_NAP_USR_VWS.DAY_CAL
) g
ON liveonsite.live_date = g.day_date
LEFT JOIN anniv AS an
ON EXTRACT(YEAR from liveonsite.live_date) = an.day_year 
	AND liveonsite.rms_sku_num = an.rms_sku_num 
	AND liveonsite.channel_country = an.channel_country
LEFT JOIN price p
ON liveonsite.rms_sku_num = p.rms_sku_num 
	AND liveonsite.channel_brand = p.channel_brand
	AND liveonsite.selling_channel = p.selling_channel
	AND (liveonsite.live_date BETWEEN p.eff_begin_tmstp AND p.eff_end_tmstp)
WHERE liveonsite.channel_brand is not null AND liveonsite.selling_channel is not null
GROUP BY  1,2,3,4,5
HAVING epm_choice_num is not null
)
WITH DATA primary index (epm_choice_num, week_num, channel_country, channel_brand, selling_channel)
ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS COLUMN (WEEK_NUM ,EPM_CHOICE_NUM,CHANNEL_COUNTRY, CHANNEL_BRAND, SELLING_CHANNEL) ON liveonsite_price;
COLLECT STATISTICS COLUMN (WEEK_NUM ,CHANNEL_COUNTRY,EPM_CHOICE_NUM) ON liveonsite_price;
COLLECT STATISTICS COLUMN (CHANNEL_COUNTRY ,EPM_CHOICE_NUM) ON liveonsite_price;
COLLECT STATISTICS COLUMN (WEEK_START_DATE) ON liveonsite_price;
COLLECT STATISTICS COLUMN (WEEK_NUM) ON liveonsite_price;



-- Collect inventory information for CCs within our scope.

CREATE multiset volatile TABLE inventory AS (
SELECT  g.week_num
       ,epm_choice_num
       ,d.channel_country
       ,COUNT(distinct CASE WHEN business_unit_desc = 'N.COM' THEN location_id else null end) inv_ncom_location_ct
       ,COUNT(distinct CASE WHEN business_unit_desc = 'N.CA' THEN location_id else null end) inv_nca_location_ct
       ,COUNT(distinct CASE WHEN business_unit_desc = 'OFFPRICE ONLINE' THEN location_id else null end) inv_nrhl_location_ct
       ,COUNT(distinct CASE WHEN banner = 'FP' THEN location_id else null end) inv_usfp_location_ct
       ,COUNT(distinct CASE WHEN banner = 'OP' THEN location_id else null end) inv_usop_location_ct
       ,MAX(case WHEN location_type IN ('DS_OP','DS') THEN 1 else 0 end) AS dropship_stock_flag
       --,MAX(on_replenishment) on_replenishment
FROM PRD_NAP_USR_VWS.INVENTORY_STOCK_QUANTITY_BY_DAY_FACT AS p
INNER JOIN (
	SELECT  distinct store_num
	       ,business_unit_desc
	       ,CASE WHEN business_unit_desc IN ('FULL LINE','N.COM','N.CA','FULL LINE CANADA','TRUNK CLUB') THEN 'FP'
	             WHEN business_unit_desc IN ('RACK CANADA','RACK','OFFPRICE ONLINE') THEN 'OP'  ELSE null END  AS banner
	       ,CASE WHEN business_unit_desc IN ('FULL LINE','N.COM','TRUNK CLUB','RACK','OFFPRICE ONLINE') THEN 'US'
	             WHEN business_unit_desc IN ('RACK CANADA','N.CA','FULL LINE CANADA') THEN 'CA'  ELSE null END AS channel_country
	FROM PRD_NAP_USR_VWS.STORE_DIM
) AS d
ON p.location_id = d.store_num
INNER JOIN (
	SELECT  distinct rms_sku_num
	       ,channel_country
	       ,epm_choice_num
	FROM sku
) AS a
ON p.rms_sku_id = a.rms_sku_num 
AND d.channel_country = a.channel_country
INNER JOIN PRD_NAP_USR_VWS.DAY_CAL AS g
ON (p.snapshot_date + INTERVAL '1' DAY) = g.day_date --snapshot date is the snapshot of inventory taken the day before
WHERE snapshot_date >= (current_date - 21)
AND stock_on_hand_qty > 0
GROUP BY  1,2,3
HAVING epm_choice_num is not null 
)
WITH DATA primary index (epm_choice_num, week_num, channel_country)
ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS COLUMN (WEEK_NUM ,EPM_CHOICE_NUM,CHANNEL_COUNTRY) ON inventory;


-- Get the actual demand (order_line and BOPUS) for online channel.

CREATE multiset volatile TABLE orderdetail AS (

SELECT  
       source_channel_country_code as channel_country
       ,CASE WHEN source_channel_code = 'FULL_LINE' THEN 'NORDSTROM'
       		WHEN source_channel_code = 'RACK' THEN 'NORDSTROM_RACK' ELSE null END as channel_brand
       	,'ONLINE' as selling_channel
		,epm_choice_num
       ,g.week_num
       ,region
       ,dma_cd
       ,SUM(order_line_quantity) order_line_quantity
       ,SUM(order_line_amount_usd) order_line_amount_usd
       ,SUM(case WHEN LAST_RELEASED_NODE_TYPE_CODE = 'DS' THEN order_line_quantity else 0 end)                              AS dropship_order_line_quantity
       ,SUM(case WHEN LAST_RELEASED_NODE_TYPE_CODE = 'DS' THEN order_line_amount_usd else 0 end)                            AS dropship_order_line_amount_usd
       ,SUM(case WHEN PROMISE_TYPE_CODE like '%PICKUP%' THEN order_line_quantity else 0 end)                                AS bopus_order_line_quantity
       ,SUM(case WHEN PROMISE_TYPE_CODE like '%PICKUP%' THEN order_line_amount_usd else 0 end)                              AS bopus_order_line_amount_usd
       ,SUM(case WHEN CANCEL_REASON_CODE = 'MERCHANDISE_NOT_AVAILABLE' THEN order_line_quantity else 0 end)                 AS MERCHANDISE_NOT_AVAILABLE_order_line_quantity
       ,SUM(case WHEN CANCEL_REASON_CODE = 'MERCHANDISE_NOT_AVAILABLE' THEN order_line_amount_usd else 0 end)               AS MERCHANDISE_NOT_AVAILABLE_order_line_amount_usd
FROM (
	SELECT  order_num
	       ,order_line_num
	       ,upc_code
	       ,rms_sku_num
	       ,promise_type_code
	       ,LAST_RELEASED_NODE_TYPE_CODE
	       ,bill_zip_code
	       ,destination_zip_code
	       ,order_date_pacific
	       ,source_channel_code
	       ,source_channel_country_code
	       ,CANCEL_REASON_CODE
	       ,order_line_amount_usd
	       ,order_line_quantity
	FROM prd_nap_usr_vws.ORDER_LINE_DETAIL_FACT
	WHERE ORDER_DATE_PACIFIC >= (current_date - 21) -- OMS started supporting Ship order 100% after 2021-04-29
	AND ORDER_DATE_PACIFIC <> '4444-04-04' -- excluding orders WHERE NAP received partial data only. eg: canceled event only 
) p
INNER JOIN (
	SELECT  distinct rms_sku_num
	       ,channel_country
	       ,epm_choice_num
	FROM sku
) AS a
ON p.rms_sku_num = a.rms_sku_num 
AND p.source_channel_country_code = a.channel_country
INNER JOIN PRD_NAP_USR_VWS.DAY_CAL g
ON p.order_date_pacific = g.day_date
LEFT JOIN T2DL_SCOR_FULFILLMENT_PLANNING.region_dma_zip3_vw z
ON substr(trim(cast(coalesce(p.destination_zip_code, p.bill_zip_code) AS varchar(10))), 1, 3) = cast(z.zip3 AS varchar(10))
WHERE (CANCEL_REASON_CODE = 'MERCHANDISE_NOT_AVAILABLE' or CANCEL_REASON_CODE is null or CANCEL_REASON_CODE = '')
GROUP BY  1,2,3,4,5,6,7
HAVING epm_choice_num is not null )
WITH DATA primary index (epm_choice_num, week_num, channel_country, channel_brand, selling_channel)
ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS COLUMN (CHANNEL_COUNTRY, CHANNEL_BRAND, SELLING_CHANNEL ,EPM_CHOICE_NUM,WEEK_NUM) ON orderdetail;


-- Get product funnel information for CCs (reviews, product views).  Join through web_style_id (the online style indicator for a product)

CREATE multiset volatile TABLE digital AS (
SELECT  g.week_num
		,channel_country
		,channel_brand
       ,epm_choice_num
       ,avg(review_count) review_count
       ,SUM(product_views) product_views
FROM (
	SELECT  event_date_pacific
			,channelcountry as channel_country
			,CASE WHEN channel='FULL_LINE' THEN 'NORDSTROM' 
				WHEN channel='RACK' THEN 'NORDSTROM_RACK' 
				ELSE null END as channel_brand
	       ,epm_choice_num
	       ,avg(review_count) review_count
	       ,SUM(product_views) product_views
	FROM T2DL_DAS_PRODUCT_FUNNEL.product_funnel_daily p
	INNER JOIN (
		SELECT  distinct sku.channel_country
		       ,sku.epm_choice_num
		       ,web_style_id
		FROM sku
		INNER JOIN (
			SELECT  distinct web_style_id
			       ,rms_style_group_num
			       ,rms_sku_id
			       ,country_cd AS channel_country
			FROM T2DL_DAS_ETE_INSTRUMENTATION.LIVE_ON_SITE_PCDB_US_OPR a
		) AS style_group
		ON sku.rms_sku_num = style_group.rms_sku_id AND sku.channel_country = style_group.channel_country AND style_group.web_style_id is not null
	) AS a
	ON a.web_style_id = p.style_id AND a.channel_country = p.channelcountry
	WHERE event_date_pacific >= (current_date - 21)
	GROUP BY  1,2,3,4
	HAVING epm_choice_num is not null
) AS d
INNER JOIN PRD_NAP_USR_VWS.DAY_CAL g
ON d.event_date_pacific = g.day_date
GROUP BY  1,2,3,4 
)
WITH DATA primary index (epm_choice_num, week_num, channel_country, channel_brand)
ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS COLUMN (WEEK_NUM ,CHANNEL_COUNTRY, CHANNEL_BRAND,EPM_CHOICE_NUM) ON digital;


-- Combine daily event data from FP events with Rack specific events

CREATE multiset volatile TABLE events AS (
	select
		cal.week_num
		, case when max(op.rcom_binary) = 1 then 'Clear the Rack' end as op_event_name
		, max(fp.event_name) as fp_event_name
	from PRD_NAP_USR_VWS.DAY_CAL AS cal
	left join T3DL_ACE_OP.rack_sales_shipping AS op -- includes future data for 1-2 quarters
	on op.date_id = cal.day_date
	left join PRD_NAP_USR_VWS.SCALED_EVENTS_DATES_DIM AS fp
	on fp.day_date = cal.day_date
	where cal.day_date >= (current_date - 21)
	group by 1
)
WITH DATA primary index (week_num)
ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS COLUMN (WEEK_NUM) ON events;


-- To avoid potential duplication, clear weeks that are about to be loaded or regenerated from the master table.
-- Aim to regenerate last 3 weeks with cushion to avoid loading incomplete (current) week.

DELETE FROM T2DL_DAS_INV_POSITION_FORECAST.inventory_position_master_table 
--DELETE FROM T2DL_DAS_INV_POSITION_FORECAST.inventory_position_master_table
WHERE (week_start_date >= current_date - 21 AND week_start_date < current_date - 3);


-- INSERT aggregated information at the CC/DMA/WEEK level for the past 3 weeks.  Combine liveonsite-price, inventory, and demand data
-- with event, anniversary, and replenishment data to form master table.

INSERT INTO T2DL_DAS_INV_POSITION_FORECAST.inventory_position_master_table 
--INSERT INTO T2DL_DAS_INV_POSITION_FORECAST.inventory_position_master_table
SELECT  a.week_num
       ,a.week_start_date
       ,a.channel_brand
       ,a.selling_channel
       ,b.*
       ,a.anniversary_flag
       ,a.sku_ct
       ,a.regular_sku_ct
       ,a.skuday_ct
       ,a.regular_skuday_ct
       ,a.clearance_skuday_ct
       ,a.promotion_skuday_ct
       ,a.regular_price_amt
       ,a.current_price_amt
       ,c.inv_ncom_location_ct
       ,c.inv_nca_location_ct
       ,c.inv_nrhl_location_ct
       ,c.inv_usfp_location_ct
       ,c.inv_usop_location_ct
       ,d.region
       ,d.dma_cd
       ,d.order_line_quantity
       ,d.order_line_amount_usd
       ,d.bopus_order_line_quantity
       ,d.bopus_order_line_amount_usd
       ,d.dropship_order_line_quantity
       ,d.dropship_order_line_amount_usd
       ,d.MERCHANDISE_NOT_AVAILABLE_order_line_quantity
       ,d.MERCHANDISE_NOT_AVAILABLE_order_line_amount_usd
       ,e.review_count
       ,e.product_views
       ,f.fp_event_name
       ,f.op_event_name
       ,coalesce(g.rp_ind,0) as rp_ind
FROM liveonsite_price a
INNER JOIN (
	SELECT  channel_country
	       ,epm_choice_num
	       ,COUNT(distinct rms_sku_num) total_sku_ct
	       ,COUNT(distinct size_1_num) total_size1_ct
	       ,MAX(rms_style_num) rms_style_num
	       ,MAX(color_num) color_num
	       ,MAX(brand_name) brand_name
	       ,MAX(manufacturer_num) manufacturer_num
	       ,MAX(prmy_supp_num) prmy_supp_num
	       ,MAX(sbclass_num) sbclass_num
	       ,MAX(class_num) class_num
	       ,MAX(dept_num) dept_num
	       ,MAX(grp_num) grp_num
	       ,MAX(div_num) div_num
	       ,MAX(style_desc) style_desc
	       ,MAX(nord_display_color) nord_display_color
	       ,MAX(return_disposition_code) return_disposition_code
	       ,MIN(live_date) live_start_date
	       ,MAX(selling_channel_eligibility_list) selling_channel_eligibility_list
	       ,MAX(drop_ship_eligible_ind) drop_ship_eligible_ind
	FROM sku
	GROUP BY 1,2
) b
ON a.channel_country = b.channel_country 
AND a.epm_choice_num = b.epm_choice_num
LEFT JOIN inventory AS c
ON a.channel_country = c.channel_country 
AND a.epm_choice_num = c.epm_choice_num 
AND a.week_num = c.week_num
LEFT JOIN orderdetail AS d
ON a.epm_choice_num = d.epm_choice_num 
AND a.channel_country = d.channel_country
AND a.channel_brand = d.channel_brand
AND a.selling_channel = d.selling_channel
AND a.week_num = d.week_num
LEFT JOIN digital AS e
ON a.epm_choice_num = e.epm_choice_num 
AND a.channel_country = e.channel_country
AND a.channel_brand = e.channel_brand
AND a.week_num = e.week_num
LEFT JOIN events AS f
ON a.week_num = f.week_num
LEFT JOIN RP AS g
ON a.epm_choice_num = g.epm_choice_num 
AND a.channel_country = g.channel_country
AND a.channel_brand = g.channel_brand
AND a.selling_channel = g.selling_channel
AND a.week_num = g.week_num
WHERE (a.week_start_date >= current_date - 21 AND week_start_date < current_date - 3);