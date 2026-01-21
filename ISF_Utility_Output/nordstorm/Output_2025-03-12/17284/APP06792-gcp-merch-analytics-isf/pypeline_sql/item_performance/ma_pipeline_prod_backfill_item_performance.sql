/*
Purpose:        Inserts data in {{environment_schema}} tables for Item Performance Reporting for backfill (NOTE: Need to run main script to update
                monthly and weekly tables - not date dependent so not included in this script to help performance)
                    item_performance_daily_base
Variable(s):     {{environment_schema}} T2DL_DAS_SELECTION (prod) or T3DL_ACE_ASSORTMENT
                 {{env_suffix}} '' or '_dev' tablesuffix for prod testing
Author(s):       Sara Scott
*/



--Dates table: gets dates

CREATE MULTISET VOLATILE TABLE dates AS (

SELECT tme.day_date
				, tme.day_idnt
				, tme.week_idnt
				, tme.week_desc
				, tme.week_end_day_date
				, tme.month_end_week_idnt
				, tme.month_idnt
				, tme.quarter_idnt
				, tme.fiscal_year_num
    FROM prd_nap_usr_vws.day_cal_454_dim tme
   	WHERE tme.day_date BETWEEN {start_date} and {end_date}

  )
WITH DATA PRIMARY INDEX(week_idnt, month_idnt)
ON COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX (week_idnt, month_idnt) ON dates;

CREATE MULTISET VOLATILE TABLE sku_cc_lkp AS (

SELECT
     rms_sku_num AS sku_idnt
    ,channel_country
    ,dept_num || '_' || prmy_supp_num || '_' || supp_part_num || '_' || COALESCE(color_num, 'UNKNOWN') AS customer_choice -- dept || supplier || vpn || color
FROM prd_nap_usr_vws.product_sku_dim_vw
)
WITH DATA
PRIMARY INDEX (sku_idnt, channel_country)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX (sku_idnt, channel_country)
    ,COLUMN (customer_choice)
    ,COLUMN (sku_idnt, channel_country)
    ,COLUMN (channel_country, customer_choice)
     ON sku_cc_lkp;



--Location Table: Filters to only the locations we need
CREATE MULTISET VOLATILE TABLE loc AS(

SELECT
	store_num
	, store_name
	, CASE
    	WHEN channel_num IN ('110', '120', '130', '140', '210', '220', '250', '260', '310') THEN 'US'
    	WHEN channel_num IN ('111', '121', '211', '221', '261', '311') THEN 'CA'
    	ELSE NULL
    	END AS country
	, CASE
	    WHEN channel_num IN ('110', '120', '130', '140', '111', '121', '310', '311') THEN 'FP'
	    WHEN channel_num IN ('211', '221', '261', '210', '220', '250', '260') THEN 'OP'
	    ELSE NULL
	    END AS banner
	,CAST (channel_num as VARCHAR(50)) || ', ' || CAST(channel_desc as VARCHAR(100)) AS channel
FROM  PRD_NAP_USR_VWS.STORE_DIM
WHERE channel_num in('110','111', '120', '121', '130', '140', '210', '211', '220', '221','250', '260', '261',  '310', '311')
GROUP BY 1,2,3,4,5
)
WITH DATA
PRIMARY INDEX (store_num)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX (store_num)
     ON loc;



 -- NEW / CF logic (from selection planning code - using the logic instead of T2DL_DAS_Selection.selection_cc_type
 -- because we need the location idnt's in order to map to channels)
CREATE MULTISET VOLATILE TABLE receipts_prep AS (

SELECT
    sku_idnt
	, day_idnt
	, loc_idnt
	, SUM(rcpt.rcpt_po_tot_units + rcpt.rcpt_rsk_tot_units + rcpt.rcpt_dsh_tot_units) as rcpt_tot_units
FROM prd_ma_bado.rcpt_sku_ld_cvw rcpt
WHERE rcpt.wk_idnt BETWEEN (SELECT DISTINCT month_start_week_idnt
                            FROM prd_nap_usr_vws.day_cal_454_dim
                            WHERE month_idnt = (SELECT CAST(CONCAT(CAST(CASE WHEN fiscal_month_num >= 5 THEN fiscal_year_num
                                                                        ELSE (fiscal_year_num - 1) END AS VARCHAR(4))
                                                                  ,LPAD(CAST(CASE WHEN fiscal_month_num >= 5 THEN fiscal_month_num - 4
                                                                      ELSE (fiscal_month_num + 8) END AS VARCHAR(2)), 2, '0')
                                                                  ) AS INTEGER)
                                                                -- 4 month extension to look for new at the beginning
                                                FROM prd_nap_usr_vws.day_cal_454_dim
                                                WHERE day_date = ADD_MONTHS(current_date-1, -24)))
                       AND (SELECT month_end_week_idnt
                            FROM prd_nap_usr_vws.day_cal_454_dim
                            WHERE day_date = current_date-1 )
  AND rcpt.rcpt_tot_units > 0
  AND rcpt.rcpt_tot_retl$ > 0
  AND UPPER(rcpt.po_type) <> 'PU' -- Photography Unit, ordered for the photo studio
GROUP BY sku_idnt, day_idnt, loc_idnt
)
WITH DATA
PRIMARY INDEX (sku_idnt, day_idnt, loc_idnt)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
    PRIMARY INDEX (sku_idnt, day_idnt, loc_idnt)
    ,COLUMN (sku_idnt)
    ,COLUMN (day_idnt)
    ,COLUMN (loc_idnt)
    ,COLUMN (day_idnt, loc_idnt)
    ON receipts_prep;




--DROP TABLE receipts_monthly;
CREATE MULTISET VOLATILE TABLE receipts_monthly AS (
SELECT
     cal.month_idnt
    ,cal.month_start_day_date
    ,rp.sku_idnt
    ,country
    ,banner
    ,channel
    ,SUM(rcpt_tot_units) as receipt_units
    ,DENSE_RANK() OVER (ORDER BY cal.month_idnt) AS new_cf_rank
FROM receipts_prep rp
JOIN prd_nap_usr_vws.day_cal_454_dim cal
  ON rp.day_idnt = cal.day_idnt
JOIN loc st
  ON rp.loc_idnt = st.store_num
GROUP BY cal.month_idnt, cal.month_start_day_date, rp.sku_idnt, country, banner, channel
)
WITH DATA
PRIMARY INDEX (sku_idnt)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(sku_idnt)
    ,COLUMN (month_idnt, country, channel)
    ,COLUMN (month_idnt, country, banner, new_cf_rank)
    ,COLUMN (country, sku_idnt)
    ON receipts_monthly;

CREATE MULTISET VOLATILE TABLE selection_cc_type AS (
WITH los_ccs AS (
    SELECT DISTINCT
         d.month_idnt
        ,cc.channel_country
        , CASE WHEN channel_brand = 'NORDSTROM' THEN 'FP'
        WHEN channel_brand = 'NORDSTROM_RACK' THEN 'OP'
        END AS banner
        ,cc.customer_choice
        ,1 AS los_flag
    FROM t2dl_das_site_merch.live_on_site_daily l
    JOIN sku_cc_lkp cc
      ON l.sku_id = cc.sku_idnt
     AND l.channel_country = cc.channel_country
    JOIN prd_nap_usr_vws.day_cal_454_dim d
      ON d.day_date = l.day_date
),
rcpts AS (
    SELECT
         rcpt.month_idnt
        ,rcpt.month_start_day_date
        ,new_cf_rank
        ,rcpt.country
        ,rcpt.banner
        ,rcpt.channel
        ,cc.customer_choice
        ,SUM(receipt_units) AS rcpt_units
        ,LAG(new_cf_rank) OVER (PARTITION BY cc.customer_choice, rcpt.country, rcpt.banner, rcpt.channel ORDER BY new_cf_rank) as mth_lag
    FROM receipts_monthly rcpt
    JOIN sku_cc_lkp cc
      ON cc.sku_idnt = rcpt.sku_idnt
     AND cc.channel_country = rcpt.country
    GROUP BY 1,2,3,4,5,6,7
)
SELECT
     r.month_idnt
    ,r.month_start_day_date
    ,r.country
    ,r.banner
    ,r.channel
    ,r.customer_choice
    ,COALESCE(l.los_flag, 0) AS los_flag
    ,CASE WHEN new_cf_rank - mth_lag <= 4 THEN 'CF'
          ELSE 'NEW' END AS cc_type -- considered NEW if receipted within last 4 months
    ,CURRENT_TIMESTAMP AS update_timestamp
FROM rcpts r
LEFT JOIN los_ccs l
  ON r.month_idnt = l.month_idnt
 AND r.customer_choice = l.customer_choice
 AND r.country = l.channel_country
 AND r.banner = l.banner
WHERE r.month_idnt BETWEEN (SELECT DISTINCT month_idnt
                            FROM prd_nap_usr_vws.day_cal_454_dim
                            WHERE day_date = ADD_MONTHS(current_date-1,-24))
                       AND (SELECT DISTINCT month_idnt
                            FROM prd_nap_usr_vws.day_cal_454_dim
                            WHERE day_date = current_date-1)
)
WITH DATA PRIMARY INDEX(customer_choice) ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (customer_choice)
	ON selection_cc_type;




CREATE MULTISET VOLATILE TABLE new_cf_lkp AS (

SELECT
	sku.sku_idnt
	, cc.month_idnt
	, cc.country
	, cc.banner
	, cc.channel
	, cc.cc_type
FROM sku_cc_lkp sku
JOIN selection_cc_type cc
ON sku.customer_choice = cc.customer_choice
AND sku.channel_country = cc.country
)
WITH DATA
PRIMARY INDEX (sku_idnt, country)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (sku_idnt, country)
	ON new_cf_lkp;






--Live on Site Table: gets sku's with associated live on site start date as well as if an
-- item was live on a specific day

 CREATE MULTISET VOLATILE TABLE live_on_site
AS (

   SELECT

     CASE WHEN channel_brand = 'NORDSTROM' AND channel_country = 'US' THEN '120, N.COM'
    	WHEN channel_brand = 'NORDSTROM' AND channel_country = 'CA' THEN '121, N.CA'
    	WHEN channel_brand = 'NORDSTROM_RACK' THEN '250, OFFPRICE ONLINE'
    	END AS channel
   	 , channel_brand
	 , channel_country AS country
	 , CASE WHEN channel_brand = 'NORDSTROM' AND channel_country = 'US' THEN 808
    	WHEN channel_brand = 'NORDSTROM' AND channel_country = 'CA' THEN 857
    	WHEN channel_brand = 'NORDSTROM_RACK' THEN 591
    	END AS loc_idnt
	 , CASE WHEN channel_brand = 'NORDSTROM' THEN 'FP'
  		WHEN channel_brand = 'NORDSTROM_RACK' THEN 'OP'
   		END AS banner
	 , sku_id
	 , day_date
	 , 1 as days_live
	 , day_date as first_day_published
   FROM t2dl_das_site_merch.live_on_site_daily

   GROUP BY 1,2,3,4,5,6,7,8

)
WITH DATA
PRIMARY INDEX (day_date, sku_id, channel, country)
ON COMMIT preserve rows;

COLLECT STATS
	PRIMARY INDEX (day_date, sku_id, channel, country)
	ON live_on_site;


-- Replenishment Table: gets SKUS that are on RP
CREATE MULTISET VOLATILE TABLE replenishment
AS (
	SELECT
	     r.rms_sku_idnt as sku_idnt
	    ,r.loc_idnt
	    ,r.day_idnt
	    ,CASE WHEN r.aip_rp_fl = 'Y' THEN 1 END AS replenishment_flag
	FROM prd_usr_vws.aip_rp_sku_ld_lkup r


)
WITH DATA
PRIMARY INDEX (sku_idnt, day_idnt, loc_idnt)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
    PRIMARY INDEX (sku_idnt, day_idnt, loc_idnt)
    ON replenishment;





-- Dropship Table: Gets sku's dropship status by day
CREATE MULTISET VOLATILE TABLE dropship
AS (
    SELECT
        snapshot_date AS day_dt
        , rms_sku_id AS sku_idnt
        , 1 as dropship_flag
    FROM PRD_NAP_USR_VWS.INVENTORY_STOCK_QUANTITY_BY_DAY_FACT ds
    JOIN dates dt on ds.snapshot_date = dt.day_date
    WHERE location_type IN ('DS')
    GROUP BY 1,2,3
)
WITH DATA
PRIMARY INDEX (day_dt, sku_idnt )
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX (day_dt, sku_idnt)
     ON dropship;

--fanatics indicator
CREATE MULTISET VOLATILE TABLE fanatics
AS (
	SELECT DISTINCT
	    rms_sku_num AS sku_idnt
	from PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW psd
	JOIN PRD_NAP_USR_VWS.VENDOR_PAYTO_RELATIONSHIP_DIM payto
	  ON psd.prmy_supp_num = payto.order_from_vendor_num
	WHERE payto.payto_vendor_num = '5179609'
)
WITH DATA
PRIMARY INDEX(sku_idnt)
ON COMMIT PRESERVE ROWS
;
COLLECT STATS
     PRIMARY INDEX (sku_idnt)
    ON fanatics;





CREATE MULTISET VOLATILE TABLE last_receipt
AS (
	SELECT
		 r.sku_idnt
		,r.loc_idnt
		,d.day_date as last_receipt_date
	FROM prd_ma_bado.rcpt_day_sku_loc_lkup r
	JOIN prd_nap_usr_vws.day_cal_454_dim d
	  ON r.rcpt_po_day_idnt_max = d.day_idnt
)
WITH DATA AND STATS
PRIMARY INDEX (sku_idnt, loc_idnt)
ON COMMIT preserve rows;

COLLECT STATS
     PRIMARY INDEX (sku_idnt, loc_idnt)
    ON last_receipt;


  --DROP TABLE oo_sku
CREATE MULTISET VOLATILE TABLE oo_sku
AS (
   SELECT week_idnt
   		, week_end_day_date
   		, rms_sku_num as sku_idnt
   		,  country
		,  banner
		, channel
   		, SUM(quantity_open) oo_units
   		, SUM(total_anticipated_retail_amt) oo_dollars
   FROM PRD_NAP_USR_VWS.MERCH_ON_ORDER_FACT_VW oo
   JOIN loc st
   	 ON oo.store_num = st.store_num
   JOIN dates cal
     ON oo.week_num = cal.week_idnt
   WHERE quantity_open > 0
     and first_approval_date IS NOT NULL
   GROUP BY 1,2,3,4,5,6
)
WITH DATA AND STATS
PRIMARY INDEX (sku_idnt, week_idnt, channel)
ON COMMIT preserve rows;

COLLECT STATS
     PRIMARY INDEX (sku_idnt, week_idnt, channel)
    ON oo_sku;




--Grab metrics from the price type table in NAP and roll up to week - for flags that are at the day level
-- take the max value for the week (i.e. if a sku was on dropship at any day during the week it is flagged
-- as dropship for the entire week)
CREATE MULTISET VOLATILE TABLE pricetype_sku
AS (
	 SELECT
	 	cal.day_date
	    ,cal.week_idnt
	    ,cal.week_desc
	    ,cal.week_end_day_date
	    ,cal.month_end_week_idnt
	    ,cal.month_idnt
	    ,cal.quarter_idnt
	    ,cal.fiscal_year_num
	    ,sld.sku_idnt
	    ,price_type
	    ,country
	    ,banner
	    ,channel
	   -- ,sld.loc_idnt
	    ,MAX(replenishment_flag) AS rp_flag
	    ,MAX(dropship_flag) AS dropship_flag
	    ,SUM(sales_units) AS net_sales_units
	    ,SUM(sales_dollars) AS net_sales_dollars
	    ,SUM(return_units) AS return_units
	    ,SUM(return_dollars) AS return_dollars
	    ,SUM(demand_units) AS demand_units
	    ,SUM(demand_dollars) AS demand_dollars
	    ,SUm(demand_cancel_units) AS demand_cancel_units
	    ,SUM(demand_cancel_dollars) AS demand_cancel_dollars
	    ,SUM(demand_dropship_units) AS demand_dropship_units
	    ,SUM(demand_dropship_dollars) AS demand_dropship_dollars
	    ,SUM(shipped_units) AS shipped_units
	    ,SUM(shipped_dollars) AS shipped_dollars
	    ,SUM(store_fulfill_units) AS store_fulfill_units
	    ,SUM(store_fulfill_dollars) AS store_fulfill_dollars
	    ,SUM(eoh_units) AS eoh_units
	    ,SUM(eoh_dollars) AS eoh_dollars
	    ,SUM(eoh_units * weighted_average_cost) AS eoh_cost
	    ,SUM(boh_units) AS boh_units
	    ,SUM(boh_dollars) AS boh_dollars
	    ,SUM(boh_units * weighted_average_cost) AS boh_cost
	    ,SUM(nonsellable_units) AS nonsellable_units
	    ,SUM(cost_of_goods_sold) as cost_of_goods_sold
		  ,SUM(gross_margin) as product_margin
	    ,CAST(0 AS DECIMAL(10,0)) AS receipt_units
	    ,CAST(0 AS DECIMAL(12,2)) AS receipt_dollars
	    ,CAST(0 AS DECIMAL(10,0)) AS reserve_stock_units
	    ,CAST(0 AS DECIMAL(12,2)) AS reserve_stock_dollars
	    ,CAST(0 AS DECIMAL(10,0)) AS receipt_dropship_units
	    ,CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_dollars
	    ,CAST(0 AS DECIMAL(10,0)) AS backorder_units
	    ,CAST(0 AS DECIMAL(12,2)) AS backorder_dollars
	    ,MAX(last_receipt_date) as last_receipt_date
	FROM t2dl_das_ace_mfp.sku_loc_pricetype_day sld
	JOIN loc st
	  ON sld.loc_idnt = st.store_num
	JOIN dates cal
	  ON sld.day_dt = cal.day_date
	LEFT JOIN replenishment r
	  ON sld.sku_idnt = r.sku_idnt
	 AND sld.loc_idnt = r.loc_idnt
	 AND cal.day_idnt = r.day_idnt
	LEFT JOIN dropship ds
	  ON ds.day_dt = cal.day_date
	 AND sld.sku_idnt = ds.sku_idnt
	LEFT JOIN last_receipt lr
	  ON lr.sku_idnt = sld.sku_idnt
	 AND lr.loc_idnt = sld.loc_idnt
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,39,40,41,42,43,44,45,46
)
WITH DATA
PRIMARY INDEX (day_date, week_idnt, sku_idnt, channel, country)
ON COMMIT preserve rows;

COLLECT STATS
     PRIMARY INDEX (day_date, week_idnt, sku_idnt, channel, country)
    ON pricetype_sku;


--Getting receipts - still pulling these out of MADM until receipts are stable in NAP

CREATE MULTISET VOLATILE TABLE receipt_sku
AS (
	SELECT
		cal.day_date
	    ,cal.week_idnt
	    ,cal.week_desc
		,cal.week_end_day_date
		,cal.month_end_week_idnt
		,cal.month_idnt
		,cal.quarter_idnt
	 	,cal.fiscal_year_num
	    ,sld.sku_idnt
	    ,CASE WHEN sld.clrc_ind = 'Y' THEN 'C' ELSE 'R' END AS price_type
	    ,country
	    ,banner
	    ,channel
	    --,CAST(sld.loc_idnt as INTEGER) AS loc_idnt
	    ,MAX(replenishment_flag) AS rp_flag
	    ,MAX(dropship_flag) AS dropship_flag
	    ,CAST(0 AS DECIMAL(10,0)) AS net_sales_units
	    ,CAST(0 AS DECIMAL(12,2)) AS net_sales_dollars
	    ,CAST(0 AS DECIMAL(10,0)) AS return_units
	    ,CAST(0 AS DECIMAL(12,2)) AS return_dollars
	    ,CAST(0 AS DECIMAL(10,0)) AS demand_units
	    ,CAST(0 AS DECIMAL(12,2)) AS demand_dollars
	    ,CAST(0 AS DECIMAL(10,0)) AS demand_cancel_units
	    ,CAST(0 AS DECIMAL(12,2)) AS demand_cancel_dollars
	    ,CAST(0 AS DECIMAL(10,0)) AS demand_dropship_units
	    ,CAST(0 AS DECIMAL(12,2)) AS demand_dropship_dollars
	    ,CAST(0 AS DECIMAL(10,0)) AS shipped_units
	    ,CAST(0 AS DECIMAL(12,2)) AS shipped_dollars
	    ,CAST(0 AS DECIMAL(10,0)) AS store_fulfill_units
	    ,CAST(0 AS DECIMAL(12,2)) AS store_fulfill_dollars
	    ,CAST(0 AS DECIMAL(10,0)) AS eoh_units
	    ,CAST(0 AS DECIMAL(12,2)) AS eoh_dollars
	    ,CAST(0 AS DECIMAL(12,2)) AS eoh_cost
	    ,CAST(0 AS DECIMAL(10,0)) AS boh_units
	    ,CAST(0 AS DECIMAL(12,2)) AS boh_dollars
	    ,CAST(0 AS DECIMAL(12,2)) AS boh_cost
	    ,CAST(0 AS DECIMAL(10,0)) AS nonsellable_units
	    ,CAST(0 AS DECIMAL(12,2)) AS cost_of_goods_sold
	    ,CAST(0 AS DECIMAL(12,2)) AS product_margin
	    ,SUM(CASE WHEN rcpt_typ_key IN (1, 2, 11, 7, 8) THEN rcpt_tot_units ELSE 0 END) AS receipt_units
	    ,SUM(CASE WHEN rcpt_typ_key IN (1, 2, 11, 7, 8) THEN rcpt_tot_retl$ ELSE 0 END) AS receipt_dollars
	    ,SUM(CASE WHEN rcpt_typ_key IN (7,8) THEN rcpt_tot_units ELSE 0 END) as reserve_stock_units
	    ,SUM(CASE WHEN rcpt_typ_key IN (7,8) THEN rcpt_tot_retl$ ELSE 0 END) as reserve_stock_dollars
	    ,SUM(CASE WHEN drsh_ind ='Y' AND rcpt_typ_key IN (1, 2, 11, 7, 8) THEN rcpt_tot_units ELSE 0 END) AS receipt_dropship_units
		,SUM(CASE WHEN drsh_ind ='Y' AND rcpt_typ_key IN (1, 2, 11, 7, 8) THEN rcpt_tot_retl$ ELSE 0 END) AS receipt_dropship_dollars
		,CAST(0 AS DECIMAL(10,0)) AS backorder_units
	    ,CAST(0 AS DECIMAL(12,2)) AS backorder_dollars
	    ,MAX(last_receipt_date) as last_receipt_date
	FROM prd_ma_bado.rcpt_sku_ld_rvw sld
	JOIN loc st
	  ON sld.loc_idnt = st.store_num
	JOIN dates cal
	  ON sld.DAY_IDNT = cal.day_idnt
	LEFT JOIN replenishment r
	  ON sld.sku_idnt = r.sku_idnt
	 AND sld.loc_idnt = r.loc_idnt
	 AND cal.day_idnt = r.day_idnt
	LEFT JOIN dropship ds
	  ON ds.day_dt = cal.day_date
	 AND sld.sku_idnt = ds.sku_idnt
	LEFT JOIN last_receipt lr
	  ON lr.sku_idnt = sld.sku_idnt
	 AND lr.loc_idnt = sld.loc_idnt
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,45,46
)
WITH DATA
PRIMARY INDEX (day_date, week_idnt, sku_idnt, channel, country)
ON COMMIT preserve rows;
COLLECT STATS
     PRIMARY INDEX (day_date, week_idnt, sku_idnt, channel, country)
    ON receipt_sku;

-- Getting Backorder information
CREATE MULTISET VOLATILE TABLE backorder_sku
AS (
	SELECT
		cal.day_date
	    , cal.week_idnt
	    , cal.week_desc
		, cal.week_end_day_date
		, cal.month_end_week_idnt
		, cal.month_idnt
		, cal.quarter_idnt
	 	, cal.fiscal_year_num
	    , bo.RMS_SKU_NUM as sku_idnt
	    , bo.PRICE_TYPE  AS price_type
	    , country
	    , banner
	    , channel
	   --, CAST(bo.store_num AS INTEGER) as loc_idnt
	    ,MAX(replenishment_flag) AS rp_flag
	    ,MAX(dropship_flag) AS dropship_flag
	    ,CAST(0 AS DECIMAL(10,0)) AS net_sales_units
	    ,CAST(0 AS DECIMAL(12,2)) AS net_sales_dollars
	    ,CAST(0 AS DECIMAL(10,0)) AS return_units
	    ,CAST(0 AS DECIMAL(12,2)) AS return_dollars
	    ,CAST(0 AS DECIMAL(10,0)) AS demand_units
	    ,CAST(0 AS DECIMAL(12,2)) AS demand_dollars
	    ,CAST(0 AS DECIMAL(10,0)) AS demand_cancel_units
	    ,CAST(0 AS DECIMAL(12,2)) AS demand_cancel_dollars
	    ,CAST(0 AS DECIMAL(10,0)) AS demand_dropship_units
	    ,CAST(0 AS DECIMAL(12,2)) AS demand_dropship_dollars
	    ,CAST(0 AS DECIMAL(10,0)) AS shipped_units
	    ,CAST(0 AS DECIMAL(12,2)) AS shipped_dollars
	    ,CAST(0 AS DECIMAL(10,0)) AS store_fulfill_units
	    ,CAST(0 AS DECIMAL(12,2)) AS store_fulfill_dollars
	    ,CAST(0 AS DECIMAL(10,0)) AS eoh_units
	    ,CAST(0 AS DECIMAL(12,2)) AS eoh_dollars
	    ,CAST(0 AS DECIMAL(12,2)) AS eoh_cost
	    ,CAST(0 AS DECIMAL(10,0)) AS boh_units
	    ,CAST(0 AS DECIMAL(12,2)) AS boh_dollars
	    ,CAST(0 AS DECIMAL(12,2)) AS boh_cost
	    ,CAST(0 AS DECIMAL(10,0)) AS nonsellable_units
	    ,CAST(0 AS DECIMAL(12,2)) AS cost_of_goods_sold
	    ,CAST(0 AS DECIMAL(12,2)) AS product_margin
	    ,CAST(0 AS DECIMAL(10,0)) AS receipt_units
	    ,CAST(0 AS DECIMAL(12,2)) AS receipt_dollars
	    ,CAST(0 AS DECIMAL(10,0)) AS reserve_stock_units
	    ,CAST(0 AS DECIMAL(12,2)) AS reserve_stock_dollars
	    ,CAST(0 AS DECIMAL(10,0)) AS receipt_dropship_units
	    ,CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_dollars
	    ,SUM(backorder_units) AS backorder_units
	    ,SUM(backorder_dollars) AS backorder_dollars
	    ,MAX(last_receipt_date) as last_receipt_date
	FROM T2DL_DAS_ACE_MFP.SKU_BO_HISTORY bo
	JOIN loc st
	  ON bo.store_num = st.store_num
	JOIN dates cal
	  ON bo.order_date_pacific = cal.day_date
	LEFT JOIN replenishment r
	  ON bo.rms_sku_num = r.sku_idnt
	 AND bo.store_num = r.loc_idnt
	 AND cal.day_idnt = r.day_idnt
	LEFT JOIN dropship ds
	  ON ds.day_dt = cal.day_date
	 AND bo.RMS_SKU_NUM  = ds.sku_idnt
	LEFT JOIN last_receipt lr
	  ON lr.sku_idnt = bo.RMS_SKU_NUM
	 AND lr.loc_idnt = bo.STORE_NUM
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44

)
WITH DATA
PRIMARY INDEX (day_date, week_idnt, sku_idnt, channel, country)
ON COMMIT preserve rows;

COLLECT STATS
     PRIMARY INDEX (day_date, week_idnt, sku_idnt, channel, country)
    ON backorder_sku;


CREATE MULTISET VOLATILE TABLE instock_sku
AS (
			Select tw.day_date,
			tw.country
			, loc.banner
			, loc.channel
			, tw.rms_sku_num as sku_idnt
			, SUM(NULLIF(allocated_traffic,0)) as traffic
			, SUM(case when loc_instock_ind = 1 then allocated_traffic ELSE 0 END) as instock
		FROM T2DL_DAS_TWIST.twist_daily tw
		join loc on tw.store_num = loc.store_num
		join dates on dates.day_date = tw.day_date
		GROUP BY 1,2,3,4,5



)
WITH DATA
PRIMARY INDEX (day_date, sku_idnt, channel, country)
ON COMMIT preserve rows;

COLLECT STATS
     PRIMARY INDEX (day_date, sku_idnt, channel, country)
    ON instock_sku;


   CREATE MULTISET VOLATILE TABLE digital_funnel
AS (
	SELECT
		cal.day_date
	    , CASE WHEN channel = 'FULL_LINE' AND channelcountry = 'US' THEN '120, N.COM'
	        WHEN channel = 'FULL_LINE' AND channelcountry = 'CA' THEN '121, N.CA'
	        WHEN channel = 'RACK' THEN '250, OFFPRICE ONLINE'
	        END AS channel
	    , channelcountry AS country
	    , CASE WHEN channel = 'FULL_LINE' AND channelcountry = 'US' THEN '120'
	        WHEN channel = 'FULL_LINE' AND channelcountry = 'CA' THEN '121'
	        WHEN channel = 'RACK' THEN '250'
	        END AS channel_idnt
	    , CASE WHEN channel = 'FULL_LINE' THEN 'FP'
	        WHEN channel = 'RACK' THEN 'OP'
	        END AS banner
		,f.rms_sku_num AS sku_idnt
	    ,SUM(order_sessions) AS order_sessions
	    ,SUM(add_to_bag_quantity) AS add_qty
	    ,SUM(product_views) AS product_views
	    ,SUM(product_views) * SUM(pct_instock) AS instock_views
		,SUM(order_demand) as order_demand
		,SUM(order_quantity) as order_quantity

	    GROUP BY 1,2,3,4,5,6
	FROM t2dl_das_product_funnel.product_price_funnel_daily f
	JOIN dates cal
	  ON f.event_date_pacific = cal.day_date
	  WHERE channel <> 'UNKNOWN'

)
WITH DATA
PRIMARY INDEX (day_date, sku_idnt, channel_idnt, country)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (day_date, sku_idnt, channel_idnt, country)
	ON digital_funnel;

CREATE MULTISET VOLATILE TABLE udig_sku
AS (
SELECT
    rnk.sku_idnt
    ,CASE
        WHEN udig_rank = 1 THEN '1, HOL22_N_CYBER_SPECIAL PURCHASE'
        WHEN udig_rank = 2 THEN '1, HOL22_NR_CYBER_SPECIAL PURCHASE'
        WHEN udig_rank = 3 THEN '2, CYBER 2022_US'
        WHEN udig_rank = 4 THEN '6, STOCKING STUFFERS 2022_US'
        WHEN udig_rank = 5 THEN '1, GIFTS & GLAM 2022_US'
        WHEN udig_rank = 6 THEN '3, HOLIDAY TREATMENT 2022_US'
        WHEN udig_rank = 7 THEN '4, PALETTES 2022_US'
        WHEN udig_rank = 8 THEN '5, HOLIDAY FRAGRANCE 2022_US'
        WHEN udig_rank = 9 THEN '7, FRAGRANCE STOCKING STUFFERS 2022_US'

        END AS udig
FROM
    (SELECT
        sku_idnt
        ,MIN(CASE
            WHEN item_group = '1, HOL22_N_CYBER_SPECIAL PURCHASE' THEN 1
            WHEN item_group = '1, HOL22_NR_CYBER_SPECIAL PURCHASE' THEN 2
            WHEN item_group = '2, CYBER 2022_US' THEN 3
            WHEN item_group = '6, STOCKING STUFFERS 2022_US' THEN 4
            WHEN item_group = '1, GIFTS & GLAM 2022_US' THEN 5
            WHEN item_group = '3, HOLIDAY TREATMENT 2022_US' THEN 6
            WHEN item_group = '4, PALETTES 2022_US' THEN 7
            WHEN item_group = '5, HOLIDAY FRAGRANCE 2022_US' THEN 8
            WHEN item_group = '7, FRAGRANCE STOCKING STUFFERS 2022_US' THEN 9
            END) AS udig_rank
        FROM
            (
            SELECT DISTINCT
                sku_idnt
                ,udig_itm_grp_idnt || ', ' || udig_itm_grp_nm AS item_group
            FROM PRD_USR_VWS.UDIG_ITM_GRP_SKU_LKUP
            WHERE UDIG_COLCTN_NM IN
                ('HOLIDAY 2022_RACK', 'HOLIDAY 2022_NORDSTROM', 'US BEAUTY 22 HOLIDAY REPORTING')
            ) item_group
        GROUP BY 1
    ) rnk
)
WITH DATA
PRIMARY INDEX (sku_idnt)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (sku_idnt)
	ON udig_sku;


--combine all metrics and add in new / cf flag, fanatics flag, and los flag
DELETE FROM {environment_schema}.item_performance_daily_base{env_suffix} WHERE day_date BETWEEN {start_date} and {end_date};
INSERT INTO {environment_schema}.item_performance_daily_base{env_suffix}
	SELECT
	  s.day_date
	  	,CAST(s.week_idnt as INTEGER) as week_idnt
	  	,week_desc
	    ,CAST(s.month_idnt as INTEGER) as month_idnt
	    ,CAST(s.quarter_idnt as INTEGER) as quarter_idnt
	    ,CAST(s.fiscal_year_num as INTEGER) as year_idnt
	    ,CAST(s.month_end_week_idnt as INTEGER) as month_end_week_idnt
        ,s.week_end_day_date
	    ,s.sku_idnt
	    ,s.country
	    ,s.banner
	    ,s.channel
	    ,price_type
		,CASE WHEN f.sku_idnt IS NULL then 0 else 1 end as fanatics_flag
	    ,CASE WHEN cc_type = 'NEW' then 1 ELSE 0 END AS new_flag
		,CASE WHEN cc_type = 'CF' then 1 ELSE 0 END AS cf_flag
		,days_live
		,CAST(l.first_day_published as DATE) as days_published
	    ,rp_flag AS rp_flag
	    ,dropship_flag AS dropship_flag
		,udig
	     -- Sales
	    ,COALESCE(s.net_sales_units,0) AS net_sales_units
	    ,COALESCE(s.net_sales_dollars,0) AS net_sales_dollars
	    ,COALESCE(s.return_units,0) AS return_units
	    ,COALESCE(s.return_dollars,0) AS return_dollars
	    ,COALESCE(demand_units,0) AS demand_units
	    ,COALESCE(demand_dollars,0) AS demand_dollars
	    ,COALESCE(demand_cancel_units,0) AS demand_cancel_units
	    ,COALESCE(demand_cancel_dollars,0) AS demand_cancel_dollars
	    ,COALESCE(demand_dropship_units,0) AS demand_dropship_units
	    ,COALESCE(demand_dropship_dollars,0) AS demand_dropship_dollars
	    ,COALESCE(shipped_units,0) AS shipped_units
	    ,COALESCE(shipped_dollars,0) AS shipped_dollars
	    ,COALESCE(store_fulfill_units,0) AS store_fulfill_units
	    ,COALESCE(store_fulfill_dollars,0) AS store_fulfill_dollars
	    -- inventory
	    ,COALESCE(boh_units,0) AS boh_units
	    ,COALESCE(boh_dollars,0) AS boh_dollars
	    ,COALESCE(boh_cost,0) AS boh_cost
	    ,COALESCE(eoh_units,0) AS eoh_units
	    ,COALESCE(eoh_dollars,0) AS eoh_dollars
	    ,COALESCE(eoh_cost,0) AS eoh_cost
	    ,COALESCE(nonsellable_units,0) AS nonsellable_units
	    -- receipts
	    ,COALESCE(receipt_units,0) AS receipt_units
	    ,COALESCE(receipt_dollars,0) AS receipt_dollars
	    ,COALESCE(reserve_stock_units,0) as reserve_stock_units
	    ,COALESCE(reserve_stock_dollars,0) as reserve_stock_dollars
	    ,COALESCE(receipt_dropship_units,0) as receipt_dropship_units
	    ,COALESCE(receipt_dropship_dollars,0) as receipt_dropship_dollars
	    ,last_receipt_date AS last_receipt_date
	    -- OO
	    ,COALESCE(oo.oo_units,0) as oo_units
	    ,COALESCE(oo.oo_dollars,0) as oo_dollars
	    -- digital metrics
	    ,COALESCE(add_qty,0) AS add_qty
	    ,COALESCE(product_views,0) AS product_views
	    ,COALESCE(instock_views,0) AS instock_views
	    ,COALESCE(order_demand,0) as order_demand
	    ,COALESCE(order_quantity,0) as order_quantity

	    --cost_metrics
	    ,COALESCE(cost_of_goods_sold,0) as cost_of_goods_sold
	    ,COALESCE(product_margin,0)as product_margin
		,COALESCE(CASE WHEN cost_of_goods_sold is not null then net_sales_dollars else 0 end, 0) as sales_pm
	    --backorder metrics
	    ,COALESCE(backorder_units,0) as backorder_units
	    ,COALESCE(backorder_dollars,0) as backorder_dollars
	    --instock metrics
	    ,COALESCE(traffic,0) as traffic
	    ,COALESCE(instock,0) as instock_traffic
	    FROM  (
		SELECT * FROM pricetype_sku s
			UNION ALL
		SELECT * FROM receipt_sku receipts
			UNION ALL
		SELECT * FROM backorder_sku

		) s

	 LEFT JOIN new_cf_lkp ncf
	 	ON s.month_idnt = ncf.month_idnt
	 	AND s.sku_idnt = ncf.sku_idnt
	 	and s.country = ncf.country
	 	and s.banner = ncf.banner
	 	and s.channel = ncf.channel
	  LEFT JOIN digital_funnel df
	  	ON s.sku_idnt = df.sku_idnt
	  	AND s.day_date = df.day_date
	  	AND s.country = df.country
	  	and s.banner = df.banner
	  	and s.channel = df.channel
	  LEFT JOIN oo_sku oo
	  	ON  s.day_date = oo.week_end_day_date
	  	AND s.sku_idnt = oo.sku_idnt
	  	AND s.country = oo.country
	  	AND s.channel = oo.channel
	  	and s.banner = oo.banner
	  LEFT JOIN fanatics f ON s.sku_idnt = f.sku_idnt
	  LEFT JOIN live_on_site l ON s.sku_idnt = l.sku_id
	  and s.country = l.country
	  and s.banner = l.banner
	  and s.day_date = l.day_date
	  and s.channel = l.channel
	LEFT JOIN instock_sku i ON s.sku_idnt = i.sku_idnt
	and s.day_date = i.day_date
	and s.channel = i.channel
	and s.country = i.country
	and s.banner = i.banner
	LEFT JOIN udig_sku u ON u.sku_idnt = s.sku_idnt;
COLLECT STATS
     PRIMARY INDEX(day_date, country, channel, sku_idnt)
     ON {environment_schema}.item_performance_daily_base{env_suffix};
