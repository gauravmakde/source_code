--DROP TABLE dates;
CREATE MULTISET VOLATILE TABLE dates AS (
	SELECT
		 dt.day_date
		,dt.day_idnt
		,dt.week_idnt
		,dt.week_end_day_date
		,dt.fiscal_year_num AS year_idnt
	FROM prd_nap_usr_vws.day_cal_454_dim dt
	WHERE dt.week_idnt 
		BETWEEN (SELECT DISTINCT week_idnt FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = {start_date}) --Comment out for testing or backfill
		AND (SELECT MAX(week_idnt) FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date < CURRENT_DATE) --Comment out for testing or backfill
--		BETWEEN 202403 AND 202404 --Comment in for testing or backfill timeframes
) WITH DATA PRIMARY INDEX(day_idnt) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (day_idnt)
	,COLUMN (day_date               )
	,COLUMN (day_idnt               )
	,COLUMN (week_idnt              )
	,COLUMN (year_idnt              )
	,COLUMN (day_idnt, week_idnt)
		ON dates
;

--DROP TABLE locations;
CREATE MULTISET VOLATILE TABLE locations AS (
SELECT DISTINCT
	store_num
	,TRIM(TRIM(channel_num) || ', ' ||TRIM(channel_desc)) AS chnl_label
    ,CAST(channel_num AS CHAR(3)) AS chnl_idnt
    ,channel_country AS country
	,CASE
	   WHEN channel_brand = 'NORDSTROM' THEN 'FP'
	   WHEN channel_brand = 'NORDSTROM_RACK' THEN 'OP'
	   END AS banner
FROM prd_nap_usr_vws.price_store_dim_vw
WHERE channel_num IN (110, 111, 120, 121, 130, 140, 210, 211, 220, 221, 250, 260, 261, 310, 311)
	AND (store_close_date IS NULL OR store_close_date >= (SELECT MIN(day_date) FROM dates))
) WITH DATA PRIMARY INDEX(store_num) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (store_num)
	,COLUMN (store_num, chnl_idnt)
	,COLUMN (chnl_label, chnl_idnt, country, banner)
		ON locations
;

/******************* DROPSHIP ********************/
	
--DROP TABLE dropship_lkp;
CREATE MULTISET VOLATILE TABLE dropship_lkp AS (
SELECT DISTINCT
	dt.day_date
    ,ds.rms_sku_id AS sku_idnt
FROM prd_nap_usr_vws.inventory_stock_quantity_by_day_fact ds
INNER JOIN dates dt
	ON ds.snapshot_date = dt.day_date
WHERE ds.location_type IN ('DS', 'DS_OP')
) WITH DATA PRIMARY INDEX(sku_idnt, day_date)
PARTITION BY RANGE_N(day_date BETWEEN '2019-01-01' AND '2025-12-31')
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (sku_idnt, day_date)
	,COLUMN(PARTITION)
		ON dropship_lkp;


--DROP TABLE merch_staging;
CREATE MULTISET VOLATILE TABLE merch_staging AS (
	SELECT
		  dt.week_idnt
		, dt.week_end_day_date
		, dt.day_idnt
		, base.day_dt
		, base.sku_idnt
		, base.loc_idnt
		, locs.chnl_label
		, locs.chnl_idnt
		, locs.country
		, locs.banner
		, base.price_type
		, CASE WHEN rp.rms_sku_num IS NOT NULL THEN 1 ELSE 0 END AS rp_ind
		, CASE
	    	WHEN ds.sku_idnt IS NOT NULL THEN 1
	    	WHEN base.demand_dropship_units > 0 THEN 1
	    	ELSE 0 END AS dropship_ind
		, base.sales_units
		, base.sales_dollars
		, base.return_units
		, base.return_dollars
		, base.demand_units
		, base.demand_dollars
		, base.demand_cancel_units
		, base.demand_cancel_dollars
		, base.demand_dropship_units
		, base.demand_dropship_dollars
		, base.shipped_units
		, base.shipped_dollars
		, base.store_fulfill_units
		, base.store_fulfill_dollars
		, CASE WHEN base.day_dt = dt.week_end_day_date     THEN base.eoh_units          												END AS eoh_units
		, CASE WHEN base.day_dt = dt.week_end_day_date     THEN base.eoh_dollars        												END AS eoh_dollars
		, CASE WHEN base.day_dt = dt.week_end_day_date     THEN base.eoh_units * base.weighted_average_cost   END AS eoh_cost
		, CASE WHEN base.day_dt = dt.week_end_day_date     THEN base.nonsellable_units  												END AS nonsellable_units
		-- cost
		, base.cost_of_goods_sold
		, base.gross_margin AS product_margin
		, CASE WHEN base.cost_of_goods_sold IS NOT NULL THEN base.sales_dollars ELSE NULL END AS sales_pm
	FROM t2dl_das_ace_mfp.sku_loc_pricetype_day_vw base
	--dates
	INNER JOIN dates dt
		ON base.day_dt = dt.day_date
    -- dropship indicator
	LEFT JOIN dropship_lkp ds
	    ON base.sku_idnt = ds.sku_idnt
	    AND base.day_dt = ds.day_date
	-- locations
	INNER JOIN locations locs
		ON base.loc_idnt = locs.store_num
	-- RP indicator
    LEFT JOIN prd_nap_usr_vws.merch_rp_sku_loc_dim_hist rp
		ON base.sku_idnt = rp.rms_sku_num
    	AND base.loc_idnt = rp.location_num
        AND rp.rp_period CONTAINS base.day_dt
	WHERE base.day_dt BETWEEN (SELECT MIN(day_date) FROM dates) AND (SELECT MAX(day_date) FROM dates)
) WITH DATA PRIMARY INDEX(day_idnt, sku_idnt, loc_idnt) ON COMMIT PRESERVE ROWS
;


COLLECT STATS
	 PRIMARY INDEX (day_idnt, sku_idnt, loc_idnt)
	,COLUMN (week_idnt)
	,COLUMN (day_idnt, sku_idnt, loc_idnt)
	,COLUMN (day_idnt)
	,COLUMN (loc_idnt)
	,COLUMN (week_idnt, sku_idnt, chnl_label, chnl_idnt, country, banner, price_type)
		ON merch_staging
;

--DROP TABLE merch_insert;
CREATE MULTISET VOLATILE TABLE merch_insert AS (
	SELECT
		  week_idnt
		, week_end_day_date
		, chnl_label
		, chnl_idnt
		, country
		, banner
		, sku_idnt
		, price_type
	    , dropship_ind
        , rp_ind
		, SUM(base.sales_units                     ) AS sales_units
		, SUM(base.sales_dollars                   ) AS sales_dollars
		, SUM(base.return_units                    ) AS return_units
		, SUM(base.return_dollars                  ) AS return_dollars
		, SUM(base.demand_units                    ) AS demand_units
		, SUM(base.demand_dollars                  ) AS demand_dollars
		, SUM(base.demand_cancel_units             ) AS demand_cancel_units
		, SUM(base.demand_cancel_dollars           ) AS demand_cancel_dollars
		, SUM(base.demand_dropship_units           ) AS demand_dropship_units
		, SUM(base.demand_dropship_dollars         ) AS demand_dropship_dollars
		, SUM(base.shipped_units                   ) AS shipped_units
		, SUM(base.shipped_dollars                 ) AS shipped_dollars
		, SUM(base.store_fulfill_units             ) AS store_fulfill_units
		, SUM(base.store_fulfill_dollars           ) AS store_fulfill_dollars
		, SUM(base.eoh_units                       ) AS eoh_units
		, SUM(base.eoh_dollars                     ) AS eoh_dollars
		, SUM(base.eoh_cost                        ) AS eoh_cost
		, SUM(base.nonsellable_units               ) AS nonsellable_units
		-- cost
		, SUM(cost_of_goods_sold                   ) AS cost_of_goods_sold
		, SUM(product_margin                       ) AS product_margin
		, SUM(sales_pm                             ) AS sales_pm
		, MAX(CURRENT_TIMESTAMP                    ) AS update_timestamp
	FROM merch_staging base
	GROUP BY 1,2,3,4,5,6,7,8,9,10
) WITH DATA PRIMARY INDEX(chnl_idnt, week_idnt, sku_idnt) ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS
     COLUMN (week_idnt ,chnl_label ,chnl_idnt,country ,banner ,price_type ,dropship_ind ,rp_ind)
    ,COLUMN(sku_idnt)
    ON merch_insert
;

DELETE 
FROM {environment_schema}.wbr_merch_staging{env_suffix} 
WHERE week_idnt 
    BETWEEN (SELECT MIN(week_idnt) FROM merch_insert) 
    AND (SELECT MAX(week_idnt) FROM merch_insert)
;

INSERT INTO {environment_schema}.wbr_merch_staging{env_suffix}
SELECT * FROM merch_insert
;

COLLECT STATS
	 PRIMARY INDEX (week_idnt ,chnl_idnt ,sku_idnt)
	,COLUMN (week_idnt)
    ,COLUMN (sku_idnt)
	,COLUMN (week_idnt, sku_idnt, chnl_label, chnl_idnt, country, banner, price_type)
    ,COLUMN (week_idnt ,chnl_label ,chnl_idnt,country ,banner ,price_type ,dropship_ind ,rp_ind)
		ON {environment_schema}.wbr_merch_staging{env_suffix}
;
