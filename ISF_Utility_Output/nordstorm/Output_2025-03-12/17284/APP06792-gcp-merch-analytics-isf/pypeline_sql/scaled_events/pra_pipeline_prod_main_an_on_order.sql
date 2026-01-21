-- Inventory Staging Table

CREATE MULTISET VOLATILE TABLE INV_STG
AS (
	SELECT
		rms_sku_id AS sku_idnt
		, snapshot_date
		, price_store_num
		, location_id AS loc_idnt
		, sum(stock_on_hand_qty) AS soh_qty
		, sum(in_transit_qty) AS in_transit_qty
	FROM PRD_NAP_USR_VWS.INVENTORY_STOCK_QUANTITY_BY_DAY_PHYSICAL_FACT inv
	INNER JOIN (
	   SELECT DISTINCT store_num, price_store_num
	   FROM PRD_NAP_USR_VWS.PRICE_STORE_DIM_VW
	   WHERE channel_num in (110,120,210,250)
	) loc
       ON inv.location_id = loc.store_num
    GROUP BY 1,2,3,4
	WHERE inv.snapshot_date = CURRENT_DATE - 1
	AND stock_on_hand_qty + in_transit_qty > 0
)
WITH DATA
PRIMARY INDEX ( sku_idnt, loc_idnt, snapshot_date)
ON COMMIT PRESERVE ROWS;


COLLECT STATS
    PRIMARY INDEX (sku_idnt, loc_idnt, snapshot_date)
    ,COLUMN (sku_idnt)
    ,COLUMN (loc_idnt)
    ,COLUMN (snapshot_date)
        ON INV_STG;


delete from {environment_schema}.sku_loc_daily_se_an_on_order all;
insert into {environment_schema}.sku_loc_daily_se_an_on_order
    SELECT
        sub.sku_idnt
        , sub.loc_idnt
        , day_date
        , SUM(COALESCE(sub.on_order_retail, 0)) AS an_on_order_retail
        , SUM(COALESCE(sub.on_order_cost, 0)) AS an_on_order_cost
        , SUM(COALESCE(sub.on_order_units, 0)) AS an_on_order_units
				, SUM(COALESCE(sub.oo_ordered_retail, 0)) AS an_total_ordered_oo_retail
				, SUM(COALESCE(sub.oo_ordered_cost, 0)) AS an_total_ordered_oo_cost
				, SUM(COALESCE(sub.oo_ordered, 0)) AS an_total_ordered_oo_units
				, SUM(COALESCE(sub.oo_canceled, 0)) AS an_total_canceled_oo_units
        , SUM(COALESCE(sub.in_transit_retail, 0)) AS an_in_transit_retail
        , SUM(COALESCE(sub.in_transit_cost, 0)) AS an_in_transit_cost
        , SUM(COALESCE(sub.in_transit_units, 0)) AS an_in_transit_units
        , SUM(COALESCE(sub.eoh_retail, 0)) AS eoh_retail
        , SUM(COALESCE(sub.eoh_cost, 0)) AS eoh_cost
        , SUM(COALESCE(sub.eoh_units, 0)) AS eoh_units
		, SUM(COALESCE(sub.receipts_retail, 0)) AS an_receipts_retail
		, SUM(COALESCE(sub.receipts_cost, 0)) AS an_receipts_cost
		, SUM(COALESCE(sub.receipts_units, 0)) AS an_receipts_units
        , CURRENT_TIMESTAMP AS process_tmstp
	FROM (
		SELECT -- OO
	            oo.rms_sku_num AS sku_idnt
	            , CAST(oo.store_num AS VARCHAR(20)) AS loc_idnt
              , CURRENT_DATE - 1 AS day_date
	            , SUM(oo.ANTICIPATED_RETAIL_AMT * oo.QUANTITY_OPEN) AS on_order_retail
	            , SUM(oo.UNIT_ESTIMATED_LANDING_COST  * oo.QUANTITY_OPEN) AS on_order_cost
	            , SUM(oo.quantity_open) AS on_order_units
							, SUM(oo.ANTICIPATED_RETAIL_AMT * oo.QUANTITY_ORDERED) AS oo_ordered_retail
							, SUM(oo.UNIT_ESTIMATED_LANDING_COST  * oo.QUANTITY_ORDERED) AS oo_ordered_cost
							, SUM(oo.quantity_ordered) AS oo_ordered
							, SUM(oo.quantity_canceled) AS oo_canceled
	            , CAST(0 AS DECIMAL(38,2)) AS eoh_retail
	            , CAST(0 AS DECIMAL(38,2)) AS eoh_cost
	            , CAST(0 AS INTEGER) AS eoh_units
	            , CAST(0 AS DECIMAL(38,2)) AS in_transit_retail
	            , CAST(0 AS DECIMAL(38,2)) AS in_transit_cost
	            , CAST(0 AS INTEGER) AS in_transit_units
              , CAST(0 AS DECIMAL(38,2)) AS receipts_retail
              , CAST(0 AS DECIMAL(38,2)) AS receipts_cost
              , CAST(0 AS INTEGER) AS receipts_units
	        FROM
	            PRD_NAP_USR_VWS.MERCH_ON_ORDER_FACT_VW oo
	        INNER JOIN PRD_NAP_USR_VWS.STORE_DIM loc
	            ON oo.store_num = loc.store_num
	        WHERE week_num >= (select min(wk_idnt) - 7  from T2DL_DAS_SCALED_EVENTS.scaled_event_dates where cyber_ind = 1 and ty_ly_lly = 'TY')
	            AND week_num <= (select max(wk_idnt) from T2DL_DAS_SCALED_EVENTS.scaled_event_dates where cyber_ind = 1)
	            AND channel_num in (110,120,210,250)
							AND QUANTITY_OPEN > 0
							AND (-- include closed POs with ship dates in the recent past
	 						(STATUS = 'CLOSED' AND END_SHIP_DATE >= (SELECT MAX(WEEK_END_DAY_DATE) FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM WHERE WEEK_END_DAY_DATE < CURRENT_DATE) - 45)
	 						-- include POs that aren't closed yet
	 						OR STATUS IN ('APPROVED','WORKSHEET')
	)

	        GROUP BY 1, 2, 3

	        UNION ALL

	        SELECT -- Inventory
	            cost.sku_idnt
	            , cost.loc_idnt
              , snapshot_date as day_date
	            , CAST(0 AS DECIMAL(38,9)) AS on_order_retail
	            , CAST(0 AS DECIMAL(38,9)) AS on_order_cost
	            , CAST(0 AS INTEGER) AS on_order_units
							, CAST(0 AS DECIMAL(38,9)) AS oo_ordered_retail
							, CAST(0 AS DECIMAL(38,9)) AS oo_ordered_cost
							, CAST(0 AS INTEGER) AS oo_ordered
							, CAST(0 AS INTEGER) AS oo_canceled
							, SUM(cost.eoh_units * prc.ownership_retail_price_amt) AS eoh_retail
					    , SUM(cost.eoh_cost) as eoh_cost
					    , SUM(cost.eoh_units) as eoh_units
					    , SUM(cost.in_transit_u * prc.ownership_retail_price_amt) as in_transit_retail
					    , SUM(cost.in_transit_c) as in_transit_cost
					    , SUM(cost.in_transit_u) as in_transit_units
              , CAST(0 AS DECIMAL(38,2)) AS receipts_retail
              , CAST(0 AS DECIMAL(38,2)) AS receipts_cost
              , CAST(0 AS INTEGER) AS receipts_units
				FROM (
			  	SELECT DISTINCT
			  		sku_idnt
			  		, snapshot_date
			  		, loc_idnt
			  		, price_store_num
			  		, SUM(soh_qty) as eoh_units
			  		, SUM(in_transit_qty) as in_transit_u
			  		, SUM(soh_qty * weighted_average_cost) as eoh_cost
			  		, SUM(in_transit_qty * weighted_average_cost) as in_transit_c
			  	FROM INV_STG base
			    LEFT JOIN PRD_NAP_USR_VWS.WEIGHTED_AVERAGE_COST_DATE_DIM cst
			      		ON base.sku_idnt = cst.sku_num
			      		AND base.loc_idnt = cst.location_num
			      		AND base.snapshot_date between cst.eff_begin_dt and cst.eff_end_dt - 1
			    GROUP BY 1,2,3,4
			  ) cost
			  LEFT JOIN PRD_NAP_USR_VWS.PRODUCT_PRICE_TIMELINE_DIM prc
			       ON cost.sku_idnt = prc.rms_sku_num
			  		 AND cost.price_store_num = prc.store_num
						 AND CAST(cost.snapshot_date AS TIMESTAMP) BETWEEN prc.EFF_BEGIN_TMSTP AND prc.EFF_END_TMSTP - interval '0.001' second
	        GROUP BY 1, 2, 3

        UNION ALL

        SELECT -- Receipts
          a.sku_num AS sku_idnt
          , CAST(a.store_num AS VARCHAR(10)) as loc_idnt
          , CURRENT_DATE - 1 AS day_date
          , CAST(0 AS DECIMAL(38,9)) AS on_order_retail
          , CAST(0 AS DECIMAL(38,9)) AS on_order_cost
          , CAST(0 AS INTEGER) AS on_order_units
					, CAST(0 AS DECIMAL(38,9)) AS oo_ordered_retail
					, CAST(0 AS DECIMAL(38,9)) AS oo_ordered_cost
					, CAST(0 AS INTEGER) AS oo_ordered
					, CAST(0 AS INTEGER) AS oo_canceled
          , CAST(0 AS DECIMAL(38,2)) AS eoh_retail
          , CAST(0 AS DECIMAL(38,2)) AS eoh_cost
          , CAST(0 AS INTEGER) AS eoh_units
          , CAST(0 AS DECIMAL(38,2)) AS in_transit_retail
          , CAST(0 AS DECIMAL(38,2)) AS in_transit_cost
          , CAST(0 AS INTEGER) AS in_transit_units
          , SUM(RECEIPTS_RETAIL + RECEIPTS_CROSSDOCK_RETAIL) AS receipts_retail
          , SUM(RECEIPTS_COST + RECEIPTS_CROSSDOCK_COST) AS receipts_cost
          , SUM(RECEIPTS_UNITS + RECEIPTS_CROSSDOCK_UNITS) AS receipts_units

        FROM PRD_NAP_USR_VWS.MERCH_PORECEIPT_SKU_STORE_FACT_VW a
        INNER JOIN PRD_NAP_USR_VWS.STORE_DIM loc
            ON a.store_num = loc.store_num
        WHERE TRAN_DATE <= (select min(day_dt) from T2DL_DAS_SCALED_EVENTS.scaled_event_dates where cyber_ind = 1 and ty_ly_lly = 'TY' ) -- Week Cyber launches
        AND TRAN_DATE >= (select min(day_dt) - 126 from T2DL_DAS_SCALED_EVENTS.scaled_event_dates where cyber_ind = 1 and ty_ly_lly = 'TY' ) -- 18 weeks Cyber launches
        AND channel_num in (110,120,210,250)
        GROUP BY 1, 2, 3

    ) sub
    GROUP BY 1, 2, 3;

collect statistics primary index (sku_idnt, loc_idnt, day_date)
	on {environment_schema}.sku_loc_daily_se_an_on_order;


delete from {environment_schema}.CYBER_OWNERSHIP_DAILY
	WHERE day_date = CURRENT_DATE - 1;
INSERT INTO	{environment_schema}.CYBER_OWNERSHIP_DAILY
		SELECT own.*
		FROM {environment_schema}.CYBER_OWNERSHIP_VW own
		INNER JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM c454
			ON own.day_date = c454.day_date
		WHERE own.day_date = c454.week_end_day_date ;

COLLECT STATISTICS
		    PRIMARY INDEX (vpn, supp_color, location)
		    ,COLUMN (vpn)
				,COLUMN (supp_color)
				,COLUMN (location)
		ON {environment_schema}.CYBER_OWNERSHIP_DAILY;
