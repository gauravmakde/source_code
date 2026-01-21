--DROP TABLE dates;

CREATE TEMPORARY TABLE IF NOT EXISTS dates
AS
SELECT day_date,
 day_idnt,
 week_idnt,
 week_end_day_date,
 fiscal_year_num AS year_idnt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dt
WHERE week_idnt BETWEEN (SELECT DISTINCT week_idnt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_date = {{params.start_date}}) 
   AND (SELECT MAX(week_idnt)
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE week_end_day_date < CURRENT_DATE('PST8PDT'));


CREATE TEMPORARY TABLE IF NOT EXISTS locations
AS
SELECT DISTINCT store_num,
 TRIM(TRIM(FORMAT('%11d', channel_num)) || ', ' || TRIM(channel_desc)) AS chnl_label,
 RPAD(CAST(channel_num AS STRING), 3, ' ') AS chnl_idnt,
 channel_country AS country,
  CASE
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM')
  THEN 'FP'
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
  THEN 'OP'
  ELSE NULL
  END AS banner
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw
WHERE channel_num IN (110, 111, 120, 121, 130, 140, 210, 211, 220, 221, 250, 260, 261, 310, 311)
 AND (store_close_date IS NULL OR store_close_date >= (SELECT MIN(day_date)
     FROM dates));



CREATE TEMPORARY TABLE IF NOT EXISTS dropship_lkp
AS
SELECT DISTINCT dt.day_date,
 ds.rms_sku_id AS sku_idnt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.inventory_stock_quantity_by_day_fact AS ds
 INNER JOIN dates AS dt 
 ON ds.snapshot_date = dt.day_date
WHERE LOWER(ds.location_type) IN (LOWER('DS'), LOWER('DS_OP'));



CREATE TEMPORARY TABLE IF NOT EXISTS merch_staging
AS
SELECT dt.week_idnt,
 dt.week_end_day_date,
 dt.day_idnt,
 base.day_dt,
 base.sku_idnt,
 base.loc_idnt,
 locs.chnl_label,
 locs.chnl_idnt,
 locs.country,
 locs.banner,
 base.price_type, 
 CASE WHEN rp.rms_sku_num IS NOT NULL THEN 1 ELSE 0 END AS rp_ind,
  CASE
  WHEN ds.sku_idnt IS NOT NULL OR base.demand_dropship_units > 0
  THEN 1
  ELSE 0
  END AS dropship_ind,
 base.sales_units,
 base.sales_dollars,
 base.return_units,
 base.return_dollars,
 base.demand_units,
 base.demand_dollars,
 base.demand_cancel_units,
 base.demand_cancel_dollars,
 base.demand_dropship_units,
 base.demand_dropship_dollars,
 base.shipped_units,
 base.shipped_dollars,
 base.store_fulfill_units,
 base.store_fulfill_dollars,
  CASE
  WHEN base.day_dt = dt.week_end_day_date
  THEN base.eoh_units
  ELSE NULL
  END AS eoh_units,
  CASE
  WHEN base.day_dt = dt.week_end_day_date
  THEN base.eoh_dollars
  ELSE NULL
  END AS eoh_dollars,
  CASE
  WHEN base.day_dt = dt.week_end_day_date
  THEN base.eoh_units * base.weighted_average_cost
  ELSE NULL
  END AS eoh_cost,
  CASE
  WHEN base.day_dt = dt.week_end_day_date
  THEN base.nonsellable_units
  ELSE NULL
  END AS nonsellable_units,
 base.cost_of_goods_sold,
 base.gross_margin AS product_margin,
  CASE
  WHEN base.cost_of_goods_sold IS NOT NULL
  THEN base.sales_dollars
  ELSE NULL
  END AS sales_pm
FROM `{{params.gcp_project_id}}`.t2dl_das_ace_mfp.sku_loc_pricetype_day_vw AS base
 INNER JOIN dates AS dt ON base.day_dt = dt.day_date
 LEFT JOIN dropship_lkp AS ds 
 ON LOWER(base.sku_idnt) = LOWER(ds.sku_idnt) AND base.day_dt = ds.day_date
 INNER JOIN locations AS locs 
 ON base.loc_idnt = locs.store_num
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_rp_sku_loc_dim_hist rp
		ON base.sku_idnt = rp.rms_sku_num
    	AND base.loc_idnt = rp.location_num
        AND  RANGE_CONTAINS(rp.rp_period,base.day_dt)
	WHERE base.day_dt BETWEEN (SELECT MIN(day_date) FROM dates) AND (SELECT MAX(day_date) FROM dates)
;


CREATE TEMPORARY TABLE IF NOT EXISTS merch_insert
AS
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
		, SUM(cost_of_goods_sold                   ) AS cost_of_goods_sold
		, SUM(product_margin                       ) AS product_margin
		, SUM(sales_pm                             ) AS sales_pm
		, MAX(CAST(CURRENT_DATETIME('PST8PDT') AS TIMESTAMP)) AS update_timestamp
    ,  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as update_timestamp_tz
	FROM merch_staging base
	GROUP BY 1,2,3,4,5,6,7,8,9,10
;



DELETE 
FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.wbr_merch_staging{{params.env_suffix}}
WHERE week_idnt BETWEEN (SELECT MIN(week_idnt) FROM merch_insert) AND (SELECT MAX(week_idnt) FROM merch_insert);
	

INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.wbr_merch_staging{{params.env_suffix}}
SELECT * FROM merch_insert	;
    