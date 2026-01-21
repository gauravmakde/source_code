/******************************************************************************
Name: Scaled Events Anniversary Scaled Event Base Table
APPID-Name: APP08204 - Scaled Events Reporting
Purpose: Populate the Scaled Events Base table for the evening FULL DAG refressh
Variable(s):    {environment_schema} - `{{params.gcp_project_id}}`.{{params.environment_schema}}
                {start_date} - beginning date of refresh date range
                {end_date} - ending date of refresh date range
                {start_date_partition} - static start date of DS Volatile Table partition
                {end_date_partition} - static end date of DS Volatile Table partition
                {date_event_type} - Anniversary/Cyber data indicator
DAG: merch_se_all_daily_full
TABLE NAME: `{{params.gcp_project_id}}`.{{params.environment_schema}}.scaled_event_base (T3: T3DL_ACE_PRA.scaled_event_base)
Author(s): Manuela Hurtado Gonzalez, Alli Moore
Date Last Updated: 06-10-2024
******************************************************************************/

------------------------------------------------------------- START TEMPORARY TABLES -----------------------------------------------------------------------





CREATE TEMPORARY TABLE IF NOT EXISTS locs

AS
SELECT loc_idnt,
 chnl_idnt,
  CASE
  WHEN chnl_idnt IN (110, 120, 310, 920)
  THEN 'NORDSTROM'
  WHEN chnl_idnt IN (210, 250)
  THEN 'NORDSTROM_RACK'
  ELSE NULL
  END AS channel_brand,
  CASE
  WHEN chnl_idnt IN (110, 210)
  THEN 'STORE'
  WHEN chnl_idnt IN (120, 250) OR chnl_idnt IN (310, 920)
  THEN 'ONLINE'
  ELSE NULL
  END AS event_selling_channel,
  CASE
  WHEN chnl_idnt = 110
  THEN 'FL'
  WHEN chnl_idnt IN (210, 250)
  THEN 'RK'
  WHEN chnl_idnt = 120 OR chnl_idnt IN (310, 920)
  THEN 'FC'
  ELSE NULL
  END AS event_store_type_code,
  CASE
  WHEN cyber_loc_ind = 1
  THEN 'Cyber'
  ELSE NULL
  END AS cyber_loc_ind_type,
  CASE
  WHEN anniv_loc_ind = 1
  THEN 'Anniversary'
  ELSE NULL
  END AS anniv_loc_ind_type
FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.scaled_event_locs_vw
WHERE LOWER(CASE
    WHEN anniv_loc_ind = 1
    THEN 'Anniversary'
    ELSE NULL
    END) = LOWER('{{params.date_event_type}}')
 OR LOWER(CASE
    WHEN cyber_loc_ind = 1
    THEN 'Cyber'
    ELSE NULL
    END) = LOWER('{{params.date_event_type}}');


CREATE TEMPORARY TABLE IF NOT EXISTS dates

AS
SELECT day_idnt,
 day_dt,
 wk_idnt,
 anniv_ind,
 cyber_ind,
  CASE
  WHEN anniv_ind = 1
  THEN 'Anniversary'
  WHEN cyber_ind = 1
  THEN 'Cyber'
  ELSE NULL
  END AS date_event_type,
 MIN(CASE
   WHEN anniv_ind = CAST('1' AS FLOAT64)
   THEN day_dt
   ELSE NULL
   END) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS min_dt_anniv,
 MAX(CASE
   WHEN anniv_ind = CAST('1' AS FLOAT64)
   THEN day_dt
   ELSE NULL
   END) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_dt_anniv,
 MIN(CASE
   WHEN cyber_ind = CAST('1' AS FLOAT64)
   THEN day_dt
   ELSE NULL
   END) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS min_dt_cyber,
 MAX(CASE
   WHEN cyber_ind = CAST('1' AS FLOAT64)
   THEN day_dt
   ELSE NULL
   END) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_dt_cyber,
  CASE
  WHEN anniv_ind = 1
  THEN MIN(CASE
    WHEN anniv_ind = CAST('1' AS FLOAT64)
    THEN day_dt
    ELSE NULL
    END) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
  WHEN cyber_ind = 1
  THEN MIN(CASE
    WHEN cyber_ind = CAST('1' AS FLOAT64)
    THEN day_dt
    ELSE NULL
    END) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
  ELSE NULL
  END AS min_date,
  CASE
  WHEN anniv_ind = 1
  THEN MAX(CASE
    WHEN anniv_ind = CAST('1' AS FLOAT64)
    THEN day_dt
    ELSE NULL
    END) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
  WHEN cyber_ind = 1
  THEN MAX(CASE
    WHEN cyber_ind = CAST('1' AS FLOAT64)
    THEN day_dt
    ELSE NULL
    END) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
  ELSE NULL
  END AS max_date
FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.scaled_event_dates AS tdl
WHERE LOWER(CASE
    WHEN anniv_ind = 1
    THEN 'Anniversary'
    WHEN cyber_ind = 1
    THEN 'Cyber'
    ELSE NULL
    END) = LOWER('{{params.date_event_type}}');


CREATE TEMPORARY TABLE IF NOT EXISTS rp

AS 
    SELECT sku_idnt,
          day_idnt,
          rp_date_event_type,
          loc_idnt,
          rp_ind,
          day_dt
    FROM(
     SELECT DISTINCT
        rp.rms_sku_num AS sku_idnt
        , dt.day_idnt
        , CASE
            WHEN dt.anniv_ind = 1 THEN 'Anniversary'
            WHEN dt.cyber_ind = 1 THEN 'Cyber'
			ELSE NULL
        END AS rp_date_event_type
        , loc.loc_idnt
        , 1 AS rp_ind,
        dt.day_dt
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_rp_sku_loc_dim_hist rp
    INNER JOIN locs loc
        ON loc.loc_idnt = rp.location_num
    INNER JOIN `{{params.gcp_project_id}}`.{{params.environment_schema}}.scaled_event_dates dt
        ON RANGE_CONTAINS(rp.rp_period, dt.day_dt))
    WHERE day_dt <= {{params.end_date}}
        AND rp_date_event_type = '{{params.date_event_type}}';



CREATE TEMPORARY TABLE IF NOT EXISTS dropship (
sku_idnt STRING,
day_dt DATE
) 
AS
SELECT DISTINCT rms_sku_id AS sku_idnt,
 snapshot_date AS day_dt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.inventory_stock_quantity_by_day_physical_fact
WHERE LOWER(location_type) IN (LOWER('DS'))
AND snapshot_date BETWEEN {{params.start_date}} AND {{params.end_date}};



CREATE TEMPORARY TABLE IF NOT EXISTS scaled_event_customer_ntn

AS
SELECT a.sku_num AS sku_idnt,
 c.loc_idnt AS loc_idnt_ntn,
 b.day_idnt,
 a.business_day_date AS day_dt,
 b.date_event_type,
  CASE
  WHEN LOWER(d.selling_retail_price_type_code) = LOWER('PROMOTION')
  THEN 'P'
  WHEN LOWER(d.selling_retail_price_type_code) = LOWER('REGULAR')
  THEN 'R'
  WHEN LOWER(d.selling_retail_price_type_code) = LOWER('CLEARANCE')
  THEN 'C'
  ELSE NULL
  END AS price_type,
 COUNT(DISTINCT a.acp_id) AS ntn
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_ntn_fact AS a
 INNER JOIN locs AS c ON a.store_num = c.loc_idnt
 INNER JOIN dates AS b ON a.business_day_date = b.day_dt
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim AS d ON LOWER(d.rms_sku_num) = LOWER(a.sku_num) AND LOWER(d.channel_country
       ) = LOWER('US') AND LOWER(d.selling_channel) = LOWER(c.event_selling_channel) AND LOWER(d.channel_brand) = LOWER(c
     .channel_brand) AND a.trans_utc_tmstp BETWEEN d.eff_begin_tmstp AND d.eff_end_tmstp
WHERE a.merch_ind = 1
 AND a.sku_num IS NOT NULL
GROUP BY sku_idnt,
 loc_idnt_ntn,
 b.day_idnt,
 day_dt,
 b.date_event_type,
 price_type;



DELETE FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.scaled_event_base
WHERE LOWER(date_event_type) = LOWER('{{params.date_event_type}}')
AND day_dt BETWEEN {{params.start_date}} AND {{params.end_date}}; -- Delete only rows that will be refreshed and inserted below;


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.scaled_event_base
(SELECT ap.sku_idnt,
  ap.loc_idnt,
  ap.day_idnt,
  ap.day_dt,
  COALESCE(ap.price_type, 'NA') AS price_type,
  ap.date_event_type,
  CAST(locs1.chnl_idnt AS STRING) AS chnl_idnt,
  MAX(CASE
    WHEN anniv.anniv_item_ind = 1
    THEN 'Y'
    ELSE 'N'
    END) AS anniv_ind,
  MAX(CASE
    WHEN rp.rp_ind = 1
    THEN 'Y'
    ELSE 'N'
    END) AS rp_ind,
  CAST(SUM(ap.sales_units) AS NUMERIC) AS sales_units,
  SUM(ap.sales_dollars) AS sales_dollars,
  CAST(SUM(ap.return_units) AS NUMERIC) AS return_units,
  SUM(ap.return_dollars) AS return_dollars,
  SUM(ap.ntn) AS ntn,
  CAST(SUM(ap.demand_units) AS NUMERIC) AS demand_units,
  SUM(ap.demand_dollars) AS demand_dollars,
  CAST(SUM(ap.shipped_units) AS NUMERIC) AS shipped_units,
  SUM(ap.shipped_dollars) AS shipped_dollars,
  CAST(SUM(ap.demand_cancel_units) AS NUMERIC) AS demand_cancel_units,
  SUM(ap.demand_cancel_dollars) AS demand_cancel_dollars,
  CAST(SUM(ap.demand_dropship_units) AS NUMERIC) AS demand_dropship_units,
  SUM(ap.demand_dropship_dollars) AS demand_dropship_dollars,
  CAST(SUM(ap.store_fulfill_units) AS NUMERIC) AS store_fulfill_units,
  SUM(ap.store_fulfill_dollars) AS store_fulfill_dollars,
  CAST(SUM(ap.dropship_units) AS NUMERIC) AS dropship_units,
  CAST(SUM(ap.dropship_dollars) AS NUMERIC) AS dropship_dollars,
  CAST(SUM(ap.eoh_units) AS NUMERIC) AS eoh_units,
  SUM(ap.eoh_dollars) AS eoh_dollars,
  CAST(SUM(ap.boh_units) AS NUMERIC) AS boh_units,
  SUM(ap.boh_dollars) AS boh_dollars,
  CAST(SUM(ap.nonsellable_units) AS NUMERIC) AS nonsellable_units,
  SUM(ap.cogs) AS cogs,
  CAST(SUM(ap.receipt_units) AS NUMERIC) AS receipt_units,
  ROUND(CAST(SUM(ap.receipt_dollars) AS NUMERIC), 2) AS receipt_dollars,
  CAST(SUM(ap.receipt_dropship_units) AS NUMERIC) AS receipt_dropship_units,
  CAST(SUM(ap.receipt_dropship_dollars) AS NUMERIC) AS receipt_dropship_dollars,
  CAST(CASE
    WHEN SUM(ap.sales_units) = 0
    THEN 0
    ELSE SUM(ap.sales_dollars) / SUM(ap.sales_units)
    END AS NUMERIC) AS sales_aur,
  CAST(CASE
    WHEN SUM(ap.demand_units) = 0
    THEN 0
    ELSE SUM(ap.demand_dollars) / SUM(ap.demand_units)
    END AS NUMERIC) AS demand_aur,
  CAST(CASE
    WHEN SUM(ap.eoh_units) = 0
    THEN 0
    ELSE SUM(ap.eoh_dollars) / SUM(ap.eoh_units)
    END AS NUMERIC) AS eoh_aur,
  CAST(CASE
    WHEN SUM(ap.receipt_units) = 0
    THEN 0
    ELSE SUM(ap.receipt_dollars) / SUM(ap.receipt_units)
    END AS NUMERIC) AS receipt_aur,
  COALESCE(MAX(anniv.reg_price_amt), 0) AS anniv_retail_original,
  COALESCE(MAX(anniv.spcl_price_amt), 0) AS anniv_retail_special,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS
  process_tmstp,
  `{{params.gcp_project_id}}.JWN_UDF.DEFAULT_TZ_PST`() as process_tmstp_tz,
  ROUND(CAST(SUM(ap.sales_cost) AS NUMERIC), 2) AS sales_cost,
  ROUND(CAST(SUM(ap.return_cost) AS NUMERIC), 2) AS return_cost,
  ROUND(CAST(SUM(ap.shipped_cost) AS NUMERIC), 2) AS shipped_cost,
  ROUND(CAST(SUM(ap.store_fulfill_cost) AS NUMERIC), 2) AS store_fulfill_cost,
  CAST(SUM(ap.dropship_cost) AS NUMERIC) AS dropship_cost,
  ROUND(CAST(SUM(ap.eoh_cost) AS NUMERIC), 2) AS eoh_cost,
  ROUND(CAST(SUM(ap.boh_cost) AS NUMERIC), 2) AS boh_cost,
  ROUND(CAST(SUM(ap.receipt_cost) AS NUMERIC), 2) AS receipt_cost,
  CAST(SUM(ap.receipt_dropship_cost) AS NUMERIC) AS receipt_dropship_cost,
  CAST(SUM(ap.sales_pm) AS NUMERIC) AS sales_pm
 FROM (SELECT COALESCE(sls_dmd_inv.sku_idnt, rcpt.sku_idnt) AS sku_idnt,
    TRIM(FORMAT('%11d', COALESCE(sls_dmd_inv.loc_idnt, rcpt.loc_idnt))) AS loc_idnt,
    COALESCE(sls_dmd_inv.day_idnt, rcpt.day_idnt) AS day_idnt,
    COALESCE(sls_dmd_inv.day_dt, rcpt.day_dt) AS day_dt,
    COALESCE(sls_dmd_inv.price_type, rcpt.price_type) AS price_type,
    COALESCE(sls_dmd_inv.date_event_type, rcpt.date_event_type) AS date_event_type,
    SUM(COALESCE(sls_dmd_inv.sales_units, 0)) AS sales_units,
    SUM(COALESCE(sls_dmd_inv.sales_dollars, 0)) AS sales_dollars,
    SUM(COALESCE(sls_dmd_inv.sales_cost, 0)) AS sales_cost,
    SUM(COALESCE(sls_dmd_inv.return_units, 0)) AS return_units,
    SUM(COALESCE(sls_dmd_inv.return_dollars, 0)) AS return_dollars,
    SUM(COALESCE(sls_dmd_inv.return_cost, 0)) AS return_cost,
    SUM(COALESCE(sls_dmd_inv.ntn, 0)) AS ntn,
    SUM(COALESCE(sls_dmd_inv.demand_units, 0)) AS demand_units,
    SUM(COALESCE(sls_dmd_inv.demand_dollars, 0)) AS demand_dollars,
    SUM(COALESCE(sls_dmd_inv.shipped_units, 0)) AS shipped_units,
    SUM(COALESCE(sls_dmd_inv.shipped_dollars, 0)) AS shipped_dollars,
    SUM(COALESCE(sls_dmd_inv.shipped_cost, 0)) AS shipped_cost,
    SUM(COALESCE(sls_dmd_inv.demand_cancel_units, 0)) AS demand_cancel_units,
    SUM(COALESCE(sls_dmd_inv.demand_cancel_dollars, 0)) AS demand_cancel_dollars,
    SUM(COALESCE(sls_dmd_inv.demand_dropship_units, 0)) AS demand_dropship_units,
    SUM(COALESCE(sls_dmd_inv.demand_dropship_dollars, 0)) AS demand_dropship_dollars,
    SUM(COALESCE(sls_dmd_inv.store_fulfill_units, 0)) AS store_fulfill_units,
    SUM(COALESCE(sls_dmd_inv.store_fulfill_dollars, 0)) AS store_fulfill_dollars,
    SUM(COALESCE(sls_dmd_inv.store_fulfill_cost, 0)) AS store_fulfill_cost,
    SUM(COALESCE(sls_dmd_inv.dropship_dollars, 0)) AS dropship_dollars,
    SUM(COALESCE(sls_dmd_inv.dropship_units, 0)) AS dropship_units,
    SUM(COALESCE(sls_dmd_inv.dropship_cost, 0)) AS dropship_cost,
    SUM(COALESCE(sls_dmd_inv.eoh_units, 0)) AS eoh_units,
    SUM(COALESCE(sls_dmd_inv.eoh_dollars, 0)) AS eoh_dollars,
    SUM(COALESCE(sls_dmd_inv.eoh_cost, 0)) AS eoh_cost,
    SUM(COALESCE(sls_dmd_inv.boh_units, 0)) AS boh_units,
    SUM(COALESCE(sls_dmd_inv.boh_dollars, 0)) AS boh_dollars,
    SUM(COALESCE(sls_dmd_inv.boh_cost, 0)) AS boh_cost,
    SUM(COALESCE(sls_dmd_inv.nonsellable_units, 0)) AS nonsellable_units,
    SUM(COALESCE(sls_dmd_inv.cogs, 0)) AS cogs,
    SUM(COALESCE(sls_dmd_inv.sales_pm, 0)) AS sales_pm,
    SUM(COALESCE(rcpt.receipt_units, 0)) AS receipt_units,
    SUM(COALESCE(rcpt.receipt_dollars, 0)) AS receipt_dollars,
    SUM(COALESCE(rcpt.receipt_cost, 0)) AS receipt_cost,
    SUM(COALESCE(rcpt.receipt_dropship_units, 0)) AS receipt_dropship_units,
    SUM(COALESCE(rcpt.receipt_dropship_dollars, 0)) AS receipt_dropship_dollars,
    SUM(COALESCE(rcpt.receipt_dropship_cost, 0)) AS receipt_dropship_cost
   FROM (SELECT COALESCE(sls_dmnd_inv.sku_idnt, t2.sku_idnt) AS sku_idnt,
      COALESCE(sls_dmnd_inv.loc_idnt, t2.loc_idnt) AS loc_idnt,
      COALESCE(sls_dmnd_inv.day_idnt, t2.day_idnt) AS day_idnt,
      COALESCE(sls_dmnd_inv.day_dt, t2.day_dt) AS day_dt,
      COALESCE(sls_dmnd_inv.price_type, t2.price_type) AS price_type,
      COALESCE(sls_dmnd_inv.date_event_type, t2.date_event_type) AS date_event_type,
      SUM(COALESCE(sls_dmnd_inv.sales_units, 0)) AS sales_units,
      SUM(COALESCE(sls_dmnd_inv.sales_dollars, 0)) AS sales_dollars,
      SUM(COALESCE(sls_dmnd_inv.sales_cost, 0)) AS sales_cost,
      SUM(COALESCE(sls_dmnd_inv.return_units, 0)) AS return_units,
      SUM(COALESCE(sls_dmnd_inv.return_dollars, 0)) AS return_dollars,
      SUM(COALESCE(sls_dmnd_inv.return_cost, 0)) AS return_cost,
      SUM(COALESCE(t2.ntn, 0)) AS ntn,
      SUM(COALESCE(sls_dmnd_inv.demand_units, 0)) AS demand_units,
      SUM(COALESCE(sls_dmnd_inv.demand_dollars, 0)) AS demand_dollars,
      SUM(COALESCE(sls_dmnd_inv.shipped_units, 0)) AS shipped_units,
      SUM(COALESCE(sls_dmnd_inv.shipped_dollars, 0)) AS shipped_dollars,
      SUM(COALESCE(sls_dmnd_inv.shipped_cost, 0)) AS shipped_cost,
      SUM(COALESCE(sls_dmnd_inv.demand_cancel_units, 0)) AS demand_cancel_units,
      SUM(COALESCE(sls_dmnd_inv.demand_cancel_dollars, 0)) AS demand_cancel_dollars,
      SUM(COALESCE(sls_dmnd_inv.demand_dropship_units, 0)) AS demand_dropship_units,
      SUM(COALESCE(sls_dmnd_inv.demand_dropship_dollars, 0)) AS demand_dropship_dollars,
      SUM(COALESCE(sls_dmnd_inv.store_fulfill_units, 0)) AS store_fulfill_units,
      SUM(COALESCE(sls_dmnd_inv.store_fulfill_dollars, 0)) AS store_fulfill_dollars,
      SUM(COALESCE(sls_dmnd_inv.store_fulfill_cost, 0)) AS store_fulfill_cost,
      SUM(COALESCE(sls_dmnd_inv.dropship_dollars, 0)) AS dropship_dollars,
      SUM(COALESCE(sls_dmnd_inv.dropship_units, 0)) AS dropship_units,
      SUM(COALESCE(sls_dmnd_inv.dropship_cost, 0)) AS dropship_cost,
      SUM(COALESCE(sls_dmnd_inv.eoh_units, 0)) AS eoh_units,
      SUM(COALESCE(sls_dmnd_inv.eoh_dollars, 0)) AS eoh_dollars,
      SUM(COALESCE(sls_dmnd_inv.eoh_cost, 0)) AS eoh_cost,
      SUM(COALESCE(sls_dmnd_inv.boh_units, 0)) AS boh_units,
      SUM(COALESCE(sls_dmnd_inv.boh_dollars, 0)) AS boh_dollars,
      SUM(COALESCE(sls_dmnd_inv.boh_cost, 0)) AS boh_cost,
      SUM(COALESCE(sls_dmnd_inv.nonsellable_units, 0)) AS nonsellable_units,
      SUM(COALESCE(ROUND(CAST(sls_dmnd_inv.cogs AS NUMERIC), 2), 0)) AS cogs,
      SUM(COALESCE(sls_dmnd_inv.sales_pm, 0)) AS sales_pm
     FROM (SELECT base.sku_idnt,
        base.loc_idnt,
        dates.day_idnt,
        base.day_dt,
        base.price_type,
        dates.date_event_type,
        SUM(base.sales_units) AS sales_units,
        SUM(base.sales_dollars) AS sales_dollars,
        SUM(base.cost_of_goods_sold) AS sales_cost,
        SUM(base.return_units) AS return_units,
        SUM(base.return_dollars) AS return_dollars,
        SUM(base.return_units * base.weighted_average_cost) AS return_cost,
        SUM(base.demand_units) AS demand_units,
        SUM(base.demand_dollars) AS demand_dollars,
        SUM(base.shipped_units) AS shipped_units,
        SUM(base.shipped_dollars) AS shipped_dollars,
        SUM(base.shipped_units * base.weighted_average_cost) AS shipped_cost,
        SUM(base.demand_cancel_units) AS demand_cancel_units,
        SUM(base.demand_cancel_dollars) AS demand_cancel_dollars,
        SUM(base.demand_dropship_units) AS demand_dropship_units,
        SUM(base.demand_dropship_dollars) AS demand_dropship_dollars,
        SUM(base.store_fulfill_units) AS store_fulfill_units,
        SUM(base.store_fulfill_dollars) AS store_fulfill_dollars,
        SUM(base.store_fulfill_units * base.weighted_average_cost) AS store_fulfill_cost,
        SUM(CASE
          WHEN dropship.sku_idnt IS NOT NULL
          THEN base.sales_dollars
          ELSE NULL
          END) AS dropship_dollars,
        SUM(CASE
          WHEN dropship.sku_idnt IS NOT NULL
          THEN base.sales_units
          ELSE NULL
          END) AS dropship_units,
        SUM(CASE
          WHEN dropship.sku_idnt IS NOT NULL
          THEN base.cost_of_goods_sold
          ELSE NULL
          END) AS dropship_cost,
        SUM(base.eoh_units) AS eoh_units,
        SUM(base.eoh_dollars) AS eoh_dollars,
        SUM(base.eoh_units * base.weighted_average_cost) AS eoh_cost,
        SUM(COALESCE(ns.nonsellable_qty, 0)) AS nonsellable_units,
        SUM(base.boh_units) AS boh_units,
        SUM(base.boh_dollars) AS boh_dollars,
        SUM(base.boh_units * base.weighted_average_cost) AS boh_cost,
        SUM(base.cost_of_goods_sold) AS cogs,
        SUM(CASE
          WHEN base.cost_of_goods_sold IS NOT NULL
          THEN base.sales_dollars
          ELSE NULL
          END) AS sales_pm
       FROM `{{params.gcp_project_id}}`.t2dl_das_ace_mfp.sku_loc_pricetype_day_vw AS base
        INNER JOIN locs ON base.loc_idnt = locs.loc_idnt
        INNER JOIN dates ON base.day_dt = dates.day_dt
        LEFT JOIN dropship ON LOWER(base.sku_idnt) = LOWER(dropship.sku_idnt) AND base.day_dt = dropship.day_dt
        LEFT JOIN `{{params.gcp_project_id}}`.{{params.environment_schema}}.anniversary_nonsell AS ns ON base.loc_idnt = CAST(ns.location_id AS FLOAT64)
           AND base.day_dt = ns.snapshot_date AND LOWER(base.sku_idnt) = LOWER(ns.rms_sku_id) AND LOWER(base.price_type
           ) = LOWER(ns.current_price_type)
       GROUP BY base.sku_idnt,
        base.loc_idnt,
        dates.day_idnt,
        base.day_dt,
        base.price_type,
        dates.date_event_type) AS sls_dmnd_inv
      FULL JOIN (SELECT sku_idnt,
        CAST(loc_idnt_ntn AS INTEGER) AS loc_idnt,
        day_idnt,
        day_dt,
        price_type,
        date_event_type,
        ntn
       FROM scaled_event_customer_ntn) AS t2 ON LOWER(sls_dmnd_inv.sku_idnt) = LOWER(t2.sku_idnt) AND sls_dmnd_inv.loc_idnt
           = t2.loc_idnt AND sls_dmnd_inv.day_idnt = t2.day_idnt AND LOWER(sls_dmnd_inv.price_type) = LOWER(t2.price_type
         )
     GROUP BY sku_idnt,
      loc_idnt,
      day_idnt,
      day_dt,
      price_type,
      date_event_type) AS sls_dmd_inv
    FULL JOIN (SELECT base0.sku_num AS sku_idnt,
      base0.store_num AS loc_idnt,
      dates0.day_idnt,
      base0.tran_date AS day_dt,
       CASE
       WHEN LOWER(pt.ownership_retail_price_type_code) = LOWER('PROMOTION')
       THEN 'P'
       WHEN LOWER(pt.ownership_retail_price_type_code) = LOWER('CLEARANCE')
       THEN 'C'
       ELSE 'R'
       END AS price_type,
      dates0.date_event_type,
      SUM(base0.receipts_units + base0.receipts_crossdock_units) AS receipt_units,
      SUM(base0.receipts_retail + base0.receipts_crossdock_retail) AS receipt_dollars,
      SUM(base0.receipts_cost + base0.receipts_crossdock_cost) AS receipt_cost,
      SUM(CASE
        WHEN LOWER(base0.dropship_ind) = LOWER('Y')
        THEN base0.receipts_units + base0.receipts_crossdock_units
        ELSE 0
        END) AS receipt_dropship_units,
      SUM(CASE
        WHEN LOWER(base0.dropship_ind) = LOWER('Y')
        THEN base0.receipts_retail + base0.receipts_crossdock_retail
        ELSE 0
        END) AS receipt_dropship_dollars,
      SUM(CASE
        WHEN LOWER(base0.dropship_ind) = LOWER('Y')
        THEN base0.receipts_cost + base0.receipts_crossdock_cost
        ELSE 0
        END) AS receipt_dropship_cost
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_poreceipt_sku_store_fact_vw AS base0
      INNER JOIN locs AS locs0 ON base0.store_num = locs0.loc_idnt
      INNER JOIN dates AS dates0 ON base0.tran_date = dates0.day_dt
      LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw AS ps ON base0.store_num = ps.store_num
      LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim AS pt ON LOWER(pt.rms_sku_num) = LOWER(base0.sku_num) AND CAST(pt.store_num AS FLOAT64)
         = ps.price_store_num 
         AND RANGE_CONTAINS(RANGE(CAST(pt.eff_begin_tmstp AS TIMESTAMP), CAST(pt.eff_end_tmstp AS TIMESTAMP)), TIMESTAMP(base0.event_time
         , 'GMT'))
     GROUP BY sku_idnt,
      loc_idnt,
      dates0.day_idnt,
      day_dt,
      price_type,
      dates0.date_event_type) AS rcpt ON LOWER(sls_dmd_inv.sku_idnt) = LOWER(rcpt.sku_idnt) AND sls_dmd_inv.loc_idnt =
        rcpt.loc_idnt AND sls_dmd_inv.day_idnt = rcpt.day_idnt AND LOWER(sls_dmd_inv.price_type) = LOWER(rcpt.price_type
       )
   GROUP BY sku_idnt,
    loc_idnt,
    day_idnt,
    day_dt,
    price_type,
    date_event_type) AS ap
  LEFT JOIN rp ON ap.sku_idnt = rp.sku_idnt 
  AND ap.loc_idnt = CAST(rp.loc_idnt AS STRING) 
  AND ap.day_idnt = rp.day_idnt
  LEFT JOIN locs AS locs1 
  ON ap.loc_idnt = CAST(locs1.loc_idnt AS STRING)
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.environment_schema}}.anniversary_sku_chnl_date AS anniv 
  ON LOWER(ap.sku_idnt) = LOWER(anniv.sku_idnt) 
  AND LOWER(anniv.channel_country) = LOWER('US') 
  AND LOWER(locs1.event_selling_channel) = LOWER(anniv.selling_channel
        ) AND LOWER(locs1.event_store_type_code) = LOWER(anniv.store_type_code) AND ap.day_idnt = anniv.day_idnt AND
    LOWER('Anniversary') = LOWER('{{params.date_event_type}}')
 GROUP BY ap.sku_idnt,
  ap.loc_idnt,
  ap.day_idnt,
  ap.day_dt,
  price_type,
  ap.date_event_type,
  locs1.chnl_idnt,
  process_tmstp);
