/******************************************************************************
** FILE: inventory_position_master.sql
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
**		The live_date given comes from {{params.gcp_project_id}}.{{params.dbenv}}_NAP_USR_VWS.PRODUCT_SKU_DIM_VW and may not be wholly reliable.
**		Pricing information at a given time is determined from effective timestamps in {{params.gcp_project_id}}.{{params.dbenv}}_NAP_USR_VWS.PRODUCT_PRICE_FORECAST_DIM.  These can generally change
**			to accommodate NAP data events (such that there is no overlap), though they should be stable for historical events in this table.  Note that, while
**			effective timestamps ensure no overlap between pricing events, they can generally miss pricing information from the beginning of an item's history.
**
******************************************************************************/




CREATE TEMPORARY TABLE IF NOT EXISTS sku

AS
SELECT rms_sku_num,
 channel_country,
 rms_style_num,
 epm_choice_num,
 style_desc,
 brand_name,
 web_style_num,
 prmy_supp_num,
 manufacturer_num,
 sbclass_num,
 class_num,
 dept_num,
 grp_num,
 div_num,
 color_num,
 color_desc,
 nord_display_color,
 size_1_num,
 size_2_num,
 return_disposition_code,
 selling_status_desc,
  CASE
  WHEN live_date > CURRENT_DATE('PST8PDT')
  THEN NULL
  ELSE live_date
  END AS live_date,
 selling_channel_eligibility_list,
 drop_ship_eligible_ind
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw;


CREATE TEMPORARY TABLE IF NOT EXISTS liveonsite_base
AS
SELECT p.rms_sku_num,
 p.channel_country,
 p.channel_brand,
 'ONLINE' AS selling_channel,
 a.epm_choice_num,
 p.eff_begin_tmstp,
 p.eff_end_tmstp 
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.product_online_purchasable_item_dim AS p
 INNER JOIN (SELECT DISTINCT rms_sku_num,
   channel_country,
   epm_choice_num
  FROM sku) AS a ON LOWER(p.rms_sku_num) = LOWER(a.rms_sku_num) AND LOWER(p.channel_country) = LOWER(a.channel_country)
WHERE LOWER(p.is_online_purchasable) = LOWER('Y')
 AND LOWER(p.channel_brand) IN (LOWER('NORDSTROM'), LOWER('NORDSTROM_RACK'))
 AND LOWER(p.channel_country) = LOWER('US')
 AND a.epm_choice_num IS NOT NULL;



CREATE TEMPORARY TABLE IF NOT EXISTS liveonsite AS (
    SELECT
        rms_sku_num,
        channel_country,
        channel_brand,
        selling_channel,
        epm_choice_num,
        live_date 
    FROM
      (
      SELECT  pa.rms_sku_num
       ,pa.channel_country 
       ,pa.channel_brand
       ,pa.selling_channel
       ,pa.epm_choice_num
       ,pa.eff_begin_tmstp
       ,pa.eff_end_tmstp
       ,CAST(RANGE_START(eff_period) AS date) AS live_date
       From liveonsite_base AS pa
    CROSS JOIN 
        UNNEST(generate_range_array(RANGE(pa.eff_begin_tmstp, pa.eff_end_tmstp), interval 180 day)) AS eff_period
)  as a)
;
CREATE TEMPORARY TABLE IF NOT EXISTS liveonsite_price

AS
SELECT g.week_num,
 liveonsite.channel_country,
 liveonsite.channel_brand,
 liveonsite.selling_channel,
 liveonsite.epm_choice_num,
 0 AS anniversary_flag,
 MIN(g.week_start_date) AS week_start_date,
 COUNT(DISTINCT liveonsite.rms_sku_num) AS sku_ct,
 0 AS regular_sku_ct,
 0 AS skuday_ct,
 0 AS regular_skuday_ct,
 0 AS clearance_skuday_ct,
 0 AS promotion_skuday_ct,
 0 AS regular_price_amt,
 0 AS current_price_amt
FROM (SELECT *
  FROM liveonsite
  WHERE liveonsite.live_date BETWEEN {{params.start_date}} AND (CURRENT_DATE('PST8PDT'))) AS liveonsite
 INNER JOIN (SELECT day_date,
   week_num,
   MIN(day_date) OVER (PARTITION BY week_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS
   week_start_date
  FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.day_cal) AS g ON liveonsite.live_date = g.day_date
WHERE liveonsite.channel_brand IS NOT NULL
 AND liveonsite.selling_channel IS NOT NULL
GROUP BY liveonsite.channel_country,
 liveonsite.channel_brand,
 liveonsite.selling_channel,
 liveonsite.epm_choice_num,
 g.week_num
HAVING liveonsite.epm_choice_num IS NOT NULL;


CREATE TEMPORARY TABLE IF NOT EXISTS inventory
AS
SELECT g.week_num,
 a.epm_choice_num,
 d.channel_country,
 COUNT(DISTINCT CASE
   WHEN LOWER(d.business_unit_desc) = LOWER('N.COM')
   THEN p.location_id
   ELSE NULL
   END) AS inv_ncom_location_ct,
 COUNT(DISTINCT CASE
   WHEN LOWER(d.business_unit_desc) = LOWER('N.CA')
   THEN p.location_id
   ELSE NULL
   END) AS inv_nca_location_ct,
 COUNT(DISTINCT CASE
   WHEN LOWER(d.business_unit_desc) = LOWER('OFFPRICE ONLINE')
   THEN p.location_id
   ELSE NULL
   END) AS inv_nrhl_location_ct,
 COUNT(DISTINCT CASE
   WHEN LOWER(d.banner) = LOWER('FP')
   THEN p.location_id
   ELSE NULL
   END) AS inv_usfp_location_ct,
 COUNT(DISTINCT CASE
   WHEN LOWER(d.banner) = LOWER('OP')
   THEN p.location_id
   ELSE NULL
   END) AS inv_usop_location_ct,
 MAX(CASE
   WHEN LOWER(p.location_type) IN (LOWER('DS_OP'), LOWER('DS'))
   THEN 1
   ELSE 0
   END) AS dropship_stock_flag
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.inventory_stock_quantity_by_day_fact AS p
 INNER JOIN (SELECT DISTINCT store_num,
   business_unit_desc,
    CASE
    WHEN LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('N.COM'), LOWER('N.CA'), LOWER('FULL LINE CANADA'),
      LOWER('TRUNK CLUB'))
    THEN 'FP'
    WHEN LOWER(business_unit_desc) IN (LOWER('RACK CANADA'), LOWER('RACK'), LOWER('OFFPRICE ONLINE'))
    THEN 'OP'
    ELSE NULL
    END AS banner,
    CASE
    WHEN LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('N.COM'), LOWER('TRUNK CLUB'), LOWER('RACK'), LOWER('OFFPRICE ONLINE'
       ))
    THEN 'US'
    WHEN LOWER(business_unit_desc) IN (LOWER('RACK CANADA'), LOWER('N.CA'), LOWER('FULL LINE CANADA'))
    THEN 'CA'
    ELSE NULL
    END AS channel_country
  FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.store_dim) AS d ON CAST(p.location_id AS FLOAT64) = d.store_num
 INNER JOIN (SELECT DISTINCT rms_sku_num,
   channel_country,
   epm_choice_num
  FROM sku) AS a ON LOWER(p.rms_sku_id) = LOWER(a.rms_sku_num) AND LOWER(d.channel_country) = LOWER(a.channel_country)
 INNER JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.day_cal AS g ON DATE_ADD(p.snapshot_date, INTERVAL 1 DAY) = g.day_date
WHERE p.snapshot_date >= {{params.start_date}}
 AND p.stock_on_hand_qty > 0
GROUP BY g.week_num,
 a.epm_choice_num,
 d.channel_country
HAVING a.epm_choice_num IS NOT NULL;


CREATE TEMPORARY TABLE IF NOT EXISTS orderdetail
AS
SELECT p.source_channel_country_code AS channel_country,
  CASE
  WHEN LOWER(p.source_channel_code) = LOWER('FULL_LINE')
  THEN 'NORDSTROM'
  WHEN LOWER(p.source_channel_code) = LOWER('RACK')
  THEN 'NORDSTROM_RACK'
  ELSE NULL
  END AS channel_brand,
 'ONLINE' AS selling_channel,
 a.epm_choice_num,
 g.week_num,
 CAST(trunc(dma.us_dma_code) AS INTEGER) AS dma_cd,
 SUM(p.order_line_quantity) AS order_line_quantity,
 SUM(p.order_line_amount_usd) AS order_line_amount_usd,
 SUM(CASE
   WHEN LOWER(p.last_released_node_type_code) = LOWER('DS')
   THEN p.order_line_quantity
   ELSE 0
   END) AS dropship_order_line_quantity,
 SUM(CASE
   WHEN LOWER(p.last_released_node_type_code) = LOWER('DS')
   THEN p.order_line_amount_usd
   ELSE 0
   END) AS dropship_order_line_amount_usd,
 SUM(CASE
   WHEN LOWER(p.promise_type_code) LIKE LOWER('%PICKUP%')
   THEN p.order_line_quantity
   ELSE 0
   END) AS bopus_order_line_quantity,
 SUM(CASE
   WHEN LOWER(p.promise_type_code) LIKE LOWER('%PICKUP%')
   THEN p.order_line_amount_usd
   ELSE 0
   END) AS bopus_order_line_amount_usd,
 SUM(CASE
   WHEN LOWER(p.cancel_reason_code) = LOWER('MERCHANDISE_NOT_AVAILABLE')
   THEN p.order_line_quantity
   ELSE 0
   END) AS merchandise_not_available_order_line_quantity,
 SUM(CASE
   WHEN LOWER(p.cancel_reason_code) = LOWER('MERCHANDISE_NOT_AVAILABLE')
   THEN p.order_line_amount_usd
   ELSE 0
   END) AS merchandise_not_available_order_line_amount_usd
FROM (SELECT order_num,
   order_line_num,
   upc_code,
   rms_sku_num,
   promise_type_code,
   last_released_node_type_code,
   bill_zip_code,
   destination_zip_code,
   order_date_pacific,
   source_channel_code,
   source_channel_country_code,
   cancel_reason_code,
   order_line_amount_usd,
   order_line_quantity
  FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.order_line_detail_fact
  WHERE order_date_pacific >= {{params.start_date}} AND order_date_pacific < '4444-04-04'
   OR order_date_pacific > '4444-04-04') AS p
 INNER JOIN (SELECT DISTINCT rms_sku_num,
   channel_country,
   epm_choice_num
  FROM sku) AS a ON LOWER(p.rms_sku_num) = LOWER(a.rms_sku_num) AND LOWER(p.source_channel_country_code) = LOWER(a.channel_country
    )
 INNER JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.day_cal AS g ON p.order_date_pacific = g.day_date
 LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.org_us_zip_dma AS dma ON LOWER(TRIM(SUBSTR(COALESCE(p.destination_zip_code, p.bill_zip_code)
     , 1, 10))) = LOWER(dma.us_zip_code)
WHERE LOWER(p.cancel_reason_code) = LOWER('MERCHANDISE_NOT_AVAILABLE')
 OR p.cancel_reason_code IS NULL
 OR LOWER(p.cancel_reason_code) = LOWER('')
GROUP BY channel_country,
 channel_brand,
 selling_channel,
 a.epm_choice_num,
 g.week_num,
 dma_cd
HAVING a.epm_choice_num IS NOT NULL;


CREATE TEMPORARY TABLE IF NOT EXISTS digital
AS
SELECT g.week_num,
 d.channel_country,
 d.channel_brand,
 d.epm_choice_num,
 AVG(d.review_count) AS review_count,
 SUM(d.product_views) AS product_views
FROM (SELECT p.event_date_pacific,
   p.channelcountry AS channel_country,
    CASE
    WHEN LOWER(p.channel) = LOWER('FULL_LINE')
    THEN 'NORDSTROM'
    WHEN LOWER(p.channel) = LOWER('RACK')
    THEN 'NORDSTROM_RACK'
    ELSE NULL
    END AS channel_brand,
   a.epm_choice_num,
   AVG(p.review_count) AS review_count,
   SUM(p.product_views) AS product_views
  FROM t2dl_das_product_funnel.product_funnel_daily AS p
   INNER JOIN (SELECT DISTINCT channel_country,
     epm_choice_num,
     web_style_num AS web_style_id
    FROM sku
    WHERE web_style_num IS NOT NULL) AS a ON p.style_id = a.web_style_id AND LOWER(a.channel_country) = LOWER(p.channelcountry
      )
  WHERE p.event_date_pacific >= {{params.start_date}}
  GROUP BY p.event_date_pacific,
   channel_country,
   channel_brand,
   a.epm_choice_num
  HAVING a.epm_choice_num IS NOT NULL) AS d
 INNER JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.day_cal AS g ON d.event_date_pacific = g.day_date
GROUP BY g.week_num,
 d.channel_country,
 d.channel_brand,
 d.epm_choice_num;


CREATE TEMPORARY TABLE IF NOT EXISTS events

AS
SELECT cal.week_num,
 MAX(op.rcom_event_name) AS op_event_name,
 MAX(fp.event_name) AS fp_event_name
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.day_cal AS cal
 LEFT JOIN t2dl_das_store_sales_forecasting.loyalty_events_sales_shipping AS op ON cal.day_date = op.event_date
 LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.scaled_events_dates_dim AS fp ON cal.day_date = fp.day_date
WHERE cal.day_date >= {{params.start_date}}
GROUP BY cal.week_num;



DELETE FROM {{params.gcp_project_id}}.{{params.ip_forecast_t2_schema}}.inventory_position_master_table
WHERE (week_start_date >= {{params.start_date}} AND week_start_date < {{params.end_date}} );

INSERT INTO {{params.gcp_project_id}}.{{params.ip_forecast_t2_schema}}.inventory_position_master_table
(SELECT a.week_num,
  a.week_start_date,
  a.channel_brand,
  a.selling_channel,
  b.channel_country,
  b.epm_choice_num,
  b.total_sku_ct,
  b.total_size1_ct,
  b.rms_style_num,
  b.color_num,
  b.brand_name,
  b.manufacturer_num,
  b.prmy_supp_num,
  b.sbclass_num,
  b.class_num,
  b.dept_num,
  b.grp_num,
  b.div_num,
  b.style_desc,
  b.nord_display_color,
  b.return_disposition_code,
  b.live_start_date,
  b.selling_channel_eligibility_list,
  b.drop_ship_eligible_ind,
  a.anniversary_flag,
  a.sku_ct,
  a.regular_sku_ct,
  a.skuday_ct,
  a.regular_skuday_ct,
  a.clearance_skuday_ct,
  a.promotion_skuday_ct,
  CAST(a.regular_price_amt AS NUMERIC) AS regular_price_amt,
  CAST(a.current_price_amt AS NUMERIC) AS current_price_amt,
  c.inv_ncom_location_ct,
  c.inv_nca_location_ct,
  c.inv_nrhl_location_ct,
  c.inv_usfp_location_ct,
  c.inv_usop_location_ct,
  d.dma_cd,
  d.order_line_quantity,
  d.order_line_amount_usd,
  d.bopus_order_line_quantity,
  d.bopus_order_line_amount_usd,
  d.dropship_order_line_quantity,
  d.dropship_order_line_amount_usd,
  d.merchandise_not_available_order_line_quantity,
  d.merchandise_not_available_order_line_amount_usd,
  CAST(e.review_count AS FLOAT64) AS review_count,
  e.product_views,
  f.fp_event_name,
  f.op_event_name,
  0 AS rp_ind
 FROM liveonsite_price AS a
  INNER JOIN (SELECT channel_country,
    epm_choice_num,
    COUNT(DISTINCT rms_sku_num) AS total_sku_ct,
    COUNT(DISTINCT size_1_num) AS total_size1_ct,
    MAX(rms_style_num) AS rms_style_num,
    MAX(color_num) AS color_num,
    MAX(brand_name) AS brand_name,
    MAX(manufacturer_num) AS manufacturer_num,
    MAX(prmy_supp_num) AS prmy_supp_num,
    MAX(sbclass_num) AS sbclass_num,
    MAX(class_num) AS class_num,
    MAX(dept_num) AS dept_num,
    MAX(grp_num) AS grp_num,
    MAX(div_num) AS div_num,
    MAX(style_desc) AS style_desc,
    MAX(nord_display_color) AS nord_display_color,
    MAX(return_disposition_code) AS return_disposition_code,
    MIN(live_date) AS live_start_date,
    MAX(selling_channel_eligibility_list) AS selling_channel_eligibility_list,
    MAX(drop_ship_eligible_ind) AS drop_ship_eligible_ind
   FROM sku
   GROUP BY channel_country,
    epm_choice_num) AS b ON LOWER(a.channel_country) = LOWER(b.channel_country) AND a.epm_choice_num = b.epm_choice_num
  LEFT JOIN inventory AS c ON LOWER(a.channel_country) = LOWER(c.channel_country) AND a.epm_choice_num = c.epm_choice_num
      AND a.week_num = c.week_num
  LEFT JOIN orderdetail AS d ON a.epm_choice_num = d.epm_choice_num AND LOWER(a.channel_country) = LOWER(d.channel_country
        ) AND LOWER(a.channel_brand) = LOWER(d.channel_brand) AND LOWER(a.selling_channel) = LOWER(d.selling_channel)
   AND a.week_num = d.week_num
  LEFT JOIN digital AS e ON a.epm_choice_num = e.epm_choice_num AND LOWER(a.channel_country) = LOWER(e.channel_country)
    AND LOWER(a.channel_brand) = LOWER(e.channel_brand) AND a.week_num = e.week_num
  LEFT JOIN events AS f ON a.week_num = f.week_num
 WHERE a.week_start_date >= {{params.start_date}}
  AND a.week_start_date < {{params.end_date}} );