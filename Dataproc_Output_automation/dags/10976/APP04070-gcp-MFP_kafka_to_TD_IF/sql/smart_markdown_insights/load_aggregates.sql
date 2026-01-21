BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;


BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS my_params AS
SELECT DATE_SUB(dw_batch_dt, INTERVAL 6 DAY) AS last_sun_1wk,
 dw_batch_dt AS last_sat
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl
WHERE LOWER(interface_code) = LOWER('SMD_INSIGHTS_WKLY');

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;

END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS inv_data (
  metrics_date DATE NOT NULL,
  rms_sku_id STRING,
  rms_style_num STRING,
  color_num STRING,
  channel_country STRING NOT NULL,
  location_id STRING NOT NULL,
  channel_brand STRING NOT NULL,
  selling_channel STRING NOT NULL,
  prmy_supp_num STRING,
  supp_part_num STRING,
  drop_ship_eligible_ind STRING,
  npg_ind STRING,
  fp_replenishment_eligible_ind STRING,
  op_replenishment_eligible_ind STRING,
  supp_color STRING,
  return_disposition_code STRING,
  selling_status_code STRING,
  vendor_name STRING,
  vendor_label_name STRING,
  fulfillment_type_code STRING
);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;

INSERT INTO inv_data
(SELECT DISTINCT isibdf.metrics_date,
  CAST(t.A1459076419 AS STRING) AS rms_sku_id,
  psd.brand_label_display_name AS rms_style_num,
  isibdf.color_num,
  isibdf.channel_country,
  TRIM(isibdf.location_id) AS location_id,
  isibdf.channel_brand,
  isibdf.selling_channel,
  psd.supp_part_num AS prmy_supp_num,
  CAST(psd.epm_choice_num AS STRING) AS supp_part_num,
   CASE
   WHEN CAST(psd.hazardous_material_class_desc AS INTEGER) IN (3040, 4000, 4020, 4040)
   THEN 'Y'
   ELSE 'N'
   END AS drop_ship_eligible_ind,
  SUBSTR(psd.msrp_currency_code, 1, 1) AS npg_ind,
  psd.fp_item_planning_eligible_ind AS fp_replenishment_eligible_ind,
  psd.op_item_planning_eligible_ind AS op_replenishment_eligible_ind,
  psd.size_2_desc AS supp_color,
  psd.brand_name AS return_disposition_code,
  psrd.is_sellable_ind AS selling_status_code,
  vd.payto_num AS vendor_name,
  pstd.vendor_label_code AS vendor_label_name,
  psd.hazardous_material_class_desc AS fulfillment_type_code
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.inventory_sales_insights_by_day_fact AS isibdf
  INNER JOIN (`{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_fact AS cmibwf LEFT JOIN (SELECT last_sat AS  A1459076419 FROM my_params) AS t ON TRUE) 
  ON LOWER(COALESCE(isibdf.rms_style_num, 'NA')) = LOWER(COALESCE(cmibwf.rms_style_num,'NA')) 
    AND LOWER(COALESCE(isibdf.color_num, 'NA')) = LOWER(COALESCE(cmibwf.color_num, 'NA')) 
    AND LOWER(isibdf.channel_country) = LOWER(cmibwf.channel_country) 
    AND LOWER(isibdf.channel_brand) = LOWER(cmibwf.channel_brand)
    AND LOWER(isibdf.selling_channel) = LOWER(cmibwf.selling_channel) AND cmibwf.snapshot_date = t.A1459076419
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS psd 
  ON LOWER(COALESCE(psd.rms_style_num, 'NA')) = LOWER(COALESCE(isibdf.rms_style_num, 'NA')) 
    AND LOWER(COALESCE(psd.color_num, 'NA')) = LOWER(COALESCE(isibdf.color_num, 'NA')) 
    AND LOWER(psd.channel_country) = LOWER(isibdf.channel_country)
    AND RANGE_CONTAINS(RANGE(psd.EFF_BEGIN_TMSTP_UTC, psd.EFF_END_TMSTP_UTC), TIMESTAMP(datetime(timestamp(isibdf.METRICS_DATE) + INTERVAL '1' DAY  - interval '0.001' second,'GMT') ))
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_style_dim_hist AS pstd 
    ON psd.rms_style_num = cast(pstd.epm_style_num as string)
   AND cast(psd.epm_sku_num as string) = pstd.channel_country  
   AND RANGE_CONTAINS(RANGE(pstd.EFF_BEGIN_TMSTP_UTC, pstd.EFF_END_TMSTP_UTC),TIMESTAMP( datetime(timestamp(isibdf.METRICS_DATE) + INTERVAL '1' DAY  - interval '0.001' second,'GMT')))
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_item_selling_rights_dim AS psrd 
  ON LOWER(psrd.rms_sku_num) = LOWER(isibdf.rms_sku_id) 
    AND LOWER(psrd.channel_country) = LOWER(isibdf.channel_country) 
    AND LOWER(psrd.channel_brand) = LOWER(isibdf.channel_brand) 
    AND LOWER(psrd.selling_channel) = LOWER(isibdf.selling_channel)
    AND RANGE_CONTAINS(RANGE(psrd.eff_begin_tmstp_utc, psrd.eff_end_tmstp_utc), timestamp(datetime(timestamp(isibdf.METRICS_DATE) + INTERVAL '1' DAY  - interval '0.001' second,'GMT') ))
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.vendor_dim AS vd 
  ON LOWER(vd.vendor_num) = LOWER(psd.supp_part_num)
 WHERE isibdf.metrics_date = (SELECT last_sat FROM my_params)
  AND (LOWER(isibdf.selling_channel) = LOWER('ONLINE') AND LOWER(isibdf.store_type_code) IN (LOWER('FC'), LOWER('LH'), LOWER('OC'), LOWER('OF'), LOWER('RK')) OR LOWER(isibdf.selling_channel) = LOWER('STORE') AND LOWER(isibdf.store_type_code) IN (LOWER('FL'), LOWER('NL'), LOWER('RK'), LOWER('SS'), LOWER('VS'))));
       
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS raw_data (
  metrics_date DATE NOT NULL,
  rms_sku_id STRING,
  rms_style_num STRING,
  color_num STRING,
  channel_country STRING NOT NULL,
  channel_brand STRING NOT NULL,
  selling_channel STRING NOT NULL,
  is_online_purchasable STRING,
  online_purchasable_eff_begin_tmstp TIMESTAMP,
  online_purchasable_eff_end_tmstp TIMESTAMP,
  weighted_average_cost NUMERIC,
  promotion_ind STRING,
  regular_price_amt NUMERIC,
  base_retail_drop_ship_amt NUMERIC,
  regular_price_percent_off NUMERIC,
  current_price_amt NUMERIC,
  ownership_price_amt NUMERIC,
  prmy_supp_num STRING,
  supp_part_num STRING,
  compare_at_retail_price_amt NUMERIC,
  base_retail_percentage_off_compare_at_retail_pct NUMERIC,
  drop_ship_eligible_ind STRING,
  npg_ind STRING,
  fp_replenishment_eligible_ind STRING,
  op_replenishment_eligible_ind STRING,
  supp_color STRING,
  return_disposition_code STRING,
  selling_status_code STRING,
  vendor_name STRING,
  vendor_label_name STRING,
  fulfillment_type_code STRING
);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


INSERT INTO raw_data
SELECT inv.metrics_date
  , inv.rms_sku_id
  , inv.rms_style_num
  , TRIM(inv.color_num) as color_num
  , inv.channel_country
  , inv.channel_brand
  , inv.selling_channel
  , purch.is_online_purchasable
  , purch.eff_begin_tmstp AS online_purchasable_eff_begin_tmstp
  , purch.eff_end_tmstp AS online_purchasable_eff_end_tmstp
  , COALESCE(wacd.weighted_average_cost,wacc.weighted_average_cost,0) AS weighted_average_cost
  , (CASE WHEN pptd.selling_retail_price_type_code = 'P' THEN 'Y' ELSE 'N' END) AS promotion_ind
  , CAST(pptd.regular_price_amt AS NUMERIC)
  , pptd.regular_price_amt AS base_retail_drop_ship_amt
  , CAST((pptd.regular_price_amt - pptd.ownership_price_amt) / CAST(COALESCE(pptd.regular_price_amt,0) AS FLOAT64) AS NUMERIC) AS regular_price_percent_off
  , pptd.current_price_amt
  , pptd.ownership_price_amt
  , inv.prmy_supp_num
  , inv.supp_part_num
  , pptd.compare_at_value AS compare_at_retail_price_amt
  , pptd.base_retail_percentage_off_compare_at_value AS base_retail_percentage_off_compare_at_retail_pct
  , inv.drop_ship_eligible_ind
  , inv.npg_ind
  , inv.fp_replenishment_eligible_ind
  , inv.op_replenishment_eligible_ind
  , inv.supp_color
  , inv.return_disposition_code
  , inv.selling_status_code
  , inv.vendor_name
  , inv.vendor_label_name
  , inv.fulfillment_type_code
FROM inv_data inv
LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.STORE_DIM orgstore
ON CAST(TRUNC(CAST(inv.location_id AS FLOAT64)) AS INT64) = orgstore.store_num
LEFT JOIN (
SELECT DISTINCT pptd_price.rms_sku_num,
  pptd_price.channel_country,
  pptd_price.channel_brand,
  pptd_price.selling_channel,
(CASE
WHEN LOWER(pptd_price.selling_retail_price_type_code) = LOWER('REGULAR' )  THEN 'R'
WHEN LOWER(pptd_price.selling_retail_price_type_code) = LOWER('PROMOTION') THEN 'P'
WHEN LOWER(pptd_price.selling_retail_price_type_code) = LOWER('CLEARANCE') THEN 'C'
ELSE pptd_price.selling_retail_price_type_code
END) as selling_retail_price_type_code,
pptd_price.ownership_retail_price_amt     as ownership_price_amt,
pptd_price.regular_price_amt,
pptd_price.selling_retail_price_amt       as current_price_amt,
pptd_price.compare_at_retail_price_amt as compare_at_value,
pptd_price.base_retail_percentage_off_compare_at_retail_pct as base_retail_percentage_off_compare_at_value,
pptd_price.eff_begin_tmstp_utc,
pptd_price.eff_end_tmstp_utc
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_PRICE_TIMELINE_DIM as pptd_price
JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.ETL_BATCH_DT_LKUP AS eltn
ON eltn.INTERFACE_CODE  = 'SMD_INSIGHTS_WKLY'
WHERE pptd_price.eff_end_tmstp_utc   >=  TIMESTAMP(datetime(timestamp(eltn.EXTRACT_START_DT) + INTERVAL '1' DAY  - interval '0.001' second,'GMT'))  
) AS pptd
ON pptd.rms_sku_num = inv.rms_sku_id
AND inv.channel_country = pptd.channel_country
AND pptd.channel_brand = inv.channel_brand
AND pptd.selling_channel = inv.selling_channel
AND RANGE_CONTAINS(RANGE(pptd.EFF_BEGIN_TMSTP_UTC, pptd.EFF_END_TMSTP_UTC), TIMESTAMP(datetime(timestamp(inv.METRICS_DATE) + INTERVAL '1' DAY  - interval '0.001' second,'GMT') ))
LEFT JOIN (
SELECT DISTINCT rms_sku_num,
channel_country,
channel_brand,
selling_channel,
opid.is_online_purchasable,
CAST(eff_begin_tmstp AS TIMESTAMP) AS eff_begin_tmstp,
CAST(eff_end_tmstp AS TIMESTAMP) AS eff_end_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_USR_VWS.PRODUCT_ONLINE_PURCHASABLE_ITEM_DIM AS opid
JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.ETL_BATCH_DT_LKUP AS eltn
ON LOWER(eltn.INTERFACE_CODE)  = LOWER('SMD_INSIGHTS_WKLY')
WHERE CAST(eff_end_tmstp AS TIMESTAMP ) >= CAST(eltn.EXTRACT_START_DT AS TIMESTAMP )
) AS purch
ON purch.rms_sku_num = inv.rms_sku_id
AND purch.channel_country = inv.channel_country
AND purch.channel_brand = inv.channel_brand
 AND RANGE_CONTAINS(RANGE((purch.EFF_BEGIN_TMSTP), (purch.EFF_END_TMSTP)), timestamp(datetime(timestamp(inv.METRICS_DATE) + INTERVAL '1' DAY  - interval '0.001' second,'GMT')) )

LEFT JOIN
(SELECT * FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.WEIGHTED_AVERAGE_COST_DATE_DIM  wac1
WHERE   wac1.EFF_END_DT  >= (select elt.EXTRACT_START_DT from `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.ETL_BATCH_DT_LKUP AS elt
where elt.INTERFACE_CODE  = 'SMD_INSIGHTS_WKLY')
) wacd
ON inv.rms_sku_id = wacd.sku_num
AND CAST(TRUNC( CAST(orgstore.store_num AS FLOAT64)) AS INT64) = CAST(TRUNC( CAST(wacd.location_num AS FLOAT64) )AS INT64)
AND inv.METRICS_DATE >= wacd.EFF_BEGIN_DT
AND inv.METRICS_DATE < wacd.EFF_END_DT
LEFT JOIN
(SELECT * FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.WEIGHTED_AVERAGE_COST_CHANNEL_DIM  wac2
WHERE   wac2.EFF_END_DT  >=  (select elt.EXTRACT_START_DT from `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.ETL_BATCH_DT_LKUP AS elt
where elt.INTERFACE_CODE  = 'SMD_INSIGHTS_WKLY')
) wacc
ON inv.rms_sku_id = wacc.sku_num
AND orgstore.channel_num = wacc.channel_num
AND inv.METRICS_DATE >= wacc.EFF_BEGIN_DT
AND inv.METRICS_DATE < wacc.EFF_END_DT;



BEGIN
SET _ERROR_CODE  =  0;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact 
SET
    anniversary_item_ind = 'N'
WHERE snapshot_date = (SELECT last_sat FROM my_params);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact AS tgt 
SET
    anniversary_item_ind = src.anniversary_item_ind,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
    dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() 
FROM (SELECT DISTINCT isibdf.rms_style_num, isibdf.color_num, isibdf.channel_country, isibdf.selling_channel, isibdf.channel_brand, 'Y' AS anniversary_item_ind
        FROM raw_data AS isibdf
      INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_price_timeline_dim AS pptd_price 
        ON LOWER(isibdf.rms_sku_id) = LOWER(pptd_price.rms_sku_num) 
        AND LOWER(isibdf.channel_country) = LOWER(pptd_price.channel_country) 
        AND LOWER(isibdf.selling_channel) = LOWER(pptd_price.selling_channel) 
        AND LOWER(isibdf.channel_brand) = LOWER(pptd_price.channel_brand)
        AND RANGE_CONTAINS(RANGE(pptd_price.EFF_BEGIN_TMSTP_UTC, case when pptd_price.EFF_END_TMSTP_UTC < CURRENT_TIMESTAMP then pptd_price.EFF_END_TMSTP_UTC +interval 45 day else pptd_price.EFF_END_TMSTP_UTC end), timestamp(datetime(timestamp(isibdf.METRICS_DATE) + INTERVAL '1' DAY  - interval '0.001' second,'GMT') ))
      INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_promotion_timeline_dim AS pptd_promo 
        ON LOWER(isibdf.rms_sku_id) = LOWER(pptd_promo.rms_sku_num) 
        AND LOWER(isibdf.channel_country) = LOWER(pptd_promo.channel_country) 
        AND LOWER(isibdf.selling_channel) = LOWER(pptd_promo.selling_channel)
        AND LOWER(isibdf.channel_brand) = LOWER(pptd_promo.channel_brand) 
        AND LOWER(pptd_price.selling_retail_record_id) = LOWER(pptd_promo.promo_id) 
        AND LOWER(pptd_promo.promotion_type_code) = LOWER('SIMPLE')
        AND RANGE_CONTAINS(RANGE(pptd_promo.EFF_BEGIN_TMSTP_UTC, case when pptd_promo.EFF_END_TMSTP_UTC < CURRENT_TIMESTAMP then pptd_promo.EFF_END_TMSTP_UTC +interval 45 day else pptd_promo.EFF_END_TMSTP_UTC end), TIMESTAMP(datetime(timestamp(isibdf.METRICS_DATE) + INTERVAL '1' DAY  - interval '0.001' second,'GMT') ))
      INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS elt 
        ON LOWER(elt.interface_code) = LOWER('SMD_INSIGHTS_WKLY')
      WHERE LOWER(pptd_promo.enticement_tags) LIKE LOWER('%ANNIVERSARY_SALE%')) AS src
WHERE tgt.snapshot_date = (SELECT last_sat FROM my_params) 
  AND LOWER(tgt.rms_style_num) = LOWER(src.rms_style_num) 
  AND LOWER(tgt.color_num) = LOWER(src.color_num) 
  AND LOWER(tgt.channel_country) = LOWER(src.channel_country) 
  AND LOWER(tgt.selling_channel) = LOWER(src.selling_channel) 
  AND LOWER(tgt.channel_brand) = LOWER(src.channel_brand);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
 SET
    weighted_average_cost = src.weighted_average_cost,
    regular_price_amt = src.regular_price_amt,
    base_retail_drop_ship_amt = CAST(src.base_retail_drop_ship_amt AS NUMERIC),
    regular_price_percent_off = IFNULL(src.regular_price_percent_off, 0),
    promotion_ind = src.promotion_ind,
    rack_ind = 'N',
    drop_ship_eligible_ind = src.drop_ship_eligible_ind,
    npg_ind = src.npg_ind,
    fp_replenishment_eligible_ind = src.fp_replenishment_eligible_ind,
    op_replenishment_eligible_ind = src.op_replenishment_eligible_ind,
    current_price_amt = src.current_price_amt,
    ownership_price_amt = src.ownership_price_amt,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
    dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() 
FROM
(SELECT rms_style_num, color_num, channel_country, channel_brand, selling_channel, AVG(weighted_average_cost) AS weighted_average_cost, MIN(regular_price_amt) AS regular_price_amt, AVG(CASE WHEN LOWER(drop_ship_eligible_ind) = LOWER('Y') THEN regular_price_amt ELSE NULL END) AS base_retail_drop_ship_amt, AVG(regular_price_percent_off) AS regular_price_percent_off, MAX(promotion_ind) AS promotion_ind, MAX(drop_ship_eligible_ind) AS drop_ship_eligible_ind, MAX(npg_ind) AS npg_ind, MAX(fp_replenishment_eligible_ind) AS fp_replenishment_eligible_ind, MAX(op_replenishment_eligible_ind) AS op_replenishment_eligible_ind, MIN(current_price_amt) AS current_price_amt, MIN(ownership_price_amt) AS ownership_price_amt
    FROM raw_data
    GROUP BY rms_style_num, color_num, channel_country, channel_brand, selling_channel) AS src  
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT last_sat  FROM my_params) 
  AND LOWER(COALESCE(src.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) 
  AND LOWER(COALESCE(src.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) 
  AND LOWER(src.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country) 
  AND LOWER(src.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand) 
  AND LOWER(src.selling_channel) = LOWER(clearance_markdown_insights_by_week_fact.selling_channel);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
SET
    price_variance_ind = src.price_variance_ind,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) ,
    dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() 
FROM
(SELECT rms_style_num, color_num, channel_country, channel_brand, selling_channel, CASE WHEN MIN(ownership_price_amt) <> MAX(ownership_price_amt) THEN 'Y' ELSE 'N' END AS price_variance_ind
    FROM raw_data
    GROUP BY rms_style_num, color_num, channel_country, channel_brand, selling_channel) AS src 
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT last_sat  FROM my_params) 
  AND LOWER(COALESCE(src.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) 
  AND LOWER(COALESCE(src.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) 
  AND LOWER(src.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country) 
  AND LOWER(src.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand) 
  AND LOWER(src.selling_channel) = LOWER(clearance_markdown_insights_by_week_fact.selling_channel);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
SET
    msrp_amt = t2.msrp_amt,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
    dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() 
FROM
(SELECT rms_style_num, color_num, channel_country, channel_brand, selling_channel, CASE WHEN compare_at_retail_price_amt IS NULL THEN regular_price_amt ELSE compare_at_retail_price_amt END AS msrp_amt, COUNT(CASE WHEN compare_at_retail_price_amt IS NULL THEN regular_price_amt ELSE compare_at_retail_price_amt END) AS msrp_amt_count
    FROM raw_data
    GROUP BY rms_style_num, color_num, channel_country, channel_brand, selling_channel, msrp_amt
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_style_num, color_num, channel_country, channel_brand, selling_channel ORDER BY msrp_amt_count DESC, msrp_amt)) = 1) AS t2 
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT last_sat  FROM my_params) 
  AND LOWER(COALESCE(t2.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) 
  AND LOWER(COALESCE(t2.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) 
  AND LOWER(t2.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country) 
  AND LOWER(t2.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand) 
  AND LOWER(t2.selling_channel) = LOWER(clearance_markdown_insights_by_week_fact.selling_channel);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
SET
    compare_at_percent_off = t2.compare_at_percent_off,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) ,
    dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() 
FROM 
    (SELECT rms_style_num, color_num, channel_country, channel_brand, selling_channel, CASE WHEN compare_at_retail_price_amt IS NULL THEN regular_price_amt ELSE compare_at_retail_price_amt END AS msrp_amt, COALESCE(base_retail_percentage_off_compare_at_retail_pct, 0) AS compare_at_percent_off, COUNT(CASE WHEN compare_at_retail_price_amt IS NULL THEN regular_price_amt ELSE compare_at_retail_price_amt END) AS msrp_amt_count, COUNT(base_retail_percentage_off_compare_at_retail_pct) AS compare_at_percent_off_count
    FROM raw_data
    GROUP BY rms_style_num, color_num, channel_country, channel_brand, selling_channel, msrp_amt, compare_at_percent_off
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_style_num, color_num, channel_country, channel_brand, selling_channel ORDER BY msrp_amt_count DESC, msrp_amt, compare_at_percent_off_count DESC, compare_at_percent_off)) = 1) AS t2 
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT last_sat  FROM my_params) AND LOWER(COALESCE(t2.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) AND LOWER(COALESCE(t2.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) AND LOWER(t2.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country) AND LOWER(t2.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand) AND LOWER(t2.selling_channel) = LOWER(clearance_markdown_insights_by_week_fact.selling_channel) AND t2.msrp_amt = clearance_markdown_insights_by_week_fact.msrp_amt;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
SET
    return_disposition_code = t2.return_disposition_code 
FROM
    (SELECT rms_style_num, color_num, channel_country, channel_brand, selling_channel, return_disposition_code, CASE WHEN LOWER(return_disposition_code) = LOWER('RTI') THEN 1 WHEN LOWER(return_disposition_code) = LOWER('RTS') THEN 2 WHEN LOWER(return_disposition_code) = LOWER('QA') THEN 3 WHEN LOWER(return_disposition_code) = LOWER('RCK') THEN 4 WHEN LOWER(return_disposition_code) = LOWER('RK7') THEN 5 ELSE 6 END AS return_disposition_code_priority, COUNT(return_disposition_code) AS return_disposition_code_cnt
    FROM raw_data
    WHERE LOWER(selling_channel) IN (LOWER('STORE'), LOWER('ONLINE'))
    GROUP BY rms_style_num, color_num, channel_country, channel_brand, selling_channel, return_disposition_code
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_style_num, color_num, channel_country, channel_brand, selling_channel ORDER BY CASE WHEN LOWER(return_disposition_code) = LOWER('RTI') THEN 1 WHEN LOWER(return_disposition_code) = LOWER('RTS') THEN 2 WHEN LOWER(return_disposition_code) = LOWER('QA') THEN 3 WHEN LOWER(return_disposition_code) = LOWER('RCK') THEN 4 WHEN LOWER(return_disposition_code) = LOWER('RK7') THEN 5 ELSE 6 END, return_disposition_code_cnt DESC)) = 1) AS t2
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT last_sat   FROM my_params) AND LOWER(COALESCE(t2.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) AND LOWER(COALESCE(t2.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) AND LOWER(t2.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country) AND LOWER(t2.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand) AND LOWER(t2.selling_channel) = LOWER(clearance_markdown_insights_by_week_fact.selling_channel) AND LOWER(clearance_markdown_insights_by_week_fact.selling_channel) IN (LOWER('STORE'), LOWER('ONLINE'));

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
SET
    selling_status_code = src.selling_status_code,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) ,
    dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() 
FROM 
    (SELECT rms_style_num, color_num, channel_country, channel_brand, selling_channel, CASE WHEN MAX(CASE WHEN LOWER(selling_status_code) = LOWER('UNBLOCKED') THEN 1 ELSE 0 END) > 0 THEN 'UNBLOCKED' WHEN MIN(CASE WHEN LOWER(selling_status_code) = LOWER('BLOCKED') THEN 1 ELSE 0 END) = 1 THEN 'BLOCKED' ELSE 'UNBLOCKED' END AS selling_status_code
    FROM raw_data
    GROUP BY rms_style_num, color_num, channel_country, channel_brand, selling_channel) AS src 
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT last_sat FROM my_params) AND LOWER(COALESCE(src.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) AND LOWER(COALESCE(src.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) AND LOWER(src.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country) AND LOWER(src.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand) AND LOWER(src.selling_channel) = LOWER(clearance_markdown_insights_by_week_fact.selling_channel);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
SET
    prmy_supp_num = src.prmy_supp_num,
    supp_color = src.supp_color,
    supp_part_num = src.supp_part_num,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) ,
    dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() 
FROM
    (SELECT rms_style_num, color_num, channel_country, channel_brand, selling_channel, MIN(prmy_supp_num) AS prmy_supp_num, MIN(supp_part_num) AS supp_part_num, MIN(supp_color) AS supp_color
    FROM raw_data
    GROUP BY rms_style_num, color_num, channel_country, channel_brand, selling_channel) AS src 
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT last_sat FROM my_params) AND LOWER(COALESCE(src.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) AND LOWER(COALESCE(src.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) AND LOWER(src.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country) AND LOWER(src.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand) AND LOWER(src.selling_channel) = LOWER(clearance_markdown_insights_by_week_fact.selling_channel);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
 SET
    vendor_name = t1.vendor_name,
    vendor_label_name = t1.vendor_label_name,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) ,
    dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() 
FROM 
  (SELECT rms_style_num, color_num, channel_country, channel_brand, selling_channel, vendor_name, vendor_label_name, COUNT(vendor_name) AS vendor_name_cnt
    FROM raw_data
    GROUP BY rms_style_num, color_num, channel_country, channel_brand, selling_channel, vendor_name, vendor_label_name
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_style_num, color_num, channel_country, channel_brand, selling_channel ORDER BY vendor_name_cnt DESC, vendor_name, vendor_label_name)) = 1) AS t1
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT last_sat   FROM my_params) AND LOWER(COALESCE(t1.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) AND LOWER(COALESCE(t1.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) AND LOWER(t1.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country) AND LOWER(t1.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand) AND LOWER(t1.selling_channel) = LOWER(clearance_markdown_insights_by_week_fact.selling_channel);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
SET
    first_rack_date = src.first_rack_date,
    rack_ind = 'Y',
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) ,
    dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() 
FROM 
(SELECT trn.rms_style_num, trn.color_num, trn.channel_country, trn.selling_channel, MIN(trn.event_date) AS first_rack_date
    FROM (SELECT DISTINCT psd.rms_style_num, psd.color_num, psdv.channel_country, psdv.channel_brand, psdv.selling_channel, tclf.event_date  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.transfer_created_logical_fact AS tclf
                INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.price_store_dim_vw AS psdv ON CAST(SUBSTR(SUBSTR(CAST(tclf.to_location_id AS STRING), 1, 10), 3 * -1) AS FLOAT64) = psdv.store_num
                INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_vw AS psd ON LOWER(tclf.rms_sku_num) = LOWER(psd.rms_sku_num) AND LOWER(psdv.channel_country) = LOWER(psd.channel_country)
                INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_fact AS cmibwf ON LOWER(COALESCE(psd.rms_style_num, 'NA')) = LOWER(COALESCE(cmibwf.rms_style_num, 'NA')) AND LOWER(COALESCE(psd.color_num, 'NA')) = LOWER(COALESCE(cmibwf.color_num, 'NA')) AND LOWER(psdv.channel_country) = LOWER(cmibwf.channel_country) AND LOWER(psdv.channel_brand) = LOWER(cmibwf.channel_brand) AND LOWER(psdv.selling_channel) = LOWER(cmibwf.selling_channel)
            WHERE LOWER(tclf.transfer_context_value) = LOWER('RKING') AND cmibwf.snapshot_date = (SELECT last_sat  FROM my_params)) AS trn
        LEFT JOIN (SELECT psd0.rms_style_num, psd0.color_num, cmrf.channel_country, cmrf.channel_brand, MAX(cmrf.effective_tmstp) AS last_reset_date
            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_reset_fact AS cmrf
                INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_vw AS psd0 ON LOWER(cmrf.rms_sku_num) = LOWER(psd0.rms_sku_num) AND LOWER(cmrf.channel_country) = LOWER(psd0.channel_country)
                INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_fact AS cmibwf0 ON LOWER(COALESCE(psd0.rms_style_num, 'NA')) = LOWER(COALESCE(cmibwf0.rms_style_num, 'NA')) AND LOWER(COALESCE(psd0.color_num, 'NA')) = LOWER(COALESCE(cmibwf0.color_num, 'NA')) AND LOWER(cmrf.channel_country) = LOWER(cmibwf0.channel_country) AND LOWER(cmrf.channel_brand) = LOWER(cmibwf0.channel_brand) AND LOWER(cmrf.selling_channel) = LOWER(cmibwf0.selling_channel)
            WHERE cmibwf0.snapshot_date = (SELECT last_sat  FROM my_params)
            GROUP BY cmrf.channel_country, cmrf.channel_brand, psd0.rms_style_num, psd0.color_num) AS rst ON LOWER(COALESCE(trn.rms_style_num, 'NA')) = LOWER(COALESCE(rst.rms_style_num, 'NA')) AND LOWER(COALESCE(trn.color_num, 'NA')) = LOWER(COALESCE(rst.color_num, 'NA')) AND LOWER(trn.channel_country) = LOWER(rst.channel_country) AND LOWER(trn.channel_brand) = LOWER(rst.channel_brand)
    WHERE rst.last_reset_date IS NULL OR CAST(rst.last_reset_date AS DATE) <= trn.event_date
    GROUP BY trn.rms_style_num, trn.color_num, trn.channel_country, trn.selling_channel) AS src
WHERE LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) = LOWER(COALESCE(src.rms_style_num, 'NA')) AND LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) = LOWER(COALESCE(src.color_num, 'NA')) AND LOWER(clearance_markdown_insights_by_week_fact.channel_country) = LOWER(src.channel_country) AND LOWER(clearance_markdown_insights_by_week_fact.selling_channel) = LOWER(src.selling_channel) AND clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT last_sat   FROM my_params);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
SET
    weighted_average_cost = omni.weighted_average_cost,
    regular_price_amt = omni.regular_price_amt,
    base_retail_drop_ship_amt = CAST(omni.base_retail_drop_ship_amt AS NUMERIC),
    regular_price_percent_off = IFNULL(omni.regular_price_percent_off, 0),
    promotion_ind = omni.promotion_ind,
    rack_ind = 'N',
    drop_ship_eligible_ind = omni.drop_ship_eligible_ind,
    npg_ind = omni.npg_ind,
    fp_replenishment_eligible_ind = omni.fp_replenishment_eligible_ind,
    op_replenishment_eligible_ind = omni.op_replenishment_eligible_ind,
    current_price_amt = omni.current_price_amt,
    ownership_price_amt = omni.ownership_price_amt,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
    dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()  
FROM
(SELECT rms_style_num, color_num, channel_country, channel_brand, AVG(weighted_average_cost) AS weighted_average_cost, MIN(regular_price_amt) AS regular_price_amt, AVG(CASE WHEN LOWER(drop_ship_eligible_ind) = LOWER('Y') THEN regular_price_amt ELSE NULL END) AS base_retail_drop_ship_amt, AVG(regular_price_percent_off) AS regular_price_percent_off, MAX(promotion_ind) AS promotion_ind, MAX(drop_ship_eligible_ind) AS drop_ship_eligible_ind, MAX(npg_ind) AS npg_ind, MAX(fp_replenishment_eligible_ind) AS fp_replenishment_eligible_ind, MAX(op_replenishment_eligible_ind) AS op_replenishment_eligible_ind, MIN(current_price_amt) AS current_price_amt, MIN(ownership_price_amt) AS ownership_price_amt
    FROM raw_data
    GROUP BY rms_style_num, color_num, channel_country, channel_brand) AS omni 
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT last_sat
                            FROM my_params) AND LOWER(COALESCE(omni.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) AND LOWER(COALESCE(omni.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) AND LOWER(omni.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country) AND LOWER(omni.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand) AND (LOWER(clearance_markdown_insights_by_week_fact.selling_channel) = LOWER('OMNI') OR clearance_markdown_insights_by_week_fact.total_inv_qty = 0);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact SET
    anniversary_item_ind = 'N'
WHERE snapshot_date = (SELECT last_sat
            FROM my_params) AND (LOWER(selling_channel) = LOWER('OMNI') OR total_inv_qty = 0);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact AS tgt 
SET
	anniversary_item_ind = src.anniversary_item_ind,
	dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) ,
  dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() 
FROM 
(
		SELECT DISTINCT
			isibdf.rms_style_num,
			isibdf.color_num,
			isibdf.channel_country,
			isibdf.selling_channel,
			isibdf.channel_brand,
			'Y' AS anniversary_item_ind
		FROM raw_data AS isibdf
	   JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_PRICE_TIMELINE_DIM as pptd_price
        ON  isibdf.rms_sku_id       = pptd_price.rms_sku_num
        AND isibdf.channel_country  = pptd_price.channel_country
        AND isibdf.selling_channel  = pptd_price.selling_channel
        AND isibdf.channel_brand    = pptd_price.channel_brand
        AND RANGE_CONTAINS(RANGE(pptd_price.EFF_BEGIN_TMSTP_UTC, case when pptd_price.EFF_END_TMSTP_UTC < CURRENT_TIMESTAMP then pptd_price.EFF_END_TMSTP_UTC +interval 45 day else pptd_price.EFF_END_TMSTP_UTC end), TIMESTAMP(datetime(timestamp(isibdf.METRICS_DATE) + INTERVAL '1' DAY  - interval '0.001' second,'GMT') ))
	   JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_USR_VWS.PRODUCT_PROMOTION_TIMELINE_DIM pptd_promo
        ON  isibdf.rms_sku_id       = pptd_promo.rms_sku_num
        AND isibdf.channel_country  = pptd_promo.channel_country
        AND isibdf.selling_channel  = pptd_promo.selling_channel
        AND isibdf.channel_brand    = pptd_promo.channel_brand
        AND pptd_price.selling_retail_record_id	= pptd_promo.promo_id
        AND pptd_promo.promotion_type_code = 'SIMPLE'
        AND RANGE_CONTAINS(RANGE(pptd_promo.EFF_BEGIN_TMSTP_UTC, case when pptd_promo.EFF_END_TMSTP_UTC < CURRENT_TIMESTAMP then pptd_promo.EFF_END_TMSTP_UTC +interval 45 day else pptd_promo.EFF_END_TMSTP_UTC end), TIMESTAMP(datetime(timestamp(isibdf.METRICS_DATE) + INTERVAL '1' DAY  - interval '0.001' second,'GMT') ))
		JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.ETL_BATCH_DT_LKUP AS elt ON elt.INTERFACE_CODE  = 'SMD_INSIGHTS_WKLY'
		WHERE  pptd_promo.enticement_tags LIKE '%ANNIVERSARY_SALE%'
) AS src
 WHERE tgt.snapshot_date = (SELECT last_SAT from my_params)
 AND tgt.rms_style_num		= src.rms_style_num
AND tgt.color_num			  = src.color_num
AND tgt.channel_country	= src.channel_country
AND tgt.selling_channel  = src.selling_channel
AND tgt.channel_brand    = src.channel_brand
AND (tgt.selling_channel = 'OMNI'
  OR tgt.total_inv_qty   = 0) ;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
SET
    price_variance_ind = omni.price_variance_ind,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) ,
    dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() 
FROM 
    (SELECT rms_style_num, color_num, channel_country, channel_brand, CASE WHEN MIN(ownership_price_amt) <> MAX(ownership_price_amt) THEN 'Y' ELSE 'N' END AS price_variance_ind
    FROM raw_data
    GROUP BY rms_style_num, color_num, channel_country, channel_brand) AS omni 
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT last_sat  FROM my_params) AND LOWER(COALESCE(omni.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) AND LOWER(COALESCE(omni.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) AND LOWER(omni.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country) AND LOWER(omni.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand) AND (LOWER(clearance_markdown_insights_by_week_fact.selling_channel) = LOWER('OMNI') OR clearance_markdown_insights_by_week_fact.total_inv_qty = 0);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
SET
    msrp_amt = t2.msrp_amt,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) ,
    dw_sys_updt_tmstp_tz =`{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() 
FROM
    (SELECT rms_style_num, color_num, channel_country, channel_brand, CASE WHEN compare_at_retail_price_amt IS NULL THEN regular_price_amt ELSE compare_at_retail_price_amt END AS msrp_amt, COUNT(CASE WHEN compare_at_retail_price_amt IS NULL THEN regular_price_amt ELSE compare_at_retail_price_amt END) AS msrp_amt_count
    FROM raw_data
    GROUP BY rms_style_num, color_num, channel_country, channel_brand, msrp_amt
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_style_num, color_num, channel_country, channel_brand ORDER BY msrp_amt_count DESC, msrp_amt)) = 1) AS t2 
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT last_sat    FROM my_params) AND LOWER(COALESCE(t2.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) AND LOWER(COALESCE(t2.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) AND LOWER(t2.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country) AND LOWER(t2.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand) AND (LOWER(clearance_markdown_insights_by_week_fact.selling_channel) = LOWER('OMNI') OR clearance_markdown_insights_by_week_fact.total_inv_qty = 0);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
SET
    compare_at_percent_off = t2.compare_at_percent_off,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) ,
    dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() 
FROM 
(SELECT rms_style_num, color_num, channel_country, channel_brand, CASE WHEN compare_at_retail_price_amt IS NULL THEN regular_price_amt ELSE compare_at_retail_price_amt END AS msrp_amt, COALESCE(base_retail_percentage_off_compare_at_retail_pct, 0) AS compare_at_percent_off, COUNT(CASE WHEN compare_at_retail_price_amt IS NULL THEN regular_price_amt ELSE compare_at_retail_price_amt END) AS msrp_amt_count, COUNT(COALESCE(base_retail_percentage_off_compare_at_retail_pct, 0)) AS compare_at_percent_off_count
    FROM raw_data
    GROUP BY rms_style_num, color_num, channel_country, channel_brand, msrp_amt, compare_at_percent_off
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_style_num, color_num, channel_country, channel_brand ORDER BY msrp_amt_count DESC, msrp_amt, compare_at_percent_off_count DESC, compare_at_percent_off)) = 1) AS t2 
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT last_sat  FROM my_params) AND LOWER(COALESCE(t2.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) AND LOWER(COALESCE(t2.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) AND LOWER(t2.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country) AND LOWER(t2.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand) AND t2.msrp_amt = clearance_markdown_insights_by_week_fact.msrp_amt AND (LOWER(clearance_markdown_insights_by_week_fact.selling_channel) = LOWER('OMNI') OR clearance_markdown_insights_by_week_fact.total_inv_qty = 0);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
SET
    return_disposition_code = t2.return_disposition_code 
FROM 
     (SELECT rms_style_num, color_num, channel_country, channel_brand, selling_channel, return_disposition_code, CASE WHEN LOWER(return_disposition_code) = LOWER('RTI') THEN 1 WHEN LOWER(return_disposition_code) = LOWER('RTS') THEN 2 WHEN LOWER(return_disposition_code) = LOWER('QA') THEN 3 WHEN LOWER(return_disposition_code) = LOWER('RCK') THEN 4 WHEN LOWER(return_disposition_code) = LOWER('RK7') THEN 5 ELSE 6 END AS return_disposition_code_priority, COUNT(return_disposition_code) AS return_disposition_code_cnt
    FROM raw_data
    WHERE LOWER(selling_channel) = LOWER('OMNI')
    GROUP BY rms_style_num, color_num, channel_country, channel_brand, selling_channel, return_disposition_code
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_style_num, color_num, channel_country, channel_brand, selling_channel ORDER BY CASE WHEN LOWER(return_disposition_code) = LOWER('RTI') THEN 1 WHEN LOWER(return_disposition_code) = LOWER('RTS') THEN 2 WHEN LOWER(return_disposition_code) = LOWER('QA') THEN 3 WHEN LOWER(return_disposition_code) = LOWER('RCK') THEN 4 WHEN LOWER(return_disposition_code) = LOWER('RK7') THEN 5 ELSE 6 END, return_disposition_code_cnt DESC)) = 1) AS t2 
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT last_sat
                                FROM my_params) AND LOWER(COALESCE(t2.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) AND LOWER(COALESCE(t2.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) AND LOWER(t2.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country) AND LOWER(t2.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand) AND LOWER(t2.selling_channel) = LOWER(clearance_markdown_insights_by_week_fact.selling_channel) AND LOWER(clearance_markdown_insights_by_week_fact.selling_channel) = LOWER('OMNI');
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
SET
    selling_status_code = omni.selling_status_code,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) ,
    dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() 
FROM 
    (SELECT rms_style_num, color_num, channel_country, channel_brand, CASE WHEN MAX(CASE WHEN LOWER(selling_status_code) = LOWER('UNBLOCKED') THEN 1 ELSE 0 END) > 0 THEN 'UNBLOCKED' WHEN MIN(CASE WHEN LOWER(selling_status_code) = LOWER('BLOCKED') THEN 1 ELSE 0 END) = 1 THEN 'BLOCKED' ELSE 'UNBLOCKED' END AS selling_status_code
    FROM raw_data
    GROUP BY rms_style_num, color_num, channel_country, channel_brand) AS omni 
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT last_sat
                            FROM my_params) AND LOWER(COALESCE(omni.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) AND LOWER(COALESCE(omni.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) AND LOWER(omni.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country) AND LOWER(omni.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand) AND (LOWER(clearance_markdown_insights_by_week_fact.selling_channel) = LOWER('OMNI') OR clearance_markdown_insights_by_week_fact.total_inv_qty = 0);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
SET
    prmy_supp_num = omni.prmy_supp_num,
    supp_color = omni.supp_color,
    supp_part_num = omni.supp_part_num,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) ,
    dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() 
FROM
    (SELECT rms_style_num, color_num, channel_country, channel_brand, MIN(prmy_supp_num) AS prmy_supp_num, MIN(supp_part_num) AS supp_part_num, MIN(supp_color) AS supp_color
    FROM raw_data
    GROUP BY rms_style_num, color_num, channel_country, channel_brand) AS omni 
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT last_sat
                            FROM my_params) AND LOWER(COALESCE(omni.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) AND LOWER(COALESCE(omni.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) AND LOWER(omni.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country) AND LOWER(omni.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand) AND (LOWER(clearance_markdown_insights_by_week_fact.selling_channel) = LOWER('OMNI') OR clearance_markdown_insights_by_week_fact.total_inv_qty = 0);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
SET
    vendor_name = t1.vendor_name,
    vendor_label_name = t1.vendor_label_name,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) ,
    dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() 
FROM
    (SELECT rms_style_num, color_num, channel_country, channel_brand, vendor_name, vendor_label_name, COUNT(vendor_name) AS vendor_name_cnt
    FROM raw_data
    GROUP BY rms_style_num, color_num, channel_country, channel_brand, vendor_name, vendor_label_name
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_style_num, color_num, channel_country, channel_brand ORDER BY vendor_name_cnt DESC, vendor_name, vendor_label_name)) = 1) AS t1 
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT last_sat  FROM my_params) AND LOWER(COALESCE(t1.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) AND LOWER(COALESCE(t1.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) AND LOWER(t1.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country) AND LOWER(t1.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand) AND (LOWER(clearance_markdown_insights_by_week_fact.selling_channel) = LOWER('OMNI') OR clearance_markdown_insights_by_week_fact.total_inv_qty = 0);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
SET
    is_online_purchasable = iop.is_online_purchasable FROM 
    (SELECT rd.rms_style_num, rd.color_num, rd.channel_country, rd.channel_brand, rd.selling_channel, CASE WHEN MAX(CASE WHEN LOWER(popid.is_online_purchasable) = LOWER('Y') THEN 1 ELSE 0 END) = 1 THEN 'Y' WHEN MAX(CASE WHEN LOWER(popid.is_online_purchasable) = LOWER('N') THEN 1 ELSE 0 END) = 1 THEN 'N' ELSE NULL END AS is_online_purchasable
    FROM raw_data AS rd
        LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_online_purchasable_item_dim AS popid ON LOWER(popid.rms_sku_num) = LOWER(rd.rms_sku_id) AND LOWER(popid.channel_country) = LOWER(rd.channel_country) AND LOWER(popid.channel_brand) = LOWER(rd.channel_brand) AND LOWER(popid.selling_channel) = LOWER(rd.selling_channel) AND LOWER(rd.selling_channel) IN (LOWER('ONLINE'), LOWER('STORE')) 
        AND cast(rd.metrics_date as timestamp) BETWEEN popid.eff_begin_tmstp_utc AND popid.eff_end_tmstp_utc 
        AND RANGE_CONTAINS(RANGE(popid.eff_begin_tmstp_utc, popid.eff_end_tmstp_utc), TIMESTAMP(datetime(timestamp(rd.metrics_date) + INTERVAL '1' DAY  - interval '0.001' second,'GMT') ))
    WHERE rd.metrics_date = (SELECT last_sat FROM my_params)
    GROUP BY rd.rms_style_num, rd.color_num, rd.channel_country, rd.channel_brand, rd.selling_channel) AS iop 
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT last_sat FROM my_params) AND LOWER(COALESCE(iop.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) AND LOWER(COALESCE(iop.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) AND LOWER(iop.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country) AND LOWER(iop.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand) AND LOWER(iop.selling_channel) = LOWER(clearance_markdown_insights_by_week_fact.selling_channel);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
SET
    is_online_purchasable = iop_omni.is_online_purchasable FROM 
    (SELECT rd.rms_style_num, rd.color_num, rd.channel_country, rd.channel_brand, CASE WHEN MAX(CASE WHEN LOWER(popid.is_online_purchasable) = LOWER('Y') THEN 1 ELSE 0 END) = 1 THEN 'Y' ELSE NULL END AS is_online_purchasable
    FROM raw_data AS rd
        LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_online_purchasable_item_dim AS popid ON LOWER(popid.rms_sku_num) = LOWER(rd.rms_sku_id) AND LOWER(popid.channel_country) = LOWER(rd.channel_country) AND LOWER(popid.channel_brand) = LOWER(rd.channel_brand) AND LOWER(rd.selling_channel) IN (LOWER('ONLINE'), LOWER('STORE')) AND LOWER(popid.selling_channel) = LOWER(rd.selling_channel) AND cast(rd.metrics_date as timestamp) BETWEEN popid.eff_begin_tmstp_utc AND popid.eff_end_tmstp_utc
        AND RANGE_CONTAINS(RANGE(popid.eff_begin_tmstp_utc, popid.eff_end_tmstp_utc), timestamp(datetime(timestamp(rd.metrics_date) + INTERVAL '1' DAY  - interval '0.001' second,'GMT') ))
    WHERE rd.metrics_date = (SELECT last_sat FROM my_params)
    GROUP BY rd.rms_style_num, rd.color_num, rd.channel_country, rd.channel_brand) AS iop_omni 
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT last_sat  FROM my_params) AND LOWER(COALESCE(iop_omni.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) AND LOWER(COALESCE(iop_omni.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) AND LOWER(iop_omni.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country) AND LOWER(iop_omni.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand) AND LOWER(clearance_markdown_insights_by_week_fact.selling_channel) = LOWER('OMNI');
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS temp_popid_records AS
SELECT rd.rms_style_num,
 rd.color_num,
 rd.channel_country,
 rd.channel_brand,
 rd.selling_channel,
 popid.is_online_purchasable,
 MIN(popid.eff_begin_tmstp_utc) AS online_purchasable_eff_begin_tmstp,
 MAX(popid.eff_end_tmstp_utc) AS online_purchasable_eff_end_tmstp
FROM raw_data AS rd
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_online_purchasable_item_dim AS popid ON LOWER(popid.rms_sku_num) = LOWER(rd.rms_sku_id ) AND LOWER(popid.channel_country) = LOWER(rd.channel_country) AND LOWER(popid.channel_brand) = LOWER(rd.channel_brand ) 
 AND cast(rd.metrics_date as timestamp) BETWEEN popid.eff_begin_tmstp_utc AND popid.eff_end_tmstp_utc
AND RANGE_CONTAINS(RANGE(popid.eff_begin_tmstp_utc, popid.eff_end_tmstp_utc), TIMESTAMP(datetime(timestamp(rd.metrics_date) + INTERVAL '1' DAY  - interval '0.001' second,'GMT') ))
WHERE rd.metrics_date = (SELECT last_sat   FROM my_params)
 AND LOWER(popid.selling_channel) = LOWER(rd.selling_channel)
 AND LOWER(rd.selling_channel) IN (LOWER('ONLINE'), LOWER('STORE'))
 AND LOWER(popid.is_online_purchasable) IN (LOWER('Y'), LOWER('N'))
GROUP BY rd.rms_style_num,
 rd.color_num,
 rd.channel_country,
 rd.channel_brand,
 rd.selling_channel,
 popid.is_online_purchasable;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
 SET
    online_purchasable_eff_begin_tmstp = temp.online_purchasable_eff_begin_tmstp,
    online_purchasable_eff_end_tmstp = temp.online_purchasable_eff_end_tmstp FROM
    (SELECT rms_style_num, color_num, channel_country, channel_brand, online_purchasable_eff_begin_tmstp, online_purchasable_eff_end_tmstp, is_online_purchasable, selling_channel
    FROM temp_popid_records AS temp) AS temp 
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT last_sat   FROM my_params) AND LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) = LOWER(COALESCE(temp.rms_style_num, 'NA')) AND LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) = LOWER(COALESCE(temp.color_num, 'NA')) AND LOWER(clearance_markdown_insights_by_week_fact.channel_country) = LOWER(temp.channel_country) AND LOWER(clearance_markdown_insights_by_week_fact.channel_brand) = LOWER(temp.channel_brand) AND LOWER(clearance_markdown_insights_by_week_fact.is_online_purchasable) = LOWER(temp.is_online_purchasable) AND LOWER(clearance_markdown_insights_by_week_fact.selling_channel) = LOWER(temp.selling_channel);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
    
     SET
    online_purchasable_eff_begin_tmstp = temp.online_purchasable_eff_begin_tmstp,
    online_purchasable_eff_end_tmstp = temp.online_purchasable_eff_end_tmstp FROM (SELECT rms_style_num, color_num, channel_country, channel_brand, MIN(online_purchasable_eff_begin_tmstp) AS online_purchasable_eff_begin_tmstp, MAX(online_purchasable_eff_end_tmstp) AS online_purchasable_eff_end_tmstp, is_online_purchasable
    FROM temp_popid_records AS temp
    GROUP BY rms_style_num, color_num, channel_country, channel_brand, is_online_purchasable) AS temp 
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT last_sat
                                FROM my_params) AND LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) = LOWER(COALESCE(temp.rms_style_num, 'NA')) AND LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) = LOWER(COALESCE(temp.color_num, 'NA')) AND LOWER(clearance_markdown_insights_by_week_fact.channel_country) = LOWER(temp.channel_country) AND LOWER(clearance_markdown_insights_by_week_fact.channel_brand) = LOWER(temp.channel_brand) AND LOWER(clearance_markdown_insights_by_week_fact.is_online_purchasable) = LOWER(temp.is_online_purchasable) AND LOWER(clearance_markdown_insights_by_week_fact.selling_channel) = LOWER('OMNI');
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS on_order (
rms_style_num STRING,
color_num STRING,
channel_country STRING NOT NULL,
channel_brand STRING NOT NULL,
selling_channel STRING NOT NULL,
store_type_code STRING,
store_abbrev_name STRING,
on_order_units_qty INTEGER
);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO on_order
(SELECT psd.rms_style_num,
  psd.color_num,
  psdv.channel_country,
  psdv.channel_brand,
  psdv.selling_channel,
  psdv.store_type_code,
  psdv.store_abbrev_name,
  oo.quantity_open AS on_order_units_qty
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_on_order_snapshot_fact AS oo
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.price_store_dim_vw AS psdv ON oo.store_num = psdv.store_num
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS psd ON LOWER(oo.rms_sku_num) = LOWER(psd.rms_sku_num) AND LOWER(psdv.channel_country) = LOWER(psd.channel_country)
  AND RANGE_CONTAINS(RANGE(psd.eff_begin_tmstp_utc, psd.eff_end_tmstp_utc), timestamp(datetime(timestamp(oo.snapshot_week_date) + INTERVAL '1' DAY  - interval '0.001' second,'GMT') ))
 WHERE oo.snapshot_week_date = (SELECT last_sat FROM my_params)
  AND LOWER(psdv.store_type_code) IN (LOWER('FC'), LOWER('FL'), LOWER('LH'), LOWER('NL'), LOWER('OC'), LOWER('OF'),  LOWER('RK'), LOWER('SS'), LOWER('VS')));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN(rms_style_num), COLUMN(color_num), COLUMN(channel_country), COLUMN(channel_brand), COLUMN(selling_channel) ON on_order;
BEGIN
SET _ERROR_CODE  =  0;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact 
SET
    on_order_units_qty = 0
WHERE snapshot_date = (SELECT last_sat  FROM my_params);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
SET
    on_order_units_qty = src.on_order_units_qty FROM
    (SELECT rms_style_num, color_num, channel_country, channel_brand, selling_channel, SUM(on_order_units_qty) AS on_order_units_qty
    FROM on_order
    GROUP BY rms_style_num, color_num, channel_country, channel_brand, selling_channel) AS src 
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT last_sat  FROM my_params) AND LOWER(COALESCE(src.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) AND LOWER(COALESCE(src.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) AND LOWER(src.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country) AND LOWER(src.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand) AND LOWER(src.selling_channel) = LOWER(clearance_markdown_insights_by_week_fact.selling_channel);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact 
SET
    on_order_units_qty = omni.on_order_units_qty FROM
    (SELECT rms_style_num, color_num, channel_country, channel_brand, SUM(on_order_units_qty) AS on_order_units_qty
    FROM on_order
    GROUP BY rms_style_num, color_num, channel_country, channel_brand) AS omni 
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT last_sat
                            FROM my_params) AND LOWER(COALESCE(omni.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) AND LOWER(COALESCE(omni.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) AND LOWER(omni.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country) AND LOWER(omni.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand) AND LOWER(clearance_markdown_insights_by_week_fact.selling_channel) = LOWER('OMNI');
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS rack_retail (
rms_style_num STRING,
color_num STRING,
channel_country STRING NOT NULL,
selling_channel STRING NOT NULL,
current_price_amt NUMERIC
);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact SET
    current_rack_retail = clearance_markdown_insights_by_week_fact.regular_price_amt
WHERE snapshot_date = (SELECT last_sat  FROM my_params) AND LOWER(channel_brand) = LOWER('NORDSTROM') AND regular_price_amt IS NOT NULL;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO rack_retail
(SELECT cmibwf.rms_style_num,
  cmibwf.color_num,
  cmibwf.channel_country,
  cmibwf.selling_channel,
  pptd.selling_retail_price_amt AS current_rack_retail
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_fact AS cmibwf
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_vw AS psd ON LOWER(cmibwf.rms_style_num) = LOWER(psd.rms_style_num) AND
     LOWER(cmibwf.color_num) = LOWER(psd.color_num) AND LOWER(cmibwf.channel_country) = LOWER(psd.channel_country)
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_price_timeline_dim AS pptd ON LOWER(psd.rms_sku_num) = LOWER(pptd.rms_sku_num) AND
      LOWER(psd.channel_country) = LOWER(pptd.channel_country) AND LOWER(cmibwf.channel_brand) = LOWER(pptd.channel_brand
      ) AND LOWER(cmibwf.selling_channel) = LOWER(pptd.selling_channel)
 WHERE cmibwf.snapshot_date = (SELECT last_sat
    FROM my_params)
  AND CAST(pptd.eff_begin_tmstp AS DATE) <= (SELECT last_sat  FROM my_params)
  AND CAST(pptd.eff_end_tmstp AS DATE) > (SELECT last_sat   FROM my_params)
  AND LOWER(pptd.channel_brand) = LOWER('NORDSTROM_RACK'));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN(rms_style_num), COLUMN(color_num), COLUMN(channel_country), COLUMN(selling_channel) ON rack_retail;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
SET
    current_rack_retail = t1.current_rack_retail FROM 
    (SELECT rms_style_num, color_num, channel_country, selling_channel, current_price_amt AS current_rack_retail, COUNT(current_price_amt) AS current_rack_retail_cnt
    FROM rack_retail
    GROUP BY rms_style_num, color_num, channel_country, selling_channel, current_price_amt
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_style_num, color_num, channel_country, selling_channel ORDER BY current_rack_retail_cnt DESC, current_price_amt)) = 1) AS t1 
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT last_sat
                            FROM my_params) AND LOWER(COALESCE(t1.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) AND LOWER(COALESCE(t1.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) AND LOWER(t1.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country) AND LOWER(clearance_markdown_insights_by_week_fact.channel_brand) = LOWER('NORDSTROM') AND LOWER(t1.selling_channel) = LOWER(clearance_markdown_insights_by_week_fact.selling_channel);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
SET
    current_rack_retail = t2.current_rack_retail FROM 
    (SELECT rms_style_num, color_num, channel_country, current_price_amt AS current_rack_retail, COUNT(current_price_amt) AS current_rack_retail_cnt
    FROM rack_retail
    GROUP BY rms_style_num, color_num, channel_country, current_rack_retail
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_style_num, color_num, channel_country ORDER BY current_rack_retail_cnt DESC, current_rack_retail)) = 1) AS t2 
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT last_sat
                            FROM my_params) AND LOWER(COALESCE(t2.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) AND LOWER(COALESCE(t2.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) AND LOWER(t2.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country) AND LOWER(clearance_markdown_insights_by_week_fact.channel_brand) = LOWER('NORDSTROM') AND LOWER(clearance_markdown_insights_by_week_fact.selling_channel) = LOWER('OMNI');
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact AS clearance_markdown_insights_by_week_fact0
SET
    first_rack_date = src.first_rack_date,
    rack_ind = 'Y',
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) ,
    dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() 
  FROM
    (SELECT rms_style_num, color_num, channel_country, channel_brand, MIN(first_rack_date) AS first_rack_date
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_fact
    WHERE snapshot_date = (SELECT last_sat  FROM my_params)
    GROUP BY rms_style_num, color_num, channel_country, channel_brand
    HAVING first_rack_date IS NOT NULL) AS src -- AS clearance_markdown_insights_by_week_fact0 
WHERE LOWER(COALESCE(clearance_markdown_insights_by_week_fact0.rms_style_num, 'NA')) = LOWER(COALESCE(src.rms_style_num, 'NA')) AND LOWER(COALESCE(clearance_markdown_insights_by_week_fact0.color_num, 'NA')) = LOWER(COALESCE(src.color_num, 'NA')) AND LOWER(clearance_markdown_insights_by_week_fact0.channel_country) = LOWER(src.channel_country) AND LOWER(clearance_markdown_insights_by_week_fact0.channel_brand) = LOWER(src.channel_brand) AND (LOWER(clearance_markdown_insights_by_week_fact0.selling_channel) = LOWER('OMNI') OR clearance_markdown_insights_by_week_fact0.total_inv_qty = 0) AND clearance_markdown_insights_by_week_fact0.snapshot_date = (SELECT last_sat
            FROM my_params);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN(snapshot_date), COLUMN(rms_style_num), COLUMN(color_num), COLUMN(channel_country), COLUMN(channel_brand), COLUMN(selling_channel) ON `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT;
-- /*SET QUERY_BAND = NONE FOR SESSION;*/
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
END;
