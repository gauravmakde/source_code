BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;

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
  psd.rms_sku_num AS rms_sku_id,
  psd.rms_style_num,
  isibdf.color_num,
  isibdf.channel_country,
  TRIM(isibdf.location_id) AS location_id,
  isibdf.channel_brand,
  isibdf.selling_channel,
  psd.prmy_supp_num,
  psd.supp_part_num,
   CASE
   WHEN CAST(TRUNC(CAST(psd.fulfillment_type_code AS FLOAT64)) AS INTEGER) IN (3040, 4000, 4020, 4040)
   THEN 'Y'
   ELSE 'N'
   END AS drop_ship_eligible_ind,
  psd.npg_ind,
  psd.fp_replenishment_eligible_ind,
  psd.op_replenishment_eligible_ind,
  psd.supp_color,
  psd.return_disposition_code,
  psrd.selling_status_code,
  vd.vendor_name,
  pstd.vendor_label_name,
  psd.fulfillment_type_code
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.inventory_sales_insights_by_day_hist_fact AS isibdf
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl
  ON LOWER(etl.interface_code) = LOWER('CMD_WKLY')
  AND isibdf.metrics_date = etl.dw_batch_dt
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_hist_fact AS cmibwf
  ON LOWER(COALESCE(isibdf.rms_style_num
          , 'NA')) = LOWER(COALESCE(cmibwf.rms_style_num, 'NA')) AND
		  LOWER(COALESCE(isibdf.color_num, 'NA')) = LOWER(COALESCE(cmibwf
          .color_num, 'NA')) 
		  AND LOWER(isibdf.channel_country) = LOWER(cmibwf.channel_country)
		  AND LOWER(isibdf.channel_brand) = LOWER(cmibwf.channel_brand) 
	   AND LOWER(isibdf.selling_channel) = LOWER(cmibwf.selling_channel) 
	   AND etl.dw_batch_dt = cmibwf.snapshot_date
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS psd
  ON LOWER(COALESCE(psd.rms_style_num, 'NA')) = LOWER(COALESCE(isibdf
       .rms_style_num, 'NA')) 
	   AND LOWER(COALESCE(psd.color_num, 'NA')) = LOWER(COALESCE(isibdf.color_num, 'NA')) 
	   AND LOWER(psd.channel_country) = LOWER(isibdf.channel_country)
     
	     AND RANGE_CONTAINS(RANGE(psd.EFF_BEGIN_TMSTP_UTC, psd.EFF_END_TMSTP_UTC), TIMESTAMP(datetime(timestamp(isibdf.METRICS_DATE) + INTERVAL '1' DAY  - interval '0.001' second,'GMT') ))

  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_style_dim_hist AS pstd ON
  psd.epm_style_num = pstd.epm_style_num 
  AND LOWER(psd.channel_country) = LOWER(pstd.channel_country)
   AND RANGE_CONTAINS(RANGE(pstd.EFF_BEGIN_TMSTP_UTC, pstd.EFF_END_TMSTP_UTC),TIMESTAMP( datetime(timestamp(isibdf.METRICS_DATE) + INTERVAL '1' DAY  - interval '0.001' second,'GMT')))
 
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_item_selling_rights_dim AS psrd 
  ON LOWER(psrd.rms_sku_num) = LOWER(isibdf.rms_sku_id) 
	   AND LOWER(psrd.channel_country) = LOWER(isibdf.channel_country)	   
	   AND LOWER(psrd.channel_brand) = LOWER(isibdf.channel_brand
      ) AND LOWER(psrd.selling_channel) = LOWER(isibdf.selling_channel)
	  AND RANGE_CONTAINS(RANGE(psrd.eff_begin_tmstp_utc, psrd.eff_end_tmstp_utc), timestamp(datetime(timestamp(isibdf.METRICS_DATE) + INTERVAL '1' DAY  - interval '0.001' second,'GMT') ))
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.vendor_dim AS vd
  ON LOWER(vd.vendor_num) = LOWER(psd.prmy_supp_num)
 WHERE LOWER(isibdf.selling_channel) = LOWER('ONLINE') 
 AND LOWER(isibdf.store_type_code) IN (LOWER('FC'), LOWER('LH'),
     LOWER('OC'), LOWER('OF'), LOWER('RK'))
  OR LOWER(isibdf.selling_channel) = LOWER('STORE') AND LOWER(isibdf.store_type_code) IN (LOWER('FL'), LOWER('NL'),
     LOWER('RK'), LOWER('SS'), LOWER('VS')));
	 
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
weighted_average_cost NUMERIC(38,9),
promotion_ind STRING,
regular_price_amt NUMERIC(15,2),
base_retail_drop_ship_amt NUMERIC(15,2),
regular_price_percent_off NUMERIC(12,2),
current_price_amt NUMERIC(15,2),
ownership_price_amt NUMERIC(15,2),
prmy_supp_num STRING,
supp_part_num STRING,
compare_at_retail_price_amt NUMERIC(15,2),
base_retail_percentage_off_compare_at_retail_pct NUMERIC(15,2),
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
(SELECT inv.metrics_date,
  inv.rms_sku_id,
  inv.rms_style_num,
  TRIM(inv.color_num) AS color_num,
  inv.channel_country,
  inv.channel_brand,
  inv.selling_channel,
  purch.is_online_purchasable,
  purch.eff_begin_tmstp_utc AS online_purchasable_eff_begin_tmstp,

  purch.eff_end_tmstp_utc AS online_purchasable_eff_end_tmstp,
  COALESCE(wacd.weighted_average_cost, wacc.weighted_average_cost, 0) AS weighted_average_cost,
   CASE
   WHEN LOWER(pptd.selling_retail_price_type_code) = LOWER('P')
   THEN 'Y'
   ELSE 'N'
   END AS promotion_ind,
  pptd.regular_price_amt,
  pptd.regular_price_amt AS base_retail_drop_ship_amt,
  ROUND(CAST(TRUNC((pptd.regular_price_amt - pptd.ownership_price_amt) / CAST(IF(pptd.regular_price_amt = 0, NULL, pptd.regular_price_amt) AS NUMERIC), 2) AS NUMERIC)
   , 2) AS regular_price_percent_off,
  pptd.current_price_amt,
  pptd.ownership_price_amt,
  inv.prmy_supp_num,
  inv.supp_part_num,
  pptd.compare_at_value AS compare_at_retail_price_amt,
  pptd.base_retail_percentage_off_compare_at_value AS base_retail_percentage_off_compare_at_retail_pct,
  inv.drop_ship_eligible_ind,
  inv.npg_ind,
  inv.fp_replenishment_eligible_ind,
  inv.op_replenishment_eligible_ind,
  inv.supp_color,
  inv.return_disposition_code,
  inv.selling_status_code,
  inv.vendor_name,
  inv.vendor_label_name,
  inv.fulfillment_type_code
 FROM inv_data AS inv
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS orgstore ON CAST(TRUNC(CAST(CASE
     WHEN TRIM(inv.location_id) = ''
     THEN '0'
     ELSE TRIM(inv.location_id)
     END AS FLOAT64)) AS INTEGER) = orgstore.store_num
  LEFT JOIN (SELECT DISTINCT pptd_price.rms_sku_num,
    pptd_price.channel_country,
    pptd_price.channel_brand,
    pptd_price.selling_channel,
     CASE
     WHEN LOWER(pptd_price.selling_retail_price_type_code) = LOWER('REGULAR')
     THEN 'R'
     WHEN LOWER(pptd_price.selling_retail_price_type_code) = LOWER('PROMOTION')
     THEN 'P'
     WHEN LOWER(pptd_price.selling_retail_price_type_code) = LOWER('CLEARANCE')
     THEN 'C'
     ELSE pptd_price.selling_retail_price_type_code
     END AS selling_retail_price_type_code,
    pptd_price.ownership_retail_price_amt AS ownership_price_amt,
    pptd_price.regular_price_amt,
    pptd_price.selling_retail_price_amt AS current_price_amt,
    pptd_price.compare_at_retail_price_amt AS compare_at_value,
    pptd_price.base_retail_percentage_off_compare_at_retail_pct AS base_retail_percentage_off_compare_at_value,
    pptd_price.eff_begin_tmstp_UTC,
    pptd_price.eff_end_tmstp_UTC
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_price_timeline_dim AS pptd_price
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl 
    ON LOWER(etl.interface_code) = LOWER('CMD_WKLY')
	-- DASCLM-2154: subtract a MS from this to get time, add a DAY to get end of day, as well as set to GMT
  
  WHERE pptd_price.eff_end_tmstp_utc >= (CAST(etl.EXTRACT_START_DT AS TIMESTAMP) + INTERVAL '1' DAY - INTERVAL '0.001' SECOND )
	) 
	AS pptd 
	ON
      LOWER(pptd.rms_sku_num) = LOWER(inv.rms_sku_id) AND LOWER(inv.channel_country) = LOWER(pptd.channel_country) AND
     LOWER(pptd.channel_brand) = LOWER(inv.channel_brand) AND LOWER(pptd.selling_channel) = LOWER(inv.selling_channel)
	AND range_contains(range(pptd.EFF_BEGIN_TMSTP_utc, pptd.EFF_END_TMSTP_utc) , CAST(inv.METRICS_DATE AS TIMESTAMP) + INTERVAL '1' DAY - INTERVAL '0.001' SECOND )
  LEFT JOIN (SELECT DISTINCT 
    opid.rms_sku_num,
    opid.channel_country,
    opid.channel_brand,
    opid.selling_channel,
    opid.is_online_purchasable,
    opid.eff_begin_tmstp_utc,
    opid.eff_end_tmstp_utc
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_online_purchasable_item_dim AS opid
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS eltn
	ON LOWER(eltn.interface_code) = LOWER('CMD_WKLY') 
   WHERE opid.EFF_END_TMSTP_utc >= CAST(eltn.EXTRACT_START_DT AS TIMESTAMP) + INTERVAL '1' DAY - INTERVAL '0.001' SECOND
	) AS purch
	ON
     LOWER(purch.rms_sku_num) = LOWER(inv.rms_sku_id) 
	 AND LOWER(purch.channel_country) = LOWER(inv.channel_country) 
	 AND LOWER(purch.channel_brand) = LOWER(inv.channel_brand)
	  AND RANGE_CONTAINS(RANGE(purch.EFF_BEGIN_TMSTP_UTC, purch.EFF_END_TMSTP_UTC) , CAST(inv.METRICS_DATE AS TIMESTAMP) + INTERVAL '1' DAY - INTERVAL '0.001' SECOND )
  LEFT JOIN (SELECT sku_num,
    location_num,
    weighted_average_cost,
    eff_begin_dt,
    eff_end_dt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.weighted_average_cost_date_dim AS wac1
   WHERE eff_end_dt >= (SELECT extract_start_dt
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS elt
      WHERE LOWER(interface_code) = LOWER('CMD_WKLY'))) AS wacd ON LOWER(inv.rms_sku_id) = LOWER(wacd.sku_num) AND LOWER(TRIM(FORMAT('%11d'
         , orgstore.store_num))) = LOWER(TRIM(wacd.location_num)) AND inv.metrics_date >= wacd.eff_begin_dt AND inv.metrics_date
     < wacd.eff_end_dt
  LEFT JOIN (SELECT sku_num,
    channel_num,
    weighted_average_cost,
    eff_begin_dt,
    eff_end_dt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.weighted_average_cost_channel_dim AS wac2
   WHERE eff_end_dt >= (SELECT extract_start_dt
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS elt
      WHERE LOWER(interface_code) = LOWER('CMD_WKLY'))) AS wacc ON LOWER(inv.rms_sku_id) = LOWER(wacc.sku_num) AND
      orgstore.channel_num = wacc.channel_num AND inv.metrics_date >= wacc.eff_begin_dt AND inv.metrics_date < wacc.eff_end_dt
    );
	
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact 
 SET anniversary_item_ind = 'N'
WHERE snapshot_date = (SELECT dw_batch_dt
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
        WHERE LOWER(interface_code) = LOWER('CMD_WKLY'));
		

END;

BEGIN

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt 
SET
    anniversary_item_ind = src.anniversary_item_ind,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP)
     ,dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()  --Added tz column
	FROM 
	(SELECT DISTINCT isibdf.rms_style_num, isibdf.color_num, isibdf.channel_country, isibdf.selling_channel, isibdf.channel_brand, 'Y' AS anniversary_item_ind
        FROM raw_data AS isibdf
            INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_price_timeline_dim AS pptd_price
			ON LOWER(isibdf.rms_sku_id) = LOWER(pptd_price.rms_sku_num) 
			AND LOWER(isibdf.channel_country) = LOWER(pptd_price.channel_country) 
			AND LOWER(isibdf.selling_channel) = LOWER(pptd_price.selling_channel) 
			AND LOWER(isibdf.channel_brand) = LOWER(pptd_price.channel_brand)
			 AND RANGE_CONTAINS(RANGE(pptd_price.EFF_BEGIN_TMSTP_UTC,
              CASE
		 
          WHEN pptd_price.EFF_END_TMSTP_UTC < CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP)
          THEN pptd_price.EFF_END_TMSTP_UTC + INTERVAL '45' DAY
          ELSE pptd_price.EFF_END_TMSTP_UTC
          END) , CAST(isibdf.METRICS_DATE AS TIMESTAMP) + INTERVAL '1' DAY - INTERVAL '0.001' SECOND )
            INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_promotion_timeline_dim AS pptd_promo 
			ON LOWER(isibdf.rms_sku_id) = LOWER(pptd_promo.rms_sku_num) 
			AND LOWER(isibdf.channel_country) = LOWER(pptd_promo.channel_country) 
			AND LOWER(isibdf.selling_channel) = LOWER(pptd_promo.selling_channel) 
			AND LOWER(isibdf.channel_brand) = LOWER(pptd_promo.channel_brand) 
			AND LOWER(pptd_price.selling_retail_record_id) = LOWER(pptd_promo.promo_id) 
			AND LOWER(pptd_promo.promotion_type_code) = LOWER('SIMPLE')
			  AND RANGE_CONTAINS(RANGE(pptd_promo.EFF_BEGIN_TMSTP_UTC, CASE
                                                  WHEN pptd_promo.EFF_END_TMSTP_UTC < CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP)
                                                  THEN pptd_promo.EFF_END_TMSTP_UTC + INTERVAL '45' DAY
                                                  ELSE pptd_promo.EFF_END_TMSTP_UTC 
                                                END) , CAST(isibdf.METRICS_DATE AS TIMESTAMP) + INTERVAL '1' DAY - INTERVAL '0.001' SECOND )
        WHERE LOWER(pptd_promo.enticement_tags) LIKE LOWER('%ANNIVERSARY_SALE%')) AS src
		
WHERE tgt.snapshot_date = (SELECT dw_batch_dt
                            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                            WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) 
							AND LOWER(tgt.rms_style_num) = LOWER(src.rms_style_num) 
							AND LOWER(tgt.color_num) = LOWER(src.color_num) 
							AND LOWER(tgt.channel_country) = LOWER(src.channel_country) 
							AND LOWER(tgt.selling_channel) = LOWER(src.selling_channel) 
							AND LOWER(tgt.channel_brand) = LOWER(src.channel_brand);


END;

BEGIN


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt 
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
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP)
    ,dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() ---added TZ column
     FROM (SELECT rms_style_num, color_num, channel_country, channel_brand, selling_channel, AVG(weighted_average_cost) AS weighted_average_cost, MIN(regular_price_amt) AS regular_price_amt, AVG(CASE WHEN LOWER(drop_ship_eligible_ind) = LOWER('Y') THEN regular_price_amt ELSE NULL END) AS base_retail_drop_ship_amt, AVG(regular_price_percent_off) AS regular_price_percent_off,
	   -- TRUNC & CAST are used here to maintain 2 decimal places vs rounding (.99 to 1)
	MAX(promotion_ind) AS promotion_ind, 
	MAX(drop_ship_eligible_ind) AS drop_ship_eligible_ind, MAX(npg_ind) AS npg_ind, 
	MAX(fp_replenishment_eligible_ind) AS fp_replenishment_eligible_ind,MAX(op_replenishment_eligible_ind) AS op_replenishment_eligible_ind,MIN(current_price_amt) AS current_price_amt, 
	MIN(ownership_price_amt) AS ownership_price_amt
        FROM raw_data
        GROUP BY rms_style_num, color_num, channel_country, channel_brand, selling_channel) AS src
WHERE tgt.snapshot_date = (SELECT dw_batch_dt
                            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                            WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND LOWER(COALESCE(src.rms_style_num, 'NA')) = LOWER(COALESCE(tgt.rms_style_num, 'NA')) AND LOWER(COALESCE(src.color_num, 'NA')) = LOWER(COALESCE(tgt.color_num, 'NA')) AND LOWER(src.channel_country) = LOWER(tgt.channel_country) AND LOWER(src.channel_brand) = LOWER(tgt.channel_brand) AND LOWER(src.selling_channel) = LOWER(tgt.selling_channel);

END;

BEGIN


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt 
SET
    price_variance_ind = src.price_variance_ind,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP)
    ,dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()   --added TZ column
     FROM (SELECT rms_style_num, color_num, channel_country, channel_brand, selling_channel, CASE WHEN MIN(ownership_price_amt) <> MAX(ownership_price_amt) THEN 'Y' ELSE 'N' END AS price_variance_ind
        FROM raw_data
        GROUP BY rms_style_num, color_num, channel_country, channel_brand, selling_channel) AS src
WHERE tgt.snapshot_date = (SELECT dw_batch_dt
                            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                            WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND LOWER(COALESCE(src.rms_style_num, 'NA')) = LOWER(COALESCE(tgt.rms_style_num, 'NA')) AND LOWER(COALESCE(src.color_num, 'NA')) = LOWER(COALESCE(tgt.color_num, 'NA')) AND LOWER(src.channel_country) = LOWER(tgt.channel_country) AND LOWER(src.channel_brand) = LOWER(tgt.channel_brand) AND LOWER(src.selling_channel) = LOWER(tgt.selling_channel);

END;

BEGIN


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact

 AS tgt 
 SET
    msrp_amt = t2.msrp_amt,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) 
    ,dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()   --added TZ column
	FROM
	(SELECT rms_style_num, color_num, channel_country, channel_brand, selling_channel,
	CASE WHEN compare_at_retail_price_amt IS NULL 
	THEN regular_price_amt 
	ELSE compare_at_retail_price_amt 
	END AS msrp_amt, 
	COUNT(CASE WHEN compare_at_retail_price_amt IS NULL 
	THEN regular_price_amt 
	ELSE compare_at_retail_price_amt END) AS msrp_amt_count
        FROM raw_data
        GROUP BY rms_style_num, color_num, channel_country, channel_brand, selling_channel, msrp_amt
        QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_style_num, color_num, channel_country, channel_brand, selling_channel 
		ORDER BY msrp_amt_count DESC, msrp_amt)) = 1) AS t2
		
WHERE tgt.snapshot_date = (SELECT dw_batch_dt
                            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                            WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND LOWER(COALESCE(t2.rms_style_num, 'NA')) = LOWER(COALESCE(tgt.rms_style_num, 'NA')) AND LOWER(COALESCE(t2.color_num, 'NA')) = LOWER(COALESCE(tgt.color_num, 'NA')) AND LOWER(t2.channel_country) = LOWER(tgt.channel_country) AND LOWER(t2.channel_brand) = LOWER(tgt.channel_brand) AND LOWER(t2.selling_channel) = LOWER(tgt.selling_channel);


END;

BEGIN


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt  SET
    compare_at_percent_off = t2.compare_at_percent_off,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP)
    ,dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()   --added TZ column
	FROM 
	(SELECT rms_style_num, color_num, channel_country, channel_brand, selling_channel, 
	CASE 
	WHEN compare_at_retail_price_amt IS NULL 
	THEN regular_price_amt 
	ELSE compare_at_retail_price_amt
	END AS msrp_amt, 
	COALESCE(base_retail_percentage_off_compare_at_retail_pct, 0) AS compare_at_percent_off, 
	COUNT(CASE WHEN compare_at_retail_price_amt IS NULL THEN regular_price_amt ELSE compare_at_retail_price_amt END) AS msrp_amt_count, COUNT(base_retail_percentage_off_compare_at_retail_pct) AS compare_at_percent_off_count
        FROM raw_data
        GROUP BY rms_style_num, color_num, channel_country, channel_brand, selling_channel, msrp_amt, compare_at_percent_off
        QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_style_num, color_num, channel_country, channel_brand, selling_channel 
		ORDER BY msrp_amt_count 
		DESC, msrp_amt, compare_at_percent_off_count DESC, compare_at_percent_off)) = 1) AS t2
WHERE tgt.snapshot_date = (SELECT dw_batch_dt
                                FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                                WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND LOWER(COALESCE(t2.rms_style_num, 'NA')) = LOWER(COALESCE(tgt.rms_style_num, 'NA')) AND LOWER(COALESCE(t2.color_num, 'NA')) = LOWER(COALESCE(tgt.color_num, 'NA')) AND LOWER(t2.channel_country) = LOWER(tgt.channel_country) AND LOWER(t2.channel_brand) = LOWER(tgt.channel_brand) AND LOWER(t2.selling_channel) = LOWER(tgt.selling_channel) AND t2.msrp_amt = tgt.msrp_amt;

END;

BEGIN


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt  
SET
    return_disposition_code = t2.return_disposition_code

    FROM (SELECT rms_style_num, color_num, channel_country, channel_brand, selling_channel, return_disposition_code, CASE WHEN LOWER(return_disposition_code) = LOWER('RTI') THEN 1 WHEN LOWER(return_disposition_code) = LOWER('RTS') THEN 2 WHEN LOWER(return_disposition_code) = LOWER('QA') THEN 3 WHEN LOWER(return_disposition_code) = LOWER('RCK') THEN 4 WHEN LOWER(return_disposition_code) = LOWER('RK7') THEN 5 ELSE 6 END AS return_disposition_code_priority, COUNT(return_disposition_code) AS return_disposition_code_cnt
        FROM raw_data
        WHERE LOWER(selling_channel) IN (LOWER('STORE'), LOWER('ONLINE'))
        GROUP BY rms_style_num, color_num, channel_country, channel_brand, selling_channel, return_disposition_code
        QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_style_num, color_num, channel_country, channel_brand, selling_channel ORDER BY CASE WHEN LOWER(return_disposition_code) = LOWER('RTI') THEN 1 WHEN LOWER(return_disposition_code) = LOWER('RTS') THEN 2 WHEN LOWER(return_disposition_code) = LOWER('QA') THEN 3 WHEN LOWER(return_disposition_code) = LOWER('RCK') THEN 4 WHEN LOWER(return_disposition_code) = LOWER('RK7') THEN 5 ELSE 6 END, return_disposition_code_cnt DESC)) = 1) AS t2
WHERE tgt.snapshot_date = (SELECT dw_batch_dt
                                FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                                WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND LOWER(COALESCE(t2.rms_style_num, 'NA')) = LOWER(COALESCE(tgt.rms_style_num, 'NA')) AND LOWER(COALESCE(t2.color_num, 'NA')) = LOWER(COALESCE(tgt.color_num, 'NA')) AND LOWER(t2.channel_country) = LOWER(tgt.channel_country) AND LOWER(t2.channel_brand) = LOWER(tgt.channel_brand) AND LOWER(t2.selling_channel) = LOWER(tgt.selling_channel) AND LOWER(tgt.selling_channel) IN (LOWER('STORE'), LOWER('ONLINE'));

END;

BEGIN

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt 
SET
    selling_status_code = src.selling_status_code,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP)
    ,dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()   --added TZ column
       FROM (SELECT rms_style_num, color_num, channel_country, channel_brand, selling_channel, 
	CASE WHEN MAX(CASE WHEN LOWER(selling_status_code) = LOWER('UNBLOCKED') THEN 1 ELSE 0 END) > 0 
	THEN 'UNBLOCKED' WHEN MIN(CASE WHEN LOWER(selling_status_code) = LOWER('BLOCKED') THEN 1 ELSE 0 END) = 1 THEN 'BLOCKED' ELSE 'UNBLOCKED' END AS selling_status_code
        FROM raw_data
        GROUP BY rms_style_num, color_num, channel_country, channel_brand, selling_channel) AS src
WHERE tgt.snapshot_date = (SELECT dw_batch_dt
                            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                            WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND LOWER(COALESCE(src.rms_style_num, 'NA')) = LOWER(COALESCE(tgt.rms_style_num, 'NA')) AND LOWER(COALESCE(src.color_num, 'NA')) = LOWER(COALESCE(tgt.color_num, 'NA')) AND LOWER(src.channel_country) = LOWER(tgt.channel_country) AND LOWER(src.channel_brand) = LOWER(tgt.channel_brand) AND LOWER(src.selling_channel) = LOWER(tgt.selling_channel);

END;

BEGIN


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt
 SET
    prmy_supp_num = src.prmy_supp_num,
    supp_color = src.supp_color,
    supp_part_num = src.supp_part_num,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP)
    ,dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()  --added TZ column
	FROM (SELECT rms_style_num, color_num, channel_country, channel_brand, selling_channel, MIN(prmy_supp_num) AS prmy_supp_num, MIN(supp_part_num) AS supp_part_num, MIN(supp_color) AS supp_color
        FROM raw_data
        GROUP BY rms_style_num, color_num, channel_country, channel_brand, selling_channel) AS src
WHERE tgt.snapshot_date = (SELECT dw_batch_dt
                            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                            WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND LOWER(COALESCE(src.rms_style_num, 'NA')) = LOWER(COALESCE(tgt.rms_style_num, 'NA')) AND LOWER(COALESCE(src.color_num, 'NA')) = LOWER(COALESCE(tgt.color_num, 'NA')) AND LOWER(src.channel_country) = LOWER(tgt.channel_country) AND LOWER(src.channel_brand) = LOWER(tgt.channel_brand) AND LOWER(src.selling_channel) = LOWER(tgt.selling_channel);

END;

BEGIN

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt SET
    vendor_name = t1.vendor_name,
    vendor_label_name = t1.vendor_label_name,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP)
     ,dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()  --added TZ column
     FROM (SELECT rms_style_num, color_num, channel_country, channel_brand, selling_channel, vendor_name, vendor_label_name, COUNT(vendor_name) AS vendor_name_cnt
        FROM raw_data
        GROUP BY rms_style_num, color_num, channel_country, channel_brand, selling_channel, vendor_name, vendor_label_name
        QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_style_num, color_num, channel_country, channel_brand, selling_channel ORDER BY vendor_name_cnt DESC, vendor_name, vendor_label_name)) = 1) AS t1
WHERE tgt.snapshot_date = (SELECT dw_batch_dt
                            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                            WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND LOWER(COALESCE(t1.rms_style_num, 'NA')) = LOWER(COALESCE(tgt.rms_style_num, 'NA')) AND LOWER(COALESCE(t1.color_num, 'NA')) = LOWER(COALESCE(tgt.color_num, 'NA')) AND LOWER(t1.channel_country) = LOWER(tgt.channel_country) AND LOWER(t1.channel_brand) = LOWER(tgt.channel_brand) AND LOWER(t1.selling_channel) = LOWER(tgt.selling_channel);


END;

BEGIN


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt  
SET
    first_rack_date = src.first_rack_date,
    rack_ind = 'Y',
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) 
     ,dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()  --added TZ column
	FROM 
	(SELECT trn.rms_style_num, trn.color_num, trn.channel_country, trn.selling_channel, MIN(trn.event_date) AS first_rack_date
        FROM (SELECT DISTINCT psd.rms_style_num, psd.color_num, psdv.channel_country, psdv.channel_brand, psdv.selling_channel, tclf.event_date
                FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.transfer_created_logical_fact AS tclf
                    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl ON LOWER(etl.interface_code) = LOWER('CMD_WKLY')
					AND tclf.EVENT_TIMESTAMP_UTC <= CAST(etl.DW_BATCH_DT AS TIMESTAMP) + INTERVAL '1' DAY - INTERVAL '0.001' SECOND

                    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.price_store_dim_vw AS psdv ON CAST(SUBSTR(SUBSTR(CAST(tclf.to_location_id AS STRING), 1, 10), 3 * -1) AS FLOAT64) = psdv.store_num
                    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS psd 
					ON LOWER(tclf.rms_sku_num) = LOWER(psd.rms_sku_num) AND LOWER(psdv.channel_country) = LOWER(psd.channel_country)
					AND RANGE_CONTAINS(RANGE(psd.EFF_BEGIN_TMSTP_UTC, psd.EFF_END_TMSTP_UTC) , CAST(etl.DW_BATCH_DT AS TIMESTAMP) + INTERVAL '1' DAY - INTERVAL '0.001' SECOND )
                    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_hist_fact AS cmibwf ON LOWER(COALESCE(psd.rms_style_num, 'NA')) = LOWER(COALESCE(cmibwf.rms_style_num, 'NA')) AND LOWER(COALESCE(psd.color_num, 'NA')) = LOWER(COALESCE(cmibwf.color_num, 'NA')) AND LOWER(psdv.channel_country) = LOWER(cmibwf.channel_country) AND LOWER(psdv.channel_brand) = LOWER(cmibwf.channel_brand) AND LOWER(psdv.selling_channel) = LOWER(cmibwf.selling_channel) AND etl.dw_batch_dt = cmibwf.snapshot_date
                WHERE LOWER(tclf.transfer_context_value) = LOWER('RKING')) AS trn
            LEFT JOIN (SELECT psd0.rms_style_num, psd0.color_num, cmrf.channel_country, cmrf.channel_brand, MAX(cmrf.effective_tmstp_utc) AS last_reset_date
                FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_reset_fact AS cmrf
                    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl0 ON LOWER(etl0.interface_code) = LOWER('CMD_WKLY')
                    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS psd0 
					ON LOWER(cmrf.rms_sku_num) = LOWER(psd0.rms_sku_num) AND LOWER(cmrf.channel_country) = LOWER(psd0.channel_country)
				 AND RANGE_CONTAINS(RANGE(psd0.EFF_BEGIN_TMSTP_UTC, psd0.EFF_END_TMSTP_UTC) , CAST(etl0.DW_BATCH_DT AS TIMESTAMP) + INTERVAL '1' DAY - INTERVAL '0.001' SECOND )
                    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_hist_fact AS cmibwf0 
					ON LOWER(COALESCE(psd0.rms_style_num, 'NA')) = LOWER(COALESCE(cmibwf0.rms_style_num, 'NA')) AND LOWER(COALESCE(psd0.color_num, 'NA')) = LOWER(COALESCE(cmibwf0.color_num, 'NA')) AND LOWER(cmrf.channel_country) = LOWER(cmibwf0.channel_country) AND LOWER(cmrf.channel_brand) = LOWER(cmibwf0.channel_brand) AND LOWER(cmrf.selling_channel) = LOWER(cmibwf0.selling_channel) AND etl0.dw_batch_dt = cmibwf0.snapshot_date
                GROUP BY cmrf.channel_country, cmrf.channel_brand, psd0.rms_style_num, psd0.color_num) AS rst ON LOWER(COALESCE(trn.rms_style_num, 'NA')) = LOWER(COALESCE(rst.rms_style_num, 'NA')) AND LOWER(COALESCE(trn.color_num, 'NA')) = LOWER(COALESCE(rst.color_num, 'NA')) AND LOWER(trn.channel_country) = LOWER(rst.channel_country) AND LOWER(trn.channel_brand) = LOWER(rst.channel_brand)
        WHERE rst.last_reset_date IS NULL OR CAST(rst.last_reset_date AS DATE) <= trn.event_date
        GROUP BY trn.rms_style_num, trn.color_num, trn.channel_country, trn.selling_channel) AS src
WHERE LOWER(COALESCE(tgt.rms_style_num, 'NA')) = LOWER(COALESCE(src.rms_style_num, 'NA')) AND LOWER(COALESCE(tgt.color_num, 'NA')) = LOWER(COALESCE(src.color_num, 'NA')) AND LOWER(tgt.channel_country) = LOWER(src.channel_country) AND LOWER(tgt.selling_channel) = LOWER(src.selling_channel) AND tgt.snapshot_date = (SELECT dw_batch_dt
            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
            WHERE LOWER(interface_code) = LOWER('CMD_WKLY'));

END;


BEGIN


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt 
SET
    weighted_average_cost = omni.weighted_average_cost,
    regular_price_amt = omni.regular_price_amt,
    base_retail_drop_ship_amt = CAST(omni.base_retail_drop_ship_amt AS NUMERIC),
    regular_price_percent_off = IFNULL(omni.regular_price_percent_off, 0),
	-- ANNIVERSARY_ITEM_IND   = omni.ANNIVERSARY_ITEM_IND,
    promotion_ind = omni.promotion_ind,
    rack_ind = 'N',
    drop_ship_eligible_ind = omni.drop_ship_eligible_ind,
    npg_ind = omni.npg_ind,
    fp_replenishment_eligible_ind = omni.fp_replenishment_eligible_ind,
    op_replenishment_eligible_ind = omni.op_replenishment_eligible_ind,
    current_price_amt = omni.current_price_amt,
    ownership_price_amt = omni.ownership_price_amt,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) 
    ,dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()  --added TZ column
	FROM 
	(SELECT rms_style_num, color_num, channel_country, channel_brand, 
	AVG(weighted_average_cost) AS weighted_average_cost, 
	MIN(regular_price_amt) AS regular_price_amt, 
	AVG(CASE WHEN LOWER(drop_ship_eligible_ind) = LOWER('Y')
	THEN regular_price_amt ELSE NULL END) AS base_retail_drop_ship_amt, AVG(regular_price_percent_off) AS regular_price_percent_off,
	--MAX(ANNIVERSARY_ITEM_IND) AS ANNIVERSARY_ITEM_IND
	MAX(promotion_ind) AS promotion_ind, 
	MAX(drop_ship_eligible_ind) AS drop_ship_eligible_ind, MAX(npg_ind) AS npg_ind, 
	MAX(fp_replenishment_eligible_ind) AS fp_replenishment_eligible_ind, MAX(op_replenishment_eligible_ind) AS op_replenishment_eligible_ind, MIN(current_price_amt) AS current_price_amt, 
	MIN(ownership_price_amt) AS ownership_price_amt
        FROM raw_data
        GROUP BY rms_style_num, color_num, channel_country, channel_brand) AS omni
WHERE tgt.snapshot_date = (SELECT dw_batch_dt
                            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                            WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND LOWER(COALESCE(omni.rms_style_num, 'NA')) = LOWER(COALESCE(tgt.rms_style_num, 'NA')) AND LOWER(COALESCE(omni.color_num, 'NA')) = LOWER(COALESCE(tgt.color_num, 'NA')) AND LOWER(omni.channel_country) = LOWER(tgt.channel_country) AND LOWER(omni.channel_brand) = LOWER(tgt.channel_brand) AND (LOWER(tgt.selling_channel) = LOWER('OMNI') OR tgt.total_inv_qty = 0);

END;

BEGIN

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact
 SET anniversary_item_ind = 'N'
WHERE snapshot_date = (SELECT dw_batch_dt
            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
            WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND (LOWER(selling_channel) = LOWER('OMNI') OR total_inv_qty = 0);
			
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt 
SET
    anniversary_item_ind = src.anniversary_item_ind,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) 
     ,dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()  --added TZ column
    FROM (SELECT DISTINCT isibdf.rms_style_num, isibdf.color_num, isibdf.channel_country, isibdf.selling_channel, isibdf.channel_brand, 'Y' AS anniversary_item_ind
        FROM raw_data AS isibdf
            INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_price_timeline_dim AS pptd_price ON LOWER(isibdf.rms_sku_id) = LOWER(pptd_price.rms_sku_num) 
			AND LOWER(isibdf.channel_country) = LOWER(pptd_price.channel_country) 
			AND LOWER(isibdf.selling_channel) = LOWER(pptd_price.selling_channel) 
			AND LOWER(isibdf.channel_brand) = LOWER(pptd_price.channel_brand)
			  AND RANGE_CONTAINS(RANGE(pptd_price.EFF_BEGIN_TMSTP_UTC, CASE
                                                   WHEN pptd_price.EFF_END_TMSTP_UTC < CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) 
                                                   THEN pptd_price.EFF_END_TMSTP_UTC + INTERVAL '45' DAY
                                                   ELSE pptd_price.EFF_END_TMSTP_UTC 
                                                 END) , CAST(isibdf.METRICS_DATE AS TIMESTAMP) + INTERVAL '1' DAY - INTERVAL '0.001' SECOND )
            INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_promotion_timeline_dim AS pptd_promo ON LOWER(isibdf.rms_sku_id) = LOWER(pptd_promo.rms_sku_num) 
			AND LOWER(isibdf.channel_country) = LOWER(pptd_promo.channel_country) 
			AND LOWER(isibdf.selling_channel) = LOWER(pptd_promo.selling_channel) 
			AND LOWER(isibdf.channel_brand) = LOWER(pptd_promo.channel_brand) 
			AND LOWER(pptd_price.selling_retail_record_id) = LOWER(pptd_promo.promo_id) 
			AND LOWER(pptd_promo.promotion_type_code) = LOWER('SIMPLE')
            AND RANGE_CONTAINS(RANGE(pptd_promo.EFF_BEGIN_TMSTP_UTC, CASE
                                                 WHEN pptd_promo.EFF_END_TMSTP_UTC < CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP)
                                                 THEN pptd_promo.EFF_END_TMSTP_UTC + INTERVAL '45' DAY
                                                 ELSE pptd_promo.EFF_END_TMSTP_UTC
                                               END) , CAST(isibdf.METRICS_DATE AS TIMESTAMP) + INTERVAL '1' DAY - INTERVAL '0.001' SECOND)
        WHERE LOWER(pptd_promo.enticement_tags) LIKE LOWER('%ANNIVERSARY_SALE%')) AS src
WHERE tgt.snapshot_date = (SELECT dw_batch_dt
                                FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                                WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND LOWER(tgt.rms_style_num) = LOWER(src.rms_style_num) AND LOWER(tgt.color_num) = LOWER(src.color_num) AND LOWER(tgt.channel_country) = LOWER(src.channel_country) AND LOWER(tgt.selling_channel) = LOWER(src.selling_channel) AND LOWER(tgt.channel_brand) = LOWER(src.channel_brand) AND (LOWER(tgt.selling_channel) = LOWER('OMNI') OR tgt.total_inv_qty = 0);			

END;



BEGIN

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt 
SET
    price_variance_ind = omni.price_variance_ind,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) 
     ,dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()  --added TZ column
    FROM (SELECT rms_style_num, color_num, channel_country, channel_brand, 
	CASE WHEN 
	MIN(ownership_price_amt) <> MAX(ownership_price_amt) THEN 'Y' ELSE 'N'END AS price_variance_ind
    FROM raw_data
   GROUP BY rms_style_num, color_num, channel_country, channel_brand) AS omni
WHERE tgt.snapshot_date = (SELECT dw_batch_dt
                            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                            WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND LOWER(COALESCE(omni.rms_style_num, 'NA')) = LOWER(COALESCE(tgt.rms_style_num, 'NA')) AND LOWER(COALESCE(omni.color_num, 'NA')) = LOWER(COALESCE(tgt.color_num, 'NA')) AND LOWER(omni.channel_country) = LOWER(tgt.channel_country) AND LOWER(omni.channel_brand) = LOWER(tgt.channel_brand) AND (LOWER(tgt.selling_channel) = LOWER('OMNI') OR tgt.total_inv_qty = 0);

END;

BEGIN

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt  SET
    msrp_amt = t2.msrp_amt,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) 
     ,dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()  --added TZ column

    FROM (SELECT rms_style_num, color_num, channel_country, channel_brand, CASE WHEN compare_at_retail_price_amt IS NULL THEN regular_price_amt ELSE compare_at_retail_price_amt END AS msrp_amt, COUNT(CASE WHEN compare_at_retail_price_amt IS NULL THEN regular_price_amt ELSE compare_at_retail_price_amt END) AS msrp_amt_count
        FROM raw_data
        GROUP BY rms_style_num, color_num, channel_country, channel_brand, msrp_amt
        QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_style_num, color_num, channel_country, channel_brand ORDER BY msrp_amt_count DESC, msrp_amt)) = 1) AS t2
WHERE tgt.snapshot_date = (SELECT dw_batch_dt
                            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                            WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND LOWER(COALESCE(t2.rms_style_num, 'NA')) = LOWER(COALESCE(tgt.rms_style_num, 'NA')) AND LOWER(COALESCE(t2.color_num, 'NA')) = LOWER(COALESCE(tgt.color_num, 'NA')) AND LOWER(t2.channel_country) = LOWER(tgt.channel_country) AND LOWER(t2.channel_brand) = LOWER(tgt.channel_brand) AND (LOWER(tgt.selling_channel) = LOWER('OMNI') OR tgt.total_inv_qty = 0);

END;

BEGIN

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt 
 SET
    compare_at_percent_off = t2.compare_at_percent_off,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) 
     ,dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()  --added TZ column
    FROM (SELECT rms_style_num, color_num, channel_country, channel_brand, 
    CASE WHEN compare_at_retail_price_amt IS NULL THEN regular_price_amt ELSE compare_at_retail_price_amt END AS msrp_amt, 
    COALESCE(base_retail_percentage_off_compare_at_retail_pct, 0) AS compare_at_percent_off, COUNT(CASE WHEN compare_at_retail_price_amt IS NULL THEN regular_price_amt ELSE compare_at_retail_price_amt END) AS msrp_amt_count, COUNT(COALESCE(base_retail_percentage_off_compare_at_retail_pct, 0)) AS compare_at_percent_off_count
        FROM raw_data
        GROUP BY rms_style_num, color_num, channel_country, channel_brand, msrp_amt, compare_at_percent_off
        QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_style_num, color_num, channel_country, channel_brand ORDER BY msrp_amt_count DESC, msrp_amt, compare_at_percent_off_count DESC, compare_at_percent_off)) = 1) AS t2
WHERE tgt.snapshot_date = (SELECT dw_batch_dt
                                FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                                WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND LOWER(COALESCE(t2.rms_style_num, 'NA')) = LOWER(COALESCE(tgt.rms_style_num, 'NA')) AND LOWER(COALESCE(t2.color_num, 'NA')) = LOWER(COALESCE(tgt.color_num, 'NA')) AND LOWER(t2.channel_country) = LOWER(tgt.channel_country) AND LOWER(t2.channel_brand) = LOWER(tgt.channel_brand) AND t2.msrp_amt = tgt.msrp_amt AND (LOWER(tgt.selling_channel) = LOWER('OMNI') OR tgt.total_inv_qty = 0);

END;

BEGIN

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt  SET
    return_disposition_code = t2.return_disposition_code 
	FROM (SELECT rms_style_num, color_num, channel_country, channel_brand, selling_channel, return_disposition_code, CASE WHEN LOWER(return_disposition_code) = LOWER('RTI') THEN 1 WHEN LOWER(return_disposition_code) = LOWER('RTS') THEN 2 WHEN LOWER(return_disposition_code) = LOWER('QA') THEN 3 WHEN LOWER(return_disposition_code) = LOWER('RCK') THEN 4 WHEN LOWER(return_disposition_code) = LOWER('RK7') THEN 5 ELSE 6 END AS return_disposition_code_priority, COUNT(return_disposition_code) AS return_disposition_code_cnt
        FROM raw_data
        WHERE LOWER(selling_channel) = LOWER('OMNI')
        GROUP BY rms_style_num, color_num, channel_country, channel_brand, selling_channel, return_disposition_code
        QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_style_num, color_num, channel_country, channel_brand, selling_channel ORDER BY CASE WHEN LOWER(return_disposition_code) = LOWER('RTI') THEN 1 WHEN LOWER(return_disposition_code) = LOWER('RTS') THEN 2 WHEN LOWER(return_disposition_code) = LOWER('QA') THEN 3 WHEN LOWER(return_disposition_code) = LOWER('RCK') THEN 4 WHEN LOWER(return_disposition_code) = LOWER('RK7') THEN 5 ELSE 6 END, return_disposition_code_cnt DESC)) = 1) AS t2
WHERE tgt.snapshot_date = (SELECT dw_batch_dt
                                FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                                WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND LOWER(COALESCE(t2.rms_style_num, 'NA')) = LOWER(COALESCE(tgt.rms_style_num, 'NA')) AND LOWER(COALESCE(t2.color_num, 'NA')) = LOWER(COALESCE(tgt.color_num, 'NA')) AND LOWER(t2.channel_country) = LOWER(tgt.channel_country) AND LOWER(t2.channel_brand) = LOWER(tgt.channel_brand) AND LOWER(t2.selling_channel) = LOWER(tgt.selling_channel) AND LOWER(tgt.selling_channel) = LOWER('OMNI');

END;

BEGIN

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt SET
    selling_status_code = omni.selling_status_code,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP)
     ,dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()  --added TZ column
     FROM (SELECT rms_style_num, color_num, channel_country, channel_brand, CASE WHEN MAX(CASE WHEN LOWER(selling_status_code) = LOWER('UNBLOCKED') THEN 1 ELSE 0 END) > 0 THEN 'UNBLOCKED' WHEN MIN(CASE WHEN LOWER(selling_status_code) = LOWER('BLOCKED') THEN 1 ELSE 0 END) = 1 THEN 'BLOCKED' ELSE 'UNBLOCKED' END AS selling_status_code
        FROM raw_data
        GROUP BY rms_style_num, color_num, channel_country, channel_brand) AS omni
WHERE tgt.snapshot_date = (SELECT dw_batch_dt
                            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                            WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND LOWER(COALESCE(omni.rms_style_num, 'NA')) = LOWER(COALESCE(tgt.rms_style_num, 'NA')) AND LOWER(COALESCE(omni.color_num, 'NA')) = LOWER(COALESCE(tgt.color_num, 'NA')) AND LOWER(omni.channel_country) = LOWER(tgt.channel_country) AND LOWER(omni.channel_brand) = LOWER(tgt.channel_brand) AND (LOWER(tgt.selling_channel) = LOWER('OMNI') OR tgt.total_inv_qty = 0);

END;

BEGIN

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt SET
    prmy_supp_num = omni.prmy_supp_num,
    supp_color = omni.supp_color,
    supp_part_num = omni.supp_part_num,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) 
     ,dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()  --added TZ column
    FROM (SELECT rms_style_num, color_num, channel_country, channel_brand, MIN(prmy_supp_num) AS prmy_supp_num, MIN(supp_part_num) AS supp_part_num, MIN(supp_color) AS supp_color
        FROM raw_data
        GROUP BY rms_style_num, color_num, channel_country, channel_brand) AS omni
WHERE tgt.snapshot_date = (SELECT dw_batch_dt
                            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                            WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND LOWER(COALESCE(omni.rms_style_num, 'NA')) = LOWER(COALESCE(tgt.rms_style_num, 'NA')) AND LOWER(COALESCE(omni.color_num, 'NA')) = LOWER(COALESCE(tgt.color_num, 'NA')) AND LOWER(omni.channel_country) = LOWER(tgt.channel_country) AND LOWER(omni.channel_brand) = LOWER(tgt.channel_brand) AND (LOWER(tgt.selling_channel) = LOWER('OMNI') OR tgt.total_inv_qty = 0);

END;

BEGIN

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt SET
    vendor_name = t1.vendor_name,
    vendor_label_name = t1.vendor_label_name,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) 
    ,dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()  --added TZ column
    FROM (SELECT rms_style_num, color_num, channel_country, channel_brand, vendor_name, vendor_label_name, COUNT(vendor_name) AS vendor_name_cnt
        FROM raw_data
        GROUP BY rms_style_num, color_num, channel_country, channel_brand, vendor_name, vendor_label_name
        QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_style_num, color_num, channel_country, channel_brand ORDER BY vendor_name_cnt DESC, vendor_name, vendor_label_name)) = 1) AS t1
WHERE tgt.snapshot_date = (SELECT dw_batch_dt
                            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                            WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND LOWER(COALESCE(t1.rms_style_num, 'NA')) = LOWER(COALESCE(tgt.rms_style_num, 'NA')) AND LOWER(COALESCE(t1.color_num, 'NA')) = LOWER(COALESCE(tgt.color_num, 'NA')) AND LOWER(t1.channel_country) = LOWER(tgt.channel_country) AND LOWER(t1.channel_brand) = LOWER(tgt.channel_brand) AND (LOWER(tgt.selling_channel) = LOWER('OMNI') OR tgt.total_inv_qty = 0);

END;

BEGIN

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt  
SET
    is_online_purchasable = iop.is_online_purchasable
     FROM (SELECT rd.rms_style_num, rd.color_num, rd.channel_country, rd.channel_brand, rd.selling_channel, CASE WHEN MAX(CASE WHEN LOWER(popid.is_online_purchasable) = LOWER('Y') THEN 1 ELSE 0 END) = 1 THEN 'Y' WHEN MAX(CASE WHEN LOWER(popid.is_online_purchasable) = LOWER('N') THEN 1 ELSE 0 END) = 1 
	THEN 'N' ELSE NULL END AS is_online_purchasable
        FROM raw_data AS rd
            INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl ON LOWER(etl.interface_code) = LOWER('CMD_WKLY') AND rd.metrics_date = etl.dw_batch_dt
            LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_online_purchasable_item_dim AS popid ON LOWER(popid.rms_sku_num) = LOWER(rd.rms_sku_id) AND LOWER(popid.channel_country) = LOWER(rd.channel_country) AND LOWER(popid.channel_brand) = LOWER(rd.channel_brand) AND LOWER(popid.selling_channel) = LOWER(rd.selling_channel) AND LOWER(rd.selling_channel) IN (LOWER('ONLINE'), LOWER('STORE'))
		  AND RANGE_CONTAINS(RANGE(popid.EFF_BEGIN_TMSTP_UTC, popid.EFF_END_TMSTP_UTC) , CAST(rd.METRICS_DATE AS TIMESTAMP) + INTERVAL '1' DAY - INTERVAL '0.001' SECOND )
			
        GROUP BY rd.rms_style_num, rd.color_num, rd.channel_country, rd.channel_brand, rd.selling_channel) AS iop
WHERE tgt.snapshot_date = (SELECT dw_batch_dt
                            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                            WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND LOWER(COALESCE(iop.rms_style_num, 'NA')) = LOWER(COALESCE(tgt.rms_style_num, 'NA')) AND LOWER(COALESCE(iop.color_num, 'NA')) = LOWER(COALESCE(tgt.color_num, 'NA')) AND LOWER(iop.channel_country) = LOWER(tgt.channel_country) AND LOWER(iop.channel_brand) = LOWER(tgt.channel_brand) AND LOWER(iop.selling_channel) = LOWER(tgt.selling_channel);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt 
   SET
    is_online_purchasable = iop_omni.is_online_purchasable 
	FROM 
	(SELECT rd.rms_style_num, rd.color_num, rd.channel_country, rd.channel_brand, CASE WHEN 
	MAX(CASE WHEN LOWER(popid.is_online_purchasable) = LOWER('Y') 
	THEN 1 ELSE 0 END) = 1 
	THEN 'Y' ELSE NULL END AS is_online_purchasable
        FROM raw_data AS rd
            INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl ON LOWER(etl.interface_code) = LOWER('CMD_WKLY') AND rd.metrics_date = etl.dw_batch_dt
            LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_online_purchasable_item_dim AS popid ON LOWER(popid.rms_sku_num) = LOWER(rd.rms_sku_id) AND LOWER(popid.channel_country) = LOWER(rd.channel_country) AND LOWER(popid.channel_brand) = LOWER(rd.channel_brand) 
			AND LOWER(rd.selling_channel) IN (LOWER('ONLINE'), LOWER('STORE')) 
     AND LOWER(popid.selling_channel) = LOWER(rd.selling_channel)
	     AND RANGE_CONTAINS(RANGE(popid.EFF_BEGIN_TMSTP_UTC, popid.EFF_END_TMSTP_UTC) , CAST(rd.METRICS_DATE AS TIMESTAMP) + INTERVAL '1' DAY - INTERVAL '0.001' SECOND )
        GROUP BY rd.rms_style_num, rd.color_num, rd.channel_country, rd.channel_brand) AS iop_omni
WHERE tgt.snapshot_date = (SELECT dw_batch_dt
                            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                            WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND LOWER(COALESCE(iop_omni.rms_style_num, 'NA')) = LOWER(COALESCE(tgt.rms_style_num, 'NA')) AND LOWER(COALESCE(iop_omni.color_num, 'NA')) = LOWER(COALESCE(tgt.color_num, 'NA')) AND LOWER(iop_omni.channel_country) = LOWER(tgt.channel_country) AND LOWER(iop_omni.channel_brand) = LOWER(tgt.channel_brand) AND LOWER(tgt.selling_channel) = LOWER('OMNI');
							

END;

BEGIN

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
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl ON LOWER(etl.interface_code) = LOWER('CMD_WKLY') 
 AND rd.metrics_date
    = etl.dw_batch_dt
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_online_purchasable_item_dim AS popid ON LOWER(popid.rms_sku_num) = LOWER(rd.rms_sku_id
      ) 
	  AND LOWER(popid.channel_country) = LOWER(rd.channel_country) 
	  AND LOWER(popid.channel_brand) = LOWER(rd.channel_brand
     ) AND LOWER(popid.selling_channel) = LOWER(rd.selling_channel)
	  AND RANGE_CONTAINS(RANGE(popid.EFF_BEGIN_TMSTP_UTC, popid.EFF_END_TMSTP_UTC) , CAST(rd.METRICS_DATE AS TIMESTAMP) + INTERVAL '1' DAY - INTERVAL '0.001' SECOND )
WHERE LOWER(rd.selling_channel) IN (LOWER('ONLINE'), LOWER('STORE'))
 AND LOWER(popid.is_online_purchasable) IN (LOWER('Y'), LOWER('N'))
GROUP BY rd.rms_style_num,
 rd.color_num,
 rd.channel_country,
 rd.channel_brand,
 rd.selling_channel,
 popid.is_online_purchasable;

END;

BEGIN

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt 
 SET
    online_purchasable_eff_begin_tmstp = temp.online_purchasable_eff_begin_tmstp,
    online_purchasable_eff_begin_tmstp_tz = temp.online_purchasable_eff_begin_tmstp_tz,
    online_purchasable_eff_end_tmstp = temp.online_purchasable_eff_end_tmstp,
    online_purchasable_eff_end_tmstp_tz = temp.online_purchasable_eff_end_tmstp_tz

    FROM (SELECT rms_style_num, color_num, channel_country, channel_brand, online_purchasable_eff_begin_tmstp, online_purchasable_eff_end_tmstp, is_online_purchasable, selling_channel, 
   `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(online_purchasable_eff_begin_tmstp as string)) as online_purchasable_eff_begin_tmstp_tz,

   `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(online_purchasable_eff_end_tmstp as string)) as online_purchasable_eff_end_tmstp_tz

        FROM temp_popid_records AS temp) AS temp
WHERE tgt.snapshot_date = (SELECT dw_batch_dt
                                FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                                WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND LOWER(COALESCE(tgt.rms_style_num, 'NA')) = LOWER(COALESCE(temp.rms_style_num, 'NA')) AND LOWER(COALESCE(tgt.color_num, 'NA')) = LOWER(COALESCE(temp.color_num, 'NA')) AND LOWER(tgt.channel_country) = LOWER(temp.channel_country) AND LOWER(tgt.channel_brand) = LOWER(temp.channel_brand) AND LOWER(tgt.is_online_purchasable) = LOWER(temp.is_online_purchasable) AND LOWER(tgt.selling_channel) = LOWER(temp.selling_channel);

END;

BEGIN


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS  tgt 
SET
    online_purchasable_eff_begin_tmstp = temp.online_purchasable_eff_begin_tmstp,
    online_purchasable_eff_begin_tmstp_tz = temp.online_purchasable_eff_begin_tmstp_tz,
    online_purchasable_eff_end_tmstp = temp.online_purchasable_eff_end_tmstp,
     online_purchasable_eff_end_tmstp_tz = temp.online_purchasable_eff_end_tmstp_tz

    FROM (SELECT rms_style_num, color_num, channel_country, channel_brand, is_online_purchasable, 
    MIN(online_purchasable_eff_begin_tmstp) AS online_purchasable_eff_begin_tmstp,

    MAX(online_purchasable_eff_end_tmstp) AS online_purchasable_eff_end_tmstp,
   `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(MIN(online_purchasable_eff_begin_tmstp) AS STRING)) as online_purchasable_eff_begin_tmstp_tz, `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(MAX(online_purchasable_eff_end_tmstp) AS STRING)) as online_purchasable_eff_end_tmstp_tz

        FROM temp_popid_records AS temp
        GROUP BY rms_style_num, color_num, channel_country, channel_brand, is_online_purchasable) AS temp
WHERE tgt.snapshot_date = (SELECT dw_batch_dt
                                FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                                WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND LOWER(COALESCE(tgt.rms_style_num, 'NA')) = LOWER(COALESCE(temp.rms_style_num, 'NA')) AND LOWER(COALESCE(tgt.color_num, 'NA')) = LOWER(COALESCE(temp.color_num, 'NA')) AND LOWER(tgt.channel_country) = LOWER(temp.channel_country) AND LOWER(tgt.channel_brand) = LOWER(temp.channel_brand) AND LOWER(tgt.is_online_purchasable) = LOWER(temp.is_online_purchasable) AND LOWER(tgt.selling_channel) = LOWER('OMNI');

END;

BEGIN

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

END;

BEGIN


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
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl ON LOWER(etl.interface_code) = LOWER('CMD_WKLY') AND oo.snapshot_week_date
     = etl.dw_batch_dt
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.price_store_dim_vw AS psdv ON oo.store_num = psdv.store_num
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS psd ON LOWER(oo.rms_sku_num) = LOWER(psd.rms_sku_num) AND LOWER(psdv
     .channel_country) = LOWER(psd.channel_country) 
	AND RANGE_CONTAINS(RANGE(psd.EFF_BEGIN_TMSTP_UTC, psd.EFF_END_TMSTP_UTC) , CAST(oo.SNAPSHOT_WEEK_DATE AS TIMESTAMP) + INTERVAL '1' DAY - INTERVAL '0.001' SECOND )
 WHERE LOWER(psdv.store_type_code) IN (LOWER('FC'), LOWER('FL'), LOWER('LH'), LOWER('NL'), LOWER('OC'), LOWER('OF'),
    LOWER('RK'), LOWER('SS'), LOWER('VS')));
	
EXCEPTION WHEN ERROR THEN
SET _ERROR_MESSAGE  =  @@error.message;

END;



BEGIN
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact SET
    on_order_units_qty = 0,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP)
    ,dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()  --added TZ column
WHERE snapshot_date = (SELECT dw_batch_dt
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
        WHERE LOWER(interface_code) = LOWER('CMD_WKLY'));
		

END;

BEGIN


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt  SET
    on_order_units_qty = src.on_order_units_qty,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP)
 ,dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()  --added TZ column
     FROM (SELECT rms_style_num, color_num, channel_country, channel_brand, selling_channel, SUM(on_order_units_qty) AS on_order_units_qty
        FROM on_order
        GROUP BY rms_style_num, color_num, channel_country, channel_brand, selling_channel) AS src
WHERE tgt.snapshot_date = (SELECT dw_batch_dt
                            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                            WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND LOWER(COALESCE(src.rms_style_num, 'NA')) = LOWER(COALESCE(tgt.rms_style_num, 'NA')) AND LOWER(COALESCE(src.color_num, 'NA')) = LOWER(COALESCE(tgt.color_num, 'NA')) AND LOWER(src.channel_country) = LOWER(tgt.channel_country) AND LOWER(src.channel_brand) = LOWER(tgt.channel_brand) AND LOWER(src.selling_channel) = LOWER(tgt.selling_channel);

END;

BEGIN


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt 
SET
    on_order_units_qty = omni.on_order_units_qty,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP)
     ,dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()  --added TZ column
     FROM (SELECT rms_style_num, color_num, channel_country, channel_brand, SUM(on_order_units_qty) AS on_order_units_qty
        FROM on_order
        GROUP BY rms_style_num, color_num, channel_country, channel_brand) AS omni
WHERE tgt.snapshot_date = (SELECT dw_batch_dt
                            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                            WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND LOWER(COALESCE(omni.rms_style_num, 'NA')) = LOWER(COALESCE(tgt.rms_style_num, 'NA')) AND LOWER(COALESCE(omni.color_num, 'NA')) = LOWER(COALESCE(tgt.color_num, 'NA')) AND LOWER(omni.channel_country) = LOWER(tgt.channel_country) AND LOWER(omni.channel_brand) = LOWER(tgt.channel_brand) AND LOWER(tgt.selling_channel) = LOWER('OMNI');

END;

BEGIN


CREATE TEMPORARY TABLE IF NOT EXISTS rack_retail (
rms_style_num STRING,
color_num STRING,
channel_country STRING NOT NULL,
selling_channel STRING NOT NULL,
current_price_amt NUMERIC(15,2)
);

END;

BEGIN


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact SET
    current_rack_retail = clearance_markdown_insights_by_week_hist_fact.regular_price_amt,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP)
     ,dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()  --added TZ column
WHERE snapshot_date = (SELECT dw_batch_dt
                FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND LOWER(channel_brand) = LOWER('NORDSTROM') AND regular_price_amt IS NOT NULL;

END;

BEGIN

INSERT INTO rack_retail
(SELECT cmibwf.rms_style_num,
  cmibwf.color_num,
  cmibwf.channel_country,
  cmibwf.selling_channel,
  pptd.selling_retail_price_amt AS current_rack_retail
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_hist_fact AS cmibwf
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl 
  ON LOWER(etl.interface_code) = LOWER('CMD_WKLY') 
  AND cmibwf.snapshot_date = etl.dw_batch_dt
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS psd 
  ON LOWER(cmibwf.rms_style_num) = LOWER(psd.rms_style_num) 
  AND LOWER(cmibwf.color_num) = LOWER(psd.color_num) AND LOWER(cmibwf.channel_country) = LOWER(psd.channel_country)
     AND RANGE_CONTAINS(RANGE(psd.EFF_BEGIN_TMSTP_UTC, psd.EFF_END_TMSTP_UTC) , CAST(etl.DW_BATCH_DT AS TIMESTAMP) + INTERVAL '1' DAY - INTERVAL '0.001' SECOND )
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_price_timeline_dim AS pptd ON LOWER(psd.rms_sku_num) = LOWER(pptd.rms_sku_num) 
  AND LOWER(psd.channel_country) = LOWER(pptd.channel_country) 
  AND LOWER(cmibwf.channel_brand) = LOWER(pptd.channel_brand) 
  AND LOWER(cmibwf.selling_channel) = LOWER(pptd.selling_channel)
 AND RANGE_CONTAINS(RANGE(pptd.EFF_BEGIN_TMSTP_UTC, pptd.EFF_END_TMSTP_UTC) , CAST(etl.DW_BATCH_DT AS TIMESTAMP) + INTERVAL '1' DAY - INTERVAL '0.001' SECOND )
 WHERE LOWER(pptd.channel_brand) = LOWER('NORDSTROM_RACK'));
 

END;

BEGIN

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt SET
    current_rack_retail = t1.current_rack_retail,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP)
    ,dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()  --added TZ column
    FROM (SELECT rms_style_num, color_num, channel_country, selling_channel, current_price_amt AS current_rack_retail, COUNT(current_price_amt) AS current_rack_retail_cnt
        FROM rack_retail
        GROUP BY rms_style_num, color_num, channel_country, selling_channel, current_price_amt
        QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_style_num, color_num, channel_country, selling_channel ORDER BY current_rack_retail_cnt DESC, current_price_amt)) = 1) AS t1
WHERE tgt.snapshot_date = (SELECT dw_batch_dt
                            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                            WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND LOWER(COALESCE(t1.rms_style_num, 'NA')) = LOWER(COALESCE(tgt.rms_style_num, 'NA')) AND LOWER(COALESCE(t1.color_num, 'NA')) = LOWER(COALESCE(tgt.color_num, 'NA')) AND LOWER(t1.channel_country) = LOWER(tgt.channel_country) AND LOWER(t1.selling_channel) = LOWER(tgt.selling_channel) AND LOWER(tgt.channel_brand) = LOWER('NORDSTROM');
EXCEPTION WHEN ERROR THEN

END;

BEGIN


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt SET
    current_rack_retail = t2.current_rack_retail,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) 
     ,dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()  --added TZ column
    FROM (SELECT rms_style_num, color_num, channel_country, current_price_amt AS current_rack_retail, COUNT(current_price_amt) AS current_rack_retail_cnt
        FROM rack_retail
        GROUP BY rms_style_num, color_num, channel_country, current_rack_retail
        QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_style_num, color_num, channel_country ORDER BY current_rack_retail_cnt DESC, current_rack_retail)) = 1) AS t2
WHERE tgt.snapshot_date = (SELECT dw_batch_dt
                            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                            WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND LOWER(COALESCE(t2.rms_style_num, 'NA')) = LOWER(COALESCE(tgt.rms_style_num, 'NA')) AND LOWER(COALESCE(t2.color_num, 'NA')) = LOWER(COALESCE(tgt.color_num, 'NA')) AND LOWER(t2.channel_country) = LOWER(tgt.channel_country) AND LOWER(tgt.channel_brand) = LOWER('NORDSTROM') AND LOWER(tgt.selling_channel) = LOWER('OMNI');
							

END;

BEGIN

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt 
SET
    first_rack_date = src.first_rack_date,
    rack_ind = 'Y',
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP)
     ,dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()  --added TZ column
	FROM (SELECT rms_style_num, color_num, channel_country, channel_brand, MIN(first_rack_date) AS first_rack_date
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_hist_fact
        WHERE snapshot_date = (SELECT dw_batch_dt
                    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                    WHERE LOWER(interface_code) = LOWER('CMD_WKLY'))
        GROUP BY rms_style_num, color_num, channel_country, channel_brand
        HAVING first_rack_date IS NOT NULL) AS src
WHERE tgt.snapshot_date = (SELECT dw_batch_dt
                            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                            WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND LOWER(COALESCE(tgt.rms_style_num, 'NA')) = LOWER(COALESCE(src.rms_style_num, 'NA')) AND LOWER(COALESCE(tgt.color_num, 'NA')) = LOWER(COALESCE(src.color_num, 'NA')) AND LOWER(tgt.channel_country) = LOWER(src.channel_country) AND LOWER(tgt.channel_brand) = LOWER(src.channel_brand) AND (LOWER(tgt.selling_channel) = LOWER('OMNI') OR tgt.total_inv_qty = 0);
END;

