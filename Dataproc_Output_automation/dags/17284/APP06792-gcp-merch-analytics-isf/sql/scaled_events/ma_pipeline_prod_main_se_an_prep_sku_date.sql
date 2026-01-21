/******************************************************************************
Name: Scaled Events Anniversary SKU/CHNL/DATE Table
APPID-Name: APP08204 - Scaled Events Reporting
Purpose: Populate the Anniversary SKU/CHNL/DATE lookup table
Variable(s):    {environment_schema} - T2DL_DAS_SCALED_EVENTS
DAG: merch_se_an_prep_sku_date
TABLE NAME: T2DL_DAS_SCALED_EVENTS.ANNIVERSARY_SKU_CHNL_DATE (T3: T3DL_ACE_PRA.ANNIVERSARY_SKU_CHNL_DATE)
Author(s): Alli Moore, Manuela Hurtado
Date Last Updated: 05-13-2024
******************************************************************************/
	


--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'ANNIVERSARY_SKU_CHNL_DATE', OUT_RETURN_MSG);
DROP TABLE IF EXISTS `{{params.gcp_project_id}}`.{{params.environment_schema}}.anniversary_sku_chnl_date;



CREATE TABLE IF NOT EXISTS `{{params.gcp_project_id}}`.{{params.environment_schema}}.anniversary_sku_chnl_date (
sku_idnt STRING NOT NULL,
channel_country STRING NOT NULL,
selling_channel STRING NOT NULL,
store_type_code STRING NOT NULL,
day_idnt INTEGER NOT NULL,
day_dt DATE NOT NULL,
ty_ly_ind STRING NOT NULL,
anniv_item_ind SMALLINT NOT NULL,
reg_price_amt NUMERIC(20,2) DEFAULT 0.00,
spcl_price_amt NUMERIC(20,2) DEFAULT 0.00
)
PARTITION BY RANGE_BUCKET(day_idnt, GENERATE_ARRAY(2023001, 2024364, 1))
/* CLUSTER BY sku_idnt */
;


--GRANT SELECT ON T2DL_DAS_SCALED_EVENTS.ANNIVERSARY_SKU_CHNL_DATE  TO PUBLIC


CREATE TEMPORARY TABLE IF NOT EXISTS anniv_sku
AS
SELECT pptd.rms_sku_num AS sku_idnt,
 pptd.channel_country,
 pptd.store_num,
 pptd.selling_channel,
  CASE
  WHEN EXTRACT(YEAR FROM pptd.enticement_start_tmstp_utc) = EXTRACT(YEAR FROM CURRENT_DATE('PST8PDT'))
  THEN 'TY'
  ELSE 'LY'
  END AS ty_ly_ind,
 AVG(price_dim.regular_price_amt) AS regular_price_amt,
 AVG(price_dim.selling_retail_price_amt) AS current_price_amt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_promotion_timeline_dim AS pptd
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim AS price_dim 
 ON LOWER(price_dim.rms_sku_num) = LOWER(pptd.rms_sku_num
        ) AND LOWER(price_dim.channel_country) = LOWER(pptd.channel_country) AND LOWER(price_dim.selling_channel) =
      LOWER(pptd.selling_channel) AND LOWER(price_dim.channel_brand) = LOWER(pptd.channel_brand) AND LOWER(price_dim.selling_retail_record_id
     ) = LOWER(pptd.promo_id) AND RANGE_OVERLAPS(RANGE(price_dim.eff_begin_tmstp_utc, price_dim.eff_end_tmstp_utc
    ), RANGE(pptd.enticement_start_tmstp_utc, pptd.enticement_end_tmstp_utc))
WHERE LOWER(pptd.channel_country) = LOWER('US')
 AND LOWER(pptd.enticement_tags) LIKE LOWER('%ANNIVERSARY_SALE%')
 AND EXTRACT(YEAR FROM pptd.enticement_start_tmstp_utc) <= EXTRACT(YEAR FROM CURRENT_DATE('PST8PDT'))
 AND EXTRACT(YEAR FROM pptd.enticement_start_tmstp_utc) >= EXTRACT(YEAR FROM CURRENT_DATE('PST8PDT')) - 1
 AND pptd.rms_sku_num NOT IN (SELECT DISTINCT rms_sku_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS psdv
   WHERE LOWER(channel_country) = LOWER('US') AND LOWER(style_group_num) = LOWER('6133999') AND LOWER(color_num) = LOWER('101'
        ) AND LOWER(supp_part_num) = LOWER('DA6364')
    OR LOWER(channel_country) = LOWER('US') AND LOWER(style_group_num) = LOWER('6088468') AND LOWER(color_num) = LOWER('100'
        ) AND LOWER(supp_part_num) = LOWER('BQ6806'))
GROUP BY sku_idnt,
 pptd.channel_country,
 pptd.store_num,
 pptd.selling_channel,
 ty_ly_ind;


--COLLECT STATISTICS COLUMN(sku_idnt, store_num) ON anniv_sku


CREATE TEMPORARY TABLE IF NOT EXISTS anniv_sku_final
AS
SELECT sku_idnt,
 channel_country,
 store_num,
 selling_channel,
 ty_ly_ind,
 MAX(regular_price_amt) AS regular_price_amt,
 MAX(current_price_amt) AS current_price_amt
FROM (SELECT *
   FROM anniv_sku
   UNION ALL
   SELECT a.sku_idnt,
    a.channel_country,
    '1' AS store_num,
    'STORE' AS selling_channel,
    a.ty_ly_ind,
    a.regular_price_amt,
    a.current_price_amt
   FROM anniv_sku AS a
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS psd ON LOWER(psd.rms_sku_num) = LOWER(a.sku_idnt) AND LOWER(psd.channel_country
       ) = LOWER(a.channel_country)
   WHERE psd.div_num = 340
    AND LOWER(a.store_num) = LOWER('808')) AS a
GROUP BY sku_idnt,
 channel_country,
 store_num,
 selling_channel,
 ty_ly_ind;


--COLLECT STATISTICS COLUMN(sku_idnt, store_num) ON anniv_sku_final


CREATE TEMPORARY TABLE IF NOT EXISTS price_store_lkp
AS
SELECT DISTINCT price_store_num,
 channel_country,
 selling_channel,
 store_type_code,
 store_type_desc,
 channel_num,
 channel_desc,
 channel_brand
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw
WHERE store_close_date IS NULL
 AND LOWER(channel_country) = LOWER('US')
 AND LOWER(channel_brand) = LOWER('NORDSTROM')
 AND channel_num IS NOT NULL;


--COLLECT STATISTICS COLUMN(store_type_code, price_store_num) 	,COLUMN (selling_channel, channel_country) 	ON price_store_lkp


CREATE TEMPORARY TABLE IF NOT EXISTS anniv_sku_chnl
AS
SELECT DISTINCT a.sku_idnt,
 a.ty_ly_ind,
 ps.store_type_code,
 a.channel_country,
 ps.channel_num,
 a.selling_channel,
 a.regular_price_amt,
 a.current_price_amt
FROM anniv_sku_final AS a
 INNER JOIN price_store_lkp AS ps ON ps.price_store_num = CAST(a.store_num AS FLOAT64) AND LOWER(ps.channel_country) =
    LOWER(a.channel_country) AND LOWER(ps.selling_channel) = LOWER(a.selling_channel);


--COLLECT STATISTICS COLUMN(sku_idnt, store_type_code) 	,COLUMN (selling_channel, channel_country) 	ON anniv_sku_chnl


CREATE TEMPORARY TABLE IF NOT EXISTS anniv_dates
AS
SELECT DISTINCT day_idnt,
 day_dt,
 ty_ly_lly AS ty_ly_ind,
 event_country AS country
FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.scaled_event_dates AS sed
WHERE anniv_ind = 1
 AND LOWER(ty_ly_lly) IN (LOWER('TY'), LOWER('LY'))
 AND LOWER(event_type) <> LOWER('Non-Event');


--COLLECT STATISTICS COLUMN(day_dt) ON anniv_dates


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.anniversary_sku_chnl_date;


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.anniversary_sku_chnl_date
(SELECT sku.sku_idnt,
  sku.channel_country,
  sku.selling_channel,
  sku.store_type_code,
  dates.day_idnt,
  dates.day_dt,
  dates.ty_ly_ind,
  CAST(trunc(cast('1' as float64)) AS SMALLINT) AS anniv_ind,
  AVG(sku.regular_price_amt) AS reg_price_amt,
  AVG(sku.current_price_amt) AS spcl_price_amt
 FROM anniv_sku_chnl AS sku
  INNER JOIN anniv_dates AS dates ON LOWER(dates.country) = LOWER(sku.channel_country) AND LOWER(dates.ty_ly_ind) =
    LOWER(sku.ty_ly_ind)
 WHERE sku.store_type_code IS NOT NULL
 GROUP BY sku.sku_idnt,
  sku.channel_country,
  sku.selling_channel,
  sku.store_type_code,
  dates.day_idnt,
  dates.day_dt,
  dates.ty_ly_ind,
  anniv_ind);


--COLLECT STATISTICS     COLUMN(sku_idnt)     , COLUMN(partition) ON T2DL_DAS_SCALED_EVENTS.ANNIVERSARY_SKU_CHNL_DATE 
-- COLLECT STATISTICS
--     COLUMN(sku_idnt)
--     , COLUMN(partition)
-- ON {environment_schema}.ANNIVERSARY_SKU_CHNL_DATE ;
