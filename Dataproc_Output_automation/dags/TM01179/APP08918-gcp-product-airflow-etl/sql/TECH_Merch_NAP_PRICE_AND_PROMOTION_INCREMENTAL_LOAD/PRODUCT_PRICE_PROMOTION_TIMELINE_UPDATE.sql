

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_promotion_timeline_pushdown_stg;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_promotion_timeline_pushdown_stg
(
rms_sku_num ,
channel_country,
selling_channel,
channel_brand,
promo_id,
eff_begin_tmstp,
eff_begin_tmstp_tz,
eff_end_tmstp,
eff_end_tmstp_tz,
source_inactive_desc_drvd,
source_inactive_tmstp
)
SELECT DISTINCT
promotion.rms_sku_num,promotion.channel_country,promotion.selling_channel,promotion.channel_brand,
promotion.promo_id,promotion.eff_begin_tmstp,promotion.eff_begin_tmstp_tz,promotion.eff_end_tmstp,promotion.eff_end_tmstp_tz,
'FUTURE_CLEANUP' AS source_inactive_desc_drvd,
CAST(NULL AS TIMESTAMP) AS source_inactive_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_promotion_timeline_dim promotion
LEFT OUTER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_price_timeline_dim price
	ON LOWER(promotion.rms_sku_num)=LOWER(price.rms_sku_num)
	AND LOWER(promotion.channel_country)=LOWER(price.channel_country)
	AND LOWER(promotion.selling_channel)=LOWER(price.selling_channel)
	AND LOWER(promotion.channel_brand)=LOWER(price.channel_brand)
	AND LOWER(promotion.promo_id)=LOWER(price.selling_retail_record_id)
	AND RANGE_OVERLAPS(RANGE(promotion.eff_begin_tmstp, promotion.eff_end_tmstp), RANGE(price.eff_begin_tmstp, price.eff_end_tmstp))
INNER JOIN (SELECT rms_sku_num,channel_country,selling_channel,channel_brand,MIN(min_asof_tmstp_utc) AS min_asof_tmstp
	  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_price_timeline_key_stg
	  GROUP BY rms_sku_num,channel_country,selling_channel,channel_brand) AS price_key_stg
	ON LOWER(promotion.rms_sku_num)=LOWER(price_key_stg.rms_sku_num)
	AND LOWER(promotion.channel_country)=LOWER(price_key_stg.channel_country)
	AND LOWER(promotion.selling_channel)=LOWER(price_key_stg.selling_channel)
	AND LOWER(promotion.channel_brand)=LOWER(price_key_stg.channel_brand)
WHERE --SOURCE_INACTIVE_DESC_DRVD IS NOT NULL AND
 (LOWER(promotion.promotion_type_code) =LOWER('SIMPLE') OR (LOWER(promotion.promotion_type_code) =LOWER('ENTICEMENT') AND promotion.promo_id LIKE 'C%'))
AND price.rms_sku_num IS NULL
AND (promotion.eff_begin_tmstp>=price_key_stg.min_asof_tmstp  AND promotion.eff_end_tmstp>=price_key_stg.min_asof_tmstp )
;



BEGIN TRANSACTION;

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.product_promotion_timeline_opr AS target SET
    source_inactive_ind = 'Y',
    source_inactive_desc = 'FUTURE_CLEANUP',
    source_inactive_tmstp = SOURCE.source_inactive_tmstp_fc,
    source_inactive_tmstp_tz = SOURCE.source_event_tmstp_tz,
    dw_batch_id = CAST(RPAD(FORMAT_TIMESTAMP('%Y%m%d%I%M%S', CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)), 14, ' ') AS BIGINT),
    dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) 
FROM (SELECT stg.rms_sku_num, stg.channel_country, stg.selling_channel, stg.channel_brand, stg.promo_id, stg.eff_begin_tmstp_utc, stg.eff_end_tmstp_utc, MAX(price_opr.source_event_tmstp_utc) AS source_inactive_tmstp_fc,`{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE` (CAST(MAX(price_opr.source_event_tmstp) AS STRING)) AS source_event_tmstp_tz
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_promotion_timeline_pushdown_stg AS stg
            INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_price_timeline_opr AS price_opr ON LOWER(price_opr.rms_sku_num) = LOWER(stg.rms_sku_num) AND LOWER(price_opr.channel_brand) = LOWER(stg.channel_brand) AND LOWER(price_opr.channel_country) = LOWER(stg.channel_country) AND LOWER(price_opr.selling_channel) = LOWER(stg.selling_channel)
        WHERE price_opr.dw_sys_load_date BETWEEN (SELECT MIN(CAST(source_event_tmstp AS DATE))
                    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_price_timeline_key_stg) AND (SELECT MAX(CAST(source_event_tmstp AS DATE))
                    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_price_timeline_key_stg) AND LOWER(stg.source_inactive_desc_drvd) = LOWER('FUTURE_CLEANUP')
        GROUP BY stg.rms_sku_num, stg.channel_country, stg.selling_channel, stg.channel_brand, stg.promo_id, stg.eff_begin_tmstp_utc, stg.eff_end_tmstp_utc) AS SOURCE
WHERE LOWER(target.rms_sku_num) = LOWER(SOURCE.rms_sku_num) AND LOWER(target.channel_brand) = LOWER(SOURCE.channel_brand) AND LOWER(target.channel_country) = LOWER(SOURCE.channel_country) AND LOWER(target.selling_channel) = LOWER(SOURCE.selling_channel) AND (LOWER(COALESCE(target.simple_promotion_record_id, '-1')) = LOWER(SOURCE.promo_id) OR LOWER(COALESCE(target.enticement_record_id, '-1')) = LOWER(SOURCE.promo_id)) AND target.asof_tmstp = SOURCE.eff_begin_tmstp_utc AND target.dw_sys_load_date BETWEEN DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 60 MONTH) AND (CURRENT_DATE('PST8PDT'));


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_promotion_timeline_dim AS promotion_dim
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_promotion_timeline_pushdown_stg AS stg
    WHERE LOWER(promotion_dim.rms_sku_num) = LOWER(stg.rms_sku_num) AND LOWER(promotion_dim.channel_brand) = LOWER(stg.channel_brand) AND LOWER(promotion_dim.channel_country) = LOWER(stg.channel_country) AND LOWER(promotion_dim.selling_channel) = LOWER(stg.selling_channel) AND LOWER(promotion_dim.promo_id) = LOWER(stg.promo_id) AND promotion_dim.eff_begin_tmstp = stg.eff_begin_tmstp_utc AND promotion_dim.eff_end_tmstp = stg.eff_end_tmstp_utc  AND LOWER(source_inactive_desc_drvd) = LOWER('FUTURE_CLEANUP'));
COMMIT TRANSACTION;

