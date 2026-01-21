--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=stg-dim;AppSubArea=ITEM_SELLING_STATUS;' UPDATE FOR SESSION;

-- NONSEQUENCED VALIDTIME --Purge work table for staging temporal rows

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_status_dim_vtw;

--.IF ERRORCODE <> 0 THEN .QUIT 1

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_status_dim_vtw (rms_sku_num, channel_country, channel_brand,
 selling_channel, selling_status_code,eff_begin_tmstp,eff_begin_tmstp_tz,eff_end_tmstp,eff_end_tmstp_tz)


  WITH SRC AS 
   (SELECT 
   distinct rms_sku_num,
        channel_country,
        channel_brand,
        selling_channel,
        selling_status_code,
        eff_begin_tmstp,
        eff_begin_tmstp_tz,
        eff_end_tmstp,
        eff_end_tmstp_tz,
        range(eff_begin_tmstp, eff_end_tmstp ) AS eff_period

     FROM (

    SELECT rms_sku_num,channel_country,channel_brand,selling_channel,selling_status_code,MIN(eff_begin_tmstp_tz) AS eff_begin_tmstp_tz,MAX(eff_end_tmstp_tz) AS eff_end_tmstp_tz, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
            FROM  

    (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num,channel_country,channel_brand,selling_channel,selling_status_code ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM
    (
     SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY rms_sku_num,channel_country,channel_brand,selling_channel,selling_status_code
				ORDER BY eff_begin_tmstp) >= DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
    FROM (   
      SELECT 
        rms_sku_num,
        channel_country,
        channel_brand,
        selling_channel,
        selling_status_code,
        src1.eff_begin_tmstp,
        `{{params.gcp_project_id}}.jwn_udf.UDF_TIME_ZONE`(cast(src1.eff_begin_tmstp as string)) as eff_begin_tmstp_tz

              , (COALESCE(MIN(src1.eff_begin_tmstp) OVER(PARTITION BY src1.rms_sku_num, src1.channel_country, src1.channel_brand, src1.selling_channel
            ORDER BY src1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
           ,TIMESTAMP'9999-12-31 23:59:59.999999+00:00') ) as  eff_end_tmstp,

           `{{params.gcp_project_id}}.jwn_udf.UDF_TIME_ZONE`(cast(COALESCE(MIN(src1.eff_begin_tmstp) OVER(PARTITION BY src1.rms_sku_num, src1.channel_country, src1.channel_brand, src1.selling_channel
            ORDER BY src1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
           ,TIMESTAMP'9999-12-31 23:59:59.999999+00:00') AS STRING)) as  eff_end_tmstp_tz
       FROM (SELECT DISTINCT itemid AS rms_sku_num,
          sellingcountry AS channel_country,
          sellingbrand AS channel_brand,
          'ONLINE' AS selling_channel,
          sellingstatus AS selling_status_code
          , nord_udf.epoch_tmstp(cast(lastupdatedtimestamp as int64)) AS eff_begin_tmstp
         FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_item_selling_status_ldg)
 AS src1) AS src2
         	  WHERE eff_begin_tmstp < eff_end_tmstp
   ) AS ordered_data
   --AS src
    ) AS grouped_data
GROUP BY rms_sku_num,channel_country,channel_brand,selling_channel,selling_status_code, range_group
ORDER BY rms_sku_num,channel_country,channel_brand,selling_channel,selling_status_code, eff_begin_tmstp
     )
   )





SELECT DISTINCT 
  rms_sku_num,
  channel_country,
  channel_brand,
  selling_channel,
  selling_status_code,
  MIN(RANGE_START(eff_period)) AS eff_begin,
  `{{params.gcp_project_id}}.jwn_udf.UDF_TIME_ZONE`(cast(MIN(RANGE_START(eff_period)) as string)) as eff_begin_tmstp_tz,
  MAX(RANGE_END(eff_period)) AS eff_end,
  `{{params.gcp_project_id}}.jwn_udf.UDF_TIME_ZONE`(cast(MAX(RANGE_END(eff_period)) as string)) as eff_end_tmstp_tz
 FROM 
(

 SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num,channel_country,channel_brand,selling_channel,selling_status_code
				ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM
(
SELECT *,CASE WHEN LAG(eff_end_tmstp) OVER (PARTITION BY rms_sku_num,channel_country,channel_brand,selling_channel,selling_status_code
 ORDER BY eff_begin_tmstp) >= DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
            FROM
 (SELECT  --normalize
 distinct 
    src.rms_sku_num,
    src.channel_country,
    src.channel_brand,
    src.selling_channel,
    src.selling_status_code,
    tgt.eff_begin_tmstp_utc as eff_begin_tmstp,
    tgt.eff_end_tmstp_utc as eff_end_tmstp,
    COALESCE( RANGE_INTERSECT(src.eff_period , range(TGT.eff_begin_tmstp_utc,TGT.eff_end_tmstp_utc))
            , src.eff_period ) AS eff_period
   FROM SRC
      LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_item_selling_status_dim AS tgt ON LOWER(src.rms_sku_num) = LOWER(tgt.rms_sku_num
           ) AND LOWER(src.channel_country) = LOWER(tgt.channel_country) AND LOWER(src.channel_brand) = LOWER(tgt.channel_brand
          ) AND LOWER(src.selling_channel) = LOWER(tgt.selling_channel)
     WHERE tgt.rms_sku_num IS NULL
      OR LOWER(src.selling_status_code) <> LOWER(tgt.selling_status_code)
      OR tgt.selling_status_code IS NULL AND src.selling_status_code IS NOT NULL
      OR src.selling_status_code IS NULL AND tgt.selling_status_code IS NOT NULL
      ) AS ordered_data
) AS grouped_data

 WHERE NOT EXISTS (SELECT 1 AS `A12180`
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_status_dim_vtw
      WHERE LOWER(rms_sku_num) = LOWER(grouped_data.rms_sku_num)
      AND LOWER(channel_country) = LOWER(grouped_data.channel_country)
      AND LOWER(channel_brand) = LOWER(grouped_data.channel_brand)
      AND LOWER(selling_channel) = LOWER(grouped_data.selling_channel))
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, channel_country, channel_brand, selling_channel)) = 1)

GROUP BY rms_sku_num,channel_country,channel_brand,selling_channel,selling_status_code,range_group
ORDER BY rms_sku_num,channel_country,channel_brand,selling_channel,selling_status_code
   ;
      
DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_status_dim AS tgt
WHERE EXISTS (SELECT *
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_status_dim_vtw AS src
 WHERE LOWER(rms_sku_num) = LOWER(tgt.rms_sku_num)
  AND LOWER(channel_country) = LOWER(tgt.channel_country)
  AND LOWER(channel_brand) = LOWER(tgt.channel_brand)
  AND LOWER(selling_channel) = LOWER(tgt.selling_channel));


--.IF ERRORCODE <> 0 THEN .QUIT 4

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_status_dim (rms_sku_num, channel_country, channel_brand, selling_channel,
 selling_status_code, eff_begin_tmstp,eff_begin_tmstp_tz, eff_end_tmstp,eff_end_tmstp_tz, dw_batch_id, dw_batch_date, dw_sys_load_tmstp)
(SELECT DISTINCT rms_sku_num,
  channel_country,
  channel_brand,
  selling_channel,
  selling_status_code,
  eff_begin_tmstp,
  eff_begin_tmstp_tz,
  eff_end_tmstp,
  eff_end_tmstp_tz,
  dw_batch_id,
  dw_batch_date,
  dw_sys_load_tmstp
 FROM (SELECT rms_sku_num,
    channel_country,
    channel_brand,
    selling_channel,
    selling_status_code,
    eff_begin_tmstp,
    eff_begin_tmstp_tz,
    eff_end_tmstp,
    eff_end_tmstp_tz,
    CAST(RPAD(FORMAT_TIMESTAMP('%Y%m%d%I%M%S', CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)), 14, ' ') AS BIGINT)
    AS dw_batch_id,
    CURRENT_DATE('PST8PDT') AS dw_batch_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_status_dim_vtw) AS t
 WHERE NOT EXISTS (SELECT 1 AS `A12180`
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_status_dim
  WHERE LOWER(rms_sku_num) = LOWER(t.rms_sku_num)
    AND LOWER(channel_country) = LOWER(t.channel_country)
    AND LOWER(channel_brand) = LOWER(t.channel_brand)
    AND LOWER(selling_channel) = LOWER(t.selling_channel))
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, channel_country, channel_brand, selling_channel)) = 1);
  

