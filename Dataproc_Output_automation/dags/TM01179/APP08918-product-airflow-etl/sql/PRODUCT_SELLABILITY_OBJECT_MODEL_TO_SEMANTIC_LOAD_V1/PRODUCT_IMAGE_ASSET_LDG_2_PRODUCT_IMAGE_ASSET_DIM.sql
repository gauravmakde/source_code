TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_image_asset_dim_vtw;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_image_asset_dim_vtw (asset_id, channel_country, channel_brand, selling_channel,
 color_num, rms_style_group_num, image_url, is_hero_image, image_index, shot_name, eff_begin_tmstp,eff_begin_tmstp_tz
, eff_end_tmstp,eff_end_tmstp_tz)

--with cte
WITH SRC1 AS (
    SELECT 
        asset_id,
        channel_country,
        channel_brand,
        selling_channel,
        color_num,
        rms_style_group_num,
        image_url,
        is_hero_image,
        image_index,
        shot_name,
        eff_begin_tmstp,
        eff_end_tmstp
    FROM (
        -- Inner normalize
        SELECT 
            asset_id,
            channel_country,
            channel_brand,
            selling_channel,
            color_num,
            rms_style_group_num,
            image_url,
            is_hero_image,
            image_index,
            shot_name,
            MIN(eff_begin_tmstp) AS eff_begin_tmstp,
            MAX(eff_end_tmstp) AS eff_end_tmstp
        FROM (
            SELECT *,
                SUM(discontinuity_flag) OVER (
                    PARTITION BY 
                        asset_id, 
                        channel_country, 
                        channel_brand,
                        selling_channel,
                        color_num,
                        rms_style_group_num,
                        image_url,
                        is_hero_image,
                        image_index,
                        shot_name 
                    ORDER BY eff_begin_tmstp 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS range_group
            FROM (
                SELECT *,
                    CASE 
                        WHEN LAG(eff_end_tmstp) OVER (
                            PARTITION BY 
                                asset_id, 
                                channel_country, 
                                channel_brand,
                                selling_channel,
                                color_num,
                                rms_style_group_num,
                                image_url,
                                is_hero_image,
                                image_index,
                                shot_name 
                            ORDER BY eff_begin_tmstp
                        ) >= DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                        THEN 0
                        ELSE 1
                    END AS discontinuity_flag
                FROM (
                    SELECT DISTINCT 
                        assetid AS asset_id,
                        channelcountry AS channel_country,
                        channelbrand AS channel_brand,
                        sellingchannel AS selling_channel,
                        colorcode AS color_num,
                        stylegroupnum AS rms_style_group_num,
                        url AS image_url,
                        CASE
                            WHEN LOWER(isheroimage) = LOWER('true') THEN 'Y'
                            ELSE 'N'
                        END AS is_hero_image,
                        CAST(TRUNC(CAST(imageindex AS FLOAT64)) AS INTEGER) AS image_index,
                        shotname AS shot_name,
                        eff_begin_tmstp,
                        eff_end_tmstp,
                        RANGE(eff_begin_tmstp, eff_end_tmstp) AS eff_period
                    FROM (
                        SELECT *, 
                            CAST(`{{params.gcp_project_id}}`.jwn_udf.iso8601_tmstp(lastupdatedat) AS TIMESTAMP) AS eff_begin_tmstp,
                            CAST('9999-12-31 23:59:59.999999+00:00' AS TIMESTAMP) AS eff_end_tmstp 
                        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_image_asset_ldg
                    ) AS product_image_asset_ldg
                    QUALIFY LOWER(`{{params.gcp_project_id}}`.jwn_udf.iso8601_tmstp(lastupdatedat)) < LOWER(
                        COALESCE(
                            MAX(`{{params.gcp_project_id}}`.jwn_udf.iso8601_tmstp(lastupdatedat)) OVER (
                                PARTITION BY 
                                    assetid, 
                                    channelcountry, 
                                    channelbrand, 
                                    sellingchannel, 
                                    color_num, 
                                    rms_style_group_num 
                                ORDER BY `{{params.gcp_project_id}}`.jwn_udf.iso8601_tmstp(lastupdatedat) 
                                ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING
                            ), 
                            '0000-01-01 00:00:00'
                        )
                    )
                )) AS ordered_data
            ) AS grouped_data
            GROUP BY 
                asset_id, 
                channel_country, 
                channel_brand,
                selling_channel,
                color_num,
                rms_style_group_num,
                image_url,
                is_hero_image,
                image_index,
                shot_name,
                range_group
            ORDER BY  
                asset_id, 
                channel_country, 
                channel_brand,
                selling_channel,
                color_num,
                rms_style_group_num,
                image_url,
                is_hero_image,
                image_index,
                shot_name,
                eff_begin_tmstp
        )
    )



SELECT
asset_id
,channel_country
,channel_brand
,selling_channel
,color_num
,rms_style_group_num
,image_url
,is_hero_image
,image_index
,shot_name
,eff_begin
,`{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(cast(eff_begin as string)) as eff_begin_tmstp_tz
,eff_end
,`{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(cast(eff_end as string)) as eff_end_tmstp_tz
FROM
--second normalize

(
  SELECT
      asset_id, 
                channel_country, 
                channel_brand,
                selling_channel,
                color_num,
                rms_style_group_num,
                image_url,
                is_hero_image,
                image_index,
                shot_name,
      MIN(RANGE_START(eff_period)) AS eff_begin,
      MAX(RANGE_END(eff_period))   AS eff_end
       from  (
    --inner normalize
            SELECT asset_id, channel_country, channel_brand,selling_channel,color_num,rms_style_group_num,image_url,is_hero_image,image_index,shot_name,eff_period,range_group
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY asset_id, channel_country, channel_brand,selling_channel,color_num,rms_style_group_num,image_url,is_hero_image,image_index,shot_name ORDER BY eff_begin_tmstp_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM
      (
        
            --  NORMALIZE
         SELECT  asset_id, channel_country, channel_brand,selling_channel,color_num,rms_style_group_num,image_url,is_hero_image,image_index,shot_name,eff_begin_tmstp_utc,eff_end_tmstp_utc,
            CASE 
                WHEN LAG(eff_end_tmstp_utc) OVER (PARTITION BY asset_id, channel_country, channel_brand,selling_channel,color_num,rms_style_group_num,image_url,is_hero_image,image_index,shot_name ORDER BY eff_begin_tmstp_utc) >= 
                DATE_SUB(eff_begin_tmstp_utc, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag,
             eff_period ,
             from 
  (SELECT distinct SRC.asset_id,
  SRC.channel_country,
  SRC.channel_brand,
  SRC.selling_channel,
  SRC.color_num,
  SRC.rms_style_group_num,
  SRC.image_url,
  SRC.is_hero_image,
  SRC.image_index,
  SRC.shot_name,
  COALESCE( RANGE_INTERSECT(SRC.eff_period, RANGE(TGT.eff_begin_tmstp_utc,TGT.eff_end_tmstp_utc)), SRC.eff_period ) AS eff_period,TGT.eff_begin_tmstp_utc,TGT.eff_end_tmstp_utc
  FROM (
    --from cte
    select *,RANGE( eff_begin_tmstp, eff_end_tmstp ) AS eff_period
     from SRC1
  ) AS
  SRC
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_image_asset_dim AS tgt ON LOWER(SRC.asset_id) = LOWER(tgt.asset_id) AND LOWER(SRC.channel_country
         ) = LOWER(tgt.channel_country) AND LOWER(SRC.channel_brand) = LOWER(tgt.channel_brand) AND LOWER(SRC.selling_channel
       ) = LOWER(tgt.selling_channel) AND LOWER(SRC.color_num) = LOWER(tgt.color_num) AND LOWER(SRC.rms_style_group_num
     ) = LOWER(tgt.rms_style_group_num)
	  AND  RANGE_OVERLAPS (RANGE(tgt.eff_begin_tmstp, tgt.eff_end_tmstp),(RANGE(tgt.eff_begin_tmstp, tgt.eff_end_tmstp)))
 WHERE tgt.asset_id IS NULL
  OR LOWER(SRC.image_url) <> LOWER(tgt.image_url)
  OR tgt.image_url IS NULL AND SRC.image_url IS NOT NULL
  OR SRC.image_url IS NULL AND tgt.image_url IS NOT NULL
  OR LOWER(SRC.is_hero_image) <> LOWER(tgt.is_hero_image)
  OR tgt.is_hero_image IS NULL
  OR SRC.image_index <> tgt.image_index
  OR tgt.image_index IS NULL
  OR LOWER(SRC.shot_name) <> LOWER(tgt.shot_name) AND SRC.shot_name IS NOT NULL AND tgt.shot_name IS NOT NULL
  OR tgt.shot_name IS NULL AND SRC.shot_name IS NOT NULL
  OR SRC.shot_name IS NULL AND tgt.shot_name IS NOT NULL
 )
 --ordered
 )  AS ordered_data
) AS grouped_data)
GROUP BY asset_id, channel_country, channel_brand,selling_channel,color_num,rms_style_group_num,image_url,is_hero_image,image_index,shot_name,range_group
ORDER BY  asset_id, channel_country, channel_brand,selling_channel,color_num,rms_style_group_num,image_url,is_hero_image,image_index,shot_name);



BEGIN TRANSACTION;

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_image_asset_dim AS tgt
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_image_asset_dim_vtw AS src
    WHERE LOWER(asset_id) = LOWER(tgt.asset_id) AND LOWER(channel_country) = LOWER(tgt.channel_country) AND LOWER(channel_brand) = LOWER(tgt.channel_brand) AND LOWER(selling_channel) = LOWER(tgt.selling_channel) AND LOWER(color_num) = LOWER(tgt.color_num) AND LOWER(rms_style_group_num) = LOWER(tgt.rms_style_group_num));
	
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_image_asset_dim (asset_id, channel_country, channel_brand, selling_channel, color_num,
 rms_style_group_num, image_url, is_hero_image, image_index, shot_name, eff_begin_tmstp, eff_end_tmstp, dw_batch_id,
 dw_batch_date, dw_sys_load_tmstp)
(SELECT asset_id,
  channel_country,
  channel_brand,
  selling_channel,
  color_num,
  rms_style_group_num,
  image_url,
  is_hero_image,
  image_index,
  shot_name,
  eff_begin_tmstp,
  eff_end_tmstp,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT_SELLABILITY')) AS dw_batch_id,
   (SELECT curr_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT_SELLABILITY')) AS dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', current_datetime('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_image_asset_dim_vtw);
COMMIT TRANSACTION;

