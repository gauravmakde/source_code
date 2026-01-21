
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_right_sellable_audience_dim_vtw;
--.IF ERRORCODE <> 0 THEN .QUIT 1

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_right_sellable_audience_dim_vtw
(
  rms_sku_num
  , channel_country
  , channel_brand
  , selling_channel
  , selling_rights_start_tmstp
   ,selling_rights_start_tmstp_tz
  , sellable_audience
  , eff_begin_tmstp
  ,eff_begin_tmstp_tz
  , eff_end_tmstp
  ,eff_end_tmstp_tz
  
)



WITH SRC AS
  (
    SELECT DISTINCT  --NORMALIZE
        rms_sku_num
      , channel_country
      , channel_brand
      , selling_channel
      , selling_rights_start_tmstp
      , selling_rights_start_tmstp_tz
      , sellable_audience
      ,eff_begin_tmstp
      ,eff_end_tmstp
      ,range( eff_begin_tmstp, eff_end_tmstp ) AS eff_period     
      ,eff_begin_tmstp_tz,
       eff_end_tmstp_tz
       FROM
       (
    --inner normalize
            SELECT rms_sku_num,channel_country,channel_brand,selling_channel,selling_rights_start_tmstp,selling_rights_start_tmstp_tz,sellable_audience,MIN(eff_begin_tmstp_tz) AS eff_begin_tmstp_tz,MAX(eff_end_tmstp_tz) AS eff_end_tmstp_tz, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
            FROM     
          (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num,channel_country,channel_brand,selling_channel,selling_rights_start_tmstp,selling_rights_start_tmstp_tz,sellable_audience ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM

           ( SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY rms_sku_num,channel_country,channel_brand,selling_channel,selling_rights_start_tmstp,selling_rights_start_tmstp_tz,sellable_audience ORDER BY eff_begin_tmstp) >= 
                DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
    FROM (
      SELECT
        SRC1.rms_sku_num
      , SRC1.channel_country
      , SRC1.channel_brand
      , SRC1.selling_channel
      , SRC1.selling_rights_start_tmstp
      , SRC1.selling_rights_start_tmstp_tz
      , SRC1.sellable_audience
      , SRC1.eff_begin_tmstp      
      , SRC1.eff_begin_tmstp_tz
      ,COALESCE(MIN(SRC1.eff_begin_tmstp) OVER(PARTITION BY SRC1.rms_sku_num, SRC1.channel_country, SRC1.channel_brand, SRC1.   selling_channel, SRC1.sellable_audience
            ORDER BY SRC1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
           ,cast('9999-12-31 23:59:59.999999+00:00' as TIMESTAMP)) as eff_end_tmstp,

           `{{params.gcp_project_id}}`.jwn_udf.UDF_TIME_ZONE(cast(COALESCE(MIN(SRC1.eff_begin_tmstp) OVER(PARTITION BY SRC1.rms_sku_num, SRC1.channel_country, SRC1.channel_brand, SRC1.   selling_channel, SRC1.sellable_audience
            ORDER BY SRC1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
           ,cast('9999-12-31 23:59:59.999999+00:00' as TIMESTAMP)) as string)) as eff_end_tmstp_tz


      FROM (
        SELECT DISTINCT rmsskunum as rms_sku_num
          , sellingCountry AS channel_country
          , sellingBrand AS channel_brand
          , sellingChannel AS selling_channel
          , sellableAudience AS sellable_audience
          ,CAST(startTime AS TIMESTAMP) AS selling_rights_start_tmstp
          ,`{{params.gcp_project_id}}`.jwn_udf.UDF_TIME_ZONE(`{{params.gcp_project_id}}`.jwn_udf.ISO8601_TMSTP(startTime)) as selling_rights_start_tmstp_tz
         ,CAST(startTime AS TIMESTAMP) AS eff_begin_tmstp        
         ,`{{params.gcp_project_id}}`.jwn_udf.UDF_TIME_ZONE(`{{params.gcp_project_id}}`.jwn_udf.ISO8601_TMSTP(startTime)) as eff_begin_tmstp_tz,
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_STG.PRODUCT_ITEM_SELLING_RIGHTS_LDG
        WHERE startTime IS NOT NULL
          AND sellableAudience IS NOT NULL
      ) SRC1
    ) SRC2

	  WHERE eff_begin_tmstp < eff_end_tmstp

           ) AS ordered_data
          ) AS grouped_data
          GROUP BY rms_sku_num,channel_country,channel_brand,selling_channel,selling_rights_start_tmstp,selling_rights_start_tmstp_tz,sellable_audience, range_group
ORDER BY  rms_sku_num,channel_country,channel_brand,selling_channel,selling_rights_start_tmstp,selling_rights_start_tmstp_tz,sellable_audience, eff_begin_tmstp
  ))




SELECT
    NRML.rms_sku_num
  , NRML.channel_country
  , NRML.channel_brand
  , NRML.selling_channel
  , NRML.selling_rights_start_tmstp
  , selling_rights_start_tmstp_tz
  , NRML.sellable_audience
  , MIN(RANGE_START(NRML.eff_period)) AS eff_begin
  , `{{params.gcp_project_id}}`.jwn_udf.UDF_TIME_ZONE (CAST(MIN(RANGE_START(NRML.eff_period))AS STRING)) AS eff_begin_tmstp_tz
  , MAX(RANGE_END(NRML.eff_period))   AS eff_end
  , `{{params.gcp_project_id}}`.jwn_udf.UDF_TIME_ZONE (CAST(MAX(RANGE_END(NRML.eff_period))AS STRING)) AS eff_end_tmstp_tz
FROM (
  --NONSEQUENCED VALIDTIME  --NORMALIZE
SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num,channel_country,channel_brand,selling_channel,selling_rights_start_tmstp,selling_rights_start_tmstp_tz,sellable_audience
				ORDER BY eff_begin_tmstp_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM

(

SELECT 
 rms_sku_num,
 channel_country
,channel_brand
,selling_channel
,selling_rights_start_tmstp
,selling_rights_start_tmstp_tz
,sellable_audience,
eff_begin_tmstp_utc,
eff_end_tmstp_utc,
eff_period,
CASE 
                WHEN LAG(eff_end_tmstp_utc) OVER (PARTITION BY  rms_sku_num,channel_country,channel_brand,selling_channel,selling_rights_start_tmstp,selling_rights_start_tmstp_tz,sellable_audience
 ORDER BY eff_begin_tmstp_utc) >= 
                DATE_SUB(eff_begin_tmstp_utc, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
            FROM
(
  SELECT Distinct
    SRC.rms_sku_num
  , SRC.channel_country
  , SRC.channel_brand
  , SRC.selling_channel
  , SRC.selling_rights_start_tmstp
  , SRC.selling_rights_start_tmstp_tz
  , SRC.sellable_audience
  , TGT.eff_begin_tmstp_utc
  , TGT.eff_end_tmstp_utc
   , COALESCE(RANGE_INTERSECT(SRC.eff_period,RANGE(TGT.eff_begin_tmstp_utc ,TGT.eff_end_tmstp_utc)), SRC.eff_period ) AS eff_period  
  FROM SRC
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_item_selling_right_sellable_audience_dim_hist TGT
  ON SRC.rms_sku_num = TGT.rms_sku_num
  AND SRC.sellable_audience = TGT.sellable_audience
  AND RANGE_OVERLAPS(SRC.eff_period,RANGE(TGT.eff_begin_tmstp_utc , TGT.eff_end_tmstp_utc))
  WHERE ( TGT.sellable_audience IS NULL OR
     (  (SRC.selling_rights_start_tmstp <> TGT.selling_rights_start_tmstp_utc OR (SRC.selling_rights_start_tmstp IS NOT NULL AND TGT.selling_rights_start_tmstp_utc IS NULL) OR (SRC.selling_rights_start_tmstp IS NULL AND TGT.selling_rights_start_tmstp_utc IS NOT NULL))
     )  )
) AS ordered_data
 
)AS grouped_data
) NRML
GROUP BY rms_sku_num,channel_country,channel_brand,selling_channel,selling_rights_start_tmstp,selling_rights_start_tmstp_tz,sellable_audience,range_group
ORDER BY  rms_sku_num,channel_country,channel_brand,selling_channel,selling_rights_start_tmstp,selling_rights_start_tmstp_tz,sellable_audience
;





BEGIN TRANSACTION;

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_right_sellable_audience_dim AS tgt
WHERE EXISTS (SELECT *
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_right_sellable_audience_dim_vtw AS src
 WHERE LOWER(rms_sku_num) = LOWER(tgt.rms_sku_num)
  AND LOWER(channel_country) = LOWER(tgt.channel_country)
  AND LOWER(channel_brand) = LOWER(tgt.channel_brand)
  AND LOWER(selling_channel) = LOWER(tgt.selling_channel)
  AND LOWER(sellable_audience) = LOWER(tgt.sellable_audience));



INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_DIM.PRODUCT_ITEM_SELLING_RIGHT_SELLABLE_AUDIENCE_DIM
(
  rms_sku_num
  , channel_country
  , channel_brand
  , selling_channel
  , selling_rights_start_tmstp
  ,selling_rights_start_tmstp_tz
  , sellable_audience
  , eff_begin_tmstp
  ,eff_begin_tmstp_tz
  , eff_end_tmstp
  ,eff_end_tmstp_tz
  , dw_batch_id
  , dw_batch_date
  , dw_sys_load_tmstp
  
)
SELECT rms_sku_num,
 channel_country,
 channel_brand,
 selling_channel,
 selling_rights_start_tmstp,
 selling_rights_start_tmstp_tz,
 sellable_audience,
 eff_begin_tmstp,
 eff_begin_tmstp_tz,
 eff_end_tmstp,
 eff_end_tmstp_tz,
 CAST(RPAD(FORMAT_TIMESTAMP('%Y%m%d%I%M%S', CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME)), 14, ' ') AS BIGINT)
 AS dw_batch_id,
 CURRENT_DATE('PST8PDT') AS dw_batch_date,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', current_datetime('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_right_sellable_audience_dim_vtw;


COMMIT TRANSACTION ;

