
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_right_store_dim_vtw;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_right_store_dim_vtw 
(rms_sku_num,
 channel_country,
 channel_brand,
 selling_channel,
 store_num,
 selling_rights_start_tmstp,
 selling_rights_start_tmstp_tz,
 is_sellable_ind,
 eff_begin_tmstp,
 eff_begin_tmstp_tz,
 eff_end_tmstp,
 eff_end_tmstp_tz)



WITH SRC AS
   (SELECT DISTINCT   --normalize
      rms_sku_num,
      channel_country,
      channel_brand,
      selling_channel,
      selling_rights_start_tmstp,
      selling_rights_start_tmstp_tz,
      is_sellable_ind,
      store_num,
      eff_begin_tmstp,
      eff_begin_tmstp_tz,
      eff_end_tmstp,
      eff_end_tmstp_tz,
      RANGE( eff_begin_tmstp, eff_end_tmstp ) AS eff_period
     FROM 
     
      (
    --inner normalize
            SELECT rms_sku_num,channel_country,channel_brand,selling_channel,selling_rights_start_tmstp,selling_rights_start_tmstp_tz,is_sellable_ind,store_num,
            MIN(eff_begin_tmstp) AS eff_begin_tmstp,MIN(eff_begin_tmstp_tz) AS eff_begin_tmstp_tz, MAX(eff_end_tmstp) AS eff_end_tmstp,
            MAX(eff_end_tmstp_tz) AS eff_end_tmstp_tz
            FROM      
     (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num,channel_country,channel_brand,selling_channel,selling_rights_start_tmstp,is_sellable_ind,store_num ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM
      ( SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY rms_sku_num,channel_country,channel_brand,selling_channel,selling_rights_start_tmstp,is_sellable_ind,store_num ORDER BY eff_begin_tmstp) >= DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
            FROM    
     
     (SELECT 
        SRC1.rms_sku_num,
        SRC1.channel_country,
        SRC1.channel_brand,
        SRC1.selling_channel,
        SRC1.selling_rights_start_tmstp,
        SRC1.selling_rights_start_tmstp_tz,
        SRC1.is_sellable_ind,
        SRC1.store_num, 
        SRC1.eff_begin_tmstp, 
        SRC1.eff_begin_tmstp_tz,
        COALESCE(MIN(SRC1.eff_begin_tmstp) OVER(PARTITION BY SRC1.rms_sku_num, SRC1.channel_country, SRC1.channel_brand, SRC1.selling_channel, SRC1.store_num   ORDER BY SRC1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING),TIMESTAMP '9999-12-31 23:59:59.999999+00:00') as eff_end_tmstp,
        `{{params.gcp_project_id}}.jwn_udf.UDF_TIME_ZONE`(cast(COALESCE(MIN(SRC1.eff_begin_tmstp) OVER(PARTITION BY SRC1.rms_sku_num, SRC1.channel_country, SRC1.channel_brand, SRC1.selling_channel, SRC1.store_num   ORDER BY SRC1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING),TIMESTAMP '9999-12-31 23:59:59.999999+00:00')
         as string)) as eff_end_tmstp_tz
       FROM        
       (SELECT DISTINCT rmsskunum AS rms_sku_num,
             sellingcountry AS channel_country,
             sellingbrand AS channel_brand,
             sellingchannel AS selling_channel,
             issellable AS is_sellable_ind,
             storenum AS store_num,
             CAST(startTime AS TIMESTAMP) AS selling_rights_start_tmstp,
             `{{params.gcp_project_id}}.jwn_udf.UDF_TIME_ZONE`(startTime) AS selling_rights_start_tmstp_tz,
             CAST(startTime AS TIMESTAMP) AS eff_begin_tmstp,
             `{{params.gcp_project_id}}.jwn_udf.UDF_TIME_ZONE`(startTime) as eff_begin_tmstp_tz,
            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_ldg 
            WHERE CAST(startTime AS TIMESTAMP) IS NOT NULL
              AND storenum IS NOT NULL) AS SRC1 
              ) SRC2


      WHERE eff_begin_tmstp < eff_end_tmstp
      ) AS ordered_data
     )  AS grouped_data
          GROUP BY rms_sku_num,channel_country,channel_brand,selling_channel,selling_rights_start_tmstp,selling_rights_start_tmstp_tz,is_sellable_ind,store_num,range_group
ORDER BY rms_sku_num,channel_country,channel_brand,selling_channel,selling_rights_start_tmstp,is_sellable_ind,store_num, eff_begin_tmstp
 
   )) --AS SRC



SELECT 
  rms_sku_num,
  channel_country,
  channel_brand,
  selling_channel,
  store_num,
  selling_rights_start_tmstp,
  `{{params.gcp_project_id}}.jwn_udf.UDF_TIME_ZONE`(cast(selling_rights_start_tmstp as string)) as selling_rights_start_tmstp_tz,
  is_sellable_ind,
  MIN(RANGE_START(eff_period)) AS eff_begin,
  `{{params.gcp_project_id}}.jwn_udf.UDF_TIME_ZONE` (CAST(MIN(RANGE_START(eff_period))AS STRING)) AS eff_begin_tmstp_tz,
  MAX(RANGE_END(eff_period)) AS eff_end,
  `{{params.gcp_project_id}}.jwn_udf.UDF_TIME_ZONE` (CAST(MAX(RANGE_END(eff_period))AS STRING)) AS eff_end_tmstp_tz,
 FROM 
(
  --NONSEQUENCED VALIDTIME  --NORMALIZE
SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num,channel_country,channel_brand,selling_channel,store_num,selling_rights_start_tmstp,selling_rights_start_tmstp_tz,is_sellable_ind
				ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM
(

SELECT 
rms_sku_num,
channel_country,
channel_brand,
selling_channel,
store_num,
selling_rights_start_tmstp,
selling_rights_start_tmstp_tz,
is_sellable_ind,
eff_begin_tmstp,
eff_end_tmstp,
eff_period,
CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY  rms_sku_num,channel_country,channel_brand,selling_channel,store_num,selling_rights_start_tmstp,selling_rights_start_tmstp_tz,is_sellable_ind
 ORDER BY eff_begin_tmstp) >= DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
            FROM
 (SELECT   --normalize
    SRC.rms_sku_num,
    SRC.channel_country,
    SRC.channel_brand,
    SRC.selling_channel,
    SRC.store_num,
    SRC.selling_rights_start_tmstp,
    SRC.selling_rights_start_tmstp_tz,
    SRC.is_sellable_ind,
    TGT.eff_begin_tmstp_utc AS eff_begin_tmstp,
    TGT.eff_end_tmstp_utc AS eff_end_tmstp,
    COALESCE(RANGE_INTERSECT(SRC.eff_period , RANGE(TGT.eff_begin_tmstp_utc,TGT.eff_end_tmstp_utc)), SRC.eff_period ) AS eff_period,

   FROM SRC

       LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_item_selling_right_store_dim_hist AS tgt 
       ON LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) 
       AND LOWER(SRC.store_num) = LOWER(tgt.store_num)
       AND RANGE_OVERLAPS (RANGE(TGT.eff_begin_tmstp_utc, TGT.eff_end_tmstp_utc) , SRC.eff_period )
       WHERE ( TGT.store_num IS NULL 
       OR ((SRC.selling_rights_start_tmstp <> TGT.selling_rights_start_tmstp_utc     
       OR (SRC.selling_rights_start_tmstp IS NOT NULL AND TGT.selling_rights_start_tmstp_utc IS NULL) 
       OR (SRC.selling_rights_start_tmstp IS NULL AND TGT.selling_rights_start_tmstp_utc IS NOT NULL))))


 ))
 
)
 AS grouped_data
 GROUP BY rms_sku_num,channel_country,channel_brand,selling_channel,store_num,selling_rights_start_tmstp,is_sellable_ind,range_group
 ORDER BY rms_sku_num,channel_country,channel_brand,selling_channel,store_num,selling_rights_start_tmstp
;



begin;
DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_right_store_dim AS tgt
WHERE EXISTS (SELECT *
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_right_store_dim_vtw AS src
 WHERE LOWER(rms_sku_num) = LOWER(tgt.rms_sku_num)
  AND LOWER(channel_country) = LOWER(tgt.channel_country)
  AND LOWER(channel_brand) = LOWER(tgt.channel_brand)
  AND LOWER(selling_channel) = LOWER(tgt.selling_channel)
  AND LOWER(store_num) = LOWER(tgt.store_num));


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_right_store_dim (rms_sku_num, channel_country, channel_brand,
 selling_channel, store_num, selling_rights_start_tmstp,selling_rights_start_tmstp_tz, is_sellable_ind, eff_begin_tmstp,eff_begin_tmstp_tz, eff_end_tmstp,eff_end_tmstp_tz, dw_batch_id,
 dw_batch_date, dw_sys_load_tmstp)
(SELECT DISTINCT rms_sku_num,
  channel_country,
  channel_brand,
  selling_channel,
  store_num,
  selling_rights_start_tmstp,
  selling_rights_start_tmstp_tz,
  is_sellable_ind,
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
    store_num,
    selling_rights_start_tmstp,
    selling_rights_start_tmstp_tz,
    is_sellable_ind,
    eff_begin_tmstp,
    eff_begin_tmstp_tz,
    eff_end_tmstp,
    eff_end_tmstp_tz,
    CAST(RPAD(FORMAT_TIMESTAMP('%Y%m%d%I%M%S', CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)), 14, ' ') AS BIGINT)
    AS dw_batch_id,
    CURRENT_DATE('PST8PDT') AS dw_batch_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_right_store_dim_vtw) AS t
 WHERE NOT EXISTS (SELECT 1 AS `A12180`
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_right_store_dim
  WHERE LOWER(rms_sku_num) = LOWER(t.rms_sku_num)
    AND LOWER(channel_country) = LOWER(t.channel_country)
    AND LOWER(channel_brand) = LOWER(t.channel_brand)
    AND LOWER(selling_channel) = LOWER(t.selling_channel)
    AND LOWER(store_num) = LOWER(t.store_num))
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, channel_country, channel_brand, selling_channel, store_num)) = 1
 );

commit transaction;