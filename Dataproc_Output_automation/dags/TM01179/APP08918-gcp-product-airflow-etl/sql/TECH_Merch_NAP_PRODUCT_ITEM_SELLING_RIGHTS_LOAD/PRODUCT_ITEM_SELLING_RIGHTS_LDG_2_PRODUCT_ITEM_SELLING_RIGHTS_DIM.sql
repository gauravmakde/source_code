--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=stg-dim;AppSubArea=ITEM_SELLING_RIGHTS;' UPDATE FOR SESSION;

-- NONSEQUENCED VALIDTIME --Purge work table for staging temporal rows
TRUNCATE TABLE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_dim_vtw;
--.IF ERRORCODE <> 0 THEN .QUIT 1

--Purge work table for staging records to be deleted
TRUNCATE TABLE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_del_stg;
--.IF ERRORCODE <> 0 THEN .QUIT 2

--Identify records for future clean up
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_del_stg 
(rms_sku_num, 
channel_country, 
channel_brand,
selling_channel, 
eff_begin_tmstp,
eff_begin_tmstp_tz)
(SELECT rights_dim.rms_sku_num,
  rights_dim.channel_country,
  rights_dim.channel_brand,
  rights_dim.selling_channel,
  rights_dim.eff_begin_tmstp,
  rights_dim.eff_begin_tmstp_tz
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_dim AS rights_dim
 INNER JOIN (SELECT DISTINCT opr.rms_sku_num,
             opr.channel_country,
             opr.channel_brand,
             opr.selling_channel,
             opr.selling_rights_start_tmstp AS min_begin_tmstp
            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.product_item_selling_rights_opr AS opr
            INNER JOIN (SELECT DISTINCT rmsskunum AS rms_sku_num,
                         sellingcountry,
                         sellingchannel,
                         sellingbrand,
                         `{{params.gcp_project_id}}.jwn_udf.ISO8601_TMSTP`(lastupdatedtimestamp) AS src_event_tmstp,
                         `{{params.gcp_project_id}}.jwn_udf.ISO8601_TMSTP`(starttime) AS selling_rights_start_tmstp
                        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_item_selling_rights_ldg
                        QUALIFY (ROW_NUMBER() OVER (PARTITION BY rmsskunum, sellingcountry, sellingbrand, sellingchannel ORDER BY lastupdatedtimestamp, selling_rights_start_tmstp)) = 1) AS oldest_ldg 
            ON LOWER(oldest_ldg.rms_sku_num) = LOWER(opr.rms_sku_num) 
            AND LOWER(oldest_ldg.sellingcountry) = LOWER(opr.channel_country) 
            AND LOWER(oldest_ldg.sellingbrand) = LOWER(opr.channel_brand) 
            AND LOWER(oldest_ldg.sellingchannel) = LOWER(opr.selling_channel) 
            AND CAST(oldest_ldg.selling_rights_start_tmstp AS TIMESTAMP) = opr.selling_rights_start_tmstp) AS STG      
 ON LOWER(rights_dim.rms_sku_num) = LOWER(STG.rms_sku_num) 
 AND LOWER(rights_dim.channel_country) = LOWER(STG.channel_country) 
 AND LOWER(rights_dim.channel_brand) = LOWER(STG.channel_brand)
 AND LOWER(rights_dim.selling_channel) = LOWER(STG.selling_channel) 
 AND rights_dim.eff_begin_tmstp >= STG.min_begin_tmstp);

--.IF ERRORCODE <> 0 THEN .QUIT 3

-- Collect stats on the delete table


--Explicit transaction on Teradata ensures that we do not corrupt target table if failure in middle of updating target table
BEGIN TRANSACTION;
--.IF ERRORCODE <> 0 THEN .QUIT 4

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_dim AS tgt
WHERE EXISTS (SELECT *
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_del_stg AS src
 WHERE LOWER(rms_sku_num) = LOWER(tgt.rms_sku_num)
  AND LOWER(channel_country) = LOWER(tgt.channel_country)
  AND LOWER(channel_brand) = LOWER(tgt.channel_brand)
  AND LOWER(selling_channel) = LOWER(tgt.selling_channel)
  AND eff_begin_tmstp = tgt.eff_begin_tmstp);

--.IF ERRORCODE <> 0 THEN .QUIT 5

-- Set the end timestamp to default for all the skus that had future cleanup
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_dim AS target 
SET eff_end_tmstp = CAST(CAST('9999-12-31 23:59:59.999999+00:' AS DATETIME) AS TIMESTAMP),
eff_end_tmstp_tz = `{{params.gcp_project_id}}.jwn_udf.UDF_TIME_ZONE`(CAST(CAST(CAST('9999-12-31 23:59:59.999999+00:' AS DATETIME) AS TIMESTAMP) AS STRING))
FROM (SELECT dim.rms_sku_num,
   dim.channel_country,
   dim.channel_brand,
   dim.selling_channel,
   MAX(dim.eff_begin_tmstp) AS max_eff_begin_tmstp,
   MAX(dim.eff_end_tmstp) AS max_eff_end_tmstp
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_dim AS dim
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_del_stg AS pdel 
  ON LOWER(dim.rms_sku_num) = LOWER(pdel.rms_sku_num) 
  AND LOWER(dim.channel_country) = LOWER(pdel.channel_country) 
  AND LOWER(dim.channel_brand) = LOWER(pdel.channel_brand) 
  AND LOWER(dim.selling_channel) = LOWER(pdel.selling_channel)
  GROUP BY dim.rms_sku_num,
   dim.channel_country,
   dim.channel_brand,
   dim.selling_channel) AS SOURCE
WHERE LOWER(target.rms_sku_num) = LOWER(SOURCE.rms_sku_num) 
AND LOWER(target.channel_brand) = LOWER(SOURCE.channel_brand) 
AND LOWER(target.channel_country) = LOWER(SOURCE.channel_country) 
AND LOWER(target.selling_channel) = LOWER(SOURCE.selling_channel) 
AND target.eff_begin_tmstp = SOURCE.max_eff_begin_tmstp
AND target.eff_end_tmstp = SOURCE.max_eff_end_tmstp;

--.IF ERRORCODE <> 0 THEN .QUIT 6








INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_dim_vtw
(   rms_sku_num
  , channel_country
  , channel_brand
  , selling_channel
  , is_sellable_ind
  , selling_status_code
  , selling_rights_start_tmstp
  , selling_rights_start_tmstp_tz
  , selling_status_start_tmstp
  , selling_status_start_tmstp_tz
  , eff_begin_tmstp
  , eff_begin_tmstp_tz
  , eff_end_tmstp
  , eff_end_tmstp_tz
)


WITH SRC1 AS 
(select 
rms_sku_num, channel_country, channel_brand,selling_channel,is_sellable_ind,selling_status_code,selling_rights_start_tmstp,selling_status_start_tmstp,
eff_begin_tmstp,
eff_end_tmstp
 from 
(
    --inner normalize
            SELECT rms_sku_num, channel_country, channel_brand,selling_channel,is_sellable_ind,selling_status_code,selling_rights_start_tmstp,selling_status_start_tmstp, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
            FROM
(
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num, channel_country, channel_brand,selling_channel,is_sellable_ind,selling_status_code,selling_rights_start_tmstp,selling_status_start_tmstp ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM

(
        SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY rms_sku_num, channel_country, channel_brand,selling_channel,is_sellable_ind,
                selling_status_code,selling_rights_start_tmstp,selling_status_start_tmstp ORDER BY eff_begin_tmstp) >= 
                DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
         from
             (SELECT DISTINCT  --NORMALIZE
                   rights_opr.rms_sku_num,
                   rights_opr.channel_country,
                   rights_opr.channel_brand,
                   rights_opr.selling_channel,
                   rights_opr.is_sellable_ind,
                   CASE WHEN status_dim.rms_sku_num IS NOT NULL THEN status_dim.selling_status_code ELSE 'UNBLOCKED' END AS selling_status_code,
                   rights_opr.selling_rights_start_tmstp,
                   cast(CASE WHEN status_dim.rms_sku_num IS NOT NULL THEN status_dim.eff_begin_tmstp ELSE NULL END as timestamp) AS selling_status_start_tmstp,
                   rights_opr.eff_begin_tmstp,
                   rights_opr.eff_end_tmstp
                  
                   FROM (SELECT opr.rms_sku_num, opr.channel_country, opr.channel_brand, opr.selling_channel, opr.selling_rights_start_tmstp, opr.store_num, 
                         opr.sellable_audience, opr.is_sellable_ind, opr.selling_rights_cancelled_ind, opr.selling_rights_cancelled_desc, opr.selling_rights_cancelled_event_tmstp, 
                         opr.src_event_tmstp, opr.dw_batch_id, opr.dw_batch_date, opr.dw_sys_load_date, opr.dw_sys_load_tmstp, opr.dw_sys_updt_tmstp,

                         opr.selling_rights_start_tmstp AS eff_begin_tmstp,
                         COALESCE(MIN(CAST(oldest_ldg.selling_rights_start_tmstp AS TIMESTAMP)) OVER(PARTITION BY rms_sku_num, channel_country, channel_brand, selling_channel ORDER BY oldest_ldg.selling_rights_start_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) ,TIMESTAMP'9999-12-31 23:59:59.999999+00:00') AS eff_end_tmstp



                         FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.product_item_selling_rights_opr AS opr 
                         INNER JOIN (SELECT DISTINCT rmsskunum,
                                     sellingcountry,
                                     sellingchannel,
                                     sellingbrand,  
                                     `{{params.gcp_project_id}}.jwn_udf.ISO8601_TMSTP`(lastupdatedtimestamp) as src_event_tmstp,
                                     `{{params.gcp_project_id}}.jwn_udf.ISO8601_TMSTP`(starttime) as selling_rights_start_tmstp
                                     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_item_selling_rights_ldg
                                     QUALIFY (ROW_NUMBER() OVER (PARTITION BY rmsskunum, sellingcountry, sellingbrand, sellingchannel order by selling_rights_start_tmstp asc )) = 1) AS oldest_ldg 
				                 ON LOWER(opr.rms_sku_num) = LOWER(oldest_ldg.rmsskunum) 
	                       AND LOWER(opr.channel_country) = LOWER(oldest_ldg.sellingcountry) 
	                       AND LOWER(opr.channel_brand) = LOWER(oldest_ldg.sellingbrand) 
	                       AND LOWER(opr.selling_channel) = LOWER(oldest_ldg.sellingchannel) 
	                       AND cast(opr.selling_rights_start_tmstp as string) >= oldest_ldg.selling_rights_start_tmstp
	                       AND LOWER(opr.selling_rights_cancelled_ind) <> LOWER('Y')) AS rights_opr
                   LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_item_selling_status_dim AS status_dim 
                   ON LOWER(rights_opr.rms_sku_num) = LOWER(status_dim.rms_sku_num)  
                   AND LOWER(rights_opr.channel_country) = LOWER(status_dim.channel_country)  
                   AND LOWER(rights_opr.channel_brand) = LOWER(status_dim.channel_brand)  
                   AND LOWER(rights_opr.selling_channel) = LOWER(status_dim.selling_channel)
                   AND RANGE_CONTAINS(RANGE(status_dim.eff_begin_tmstp_utc,status_dim.eff_end_tmstp_utc) ,(rights_opr.selling_rights_start_tmstp))   
                   ) --AS SRC1
) AS ordered_data
) AS grouped_data

GROUP BY rms_sku_num, channel_country, channel_brand,selling_channel,is_sellable_ind,selling_status_code,selling_rights_start_tmstp,selling_status_start_tmstp,range_group
ORDER BY rms_sku_num, channel_country, channel_brand,selling_channel,is_sellable_ind,selling_status_code,selling_rights_start_tmstp,selling_status_start_tmstp,eff_begin_tmstp))



SELECT 
    rms_sku_num
  , channel_country
  , channel_brand
  , selling_channel
  , is_sellable_ind
  , selling_status_code
  , selling_rights_start_tmstp
 , `{{params.gcp_project_id}}.jwn_udf.UDF_TIME_ZONE`(cast(selling_rights_start_tmstp as string)) as selling_rights_start_tmstp_tz
  , selling_status_start_tmstp
  , `{{params.gcp_project_id}}.jwn_udf.UDF_TIME_ZONE`(cast(selling_status_start_tmstp as string)) as selling_status_start_tmstp_tz
  , MIN(RANGE_START(eff_period)) AS eff_begin_tmstp
  , `{{params.gcp_project_id}}.jwn_udf.UDF_TIME_ZONE`(cast(MIN(RANGE_START(eff_period)) as string)) AS eff_begin_tmstp_tz
  , MAX(RANGE_END(eff_period)) AS eff_end_tmstp
  ,`{{params.gcp_project_id}}.jwn_udf.UDF_TIME_ZONE`(cast(MAX(RANGE_END(eff_period)) as string)) AS eff_end_tmstp_tz
FROM
(
SELECT rms_sku_num,channel_country,channel_brand,selling_channel,is_sellable_ind,selling_status_code,selling_rights_start_tmstp,selling_status_start_tmstp,eff_period,range_group

FROM(
  SELECT *,
  SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num,channel_country,channel_brand,selling_channel,is_sellable_ind,selling_status_code,selling_rights_start_tmstp,selling_status_start_tmstp
				ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
FROM 

(SELECT DISTINCT  --NORMALIZE
       SRC.rms_sku_num
      ,SRC.channel_country
      ,SRC.channel_brand
      ,SRC.selling_channel
      ,SRC.is_sellable_ind
      ,SRC.selling_status_code
      ,SRC.selling_rights_start_tmstp
      ,SRC.selling_status_start_tmstp
      ,TGT.eff_begin_tmstp
      ,TGT.eff_end_tmstp
      ,CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY SRC.rms_sku_num,SRC.channel_country,SRC.channel_brand,SRC.selling_channel,SRC.is_sellable_ind,SRC.selling_status_code,SRC.selling_rights_start_tmstp,SRC.selling_status_start_tmstp 
                ORDER BY eff_begin_tmstp) >= DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
      ,COALESCE( RANGE_INTERSECT(SRC.eff_period , RANGE (TGT.eff_begin_tmstp_utc,TGT.eff_end_tmstp_utc)) , SRC.eff_period ) AS eff_period
      FROM      
      (SELECT DISTINCT 
       rms_sku_num,
       channel_country,
       channel_brand,
       selling_channel,
       is_sellable_ind,
       selling_status_code,
       selling_rights_start_tmstp,
       selling_status_start_tmstp,
       range( eff_begin_tmstp, eff_end_tmstp ) AS eff_period
       FROM 
       
       (SELECT rms_sku_num,
             channel_country,
             channel_brand,
             selling_channel,
             is_sellable_ind,
             selling_status_code,
             selling_rights_start_tmstp,
             selling_status_start_tmstp,
             eff_begin_tmstp,
             eff_end_tmstp
	           
            --  selling_rights_start_tmstp AS eff_begin_tmstp,
	          --  COALESCE(MIN(SRC1.selling_rights_start_tmstp) OVER(PARTITION BY SRC1.rms_sku_num, SRC1.channel_country, SRC1.channel_brand, SRC1.selling_channel ORDER BY SRC1.selling_rights_start_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) ,TIMESTAMP'9999-12-31 23:59:59.999999+00:00') AS eff_end_tmstp
             FROM  SRC1
                ) SRC2
	     WHERE eff_begin_tmstp < eff_end_tmstp) AS SRC
      LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_item_selling_rights_dim AS tgt 
      ON LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num)
      AND LOWER(SRC.channel_country) = LOWER(tgt.channel_country) 
      AND LOWER(SRC.channel_brand) = LOWER(tgt.channel_brand)
      AND LOWER(SRC.selling_channel) = LOWER(tgt.selling_channel)
      AND RANGE_OVERLAPS(RANGE(TGT.eff_begin_tmstp_utc,TGT.eff_end_tmstp_utc) ,(SRC.eff_period))
      WHERE (tgt.rms_sku_num IS NULL
       OR (LOWER(SRC.is_sellable_ind) <> LOWER(tgt.is_sellable_ind)
       OR (tgt.is_sellable_ind IS NULL AND SRC.is_sellable_ind IS NOT NULL)
       OR (SRC.is_sellable_ind IS NULL AND tgt.is_sellable_ind IS NOT NULL))
       OR (LOWER(SRC.selling_status_code) <> LOWER(tgt.selling_status_code)
       OR (tgt.selling_status_code IS NULL AND SRC.selling_status_code IS NOT NULL)
       OR (SRC.selling_status_code IS NULL AND tgt.selling_status_code IS NOT NULL))
       OR (SRC.selling_rights_start_tmstp <> tgt.selling_rights_start_tmstp_utc
       OR (SRC.selling_rights_start_tmstp IS NOT NULL AND TGT.selling_rights_start_tmstp_utc IS NULL) 
       OR (SRC.selling_rights_start_tmstp IS NULL AND TGT.selling_rights_start_tmstp_utc IS NOT NULL)))
       ) AS ordered_data

) AS grouped_data
)
GROUP BY rms_sku_num,
channel_country,
channel_brand,
selling_channel,
is_sellable_ind,
selling_status_code,
selling_rights_start_tmstp,
selling_status_start_tmstp,range_group
ORDER BY  rms_sku_num,
channel_country,
channel_brand,
selling_channel,
is_sellable_ind,
selling_status_code,
selling_rights_start_tmstp,
selling_status_start_tmstp;


--.IF ERRORCODE <> 0 THEN .QUIT 7

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_dim AS tgt
WHERE EXISTS (SELECT *
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_dim_vtw AS src
 WHERE LOWER(rms_sku_num) = LOWER(tgt.rms_sku_num)
  AND LOWER(channel_country) = LOWER(tgt.channel_country)
  AND LOWER(channel_brand) = LOWER(tgt.channel_brand)
  AND LOWER(selling_channel) = LOWER(tgt.selling_channel));
--.IF ERRORCODE <> 0 THEN .QUIT 8

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_dim (rms_sku_num, channel_country, channel_brand, selling_channel,
 is_sellable_ind, selling_status_code, selling_rights_start_tmstp, selling_rights_start_tmstp_tz, selling_status_start_tmstp, selling_status_start_tmstp_tz, eff_begin_tmstp, eff_begin_tmstp_tz,
 eff_end_tmstp, eff_end_tmstp_tz, dw_batch_id, dw_batch_date, dw_sys_load_tmstp)
(SELECT DISTINCT rms_sku_num,
  channel_country,
  channel_brand,
  selling_channel,
  is_sellable_ind,
  selling_status_code,
  selling_rights_start_tmstp,
  selling_rights_start_tmstp_tz,
  selling_status_start_tmstp,
  selling_status_start_tmstp_tz,
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
    is_sellable_ind,
    selling_status_code,
    selling_rights_start_tmstp,
    selling_rights_start_tmstp_tz,
    selling_status_start_tmstp,
    selling_status_start_tmstp_tz,
    eff_begin_tmstp,
    eff_begin_tmstp_tz,
    eff_end_tmstp,
    eff_end_tmstp_tz,
    CAST(RPAD(FORMAT_TIMESTAMP('%Y%m%d%I%M%S', CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)), 14, ' ') AS BIGINT)
    AS dw_batch_id,
    CURRENT_DATE('PST8PDT') AS dw_batch_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_dim_vtw) AS t
 WHERE NOT EXISTS (SELECT 1 AS `A12180`
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_dim
   WHERE rms_sku_num = t.rms_sku_num
    AND channel_country = t.channel_country
    AND channel_brand = t.channel_brand
    AND selling_channel = t.selling_channel)
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, channel_country, channel_brand, selling_channel)) = 1);

--.IF ERRORCODE <> 0 THEN .QUIT 9

COMMIT TRANSACTION;
--.IF ERRORCODE <> 0 THEN .QUIT 10


