--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=stg-dim;AppSubArea=ITEM_SELLING_RIGHTS_STATUS;' UPDATE FOR SESSION;

-- NONSEQUENCED VALIDTIME --Purge work table for staging temporal rows

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_dim_vtw;

--.IF ERRORCODE <> 0 THEN .QUIT 1
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_dim_vtw (rms_sku_num, channel_country, channel_brand,
 selling_channel, is_sellable_ind, selling_status_code, selling_rights_start_tmstp ,selling_rights_start_tmstp_tz ,selling_status_start_tmstp, selling_status_start_tmstp_tz, eff_begin_tmstp , eff_begin_tmstp_tz, eff_end_tmstp,eff_end_tmstp_tz)




WITH src AS
   (
    SELECT distinct  --normalize
        rms_sku_num,
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
        range( eff_begin_tmstp, eff_end_tmstp ) AS eff_period

        FROM( SELECT rms_sku_num,channel_country,channel_brand,selling_channel,is_sellable_ind,selling_status_code,selling_rights_start_tmstp,selling_rights_start_tmstp_tz,selling_status_start_tmstp,selling_status_start_tmstp_tz,MIN(eff_begin_tmstp_tz) AS eff_begin_tmstp_tz,MAX(eff_end_tmstp_tz) AS eff_end_tmstp_tz, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp

         FROM     
          (
               
               
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num,channel_country,channel_brand,selling_channel,is_sellable_ind,selling_status_code,selling_rights_start_tmstp,selling_rights_start_tmstp_tz,selling_status_start_tmstp ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                
          FROM

           ( SELECT  *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY rms_sku_num,channel_country,channel_brand,selling_channel,is_sellable_ind,selling_status_code,selling_rights_start_tmstp,selling_rights_start_tmstp_tz,selling_status_start_tmstp
				ORDER BY eff_begin_tmstp) >= DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag ,
               
            `{{params.gcp_project_id}}`.jwn_udf.UDF_TIME_ZONE(cast(eff_begin_tmstp as string)) as eff_begin_tmstp_tz,  
       FROM (
        SELECT distinct 
        rms_sku_num,
        channel_country,
        channel_brand,
        selling_channel,
        is_sellable_ind,
        selling_status_code,
        selling_rights_start_tmstp,
        selling_rights_start_tmstp_tz,
        src_event_tmstp  as selling_status_start_tmstp,
        `{{params.gcp_project_id}}`.jwn_udf.UDF_TIME_ZONE(cast(src_event_tmstp as string)) as selling_status_start_tmstp_tz,
        CASE WHEN src_event_tmstp > eff_begin_tmstp THEN src_event_tmstp ELSE eff_begin_tmstp END AS eff_begin_tmstp,

       (COALESCE(MIN(selling_rights_start_tmstp) OVER(PARTITION BY rms_sku_num, channel_country, channel_brand, src1.selling_channel
            ORDER BY selling_rights_start_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
           ,TIMESTAMP'9999-12-31 23:59:59.999999+00:00') ) as  eff_end_tmstp,


           `{{params.gcp_project_id}}`.jwn_udf.UDF_TIME_ZONE(CAST(COALESCE(MIN(selling_rights_start_tmstp) OVER(PARTITION BY rms_sku_num, channel_country, channel_brand, src1.selling_channel
            ORDER BY selling_rights_start_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
           ,TIMESTAMP'9999-12-31 23:59:59.999999+00:00') AS STRING)) as eff_end_tmstp_tz

       FROM (SELECT DISTINCT right_dim.rms_sku_num,
          right_dim.channel_country,
          right_dim.channel_brand,
          right_dim.selling_channel,
          right_dim.is_sellable_ind,
          rights_status.selling_status_code,
          right_dim.selling_rights_start_tmstp_utc as selling_rights_start_tmstp,
          right_dim.selling_rights_start_tmstp_tz,
			    rights_status.src_event_tmstp AS src_event_tmstp,
          right_dim.eff_begin_tmstp_utc as eff_begin_tmstp
         
         FROM 
         (SELECT status_dim.rms_sku_num,
            status_dim.channel_country,
            status_dim.channel_brand,
            status_dim.selling_channel,
            status_dim.selling_status_code,
			      nord_udf.epoch_tmstp(cast(status_ldg.lastupdatedtimestamp as int64)) AS src_event_tmstp
           FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_item_selling_status_ldg AS status_ldg
            INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_item_selling_status_dim AS status_dim ON LOWER(status_dim.rms_sku_num) =
                LOWER(status_ldg.itemid) AND LOWER(status_dim.channel_country) = LOWER(status_ldg.sellingcountry) AND
               LOWER(status_dim.channel_brand) = LOWER(status_ldg.sellingbrand) AND LOWER(status_dim.selling_channel) =
              LOWER('ONLINE')
			 AND RANGE_CONTAINS(range(status_dim.eff_begin_tmstp_utc ,status_dim.eff_end_tmstp_utc) ,nord_udf.epoch_tmstp(cast(status_ldg.lastupdatedtimestamp as int64)))
           QUALIFY (ROW_NUMBER() OVER (PARTITION BY status_dim.rms_sku_num, status_dim.channel_country, status_dim.channel_brand
                , status_dim.selling_channel ORDER BY status_ldg.lastupdatedtimestamp DESC)) = 1) AS rights_status
          INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_item_selling_rights_dim AS right_dim ON LOWER(right_dim.rms_sku_num) =
               LOWER(rights_status.rms_sku_num) AND LOWER(right_dim.channel_country) = LOWER(rights_status.channel_country
                ) AND LOWER(right_dim.channel_brand) = LOWER(rights_status.channel_brand) AND LOWER(right_dim.selling_channel
              ) = LOWER(rights_status.selling_channel) 
              			AND right_dim.eff_end_tmstp_utc >= rights_status.src_event_tmstp
			
              AND LOWER(right_dim.selling_status_code) <> LOWER(rights_status.selling_status_code
      )) AS src1) 
      
      AS src2 
      WHERE eff_begin_tmstp < eff_end_tmstp
      )  AS ordered_data 
          )  AS grouped_data
          GROUP BY rms_sku_num,channel_country,channel_brand,selling_channel,is_sellable_ind,selling_status_code,selling_rights_start_tmstp,selling_rights_start_tmstp_tz,selling_status_start_tmstp,selling_status_start_tmstp_tz,range_group
          ORDER BY rms_sku_num,channel_country,channel_brand,selling_channel,is_sellable_ind,selling_status_code,selling_rights_start_tmstp,selling_rights_start_tmstp_tz,selling_status_start_tmstp
        )
      
   )



SELECT DISTINCT 
  rms_sku_num,
  channel_country,
  channel_brand,
  selling_channel,
  is_sellable_ind,
  selling_status_code,
  selling_rights_start_tmstp ,
  selling_rights_start_tmstp_tz ,
  selling_status_start_tmstp,
  `{{params.gcp_project_id}}`.jwn_udf.UDF_TIME_ZONE(cast(selling_status_start_tmstp as string)) as selling_status_start_tmstp_tz,
  MIN(RANGE_START(eff_period)) AS eff_begin,
  MIN(eff_begin_tmstp_tz) AS eff_begin_tmstp_tz,
  MAX(RANGE_END(eff_period))   AS eff_end,
  MAX(eff_end_tmstp_tz) AS eff_end_tmstp_tz
 FROM 
 
 (
  --NONSEQUENCED VALIDTIME  --NORMALIZE
SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num,channel_country,channel_brand,selling_channel,is_sellable_ind,selling_status_code,selling_rights_start_tmstp,selling_rights_start_tmstp_tz,selling_status_start_tmstp
				ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM
 (
 SELECT *,CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY  rms_sku_num,channel_country,channel_brand,selling_channel,is_sellable_ind,selling_status_code,selling_rights_start_tmstp,selling_rights_start_tmstp_tz,selling_status_start_tmstp
 ORDER BY eff_begin_tmstp) >= DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
            FROM
(

 
 SELECT   --normalize
    src.rms_sku_num,
    src.channel_country,
    src.channel_brand,
    src.selling_channel,
    src.is_sellable_ind,
    src.selling_status_code,
    src.selling_rights_start_tmstp,
    src.selling_rights_start_tmstp_tz,
    src.selling_status_start_tmstp,
    tgt.eff_begin_tmstp,
    tgt.eff_end_tmstp,
   COALESCE( RANGE_INTERSECT(src.eff_period , range(tgt.eff_begin_tmstp_utc,tgt.eff_end_tmstp_utc))
            , src.eff_period ) AS eff_period,
            tgt.eff_begin_tmstp_tz,
            tgt.eff_end_tmstp_tz
   FROM src

  
      LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_item_selling_rights_dim AS tgt ON LOWER(src.rms_sku_num) = LOWER(tgt.rms_sku_num
           ) AND LOWER(src.channel_country) = LOWER(tgt.channel_country) AND LOWER(src.channel_brand) = LOWER(tgt.channel_brand
          ) AND LOWER(src.selling_channel) = LOWER(tgt.selling_channel)
            AND RANGE_OVERLAPS(eff_period , range(tgt.eff_begin_tmstp_utc, tgt.eff_end_tmstp_utc))

     WHERE tgt.rms_sku_num IS NULL
      OR LOWER(src.selling_status_code) <> LOWER(tgt.selling_status_code)
      OR tgt.selling_status_code IS NULL AND src.selling_status_code IS NOT NULL
      OR src.selling_status_code IS NULL AND tgt.selling_status_code IS NOT NULL) --AS t6
   AS ordered_data

 ) AS grouped_data

WHERE NOT EXISTS (SELECT 1 AS `A12180`
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_dim_vtw
     WHERE rms_sku_num = grouped_data.rms_sku_num
      AND channel_country = grouped_data.channel_country
      AND channel_brand = grouped_data.channel_brand
      AND selling_channel = grouped_data.selling_channel) 
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, channel_country, channel_brand, selling_channel)) = 1

 ) 

GROUP BY rms_sku_num,channel_country,channel_brand,selling_channel,is_sellable_ind,selling_status_code,selling_rights_start_tmstp,selling_rights_start_tmstp_tz,selling_status_start_tmstp,range_group
ORDER BY  rms_sku_num,channel_country,channel_brand,selling_channel,is_sellable_ind,selling_status_code,selling_rights_start_tmstp,selling_rights_start_tmstp_tz,selling_status_start_tmstp,eff_begin

 ;

--.IF ERRORCODE <> 0 THEN .QUIT 6

--Explicit transaction on Teradata ensures that we do not corrupt target table if failure in middle of updating target table

BEGIN TRANSACTION;

--.IF ERRORCODE <> 0 THEN .QUIT 2

-- SEQUENCED VALIDTIME

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_dim AS tgt
WHERE EXISTS (SELECT 1
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_dim_vtw AS src
 WHERE LOWER(rms_sku_num) = LOWER(tgt.rms_sku_num)
  AND LOWER(channel_country) = LOWER(tgt.channel_country)
  AND LOWER(channel_brand) = LOWER(tgt.channel_brand)
  AND LOWER(selling_channel) = LOWER(tgt.selling_channel)
  AND src.eff_begin_tmstp <= tgt.eff_begin_tmstp
  AND src.eff_end_tmstp >= tgt.eff_end_tmstp
  );

UPDATE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_dim AS TGT
SET TGT.eff_end_tmstp = SRC.eff_begin_tmstp,
TGT.eff_end_tmstp_tz = SRC.eff_begin_tmstp_tz
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_dim_vtw AS SRC
WHERE LOWER(SRC.rms_sku_num) = LOWER(TGT.rms_sku_num)
  AND LOWER(SRC.channel_country) = LOWER(TGT.channel_country)
  AND LOWER(SRC.channel_brand) = LOWER(TGT.channel_brand)
  AND LOWER(SRC.selling_channel) = LOWER(TGT.selling_channel)
  AND SRC.eff_begin_tmstp >= TGT.eff_begin_tmstp
  AND SRC.eff_begin_tmstp < TGT.eff_end_tmstp 
  AND SRC.eff_end_tmstp > TGT.eff_end_tmstp ;

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_dim AS TGT
SET TGT.eff_begin_tmstp = SRC.eff_end_tmstp,
TGT.eff_begin_tmstp_tz = SRC.eff_end_tmstp_tz
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_dim_vtw AS SRC
WHERE LOWER(SRC.rms_sku_num) = LOWER(TGT.rms_sku_num)
  AND LOWER(SRC.channel_country) = LOWER(TGT.channel_country)
  AND LOWER(SRC.channel_brand) = LOWER(TGT.channel_brand)
  AND LOWER(SRC.selling_channel) = LOWER(TGT.selling_channel)
  AND SRC.eff_end_tmstp > TGT.eff_begin_tmstp
  AND SRC.eff_end_tmstp <= TGT.eff_end_tmstp;


--.IF ERRORCODE <> 0 THEN .QUIT 7

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_dim (rms_sku_num, channel_country, channel_brand, selling_channel,
 is_sellable_ind, selling_status_code, selling_rights_start_tmstp,selling_rights_start_tmstp_tz, selling_status_start_tmstp,selling_status_start_tmstp_tz, eff_begin_tmstp, eff_begin_tmstp_tz ,
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
   WHERE LOWER(rms_sku_num) = LOWER(t.rms_sku_num)
    AND LOWER(channel_country) = LOWER(t.channel_country)
    AND LOWER(channel_brand) = LOWER(t.channel_brand)
    AND LOWER(selling_channel) = LOWER(t.selling_channel))
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, channel_country, channel_brand, selling_channel)) = 1);

--.IF ERRORCODE <> 0 THEN .QUIT 8

COMMIT TRANSACTION;
--.IF ERRORCODE <> 0 THEN .QUIT 9

--COLLECT STATISTICS       COLUMN ( rms_sku_num ) ,       COLUMN ( selling_status_code ) ,       COLUMN ( is_sellable_ind ) ,       COLUMN ( rms_sku_num,channel_country,channel_brand,selling_channel ) ,       COLUMN ( rms_sku_num,channel_country) ,       COLUMN ( eff_begin_tmstp ) ,       COLUMN ( eff_end_tmstp ),       COLUMN ( dw_batch_date ),       COLUMN ( dw_sys_load_tmstp )           ON cf-nordstrom.prd_nap_dim.PRODUCT_ITEM_SELLING_RIGHTS_DIM 