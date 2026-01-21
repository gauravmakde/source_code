--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=stg-dim;AppSubArea=RP_WH_RANGED;' UPDATE FOR SESSION;

-- NONSEQUENCED VALIDTIME --Purge work table for staging temporal rows
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.item_warehouse_ranged_dim_vtw;
--.IF ERRORCODE <> 0 THEN .QUIT 1

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.item_warehouse_ranged_dim_vtw
(
    rms_sku_num
  , location_num
  , item_ranged_ind
  , eff_begin_tmstp
  , eff_begin_tmstp_tz
  , eff_end_tmstp
  , eff_end_tmstp_tz
)



WITH src as 
  (
    SELECT DISTINCT  --normalize
        rms_sku_num
      , location_num
      , item_ranged_ind
      , eff_begin_tmstp
      , eff_end_tmstp
      , eff_begin_tmstp_tz
      , eff_end_tmstp_tz
      , range( eff_begin_tmstp, eff_end_tmstp ) AS eff_period
    FROM 
    

(
    --inner normalize
            SELECT rms_sku_num,location_num,item_ranged_ind ,
            MIN(eff_begin_tmstp_tz) AS eff_begin_tmstp_tz,MAX(eff_end_tmstp_tz) AS eff_end_tmstp_tz, 
            MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
            FROM

    (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num,location_num,item_ranged_ind  ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM
    (  
     SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY rms_sku_num,location_num,item_ranged_ind 
				ORDER BY eff_begin_tmstp) >= DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag FROM
     ( SELECT
        SRC1.rms_sku_num
      , SRC1.location_num
      , SRC1.item_ranged_ind
      , SRC1.eff_begin_tmstp
      , `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(eff_begin_tmstp as string)) AS eff_begin_tmstp_tz
      , (COALESCE(MIN(SRC1.eff_begin_tmstp) OVER(PARTITION BY SRC1.rms_sku_num, SRC1.location_num
            ORDER BY SRC1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
           ,TIMESTAMP'9999-12-31 23:59:59.999999+00:00')) as eff_end_tmstp

      , `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(COALESCE(MIN(SRC1.eff_begin_tmstp) OVER(PARTITION BY SRC1.rms_sku_num, SRC1.location_num
            ORDER BY SRC1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
           ,TIMESTAMP'9999-12-31 23:59:59.999999+00:00') as string)) as eff_end_tmstp_tz

      FROM (
        SELECT DISTINCT rmsSkuId AS rms_sku_num
          , locationId AS location_num
          , CASE WHEN TRIM(itemRangedIndicator) = 'true' THEN 'Y' ELSE 'N' END AS item_ranged_ind
          -- , `{{params.gcp_project_id}}`.nord_udf.epoch_tmstp(CAST(FLOOR(CAST(lastUpdatedTime AS FLOAT64)) AS INT64)) AS eff_begin_tmstp
          , cast(`{{params.gcp_project_id}}`.NORD_UDF.ISO8601_TMSTP(lastUpdatedTime) as timestamp) AS eff_begin_tmstp
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.item_warehouse_ranged_ldg
        GROUP BY 1,2,3,4
      ) SRC1
    ) SRC2
    WHERE eff_begin_tmstp < eff_end_tmstp
    ) AS ordered_data
    ) AS grouped_data
   GROUP BY rms_sku_num,location_num,item_ranged_ind ,range_group
   ORDER BY rms_sku_num,location_num,item_ranged_ind,eff_begin_tmstp
)
  ) 


SELECT
    NRML.rms_sku_num
  , cast(FLOOR(CAST(NRML.location_num AS FLOAT64)) as int64) AS location_num
  , NRML.item_ranged_ind
  , MIN(RANGE_START(NRML.eff_period)) AS eff_begin
  , `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(MIN(RANGE_START(NRML.eff_period)) AS STRING)) AS eff_begin_tmstp_tz
  , MAX(RANGE_END(NRML.eff_period))   AS eff_end
  , `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(MAX(RANGE_END(nrml.eff_period))AS STRING)) AS eff_end_tmstp_tz
FROM (
SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num,location_num,item_ranged_ind
				ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM
(
SELECT *, CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY  rms_sku_num,location_num,item_ranged_ind
 ORDER BY eff_begin_tmstp) >= DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
            FROM
(

  -- NONSEQUENCED VALIDTIME
  SELECT DISTINCT  --normalize
    SRC.rms_sku_num
  , SRC.location_num
  , SRC.item_ranged_ind
  , TGT.eff_begin_tmstp_utc as eff_begin_tmstp
  , TGT.eff_end_tmstp_utc as eff_end_tmstp
  , COALESCE(  RANGE_INTERSECT( SRC.eff_period,range(TGT.eff_begin_tmstp_utc,TGT.eff_end_tmstp_utc))
            , SRC.eff_period ) AS eff_period
            , TGT.eff_begin_tmstp_tz
            , TGT.eff_end_tmstp_tz
  FROM 
  
  SRC
  
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.item_warehouse_ranged_dim TGT
  ON SRC.rms_sku_num = TGT.rms_sku_num
  AND cast(FLOOR(CAST(SRC.location_num AS FLOAT64)) as int64) = TGT.location_num
  AND  RANGE_OVERLAPS(SRC.eff_period, range(TGT.eff_begin_tmstp_utc, TGT.eff_end_tmstp_utc))
  WHERE ( TGT.rms_sku_num IS NULL OR
     (  (SRC.item_ranged_ind <> TGT.item_ranged_ind OR (SRC.item_ranged_ind IS NOT NULL AND TGT.item_ranged_ind IS NULL) OR (SRC.item_ranged_ind IS NULL AND TGT.item_ranged_ind IS NOT NULL))
     )  )

) AS ordered_data 
)AS grouped_data
) NRML

GROUP BY  rms_sku_num,location_num,item_ranged_ind,range_group
ORDER BY  rms_sku_num,location_num,item_ranged_ind,eff_begin

;
--.IF ERRORCODE <> 0 THEN .QUIT 2

--Explicit transaction on Teradata ensures that we do not corrupt target table if failure in middle of updating target table
-- BT;
--.IF ERRORCODE <> 0 THEN .QUIT 3

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.item_warehouse_ranged_dim AS TGT
WHERE EXISTS (
  SELECT 1
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.item_warehouse_ranged_dim_vtw AS SRC
 WHERE LOWER(src.rms_sku_num) = LOWER(tgt.rms_sku_num)
  AND src.location_num = tgt.location_num
  AND SRC.eff_begin_tmstp <= TGT.eff_begin_tmstp
  AND SRC.eff_end_tmstp >= TGT.eff_end_tmstp
);

UPDATE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.item_warehouse_ranged_dim AS TGT
SET TGT.eff_end_tmstp = SRC.eff_begin_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.item_warehouse_ranged_dim_vtw AS SRC
 WHERE LOWER(src.rms_sku_num) = LOWER(tgt.rms_sku_num)
  AND src.location_num = tgt.location_num
  AND SRC.eff_begin_tmstp >= TGT.eff_begin_tmstp
  AND SRC.eff_begin_tmstp < TGT.eff_end_tmstp 
  AND SRC.eff_end_tmstp > TGT.eff_end_tmstp ;


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.item_warehouse_ranged_dim AS TGT
SET TGT.eff_begin_tmstp = SRC.eff_end_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.item_warehouse_ranged_dim_vtw AS SRC
 WHERE LOWER(src.rms_sku_num) = LOWER(tgt.rms_sku_num)
  AND src.location_num = tgt.location_num
  AND SRC.eff_end_tmstp > TGT.eff_begin_tmstp
  AND SRC.eff_end_tmstp <= TGT.eff_end_tmstp;
--.IF ERRORCODE <> 0 THEN .QUIT 4

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.item_warehouse_ranged_dim
(
    rms_sku_num
  , location_num
  , item_ranged_ind
  , eff_begin_tmstp
  , eff_begin_tmstp_tz
  , eff_end_tmstp
  , eff_end_tmstp_tz
  , dw_batch_id
  , dw_batch_date
  , dw_sys_load_tmstp
)
SELECT rms_sku_num
  , location_num
  , item_ranged_ind
  , eff_begin_tmstp
  , eff_begin_tmstp_tz
  , eff_end_tmstp
  ,eff_end_tmstp_tz
  -- , CAST(CAST(CAST(CURRENT_TIMESTAMP AS FORMAT 'YYYYMMDDHHMISS') AS CHAR(14)) AS BIGINT) AS dw_batch_id
  ,CAST(RPAD(FORMAT_TIMESTAMP('%Y%m%d%H%M%S', CURRENT_TIMESTAMP()),14, ' ') AS BIGINT) AS dw_batch_id
  , current_date('PST8PDT') AS dw_batch_date
  , current_datetime('PST8PDT') AS dw_sys_load_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.item_warehouse_ranged_dim_vtw
;
--.IF ERRORCODE <> 0 THEN .QUIT 5

-- ET;
--.IF ERRORCODE <> 0 THEN .QUIT 6



-- COLLECT STATISTICS
--       COLUMN ( item_ranged_ind),
--       COLUMN ( rms_sku_num ),
--       COLUMN ( rms_sku_num , location_num)
--        ON `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.ITEM_WAREHOUSE_RANGED_DIM;

