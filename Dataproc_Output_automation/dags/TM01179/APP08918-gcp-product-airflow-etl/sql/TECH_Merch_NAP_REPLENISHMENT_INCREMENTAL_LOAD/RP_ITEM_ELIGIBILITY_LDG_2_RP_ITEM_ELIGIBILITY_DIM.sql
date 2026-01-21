-- NONSEQUENCED VALIDTIME --Purge work table for staging temporal rows

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_item_eligibility_dim_vtw;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_item_eligibility_dim_vtw
(
    rms_sku_num
  , channel_brand
  , is_replenishment_eligible_ind
  , eff_begin_tmstp
  , eff_begin_tmstp_tz
  , eff_end_tmstp
  , eff_end_tmstp_tz
)

WITH SRC AS (
  SELECT
    --  NORMALIZE
        rms_sku_num
      , channel_brand
      , is_replenishment_eligible_ind
      , RANGE(eff_begin_tmstp, eff_end_tmstp ) AS eff_period
     FROM  (
    --inner normalize
            SELECT rms_sku_num, channel_brand,is_replenishment_eligible_ind, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num,channel_brand ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM

(
        SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY rms_sku_num, channel_brand ORDER BY eff_begin_tmstp) >= 
                DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
         FROM
     (
      SELECT
        SRC1.rms_sku_num
      , SRC1.channel_brand
      , SRC1.is_replenishment_eligible_ind
      , SRC1.eff_begin_tmstp
      , COALESCE(MIN(SRC1.eff_begin_tmstp) OVER(PARTITION BY SRC1.rms_sku_num ,SRC1.channel_brand
            ORDER BY SRC1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
           ,TIMESTAMP'9999-12-31 23:59:59.999999+00:00') AS eff_end_tmstp
      FROM (
        SELECT rmsSkuId AS rms_sku_num
          , channelBrand AS channel_brand
          , CASE WHEN TRIM(isReplenishmentEligible) = 'true' THEN 'Y' ELSE 'N' END  AS is_replenishment_eligible_ind
          , cast(`{{params.gcp_project_id}}.JWN_UDF.ISO8601_TMSTP`(lastUpdatedTime)as timestamp) AS eff_begin_tmstp
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_STG.RP_ITEM_ELIGIBILITY_LDG
        GROUP BY 1,2,3,4
      ) SRC1
    ) SRC2
    WHERE  eff_begin_tmstp < eff_end_tmstp
) AS ordered_data
) AS grouped_data
GROUP BY rms_sku_num, channel_brand,is_replenishment_eligible_ind,range_group
ORDER BY  rms_sku_num, channel_brand, eff_begin_tmstp))





(SELECT DISTINCT
    rms_sku_num
  , channel_brand
  , is_replenishment_eligible_ind
  , RANGE_START(eff_period) AS eff_begin
  ,`{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(RANGE_START(eff_period)AS STRING)) AS eff_begin_tz
  , RANGE_END(eff_period)   AS eff_end
  ,`{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(RANGE_END(eff_period)AS STRING)) AS eff_end_tz
FROM  (
    --inner normalize
            SELECT rms_sku_num, channel_brand,is_replenishment_eligible_ind, MIN(eff_begin_tmstp) AS eff_begin_tmstp,eff_period, MAX(eff_end_tmstp) AS eff_end_tmstp
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num,channel_brand ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM

(
        SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY rms_sku_num, channel_brand ORDER BY eff_begin_tmstp) >= 
                DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
         FROM (
  -- NONSEQUENCED VALIDTIME
  SELECT 
  -- NORMALIZE
    SRC.rms_sku_num
  , SRC.channel_brand
  , SRC.is_replenishment_eligible_ind
  , TGT.eff_end_tmstp
  , TGT.eff_begin_tmstp
  , COALESCE(RANGE_INTERSECT(SRC.eff_period , RANGE(TGT.eff_begin_tmstp_utc,TGT.eff_end_tmstp_utc)), SRC.eff_period ) AS eff_period
  FROM SRC

  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.RP_ITEM_ELIGIBILITY_DIM TGT
  ON LOWER(SRC.rms_sku_num) = LOWER(TGT.rms_sku_num)
  AND LOWER(SRC.channel_brand) = LOWER(TGT.channel_brand)
  AND RANGE_OVERLAPS(SRC.eff_period, RANGE(TGT.eff_begin_tmstp_utc, TGT.eff_end_tmstp_utc))
  WHERE ( TGT.rms_sku_num IS NULL OR
     (  (SRC.is_replenishment_eligible_ind <> TGT.is_replenishment_eligible_ind 
     OR (SRC.is_replenishment_eligible_ind IS NOT NULL 
     AND TGT.is_replenishment_eligible_ind IS NULL) OR (SRC.is_replenishment_eligible_ind IS NULL 
     AND TGT.is_replenishment_eligible_ind IS NOT NULL))
     )  )
) NRML
WHERE NOT EXISTS (SELECT 1 AS `A12180`
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_item_eligibility_dim_vtw
     WHERE LOWER(rms_sku_num) = LOWER(NRML.rms_sku_num)
      AND LOWER(channel_brand) = LOWER(NRML.channel_brand)) 
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, channel_brand)) = 1)  AS ordered_data
) AS grouped_data
GROUP BY rms_sku_num, channel_brand,eff_period,is_replenishment_eligible_ind
ORDER BY  rms_sku_num, channel_brand, eff_begin_tmstp));


BEGIN TRANSACTION;
-- SEQUENCED VALIDTIME

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_item_eligibility_dim AS TGT
WHERE EXISTS (
  SELECT 1
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_item_eligibility_dim_vtw AS SRC
    WHERE LOWER(rms_sku_num) = LOWER(tgt.rms_sku_num) 
	AND LOWER(channel_brand) = LOWER(tgt.channel_brand)
  AND SRC.eff_begin_tmstp <= TGT.eff_begin_tmstp
  AND SRC.eff_end_tmstp >= TGT.eff_end_tmstp
);

UPDATE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_item_eligibility_dim AS TGT
SET TGT.eff_end_tmstp = SRC.eff_begin_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_item_eligibility_dim_vtw AS SRC
    WHERE LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) 
	AND LOWER(SRC.channel_brand) = LOWER(tgt.channel_brand)
  AND SRC.eff_begin_tmstp >= TGT.eff_begin_tmstp
  AND SRC.eff_begin_tmstp < TGT.eff_end_tmstp 
  AND SRC.eff_end_tmstp > TGT.eff_end_tmstp ;


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_item_eligibility_dim AS TGT
SET TGT.eff_begin_tmstp = SRC.eff_end_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_item_eligibility_dim_vtw AS SRC
    WHERE LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) 
	AND LOWER(SRC.channel_brand) = LOWER(tgt.channel_brand)
  AND SRC.eff_end_tmstp > TGT.eff_begin_tmstp
  AND SRC.eff_end_tmstp <= TGT.eff_end_tmstp;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_item_eligibility_dim (rms_sku_num, channel_brand, is_replenishment_eligible_ind,
 eff_begin_tmstp,
  eff_begin_tmstp_tz,
  eff_end_tmstp,
  eff_end_tmstp_tz, dw_batch_id, dw_batch_date, dw_sys_load_tmstp)
(SELECT DISTINCT rms_sku_num,
  channel_brand,
  is_replenishment_eligible_ind,
  eff_begin_tmstp,
  eff_begin_tmstp_tz,
  eff_end_tmstp,
  eff_end_tmstp_tz,
  dw_batch_id,
  dw_batch_date,
  dw_sys_load_tmstp
 FROM (SELECT rms_sku_num,
    channel_brand,
    is_replenishment_eligible_ind,
    eff_begin_tmstp,
  eff_begin_tmstp_tz,
  eff_end_tmstp,
  eff_end_tmstp_tz,
    CAST(RPAD(FORMAT_TIMESTAMP('%Y%m%d%I%M%S', CURRENT_DATETIME()), 14, ' ') AS BIGINT)
    AS dw_batch_id,
    CURRENT_DATE AS dw_batch_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME()) AS DATETIME) AS dw_sys_load_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_item_eligibility_dim_vtw) AS t
 WHERE NOT EXISTS (SELECT 1 AS `A12180`
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_item_eligibility_dim
   WHERE LOWER(rms_sku_num) = LOWER(t.rms_sku_num)
    AND LOWER(channel_brand) = LOWER(t.channel_brand))
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, channel_brand)) = 1);

COMMIT TRANSACTION;

