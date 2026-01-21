-- NONSEQUENCED VALIDTIME
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_setting_dtl_dim_vtw;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_setting_delete_stg;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_setting_delete_stg 
(rms_sku_num, location_num, replenishment_setting_type_code,
 replenishment_setting_cancelled_desc, eff_begin_tmstp,eff_begin_tmstp_tz
 )
(SELECT dim.rms_sku_num,
  dim.location_num,
  dim.replenishment_setting_type_code,
  'CANCELLED_FUTURE',
  dim.eff_begin_tmstp,
  dim.eff_begin_tmstp_tz
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_setting_dtl_dim AS dim
  INNER JOIN
   (SELECT op.rms_sku_num,
    op.location_num,
    op.replenishment_setting_type_code,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(op.replenishment_setting_start_date AS DATETIME)) AS DATETIME) AS
    replenishment_setting_start_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.rp_setting_dtl_opr AS op
    INNER JOIN 
    (SELECT rmsskuid AS rms_sku_num,
      CAST(trunc(CAST(locationid AS FLOAT64)) AS INTEGER) AS location_num,
      replenishmentsettingtype AS replenishment_setting_type_code
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_settings_ldg
     WHERE endtime IS NOT NULL
     GROUP BY rms_sku_num,
      location_num,
      replenishment_setting_type_code) AS LDG 
      ON LOWER(op.rms_sku_num) = LOWER(LDG.rms_sku_num) 
      AND op.location_num =
       LDG.location_num 
       AND LOWER(LDG.replenishment_setting_type_code) = LOWER(op.replenishment_setting_type_code)
   WHERE LOWER(op.replenishment_setting_cancelled_desc) = LOWER('CANCELLED_FUTURE')
    AND LOWER(op.replenishment_setting_cancelled_ind) = LOWER('Y')
   GROUP BY op.rms_sku_num,
    op.location_num,
    op.replenishment_setting_type_code,
    replenishment_setting_start_date) AS OPR
     ON LOWER(dim.rms_sku_num) = LOWER(OPR.rms_sku_num) 
     AND dim.location_num = OPR.location_num 
     AND LOWER(dim.replenishment_setting_type_code) = LOWER(OPR.replenishment_setting_type_code)
 WHERE CAST(dim.eff_begin_tmstp AS DATETIME) >= OPR.replenishment_setting_start_date);

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_setting_delete_stg 
(rms_sku_num, location_num, replenishment_setting_type_code,
 replenishment_setting_cancelled_desc, eff_begin_tmstp,eff_begin_tmstp_tz)
(SELECT dim.rms_sku_num,
  dim.location_num,
  dim.replenishment_setting_type_code,
  'CANCELLED_CURRENT',
  dim.eff_begin_tmstp,
  dim.eff_begin_tmstp_tz
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_setting_dtl_dim AS dim
  INNER JOIN 
  (SELECT op.rms_sku_num,
    op.location_num,
    op.replenishment_setting_type_code,
    op.replenishment_setting_cancelled_event_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.rp_setting_dtl_opr AS op
    INNER JOIN 
    (SELECT rmsskuid AS rms_sku_num,
      CAST(TRUNC(CAST(locationid AS FLOAT64)) AS INTEGER) AS location_num,
      replenishmentsettingtype AS replenishment_setting_type_code
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_settings_ldg
     WHERE endtime IS NOT NULL
     GROUP BY rms_sku_num,
      location_num,
      replenishment_setting_type_code) AS LDG
       ON LOWER(op.rms_sku_num) = LOWER(LDG.rms_sku_num) 
       AND op.location_num = LDG.location_num 
       AND LOWER(LDG.replenishment_setting_type_code) = LOWER(op.replenishment_setting_type_code)
   WHERE LOWER(op.replenishment_setting_cancelled_desc) = LOWER('CANCELLED_CURRENT')
    AND LOWER(op.replenishment_setting_cancelled_ind) = LOWER('Y')
   GROUP BY op.rms_sku_num,
    op.location_num,
    op.replenishment_setting_type_code,
    op.replenishment_setting_cancelled_event_tmstp) AS OPR
     ON LOWER(dim.rms_sku_num) = LOWER(OPR.rms_sku_num) 
     AND dim.location_num= OPR.location_num 
     AND LOWER(dim.replenishment_setting_type_code) = LOWER(OPR.replenishment_setting_type_code)
 WHERE dim.eff_end_tmstp >= OPR.replenishment_setting_cancelled_event_tmstp);

BEGIN TRANSACTION;

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_setting_dtl_dim AS tgt
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_setting_delete_stg AS src
    WHERE LOWER(rms_sku_num) = LOWER(tgt.rms_sku_num) 
    AND location_num = tgt.location_num 
    AND LOWER(replenishment_setting_type_code) = LOWER(tgt.replenishment_setting_type_code) 
    AND eff_begin_tmstp = tgt.eff_begin_tmstp);

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_setting_dtl_dim AS target  
SET eff_end_tmstp = CAST(CAST('9999-12-31 23:59:59.999999+00:00' AS DATETIME) AS TIMESTAMP),
     eff_end_tmstp_tz='+00:00'
 FROM (SELECT dim.rms_sku_num, dim.location_num, dim.replenishment_setting_type_code, MAX(dim.eff_begin_tmstp) AS max_eff_begin_tmstp, MAX(dim.eff_end_tmstp) AS max_eff_end_tmstp
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_setting_dtl_dim AS dim
            INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_setting_delete_stg AS pdel ON LOWER(dim.rms_sku_num) = LOWER(pdel.rms_sku_num) AND dim.location_num = pdel.location_num AND LOWER(dim.replenishment_setting_type_code) = LOWER(pdel.replenishment_setting_type_code)
        WHERE LOWER(pdel.replenishment_setting_cancelled_desc) = LOWER('CANCELLED_FUTURE')
        GROUP BY dim.rms_sku_num, dim.location_num, dim.replenishment_setting_type_code) AS SOURCE
WHERE LOWER(target.rms_sku_num) = LOWER(SOURCE.rms_sku_num) AND target.location_num = SOURCE.location_num AND LOWER(target.replenishment_setting_type_code) = LOWER(SOURCE.replenishment_setting_type_code) AND target.eff_begin_tmstp = SOURCE.max_eff_begin_tmstp AND target.eff_end_tmstp = SOURCE.max_eff_end_tmstp;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_STG.RP_SETTING_DTL_DIM_VTW
(   RMS_SKU_NUM
  , LOCATION_NUM
  , REPLENISHMENT_SETTING_TYPE_CODE
  , REPLENISHMENT_SETTING_VALUE_DESC
  , REPLENISHMENT_SETTING_LEVEL_CODE
  , EFF_BEGIN_TMSTP
  , EFF_BEGIN_TMSTP_tz
  , EFF_END_TMSTP
  , EFF_END_TMSTP_tz
)

WITH SRC AS (
  SELECT 
    --NORMALIZE
        rms_sku_num
      , location_num
      , replenishment_setting_type_code
      , replenishment_setting_value_desc
      , replenishment_setting_level_code
      , RANGE( eff_begin_tmstp, eff_end_tmstp ) AS eff_period
    FROM  (
    --inner normalize
            SELECT rms_sku_num, location_num,replenishment_setting_type_code,replenishment_setting_value_desc,replenishment_setting_level_code,max_eff_end_tmstp, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num,location_num ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM

(
        SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY rms_sku_num, location_num ORDER BY eff_begin_tmstp) >= 
                DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
         from (
	     SELECT src1.rms_sku_num
        ,src1.location_num
        ,src1.replenishment_setting_type_code
        ,src1.replenishment_setting_value_desc
        ,src1.replenishment_setting_level_code
        ,src1.eff_begin_tmstp
	    ,CASE WHEN src1.replenishment_setting_cancelled_ind = 'Y' THEN replenishment_setting_cancelled_event_tmstp
	             ELSE MIN(src1.eff_begin_tmstp) OVER(PARTITION BY src1.rms_sku_num, src1.location_num, src1.replenishment_setting_type_code
	            ORDER BY src1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) END AS max_eff_end_tmstp
	    , COALESCE((CASE WHEN src1.replenishment_setting_cancelled_ind = 'Y' THEN replenishment_setting_cancelled_event_tmstp
	             ELSE MIN(src1.eff_begin_tmstp) OVER(PARTITION BY src1.rms_sku_num, src1.location_num, src1.replenishment_setting_type_code
	            ORDER BY src1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) END),TIMESTAMP'9999-12-31 23:59:59.999999+00:00') AS eff_end_tmstp
	     FROM(
			SELECT
			SRC0.*,
			CASE
			WHEN src_event_tmstp >= replenishment_setting_start_tmstp THEN src_event_tmstp
			ELSE replenishment_setting_start_tmstp END AS eff_begin_tmstp
			FROM
			(SELECT op.rms_sku_num,
 op.location_num,
CAST(op.replenishment_setting_start_date AS timestamp) AS
 replenishment_setting_start_tmstp,
 op.replenishment_setting_type_code,
 op.replenishment_setting_value_desc,
 op.replenishment_setting_level_code,
 op.replenishment_setting_cancelled_ind,
 op.replenishment_setting_cancelled_event_tmstp,
 op.src_event_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.rp_setting_dtl_opr AS op
 INNER JOIN (SELECT rmsskuid AS rms_sku_num,
   CAST(trunc(CAST(locationid AS FLOAT64)) AS INTEGER) AS location_num,
   CAST(startdate AS DATE) AS replenishment_setting_start_dt,
   replenishmentsettingtype AS replenishment_setting_type_code,
   `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(endtime) AS replenishment_setting_cancelled_event_tmstp,
   `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(lastupdatedtime) AS src_cancelled_event_tmstp
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_settings_ldg
  GROUP BY rms_sku_num,
   location_num,
   replenishment_setting_start_dt,
   replenishment_setting_type_code,
   replenishment_setting_cancelled_event_tmstp,
   src_cancelled_event_tmstp) AS LDG 
   ON LOWER(op.rms_sku_num) = LOWER(LDG.rms_sku_num)
    AND op.location_num = LDG.location_num
     AND LOWER(LDG.replenishment_setting_type_code) = LOWER(op.replenishment_setting_type_code)
WHERE LOWER(op.replenishment_setting_cancelled_desc) <> LOWER('CANCELLED_FUTURE')
 AND (op.src_event_tmstp >= CAST(LDG.src_cancelled_event_tmstp AS TIMESTAMP) 
 OR op.replenishment_setting_cancelled_event_tmstp
      >= CAST(LDG.replenishment_setting_cancelled_event_tmstp AS TIMESTAMP)
       OR op.replenishment_setting_start_date
     >= LDG.replenishment_setting_start_dt)
GROUP BY op.rms_sku_num,
 op.location_num,
 replenishment_setting_start_tmstp,
 op.replenishment_setting_type_code,
 op.replenishment_setting_value_desc,
 op.replenishment_setting_level_code,
 op.replenishment_setting_cancelled_ind,
 op.replenishment_setting_cancelled_event_tmstp,
 op.src_event_tmstp
QUALIFY (ROW_NUMBER() OVER (PARTITION BY op.rms_sku_num, op.location_num, op.replenishment_setting_type_code,
     replenishment_setting_start_tmstp ORDER BY op.src_event_tmstp DESC)) = 1
			) SRC0
		) SRC1
    ) SRC2
    WHERE eff_begin_tmstp < eff_end_tmstp
)  AS ordered_data
) AS grouped_data
GROUP BY rms_sku_num, location_num,replenishment_setting_type_code,replenishment_setting_value_desc,replenishment_setting_level_code,max_eff_end_tmstp, range_group
ORDER BY  rms_sku_num, location_num, eff_begin_tmstp))



(SELECT distinct
    rms_sku_num
  , location_num
  , replenishment_setting_type_code
  , replenishment_setting_value_desc
  , replenishment_setting_level_code
   ,RANGE_START (eff_period) AS eff_begin
  ,`{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(RANGE_START (eff_period) AS STRING)) AS eff_begin_tz,
  RANGE_END (eff_period) AS eff_end,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(RANGE_END (eff_period) AS STRING)) AS eff_end_tz
FROM 
 (
    --inner normalize
            SELECT rms_sku_num,  location_num, replenishment_setting_type_code,replenishment_setting_value_desc,replenishment_setting_level_code, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp,eff_period
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num, location_num, replenishment_setting_type_code,replenishment_setting_value_desc,replenishment_setting_level_code ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM

(
        SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY rms_sku_num, location_num, replenishment_setting_type_code,replenishment_setting_value_desc,replenishment_setting_level_code ORDER BY eff_begin_tmstp) >= 
                DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
FROM ( select * from(
  -- NONSEQUENCED VALIDTIME
  SELECT
  -- NORMALIZE
    src.rms_sku_num
  , src.location_num
  , src.replenishment_setting_type_code
  , src.replenishment_setting_value_desc
  , src.replenishment_setting_level_code
  , tgt.eff_end_tmstp
  , tgt.eff_begin_tmstp
  , COALESCE( range_INTERSECT (src.eff_period, range(tgt.eff_begin_tmstp,tgt.eff_end_tmstp))
            , src.eff_period ) AS eff_period
  FROM SRC

  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_setting_dtl_dim tgt
  ON lower(src.rms_sku_num) = lower(tgt.rms_sku_num)
  AND src.location_num = tgt.location_num
  AND lower(src.replenishment_setting_type_code) = lower(tgt.replenishment_setting_type_code)
  AND  range_OVERLAPS (src.eff_period, range(tgt.eff_begin_tmstp, tgt.eff_end_tmstp))
  WHERE ( tgt.rms_sku_num IS NULL
       OR (lower(src.replenishment_setting_value_desc) <> lower(tgt.replenishment_setting_value_desc) OR (src.replenishment_setting_value_desc IS NOT NULL AND tgt.replenishment_setting_value_desc IS NULL) OR (src.replenishment_setting_value_desc IS NULL AND tgt.replenishment_setting_value_desc IS NOT NULL))
       OR (lower(src.replenishment_setting_level_code) <> lower(tgt.replenishment_setting_level_code) OR (src.replenishment_setting_level_code IS NOT NULL AND tgt.replenishment_setting_level_code IS NULL) OR (src.replenishment_setting_level_code IS NULL AND tgt.replenishment_setting_level_code IS NOT NULL))
       )
) NRML
WHERE NOT EXISTS (SELECT 1 AS `A12180`
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_setting_dtl_dim_vtw
     WHERE rms_sku_num = nrml.rms_sku_num
      AND location_num = nrml.location_num
      AND replenishment_setting_type_code = nrml.replenishment_setting_type_code)) AS t12
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, location_num, replenishment_setting_type_code)) = 1)
 AS ordered_data
) AS grouped_data
GROUP BY rms_sku_num,location_num,replenishment_setting_type_code,replenishment_setting_value_desc,replenishment_setting_level_code,eff_period
ORDER BY  rms_sku_num,location_num, replenishment_setting_type_code ));

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_setting_dtl_dim AS tgt
WHERE EXISTS (SELECT 1
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_setting_dtl_dim_vtw AS src
    WHERE LOWER(src.rms_sku_num) = LOWER(tgt.rms_sku_num) 
    AND src.location_num = tgt.location_num 
    AND LOWER(src.replenishment_setting_type_code) = LOWER(tgt.replenishment_setting_type_code)
    AND src.eff_begin_tmstp <= tgt.eff_begin_tmstp
    AND src.eff_end_tmstp >= tgt.eff_end_tmstp);
	
UPDATE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_setting_dtl_dim AS TGT
SET TGT.eff_end_tmstp = SRC.eff_begin_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_setting_dtl_dim_vtw AS SRC
    WHERE LOWER(src.rms_sku_num) = LOWER(tgt.rms_sku_num) 
    AND src.location_num = tgt.location_num 
    AND LOWER(src.replenishment_setting_type_code) = LOWER(tgt.replenishment_setting_type_code)
AND SRC.eff_begin_tmstp >= TGT.eff_begin_tmstp
AND SRC.eff_begin_tmstp < TGT.eff_end_tmstp 
AND SRC.eff_end_tmstp > TGT.eff_end_tmstp ;
	
UPDATE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_setting_dtl_dim AS TGT
SET TGT.eff_begin_tmstp = SRC.eff_end_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_setting_dtl_dim_vtw AS SRC
    WHERE LOWER(src.rms_sku_num) = LOWER(tgt.rms_sku_num) 
    AND src.location_num = tgt.location_num 
    AND LOWER(src.replenishment_setting_type_code) = LOWER(tgt.replenishment_setting_type_code)
 AND SRC.eff_end_tmstp > TGT.eff_begin_tmstp
    AND SRC.eff_end_tmstp <= TGT.eff_end_tmstp;



INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_setting_dtl_dim (rms_sku_num, location_num, replenishment_setting_type_code,
 replenishment_setting_value_desc, replenishment_setting_level_code,  eff_begin_tmstp,
    eff_begin_tmstp_tz,
    eff_end_tmstp,
    eff_end_tmstp_tz, dw_batch_id,
 dw_batch_date, dw_sys_load_tmstp)
(SELECT DISTINCT rms_sku_num,
  location_num,
  replenishment_setting_type_code,
  replenishment_setting_value_desc,
  replenishment_setting_level_code,
   eff_begin_tmstp,
    eff_begin_tmstp_tz,
    eff_end_tmstp,
    eff_end_tmstp_tz,
  dw_batch_id,
  dw_batch_date,
  dw_sys_load_tmstp
 FROM (SELECT rms_sku_num,
    location_num,
    replenishment_setting_type_code,
    replenishment_setting_value_desc,
    replenishment_setting_level_code,
    eff_begin_tmstp,
    eff_begin_tmstp_tz,
    eff_end_tmstp,
    eff_end_tmstp_tz,
    CAST(RPAD(FORMAT_TIMESTAMP('%Y%m%d%I%M%S', CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)), 14, ' ') AS BIGINT)
    AS dw_batch_id,
    CURRENT_DATE('PST8PDT') AS dw_batch_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_setting_dtl_dim_vtw) AS t
 WHERE NOT EXISTS (SELECT 1 AS `A12180`
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_setting_dtl_dim
   WHERE rms_sku_num = t.rms_sku_num
    AND location_num = t.location_num
    AND replenishment_setting_type_code = t.replenishment_setting_type_code)
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, location_num, replenishment_setting_type_code)) = 1);
COMMIT TRANSACTION;

