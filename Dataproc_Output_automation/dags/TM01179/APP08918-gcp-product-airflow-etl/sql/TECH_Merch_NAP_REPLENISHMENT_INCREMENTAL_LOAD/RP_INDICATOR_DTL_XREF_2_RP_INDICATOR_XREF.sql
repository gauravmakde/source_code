--NONSEQUENCED VALIDTIME 
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_indicator_xref_vtw;

BEGIN TRANSACTION;

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_indicator_xref
WHERE (rms_sku_num, location_num) IN 
(SELECT (rms_sku_num, location_num) FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_indicator_dtl_xref
        WHERE dw_batch_id > (SELECT CAST(config_value AS BIGINT)
                    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
                    WHERE LOWER(interface_code) = LOWER('RP_LOAD_MIN_BATCH_ID')) 
                    AND dw_batch_id <= (SELECT CAST(config_value AS BIGINT) 
                    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
                    WHERE LOWER(interface_code) = LOWER('RP_LOAD_MAX_BATCH_ID'))
        GROUP BY rms_sku_num, location_num);

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_indicator_xref_vtw
(
    rms_sku_num
  , location_num
  , rp_ind_flag
  , rp_active_flag
  , eff_begin_tmstp
  , eff_end_tmstp
  , eff_begin_tmstp_tz
  , eff_end_tmstp_tz
)

WITH SRC AS (
  SELECT
        rms_sku_num
      , location_num
      , rp_ind_flag
      , rp_active_flag
      , RANGE( eff_begin_tmstp, eff_end_tmstp ) AS eff_period
    from  (
    --inner normalize
            SELECT rms_sku_num, location_num,rp_ind_flag,rp_active_flag, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
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
         from 
    (
      SELECT
        src1.rms_sku_num
      , src1.location_num
      , src1.rp_ind_flag
      , src1.rp_active_flag
      , src1.eff_begin_tmstp
          ,COALESCE( MIN(src1.eff_begin_tmstp) OVER ( PARTITION BY src1.rms_sku_num, src1.location_num 
                 ORDER BY src1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING )
                 , TIMESTAMP '9999-12-31 23:59:59.999999 UTC' ) AS eff_end_tmstp
      FROM (
      SELECT rms_sku_num,
           location_num,

           CASE 
              WHEN LOWER(is_replenishment_eligible_ind) = LOWER('N') THEN 'N'
	           WHEN CAST(eff_begin_tmstp  AS DATE) >= ON_SALE_DATE
	                   AND CAST(eff_begin_tmstp AS DATE) < OFF_SALE_DATE  THEN 'Y'
                 ELSE 'N' 
          END AS rp_ind_flag,
           CASE
              WHEN LOWER((CASE 
              WHEN LOWER(is_replenishment_eligible_ind) = LOWER('N') THEN 'N'
	         WHEN CAST(eff_begin_tmstp  AS DATE) >= ON_SALE_DATE
	                   AND CAST(eff_begin_tmstp AS DATE) < OFF_SALE_DATE  THEN 'Y'
                 ELSE 'N' 
          END)) = LOWER('N') THEN 'N'
	             WHEN (LOWER(replenishment_setting_type_code) = LOWER('REPLENISHMENT_METHOD')
	              AND LOWER(replenishment_setting_value_desc) <> LOWER('NO_REPLENISHMENT')  ) THEN 'Y'
                 ELSE 'N' 
          END AS rp_active_flag,

           eff_begin_tmstp
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_indicator_dtl_xref xref
        WHERE dw_batch_id > (SELECT CAST(config_value AS BIGINT) FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup WHERE LOWER(interface_code) =LOWER('RP_LOAD_MIN_BATCH_ID'))
        AND dw_batch_id <= (SELECT CAST(config_value AS BIGINT) FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup WHERE LOWER(interface_code) =LOWER('RP_LOAD_MAX_BATCH_ID'))
        GROUP BY 1,2,3,4,5
      ) src1
    ) src2
    WHERE eff_begin_tmstp < eff_end_tmstp
) AS ordered_data
) AS grouped_data
GROUP BY rms_sku_num, location_num,rp_ind_flag,rp_active_flag, range_group
ORDER BY  rms_sku_num, location_num, eff_begin_tmstp))


SELECT
    rms_sku_num
  , location_num
  , rp_ind_flag
  , rp_active_flag
  , RANGE_START(eff_period) AS eff_begin
  , RANGE_END(eff_period)   AS eff_end
  ,`{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(RANGE_START(eff_period) AS STRING)) AS eff_begin_tz
  ,`{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(RANGE_END(eff_period)AS STRING)) AS eff_end_tz
FROM 
 (
    --inner normalize
            SELECT rms_sku_num,  location_num, rp_ind_flag,rp_active_flag, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp,eff_period
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num, location_num,rp_ind_flag,rp_active_flag ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM

(
        SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY rms_sku_num, location_num, rp_ind_flag,rp_active_flag ORDER BY eff_begin_tmstp) >= 
                DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
FROM  
(
  -- NONSEQUENCED VALIDTIME
  SELECT
    src.rms_sku_num
  , src.location_num
  , src.rp_ind_flag
  , src.rp_active_flag
  , tgt.eff_end_tmstp
  , tgt.eff_begin_tmstp
  , COALESCE( RANGE_INTERSECT(src.eff_period,RANGE(tgt.eff_begin_tmstp,tgt.eff_end_tmstp)), src.eff_period ) AS eff_period
  FROM SRC

  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_indicator_xref tgt
  ON LOWER(src.rms_sku_num) = LOWER(tgt.rms_sku_num)
  AND src.location_num = tgt.location_num
  AND RANGE_OVERLAPS (src.eff_period, RANGE(tgt.eff_begin_tmstp, tgt.eff_end_tmstp))
  WHERE ( tgt.rms_sku_num IS NULL OR
     (  (src.rp_ind_flag <> tgt.rp_ind_flag OR (src.rp_ind_flag IS NOT NULL AND tgt.rp_ind_flag IS NULL) OR (src.rp_ind_flag IS NULL AND tgt.rp_ind_flag IS NOT NULL))
      OR (src.rp_active_flag <> tgt.rp_active_flag OR (src.rp_active_flag IS NOT NULL AND tgt.rp_active_flag IS NULL) OR (src.rp_active_flag IS NULL AND tgt.rp_active_flag IS NOT NULL))
     )  )
) AS ordered_data
)) AS grouped_data
GROUP BY rms_sku_num,location_num, rp_ind_flag,rp_active_flag,eff_period
ORDER BY  rms_sku_num,location_num, rp_ind_flag,rp_active_flag, eff_begin_tmstp);


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_indicator_xref AS TGT
WHERE EXISTS (
  SELECT 1
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_indicator_xref_vtw AS SRC
    WHERE LOWER(src.rms_sku_num) = LOWER(tgt.rms_sku_num) 
	AND src.location_num = tgt.location_num
  AND SRC.eff_begin_tmstp <= TGT.eff_begin_tmstp
  AND SRC.eff_end_tmstp >= TGT.eff_end_tmstp
);

UPDATE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_indicator_xref AS TGT
SET TGT.eff_end_tmstp = SRC.eff_begin_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_indicator_xref_vtw AS SRC
    WHERE LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) 
	AND SRC.location_num = tgt.location_num
  AND SRC.eff_begin_tmstp >= TGT.eff_begin_tmstp
  AND SRC.eff_begin_tmstp < TGT.eff_end_tmstp 
  AND SRC.eff_end_tmstp > TGT.eff_end_tmstp ;


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_indicator_xref AS TGT
SET TGT.eff_begin_tmstp = SRC.eff_end_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_indicator_xref_vtw AS SRC
    WHERE LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) 
	AND SRC.location_num = tgt.location_num
  AND SRC.eff_end_tmstp > TGT.eff_begin_tmstp
  AND SRC.eff_end_tmstp <= TGT.eff_end_tmstp;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_indicator_xref 
(
 rms_sku_num,
 location_num,
 rp_ind_flag,
 rp_active_flag,
  eff_begin_tmstp,
  eff_end_tmstp,
  eff_begin_tmstp_tz,
  eff_end_tmstp_tz,
  dw_batch_id,
  dw_batch_date,
  dw_sys_load_tmstp)
(SELECT DISTINCT rms_sku_num,
  location_num,
  rp_ind_flag,
  rp_active_flag,
  eff_begin_tmstp,
  eff_end_tmstp,
  eff_begin_tmstp_tz,
  eff_end_tmstp_tz,
  dw_batch_id,
  dw_batch_date,
  dw_sys_load_tmstp
 FROM (SELECT rms_sku_num,
    location_num,
    rp_ind_flag,
    rp_active_flag,
    eff_begin_tmstp,
    eff_end_tmstp,
    eff_begin_tmstp_tz,
    eff_end_tmstp_tz,
     (SELECT CAST(config_value AS BIGINT) AS config_value
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
     WHERE LOWER(interface_code) = LOWER('RP_LOAD_MAX_BATCH_ID')) AS dw_batch_id,
    CURRENT_DATE('PST8PDT') AS dw_batch_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_indicator_xref_vtw) AS t1
 WHERE NOT EXISTS (SELECT 1 AS `A12180`
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_indicator_xref
   WHERE rms_sku_num = t1.rms_sku_num
    AND location_num = t1.location_num)
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, location_num)) = 1);

COMMIT TRANSACTION;