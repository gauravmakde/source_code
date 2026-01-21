TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_supply_dateset_dim_vtw;


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_supply_dateset_delete_stg;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_supply_dateset_delete_stg (rms_sku_num, location_num, eff_begin_tmstp,eff_begin_tmstp_tz)
(SELECT dim.rms_sku_num,
  dim.location_num,
  dim.eff_begin_tmstp,
  dim.eff_begin_tmstp_tz
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_supply_dateset_dim AS dim
  INNER JOIN (SELECT opr.rms_sku_num,
    opr.location_num,
    opr.on_supply_date,
    opr.off_supply_date,
    LDG.conv_src_event_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.rp_sku_loc_supply_dateset_opr AS opr
    INNER JOIN (SELECT rmsskuid AS rms_sku_num,
      CAST(TRUNC(CAST(locationid AS FLOAT64)) AS INTEGER) AS conv_location_num,
      `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(lastupdatedtime) AS conv_src_event_tmstp
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_supply_dateset_ldg
     GROUP BY rms_sku_num,
      conv_location_num,
      conv_src_event_tmstp) AS LDG 
   ON LOWER(opr.rms_sku_num) = LOWER(LDG.rms_sku_num)
    WHERE opr.location_num = LDG.conv_location_num
    AND LOWER(opr.rp_supply_date_set_cancelled_desc) = LOWER('CANCELLED_FUTURE')
    AND opr.rp_supply_date_set_cancelled_event_tmstp >= CAST(LDG.conv_src_event_tmstp AS TIMESTAMP)
     ) AS OPR ON LOWER(dim.rms_sku_num) = LOWER(OPR.rms_sku_num) AND dim.location_num = OPR.location_num AND dim.on_supply_date
     >= OPR.on_supply_date
 WHERE dim.eff_end_tmstp >= CAST(OPR.conv_src_event_tmstp AS TIMESTAMP)
 GROUP BY dim.rms_sku_num,
  dim.location_num,
  dim.eff_begin_tmstp,
  dim.eff_begin_tmstp_tz);


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_supply_dateset_dim AS tgt
WHERE EXISTS (SELECT *
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_supply_dateset_delete_stg AS src
 WHERE LOWER(rms_sku_num) = LOWER(tgt.rms_sku_num)
  AND location_num = tgt.location_num
  AND eff_begin_tmstp = tgt.eff_begin_tmstp);


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_supply_dateset_dim AS target 
SET
 eff_end_tmstp = CAST('9999-12-31 23:59:59.999999+00:00' AS TIMESTAMP),
 eff_end_tmstp_tz = '+00:00' FROM (SELECT dim.rms_sku_num,
   dim.location_num,
   MAX(dim.eff_begin_tmstp) AS max_eff_begin_tmstp,
   MAX(dim.eff_end_tmstp) AS max_eff_end_tmstp
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_supply_dateset_dim AS dim
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_supply_dateset_delete_stg AS pdel ON LOWER(dim.rms_sku_num) = LOWER(pdel.rms_sku_num) AND dim.location_num = pdel.location_num
  GROUP BY dim.rms_sku_num,
   dim.location_num) AS SOURCE
WHERE LOWER(target.rms_sku_num) = LOWER(SOURCE.rms_sku_num) AND target.location_num = SOURCE.location_num AND target.eff_begin_tmstp = SOURCE.max_eff_begin_tmstp AND target.eff_end_tmstp = SOURCE.max_eff_end_tmstp;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_supply_dateset_dim_vtw
(   
  rms_sku_num,
  location_num,
  on_supply_date,
  off_supply_date,
  eff_begin_tmstp,
  eff_begin_tmstp_tz,
  eff_end_tmstp,
  eff_end_tmstp_tz
)

WITH SRC AS

(select 
rms_sku_num, 
location_num, 
on_supply_date,
off_supply_date,
eff_begin_tmstp,
eff_end_tmstp
 from  (
    --inner normalize
            SELECT rms_sku_num, location_num, on_supply_date,off_supply_date, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num, location_num, on_supply_date,off_supply_date ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM

(
        SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY rms_sku_num, location_num, on_supply_date,off_supply_date ORDER BY eff_begin_tmstp) >= 
                DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
         from
 (
    SELECT DISTINCT
        src2.rms_sku_num,
        src2.location_num,
        src2.on_supply_date,
        src2.off_supply_date,
        src2.eff_begin_tmstp, 
        src2.eff_end_tmstp,
		-- src2.src_event_tmstp_tz
    FROM (
      SELECT
          src1.rms_sku_num,
          src1.location_num,
          src1.on_supply_date,
          src1.off_supply_date,
          src1.eff_begin_tmstp,
		  COALESCE(
            LEAD(src1.eff_begin_tmstp) OVER (PARTITION BY src1.rms_sku_num, src1.location_num ORDER BY src1.eff_begin_tmstp),
            TIMESTAMP('9999-12-31 23:59:59')
          ) AS eff_end_tmstp,
		  -- src1.src_event_tmstp_tz
      FROM (
        SELECT
            opr.rms_sku_num,
            opr.location_num,
            opr.on_supply_date,
            opr.off_supply_date,
            opr.src_event_tmstp AS eff_begin_tmstp,
            ldg.conv_src_event_tmstp,
			-- opr.src_event_tmstp_tz AS src_event_tmstp_tz
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.rp_sku_loc_supply_dateset_opr AS opr
        JOIN (
          SELECT 
              rmsskuid AS rms_sku_num,
              CAST(TRUNC(CAST(locationid AS FLOAT64)) AS INT64) AS location_num,
              `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(lastupdatedtime) AS conv_src_event_tmstp
          FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_supply_dateset_ldg
		  QUALIFY ROW_NUMBER() OVER (PARTITION BY rmsskuid, locationid ORDER BY 1 ASC) = 1
        ) AS ldg
        ON LOWER(opr.rms_sku_num) = LOWER(ldg.rms_sku_num)
        AND opr.location_num = ldg.location_num
        AND LOWER(CAST(opr.src_event_tmstp AS STRING)) >= LOWER(ldg.conv_src_event_tmstp)
        WHERE opr.rp_supply_date_set_cancelled_desc <> 'CANCELLED_FUTURE'
      ) AS src1
    ) AS src2
    WHERE eff_begin_tmstp < eff_end_tmstp
  )   ) AS ordered_data
) AS grouped_data
GROUP BY rms_sku_num, location_num, on_supply_date,off_supply_date, range_group
ORDER BY  rms_sku_num, location_num, on_supply_date,off_supply_date, eff_begin_tmstp))

SELECT
  rms_sku_num,
  location_num,
  on_supply_date,
  off_supply_date,
  MIN(RANGE_START(eff_period)) AS eff_begin,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(MIN(RANGE_START(eff_period)) AS STRING)) AS eff_begin_tmstp_tz,
  MAX(RANGE_END(eff_period)) AS eff_end,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(MAX(RANGE_END(eff_period)) AS STRING)) AS eff_end_tmstp_tz,
       from  (
    --inner normalize
            SELECT rms_sku_num, location_num, on_supply_date,off_supply_date,eff_period,range_group
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num, location_num, on_supply_date,off_supply_date ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM
      --  NONSEQUENCED VALIDTIME
      (
        
            --  NORMALIZE
         SELECT  src.rms_sku_num, src.location_num, src.on_supply_date,src.off_supply_date,TGT.eff_begin_tmstp,
            CASE 
                WHEN LAG(TGT.eff_end_tmstp) OVER (PARTITION BY src.rms_sku_num, src.location_num, src.on_supply_date,src.off_supply_date ORDER BY TGT.eff_begin_tmstp) >= 
                DATE_SUB(TGT.eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag,
             COALESCE(RANGE_INTERSECT(range(SRC.EFF_BEGIN_TMSTP, SRC.EFF_END_TMSTP) , RANGE(TGT.eff_begin_tmstp,TGT.eff_end_tmstp)), range(SRC.EFF_BEGIN_TMSTP, SRC.EFF_END_TMSTP)) AS eff_period ,
	-- src.src_event_tmstp_tz
  FROM SRC
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_supply_dateset_dim tgt
  ON src.rms_sku_num = tgt.rms_sku_num
  AND src.location_num = tgt.location_num
  -- AND (RANGE_START(src.eff_period) < tgt.eff_end_tmstp AND RANGE_END(src.eff_period) > tgt.eff_begin_tmstp)
    AND range_overlaps(range(SRC.EFF_BEGIN_TMSTP, SRC.EFF_END_TMSTP),range(TGT.EFF_BEGIN_TMSTP, TGT.EFF_END_TMSTP))
  WHERE (
      tgt.rms_sku_num IS NULL
      OR (src.on_supply_date <> tgt.on_supply_date OR (src.on_supply_date IS NOT NULL AND tgt.on_supply_date IS NULL) OR (src.on_supply_date IS NULL AND tgt.on_supply_date IS NOT NULL))
      OR (src.off_supply_date <> tgt.off_supply_date OR (src.off_supply_date IS NOT NULL AND tgt.off_supply_date IS NULL) OR (src.off_supply_date IS NULL AND tgt.off_supply_date IS NOT NULL))
  )
    )  AS ordered_data
) AS grouped_data)
GROUP BY rms_sku_num, location_num, on_supply_date,off_supply_date,eff_period,range_group
ORDER BY  rms_sku_num,location_num, on_supply_date,off_supply_date;



DELETE FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_supply_dateset_dim AS tgt
WHERE EXISTS (SELECT 1
    FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_supply_dateset_dim_vtw AS src
   WHERE LOWER(src.rms_sku_num) = LOWER(tgt.rms_sku_num)
    AND src.location_num = tgt.location_num
    AND src.eff_begin_tmstp <= tgt.eff_begin_tmstp
    AND src.eff_end_tmstp >= tgt.eff_end_tmstp);
	
UPDATE   `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_supply_dateset_dim AS TGT
SET TGT.eff_end_tmstp = SRC.eff_begin_tmstp
FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_supply_dateset_dim_vtw AS SRC
WHERE LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num)
AND SRC.location_num = tgt.location_num
AND SRC.eff_begin_tmstp >= TGT.eff_begin_tmstp
AND SRC.eff_begin_tmstp < TGT.eff_end_tmstp 
AND SRC.eff_end_tmstp > TGT.eff_end_tmstp ;
	
UPDATE   `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_supply_dateset_dim AS TGT
SET TGT.eff_begin_tmstp = SRC.eff_end_tmstp
FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_supply_dateset_dim_vtw AS SRC
WHERE LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num)
AND SRC.location_num = tgt.location_num
AND SRC.eff_end_tmstp > TGT.eff_begin_tmstp
AND SRC.eff_end_tmstp <= TGT.eff_end_tmstp;





INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_supply_dateset_dim (rms_sku_num, location_num, on_supply_date, off_supply_date,
 eff_begin_tmstp,eff_begin_tmstp_tz, eff_end_tmstp,eff_end_tmstp_tz, dw_batch_id, dw_batch_date, dw_sys_load_tmstp)
(SELECT DISTINCT rms_sku_num,
  location_num,
  on_supply_date,
  off_supply_date,
  eff_begin_tmstp,
  eff_begin_tmstp_tz,
  eff_end_tmstp,
  eff_end_tmstp_tz,
  dw_batch_id,
  dw_batch_date,
  dw_sys_load_tmstp
 FROM (SELECT rms_sku_num,
    location_num,
    on_supply_date,
    off_supply_date,
    eff_begin_tmstp,
	eff_begin_tmstp_tz,
    eff_end_tmstp,
	eff_end_tmstp_tz,
    CAST(RPAD(FORMAT_TIMESTAMP('%Y%m%d%I%M%S', CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)), 14, ' ') AS BIGINT)
    AS dw_batch_id,
    CURRENT_DATE('PST8PDT') AS dw_batch_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_supply_dateset_dim_vtw) AS t
 WHERE NOT EXISTS (SELECT 1 AS `A12180`
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_supply_dateset_dim
   WHERE rms_sku_num = t.rms_sku_num
    AND location_num = t.location_num)
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, location_num)) = 1);