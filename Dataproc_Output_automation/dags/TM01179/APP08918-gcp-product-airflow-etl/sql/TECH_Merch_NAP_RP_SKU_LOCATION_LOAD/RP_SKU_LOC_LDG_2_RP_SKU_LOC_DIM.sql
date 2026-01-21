
--Truncate the delete table before each load
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_del_stg;



--Load the delete table with records that needs cleanup


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_del_stg
( rms_sku_num
, location_num
, min_change_date
, dw_sys_load_tmstp
)
SELECT LDG.sku,
 cast(trunc(cast(LDG.locationid as float64)) AS INTEGER),
 MIN(PARSE_DATE('%F', LDG.changedate)) AS min_change_date,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_dim AS dim
 INNER JOIN (SELECT *
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_ldg
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY sku, locationid ORDER BY PARSE_DATE('%F', changedate))) = 1) AS LDG 
  ON LOWER(dim.rms_sku_num) = LOWER(LDG.sku) 
  AND dim.location_num = CAST(trunc(cast(LDG.locationid as float64)) AS INTEGER) 
  AND dim.change_date = PARSE_DATE('%F', LDG.changedate) 
  AND dim.on_sale_date = PARSE_DATE('%F', LDG.onsaledate) 
  AND dim.off_sale_date = PARSE_DATE('%F', LDG.offsaledate)
 WHERE cast(`{{params.gcp_project_id}}`.jwn_udf.iso8601_tmstp(LDG.LASTUPDATEDAT)AS TIMESTAMP)> DIM.DW_SOURCE_EVENT_TMSTP
GROUP BY LDG.sku,
 LDG.locationid;


BEGIN TRANSACTION;
--Delete the matching records
DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_dim AS target
WHERE EXISTS (SELECT *
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.rp_sku_loc_del_stg AS source
 WHERE LOWER(target.rms_sku_num) = LOWER(rms_sku_num)
  AND target.location_num = location_num
  AND target.change_date >= min_change_date);



--Update the effective dates to picked up so the changes will be considered in the VTW load
  UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_dim AS target SET
  eff_end_tmstp = CAST(CAST('9999-12-31 23:59:59.999999+00:' AS DATETIME) AS TIMESTAMP) FROM (SELECT dim.rms_sku_num,
   dim.location_num,
   MAX(dim.eff_begin_tmstp) AS max_eff_begin_tmstp,
   MAX(dim.eff_end_tmstp) AS max_eff_end_tmstp
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_dim AS dim
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.rp_sku_loc_del_stg AS del_stg 
  ON LOWER(dim.rms_sku_num) = LOWER(del_stg.rms_sku_num)
  AND dim.location_num = del_stg.location_num
  GROUP BY dim.rms_sku_num,dim.location_num
  HAVING CAST(max_eff_end_tmstp AS DATETIME) < CAST('9999-12-31 23:59:59' AS DATETIME)) AS SOURCE
  WHERE LOWER(target.rms_sku_num) = LOWER(SOURCE.rms_sku_num) 
  AND target.location_num = SOURCE.location_num 
  AND target.eff_begin_tmstp = SOURCE.max_eff_begin_tmstp 
  AND target.eff_end_tmstp = SOURCE.max_eff_end_tmstp 
  AND CAST(target.eff_end_tmstp AS DATETIME) <> CAST('9999-12-31 23:59:59' AS DATETIME);

--Purge work table for staging temporal rows
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_dim_vtw;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_dim_vtw
(
rms_sku_num
, location_num
, on_sale_date
, off_sale_date
, change_date
, eff_begin_tmstp
, eff_begin_tmstp_tz
, eff_end_tmstp
, eff_end_tmstp_tz
, dw_source_event_tmstp
, dw_source_event_tmstp_tz
)

WITH SRC AS 
(select 
rms_sku_num, 
location_num,
on_sale_date,
off_sale_date,
change_date,
dw_source_event_tmstp,
dw_source_event_tmstp_tz,
eff_begin_tmstp,
eff_end_tmstp
 from  (
    --inner normalize
            SELECT rms_sku_num, location_num,on_sale_date,off_sale_date,change_date,dw_source_event_tmstp,dw_source_event_tmstp_tz, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
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
  (SELECT rms_sku_num,
     location_num,
     on_sale_date,
     off_sale_date,
     change_date,
     eff_begin_tmstp,
     eff_end_tmstp,
     dw_source_event_tmstp,
    --  RANGE( eff_begin_tmstp, eff_end_tmstp ) AS eff_period,
     dw_source_event_tmstp_tz
FROM (SELECT
        rms_sku_num
      , location_num
      , on_sale_date
      , off_sale_date
      , change_date
      , eff_begin_tmstp
      , CAST(COALESCE(CAST(MIN(eff_begin_tmstp) OVER(PARTITION BY SRC1.rms_sku_num, SRC1.location_num  ORDER BY SRC1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1       FOLLOWING) AS TIMESTAMP)   ,TIMESTAMP'9999-12-31 23:59:59.999999+00:00')AS TIMESTAMP) AS  eff_end_tmstp
      , cast(SRC1.dw_source_event_tmstp as timestamp) as dw_source_event_tmstp
      , SRC1.dw_source_event_tmstp_tz
      FROM (SELECT DISTINCT sku AS rms_sku_num,
             locationid AS location_num,
             PARSE_DATE('%F', onsaledate) AS on_sale_date,
             PARSE_DATE('%F', offsaledate) AS off_sale_date,
             PARSE_DATE('%F', changedate) AS change_date,
             CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(PARSE_DATE('%F', changedate) AS DATETIME)) AS TIMESTAMP) AS eff_begin_tmstp,
             `{{params.gcp_project_id}}`.jwn_udf.iso8601_tmstp(lastupdatedat) AS dw_source_event_tmstp,
             `{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(lastupdatedat) as dw_source_event_tmstp_tz
            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_ldg
            WHERE CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(PARSE_DATE('%F', changedate) AS DATETIME)) AS DATETIME) IS NOT NULL
            AND offsaledate >= onsaledate) AS SRC1 ) SRC2
       WHERE eff_begin_tmstp < eff_end_tmstp )
                        ) AS ordered_data
) AS grouped_data
GROUP BY rms_sku_num, location_num,on_sale_date,off_sale_date,change_date,dw_source_event_tmstp,dw_source_event_tmstp_tz, range_group
ORDER BY  rms_sku_num, location_num, eff_begin_tmstp))


SELECT rms_sku_num,
 CAST(trunc(cast(location_num as float64)) AS INT64) location_num,
 on_sale_date,
 off_sale_date,
 change_date,
RANGE_START(eff_period) AS eff_begin_tmstp,
`{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(cast((RANGE_START(eff_period))as string)) as eff_begin_tmstp_tz,
RANGE_END(eff_period) AS eff_end_tmstp,
`{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(cast((RANGE_START(eff_period))as string)) as eff_end_tmstp_tz,
 dw_source_event_tmstp,
 dw_source_event_tmstp_tz
FROM 
 (
    --inner normalize
            SELECT rms_sku_num,  location_num, on_sale_date,off_sale_date,change_date, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp,eff_period,dw_source_event_tmstp,dw_source_event_tmstp_tz
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num, location_num, on_sale_date,off_sale_date,change_date ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM

(
        SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY rms_sku_num, location_num, on_sale_date,off_sale_date,change_date ORDER BY eff_begin_tmstp) >= 
                DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
FROM (SELECT DISTINCT SRC.rms_sku_num,
   SRC.location_num,
   SRC.on_sale_date,
   SRC.off_sale_date,
   SRC.change_date,
   SRC.dw_source_event_tmstp,
   SRC.dw_source_event_tmstp_tz,
   TGT.eff_end_tmstp,
   TGT.eff_begin_tmstp,
   COALESCE( RANGE_INTERSECT(RANGE (SRC.eff_begin_tmstp,SRC.eff_end_tmstp) , RANGE (TGT.eff_begin_tmstp,TGT.eff_end_tmstp)) ,RANGE (SRC.eff_begin_tmstp,SRC.eff_end_tmstp) ) AS eff_period,
  --  SRC.dw_source_event_tmstp,
  --  SRC.dw_source_event_tmstp_tz,
FROM SRC
   LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_dim AS tgt 
   ON LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) 
   AND CAST(SRC.location_num AS FLOAT64) = tgt.location_num
  WHERE tgt.location_num IS NULL
  OR (SRC.on_sale_date <> tgt.on_sale_date
   OR tgt.on_sale_date IS NULL AND SRC.on_sale_date IS NOT NULL
   OR SRC.on_sale_date IS NULL AND tgt.on_sale_date IS NOT NULL)
   OR (SRC.off_sale_date <> tgt.off_sale_date
   OR tgt.off_sale_date IS NULL AND SRC.off_sale_date IS NOT NULL
   OR SRC.off_sale_date IS NULL AND tgt.off_sale_date IS NOT NULL)
   OR (SRC.change_date <> tgt.change_date
   OR tgt.change_date IS NULL AND SRC.change_date IS NOT NULL
   OR SRC.change_date IS NULL AND tgt.change_date IS NOT NULL))
  AS ordered_data
)) AS grouped_data
GROUP BY rms_sku_num,location_num, on_sale_date,off_sale_date,change_date,eff_period,dw_source_event_tmstp,dw_source_event_tmstp_tz
ORDER BY  rms_sku_num,location_num, on_sale_date, eff_begin_tmstp);

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_dim AS tgt
WHERE EXISTS (SELECT 1
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_dim_vtw AS src
    WHERE LOWER(src.rms_sku_num) = LOWER(tgt.rms_sku_num) 
    AND src.location_num = tgt.location_num	
    AND src.eff_begin_tmstp <= tgt.eff_begin_tmstp
    AND src.eff_end_tmstp >= tgt.eff_end_tmstp);
	
UPDATE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_dim AS TGT
SET TGT.eff_end_tmstp = SRC.eff_begin_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_dim_vtw AS SRC
WHERE LOWER(src.rms_sku_num) = LOWER(tgt.rms_sku_num) 
AND src.location_num = tgt.location_num	
AND SRC.eff_begin_tmstp >= TGT.eff_begin_tmstp
AND SRC.eff_begin_tmstp < TGT.eff_end_tmstp 
AND SRC.eff_end_tmstp > TGT.eff_end_tmstp ;
	
UPDATE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_dim AS TGT
SET TGT.eff_end_tmstp = SRC.eff_begin_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_dim_vtw AS SRC
WHERE LOWER(src.rms_sku_num) = LOWER(tgt.rms_sku_num) 
AND src.location_num = tgt.location_num	
AND SRC.eff_begin_tmstp >= TGT.eff_begin_tmstp
AND SRC.eff_begin_tmstp < TGT.eff_end_tmstp 
AND SRC.eff_end_tmstp > TGT.eff_end_tmstp;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_dim (rms_sku_num, location_num, on_sale_date, off_sale_date, change_date,
 eff_begin_tmstp,eff_begin_tmstp_tz, eff_end_tmstp,eff_end_tmstp_tz, dw_source_event_tmstp, dw_source_event_tmstp_tz, dw_batch_id, dw_batch_date, dw_sys_load_tmstp)
(SELECT DISTINCT 
  rms_sku_num,
  location_num,
  on_sale_date,
  off_sale_date,
  change_date,
  eff_begin_tmstp,
  eff_begin_tmstp_tz,
  eff_end_tmstp,
  eff_end_tmstp_tz,
  dw_source_event_tmstp,
  dw_source_event_tmstp_tz,
  dw_batch_id,
  dw_batch_date,
  dw_sys_load_tmstp
 FROM (SELECT 
    rms_sku_num,
    location_num,
    on_sale_date,
    off_sale_date,
    change_date,
    eff_begin_tmstp,
    eff_begin_tmstp_tz,
    eff_end_tmstp,
    eff_end_tmstp_tz,
    dw_source_event_tmstp,
    dw_source_event_tmstp_tz,
    CAST(RPAD(FORMAT_TIMESTAMP('%Y%m%d%I%M%S', CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)), 14, ' ') AS BIGINT)
    AS dw_batch_id,
    CURRENT_DATE('PST8PDT') AS dw_batch_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_dim_vtw) AS t
 WHERE NOT EXISTS (SELECT 1 AS `A12180`
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_dim
   WHERE rms_sku_num = t.rms_sku_num
    AND location_num = t.location_num)
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, location_num)) = 1);
 
 COMMIT TRANSACTION;




