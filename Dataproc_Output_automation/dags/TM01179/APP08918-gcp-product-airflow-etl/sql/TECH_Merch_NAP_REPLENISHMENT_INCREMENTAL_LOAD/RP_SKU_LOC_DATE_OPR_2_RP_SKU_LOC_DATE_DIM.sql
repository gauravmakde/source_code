

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_date_dim_vtw;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_date_delete_stg;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_date_delete_stg (rms_sku_num, location_num, eff_begin_tmstp,eff_begin_tmstp_tz)
(SELECT dim.rms_sku_num,
  dim.location_num,
  dim.eff_begin_tmstp,
  dim.eff_begin_tmstp_tz
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_date_dim AS dim
  INNER JOIN (SELECT opr.rms_sku_num,
    opr.location_num,
    opr.on_sale_date,
    opr.off_sale_date,
    LDG.conv_src_event_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.rp_sku_loc_date_opr AS opr
    INNER JOIN (SELECT rmsskuid AS rms_sku_num,
      CAST(TRUNC(CAST(locationid AS FLOAT64)) AS INTEGER) AS conv_location_num,
      `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(lastupdatedtime) AS conv_src_event_tmstp
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_date_ldg
     GROUP BY rms_sku_num,
      conv_location_num,
      conv_src_event_tmstp) AS LDG 
	  ON LOWER(opr.rms_sku_num) = LOWER(LDG.rms_sku_num)
    AND opr.location_num = LDG.conv_location_num
    AND LOWER(opr.rp_date_set_cancelled_desc) = LOWER('CANCELLED_FUTURE')
    AND opr.rp_date_set_cancelled_event_tmstp >= CAST(LDG.conv_src_event_tmstp AS TIMESTAMP)) AS
  OPR ON LOWER(dim.rms_sku_num) = LOWER(OPR.rms_sku_num) 
  AND dim.location_num = OPR.location_num 
  AND dim.on_sale_date >= OPR.on_sale_date
 WHERE dim.eff_end_tmstp >= CAST(OPR.conv_src_event_tmstp AS TIMESTAMP)
 GROUP BY dim.rms_sku_num,
  dim.location_num,
  dim.eff_begin_tmstp,
  dim.eff_begin_tmstp_tz);
  

BEGIN TRANSACTION;

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_date_dim AS tgt
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_date_delete_stg AS src
    WHERE LOWER(rms_sku_num) = LOWER(tgt.rms_sku_num) 
	AND location_num = tgt.location_num 
	AND eff_begin_tmstp = tgt.eff_begin_tmstp);
	
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_date_dim AS target 
SET eff_end_tmstp = CAST(CAST('9999-12-31 23:59:59.999999+00:' AS DATETIME) AS TIMESTAMP) FROM (SELECT dim.rms_sku_num, 
			 dim.location_num, 
			 MAX(dim.eff_begin_tmstp) AS max_eff_begin_tmstp, 
			 MAX(dim.eff_end_tmstp) AS max_eff_end_tmstp
	  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_date_dim AS dim
      INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_date_delete_stg AS pdel 
	  ON LOWER(dim.rms_sku_num) = LOWER(pdel.rms_sku_num) 
	  AND dim.location_num = pdel.location_num
      GROUP BY dim.rms_sku_num, dim.location_num) AS SOURCE
WHERE LOWER(target.rms_sku_num) = LOWER(SOURCE.rms_sku_num) 
AND target.location_num = SOURCE.location_num 
AND target.eff_begin_tmstp = SOURCE.max_eff_begin_tmstp 
AND target.eff_end_tmstp = SOURCE.max_eff_end_tmstp;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_date_dim_vtw (
rms_sku_num, 
location_num, 
on_sale_date, 
off_sale_date,
eff_begin_tmstp,
eff_end_tmstp,
eff_begin_tmstp_tz,
eff_end_tmstp_tz)

WITH SRC AS(
    SELECT  --NORMALIZE 
		  RMS_SKU_NUM
      , LOCATION_NUM
      , ON_SALE_DATE
      , OFF_SALE_DATE
      , RANGE( EFF_BEGIN_TMSTP, EFF_END_TMSTP ) AS EFF_PERIOD
	  , eff_begin_tmstp_tz
	  , `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(eff_end_tmstp AS STRING)) as eff_end_tmstp_tz
	  
   from  (
    --inner normalize
            SELECT rms_sku_num, location_num,on_sale_date,off_sale_date,eff_begin_tmstp_tz, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
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
         from(
		SELECT SRC1.RMS_SKU_NUM
        ,SRC1.LOCATION_NUM
        ,SRC1.ON_SALE_DATE
		    ,SRC1.OFF_SALE_DATE
        ,SRC1.EFF_BEGIN_TMSTP
		    ,`{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(SRC1.EFF_BEGIN_TMSTP AS STRING)) as eff_begin_tmstp_tz
	      , COALESCE(MIN(SRC1.EFF_BEGIN_TMSTP) OVER(PARTITION BY SRC1.RMS_SKU_NUM, SRC1.LOCATION_NUM
	            ORDER BY SRC1.EFF_BEGIN_TMSTP ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) ,TIMESTAMP'9999-12-31 23:59:59.999999+00:00') AS EFF_END_TMSTP,
     FROM (SELECT 
	    op.rms_sku_num,
        op.location_num,
        op.on_sale_date,
        op.off_sale_date,
		op.src_event_tmstp AS eff_begin_tmstp,
		conv_src_event_tmstp
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.rp_sku_loc_date_opr AS op
       INNER JOIN (SELECT rmsskuid AS rms_sku_num,
						  CAST(TRUNC(CAST(locationid AS FLOAT64)) AS INTEGER) AS location_num,
						  `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(lastupdatedtime) AS conv_src_event_tmstp
				   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_date_ldg
				   QUALIFY (ROW_NUMBER() OVER (PARTITION BY rmsskuid, CAST(TRUNC(CAST(locationid AS FLOAT64)) AS INTEGER) ORDER BY `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(lastupdatedtime ))) = 1) AS t0 
	   ON LOWER(op.rms_sku_num) = LOWER(t0.rms_sku_num) 
	   AND op.location_num = t0.location_num 
	   AND op.src_event_tmstp >= CAST(t0.conv_src_event_tmstp AS TIMESTAMP)
       WHERE LOWER(op.rp_date_set_cancelled_desc) <> LOWER('CANCELLED_FUTURE')
         ) AS SRC1
		 ) AS SRC2
	    WHERE EFF_BEGIN_TMSTP < EFF_END_TMSTP
)  AS ordered_data
) AS grouped_data
GROUP BY rms_sku_num, location_num,on_sale_date,off_sale_date,eff_begin_tmstp_tz, range_group
ORDER BY  rms_sku_num, location_num, eff_begin_tmstp ))



SELECT DISTINCT 
rms_sku_num,
location_num,
on_sale_date,
off_sale_date,
RANGE_START(EFF_PERIOD) AS EFF_BEGIN,
RANGE_END(EFF_PERIOD)   AS EFF_END,
eff_begin_tmstp_tz,
eff_end_tmstp_tz
 FROM 
 (
    --inner normalize
            SELECT rms_sku_num,  location_num, on_sale_date,off_sale_date, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp,eff_period,eff_begin_tmstp_tz,eff_end_tmstp_tz
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num, location_num, on_sale_date,off_sale_date ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM

(
        SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY rms_sku_num, location_num, on_sale_date,off_sale_date ORDER BY eff_begin_tmstp) >= 
                DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
FROM (SELECT --NORMALIZE
    SRC.rms_sku_num,
    SRC.location_num,
    SRC.on_sale_date,
    SRC.off_sale_date,
    tgt.eff_end_tmstp,
    tgt.eff_begin_tmstp,
	COALESCE( RANGE_INTERSECT(SRC.EFF_PERIOD,RANGE(TGT.EFF_BEGIN_TMSTP,TGT.EFF_END_TMSTP)) , SRC.EFF_PERIOD ) AS EFF_PERIOD,
	SRC.eff_begin_tmstp_tz,
	SRC.eff_end_tmstp_tz
   FROM SRC 
      LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_date_dim AS tgt 
	  ON LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) 
	  AND SRC.location_num = tgt.location_num
	  AND RANGE_OVERLAPS(SRC.EFF_PERIOD,RANGE(TGT.EFF_BEGIN_TMSTP, TGT.EFF_END_TMSTP))
     WHERE (tgt.rms_sku_num IS NULL
      OR (SRC.on_sale_date <> tgt.on_sale_date
      OR (tgt.on_sale_date IS NULL AND SRC.on_sale_date IS NOT NULL)
      OR (SRC.on_sale_date IS NULL AND tgt.on_sale_date IS NOT NULL))
      OR (SRC.off_sale_date <> tgt.off_sale_date
      OR (tgt.off_sale_date IS NULL AND SRC.off_sale_date IS NOT NULL)
      OR (SRC.off_sale_date IS NULL AND tgt.off_sale_date IS NOT NULL)))
  ) AS ordered_data
)) AS grouped_data
GROUP BY rms_sku_num,location_num, on_sale_date,off_sale_date,eff_period,eff_begin_tmstp_tz,eff_end_tmstp_tz
ORDER BY  rms_sku_num,location_num, on_sale_date, eff_begin_tmstp)
  ;



DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_date_dim AS tgt
WHERE EXISTS (SELECT 1
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_date_dim_vtw AS src
    WHERE LOWER(rms_sku_num) = LOWER(tgt.rms_sku_num) 
	AND location_num = tgt.location_num
    AND src.eff_begin_tmstp <= tgt.eff_begin_tmstp
    AND src.eff_end_tmstp >= tgt.eff_end_tmstp);
	
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_date_dim AS TGT
SET TGT.eff_end_tmstp = SRC.eff_begin_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_date_dim_vtw AS SRC
WHERE LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) 
AND SRC.location_num = tgt.location_num
AND SRC.eff_begin_tmstp >= TGT.eff_begin_tmstp
AND SRC.eff_begin_tmstp < TGT.eff_end_tmstp 
AND SRC.eff_end_tmstp > TGT.eff_end_tmstp ;
	
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_date_dim AS TGT
SET TGT.eff_begin_tmstp = SRC.eff_end_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_date_dim_vtw AS SRC
WHERE LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) 
AND SRC.location_num = tgt.location_num
AND SRC.eff_end_tmstp > TGT.eff_begin_tmstp
AND SRC.eff_end_tmstp <= TGT.eff_end_tmstp;


	
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_date_dim (
rms_sku_num, 
location_num, 
on_sale_date, 
off_sale_date, 
eff_begin_tmstp,
eff_begin_tmstp_tz,
eff_end_tmstp,
eff_end_tmstp_tz,
dw_batch_id, 
dw_batch_date, 
dw_sys_load_tmstp)
(SELECT DISTINCT rms_sku_num,
  location_num,
  on_sale_date,
  off_sale_date,
  eff_begin_tmstp,
  eff_begin_tmstp_tz,
  eff_end_tmstp,
  eff_end_tmstp_tz,
  dw_batch_id,
  dw_batch_date,
  dw_sys_load_tmstp
 FROM (SELECT rms_sku_num,
    location_num,
    on_sale_date,
    off_sale_date,
    eff_begin_tmstp,
	eff_begin_tmstp_tz,
    eff_end_tmstp,
	eff_end_tmstp_tz,
    CAST(RPAD(FORMAT_TIMESTAMP('%Y%m%d%I%M%S', CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)), 14, ' ') AS BIGINT)
    AS dw_batch_id,
    CURRENT_DATE('PST8PDT') AS dw_batch_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_date_dim_vtw) AS t
 WHERE NOT EXISTS (SELECT 1 AS `A12180`
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_date_dim
   WHERE rms_sku_num = t.rms_sku_num
    AND location_num = t.location_num)
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, location_num)) = 1);
COMMIT TRANSACTION;
