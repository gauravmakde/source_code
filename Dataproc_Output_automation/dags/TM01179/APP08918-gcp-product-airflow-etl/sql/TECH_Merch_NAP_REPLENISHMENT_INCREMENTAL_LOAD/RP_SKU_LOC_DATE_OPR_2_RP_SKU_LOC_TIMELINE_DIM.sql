DECLARE iterations INT64 DEFAULT 0;

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.elt_control 
SET batch_id = 1
WHERE LOWER(subject_area_nm) = LOWER('NAP_RP_OSOS_DLY_LOAD') 
AND (SELECT CASE WHEN MAX(cnt) > 1 
				 THEN CAST(ERROR('Cannot insert duplicate values') AS BIGINT) 
				 ELSE MAX(cnt) END AS cnt
            FROM (SELECT subject_area_nm, active_load_ind, curr_batch_date, extract_from_tmstp, extract_to_tmstp, batch_start_tmstp, batch_end_tmstp, 
			COUNT(1) AS cnt
                    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.elt_control
                    WHERE LOWER(subject_area_nm) = LOWER('NAP_RP_OSOS_DLY_LOAD')
                    GROUP BY subject_area_nm, active_load_ind, curr_batch_date, extract_from_tmstp, extract_to_tmstp, batch_start_tmstp, batch_end_tmstp) AS t1) < 2;


BEGIN TRANSACTION;

REPEAT

SET iterations = iterations + 1;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_timeline_dim_vtw;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_timeline_delete_stg;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_timeline_delete_stg 
(rms_sku_num, 
location_num, 
eff_begin_tmstp,
eff_begin_tmstp_tz)
(SELECT dim.rms_sku_num,
  dim.location_num,
  dim.eff_begin_tmstp,
  dim.eff_begin_tmstp_tz
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_timeline_dim AS dim
  INNER JOIN (SELECT opr.rms_sku_num,
    opr.location_num,
    opr.on_sale_date,
    LDG.conv_src_event_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.rp_sku_loc_date_opr AS opr
    INNER JOIN (SELECT rmsskuid AS rms_sku_num,
      CAST(locationid AS INTEGER) AS conv_location_num,
      `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(lastupdatedtime) AS conv_src_event_tmstp
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.rp_sku_loc_date_dly_vw
     GROUP BY rms_sku_num,
      conv_location_num,
      conv_src_event_tmstp
      ) AS LDG ON TRUE
   WHERE LOWER(opr.rms_sku_num) = LOWER(LDG.rms_sku_num)
    AND opr.location_num = LDG.conv_location_num
    AND LOWER(opr.rp_date_set_cancelled_desc) = LOWER('CANCELLED_FUTURE')
    AND opr.rp_date_set_cancelled_event_tmstp >= CAST(LDG.conv_src_event_tmstp AS TIMESTAMP)
    ) 
    AS OPR ON LOWER(dim.rms_sku_num) = LOWER(OPR.rms_sku_num) 
  AND dim.location_num = OPR.location_num 
  AND CAST(dim.eff_begin_tmstp AS DATETIME) >= CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(OPR.on_sale_date AS DATETIME)) AS DATETIME)
 WHERE dim.eff_end_tmstp >= CAST(OPR.conv_src_event_tmstp AS TIMESTAMP)
 GROUP BY dim.rms_sku_num,
  dim.location_num,
  dim.eff_begin_tmstp,
  dim.eff_begin_tmstp_tz);
  
DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_timeline_dim AS tgt
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_timeline_delete_stg AS src
    WHERE LOWER(rms_sku_num) = LOWER(tgt.rms_sku_num) 
    AND location_num = tgt.location_num 
    AND eff_begin_tmstp = tgt.eff_begin_tmstp);
	
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_timeline_dim AS target 
SET eff_end_tmstp = CAST('9999-12-31 23:59:59.999999+00:00' AS TIMESTAMP),
eff_end_tmstp_tz = '+00:00'
FROM (SELECT dim.rms_sku_num, dim.location_num, MAX(dim.eff_begin_tmstp) AS max_eff_begin_tmstp, MAX(dim.eff_end_tmstp) AS max_eff_end_tmstp
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_timeline_dim AS dim
            INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_timeline_delete_stg AS pdel 
			ON LOWER(dim.rms_sku_num) = LOWER(pdel.rms_sku_num) 
			AND dim.location_num = pdel.location_num
        GROUP BY dim.rms_sku_num, dim.location_num) AS SOURCE
WHERE LOWER(target.rms_sku_num) = LOWER(SOURCE.rms_sku_num) 
AND target.location_num = SOURCE.location_num 
AND target.eff_begin_tmstp = SOURCE.max_eff_begin_tmstp 
AND target.eff_end_tmstp = SOURCE.max_eff_end_tmstp;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_timeline_dim_vtw 
(rms_sku_num, 
location_num, 
is_on_sale_ind,
eff_begin_tmstp,
eff_end_tmstp,
eff_begin_tmstp_tz,
eff_end_tmstp_tz )

WITH SRC AS (
    SELECT 
        rms_sku_num,
				location_num,
				is_on_sale_ind,
				RANGE( eff_begin_tmstp, eff_end_tmstp ) AS eff_period,
        eff_begin_tmstp_tz,
        `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(eff_end_tmstp AS STRING)) as eff_end_tmstp_tz
	FROM  (
    --inner normalize
            SELECT rms_sku_num, location_num,is_on_sale_ind,eff_begin_tmstp_tz, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
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
         FROM (
	     SELECT src1.rms_sku_num
        ,src1.location_num
        ,src1.is_on_sale_ind
        ,src1.eff_begin_tmstp
	    , COALESCE(MIN(src1.eff_begin_tmstp) OVER(PARTITION BY src1.rms_sku_num, src1.location_num
	            ORDER BY src1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) ,TIMESTAMP'9999-12-31 23:59:59.999999+00:00') AS EFF_END_TMSTP,
               `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(eff_begin_tmstp AS STRING)) as eff_begin_tmstp_tz
     FROM (SELECT op.rms_sku_num,
				  op.location_num,
				  'Y' AS is_on_sale_ind,
				   
         CAST(CASE
           WHEN CAST(DATETIME(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(op.off_sale_date AS TIMESTAMP)) AS TIMESTAMP),'America/Los_Angeles') AS TIMESTAMP) >= CAST(t0.conv_src_event_tmstp AS TIMESTAMP) 
		   AND CAST(DATETIME(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(op.on_sale_date AS TIMESTAMP)) AS TIMESTAMP),'America/Los_Angeles') AS TIMESTAMP) <= CAST(t0.conv_src_event_tmstp AS TIMESTAMP)
           THEN CAST(t0.conv_src_event_tmstp AS TIMESTAMP)
           WHEN CAST(DATETIME(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(op.off_sale_date AS TIMESTAMP)) AS TIMESTAMP),'America/Los_Angeles') AS TIMESTAMP) > CAST(t0.conv_src_event_tmstp AS TIMESTAMP) 
		   AND CAST(DATETIME(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(op.on_sale_date AS TIMESTAMP)) AS TIMESTAMP),'America/Los_Angeles') AS TIMESTAMP) > CAST(t0.conv_src_event_tmstp AS TIMESTAMP)
           THEN CAST(DATETIME(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(op.on_sale_date AS TIMESTAMP)) AS TIMESTAMP),'America/Los_Angeles') AS TIMESTAMP)
           ELSE NULL
           END AS TIMESTAMP) AS eff_begin_tmstp
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.rp_sku_loc_date_opr AS op
         INNER JOIN (SELECT rmsskuid AS rms_sku_num,
           CAST(TRUNC(CAST(locationid AS FLOAT64)) AS INTEGER) AS location_num,
           `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(lastupdatedtime) AS conv_src_event_tmstp
          FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.rp_sku_loc_date_dly_vw
          QUALIFY (ROW_NUMBER() OVER (PARTITION BY rmsskuid, CAST(TRUNC(CAST(locationid AS FLOAT64)) AS INTEGER) ORDER BY `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(lastupdatedtime))) = 1) AS t0 
		  ON LOWER(op.rms_sku_num) = LOWER(t0.rms_sku_num) 
		  AND op.location_num = t0.location_num 
		  AND op.src_event_tmstp >= CAST(t0.conv_src_event_tmstp AS
            TIMESTAMP)
        WHERE LOWER(op.rp_date_set_cancelled_desc) <> LOWER('CANCELLED_FUTURE')
         AND op.on_sale_date <> op.off_sale_date
         AND CASE
           WHEN CAST(DATETIME(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(op.off_sale_date AS TIMESTAMP)) AS TIMESTAMP),'America/Los_Angeles') AS TIMESTAMP) >= CAST(t0.conv_src_event_tmstp AS TIMESTAMP) 
		   AND CAST(DATETIME(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(op.on_sale_date AS TIMESTAMP)) AS TIMESTAMP),'America/Los_Angeles') AS TIMESTAMP) <= CAST(t0.conv_src_event_tmstp AS TIMESTAMP)
           THEN CAST(t0.conv_src_event_tmstp AS TIMESTAMP)
           WHEN CAST(DATETIME(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(op.off_sale_date AS TIMESTAMP)) AS TIMESTAMP),'America/Los_Angeles') AS TIMESTAMP) > CAST(t0.conv_src_event_tmstp AS TIMESTAMP) 
		   AND CAST(DATETIME(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(op.on_sale_date AS TIMESTAMP)) AS TIMESTAMP),'America/Los_Angeles') AS TIMESTAMP) > CAST(t0.conv_src_event_tmstp AS TIMESTAMP)
           THEN CAST(DATETIME(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(op.on_sale_date AS TIMESTAMP)) AS TIMESTAMP),'America/Los_Angeles') AS TIMESTAMP)
           ELSE NULL
           END IS NOT NULL
        UNION ALL
        SELECT op0.rms_sku_num,
         op0.location_num,
         'N' AS is_on_sale_ind,
          CASE
          WHEN op0.off_sale_date = DATE '2099-12-31'
          THEN cast('9999-12-31 23:59:59.999999+00:00' as timestamp)
          ELSE CAST(DATETIME(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(op0.off_sale_date AS DATETIME)) AS TIMESTAMP),'America/Los_Angeles') AS TIMESTAMP)
          END AS eff_begin_tmstp
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.rp_sku_loc_date_opr AS op0
         INNER JOIN (SELECT rmsskuid AS rms_sku_num,
           CAST(TRUNC(CAST(locationid AS FLOAT64)) AS INTEGER) AS location_num,
           `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(lastupdatedtime) AS conv_src_event_tmstp
          FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.rp_sku_loc_date_dly_vw
          QUALIFY (ROW_NUMBER() OVER (PARTITION BY rmsskuid, CAST(TRUNC(CAST(locationid AS FLOAT64)) AS INTEGER) ORDER BY `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(lastupdatedtime))) = 1) AS t4 
		  ON LOWER(op0.rms_sku_num) = LOWER(t4.rms_sku_num) 
		  AND op0.location_num = t4.location_num 
		  AND op0.src_event_tmstp >= CAST(t4.conv_src_event_tmstp AS
            TIMESTAMP)
        WHERE LOWER(op0.rp_date_set_cancelled_desc) <> LOWER('CANCELLED_FUTURE')
        UNION ALL
        SELECT op1.rms_sku_num,
         op1.location_num,
         'Y' AS is_on_sale_ind,
         CAST(DATETIME(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(op1.on_sale_date AS DATETIME)) AS TIMESTAMP),'America/Los_Angeles') AS TIMESTAMP) AS eff_begin_tmstp
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.rp_sku_loc_date_opr AS op1
         INNER JOIN (SELECT rmsskuid AS rms_sku_num,
           CAST(TRUNC(CAST(locationid AS FLOAT64)) AS INTEGER) AS location_num,
           `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(lastupdatedtime) AS conv_src_event_tmstp
          FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.rp_sku_loc_date_dly_vw
          QUALIFY (ROW_NUMBER() OVER (PARTITION BY rmsskuid, CAST(TRUNC(CAST(locationid AS FLOAT64)) AS INTEGER) ORDER BY `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(lastupdatedtime))) = 1) AS t8 
		  ON LOWER(op1.rms_sku_num) = LOWER(t8.rms_sku_num) 
		  AND op1.location_num = t8.location_num 
		  AND op1.src_event_tmstp >= CAST(t8.conv_src_event_tmstp AS
            TIMESTAMP)
         LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_timeline_dim AS dim 
		 ON LOWER(op1.rms_sku_num) = LOWER(dim.rms_sku_num) 
		 AND t8.location_num = dim.location_num 
		 AND CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(op1.on_sale_date AS DATETIME)) AS DATETIME) = CAST(dim.eff_begin_tmstp AS DATETIME)
        WHERE LOWER(op1.rp_date_set_cancelled_desc) <> LOWER('CANCELLED_FUTURE')
         AND op1.on_sale_date <> op1.off_sale_date
         AND dim.rms_sku_num IS NULL) AS src1
		 ) AS SRC2
     WHERE EFF_BEGIN_TMSTP < EFF_END_TMSTP
)  AS ordered_data
) AS grouped_data
GROUP BY rms_sku_num, location_num,is_on_sale_ind,eff_begin_tmstp_tz, range_group
ORDER BY  rms_sku_num, location_num, eff_begin_tmstp ))




(SELECT DISTINCT 
rms_sku_num,
location_num,
is_on_sale_ind,
RANGE_START(eff_period) AS eff_begin,
RANGE_END(eff_period)   AS eff_end,
eff_begin_tmstp_tz,
eff_end_tmstp_tz 
FROM 
 (
    --inner normalize
            SELECT rms_sku_num,  location_num, is_on_sale_ind, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp,eff_period,eff_begin_tmstp_tz,eff_end_tmstp_tz
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num, location_num, is_on_sale_ind ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM

(
        SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY rms_sku_num, location_num, is_on_sale_ind ORDER BY eff_begin_tmstp) >= 
                DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
    FROM (SELECT 
        SRC.rms_sku_num,
			  SRC.location_num,
			  SRC.is_on_sale_ind,
        TGT.eff_end_tmstp,
        TGT.eff_begin_tmstp,
			  COALESCE( RANGE_INTERSECT(SRC.EFF_PERIOD,RANGE(TGT.EFF_BEGIN_TMSTP,TGT.EFF_END_TMSTP))
            , SRC.EFF_PERIOD ) AS EFF_PERIOD
            ,SRC.eff_begin_tmstp_tz
            ,SRC.eff_end_tmstp_tz
   FROM SRC
	 
    LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_timeline_dim AS tgt ON LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) 
	AND SRC.location_num = tgt.location_num
	AND RANGE_OVERLAPS(SRC.EFF_PERIOD,RANGE(TGT.EFF_BEGIN_TMSTP, TGT.EFF_END_TMSTP))
   WHERE NOT EXISTS (SELECT 1 AS `A12180`
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_timeline_dim_vtw
     WHERE LOWER(rms_sku_num) = LOWER(SRC.rms_sku_num)
      AND location_num = SRC.location_num)) AS t18
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY t18.rms_sku_num, t18.location_num)) = 1)
  AS ordered_data
) AS grouped_data
GROUP BY rms_sku_num,location_num, is_on_sale_ind,eff_period,eff_begin_tmstp_tz,eff_end_tmstp_tz
ORDER BY  rms_sku_num,location_num, is_on_sale_ind, eff_begin_tmstp ))
 ;

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_timeline_dim  AS tgt
WHERE EXISTS (SELECT 1
    FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_timeline_dim_vtw AS src
    WHERE LOWER(src.rms_sku_num) = LOWER(tgt.rms_sku_num) 
    AND src.location_num = tgt.location_num
    AND src.eff_begin_tmstp <= tgt.eff_begin_tmstp
    AND src.eff_end_tmstp >= tgt.eff_end_tmstp);
	
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_timeline_dim  AS TGT
SET TGT.eff_end_tmstp = SRC.eff_begin_tmstp
FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_timeline_dim_vtw AS SRC
WHERE LOWER(src.rms_sku_num) = LOWER(tgt.rms_sku_num) 
AND src.location_num = tgt.location_num
AND SRC.eff_begin_tmstp >= TGT.eff_begin_tmstp
AND SRC.eff_begin_tmstp < TGT.eff_end_tmstp 
AND SRC.eff_end_tmstp > TGT.eff_end_tmstp ;
	
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_timeline_dim  AS TGT
SET TGT.eff_begin_tmstp = SRC.eff_end_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_timeline_dim_vtw AS SRC
WHERE LOWER(src.rms_sku_num) = LOWER(tgt.rms_sku_num) 
AND src.location_num = tgt.location_num
AND SRC.eff_end_tmstp > TGT.eff_begin_tmstp
AND SRC.eff_end_tmstp <= TGT.eff_end_tmstp;
	

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_timeline_dim 
(rms_sku_num, 
location_num, 
is_on_sale_ind, 
eff_begin_tmstp,
eff_end_tmstp,
eff_begin_tmstp_tz,
eff_end_tmstp_tz,
dw_batch_id, 
dw_batch_date, 
dw_sys_load_tmstp)
(SELECT DISTINCT 
rms_sku_num,
  location_num,
  is_on_sale_ind,
  eff_begin_tmstp,
  eff_end_tmstp,
  eff_begin_tmstp_tz,
  eff_end_tmstp_tz,
  dw_batch_id,
  dw_batch_date,
  dw_sys_load_tmstp
 FROM (SELECT rms_sku_num,
    location_num,
    is_on_sale_ind,
    eff_begin_tmstp,
    eff_end_tmstp,
	eff_begin_tmstp_tz,
    eff_end_tmstp_tz,
    CAST(RPAD(FORMAT_TIMESTAMP('%Y%m%d%I%M%S', CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)), 14, ' ') AS BIGINT)
    AS dw_batch_id,
    CURRENT_DATE('PST8PDT') AS dw_batch_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_timeline_dim_vtw) AS t
 WHERE NOT EXISTS (SELECT 1 AS `A12180`
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_timeline_dim
   WHERE LOWER(rms_sku_num) = LOWER(t.rms_sku_num)
    AND location_num = t.location_num)
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, location_num)) = 1);
 
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.elt_control 
SET batch_id = elt_control.batch_id + 1
WHERE LOWER(subject_area_nm) = LOWER('NAP_RP_OSOS_DLY_LOAD') 
AND (SELECT CASE WHEN MAX(cnt) > 1 
				 THEN CAST(ERROR('Cannot insert duplicate values') AS BIGINT) 
				 ELSE MAX(cnt) END AS cnt
            FROM (SELECT subject_area_nm, active_load_ind, curr_batch_date, extract_from_tmstp, extract_to_tmstp, batch_start_tmstp, batch_end_tmstp, 
				 COUNT(1) AS cnt
                    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.elt_control
                    WHERE LOWER(subject_area_nm) = LOWER('NAP_RP_OSOS_DLY_LOAD')
                    GROUP BY subject_area_nm, active_load_ind, curr_batch_date, extract_from_tmstp, extract_to_tmstp, batch_start_tmstp, batch_end_tmstp) AS t1) < 2;

UNTIL iterations >= {{params.DB_OSOS_ITERATIONS}}
END REPEAT;
                    
COMMIT TRANSACTION;



