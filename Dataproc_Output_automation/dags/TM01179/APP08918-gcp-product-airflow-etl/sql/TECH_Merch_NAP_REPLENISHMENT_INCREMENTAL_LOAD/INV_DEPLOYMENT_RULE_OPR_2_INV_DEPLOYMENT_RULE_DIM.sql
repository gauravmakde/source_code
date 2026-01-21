
--Purge the XREF work table for staging temporal rows
-- NONSEQUENCED VALIDTIME
TRUNCATE TABLE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.inv_deployment_rule_dim_vtw;
--.IF ERRORCODE <> 0 THEN .QUIT 1

--Purge work table for future clean up
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.inv_deployment_rule_delete_stg ;      
--.IF ERRORCODE <> 0 THEN .QUIT 2

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.inv_deployment_rule_delete_stg
(
rms_sku_num,
channel_num,
eff_begin_tmstp,
eff_begin_tmstp_tz
)
SELECT
dim.rms_sku_num
,dim.channel_num
,dim.eff_begin_tmstp,
dim.eff_begin_tmstp_tz
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.inv_deployment_rule_dim DIM
JOIN
(
SELECT
op.rms_sku_num
,op.channel_num
,CAST(op.deployment_rule_start_date AS TIMESTAMP) AS deployment_rule_start_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.inv_deployment_rule_opr op
JOIN
(SELECT rmsskuid AS rms_sku_num
,CAST(FLOOR(CAST(orgchannelcode AS FLOAT64)) AS int64) AS channel_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.inv_deployment_rule_ldg
WHERE endtime IS NOT NULL
GROUP BY 1,2) ldg
ON op.rms_sku_num = ldg.rms_sku_num
AND ldg.channel_num = op.channel_num
WHERE LOWER(op.deployment_rule_cancelled_desc)= 'CANCELLED_FUTURE'
AND LOWER(op.deployment_rule_cancelled_ind)= 'Y'
GROUP BY 1,2,3) opr
ON dim.rms_sku_num = opr.rms_sku_num
AND dim.channel_num = opr.channel_num
WHERE dim.eff_begin_tmstp >= opr.deployment_rule_start_date ;

--.IF ERRORCODE <> 0 THEN .QUIT 3

-- Collect stats on the delete table
-- COLLECT STATISTICS COLUMN (RMS_SKU_NUM ,CHANNEL_NUM, EFF_BEGIN_TMSTP)
--        ON `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.INV_DEPLOYMENT_RULE_DELETE_STG;

-- BT;
--.IF ERRORCODE <> 0 THEN .QUIT 4

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.inv_deployment_rule_dim AS tgt
WHERE EXISTS (SELECT *
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.inv_deployment_rule_delete_stg AS src
 WHERE LOWER(src.rms_sku_num) = LOWER(tgt.rms_sku_num)
  AND src.channel_num = tgt.channel_num
  AND src.eff_begin_tmstp = tgt.eff_begin_tmstp);


--.IF ERRORCODE <> 0 THEN .QUIT 5

-- Reset the EFF_END_TMSTP of the deleted records to default date.
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.inv_deployment_rule_dim AS target SET
 eff_end_tmstp = TIMESTAMP'9999-12-31 23:59:59.999999+00:00' FROM (SELECT dim.rms_sku_num,
   dim.channel_num,
   MAX(dim.eff_begin_tmstp) AS max_eff_begin_tmstp,
   MAX(dim.eff_end_tmstp) AS max_eff_end_tmstp
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.inv_deployment_rule_dim AS dim
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.inv_deployment_rule_delete_stg AS pdel ON LOWER(dim.rms_sku_num) = LOWER(pdel.rms_sku_num) AND dim.channel_num = pdel.channel_num
  GROUP BY dim.rms_sku_num,
   dim.channel_num) AS SOURCE
WHERE LOWER(target.rms_sku_num) = LOWER(SOURCE.rms_sku_num) AND target.channel_num = SOURCE.channel_num AND target.eff_begin_tmstp = SOURCE.max_eff_begin_tmstp AND target.eff_end_tmstp = source.max_eff_end_tmstp;
--.IF ERRORCODE <> 0 THEN .QUIT 6



INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.inv_deployment_rule_dim_vtw
(   rms_sku_num
  , channel_num
  , deployment_type_code
  , deployment_rule_set_level_code
  , changed_by_user_id
  , eff_begin_tmstp
  , eff_begin_tmstp_tz
  , eff_end_tmstp
  , eff_end_tmstp_tz
)

WITH src AS
  (
    SELECT DISTINCT  --normalize
        rms_sku_num
      , channel_num
      , deployment_type_code
      , deployment_rule_set_level_code
      , changed_by_user_id
      , eff_begin_tmstp
      , eff_begin_tmstp_tz
      , eff_end_tmstp
      , eff_end_tmstp_tz
      , RANGE( eff_begin_tmstp, eff_end_tmstp ) AS eff_period
        FROM
    
(
    --inner normalize
            SELECT rms_sku_num,channel_num,deployment_type_code,deployment_rule_set_level_code,changed_by_user_id,
            MIN(eff_begin_tmstp_tz) AS eff_begin_tmstp_tz,MAX(eff_end_tmstp_tz) AS eff_end_tmstp_tz, 
            MIN(eff_begin_tmstp) AS eff_begin_tmstp, 
            MAX(eff_end_tmstp) AS eff_end_tmstp
            FROM 

    (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num,channel_num,deployment_type_code,deployment_rule_set_level_code,changed_by_user_id ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM

     ( SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY rms_sku_num,channel_num,deployment_type_code,deployment_rule_set_level_code,changed_by_user_id 
				ORDER BY eff_begin_tmstp) >= DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag  
    
    FROM (
	     SELECT 
         src1.rms_sku_num
        ,src1.channel_num
        ,src1.deployment_type_code
        ,src1.deployment_rule_set_level_code
        ,src1.changed_by_user_id
        ,src1.eff_begin_tmstp,
         `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE (CAST(src1.eff_begin_tmstp AS STRING)) AS eff_begin_tmstp_tz
	    ,CASE WHEN src1.deployment_rule_cancelled_ind = 'Y' THEN deployment_rule_cancelled_event_tmstp
	             ELSE MIN(src1.eff_begin_tmstp) OVER(PARTITION BY src1.rms_sku_num, src1.channel_num
	            ORDER BY src1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) END AS max_eff_end_tmstp
	    , COALESCE((CASE WHEN src1.deployment_rule_cancelled_ind = 'Y' THEN deployment_rule_cancelled_event_tmstp
	             ELSE MIN(src1.eff_begin_tmstp) OVER(PARTITION BY src1.rms_sku_num, src1.channel_num
	            ORDER BY src1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) END) ,TIMESTAMP'9999-12-31 23:59:59.999999+00:00') AS eff_end_tmstp,

        `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE (cast(COALESCE((CASE WHEN src1.deployment_rule_cancelled_ind = 'Y' THEN deployment_rule_cancelled_event_tmstp
	      ELSE MIN(src1.eff_begin_tmstp) OVER(PARTITION BY src1.rms_sku_num, src1.channel_num
	      ORDER BY src1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) END) ,TIMESTAMP'9999-12-31 23:59:59.999999+00:00') as string)) as 
        eff_end_tmstp_tz      

	     FROM(
			SELECT         
			src0.*, 
			CASE
			WHEN src_event_tmstp >= CAST(deployment_rule_start_tmstp AS TIMESTAMP) THEN src_event_tmstp
			ELSE CAST(deployment_rule_start_tmstp AS TIMESTAMP) END AS eff_begin_tmstp

			FROM
			(SELECT
			 op.rms_sku_num
			,op.channel_num
			,CAST(op.deployment_rule_start_date AS DATETIME) AS deployment_rule_start_tmstp
			,op.deployment_type_code
			,op.deployment_rule_set_level_code
			,op.changed_by_user_id
			,op.deployment_rule_cancelled_ind
			,op.deployment_rule_cancelled_event_tmstp
			,op.src_event_tmstp
			FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.inv_deployment_rule_opr OP
			JOIN
			(SELECT rmsskuid AS rms_sku_num
			,CAST(floor(cast(orgchannelcode as float64)) AS integer) AS channel_num
			-- ,`{{params.gcp_project_id}}`.nord_udf.epoch_tmstp(cast(floor(cast(endtime as float64)) as int64)) AS deployment_rule_cancelled_event_tmstp
			-- ,`{{params.gcp_project_id}}`.nord_udf.epoch_tmstp(cast(floor(cast(lastupdatedtime as float64)) as int64)) AS src_cancelled_event_tmstp
      ,`{{params.gcp_project_id}}`.NORD_UDF.ISO8601_TMSTP(endtime ) AS deployment_rule_cancelled_event_tmstp
			,`{{params.gcp_project_id}}`.NORD_UDF.ISO8601_TMSTP(lastupdatedtime) AS src_cancelled_event_tmstp
			FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.inv_deployment_rule_ldg
			GROUP BY 1,2,3,4) ldg
			ON op.rms_sku_num = ldg.rms_sku_num
			AND ldg.channel_num = op.channel_num
			WHERE LOWER(op.deployment_rule_cancelled_desc) <> 'CANCELLED_FUTURE'
			AND (cast(op.src_event_tmstp as string) >= ldg.src_cancelled_event_tmstp
			OR cast(op.deployment_rule_cancelled_event_tmstp as string) >= ldg.deployment_rule_cancelled_event_tmstp)
			GROUP BY 1,2,3,4,5,6,7,8,9
			) src0
		) src1	
    ) src2
    WHERE eff_begin_tmstp < eff_end_tmstp
  ) AS ordered_data --SRC 
    ) AS grouped_data
GROUP BY rms_sku_num,channel_num,deployment_type_code,deployment_rule_set_level_code,changed_by_user_id,range_group
ORDER BY rms_sku_num,channel_num,deployment_type_code,deployment_rule_set_level_code,changed_by_user_id, eff_begin_tmstp
))

SELECT
    nrml.rms_sku_num
  , nrml.channel_num
  , nrml.deployment_type_code
  , nrml.deployment_rule_set_level_code
  , nrml.changed_by_user_id
  , MIN(RANGE_START(nrml.eff_period)) AS eff_begin
  , `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(MIN(RANGE_START(nrml.eff_period)) AS STRING)) AS eff_begin_tmstp_tz
  , MAX(RANGE_END(nrml.eff_period))   AS eff_end
  , `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(MAX(RANGE_END(nrml.eff_period))AS STRING)) AS eff_end_tmstp_tz
FROM 

(

SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num,channel_num,deployment_type_code,deployment_rule_set_level_code,changed_by_user_id
				ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM

                (

SELECT *,CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY rms_sku_num,channel_num,deployment_type_code,deployment_rule_set_level_code,changed_by_user_id
 ORDER BY eff_begin_tmstp) >= DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
            FROM


(
  -- NONSEQUENCED VALIDTIME
  SELECT DISTINCT  --normalize
    src.rms_sku_num
  , src.channel_num
  , src.deployment_type_code
  , src.deployment_rule_set_level_code
  , src.changed_by_user_id
  , tgt.eff_begin_tmstp
  , tgt.eff_end_tmstp
  , tgt.eff_begin_tmstp_tz
  , tgt.eff_end_tmstp_tz
  , COALESCE( RANGE_INTERSECT(src.eff_period,RANGE(tgt.eff_begin_tmstp,tgt.eff_end_tmstp)),src.eff_period ) AS eff_period,
            tgt.eff_begin_tmstp_tz,
            tgt.eff_end_tmstp_tz
  FROM src
  
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.inv_deployment_rule_dim TGT
  ON src.rms_sku_num = tgt.rms_sku_num
  AND src.channel_num = tgt.channel_num
  AND  RANGE_OVERLAPS (src.eff_period,RANGE(tgt.eff_begin_tmstp, tgt.eff_end_tmstp))
  WHERE ( tgt.rms_sku_num IS NULL 
       OR (src.deployment_type_code <> tgt.deployment_type_code OR (src.deployment_type_code IS NOT NULL AND tgt.deployment_type_code IS NULL) OR (src.deployment_type_code IS NULL AND tgt.deployment_type_code IS NOT NULL))
       OR (src.deployment_rule_set_level_code <> tgt.deployment_rule_set_level_code OR (src.deployment_rule_set_level_code IS NOT NULL AND tgt.deployment_rule_set_level_code IS NULL) OR (src.deployment_rule_set_level_code IS NULL AND tgt.deployment_rule_set_level_code IS NOT NULL))
       OR (src.changed_by_user_id <> tgt.changed_by_user_id OR (src.changed_by_user_id IS NOT NULL AND tgt.changed_by_user_id IS NULL) OR (src.changed_by_user_id IS NULL AND tgt.changed_by_user_id IS NOT NULL))
       )
) AS ordered_data
                ) AS grouped_data
) nrml
GROUP BY rms_sku_num,channel_num,deployment_type_code,deployment_rule_set_level_code,changed_by_user_id,range_group
ORDER BY rms_sku_num,channel_num,deployment_type_code,deployment_rule_set_level_code,changed_by_user_id


;


--.IF ERRORCODE <> 0 THEN .QUIT 7

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.inv_deployment_rule_dim AS TGT
WHERE EXISTS (
  SELECT 1
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.inv_deployment_rule_dim_vtw AS SRC
 WHERE LOWER(src.rms_sku_num) = LOWER(tgt.rms_sku_num)
  AND src.channel_num = tgt.channel_num
  AND SRC.eff_begin_tmstp <= TGT.eff_begin_tmstp
  AND SRC.eff_end_tmstp >= TGT.eff_end_tmstp
);

UPDATE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.inv_deployment_rule_dim AS TGT
SET TGT.eff_end_tmstp = SRC.eff_begin_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.inv_deployment_rule_dim_vtw AS SRC
 WHERE LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num)
  AND SRC.channel_num = tgt.channel_num
  AND SRC.eff_begin_tmstp >= TGT.eff_begin_tmstp
  AND SRC.eff_begin_tmstp < TGT.eff_end_tmstp 
  AND SRC.eff_end_tmstp > TGT.eff_end_tmstp ;


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.inv_deployment_rule_dim AS TGT
SET TGT.eff_begin_tmstp = SRC.eff_end_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.inv_deployment_rule_dim_vtw AS SRC
 WHERE LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num)
  AND SRC.channel_num = tgt.channel_num
  AND SRC.eff_end_tmstp > TGT.eff_begin_tmstp
  AND SRC.eff_end_tmstp <= TGT.eff_end_tmstp;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.inv_deployment_rule_dim
(   rms_sku_num
  , channel_num
  , deployment_type_code
  , deployment_rule_set_level_code
  , changed_by_user_id
  , eff_begin_tmstp
  , eff_begin_tmstp_tz
  , eff_end_tmstp
  , eff_end_tmstp_tz
  , dw_batch_id
  , dw_batch_date
  , dw_sys_load_tmstp
)
SELECT
    rms_sku_num
  , channel_num
  , deployment_type_code
  , deployment_rule_set_level_code
  , changed_by_user_id
  , eff_begin_tmstp
  , eff_begin_tmstp_tz
  , eff_end_tmstp
  , eff_end_tmstp_tz
  , CAST(floor(cast(RPAD(FORMAT_TIMESTAMP('%Y%m%d%H%M%S', timestamp(current_datetime('PST8PDT'))),14, ' ') as float64)) AS INT64) AS dw_batch_id
  ,current_date('PST8PDT') AS dw_batch_date
  ,current_datetime('PST8PDT') AS dw_sys_load_tmstp
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.inv_deployment_rule_dim_vtw
  ;
