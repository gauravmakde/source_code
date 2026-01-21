
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_indicator_dtl_xref_vtw;

BEGIN TRANSACTION;


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_indicator_dtl_xref
WHERE (rms_sku_num, location_num) IN (SELECT (rms_sku_num, location_num)
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_indicator_dtl_ldg
        GROUP BY rms_sku_num, location_num);


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_indicator_dtl_xref_vtw
(
  rms_sku_num
, location_num
, is_replenishment_eligible_ind
, replenishment_setting_type_code
, replenishment_setting_value_desc
, on_sale_date
, off_sale_date
, eff_begin_tmstp
, eff_begin_tmstp_tz
, eff_end_tmstp
, eff_end_tmstp_tz
)


WITH src AS
(
SELECT DISTINCT
-- NORMALIZE
 rms_sku_num
,location_num
,is_replenishment_eligible_ind
,replenishment_setting_type_code
,replenishment_setting_value_desc
,on_sale_date
,off_sale_date
,eff_begin_tmstp
,eff_begin_tmstp_tz
,eff_end_tmstp
,eff_end_tmstp_tz
,RANGE( eff_begin_tmstp, eff_end_tmstp ) AS eff_period
FROM
(
 SELECT rms_sku_num,location_num,is_replenishment_eligible_ind,replenishment_setting_type_code,replenishment_setting_value_desc,on_sale_date,off_sale_date,
 MIN(eff_begin_tmstp_tz) AS eff_begin_tmstp_tz,MAX(eff_end_tmstp_tz) AS eff_end_tmstp_tz, 
 MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
            FROM  
(
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num,location_num,is_replenishment_eligible_ind,replenishment_setting_type_code,replenishment_setting_value_desc,on_sale_date,off_sale_date ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM
( SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY rms_sku_num,location_num,is_replenishment_eligible_ind,replenishment_setting_type_code,replenishment_setting_value_desc,on_sale_date,off_sale_date 
				ORDER BY eff_begin_tmstp) >= DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag 
FROM (
SELECT
  src1.rms_sku_num
, src1.location_num
, src1.is_replenishment_eligible_ind
, src1.replenishment_setting_type_code
, src1.replenishment_setting_value_desc
, src1.on_sale_date
, src1.off_sale_date
, src1.eff_begin_tmstp
, `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(src1.eff_begin_tmstp as string)) as eff_begin_tmstp_tz
, (COALESCE(MIN(src1.eff_begin_tmstp) OVER(PARTITION BY src1.rms_sku_num, src1.location_num
 ORDER BY src1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
,TIMESTAMP'9999-12-31 23:59:59.999999+00:00') )AS  eff_end_tmstp
,`{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(COALESCE(MIN(src1.eff_begin_tmstp) OVER(PARTITION BY src1.rms_sku_num, src1.location_num
 ORDER BY src1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
,TIMESTAMP'9999-12-31 23:59:59.999999+00:00') as string)) as eff_end_tmstp_tz
FROM (
SELECT 
  rms_sku_num
, location_num
, is_replenishment_eligible_ind
, replenishment_setting_type_code
, replenishment_setting_value_desc
, on_sale_date
, off_sale_date
, eff_begin_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_indicator_dtl_ldg
GROUP BY 1,2,3,4,5,6,7,8
) src1
) src2
WHERE eff_begin_tmstp < eff_end_tmstp
) AS ordered_data
) AS grouped_data
GROUP BY rms_sku_num,location_num,is_replenishment_eligible_ind,replenishment_setting_type_code,replenishment_setting_value_desc,on_sale_date,off_sale_date, range_group
ORDER BY rms_sku_num,location_num,is_replenishment_eligible_ind,replenishment_setting_type_code,replenishment_setting_value_desc,on_sale_date,off_sale_date, eff_begin_tmstp

))





SELECT
  nrml.rms_sku_num
, nrml.location_num
, nrml.is_replenishment_eligible_ind
, nrml.replenishment_setting_type_code
, nrml.replenishment_setting_value_desc
, nrml.on_sale_date
, nrml.off_sale_date
, MIN(RANGE_START(nrml.eff_period)) AS eff_begin
,`{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(MIN(RANGE_START(nrml.eff_period))AS STRING)) AS eff_begin_tz
, MAX(RANGE_END(nrml.eff_period))   AS eff_end
,`{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(MIN(RANGE_END(nrml.eff_period)) AS STRING)) AS eff_end_tz
FROM 
(
SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num,location_num,is_replenishment_eligible_ind,replenishment_setting_type_code,replenishment_setting_value_desc,on_sale_date,off_sale_date
				ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM
(
SELECT *,CASE WHEN LAG(eff_end_tmstp) OVER (PARTITION BY rms_sku_num,location_num,is_replenishment_eligible_ind,replenishment_setting_type_code,replenishment_setting_value_desc,on_sale_date,off_sale_date
 ORDER BY eff_begin_tmstp) >= DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) THEN 0 ELSE 1 END AS discontinuity_flag
 FROM

(
SELECT DISTINCT
-- NORMALIZE
  src.rms_sku_num
, src.location_num
, src.is_replenishment_eligible_ind
, src.replenishment_setting_type_code
, src.replenishment_setting_value_desc
, src.on_sale_date
, src.off_sale_date
, tgt.eff_begin_tmstp
, tgt.eff_end_tmstp
, tgt.eff_begin_tmstp_tz
, tgt.eff_end_tmstp_tz
, COALESCE(RANGE_INTERSECT(src.eff_period, RANGE(tgt.eff_begin_tmstp,tgt.eff_end_tmstp)), src.eff_period ) AS eff_period
FROM 
src
LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_indicator_dtl_xref tgt
ON src.rms_sku_num = tgt.rms_sku_num
AND src.location_num = tgt.location_num
AND  RANGE_OVERLAPS( src.eff_period, RANGE(tgt.eff_begin_tmstp, tgt.eff_end_tmstp))
WHERE ( tgt.rms_sku_num IS NULL OR
(  (LOWER(src.is_replenishment_eligible_ind) <> LOWER(tgt.is_replenishment_eligible_ind) OR (src.is_replenishment_eligible_ind IS NOT NULL AND tgt.is_replenishment_eligible_ind IS NULL) OR (src.is_replenishment_eligible_ind IS NULL AND tgt.is_replenishment_eligible_ind IS NOT NULL))
OR (LOWER(src.replenishment_setting_type_code) <> LOWER(tgt.replenishment_setting_type_code) OR (src.replenishment_setting_type_code IS NOT NULL AND tgt.replenishment_setting_type_code IS NULL) OR (src.replenishment_setting_type_code IS NULL AND tgt.replenishment_setting_type_code IS NOT NULL))
OR (LOWER(src.replenishment_setting_value_desc) <> LOWER(tgt.replenishment_setting_value_desc) OR (src.replenishment_setting_value_desc IS NOT NULL AND tgt.replenishment_setting_value_desc IS NULL) OR (src.replenishment_setting_value_desc IS NULL AND tgt.replenishment_setting_value_desc IS NOT NULL))
OR (src.on_sale_date <> tgt.on_sale_date OR (src.on_sale_date IS NOT NULL AND tgt.on_sale_date IS NULL) OR (src.on_sale_date IS NULL AND tgt.on_sale_date IS NOT NULL))
OR (src.off_sale_date <> tgt.off_sale_date OR (src.off_sale_date IS NOT NULL AND tgt.off_sale_date IS NULL) OR (src.off_sale_date IS NULL AND tgt.off_sale_date IS NOT NULL))))
) AS ordered_data
) AS grouped_data
) NRML
GROUP BY rms_sku_num,location_num,is_replenishment_eligible_ind,replenishment_setting_type_code,replenishment_setting_value_desc,on_sale_date,off_sale_date,range_group
ORDER BY rms_sku_num,location_num,is_replenishment_eligible_ind,replenishment_setting_type_code,replenishment_setting_value_desc,on_sale_date,off_sale_date,eff_begin
;


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_indicator_dtl_xref AS TGT
WHERE EXISTS (
  SELECT 1
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_indicator_dtl_xref_vtw AS SRC
    WHERE LOWER(src.rms_sku_num) = LOWER(tgt.rms_sku_num) 
	AND src.location_num = tgt.location_num
  AND SRC.eff_begin_tmstp <= TGT.eff_begin_tmstp
  AND SRC.eff_end_tmstp >= TGT.eff_end_tmstp
);

UPDATE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_indicator_dtl_xref AS TGT
SET TGT.eff_end_tmstp = SRC.eff_begin_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_indicator_dtl_xref_vtw AS SRC
    WHERE LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) 
	AND SRC.location_num = tgt.location_num
  AND SRC.eff_begin_tmstp >= TGT.eff_begin_tmstp
  AND SRC.eff_begin_tmstp < TGT.eff_end_tmstp 
  AND SRC.eff_end_tmstp > TGT.eff_end_tmstp ;


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_indicator_dtl_xref AS TGT
SET TGT.eff_begin_tmstp = SRC.eff_end_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_indicator_dtl_xref_vtw AS SRC
    WHERE LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) 
	AND SRC.location_num = tgt.location_num
  AND SRC.eff_end_tmstp > TGT.eff_begin_tmstp
  AND SRC.eff_end_tmstp <= TGT.eff_end_tmstp;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_indicator_dtl_xref (rms_sku_num, location_num, is_replenishment_eligible_ind,
 replenishment_setting_type_code, replenishment_setting_value_desc, on_sale_date, off_sale_date, eff_begin_tmstp,
 eff_begin_tmstp_tz,
  eff_end_tmstp,
  eff_end_tmstp_tz, dw_batch_id, dw_batch_date, dw_sys_load_tmstp)
(SELECT DISTINCT rms_sku_num,
  location_num,
  is_replenishment_eligible_ind,
  replenishment_setting_type_code,
  replenishment_setting_value_desc,
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
    is_replenishment_eligible_ind,
    replenishment_setting_type_code,
    replenishment_setting_value_desc,
    on_sale_date,
    off_sale_date,
    eff_begin_tmstp,
    eff_begin_tmstp_tz,
    eff_end_tmstp,
    eff_end_tmstp_tz,
     (SELECT CAST(config_value AS BIGINT) AS config_value
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
     WHERE LOWER(interface_code) = LOWER('RP_LOAD_MAX_BATCH_ID')) AS dw_batch_id,
    CURRENT_DATE('PST8PDT') AS dw_batch_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_indicator_dtl_xref_vtw) AS t1
 WHERE NOT EXISTS (SELECT 1 AS `A12180`
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_indicator_dtl_xref
   WHERE rms_sku_num = t1.rms_sku_num
    AND location_num = t1.location_num)
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, location_num)) = 1);


COMMIT TRANSACTION;


