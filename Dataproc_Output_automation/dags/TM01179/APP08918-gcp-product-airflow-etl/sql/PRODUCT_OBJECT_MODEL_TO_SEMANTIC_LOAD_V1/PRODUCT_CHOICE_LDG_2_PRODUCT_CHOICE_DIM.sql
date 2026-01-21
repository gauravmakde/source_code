TRUNCATE TABLE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_choice_dim_vtw;


INSERT INTO `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_choice_dim_vtw
 (epm_choice_num, channel_country, choice_desc, rms_style_num_list,
 epm_style_num, color_num, nrf_color_num, color_desc, nord_display_color, eff_begin_tmstp,eff_begin_tmstp_tz,
  eff_end_tmstp,
  eff_end_tmstp_tz)


with src_1 as
 (select  epm_choice_num, channel_country, choice_desc, rms_style_num_list
	, epm_style_num, color_num, nrf_color_num, color_desc, nord_display_color,eff_begin_tmstp,eff_end_tmstp
--  range(eff_begin_tmstp,eff_end_tmstp) as eff_period 
 from  (
    --inner normalize
            SELECT  epm_choice_num, channel_country, choice_desc, rms_style_num_list
	, epm_style_num, color_num, nrf_color_num, color_desc, nord_display_color, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY epm_choice_num, channel_country, choice_desc, rms_style_num_list
	, epm_style_num, color_num, nrf_color_num, color_desc, nord_display_color ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
    FROM (
        SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY  epm_choice_num, channel_country, choice_desc, rms_style_num_list
	, epm_style_num, color_num, nrf_color_num, color_desc, nord_display_color ORDER BY eff_begin_tmstp) >= 
                DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
         from (
	SELECT SRC_2.*
	, COALESCE(MAX(eff_begin_tmstp) OVER(PARTITION BY epm_choice_num,channel_country ORDER BY eff_begin_tmstp
	           ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
	          ,TIMESTAMP'9999-12-31 23:59:59.999999+00:00') AS eff_end_tmstp
	FROM (
	SELECT
	  id AS epm_choice_num
	, marketcode AS channel_country
	, description AS choice_desc
	, legacyrmsstyleids AS rms_style_num_list
	, CAST(parentepmstyleid AS BIGINT) AS epm_style_num
	, colors_code AS color_num
	, colors_nrfcolorcode AS nrf_color_num
	, COALESCE(colors_nrfcolordescription,colors_description) AS color_desc
	, nordstromdisplaycolor AS nord_display_color,
  CAST(`{{params.dataplex_project_id}}.JWN_UDF.ISO8601_TMSTP`(COLLATE(sourcepublishtimestamp,''))as timestamp) AS eff_begin_tmstp

	FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_choice_ldg
	) SRC_2
	QUALIFY eff_begin_tmstp < eff_end_tmstp
	)) AS ordered_data
) AS grouped_data
GROUP BY  epm_choice_num, channel_country, choice_desc, rms_style_num_list
	, epm_style_num, color_num, nrf_color_num, color_desc, nord_display_color,range_group
ORDER BY  epm_choice_num, channel_country, choice_desc, rms_style_num_list
	, epm_style_num, color_num, nrf_color_num, color_desc, nord_display_color, eff_begin_tmstp) )




SELECT
epm_choice_num,
    channel_country,
    choice_desc,
    rms_style_num_list,
    epm_style_num,
    color_num,
    nrf_color_num,
    color_desc,
    nord_display_color,
    cast(min(RANGE_START(eff_period))as timestamp) AS eff_begin_tmstp,
    `{{params.dataplex_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(min(RANGE_START(eff_period)) AS string)) as eff_begin_tmstp_tz,
    cast(max(RANGE_END(eff_period))as timestamp) AS eff_end_tmstp,
    `{{params.dataplex_project_id}}.JWN_UDF.UDF_TIME_ZONE`( CAST(max(RANGE_END(eff_period)) AS string)) as eff_end_tmstp_tz
from  (
    --inner normalize
            SELECT epm_choice_num,channel_country,choice_desc,rms_style_num_list,epm_style_num,color_num,nrf_color_num,color_desc,nord_display_color,eff_period
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY epm_choice_num,channel_country,choice_desc,rms_style_num_list,epm_style_num,color_num,nrf_color_num,color_desc,nord_display_color ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM
      --  NONSEQUENCED VALIDTIME
      (
        
            --  NORMALIZE
         SELECT   SRC.epm_choice_num,SRC.channel_country,SRC.choice_desc,SRC.rms_style_num_list,SRC.epm_style_num,SRC.color_num,SRC.nrf_color_num,SRC.color_desc,SRC.nord_display_color, TGT.eff_begin_tmstp,TGT.eff_end_tmstp,
            CASE 
                WHEN LAG(eff_end_tmstp_utc) OVER (PARTITION BY SRC.epm_choice_num,SRC.channel_country,SRC.choice_desc,SRC.rms_style_num_list,SRC.epm_style_num,SRC.color_num,SRC.nrf_color_num,SRC.color_desc,SRC.nord_display_color ORDER BY eff_begin_tmstp_utc) >= 
                DATE_SUB(eff_begin_tmstp_utc, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag,
             COALESCE( SAFE.RANGE_INTERSECT(SRC.eff_period , RANGE(TGT.eff_begin_tmstp_utc,TGT.eff_end_tmstp_utc)) , SRC.eff_period ) AS eff_period,
      FROM
(SELECT 
  --normalize
   epm_choice_num, channel_country, choice_desc, rms_style_num_list
	, epm_style_num, color_num, nrf_color_num, color_desc, nord_display_color
	, RANGE(eff_begin_tmstp, eff_end_tmstp) AS eff_period
	FROM  SRC_1
) SRC 
    LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_choice_dim_hist AS tgt 
    ON SRC.epm_choice_num = tgt.epm_choice_num 
    
    AND LOWER(SRC.channel_country) = LOWER(tgt.channel_country)
   WHERE tgt.epm_choice_num IS NULL
    OR LOWER(SRC.choice_desc) <> LOWER(tgt.choice_desc)
    OR tgt.choice_desc IS NULL AND SRC.choice_desc IS NOT NULL
    OR SRC.choice_desc IS NULL AND tgt.choice_desc IS NOT NULL

    OR LOWER(SRC.rms_style_num_list) <> LOWER(tgt.rms_style_num_list) 
    OR tgt.rms_style_num_list IS NULL AND SRC.rms_style_num_list IS NOT NULL
    OR SRC.rms_style_num_list IS NULL AND tgt.rms_style_num_list IS NOT NULL

    OR (SRC.epm_style_num <> TGT.epm_style_num 
    OR (SRC.epm_style_num IS NOT NULL AND TGT.epm_style_num IS NULL) 
    OR (SRC.epm_style_num IS NULL AND TGT.epm_style_num IS NOT NULL))

    OR LOWER(SRC.color_num) <> LOWER(tgt.color_num)
    OR tgt.color_num IS NULL AND SRC.color_num IS NOT NULL
    OR SRC.color_num IS NULL AND tgt.color_num IS NOT NULL

    OR LOWER(SRC.nrf_color_num) <> LOWER(tgt.nrf_color_num)
    OR tgt.nrf_color_num IS NULL AND SRC.nrf_color_num IS NOT NULL
    OR SRC.nrf_color_num IS NULL AND tgt.nrf_color_num IS NOT NULL

    OR LOWER(SRC.color_desc) <> LOWER(tgt.color_desc)
    OR tgt.color_desc IS NULL AND SRC.color_desc IS NOT NULL
    OR SRC.color_desc IS NULL AND tgt.color_desc IS NOT NULL
    
    OR LOWER(SRC.nord_display_color) <> LOWER(tgt.nord_display_color)
    OR tgt.nord_display_color IS NULL AND SRC.nord_display_color IS NOT NULL
    OR SRC.nord_display_color IS NULL AND tgt.nord_display_color IS NOT NULL )as   ordered_data
) AS grouped_data)
GROUP BY epm_choice_num,channel_country,choice_desc,rms_style_num_list,epm_style_num,color_num,nrf_color_num,color_desc,nord_display_color,eff_period
ORDER BY epm_choice_num,channel_country,choice_desc,rms_style_num_list,epm_style_num,color_num,nrf_color_num,color_desc,nord_display_color;

BEGIN
BEGIN TRANSACTION;

--SEQUENCED VALIDTIME
DELETE FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_choice_dim AS tgt WHERE EXISTS (
  SELECT 1
    FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_choice_dim_vtw AS src
    WHERE src.epm_choice_num = tgt.epm_choice_num
     AND LOWER(src.channel_country )= LOWER(tgt.channel_country)
     AND SRC.eff_begin_tmstp <= TGT.eff_begin_tmstp
    AND SRC.eff_end_tmstp >= TGT.eff_end_tmstp
);

UPDATE  `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_choice_dim AS tgt
SET TGT.eff_end_tmstp = SRC.eff_begin_tmstp
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_choice_dim_vtw AS src
WHERE src.epm_choice_num = tgt.epm_choice_num
     AND LOWER(src.channel_country )= LOWER(tgt.channel_country)
    AND SRC.eff_begin_tmstp > TGT.eff_begin_tmstp
    AND SRC.eff_begin_tmstp <= TGT.eff_end_tmstp
AND SRC.eff_end_tmstp >= TGT.eff_end_tmstp;


UPDATE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_choice_dim AS tgt
SET TGT.eff_begin_tmstp = SRC.eff_end_tmstp
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_choice_dim_vtw AS src
WHERE src.epm_choice_num = tgt.epm_choice_num
     AND LOWER(src.channel_country )= LOWER(tgt.channel_country)
    AND SRC.eff_end_tmstp >= TGT.eff_begin_tmstp
    AND SRC.eff_begin_tmstp < TGT.eff_begin_tmstp
AND SRC.eff_end_tmstp <= TGT.eff_end_tmstp;

insert into `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_choice_dim(epm_choice_num,channel_country,choice_desc,rms_style_num_list,epm_style_num,color_num,nrf_color_num,color_desc,nord_display_color,eff_begin_tmstp_tz,eff_end_tmstp_tz,dw_batch_id,dw_batch_date,dw_sys_load_tmstp,eff_begin_tmstp,eff_end_tmstp)
with tbl as 
(SELECT 
tgt.*,
cast(src.eff_begin_tmstp as timestamp) AS src_eff_begin_tmstp, 
cast(src.eff_end_tmstp as timestamp) AS src_eff_end_tmstp, 
tgt.eff_begin_tmstp AS tgt_eff_begin_tmstp, 
tgt.eff_end_tmstp AS tgt_eff_end_tmstp 
 from `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_choice_dim_vtw AS src 
 inner join `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_choice_dim AS tgt 
 on src.epm_choice_num = tgt.epm_choice_num 
 and tgt.eff_begin_tmstp < src.eff_begin_tmstp and tgt.eff_end_tmstp > src.eff_end_tmstp)

SELECT *  except(eff_begin_tmstp, eff_end_tmstp, src_eff_begin_tmstp, src_eff_end_tmstp, tgt_eff_begin_tmstp,tgt_eff_end_tmstp ), tgt_eff_begin_tmstp, cast(src_eff_begin_tmstp as timestamp) FROM tbl
UNION ALL
SELECT * except(eff_begin_tmstp, eff_end_tmstp, src_eff_begin_tmstp, src_eff_end_tmstp, tgt_eff_begin_tmstp,tgt_eff_end_tmstp), cast(src_eff_end_tmstp as timestamp), tgt_eff_end_tmstp FROM tbl;


delete from `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_choice_dim AS tgt where exists (select 1 from `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_choice_dim_vtw AS src where src.epm_choice_num = tgt.epm_choice_num and tgt.eff_begin_tmstp < src.eff_begin_tmstp and tgt.eff_end_tmstp > src.eff_end_tmstp);



INSERT INTO `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_choice_dim (epm_choice_num,
  channel_country,
  choice_desc,
  rms_style_num_list,
  epm_style_num,
  color_num,
  nrf_color_num,
  color_desc,
  nord_display_color,
  eff_begin_tmstp,
  eff_begin_tmstp_tz,
  eff_end_tmstp,
  eff_end_tmstp_tz,
  dw_batch_id,
  dw_batch_date,
  dw_sys_load_tmstp)
(SELECT DISTINCT epm_choice_num,
  channel_country,
  choice_desc,
  rms_style_num_list,
  epm_style_num,
  color_num,
  nrf_color_num,
  color_desc,
  nord_display_color,
  eff_begin_tmstp,
  eff_begin_tmstp_tz,
  eff_end_tmstp,
  eff_end_tmstp_tz,
  dw_batch_id,
  dw_batch_date,
  dw_sys_load_tmstp
 FROM (SELECT epm_choice_num,
    channel_country,
    choice_desc,
    rms_style_num_list,
    epm_style_num,
    color_num,
    nrf_color_num,
    color_desc,
    nord_display_color,
    eff_begin_tmstp,
    eff_begin_tmstp_tz,
    eff_end_tmstp,
    eff_end_tmstp_tz,
     (SELECT batch_id
     FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
     WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT')) AS dw_batch_id,
     (SELECT curr_batch_date
     FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
     WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT')) AS dw_batch_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
   FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_choice_dim_vtw) AS t3
 WHERE NOT EXISTS (SELECT 1 AS `A12180`
   FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_choice_dim
   WHERE epm_choice_num = t3.epm_choice_num
    AND LOWER(channel_country) = LOWER(t3.channel_country))
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY epm_choice_num, channel_country)) = 1);

COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
ROLLBACK TRANSACTION;
END;

