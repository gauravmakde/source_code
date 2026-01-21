
--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=stg-dim;AppSubArea=NAP_PRODUCT;' UPDATE FOR SESSION;


--NONSEQUENCED VALIDTIME --Purge work table for staging temporal rows
TRUNCATE TABLE {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.PRODUCT_CHOICE_DIM_VTW;
--.IF ERRORCODE <> 0 THEN .QUIT 1


INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.PRODUCT_CHOICE_DIM_VTW
( epm_choice_num
, channel_country
, choice_desc
, rms_style_num_list
, epm_style_num
, color_num
, nrf_color_num
, color_desc
, nord_display_color
, eff_begin_tmstp
, eff_end_tmstp
, eff_begin_tmstp_tz
, eff_end_tmstp_tz
)
SELECT
  NRML.epm_choice_num
, NRML.channel_country
, NRML.choice_desc
, NRML.rms_style_num_list
, NRML.epm_style_num
, NRML.color_num
, NRML.nrf_color_num
, NRML.color_desc
, NRML.nord_display_color
, CAST(RANGE_START(NRML.eff_period)AS TIMESTAMP) AS eff_begin
, CAST(RANGE_END(NRML.eff_period) AS TIMESTAMP)  AS eff_end
,   jwn_udf.udf_time_zone(CAST(RANGE_START(NRML.eff_period)AS string)) as eff_begin_tmstp_tz,
  jwn_udf.udf_time_zone( CAST(RANGE_END(NRML.eff_period) AS string)) as eff_end_tmstp_tz
FROM (

--NONSEQUENCED VALIDTIME
SELECT DISTINCT
  SRC.epm_choice_num
, SRC.channel_country
, SRC.choice_desc
, SRC.rms_style_num_list
, SRC.epm_style_num
, SRC.color_num
, SRC.nrf_color_num
, SRC.color_desc
, SRC.nord_display_color
-- , COALESCE( SRC.eff_period P_INTERSECT PERIOD(TGT.eff_begin_tmstp,TGT.eff_end_tmstp)
--           , SRC.eff_period ) AS eff_period
,COALESCE( RANGE_INTERSECT (SRC.eff_period,range(TGT.eff_begin_tmstp_utc,TGT.eff_end_tmstp_utc))
          , SRC.eff_period ) AS eff_period

FROM (
	SELECT DISTINCT epm_choice_num, channel_country, choice_desc, rms_style_num_list
	, epm_style_num, color_num, nrf_color_num, color_desc, nord_display_color
	, RANGE(cast(eff_begin_tmstp as timestamp), eff_end_tmstp) AS eff_period
	FROM (
	SELECT SRC_2.*
	, COALESCE(MAX(cast(eff_begin_tmstp as timestamp)) OVER(PARTITION BY epm_choice_num,channel_country ORDER BY eff_begin_tmstp
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
	, nordstromdisplaycolor AS nord_display_color
	-- , NORD_UDF.ISO8601_TMSTP(sourcepublishtimestamp) AS eff_begin_tmstp
  -- ,timestamp(sourcepublishtimestamp) as eff_begin_tmstp
  ,cast(jwn_udf.iso8601_tmstp( COLLATE(sourcepublishtimestamp,''))as timestamp) AS eff_begin_tmstp
	FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_CHOICE_LDG
	) SRC_2
	QUALIFY cast(eff_begin_tmstp  as timestamp)< eff_end_tmstp
	) SRC_1
) SRC
LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_CHOICE_DIM_HIST TGT
ON SRC.epm_choice_num = TGT.epm_choice_num
AND LOWER(SRC.channel_country) = LOWER(TGT.channel_country)
-- AND SRC.eff_period OVERLAPS PERIOD(TGT.eff_begin_tmstp, TGT.eff_end_tmstp)
AND range_overlaps(SRC.eff_period, range(TGT.eff_begin_tmstp_utc, TGT.eff_end_tmstp_utc))
WHERE ( TGT.epm_choice_num IS NULL OR 
   (  (SRC.choice_desc <> TGT.choice_desc OR (SRC.choice_desc IS NOT NULL AND TGT.choice_desc IS NULL) OR (SRC.choice_desc IS NULL AND TGT.choice_desc IS NOT NULL))
   OR (SRC.rms_style_num_list <> TGT.rms_style_num_list OR (SRC.rms_style_num_list IS NOT NULL AND TGT.rms_style_num_list IS NULL) OR (SRC.rms_style_num_list IS NULL AND TGT.rms_style_num_list IS NOT NULL))
   OR (SRC.epm_style_num <> TGT.epm_style_num OR (SRC.epm_style_num IS NOT NULL AND TGT.epm_style_num IS NULL) OR (SRC.epm_style_num IS NULL AND TGT.epm_style_num IS NOT NULL))
   OR (SRC.color_num <> TGT.color_num OR (SRC.color_num IS NOT NULL AND TGT.color_num IS NULL) OR (SRC.color_num IS NULL AND TGT.color_num IS NOT NULL))
   OR (SRC.nrf_color_num <> TGT.nrf_color_num OR (SRC.nrf_color_num IS NOT NULL AND TGT.nrf_color_num IS NULL) OR (SRC.nrf_color_num IS NULL AND TGT.nrf_color_num IS NOT NULL))
   OR (SRC.color_desc <> TGT.color_desc OR (SRC.color_desc IS NOT NULL AND TGT.color_desc IS NULL) OR (SRC.color_desc IS NULL AND TGT.color_desc IS NOT NULL))
   OR (SRC.nord_display_color <> TGT.nord_display_color OR (SRC.nord_display_color IS NOT NULL AND TGT.nord_display_color IS NULL) OR (SRC.nord_display_color IS NULL AND TGT.nord_display_color IS NOT NULL))
     )  )
 )NRML
;
--.IF ERRORCODE <> 0 THEN .QUIT 2


--Explicit transaction on Teradata ensures that we do not corrupt target table if failure in middle of updating target table
BEGIN TRANSACTION;
--.IF ERRORCODE <> 0 THEN .QUIT 3

--SEQUENCED VALIDTIME
DELETE FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_choice_dim AS tgt
WHERE EXISTS (SELECT *
 FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.product_choice_dim_vtw AS src
 WHERE epm_choice_num = tgt.epm_choice_num
  AND LOWER(channel_country) = LOWER(tgt.channel_country))
;
--.IF ERRORCODE <> 0 THEN .QUIT 4

INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_NAP_DIM.PRODUCT_CHOICE_DIM
( epm_choice_num
, channel_country
, choice_desc
, rms_style_num_list
, epm_style_num
, color_num
, nrf_color_num
, color_desc
, nord_display_color
, eff_begin_tmstp
, eff_begin_tmstp_tz
, eff_end_tmstp
, eff_end_tmstp_tz
, dw_batch_id
, dw_batch_date
, dw_sys_load_tmstp
)
SELECT
  epm_choice_num
, channel_country
, choice_desc
, rms_style_num_list
, epm_style_num
, color_num
, nrf_color_num
, color_desc
, nord_display_color
, eff_begin_tmstp
, eff_begin_tmstp_tz
, eff_end_tmstp
, eff_end_tmstp_tz
,(SELECT BATCH_ID FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.ELT_CONTROL WHERE LOWER(Subject_Area_Nm) =LOWER('NAP_PRODUCT')) AS dw_batch_id
,(SELECT CURR_BATCH_DATE FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.ELT_CONTROL WHERE LOWER(Subject_Area_Nm) =LOWER('NAP_PRODUCT')) AS dw_batch_date
, CURRENT_DATETIME() AS dw_sys_load_tmstp
FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.PRODUCT_CHOICE_DIM_VTW
;
--.IF ERRORCODE <> 0 THEN .QUIT 5

COMMIT TRANSACTION;
--.IF ERRORCODE <> 0 THEN .QUIT 6

--COLLECT STATISTICS COLUMN(epm_choice_num), COLUMN(channel_country), COLUMN(epm_style_num), COLUMN(eff_end_tmstp)
--ON {{params.gcp_project_id}}.{{params.dbenv}}_NAP_DIM.PRODUCT_CHOICE_DIM;

