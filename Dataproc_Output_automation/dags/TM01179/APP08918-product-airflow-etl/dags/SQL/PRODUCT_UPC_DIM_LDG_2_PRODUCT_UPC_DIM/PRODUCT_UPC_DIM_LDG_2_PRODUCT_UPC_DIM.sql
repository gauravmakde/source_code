
--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=stg-dim;AppSubArea=NAP_PRODUCT;' UPDATE FOR SESSION;


--NONSEQUENCED VALIDTIME --Purge work table for staging temporal rows
TRUNCATE TABLE {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.PRODUCT_UPC_DIM_VTW;
--.IF ERRORCODE <> 0 THEN .QUIT 1


INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.PRODUCT_UPC_DIM_VTW
( upc_num
, channel_country
, upc_desc
, rms_sku_num
, epm_sku_num
, prmy_upc_ind
, upc_type_code
, eff_begin_tmstp
, eff_end_tmstp
,eff_begin_tmstp_tz
,eff_end_tmstp_tz
)
SELECT
  NRML.upc_num
, NRML.channel_country
, NRML.upc_desc
, NRML.rms_sku_num
, NRML.epm_sku_num
, NRML.prmy_upc_ind
, NRML.upc_type_code
, CAST(RANGE_START(NRML.eff_period)AS TIMESTAMP) AS eff_begin
, CAST(RANGE_END(NRML.eff_period) AS TIMESTAMP)  AS eff_end
,jwn_udf.udf_time_zone(cast(RANGE_START(NRML.eff_period) as string)) as eff_begin_tmstp_tz
,jwn_udf.udf_time_zone(cast(RANGE_end(NRML.eff_period) as string)) as eff_end_tmstp_tz
FROM (
--NONSEQUENCED VALIDTIME
SELECT DISTINCT
  SRC.upc_num
, SRC.channel_country
, SRC.upc_desc
, SRC.rms_sku_num
, SRC.epm_sku_num
, SRC.prmy_upc_ind
, SRC.upc_type_code
,COALESCE( RANGE_INTERSECT (SRC.eff_period,range(TGT.eff_begin_tmstp_utc,TGT.eff_end_tmstp_utc))
          , SRC.eff_period ) AS eff_period
FROM (
	SELECT DISTINCT upc_num, channel_country, upc_desc, rms_sku_num, epm_sku_num, prmy_upc_ind, upc_type_code
	, RANGE(eff_begin_tmstp, eff_end_tmstp) AS eff_period
	FROM (
	SELECT SRC_2.*
	, COALESCE(MAX(eff_begin_tmstp) OVER(PARTITION BY upc_num,channel_country ORDER BY eff_begin_tmstp
	           ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
	          ,TIMESTAMP'9999-12-31 23:59:59.999999+00:00') AS eff_end_tmstp
	FROM (
	SELECT
	  vendorupcs_upcs_code AS upc_num
	, marketcode AS channel_country
	, vendorupcs_upcs_description AS upc_desc
	, legacyrmsskuid AS rms_sku_num
	, id AS epm_sku_num
	,(CASE WHEN LOWER(vendorupcs_upcs_isprimaryupc)= LOWER('true') THEN 'Y' ELSE 'N' END) AS prmy_upc_ind
	,(CASE WHEN LOWER(vendorupcs_upcs_upctype)<> LOWER('null') THEN vendorupcs_upcs_upctype ELSE 'No UPC Type' END) AS upc_type_code
  	,timestamp(jwn_udf.iso8601_tmstp( COLLATE(sourcepublishtimestamp,''))) AS eff_begin_tmstp
	FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_UPC_DIM_LDG
	) SRC_2
	QUALIFY eff_begin_tmstp < eff_end_tmstp
	) SRC_1
) SRC
LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_UPC_DIM_HIST TGT
ON LOWER(SRC.upc_num) = LOWER(TGT.upc_num)
AND LOWER(SRC.channel_country) = LOWER(TGT.channel_country)
AND RANGE_CONTAINS(RANGE(TGT.eff_begin_tmstp_utc, TGT.eff_end_tmstp_utc), SRC.eff_period )
WHERE ( TGT.upc_num IS NULL OR 
   (  (LOWER(SRC.upc_desc) <> LOWER(TGT.upc_desc) OR (SRC.upc_desc IS NOT NULL AND TGT.upc_desc IS NULL) OR (SRC.upc_desc IS NULL AND TGT.upc_desc IS NOT NULL))
   OR (LOWER(SRC.rms_sku_num) <> LOWER(TGT.rms_sku_num) OR (SRC.rms_sku_num IS NOT NULL AND TGT.rms_sku_num IS NULL) OR (SRC.rms_sku_num IS NULL AND TGT.rms_sku_num IS NOT NULL))
   OR (SRC.epm_sku_num <> TGT.epm_sku_num OR (SRC.epm_sku_num IS NOT NULL AND TGT.epm_sku_num IS NULL) OR (SRC.epm_sku_num IS NULL AND TGT.epm_sku_num IS NOT NULL))
   OR (LOWER(SRC.prmy_upc_ind) <> LOWER(TGT.prmy_upc_ind) OR (SRC.prmy_upc_ind IS NOT NULL AND TGT.prmy_upc_ind IS NULL) OR (SRC.prmy_upc_ind IS NULL AND TGT.prmy_upc_ind IS NOT NULL))
   OR (LOWER(SRC.upc_type_code) <> LOWER(TGT.upc_type_code) OR (SRC.upc_type_code IS NOT NULL AND TGT.upc_type_code IS NULL) OR (SRC.upc_type_code IS NULL AND TGT.upc_type_code IS NOT NULL))
   )  )
) NRML
;
--.IF ERRORCODE <> 0 THEN .QUIT 2


--Explicit transaction on Teradata ensures that we do not corrupt target table if failure in middle of updating target table
BEGIN TRANSACTION;
--.IF ERRORCODE <> 0 THEN .QUIT 3

--SEQUENCED VALIDTIME
DELETE FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_DIM.PRODUCT_UPC_DIM AS tgt
WHERE EXISTS (SELECT *
 FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.product_upc_dim_vtw AS src
 WHERE LOWER(upc_num) = LOWER(tgt.upc_num)
  AND LOWER(channel_country) = LOWER(tgt.channel_country))
;
--.IF ERRORCODE <> 0 THEN .QUIT 4

INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_NAP_DIM.PRODUCT_UPC_DIM
( upc_num
, channel_country
, upc_desc
, rms_sku_num
, epm_sku_num
, prmy_upc_ind
, upc_type_code
, eff_begin_tmstp
, eff_end_tmstp
, dw_batch_id
, dw_batch_date
, dw_sys_load_tmstp
, eff_begin_tmstp_tz
, eff_end_tmstp_tz
)
SELECT
  upc_num
, channel_country
, upc_desc
, rms_sku_num
, epm_sku_num
, prmy_upc_ind
, upc_type_code
, eff_begin_tmstp
, eff_end_tmstp
,(SELECT BATCH_ID FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.ELT_CONTROL WHERE LOWER(Subject_Area_Nm) =LOWER('NAP_PRODUCT')) AS dw_batch_id
,(SELECT CURR_BATCH_DATE FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.ELT_CONTROL WHERE LOWER(Subject_Area_Nm) =LOWER('NAP_PRODUCT')) AS dw_batch_date
, CURRENT_DATETIME('PST8PDT') AS dw_sys_load_tmstp
, eff_begin_tmstp_tz
, eff_end_tmstp_tz
FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.PRODUCT_UPC_DIM_VTW
;

--.IF ERRORCODE <> 0 THEN .QUIT 5


COMMIT TRANSACTION;
--.IF ERRORCODE <> 0 THEN .QUIT 6

-- COLLECT STATISTICS COLUMN(upc_num), COLUMN(channel_country), COLUMN(rms_sku_num), COLUMN(eff_end_tmstp)
-- ON {{params.gcp_project_id}}.{{params.dbenv}}_NAP_DIM.PRODUCT_UPC_DIM;
