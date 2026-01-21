
--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=stg-dim;AppSubArea=NAP_PRODUCT;' UPDATE FOR SESSION;


--NONSEQUENCED VALIDTIME --Purge work table for staging temporal rows
TRUNCATE TABLE  {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.CASEPACK_SKU_XREF_VTW;
--.IF ERRORCODE<> 0 THEN .QUIT 1


INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.CASEPACK_SKU_XREF_VTW
( rms_casepack_num
, epm_casepack_num
, channel_country
, rms_sku_num
, epm_sku_num
, pack_component_seq_num
, items_per_pack_qty
, eff_begin_tmstp
, eff_end_tmstp
, eff_begin_tmstp_tz
, eff_end_tmstp_tz
)
SELECT
  NRML.rms_casepack_num
, NRML.epm_casepack_num
, NRML.channel_country
, NRML.rms_sku_num
, NRML.epm_sku_num
, NRML.pack_component_seq_num
, NRML.items_per_pack_qty

-- --, NRML.eff_period AS  eff_begin
-- ,IF(NRML.eff_period, CURRENT_TIMESTAMP(), NULL) as eff_begin
-- --, NRML.eff_period   AS eff_end
-- ,IF(NRML.eff_period, CURRENT_TIMESTAMP(), NULL) as eff_end
, RANGE_START(NRML.eff_period) AS eff_begin
, RANGE_END(NRML.eff_period)   AS eff_end
,jwn_udf.udf_time_zone(cast(RANGE_START(NRML.eff_period) as string)) as eff_begin_tmstp_tz
,jwn_udf.udf_time_zone(cast(RANGE_end(NRML.eff_period) as string)) as eff_end_tmstp_tz

FROM (
--NONSEQUENCED VALIDTIME
SELECT distinct
  SRC.rms_casepack_num
, SRC.epm_casepack_num
, SRC.channel_country
, SRC.rms_sku_num
, SRC.epm_sku_num
, SRC.pack_component_seq_num
, SRC.items_per_pack_qty

,COALESCE( RANGE_INTERSECT (SRC.eff_period,range(TGT.eff_begin_tmstp_utc,TGT.eff_end_tmstp_utc))
          , SRC.eff_period ) AS eff_period
FROM (
	SELECT distinct rms_casepack_num, epm_casepack_num, channel_country, rms_sku_num, epm_sku_num
	, pack_component_seq_num, items_per_pack_qty
	, RANGE(eff_begin_tmstp, eff_end_tmstp) AS eff_period
	FROM (
	SELECT SRC_2.*
	, COALESCE(MAX(eff_begin_tmstp) OVER(PARTITION BY epm_casepack_num, channel_country, epm_sku_num
	ORDER BY eff_begin_tmstp
	           ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
	          ,TIMESTAMP'9999-12-31 23:59:59.999999+00:00') AS eff_end_tmstp
	FROM (
SELECT COALESCE(TRIM(referencekeyfg_legacyrmscasepackid), '0') AS rms_casepack_num,
 id AS epm_casepack_num,
 marketcode AS channel_country,
 COALESCE(TRIM(skus_referencekeyfg_legacyrmsskuid), '0') AS rms_sku_num,
 skus_id AS epm_sku_num,
 skus_componentseqnum AS pack_component_seq_num,
 skus_itemperpackquantity AS items_per_pack_qty,
 --NORD_UDF.ISO8601_TMSTP(sourcepublishtimestamp)
--  cast(sourcepublishtimestamp as timestamp) as eff_begin_tmstp
cast(jwn_udf.iso8601_tmstp( COLLATE(sourcepublishtimestamp,'')) as timestamp) AS eff_begin_tmstp
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.casepack_sku_xref_ldg
	) SRC_2
	QUALIFY eff_begin_tmstp  < eff_end_tmstp
	) SRC_1
) SRC
LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.CASEPACK_SKU_XREF_HIST TGT
ON  LOWER(SRC.rms_casepack_num) = LOWER(TGT.rms_casepack_num)
AND LOWER(SRC.channel_country) = LOWER(TGT.channel_country)
AND LOWER(SRC.rms_sku_num) = LOWER(TGT.rms_sku_num)
AND RANGE_OVERLAPS(RANGE(TGT.eff_begin_tmstp_utc, TGT.eff_end_tmstp_utc), SRC.eff_period )
WHERE ( TGT.rms_sku_num IS NULL 
   OR ((SRC.epm_casepack_num <> TGT.epm_casepack_num OR (SRC.epm_casepack_num IS NOT NULL AND TGT.epm_casepack_num IS NULL) OR (SRC.epm_casepack_num IS NULL AND TGT.epm_casepack_num IS NOT NULL))
   OR (SRC.epm_sku_num <> TGT.epm_sku_num OR (SRC.epm_sku_num IS NOT NULL AND TGT.epm_sku_num IS NULL) OR (SRC.epm_sku_num IS NULL AND TGT.epm_sku_num IS NOT NULL))
   OR (SRC.pack_component_seq_num <> TGT.pack_component_seq_num OR (SRC.pack_component_seq_num IS NOT NULL AND TGT.pack_component_seq_num IS NULL) OR (SRC.pack_component_seq_num IS NULL AND TGT.pack_component_seq_num IS NOT NULL))
   OR (SRC.items_per_pack_qty <> TGT.items_per_pack_qty OR (SRC.items_per_pack_qty IS NOT NULL AND TGT.items_per_pack_qty IS NULL) OR (SRC.items_per_pack_qty IS NULL AND TGT.items_per_pack_qty IS NOT NULL))
   )  ))
 NRML
;
--.IF ERRORCODE<> 0 THEN .QUIT 2


--Explicit transaction on Teradata ensures that we do not corrupt target table if failure in middle of updating target table
BEGIN TRANSACTION;
DELETE FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.casepack_sku_xref AS tgt
WHERE EXISTS (SELECT *
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.casepack_sku_xref_vtw AS src
    WHERE LOWER(rms_casepack_num) = LOWER(tgt.rms_casepack_num) 
    AND   LOWER(channel_country) = LOWER(tgt.channel_country)
    AND   LOWER(rms_sku_num) = LOWER(tgt.rms_sku_num));

INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.casepack_sku_xref (rms_casepack_num, epm_casepack_num, channel_country, rms_sku_num, epm_sku_num
 , pack_component_seq_num, items_per_pack_qty, eff_begin_tmstp, eff_end_tmstp, dw_batch_id, dw_batch_date,
 dw_sys_load_tmstp,eff_begin_tmstp_tz, eff_end_tmstp_tz)
(SELECT DISTINCT rms_casepack_num,
  epm_casepack_num,
  channel_country,
  rms_sku_num,
  epm_sku_num,
  pack_component_seq_num,
  items_per_pack_qty,
  eff_begin_tmstp,
  eff_end_tmstp,
  dw_batch_date,
  dw_batch_date0,
  dw_sys_load_tmstp,
  eff_begin_tmstp_tz, eff_end_tmstp_tz
 FROM (SELECT rms_casepack_num,
    epm_casepack_num,
    channel_country,
    rms_sku_num,
    epm_sku_num,
    pack_component_seq_num,
    items_per_pack_qty,
    eff_begin_tmstp,
    eff_end_tmstp,
    (SELECT batch_id
     FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
     WHERE LOWER(subject_area_nm) = LOWER('NAP_Product'))  AS dw_batch_date,
    (SELECT CURR_BATCH_DATE
      FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
      WHERE LOWER(subject_area_nm) = LOWER('NAP_Product')) AS dw_batch_date0,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
    eff_begin_tmstp_tz, eff_end_tmstp_tz
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.casepack_sku_xref_vtw)AS t3
 WHERE NOT EXISTS (SELECT 1 AS `A12180`
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.casepack_sku_xref
   WHERE (epm_casepack_num) = (t3.epm_casepack_num)
    AND LOWER(channel_country) = LOWER(t3.channel_country)
    AND (epm_sku_num) = (t3.epm_sku_num))
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY epm_casepack_num, channel_country, epm_sku_num)) = 1); 


COMMIT TRANSACTION;