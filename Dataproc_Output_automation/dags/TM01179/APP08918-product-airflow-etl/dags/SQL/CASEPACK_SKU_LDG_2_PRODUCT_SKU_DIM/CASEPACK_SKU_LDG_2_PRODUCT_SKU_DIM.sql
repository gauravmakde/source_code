
--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=stg-dim;AppSubArea=NAP_PRODUCT;' UPDATE FOR SESSION;


--NONSEQUENCED VALIDTIME --Purge work table for staging temporal rows
TRUNCATE TABLE {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.CASEPACK_SKU_DIM_VTW;
----.IF ERRORCODE <> 0 THEN .QUIT 1


INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.CASEPACK_SKU_DIM_VTW
( rms_sku_num
, epm_sku_num
, channel_country
, sku_short_desc
, sku_desc
--, web_sku_num   Default to NULL
, rms_style_num
, epm_style_num
--, web_style_num   Default to NULL
, style_desc
--, epm_choice_num   Default to NULL
, supp_part_num
, prmy_supp_num
, manufacturer_num
, sbclass_num
, sbclass_desc
, class_num
, class_desc
, dept_num
, dept_desc
, grp_num
, grp_desc
, div_num
, div_desc
, cmpy_num
, cmpy_desc
, color_num
--, color_desc   Default to NULL
--, nord_display_color   Default to NULL
--, size_1_num   Default to NULL
--, size_1_desc   Default to NULL
--, size_2_num   Default to NULL
--, size_2_desc   Default to NULL
, supp_color
--, supp_size   Default to NULL
--, brand_name   Default to NULL
--, return_disposition_code   Default to NULL
--, return_disposition_desc   Default to NULL
--, selling_status_code   Default to NULL
--, selling_status_desc   Default to NULL
--, live_date   Default to NULL
--, drop_ship_eligible_ind   Default to NULL
, sku_type_code
, sku_type_desc
, pack_orderable_code
, pack_orderable_desc
, pack_sellable_code
, pack_sellable_desc
, pack_simple_code
, pack_simple_desc
--, display_seq_1   Default to NULL
--, display_seq_2   Default to NULL
, eff_begin_tmstp
, eff_end_tmstp
,eff_begin_tmstp_tz
,eff_end_tmstp_tz
)
SELECT
  NRML.rms_sku_num
, NRML.epm_sku_num
, NRML.channel_country
, NRML.sku_short_desc
, NRML.sku_desc
, NRML.rms_style_num
, NRML.epm_style_num
, NRML.style_desc
, NRML.supp_part_num
, NRML.prmy_supp_num
, NRML.manufacturer_num
, cast(floor(CAST(NRML.sbclass_num AS FLOAT64)) as int64)
, NRML.sbclass_desc
, cast(floor(CAST(NRML.class_num AS FLOAT64)) as int64)
, NRML.class_desc
, cast(floor(CAST(NRML.dept_num AS FLOAT64)) as int64)
, NRML.dept_desc
, NRML.grp_num
, NRML.grp_desc
, NRML.div_num
, NRML.div_desc
, NRML.cmpy_num
, NRML.cmpy_desc
, NRML.color_num
, NRML.supp_color
, 'P' AS sku_type_code
, 'Pack Item' AS sku_type_desc
, 'V' AS pack_orderable_code
, 'Vendor Orderable Pack' AS pack_orderable_desc
, 'N' AS pack_sellable_code
, 'Non-Sellable Pack' AS pack_sellable_desc
, 'C' AS pack_simple_code
, 'Complex Pack' AS pack_simple_desc
-- , BEGIN(NRML.eff_period) AS eff_begin
-- , END(NRML.eff_period)   AS eff_end
, CAST(RANGE_START(NRML.eff_period)AS TIMESTAMP) AS eff_begin
, CAST(RANGE_END(NRML.eff_period) AS TIMESTAMP)  AS eff_end
,jwn_udf.udf_time_zone(cast(RANGE_START(NRML.eff_period) as string)) as eff_begin_tmstp_tz
,jwn_udf.udf_time_zone(cast(RANGE_end(NRML.eff_period) as string)) as eff_end_tmstp_tz


FROM (
--NONSEQUENCED VALIDTIME
SELECT DISTINCT
  SRC.rms_sku_num
, SRC.epm_sku_num
, SRC.channel_country
, SRC.sku_short_desc
, SRC.sku_desc
, SRC.rms_style_num
, SRC.epm_style_num
, SRC.style_desc
, SRC.supp_part_num
, SRC.prmy_supp_num
, SRC.manufacturer_num
, SRC.sbclass_num
, SRC.sbclass_desc
, SRC.class_num
, SRC.class_desc
, SRC.dept_num
, SRC.dept_desc
, SRC.grp_num
, SRC.grp_desc
, SRC.div_num
, SRC.div_desc
, SRC.cmpy_num
, SRC.cmpy_desc
, SRC.color_num
, SRC.supp_color
-- , COALESCE( SRC.eff_period P_INTERSECT PERIOD(TGT.eff_begin_tmstp,TGT.eff_end_tmstp)
--           , SRC.eff_period ) AS eff_period

,COALESCE( RANGE_INTERSECT (SRC.eff_period,range(TGT.eff_begin_tmstp_utc,TGT.eff_end_tmstp_utc))
          , SRC.eff_period ) AS eff_period
-- 		  , TGT.eff_begin_tmstp_tz
-- ,TGT.eff_end_tmstp_tz
		  
FROM (
SELECT DISTINCT
  src0.rms_casepack_num AS rms_sku_num
, src0.epm_casepack_num AS epm_sku_num
, src0.channel_country
, src0.sku_short_desc
, src0.sku_desc
, src0.rms_style_num
, src0.epm_style_num
, src0.style_desc
, src0.vendor_part_num AS supp_part_num
, src0.prmy_supp_num
, src0.manufacturer_num
, src0.subclass_num AS sbclass_num
, hier.sbclass_desc
, src0.class_num
, hier.class_desc
, src0.department_num AS dept_num
, hier.dept_desc
, hier.grp_num
, hier.grp_desc
, hier.div_num
, hier.div_desc
, hier.cmpy_num
, hier.cmpy_desc
, src0.color_num
, src0.vendor_color AS supp_color
-- , COALESCE( SRC0.eff_period P_INTERSECT hier.eff_period, SRC0.eff_period ) AS eff_period
,COALESCE( RANGE_INTERSECT (range(eff_begin_tmstp_utc, eff_end_tmstp_utc),range(hier.eff_begin_tmstp_utc,hier.eff_end_tmstp_utc))
          , range(eff_begin_tmstp_utc, eff_end_tmstp_utc) ) AS eff_period

FROM (
	SELECT DISTINCT rms_casepack_num, epm_casepack_num, channel_country, sku_short_desc, sku_desc
	, rms_style_num, epm_style_num, style_desc, vendor_part_num, prmy_supp_num, manufacturer_num
	, subclass_num, class_num, department_num, color_num, vendor_color
	,RANGE( eff_begin_tmstp  , eff_end_tmstp )AS eff_period
	FROM (
	SELECT SRC_2.*
	, COALESCE(MAX((eff_begin_tmstp)) OVER(PARTITION BY epm_casepack_num,channel_country ORDER BY eff_begin_tmstp
	           ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
	          ,TIMESTAMP'9999-12-31 23:59:59.999999+00:00') AS eff_end_tmstp
	FROM (
	SELECT DISTINCT
	  COALESCE(TRIM(referencekeyfg_legacyrmscasepackid),'0') AS rms_casepack_num
	, id AS epm_casepack_num
	, marketcode AS channel_country
	, corefg_shortdescription AS sku_short_desc
	, corefg_description AS sku_desc
	, COALESCE(TRIM(referencekeyfg_legacyrmscasepackid),'0') AS rms_style_num
	, id AS epm_style_num
	, corefg_description AS style_desc
	, subclass_num
	, class_num
	, department_num
	, productcolors_nrfcolorcode AS color_num
	, MAX(CASE
	      WHEN vendor_is_supplier='true'
	       AND supplychainfg_productvendorupcs_productvendors_isprimary='true'
	      THEN supplychainfg_productvendorupcs_productvendors_number
	      END) OVER(PARTITION BY id, marketcode, sourcepublishtimestamp) AS prmy_supp_num
	, MAX(CASE
	      WHEN vendor_is_supplier='true'
	       AND supplychainfg_productvendorupcs_productvendors_isprimary='true'
	      THEN supplychainfg_productvendorupcs_productvendors_name
	      END) OVER(PARTITION BY id, marketcode, sourcepublishtimestamp) AS prmy_supp_name
	, MAX(CASE
	      WHEN vendor_is_supplier='true'
	       AND supplychainfg_productvendorupcs_productvendors_isprimary='true'
	      THEN supplychainfg_productvendorupcs_productvendors_vendorproductnumbers_code
	      END) OVER(PARTITION BY id, marketcode, sourcepublishtimestamp) AS vendor_part_num
	, MAX(CASE
	      WHEN vendor_is_supplier='true'
	       AND supplychainfg_productvendorupcs_productvendors_isprimary='true'
	      THEN supplychainfg_productvendorupcs_productvendors_vendorcolordescription
	      END) OVER(PARTITION BY id, marketcode, sourcepublishtimestamp) AS vendor_color
	, MAX(CASE
	      WHEN vendor_is_manufacturer='true'
	       AND supplychainfg_productvendorupcs_productvendors_isprimary='true'
		  THEN supplychainfg_productvendorupcs_productvendors_number
		  END) OVER(PARTITION BY id, marketcode, sourcepublishtimestamp) AS manufacturer_num
	-- ,(NORD_UDF.ISO8601_TMSTP(sourcepublishtimestamp) (NAMED eff_begin_tmstp))
  ,cast(jwn_udf.iso8601_tmstp( COLLATE(sourcepublishtimestamp,''))as timestamp) AS eff_begin_tmstp
  FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.CASEPACK_SKU_LDG
	) SRC_2
	QUALIFY eff_begin_tmstp < eff_end_tmstp
	) SRC_1
) SRC0
LEFT JOIN (SELECT subclass_num as sbclass_num, subclass_short_name as sbclass_desc, class_num, class_short_name as class_desc, dept_num, dept_name as dept_desc,
subdivision_num AS grp_num,subdivision_name AS grp_desc,division_num AS div_num,division_name AS div_desc, CAST(1000 AS SMALLINT) AS cmpy_num
, 'Nordstrom'  AS cmpy_desc,eff_begin_tmstp_utc,eff_end_tmstp_utc,RANGE(eff_begin_tmstp_utc,eff_end_tmstp_utc) as eff_period
from {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.DEPARTMENT_CLASS_SUBCLASS_DIM_HIST )hier
ON LOWER(src0.subclass_num)= LOWER(cast(hier.sbclass_num as STRING))
 AND LOWER(src0.class_num)= LOWER(cast(hier.class_num as STRING)) 
 AND LOWER(src0.department_num)= LOWER(cast(hier.dept_num as STRING))
-- AND src0.eff_period OVERLAPS PERIOD(hier.eff_begin_tmstp, hier.eff_end_tmstp)
AND RANGE_OVERLAPS(SRC0.eff_period,range(hier.eff_begin_tmstp_utc,hier.eff_end_tmstp_utc))

) SRC
LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_SKU_DIM_HIST TGT
ON SRC.epm_sku_num = TGT.epm_sku_num
AND LOWER(SRC.channel_country) = LOWER(TGT.channel_country)
-- AND SRC.eff_period OVERLAPS PERIOD(TGT.eff_begin_tmstp, TGT.eff_end_tmstp)
AND RANGE_OVERLAPS(RANGE(TGT.eff_begin_tmstp_utc,TGT.eff_end_tmstp_utc), SRC.eff_period )
WHERE ( TGT.epm_sku_num IS NULL OR 
   (  (LOWER(SRC.rms_sku_num) <> LOWER(TGT.rms_sku_num) OR (SRC.rms_sku_num IS NOT NULL AND TGT.rms_sku_num IS NULL) OR (SRC.rms_sku_num IS NULL AND TGT.rms_sku_num IS NOT NULL))
   OR (LOWER(SRC.sku_short_desc) <> LOWER(TGT.sku_short_desc) OR (SRC.sku_short_desc IS NOT NULL AND TGT.sku_short_desc IS NULL) OR (SRC.sku_short_desc IS NULL AND TGT.sku_short_desc IS NOT NULL))
   OR (LOWER(SRC.sku_desc) <> LOWER(TGT.sku_desc) OR (SRC.sku_desc IS NOT NULL AND TGT.sku_desc IS NULL) OR (SRC.sku_desc IS NULL AND TGT.sku_desc IS NOT NULL))
   OR (LOWER(SRC.rms_style_num) <> LOWER(TGT.rms_style_num) OR (SRC.rms_style_num IS NOT NULL AND TGT.rms_style_num IS NULL) OR (SRC.rms_style_num IS NULL AND TGT.rms_style_num IS NOT NULL))
   OR (SRC.epm_style_num <> TGT.epm_style_num OR (SRC.epm_style_num IS NOT NULL AND TGT.epm_style_num IS NULL) OR (SRC.epm_style_num IS NULL AND TGT.epm_style_num IS NOT NULL))
   OR (LOWER(SRC.style_desc) <> LOWER(TGT.style_desc) OR (SRC.style_desc IS NOT NULL AND TGT.style_desc IS NULL) OR (SRC.style_desc IS NULL AND TGT.style_desc IS NOT NULL))
   OR (LOWER(SRC.supp_part_num) <> LOWER(TGT.supp_part_num) OR (SRC.supp_part_num IS NOT NULL AND TGT.supp_part_num IS NULL) OR (SRC.supp_part_num IS NULL AND TGT.supp_part_num IS NOT NULL))
   OR (LOWER(SRC.prmy_supp_num) <> LOWER(TGT.prmy_supp_num) OR (SRC.prmy_supp_num IS NOT NULL AND TGT.prmy_supp_num IS NULL) OR (SRC.prmy_supp_num IS NULL AND TGT.prmy_supp_num IS NOT NULL))
   OR (LOWER(SRC.manufacturer_num) <> LOWER(TGT.manufacturer_num) OR (SRC.manufacturer_num IS NOT NULL AND TGT.manufacturer_num IS NULL) OR (SRC.manufacturer_num IS NULL AND TGT.manufacturer_num IS NOT NULL))
   OR (SRC.sbclass_num <> cast(TGT.sbclass_num as STRING) OR (SRC.sbclass_num IS NOT NULL AND TGT.sbclass_num IS NULL) OR (SRC.sbclass_num IS NULL AND TGT.sbclass_num IS NOT NULL))
   OR (LOWER(SRC.sbclass_desc) <> LOWER(TGT.sbclass_desc) OR (SRC.sbclass_desc IS NOT NULL AND TGT.sbclass_desc IS NULL) OR (SRC.sbclass_desc IS NULL AND TGT.sbclass_desc IS NOT NULL))
   OR (SRC.class_num <> cast(TGT.class_num as STRING) OR (SRC.class_num IS NOT NULL AND TGT.class_num IS NULL) OR (SRC.class_num IS NULL AND TGT.class_num IS NOT NULL))
   OR (LOWER(SRC.class_desc) <> LOWER(TGT.class_desc) OR (SRC.class_desc IS NOT NULL AND TGT.class_desc IS NULL) OR (SRC.class_desc IS NULL AND TGT.class_desc IS NOT NULL))
   OR (SRC.dept_num <> cast(TGT.dept_num as STRING) OR (SRC.dept_num IS NOT NULL AND TGT.dept_num IS NULL) OR (SRC.dept_num IS NULL AND TGT.dept_num IS NOT NULL))
   OR (LOWER(SRC.dept_desc) <> LOWER(TGT.dept_desc) OR (SRC.dept_desc IS NOT NULL AND TGT.dept_desc IS NULL) OR (SRC.dept_desc IS NULL AND TGT.dept_desc IS NOT NULL))
   OR (SRC.grp_num <> TGT.grp_num OR (SRC.grp_num IS NOT NULL AND TGT.grp_num IS NULL) OR (SRC.grp_num IS NULL AND TGT.grp_num IS NOT NULL))
   OR (LOWER(SRC.grp_desc) <> LOWER(TGT.grp_desc) OR (SRC.grp_desc IS NOT NULL AND TGT.grp_desc IS NULL) OR (SRC.grp_desc IS NULL AND TGT.grp_desc IS NOT NULL))
   OR (SRC.div_num <> TGT.div_num OR (SRC.div_num IS NOT NULL AND TGT.div_num IS NULL) OR (SRC.div_num IS NULL AND TGT.div_num IS NOT NULL))
   OR (LOWER(SRC.div_desc) <> LOWER(TGT.div_desc) OR (SRC.div_desc IS NOT NULL AND TGT.div_desc IS NULL) OR (SRC.div_desc IS NULL AND TGT.div_desc IS NOT NULL))
   OR (SRC.cmpy_num <> TGT.cmpy_num OR (SRC.cmpy_num IS NOT NULL AND TGT.cmpy_num IS NULL) OR (SRC.cmpy_num IS NULL AND TGT.cmpy_num IS NOT NULL))
   OR (LOWER(SRC.cmpy_desc) <> LOWER(TGT.cmpy_desc) OR (SRC.cmpy_desc IS NOT NULL AND TGT.cmpy_desc IS NULL) OR (SRC.cmpy_desc IS NULL AND TGT.cmpy_desc IS NOT NULL))
   OR (LOWER(SRC.color_num) <> LOWER(TGT.color_num) OR (SRC.color_num IS NOT NULL AND TGT.color_num IS NULL) OR (SRC.color_num IS NULL AND TGT.color_num IS NOT NULL))
   OR (LOWER(SRC.supp_color) <> LOWER(TGT.supp_color) OR (SRC.supp_color IS NOT NULL AND TGT.supp_color IS NULL) OR (SRC.supp_color IS NULL AND TGT.supp_color IS NOT NULL))
   )  )
) NRML
;
--.IF ERRORCODE <> 0 THEN .QUIT 2


--Explicit transaction on Teradata ensures that we do not corrupt target table if failure in middle of updating target table
BEGIN TRANSACTION;
--.IF ERRORCODE <> 0 THEN .QUIT 3

--SEQUENCED VALIDTIME
DELETE FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt
WHERE EXISTS (SELECT *
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.casepack_sku_dim_vtw AS src
    WHERE epm_sku_num = tgt.epm_sku_num 
    AND  LOWER(channel_country) =  LOWER(tgt.channel_country) 
    AND  LOWER(tgt.sku_type_code) =  LOWER('P'));


--.IF ERRORCODE <> 0 THEN .QUIT 4
INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_sku_dim (rms_sku_num, epm_sku_num, channel_country, sku_short_desc, sku_desc,
 rms_style_num, epm_style_num, style_desc, supp_part_num, prmy_supp_num, manufacturer_num, sbclass_num, sbclass_desc,
 class_num, class_desc, dept_num, dept_desc, grp_num, grp_desc, div_num, div_desc, cmpy_num, cmpy_desc, color_num,
 supp_color, sku_type_code, sku_type_desc, pack_orderable_code, pack_orderable_desc, pack_sellable_code,
 pack_sellable_desc, pack_simple_code, pack_simple_desc, eff_begin_tmstp,eff_begin_tmstp_tz, eff_end_tmstp, eff_end_tmstp_tz,dw_batch_id, dw_batch_date,
 dw_sys_load_tmstp)
(SELECT DISTINCT rms_sku_num,
  epm_sku_num,
  channel_country,
  sku_short_desc,
  sku_desc,
  rms_style_num,
  epm_style_num,
  style_desc,
  supp_part_num,
  prmy_supp_num,
  manufacturer_num,
  sbclass_num,
  sbclass_desc,
  class_num,
  class_desc,
  dept_num,
  dept_desc,
  grp_num,
  grp_desc,
  div_num,
  div_desc,
  cmpy_num,
  cmpy_desc,
  color_num,
  supp_color,
  sku_type_code,
  sku_type_desc,
  pack_orderable_code,
  pack_orderable_desc,
  pack_sellable_code,
  pack_sellable_desc,
  pack_simple_code,
  pack_simple_desc,
  eff_begin_tmstp,
  eff_begin_tmstp_tz,
  eff_end_tmstp,
  eff_end_tmstp_tz,
  dw_batch_id,
  dw_batch_date,
  dw_sys_load_tmstp
 FROM (SELECT rms_sku_num,
    epm_sku_num,
    channel_country,
    sku_short_desc,
    sku_desc,
    rms_style_num,
    epm_style_num,
    style_desc,
    supp_part_num,
    prmy_supp_num,
    manufacturer_num,
    sbclass_num,
    sbclass_desc,
    class_num,
    class_desc,
    dept_num,
    dept_desc,
    grp_num,
    grp_desc,
    div_num,
    div_desc,
    cmpy_num,
    cmpy_desc,
    color_num,
    supp_color,
    sku_type_code,
    sku_type_desc,
    pack_orderable_code,
    pack_orderable_desc,
    pack_sellable_code,
    pack_sellable_desc,
    pack_simple_code,
    pack_simple_desc,
    eff_begin_tmstp,
	 eff_begin_tmstp_tz,
    eff_end_tmstp,
	 eff_end_tmstp_tz,
     (SELECT batch_id
     FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
     WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT')) AS dw_batch_id,
     (SELECT curr_batch_date
     FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
     WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT')) AS dw_batch_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.casepack_sku_dim_vtw)AS t3
 WHERE NOT EXISTS (SELECT 1 AS `a12180`
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_sku_dim
   WHERE epm_sku_num = t3.epm_sku_num
    AND channel_country = t3.channel_country)
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY epm_sku_num, channel_country)) = 1);

--.IF ERRORCODE <> 0 THEN .QUIT 5


--Check for cases where we might need to end-date certain records because the RMS9 Sku changed
--from one EPM key to another.
UPDATE {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt SET
 eff_end_tmstp = t3.new_end_tmstp, 
 eff_end_tmstp_tz = jwn_udf.udf_time_zone(cast(t3.new_end_tmstp as string))
 
 FROM (SELECT rms_sku_num,
   channel_country,
   epm_sku_num,
   sku_desc,
   sku_type_code,
   eff_begin_tmstp,
   eff_end_tmstp AS old_eff_tmstp,eff_end_tmstp_tz,
   MIN(eff_begin_tmstp) OVER (PARTITION BY rms_sku_num, channel_country ORDER BY eff_begin_tmstp, epm_sku_num
    ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS new_end_tmstp
  FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_sku_dim
  WHERE LOWER(rms_sku_num) <> LOWER('')
  QUALIFY eff_end_tmstp > (MIN(eff_begin_tmstp) OVER (PARTITION BY rms_sku_num, channel_country ORDER BY eff_begin_tmstp
       , epm_sku_num ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING))) AS t3
WHERE LOWER(t3.rms_sku_num) = LOWER(tgt.rms_sku_num) 
AND LOWER(t3.channel_country) = LOWER(tgt.channel_country) 
AND t3.epm_sku_num = tgt.epm_sku_num 
AND t3.eff_begin_tmstp = tgt.eff_begin_tmstp 
AND t3.new_end_tmstp > tgt.eff_begin_tmstp;

--.IF ERRORCODE <> 0 THEN .QUIT 6


COMMIT TRANSACTION;
--.IF ERRORCODE <> 0 THEN .QUIT 7


-- COLLECT STATISTICS COLUMN(rms_sku_num), COLUMN(channel_country), COLUMN(epm_sku_num), COLUMN(epm_style_num)
-- , COLUMN(eff_end_tmstp), COLUMN(sku_type_code), COLUMN(color_num), COLUMN(sbclass_num, class_num, dept_num)
-- ON {{params.gcp_project_id}}.{{params.dbenv}}_NAP_DIM.PRODUCT_SKU_DIM;

