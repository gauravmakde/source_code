
--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=stg-dim;AppSubArea=NAP_PRODUCT;' UPDATE FOR SESSION;


--NONSEQUENCED VALIDTIME --Purge work table for staging temporal rows
TRUNCATE TABLE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.casepack_sku_dim_vtw;
----.IF ERRORCODE <> 0 THEN .QUIT 1



INSERT INTO `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.casepack_sku_dim_vtw
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


WITH SRC_1 as
 (select  rms_casepack_num, epm_casepack_num, channel_country, sku_short_desc, sku_desc
	, rms_style_num, epm_style_num, style_desc, vendor_part_num, prmy_supp_num, manufacturer_num
	, subclass_num, class_num, department_num, color_num, vendor_color
	, eff_begin_tmstp, eff_end_tmstp
--  range(eff_begin_tmstp,eff_end_tmstp) as eff_period 
 from  (
    --inner normalize
            SELECT rms_casepack_num, epm_casepack_num, channel_country, sku_short_desc, sku_desc
	, rms_style_num, epm_style_num, style_desc, vendor_part_num, prmy_supp_num, manufacturer_num
	, subclass_num, class_num, department_num, color_num, vendor_color, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_casepack_num, epm_casepack_num, channel_country, sku_short_desc, sku_desc
	, rms_style_num, epm_style_num, style_desc, vendor_part_num, prmy_supp_num, manufacturer_num
	, subclass_num, class_num, department_num, color_num, vendor_color ORDER BY eff_begin_tmstp 
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS range_group
    FROM (
        SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY rms_casepack_num, epm_casepack_num, channel_country, sku_short_desc, sku_desc
	, rms_style_num, epm_style_num, style_desc, vendor_part_num, prmy_supp_num, manufacturer_num
	, subclass_num, class_num, department_num, color_num, vendor_color ORDER BY eff_begin_tmstp) >= 
                DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
         from 
				 (	SELECT SRC_2.*
	, COALESCE(MAX((eff_begin_tmstp)) OVER(PARTITION BY epm_casepack_num,channel_country ORDER BY eff_begin_tmstp
	           ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING
             )
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
  ,cast(`{{params.dataplex_project_id}}.JWN_UDF.ISO8601_TMSTP`( COLLATE(sourcepublishtimestamp,''))as timestamp) AS eff_begin_tmstp
  FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.casepack_sku_ldg
	) SRC_2
	QUALIFY eff_begin_tmstp < eff_end_tmstp
	)
	) AS ordered_data
) AS grouped_data
GROUP BY rms_casepack_num, epm_casepack_num, channel_country, sku_short_desc, sku_desc
	, rms_style_num, epm_style_num, style_desc, vendor_part_num, prmy_supp_num, manufacturer_num
	, subclass_num, class_num, department_num, color_num, vendor_color, range_group
ORDER BY rms_casepack_num, epm_casepack_num, channel_country, sku_short_desc, sku_desc
	, rms_style_num, epm_style_num, style_desc, vendor_part_num, prmy_supp_num, manufacturer_num
	, subclass_num, class_num, department_num, color_num, vendor_color, eff_begin_tmstp) ) ,





 src_3 as
(SELECT 
  rms_casepack_num AS rms_sku_num
, epm_casepack_num AS epm_sku_num
, channel_country
, sku_short_desc
, sku_desc
, rms_style_num
, epm_style_num
, style_desc
, vendor_part_num AS supp_part_num
, prmy_supp_num
, manufacturer_num
, subclass_num AS sbclass_num
, sbclass_desc
, class_num
, class_desc
, department_num AS dept_num
, dept_desc
, grp_num
, grp_desc
, div_num
, div_desc
, cmpy_num
, cmpy_desc
, color_num
, vendor_color AS supp_color
,eff_period1 as eff_period
--  range(eff_begin_tmstp,eff_end_tmstp) as eff_period 
 from  (
    --inner normalize
            SELECT   rms_casepack_num, epm_casepack_num, channel_country, sku_short_desc, sku_desc, rms_style_num, epm_style_num, style_desc, vendor_part_num, prmy_supp_num, manufacturer_num, subclass_num, sbclass_desc, class_num, class_desc, department_num, dept_desc, grp_num, grp_desc, div_num, div_desc, cmpy_num, cmpy_desc, color_num, vendor_color, MIN(eff_begin_tmstp_utc) AS eff_begin_tmstp, MAX(eff_end_tmstp_utc) AS eff_end_tmstp,eff_period1
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY   rms_casepack_num, epm_casepack_num, channel_country, sku_short_desc, sku_desc, rms_style_num, epm_style_num, style_desc, vendor_part_num, prmy_supp_num, manufacturer_num, subclass_num, sbclass_desc, class_num, class_desc, department_num, dept_desc, grp_num, grp_desc, div_num, div_desc, cmpy_num, cmpy_desc, color_num, vendor_color ORDER BY eff_begin_tmstp_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
    FROM (
        SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp_utc) OVER (PARTITION BY   rms_casepack_num, epm_casepack_num, channel_country, sku_short_desc, sku_desc, rms_style_num, epm_style_num, style_desc, vendor_part_num, prmy_supp_num, manufacturer_num, subclass_num, sbclass_desc, src0.class_num, class_desc, department_num, dept_desc, grp_num, grp_desc, div_num, div_desc, cmpy_num, cmpy_desc, color_num, vendor_color ORDER BY eff_begin_tmstp_utc) >= 
                DATE_SUB(eff_begin_tmstp_utc, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag,
						COALESCE( SAFE.RANGE_INTERSECT (range(eff_begin_tmstp_utc, eff_end_tmstp_utc),range(hier.eff_begin_tmstp_utc,hier.eff_end_tmstp_utc))
          , range(eff_begin_tmstp_utc, eff_end_tmstp_utc) ) AS eff_period1
         from(
					(
	SELECT 
   rms_casepack_num, epm_casepack_num, channel_country, sku_short_desc, sku_desc
	, rms_style_num, epm_style_num, style_desc, vendor_part_num, prmy_supp_num, manufacturer_num
	, subclass_num, class_num, department_num, color_num, vendor_color
	,RANGE( eff_begin_tmstp, eff_end_tmstp)AS eff_period
	FROM  SRC_1
) SRC0
LEFT JOIN (SELECT subclass_num as sbclass_num, subclass_short_name as sbclass_desc, class_num as class_num1 , class_short_name as class_desc, dept_num, dept_name as dept_desc,
subdivision_num AS grp_num,subdivision_name AS grp_desc,division_num AS div_num,division_name AS div_desc, CAST(1000 AS SMALLINT) AS cmpy_num
, 'Nordstrom'  AS cmpy_desc,eff_begin_tmstp_utc,eff_end_tmstp_utc,RANGE(eff_begin_tmstp_utc,eff_end_tmstp_utc) as eff_period
from `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.department_class_subclass_dim_hist )hier
ON LOWER(src0.subclass_num)= LOWER(cast(hier.sbclass_num as STRING))
 AND LOWER(src0.class_num)= LOWER(cast(hier.class_num1 as STRING)) 
 AND LOWER(src0.department_num)= LOWER(cast(hier.dept_num as STRING))
AND RANGE_OVERLAPS(SRC0.eff_period,range(hier.eff_begin_tmstp_utc,hier.eff_end_tmstp_utc))
)) AS ordered_data
) AS grouped_data
GROUP BY  rms_casepack_num, epm_casepack_num, channel_country, sku_short_desc, sku_desc, rms_style_num, epm_style_num, style_desc, vendor_part_num, prmy_supp_num, manufacturer_num, subclass_num, sbclass_desc,class_num, class_desc, department_num, dept_desc, grp_num, grp_desc, div_num, div_desc, cmpy_num, cmpy_desc, color_num, vendor_color,eff_period1
ORDER BY   rms_casepack_num, epm_casepack_num, channel_country, sku_short_desc, sku_desc, rms_style_num, epm_style_num, style_desc, vendor_part_num, prmy_supp_num, manufacturer_num, subclass_num, sbclass_desc,class_num, class_desc, department_num, dept_desc, grp_num, grp_desc, div_num, div_desc, cmpy_num, cmpy_desc, color_num, vendor_color, eff_begin_tmstp) ) 




SELECT
rms_sku_num
, epm_sku_num
, channel_country
, sku_short_desc
, sku_desc
, rms_style_num
, epm_style_num
, style_desc
, supp_part_num
, prmy_supp_num
, manufacturer_num
, cast(trunc(CAST(sbclass_num AS FLOAT64)) as int64)
, sbclass_desc
, cast(trunc(CAST(class_num AS FLOAT64)) as int64)
, class_desc
, cast(trunc(CAST(dept_num AS FLOAT64)) as int64)
, dept_desc
, grp_num
, grp_desc
, div_num
, div_desc
, cmpy_num
, cmpy_desc
, color_num
, supp_color
, 'P' AS sku_type_code
, 'Pack Item' AS sku_type_desc
, 'V' AS pack_orderable_code
, 'Vendor Orderable Pack' AS pack_orderable_desc
, 'N' AS pack_sellable_code
, 'Non-Sellable Pack' AS pack_sellable_desc
, 'C' AS pack_simple_code
, 'Complex Pack' AS pack_simple_desc
-- , BEGIN(eff_period) AS eff_begin
-- , END(eff_period)   AS eff_end
, CAST(min(RANGE_START(eff_period))AS TIMESTAMP) AS eff_begin
, CAST(max(RANGE_END(eff_period)) AS TIMESTAMP)  AS eff_end
,`{{params.dataplex_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(min(RANGE_START(eff_period)) as string)) as eff_begin_tmstp_tz
,`{{params.dataplex_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(max(RANGE_END(eff_period)) as string)) as eff_end_tmstp_tz

FROM (
--NONSEQUENCED VALIDTIME
SELECT  rms_sku_num, epm_sku_num, channel_country, sku_short_desc, sku_desc, rms_style_num, epm_style_num, style_desc, supp_part_num, prmy_supp_num, manufacturer_num, sbclass_num, sbclass_desc, class_num, class_desc, dept_num, dept_desc, grp_num, grp_desc, div_num, div_desc, cmpy_num, cmpy_desc, color_num, supp_color,eff_period,range_group
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num, epm_sku_num, channel_country, sku_short_desc, sku_desc, rms_style_num, epm_style_num, style_desc, supp_part_num, prmy_supp_num, manufacturer_num, sbclass_num, sbclass_desc, class_num, class_desc, dept_num, dept_desc, grp_num, grp_desc, div_num, div_desc, cmpy_num, cmpy_desc, color_num, supp_color ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM
      --  NONSEQUENCED VALIDTIME
      (
        
            --  NORMALIZE
         SELECT     SRC.rms_sku_num, SRC.epm_sku_num, SRC.channel_country, SRC.sku_short_desc, SRC.sku_desc, SRC.rms_style_num, SRC.epm_style_num, SRC.style_desc, SRC.supp_part_num, SRC.prmy_supp_num, SRC.manufacturer_num, SRC.sbclass_num, SRC.sbclass_desc, SRC.class_num, SRC.class_desc, SRC.dept_num, SRC.dept_desc, SRC.grp_num, SRC.grp_desc, SRC.div_num, SRC.div_desc, SRC.cmpy_num, SRC.cmpy_desc, SRC.color_num, SRC.supp_color,TGT.eff_begin_tmstp,TGT.eff_end_tmstp,
            CASE 
                WHEN LAG(eff_end_tmstp_utc) OVER (PARTITION BY     SRC.rms_sku_num, SRC.epm_sku_num, SRC.channel_country, SRC.sku_short_desc, SRC.sku_desc, SRC.rms_style_num, SRC.epm_style_num, SRC.style_desc, SRC.supp_part_num, SRC.prmy_supp_num, SRC.manufacturer_num, SRC.sbclass_num, SRC.sbclass_desc, SRC.class_num, SRC.class_desc, SRC.dept_num, SRC.dept_desc, SRC.grp_num, SRC.grp_desc, SRC.div_num, SRC.div_desc, SRC.cmpy_num, SRC.cmpy_desc, SRC.color_num, SRC.supp_color ORDER BY eff_begin_tmstp_utc) >= 
                DATE_SUB(eff_begin_tmstp_utc, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag,
             COALESCE( SAFE.RANGE_INTERSECT (SRC.eff_period,range(TGT.eff_begin_tmstp_utc,TGT.eff_end_tmstp_utc))
          , SRC.eff_period ) AS eff_period ,
             FROM
            
(SELECT 
--normalize
  rms_sku_num
, epm_sku_num
, channel_country
, sku_short_desc
, sku_desc
, rms_style_num
, epm_style_num
, style_desc
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
, supp_color
,eff_period		  
FROM   SRC_3) as src
LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist TGT

ON SRC.epm_sku_num = TGT.epm_sku_num
AND LOWER(SRC.channel_country) = LOWER(TGT.channel_country)
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
) as ordered_data
) AS grouped_data)
GROUP BY rms_sku_num, epm_sku_num, channel_country, sku_short_desc, sku_desc, rms_style_num, epm_style_num, style_desc, supp_part_num, prmy_supp_num, manufacturer_num, sbclass_num, sbclass_desc, class_num, class_desc, dept_num, dept_desc, grp_num, grp_desc, div_num, div_desc, cmpy_num, cmpy_desc, color_num, supp_color,range_group,eff_period
ORDER BY rms_sku_num, epm_sku_num, channel_country, sku_short_desc, sku_desc, rms_style_num, epm_style_num, style_desc, supp_part_num, prmy_supp_num, manufacturer_num, sbclass_num, sbclass_desc, class_num, class_desc, dept_num, dept_desc, grp_num, grp_desc, div_num, div_desc, cmpy_num, cmpy_desc, color_num, supp_color
;


--.IF ERRORCODE <> 0 THEN .QUIT 2


--Explicit transaction on Teradata ensures that we do not corrupt target table if failure in middle of updating target table
BEGIN TRANSACTION;
--.IF ERRORCODE <> 0 THEN .QUIT 3

--SEQUENCED VALIDTIME
DELETE FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt
WHERE EXISTS (SELECT *
    FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.casepack_sku_dim_vtw AS src
    WHERE epm_sku_num = tgt.epm_sku_num 
    AND  LOWER(channel_country) =  LOWER(tgt.channel_country) 
    AND  LOWER(tgt.sku_type_code) =  LOWER('P')
    AND src.eff_begin_tmstp <= tgt.eff_begin_tmstp
    AND src.eff_end_tmstp >= tgt.eff_end_tmstp
    );

UPDATE  `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS TGT
SET TGT.eff_end_tmstp = SRC.eff_begin_tmstp
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.casepack_sku_dim_vtw AS SRC
WHERE SRC.epm_sku_num = TGT.epm_sku_num 
    AND  LOWER(SRC.channel_country) =  LOWER(TGT.channel_country) 
    AND  LOWER(TGT.sku_type_code) =  LOWER('P')
    -- AND SRC.eff_begin_tmstp >= TGT.eff_begin_tmstp
    -- AND SRC.eff_begin_tmstp < TGT.eff_end_tmstp 
    -- AND SRC.eff_end_tmstp > TGT.eff_end_tmstp ;
    AND SRC.eff_begin_tmstp > TGT.eff_begin_tmstp
    AND SRC.eff_begin_tmstp <= TGT.eff_end_tmstp
AND SRC.eff_end_tmstp >= TGT.eff_end_tmstp;

UPDATE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS TGT
SET TGT.eff_begin_tmstp = SRC.eff_end_tmstp
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.casepack_sku_dim_vtw AS SRC
WHERE SRC.epm_sku_num = TGT.epm_sku_num 
    AND  LOWER(SRC.channel_country) =  LOWER(TGT.channel_country) 
    AND  LOWER(TGT.sku_type_code) =  LOWER('P')
    -- AND SRC.eff_end_tmstp > TGT.eff_begin_tmstp
    -- AND SRC.eff_end_tmstp <= TGT.eff_end_tmstp;
     AND SRC.eff_end_tmstp >= TGT.eff_begin_tmstp
    AND SRC.eff_begin_tmstp < TGT.eff_begin_tmstp
AND SRC.eff_end_tmstp <= TGT.eff_end_tmstp;

insert into `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim(rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num,div_desc,cmpy_num,cmpy_desc,color_num,color_desc,nord_display_color,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,brand_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,sku_type_code,sku_type_desc,pack_orderable_code,pack_orderable_desc,pack_sellable_code,pack_sellable_desc,pack_simple_code,pack_simple_desc,display_seq_1,display_seq_2,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code,eff_begin_tmstp_tz,eff_end_tmstp_tz,dw_batch_id,dw_batch_date,dw_sys_load_tmstp,eff_begin_tmstp,eff_end_tmstp)

with tbl as 
(select 
tgt.*,
cast(src.eff_begin_tmstp as timestamp) AS src_eff_begin_tmstp, 
cast(src.eff_end_tmstp as timestamp) AS src_eff_end_tmstp, 
tgt.eff_begin_tmstp AS tgt_eff_begin_tmstp, 
tgt.eff_end_tmstp AS tgt_eff_end_tmstp 
from `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.casepack_sku_dim_vtw AS SRC 
inner join `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS TGT
on SRC.epm_sku_num = TGT.epm_sku_num 
and tgt.eff_begin_tmstp < src.eff_begin_tmstp 
and tgt.eff_end_tmstp > src.eff_end_tmstp)

select *  except(eff_begin_tmstp, eff_end_tmstp, src_eff_begin_tmstp, src_eff_end_tmstp, tgt_eff_begin_tmstp,tgt_eff_end_tmstp ), tgt_eff_begin_tmstp, cast(src_eff_begin_tmstp as timestamp) from tbl
union all
select * except(eff_begin_tmstp, eff_end_tmstp, src_eff_begin_tmstp, src_eff_end_tmstp, tgt_eff_begin_tmstp,tgt_eff_end_tmstp), cast(src_eff_end_tmstp as timestamp), tgt_eff_end_tmstp from tbl;

delete from `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS TGT where exists (select 1 from `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.casepack_sku_dim_vtw AS SRC
 where src.epm_sku_num = tgt.epm_sku_num and tgt.eff_begin_tmstp < src.eff_begin_tmstp and tgt.eff_end_tmstp > src.eff_end_tmstp);

--.IF ERRORCODE <> 0 THEN .QUIT 4
INSERT INTO `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim (rms_sku_num, epm_sku_num, channel_country, sku_short_desc, sku_desc,
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
     FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
     WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT')) AS dw_batch_id,
     (SELECT curr_batch_date
     FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
     WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT')) AS dw_batch_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
   FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.casepack_sku_dim_vtw)AS t3
 WHERE NOT EXISTS (SELECT 1 AS `a12180`
   FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim
   WHERE epm_sku_num = t3.epm_sku_num
    AND LOWER(channel_country) = LOWER(t3.channel_country))
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY epm_sku_num, channel_country)) = 1);


--Check for cases where we might need to end-date certain records because the RMS9 Sku changed
--from one EPM key to another.
UPDATE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt SET
 eff_end_tmstp = t3.new_end_tmstp, 
 eff_end_tmstp_tz = `{{params.dataplex_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(t3.new_end_tmstp as string))
 
 FROM (SELECT rms_sku_num,
   channel_country,
   epm_sku_num,
   sku_desc,
   sku_type_code,
   eff_begin_tmstp,
   eff_end_tmstp AS old_eff_tmstp,eff_end_tmstp_tz,
   MIN(eff_begin_tmstp) OVER (PARTITION BY rms_sku_num, channel_country ORDER BY eff_begin_tmstp, epm_sku_num
    ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS new_end_tmstp
  FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim
  WHERE LOWER(rms_sku_num) <> LOWER('')
  QUALIFY eff_end_tmstp > (MIN(eff_begin_tmstp) OVER (PARTITION BY rms_sku_num, channel_country ORDER BY eff_begin_tmstp
       , epm_sku_num ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING))) AS t3
WHERE LOWER(t3.rms_sku_num) = LOWER(tgt.rms_sku_num) 
AND LOWER(t3.channel_country) = LOWER(tgt.channel_country) 
AND t3.epm_sku_num = tgt.epm_sku_num 
AND t3.eff_begin_tmstp = tgt.eff_begin_tmstp 
AND t3.new_end_tmstp > tgt.eff_begin_tmstp;


COMMIT TRANSACTION;