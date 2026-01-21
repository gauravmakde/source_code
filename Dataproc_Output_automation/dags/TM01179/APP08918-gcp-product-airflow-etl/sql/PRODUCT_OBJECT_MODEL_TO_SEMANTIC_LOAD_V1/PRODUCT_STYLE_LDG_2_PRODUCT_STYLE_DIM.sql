TRUNCATE TABLE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_style_dim_vtw;


INSERT INTO `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_style_dim_vtw (epm_style_num, rms_style_num_list, channel_country, style_desc,
 style_group_num, style_group_desc, style_group_short_desc, type_level_1_num, type_level_1_desc, type_level_2_num,
 type_level_2_desc, sbclass_num, sbclass_desc, class_num, class_desc, dept_num, dept_desc, grp_num, grp_desc, div_num,
 div_desc, cmpy_num, cmpy_desc, nord_label_ind, vendor_label_code, vendor_label_name, special_treatment_type_code,
 size_standard, supp_part_num, prmy_supp_num, manufacturer_num, product_source_code, product_source_desc, genders_code,
 genders_desc, age_groups_code, age_groups_desc, msrp_amt, msrp_currency_code, hanger_type_code, hanger_type_desc,
 npg_ind, npg_sourcing_code
 , eff_begin_tmstp
 ,eff_begin_tmstp_tz
, eff_end_tmstp
,eff_end_tmstp_tz
)

WITH SRC_1 AS (
  SELECT DISTINCT          
           id,
           legacyrmsstyleids_,
           marketcode,
           description,
           rmsstylegroupid,
           style_group_desc,
           style_group_short_desc,
           typelevel1number,
           typelevel1description,
           typelevel2number,
           typelevel2description,
           subclassnumber,
           subclassdescription,
           classnumber,
           classdescription,
           departmentnumber,
           departmentdescription,
           nord_label_ind,
           labelid,
           labelname,
           specialtreatmenttypecode,
           sizes_standardcode,
           supp_part_num,
           prmy_supp_num,
           manufacturer_num,
           sourcecode,
           sourcedescription,
           genders_code,
           genders_description,
           agegroups_code,
           agegroups_description,
           msrpamtplaintxt,
           msrpcurrcycd,
           hangertypecode,
           hangertypedescription,
            CASE
            WHEN LOWER(vendorupcs_vendors_isnpgvendor) = LOWER('true')
            THEN 'Y'
            ELSE 'N'
            END AS vendorupcs_vendors_isnpgvendor,
           sourcingcategories_code
           , RANGE(eff_begin_tmstp, eff_end_tmstp) AS eff_period,
           eff_begin_tmstp,
           eff_end_tmstp
          from  (
                                                   --inner normalize
            SELECT id,legacyrmsstyleids_,marketcode,description,rmsstylegroupid,style_group_desc,style_group_short_desc,typelevel1number,typelevel1description,typelevel2number,typelevel2description,subclassnumber,subclassdescription,classnumber,classdescription,departmentnumber,departmentdescription,nord_label_ind,labelid,labelname,specialtreatmenttypecode,sizes_standardcode,prmy_supp_num,prmy_supp_name,supp_part_num,manufacturer_num,sourcecode,sourcedescription,genders_code,genders_description,agegroups_code,agegroups_description,msrpamtplaintxt,msrpcurrcycd,hangertypecode,hangertypedescription,vendorupcs_vendors_isnpgvendor,sourcingcategories_code, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY id,legacyrmsstyleids_ ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM

        (
        SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY id,legacyrmsstyleids_ ORDER BY eff_begin_tmstp) >= 
                DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
         from (
	        SELECT SRC_2.*
	      , COALESCE(MAX(eff_begin_tmstp) OVER(PARTITION BY id,marketcode ORDER BY eff_begin_tmstp
	           ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
	          ,TIMESTAMP'9999-12-31 23:59:59.999999+00:00') AS eff_end_tmstp
          FROM (

            SELECT DISTINCT id,
             legacyrmsstyleids_,
             marketcode,
             description,
             rmsstylegroupid,
              CASE
              WHEN LOWER(rmsstylegroupid) <> LOWER('')
              THEN description
              ELSE NULL
              END AS style_group_desc,
              CASE
              WHEN LOWER(rmsstylegroupid) <> LOWER('')
              THEN shortdescription
              ELSE NULL
              END AS style_group_short_desc,
             typelevel1number,
             typelevel1description,
             typelevel2number,
             typelevel2description,
             subclassnumber,
             subclassdescription,
             classnumber,
             classdescription,
             departmentnumber,
             departmentdescription,
              CASE
              WHEN LOWER(isnordstromlabel) = LOWER('true')
              THEN 'Y'
              ELSE 'N'
              END AS nord_label_ind,
             labelid,
             labelname,
             specialtreatmenttypecode,
             sizes_standardcode,
             MAX(CASE
               WHEN LOWER(vendor_is_supplier) = LOWER('true') AND LOWER(vendorupcs_vendors_isprimary) = LOWER('true')
               THEN vendorupcs_vendors_number
               ELSE NULL
               END) OVER (PARTITION BY id, marketcode, sourcepublishtimestamp RANGE BETWEEN UNBOUNDED PRECEDING AND
              UNBOUNDED FOLLOWING) AS prmy_supp_num,
             MAX(CASE
               WHEN LOWER(vendor_is_supplier) = LOWER('true') AND LOWER(vendorupcs_vendors_isprimary) = LOWER('true')
               THEN vendorupcs_vendors_name
               ELSE NULL
               END) OVER (PARTITION BY id, marketcode, sourcepublishtimestamp RANGE BETWEEN UNBOUNDED PRECEDING AND
              UNBOUNDED FOLLOWING) AS prmy_supp_name,
             MAX(CASE
               WHEN LOWER(vendor_is_supplier) = LOWER('true') AND LOWER(vendorupcs_vendors_isprimary) = LOWER('true')
               THEN vendorupcs_vendors_vendorproductnumbers_code
               ELSE NULL
               END) OVER (PARTITION BY id, marketcode, sourcepublishtimestamp RANGE BETWEEN UNBOUNDED PRECEDING AND
              UNBOUNDED FOLLOWING) AS supp_part_num,
             MAX(CASE
               WHEN LOWER(vendor_is_manufacturer) = LOWER('true') AND LOWER(vendorupcs_vendors_isprimary) = LOWER('true'
                  )
               THEN vendorupcs_vendors_number
               ELSE NULL
               END) OVER (PARTITION BY id, marketcode, sourcepublishtimestamp RANGE BETWEEN UNBOUNDED PRECEDING AND
              UNBOUNDED FOLLOWING) AS manufacturer_num,
             sourcecode,
             sourcedescription,
             genders_code,
             genders_description,
             agegroups_code,
             agegroups_description,
             msrpamtplaintxt,
             msrpcurrcycd,
             hangertypecode,
             hangertypedescription,
             MAX(CASE
               WHEN LOWER(vendor_is_supplier) = LOWER('true') AND LOWER(vendorupcs_vendors_isprimary) = LOWER('true')
               THEN vendorupcs_vendors_isnpgvendor
               ELSE NULL
               END) OVER (PARTITION BY id, marketcode, sourcepublishtimestamp RANGE BETWEEN UNBOUNDED PRECEDING AND
              UNBOUNDED FOLLOWING) AS vendorupcs_vendors_isnpgvendor,
             sourcingcategories_code,
            --  `{{params.dataplex_project_id}}.NORD_UDF.EPOCH_TMSTP`(CAST(TRUNC(CAST(sourcepublishtimestamp as float64)) AS INT64)) as eff_begin_tmstp
             cast(`{{params.dataplex_project_id}}.JWN_UDF.ISO8601_TMSTP`(sourcepublishtimestamp) as timestamp) as eff_begin_tmstp 
            FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_style_ldg) 
            AS SRC_2
          QUALIFY eff_begin_tmstp < COALESCE(MAX(eff_begin_tmstp) OVER(PARTITION BY id,marketcode ORDER BY eff_begin_tmstp
	           ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
	          ,TIMESTAMP'9999-12-31 23:59:59.999999+00:00') ) SRC_1
) AS ordered_data
) AS grouped_data
GROUP BY id,legacyrmsstyleids_,marketcode,description,rmsstylegroupid,style_group_desc,style_group_short_desc,typelevel1number,typelevel1description,typelevel2number,typelevel2description,subclassnumber,subclassdescription,classnumber,classdescription,departmentnumber,departmentdescription,nord_label_ind,labelid,labelname,specialtreatmenttypecode,sizes_standardcode,prmy_supp_num,prmy_supp_name,supp_part_num,manufacturer_num,sourcecode,sourcedescription,genders_code,genders_description,agegroups_code,agegroups_description,msrpamtplaintxt,msrpcurrcycd,hangertypecode,hangertypedescription,vendorupcs_vendors_isnpgvendor,sourcingcategories_code, range_group
ORDER BY  id,legacyrmsstyleids_, eff_begin_tmstp)),


SRC_3 AS (
  SELECT DISTINCT         
         id AS epm_style_num,
         legacyrmsstyleids_ AS rms_style_num_list,
         marketcode AS channel_country,
         description AS style_desc,
         rmsstylegroupid AS style_group_num,
         style_group_desc,
         style_group_short_desc,
         typelevel1number AS type_level_1_num,
         typelevel1description AS type_level_1_desc,
         typelevel2number AS type_level_2_num,
         typelevel2description AS type_level_2_desc,
         subclassnumber AS sbclass_num,
         subclassdescription AS sbclass_desc,
         classnumber AS class_num,
         classdescription AS class_desc,
         departmentnumber AS dept_num,
         departmentdescription AS dept_desc,
         subdivision_num AS grp_num,
         subdivision_short_name AS grp_desc,
         division_num AS div_num,
         division_short_name AS div_desc,
         '1000' AS cmpy_num,
         'Nordstrom' AS cmpy_desc,
         nord_label_ind,
         labelid AS vendor_label_code,
         labelname AS vendor_label_name,
         specialtreatmenttypecode AS special_treatment_type_code,
         sizes_standardcode AS size_standard,
         supp_part_num,
         prmy_supp_num,
         manufacturer_num,
         sourcecode AS product_source_code,
         sourcedescription AS product_source_desc,
         genders_code,
         genders_description AS genders_desc,
         agegroups_code AS age_groups_code,
         agegroups_description AS age_groups_desc,
         msrpamtplaintxt AS msrp_amt,
         msrpcurrcycd AS msrp_currency_code,
         hangertypecode AS hanger_type_code,
         hangertypedescription AS hanger_type_desc,
         vendorupcs_vendors_isnpgvendor AS npg_ind,
         sourcingcategories_code AS npg_sourcing_code,
         COALESCE(SAFE.RANGE_INTERSECT( eff_period ,RANGE(eff_begin_tmstp_utc, eff_end_tmstp_utc))
	          , eff_period ) AS eff_period
        FROM (
    --inner normalize
            SELECT id,legacyrmsstyleids_,marketcode,description,rmsstylegroupid,style_group_desc,style_group_short_desc,typelevel1number,typelevel1description,typelevel2number,typelevel2description,subclassnumber,subclassdescription,classnumber,classdescription,departmentnumber,departmentdescription,nord_label_ind,labelid,labelname,specialtreatmenttypecode,sizes_standardcode,supp_part_num,prmy_supp_num,manufacturer_num,sourcecode,sourcedescription,genders_code,genders_description,agegroups_code,agegroups_description,msrpamtplaintxt,msrpcurrcycd,hangertypecode,hangertypedescription,vendorupcs_vendors_isnpgvendor,sourcingcategories_code,eff_period,subdivision_num,subdivision_short_name,division_num,division_short_name,eff_begin_tmstp_utc, eff_end_tmstp_utc, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY id,legacyrmsstyleids_ ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM

(
        SELECT SRC0.*,
              dept.subdivision_num,
              dept.subdivision_short_name,
              dept.division_num,
              dept.division_short_name,
              dept.eff_begin_tmstp_utc, 
              dept.eff_end_tmstp_utc,
            CASE 
                WHEN CAST(LAG(SRC0.eff_end_tmstp) OVER (PARTITION BY id,legacyrmsstyleids_ ORDER BY SRC0.eff_begin_tmstp) AS TIMESTAMP) >= 
                DATE_SUB(SRC0.eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
         from
        SRC_1  AS SRC0
         LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.department_dim_hist AS dept 
         ON dept.dept_num = CAST(TRIM(SRC0.departmentnumber) AS FLOAT64)
         AND RANGE_OVERLAPS(src0.eff_period ,RANGE(dept.eff_begin_tmstp_utc,dept.eff_end_tmstp_utc))) AS ordered_data
) AS grouped_data
GROUP BY id,legacyrmsstyleids_,marketcode,description,rmsstylegroupid,style_group_desc,style_group_short_desc,typelevel1number,typelevel1description,typelevel2number,typelevel2description,subclassnumber,subclassdescription,classnumber,classdescription,departmentnumber,departmentdescription,nord_label_ind,labelid,labelname,specialtreatmenttypecode,sizes_standardcode,supp_part_num,prmy_supp_num,manufacturer_num,sourcecode,sourcedescription,genders_code,genders_description,agegroups_code,agegroups_description,msrpamtplaintxt,msrpcurrcycd,hangertypecode,hangertypedescription,vendorupcs_vendors_isnpgvendor,sourcingcategories_code,eff_period,subdivision_num,subdivision_short_name,division_num,division_short_name,eff_begin_tmstp_utc, eff_end_tmstp_utc, range_group
ORDER BY  id,legacyrmsstyleids_, eff_begin_tmstp)

          
        UNION ALL
        SELECT DISTINCT               
         epm_style_num,
         rms_style_num_list,
         channel_country,
         style_desc,
         style_group_num,
         style_group_desc,
         style_group_short_desc,
         type_level_1_num,
         type_level_1_desc,
         type_level_2_num,
         type_level_2_desc,
         SUBSTR(CAST(sbclass_num AS STRING), 1, 60) AS sty0_sbclass_num,
         sbclass_desc,
         SUBSTR(CAST(class_num AS STRING), 1, 60) AS sty0_class_num,
         class_desc,
         SUBSTR(CAST(dept_num AS STRING), 1, 60) AS sty0_dept_num,
         dept_desc,
         subdivision_num AS grp_num,
         subdivision_short_name AS grp_desc,
         division_num AS div_num,
         division_short_name AS div_desc,
         SUBSTR(CAST(cmpy_num AS STRING), 1, 60) AS sty0_cmpy_num,
         cmpy_desc,
         nord_label_ind,
         vendor_label_code,
         vendor_label_name,
         special_treatment_type_code,
         size_standard,
         supp_part_num,
         prmy_supp_num,
         manufacturer_num,
         product_source_code,
         product_source_desc,
         genders_code,
         genders_desc,
         age_groups_code,
         age_groups_desc,
         msrp_amt,
         msrp_currency_code,
         hanger_type_code,
         hanger_type_desc,
         npg_ind,
         npg_sourcing_code,
         eff_period

        from  (
    --inner normalize
            SELECT epm_style_num,rms_style_num_list,channel_country,style_desc,style_group_num,style_group_desc,style_group_short_desc,type_level_1_num,type_level_1_desc,type_level_2_num,type_level_2_desc,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,cmpy_num,cmpy_desc,nord_label_ind,vendor_label_code,vendor_label_name,special_treatment_type_code,size_standard,supp_part_num,prmy_supp_num,manufacturer_num,product_source_code,product_source_desc,genders_code,genders_desc,age_groups_code,age_groups_desc,msrp_amt,msrp_currency_code,hanger_type_code,hanger_type_desc,npg_ind,npg_sourcing_code,eff_begin_tmstp_utc,eff_end_tmstp_utc,subdivision_num,subdivision_short_name,division_num,division_short_name,eff_period, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY epm_style_num,rms_style_num_list ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM

(
        SELECT sty0.*,
            dept0.subdivision_num,
            dept0.subdivision_short_name,
            dept0.division_num,
            dept0.division_short_name,
            COALESCE(SAFE.RANGE_INTERSECT( RANGE(sty0.eff_begin_tmstp_utc, sty0.eff_end_tmstp_utc) , RANGE(dept0.eff_begin_tmstp_utc, dept0.eff_end_tmstp_utc)) , RANGE(sty0.eff_begin_tmstp_utc, sty0.eff_end_tmstp_utc) ) AS eff_period,
            CASE 
                WHEN LAG(sty0.eff_end_tmstp) OVER (PARTITION BY epm_style_num,rms_style_num_list ORDER BY sty0.eff_begin_tmstp) >= 
                DATE_SUB(sty0.eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
         from `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_style_dim_hist AS sty0

         LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.department_dim_hist AS dept0 
         ON dept0.dept_num = CAST(TRIM(FORMAT('%11d', sty0.dept_num)) AS FLOAT64)
         AND RANGE_OVERLAPS(RANGE(sty0.eff_begin_tmstp,sty0.eff_end_tmstp) , RANGE(dept0.eff_begin_tmstp, dept0.eff_end_tmstp))
          
        WHERE sty0.dw_sys_load_tmstp >= DATETIME_SUB(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
           ,INTERVAL 2 DAY)
         AND NOT EXISTS (SELECT 1
          FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_style_ldg AS ldg
          WHERE id = sty0.epm_style_num
           AND LOWER(marketcode) = LOWER(sty0.channel_country))
) AS ordered_data
) AS grouped_data
GROUP BY epm_style_num,rms_style_num_list,channel_country,style_desc,style_group_num,style_group_desc,style_group_short_desc,type_level_1_num,type_level_1_desc,type_level_2_num,type_level_2_desc,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,cmpy_num,cmpy_desc,nord_label_ind,vendor_label_code,vendor_label_name,special_treatment_type_code,size_standard,supp_part_num,prmy_supp_num,manufacturer_num,product_source_code,product_source_desc,genders_code,genders_desc,age_groups_code,age_groups_desc,msrp_amt,msrp_currency_code,hanger_type_code,hanger_type_desc,npg_ind,npg_sourcing_code,eff_begin_tmstp_utc,eff_end_tmstp_utc,subdivision_num,subdivision_short_name,division_num,division_short_name,eff_period, range_group
ORDER BY  epm_style_num,rms_style_num_list, eff_begin_tmstp))




(SELECT DISTINCT epm_style_num,
  rms_style_num_list,
  channel_country,
  style_desc,
  style_group_num,
  style_group_desc,
  style_group_short_desc,
  type_level_1_num,
  type_level_1_desc,
  type_level_2_num,
  type_level_2_desc,
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
  nord_label_ind,
  vendor_label_code,
  vendor_label_name,
  special_treatment_type_code,
  size_standard,
  supp_part_num,
  prmy_supp_num,
  manufacturer_num,
  product_source_code,
  product_source_desc,
  genders_code,
  genders_desc,
  age_groups_code,
  age_groups_desc,
  msrp_amt,
  msrp_currency_code,
  hanger_type_code,
  hanger_type_desc,
  npg_ind,
  npg_sourcing_code
  ,RANGE_START(eff_period) AS eff_begin
  ,`{{params.dataplex_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(RANGE_START(eff_period) AS STRING)) AS eff_begin_tmstp_tz
  , RANGE_END(eff_period)   AS eff_end
  ,`{{params.dataplex_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(RANGE_END(eff_period) AS STRING)) AS eff_end_tmstp_tz
 FROM (SELECT              
    epm_style_num,
    rms_style_num_list,
    channel_country,
    style_desc,
    style_group_num,
    style_group_desc,
    style_group_short_desc,
    type_level_1_num,
    type_level_1_desc,
    type_level_2_num,
    type_level_2_desc,
    CAST(sbclass_num AS INTEGER) AS sbclass_num,
    sbclass_desc,
    CAST(class_num AS INTEGER) AS class_num,
    class_desc,
    CAST(dept_num AS INTEGER) AS dept_num,
    dept_desc,
    grp_num,
    grp_desc,
    div_num,
    div_desc,
    CAST(cmpy_num AS SMALLINT) AS cmpy_num,
    cmpy_desc,
    nord_label_ind,
    vendor_label_code,
    vendor_label_name,
    special_treatment_type_code,
    size_standard,
    supp_part_num,
    prmy_supp_num,
    manufacturer_num,
    product_source_code,
    product_source_desc,
    genders_code,
    genders_desc,
    age_groups_code,
    age_groups_desc,
    ROUND(CAST(msrp_amt AS NUMERIC), 2) AS msrp_amt,
    msrp_currency_code,
    hanger_type_code,
    hanger_type_desc,
    npg_ind,
    npg_sourcing_code
    , eff_period
    , eff_begin_tmstp_tz
    , eff_end_tmstp_tz
   FROM 
 (
    --inner normalize
            SELECT epm_style_num,rms_style_num_list,channel_country,style_desc,style_group_num,style_group_desc,style_group_short_desc,type_level_1_num,type_level_1_desc,type_level_2_num,type_level_2_desc,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num,div_desc,cmpy_num,cmpy_desc,nord_label_ind,vendor_label_code,vendor_label_name,special_treatment_type_code,size_standard,supp_part_num,prmy_supp_num,manufacturer_num,product_source_code,product_source_desc,genders_code,genders_desc,age_groups_code,msrp_amt,age_groups_desc,msrp_currency_code,hanger_type_code,hanger_type_desc,npg_ind,npg_sourcing_code,eff_period,eff_begin_tmstp_tz,eff_end_tmstp_tz, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY epm_style_num,rms_style_num_list,channel_country,style_desc,style_group_num,style_group_desc,style_group_short_desc,type_level_1_num,type_level_1_desc,type_level_2_num,type_level_2_desc,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num,div_desc,cmpy_num,cmpy_desc,nord_label_ind,vendor_label_code,vendor_label_name,special_treatment_type_code,size_standard,supp_part_num,prmy_supp_num,manufacturer_num,product_source_code,product_source_desc,genders_code,genders_desc,age_groups_code,age_groups_desc,msrp_currency_code,hanger_type_code,hanger_type_desc,npg_ind,npg_sourcing_code,eff_period ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM

(
        SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY epm_style_num,rms_style_num_list,channel_country,style_desc,style_group_num,style_group_desc,style_group_short_desc,type_level_1_num,type_level_1_desc,type_level_2_num,type_level_2_desc,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num,div_desc,cmpy_num,cmpy_desc,nord_label_ind,vendor_label_code,vendor_label_name,special_treatment_type_code,size_standard,supp_part_num,prmy_supp_num,manufacturer_num,product_source_code,product_source_desc,genders_code,genders_desc,age_groups_code,age_groups_desc,msrp_currency_code,hanger_type_code,hanger_type_desc,npg_ind,npg_sourcing_code,eff_period ORDER BY eff_begin_tmstp) >= 
                DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
FROM (SELECT DISTINCT 
     SRC.epm_style_num,
      SRC.rms_style_num_list,
      SRC.channel_country,
      SRC.style_desc,
      SRC.style_group_num,
      SRC.style_group_desc,
      SRC.style_group_short_desc,
      SRC.type_level_1_num,
      SRC.type_level_1_desc,
      SRC.type_level_2_num,
      SRC.type_level_2_desc,
      SRC.sbclass_num,
      SRC.sbclass_desc,
      SRC.class_num,
      SRC.class_desc,
      SRC.dept_num,
      SRC.dept_desc,
      SRC.grp_num,
      SRC.grp_desc,
      SRC.div_num,
      SRC.div_desc,
      SRC.cmpy_num,
      SRC.cmpy_desc,
      SRC.nord_label_ind,
      SRC.vendor_label_code,
      SRC.vendor_label_name,
      SRC.special_treatment_type_code,
      SRC.size_standard,
      SRC.supp_part_num,
      SRC.prmy_supp_num,
      SRC.manufacturer_num,
      SRC.product_source_code,
      SRC.product_source_desc,
      SRC.genders_code,
      SRC.genders_desc,
      SRC.age_groups_code,
      SRC.age_groups_desc,
      SRC.msrp_amt,
      SRC.msrp_currency_code,
      SRC.hanger_type_code,
      SRC.hanger_type_desc,
      SRC.npg_ind,
      SRC.npg_sourcing_code,
       COALESCE(  SAFE.RANGE_INTERSECT (SRC.eff_period,RANGE(TGT.eff_begin_tmstp_utc,TGT.eff_end_tmstp_utc))
          , SRC.eff_period ) AS eff_period,
          TGT.eff_begin_tmstp_tz,
          TGT.eff_end_tmstp_tz,
          TGT.eff_end_tmstp,
          TGT.eff_begin_tmstp

     FROM SRC_3 AS SRC

      LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_style_dim_hist AS tgt 
      ON SRC.epm_style_num = tgt.epm_style_num 
      AND LOWER(SRC.channel_country) = LOWER(tgt.channel_country)
      AND RANGE_OVERLAPS(SRC.eff_period , RANGE(tgt.eff_begin_tmstp_utc, tgt.eff_end_tmstp_utc))
     WHERE tgt.channel_country IS NULL
      OR (LOWER(SRC.rms_style_num_list) <> LOWER(tgt.rms_style_num_list) OR 
      tgt.rms_style_num_list IS NULL AND SRC.rms_style_num_list
         IS NOT NULL)

      OR (SRC.rms_style_num_list IS NULL AND tgt.rms_style_num_list IS NOT NULL 
      OR LOWER(SRC.style_desc) <> LOWER(tgt.style_desc) 
      OR (tgt.style_desc IS NULL AND SRC.style_desc IS NOT NULL 
      OR SRC.style_desc IS NULL AND tgt.style_desc
           IS NOT NULL))

      OR (LOWER(SRC.style_group_num) <> LOWER(tgt.style_group_num) 
      OR tgt.style_group_num IS NULL AND SRC.style_group_num IS NOT NULL 
      OR (SRC.style_group_num IS NULL AND tgt.style_group_num IS NOT NULL OR LOWER(SRC.style_group_desc
             ) <> LOWER(tgt.style_group_desc)) OR (tgt.style_group_desc IS NULL AND SRC.style_group_desc IS NOT NULL OR
            SRC.style_group_desc IS NULL AND tgt.style_group_desc IS NOT NULL OR (LOWER(SRC.style_group_short_desc) <>
             LOWER(tgt.style_group_short_desc) OR tgt.style_group_short_desc IS NULL AND SRC.style_group_short_desc
             IS NOT NULL)))
      OR (SRC.style_group_short_desc IS NULL AND tgt.style_group_short_desc IS NOT NULL OR (LOWER(SRC.type_level_1_num)
             <> LOWER(tgt.type_level_1_num) OR tgt.type_level_1_num IS NULL AND SRC.type_level_1_num IS NOT NULL) OR (SRC
             .type_level_1_num IS NULL AND tgt.type_level_1_num IS NOT NULL OR LOWER(SRC.type_level_1_desc) <> LOWER(tgt
              .type_level_1_desc) OR (tgt.type_level_1_desc IS NULL AND SRC.type_level_1_desc IS NOT NULL OR SRC.type_level_1_desc
              IS NULL AND tgt.type_level_1_desc IS NOT NULL)) OR (LOWER(SRC.type_level_2_num) <> LOWER(tgt.type_level_2_num
              ) OR tgt.type_level_2_num IS NULL AND SRC.type_level_2_num IS NOT NULL OR (SRC.type_level_2_num IS NULL
              AND tgt.type_level_2_num IS NOT NULL OR LOWER(SRC.type_level_2_desc) <> LOWER(tgt.type_level_2_desc)) OR (tgt
              .type_level_2_desc IS NULL AND SRC.type_level_2_desc IS NOT NULL OR SRC.type_level_2_desc IS NULL AND tgt
              .type_level_2_desc IS NOT NULL OR (CAST(SRC.sbclass_num AS FLOAT64) <> tgt.sbclass_num OR tgt.sbclass_num
               IS NULL AND SRC.sbclass_num IS NOT NULL))))
      OR (SRC.sbclass_num IS NULL AND tgt.sbclass_num IS NOT NULL OR (LOWER(SRC.sbclass_desc) <> LOWER(tgt.sbclass_desc
               ) OR tgt.sbclass_desc IS NULL AND SRC.sbclass_desc IS NOT NULL) OR (SRC.sbclass_desc IS NULL AND tgt.sbclass_desc
              IS NOT NULL OR CAST(SRC.class_num AS FLOAT64) <> tgt.class_num OR (tgt.class_num IS NULL AND SRC.class_num
               IS NOT NULL OR SRC.class_num IS NULL AND tgt.class_num IS NOT NULL)) OR (LOWER(SRC.class_desc) <> LOWER(tgt
               .class_desc) OR tgt.class_desc IS NULL AND SRC.class_desc IS NOT NULL OR (SRC.class_desc IS NULL AND tgt
               .class_desc IS NOT NULL OR CAST(SRC.dept_num AS FLOAT64) <> tgt.dept_num) OR (tgt.dept_num IS NULL AND
               SRC.dept_num IS NOT NULL OR SRC.dept_num IS NULL AND tgt.dept_num IS NOT NULL OR (LOWER(SRC.dept_desc) <>
                LOWER(tgt.dept_desc) OR tgt.dept_desc IS NULL AND SRC.dept_desc IS NOT NULL))) OR (SRC.dept_desc IS NULL
              AND tgt.dept_desc IS NOT NULL OR SRC.grp_num <> tgt.grp_num OR (tgt.grp_num IS NULL AND SRC.grp_num
               IS NOT NULL OR SRC.grp_num IS NULL AND tgt.grp_num IS NOT NULL) OR (LOWER(SRC.grp_desc) <> LOWER(tgt.grp_desc
                ) OR tgt.grp_desc IS NULL AND SRC.grp_desc IS NOT NULL OR (SRC.grp_desc IS NULL AND tgt.grp_desc
                IS NOT NULL OR SRC.div_num <> tgt.div_num)) OR (tgt.div_num IS NULL AND SRC.div_num IS NOT NULL OR SRC.div_num
               IS NULL AND tgt.div_num IS NOT NULL OR (LOWER(SRC.div_desc) <> LOWER(tgt.div_desc) OR tgt.div_desc
                IS NULL AND SRC.div_desc IS NOT NULL) OR (SRC.div_desc IS NULL AND tgt.div_desc IS NOT NULL OR SRC.cmpy_num
                 <> CAST(tgt.cmpy_num AS STRING) OR (tgt.cmpy_num IS NULL OR LOWER(SRC.cmpy_desc) <> LOWER(tgt.cmpy_desc
                  ))))))
      OR (tgt.cmpy_desc IS NULL AND SRC.cmpy_desc IS NOT NULL OR (SRC.cmpy_desc IS NULL AND tgt.cmpy_desc IS NOT NULL OR
               LOWER(SRC.nord_label_ind) <> LOWER(tgt.nord_label_ind)) OR (tgt.nord_label_ind IS NULL AND SRC.nord_label_ind
               IS NOT NULL OR SRC.nord_label_ind IS NULL AND tgt.nord_label_ind IS NOT NULL OR (LOWER(SRC.vendor_label_code
                 ) <> LOWER(tgt.vendor_label_code) OR tgt.vendor_label_code IS NULL AND SRC.vendor_label_code
                IS NOT NULL)) OR (SRC.vendor_label_code IS NULL AND tgt.vendor_label_code IS NOT NULL OR LOWER(SRC.vendor_label_name
                ) <> LOWER(tgt.vendor_label_name) OR (tgt.vendor_label_name IS NULL AND SRC.vendor_label_name
                IS NOT NULL OR SRC.vendor_label_name IS NULL AND tgt.vendor_label_name IS NOT NULL) OR (LOWER(SRC.special_treatment_type_code
                 ) <> LOWER(tgt.special_treatment_type_code) OR tgt.special_treatment_type_code IS NULL AND SRC.special_treatment_type_code
                IS NOT NULL OR (SRC.special_treatment_type_code IS NULL AND tgt.special_treatment_type_code IS NOT NULL
                OR LOWER(SRC.size_standard) <> LOWER(tgt.size_standard)))) OR (tgt.size_standard IS NULL AND SRC.size_standard
               IS NOT NULL OR SRC.size_standard IS NULL AND tgt.size_standard IS NOT NULL OR (LOWER(SRC.supp_part_num)
                <> LOWER(tgt.supp_part_num) OR tgt.supp_part_num IS NULL AND SRC.supp_part_num IS NOT NULL) OR (SRC.supp_part_num
                IS NULL AND tgt.supp_part_num IS NOT NULL OR LOWER(SRC.prmy_supp_num) <> LOWER(tgt.prmy_supp_num) OR (tgt
                 .prmy_supp_num IS NULL AND SRC.prmy_supp_num IS NOT NULL OR SRC.prmy_supp_num IS NULL AND tgt.prmy_supp_num
                 IS NOT NULL)) OR (LOWER(SRC.manufacturer_num) <> LOWER(tgt.manufacturer_num) OR tgt.manufacturer_num
                IS NULL AND SRC.manufacturer_num IS NOT NULL OR (SRC.manufacturer_num IS NULL AND tgt.manufacturer_num
                 IS NOT NULL OR LOWER(SRC.product_source_code) <> LOWER(tgt.product_source_code)) OR (tgt.product_source_code
                 IS NULL AND SRC.product_source_code IS NOT NULL OR SRC.product_source_code IS NULL AND tgt.product_source_code
                 IS NOT NULL OR (LOWER(SRC.product_source_desc) <> LOWER(tgt.product_source_desc) OR tgt.product_source_desc
                  IS NULL AND SRC.product_source_desc IS NOT NULL)))) OR (SRC.product_source_desc IS NULL AND tgt.product_source_desc
              IS NOT NULL OR (LOWER(SRC.genders_code) <> LOWER(tgt.genders_code) OR tgt.genders_code IS NULL AND SRC.genders_code
                IS NOT NULL) OR (SRC.genders_code IS NULL AND tgt.genders_code IS NOT NULL OR LOWER(SRC.genders_desc) <>
                LOWER(tgt.genders_desc) OR (tgt.genders_desc IS NULL AND SRC.genders_desc IS NOT NULL OR SRC.genders_desc
                 IS NULL AND tgt.genders_desc IS NOT NULL)) OR (LOWER(SRC.age_groups_code) <> LOWER(tgt.age_groups_code
                 ) OR tgt.age_groups_code IS NULL AND SRC.age_groups_code IS NOT NULL OR (SRC.age_groups_code IS NULL
                 AND tgt.age_groups_code IS NOT NULL OR LOWER(SRC.age_groups_desc) <> LOWER(tgt.age_groups_desc)) OR (tgt
                 .age_groups_desc IS NULL AND SRC.age_groups_desc IS NOT NULL OR SRC.age_groups_desc IS NULL AND tgt.age_groups_desc
                 IS NOT NULL OR (SRC.msrp_amt <> tgt.msrp_amt OR tgt.msrp_amt IS NULL AND SRC.msrp_amt IS NOT NULL))) OR
           (SRC.msrp_amt IS NULL AND tgt.msrp_amt IS NOT NULL OR LOWER(SRC.msrp_currency_code) <> LOWER(tgt.msrp_currency_code
                 ) OR (tgt.msrp_currency_code IS NULL AND SRC.msrp_currency_code IS NOT NULL OR SRC.msrp_currency_code
                 IS NULL AND tgt.msrp_currency_code IS NOT NULL) OR (LOWER(SRC.hanger_type_code) <> LOWER(tgt.hanger_type_code
                  ) OR tgt.hanger_type_code IS NULL AND SRC.hanger_type_code IS NOT NULL OR (SRC.hanger_type_code
                  IS NULL AND tgt.hanger_type_code IS NOT NULL OR LOWER(SRC.hanger_type_desc) <> LOWER(tgt.hanger_type_desc
                   ))) OR (tgt.hanger_type_desc IS NULL AND SRC.hanger_type_desc IS NOT NULL OR SRC.hanger_type_desc
                 IS NULL AND tgt.hanger_type_desc IS NOT NULL OR (LOWER(SRC.npg_ind) <> LOWER(tgt.npg_ind) OR tgt.npg_ind
                  IS NULL AND SRC.npg_ind IS NOT NULL) OR (SRC.npg_ind IS NULL AND tgt.npg_ind IS NOT NULL OR LOWER(SRC
                   .npg_sourcing_code) <> LOWER(tgt.npg_sourcing_code) OR (tgt.npg_sourcing_code IS NULL AND SRC.npg_sourcing_code
                   IS NOT NULL OR SRC.npg_sourcing_code IS NULL AND tgt.npg_sourcing_code IS NOT NULL))))))
     ) AS NRML
       WHERE NOT EXISTS (SELECT 1 AS `A12180`
      FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_style_dim_vtw
      WHERE epm_style_num = NRML.epm_style_num
       AND lower(channel_country) = lower(NRML.channel_country))
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY epm_style_num, channel_country)) = 1) AS ordered_data
) AS grouped_data
GROUP BY epm_style_num,rms_style_num_list,channel_country,style_desc,style_group_num,style_group_desc,style_group_short_desc,type_level_1_num,type_level_1_desc,type_level_2_num,type_level_2_desc,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num,div_desc,cmpy_num,cmpy_desc,nord_label_ind,vendor_label_code,vendor_label_name,special_treatment_type_code,size_standard,supp_part_num,prmy_supp_num,manufacturer_num,product_source_code,product_source_desc,genders_code,genders_desc,age_groups_code,msrp_amt,age_groups_desc,msrp_currency_code,hanger_type_code,hanger_type_desc,npg_ind,npg_sourcing_code,eff_period,eff_begin_tmstp_tz,eff_end_tmstp_tz
ORDER BY  epm_style_num,rms_style_num_list,channel_country, eff_begin_tmstp)))
 ;


--SEQUENCED VALIDTIME
-- DELETE FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_style_dim AS tgt
-- WHERE EXISTS (SELECT *
--  FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_style_dim_vtw AS src
--  WHERE epm_style_num = tgt.epm_style_num
--   AND LOWER(channel_country) = LOWER(tgt.channel_country));

DELETE FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_style_dim AS tgt
WHERE EXISTS (
SELECT 1 
FROM 
`{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_style_dim_vtw AS src
WHERE 
src.epm_style_num = tgt.epm_style_num
AND LOWER(src.channel_country) = LOWER(tgt.channel_country)
AND src.eff_begin_tmstp <= tgt.eff_begin_tmstp
    AND src.eff_end_tmstp >= tgt.eff_end_tmstp
);



UPDATE  `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_style_dim AS tgt
SET tgt.eff_end_tmstp = src.eff_begin_tmstp
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_style_dim_vtw AS src
WHERE src.epm_style_num = tgt.epm_style_num
AND LOWER(src.channel_country) = LOWER(tgt.channel_country)
    AND src.eff_begin_tmstp > tgt.eff_begin_tmstp
    AND src.eff_begin_tmstp <= tgt.eff_end_tmstp
    AND src.eff_end_tmstp >= tgt.eff_end_tmstp ;


UPDATE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_style_dim AS tgt
SET tgt.eff_begin_tmstp = src.eff_end_tmstp
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_style_dim_vtw AS src
WHERE src.epm_style_num = tgt.epm_style_num
AND LOWER(src.channel_country) = LOWER(tgt.channel_country)
    AND src.eff_end_tmstp >= tgt.eff_begin_tmstp
    AND src.eff_begin_tmstp < tgt.eff_begin_tmstp
    AND src.eff_end_tmstp <= tgt.eff_end_tmstp;


INSERT INTO `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_style_dim(epm_style_num,rms_style_num_list,channel_country,style_desc,style_group_num,style_group_desc,style_group_short_desc,type_level_1_num,type_level_1_desc,type_level_2_num,type_level_2_desc,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num,div_desc,cmpy_num,cmpy_desc,nord_label_ind,vendor_label_code,vendor_label_name,special_treatment_type_code,size_standard,supp_part_num,prmy_supp_num,manufacturer_num,product_source_code,product_source_desc,genders_code,genders_desc,age_groups_code,age_groups_desc,msrp_amt,msrp_currency_code,hanger_type_code,hanger_type_desc,npg_ind,npg_sourcing_code,eff_begin_tmstp_tz,eff_end_tmstp_tz,dw_batch_id,dw_batch_date,dw_sys_load_tmstp,eff_begin_tmstp,eff_end_tmstp)
WITH tbl AS 
(SELECT tgt.*, 
src.eff_begin_tmstp AS src_eff_begin_tmstp, 
src.eff_end_tmstp AS src_eff_end_tmstp,  
tgt.eff_begin_tmstp AS tgt_eff_begin_tmstp, 
tgt.eff_end_tmstp AS tgt_eff_end_tmstp 
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_style_dim_vtw AS src 
INNER JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_style_dim AS tgt ON src.epm_style_num = tgt.epm_style_num 
AND tgt.eff_begin_tmstp < src.eff_begin_tmstp 
AND tgt.eff_end_tmstp > src.eff_end_tmstp)

SELECT * except(eff_begin_tmstp, eff_end_tmstp, src_eff_begin_tmstp, src_eff_end_tmstp, tgt_eff_begin_tmstp,tgt_eff_end_tmstp), tgt_eff_begin_tmstp, src_eff_begin_tmstp FROM tbl
UNION ALL
SELECT * except(eff_begin_tmstp, eff_end_tmstp, src_eff_begin_tmstp, src_eff_end_tmstp, tgt_eff_begin_tmstp,tgt_eff_end_tmstp), src_eff_end_tmstp, tgt_eff_end_tmstp FROM tbl;

DELETE FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_style_dim AS tgt 
WHERE EXISTS (SELECT 1 FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_style_dim_vtw AS src 
WHERE src.epm_style_num = tgt.epm_style_num AND tgt.eff_begin_tmstp < src.eff_begin_tmstp 
and tgt.eff_end_tmstp > src.eff_end_tmstp);


INSERT INTO `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_style_dim (epm_style_num, rms_style_num_list, channel_country, style_desc,
 style_group_num, style_group_desc, style_group_short_desc, type_level_1_num, type_level_1_desc, type_level_2_num,
 type_level_2_desc, sbclass_num, sbclass_desc, class_num, class_desc, dept_num, dept_desc, grp_num, grp_desc, div_num,
 div_desc, cmpy_num, cmpy_desc, nord_label_ind, vendor_label_code, vendor_label_name, special_treatment_type_code,
 size_standard, supp_part_num, prmy_supp_num, manufacturer_num, product_source_code, product_source_desc, genders_code,
 genders_desc, age_groups_code, age_groups_desc, msrp_amt, msrp_currency_code, hanger_type_code, hanger_type_desc,
 npg_ind, npg_sourcing_code, eff_begin_tmstp,eff_begin_tmstp_tz, eff_end_tmstp,eff_end_tmstp_tz, dw_batch_id, dw_batch_date, dw_sys_load_tmstp)
(SELECT DISTINCT epm_style_num,
  rms_style_num_list,
  channel_country,
  style_desc,
  style_group_num,
  style_group_desc,
  style_group_short_desc,
  type_level_1_num,
  type_level_1_desc,
  type_level_2_num,
  type_level_2_desc,
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
  nord_label_ind,
  vendor_label_code,
  vendor_label_name,
  special_treatment_type_code,
  size_standard,
  supp_part_num,
  prmy_supp_num,
  manufacturer_num,
  product_source_code,
  product_source_desc,
  genders_code,
  genders_desc,
  age_groups_code,
  age_groups_desc,
  msrp_amt,
  msrp_currency_code,
  hanger_type_code,
  hanger_type_desc,
  npg_ind,
  npg_sourcing_code,
  eff_begin_tmstp,
  eff_begin_tmstp_tz,
  eff_end_tmstp,
  eff_end_tmstp_tz,
  dw_batch_id,
  dw_batch_date,
  dw_sys_load_tmstp
 FROM (SELECT epm_style_num,
    rms_style_num_list,
    channel_country,
    style_desc,
    style_group_num,
    style_group_desc,
    style_group_short_desc,
    type_level_1_num,
    type_level_1_desc,
    type_level_2_num,
    type_level_2_desc,
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
    nord_label_ind,
    vendor_label_code,
    vendor_label_name,
    special_treatment_type_code,
    size_standard,
    supp_part_num,
    prmy_supp_num,
    manufacturer_num,
    product_source_code,
    product_source_desc,
    genders_code,
    genders_desc,
    age_groups_code,
    age_groups_desc,
    msrp_amt,
    msrp_currency_code,
    hanger_type_code,
    hanger_type_desc,
    npg_ind,
    npg_sourcing_code,
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
    (current_datetime('PST8PDT')) AS dw_sys_load_tmstp
   FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_style_dim_vtw) AS t3
 WHERE NOT EXISTS (SELECT 1 AS `A12180`
   FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_style_dim
   WHERE epm_style_num = t3.epm_style_num
    AND LOWER(channel_country) = LOWER(t3.channel_country))
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY epm_style_num, channel_country)) = 1);

 