
-- SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=style_rms_view-epm_rms_style_xref;AppSubArea=NAP_PRODUCT;' UPDATE FOR SESSION;

--Explicit transaction on Teradata ensures that we do not corrupt target table if failure in middle of updating target table

-- .IF ERRORCODE <> 0 THEN .QUIT 1

--Truncate the xref table before each load
TRUNCATE TABLE {{params.gcp_project_id}}.{{params.dbenv}}_NAP_DIM.PRODUCT_EPM_RMS_STYLE_XREF;
-- .IF ERRORCODE <> 0 THEN .QUIT 2

--Load the xref table with the view data
INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_NAP_DIM.PRODUCT_EPM_RMS_STYLE_XREF
(
epm_style_num,
rms_seq_num,
rms_style_num,
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
sbclass_long_desc,
sbclass_desc,
class_num,
class_long_desc,
class_desc,
dept_num,
dept_desc,
dept_short_desc,
grp_num,
grp_desc,
grp_short_desc,
div_num,
div_desc,
div_short_desc,
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
hanger_type_code,
hanger_type_desc,
npg_ind,
npg_sourcing_code,
vpn_label,
dw_batch_id,
dw_batch_date,
dw_sys_load_tmstp
)
WITH sku_dim AS (SELECT rms_style_num,
 epm_style_num,
 channel_country,
 MAX(dw_batch_date) AS dw_batch_date,
 MAX(dw_sys_load_tmstp) AS dw_sys_load_tmstp,
 MAX(dw_batch_id) AS dw_batch_id,
 MAX(TRIM(supp_part_num)) AS supp_part_num
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_sku_lkup
WHERE  LOWER(rms_style_num) <>  LOWER('')
 AND rms_style_num IS NOT NULL
GROUP BY channel_country,
 rms_style_num,
 epm_style_num)


SELECT style_dim.epm_style_num,
ROW_NUMBER() OVER (PARTITION BY sku_dim.epm_style_num, sku_dim.channel_country ORDER BY sku_dim.rms_style_num) AS rms_seq_num , 
style_dim.channel_country, 
 sku_dim.rms_style_num,
 style_dim.style_desc,
 style_dim.style_group_num,
 style_group_vw.style_group_desc,
 style_group_vw.style_group_short_desc,
 style_dim.type_level_1_num,
 style_dim.type_level_1_desc,
 style_dim.type_level_2_num,
 style_dim.type_level_2_desc,
 style_dim.sbclass_num,
 style_dim.sbclass_long_desc,
 style_dim.sbclass_desc,
 style_dim.class_num,
 style_dim.class_long_desc,
 style_dim.class_desc,
 style_dim.dept_num,
 style_dim.dept_long_desc AS dept_desc,
 style_dim.dept_desc AS dept_short_desc,
 style_dim.grp_num,
 style_dim.grp_long_desc AS grp_desc,
 style_dim.grp_desc AS grp_short_desc,
 style_dim.div_num,
 style_dim.div_long_desc AS div_desc,
 style_dim.div_desc AS div_short_desc,
 style_dim.cmpy_num,
 style_dim.cmpy_desc,
 style_dim.nord_label_ind,
 style_dim.vendor_label_code,
 style_dim.vendor_label_name,
 style_group_vw.special_treatment_type_code,
 style_dim.size_standard,
 REPLACE(REPLACE(TRIM(sku_dim.supp_part_num), '\'00A000A0\'xc', ''), '\'00A0\'xc', '') AS supp_part_num,
 style_dim.prmy_supp_num,
 style_dim.manufacturer_num,
 style_dim.product_source_code,
 style_dim.product_source_desc,
 style_dim.genders_code,
 style_dim.genders_desc,
 style_dim.age_groups_code,
 style_dim.age_groups_desc,
 style_dim.hanger_type_code,
 style_dim.hanger_type_desc,
 style_dim.npg_ind,
 style_dim.npg_sourcing_code,
 CONCAT(REPLACE(REPLACE(TRIM(sku_dim.supp_part_num), '\'00A000A0\'xc', ''), '\'00A0\'xc', ''), ', ', style_dim.style_desc
  ) AS vpn_label,
  CASE
  WHEN sku_dim.dw_batch_id > style_dim.dw_batch_id
  THEN CAST(FLOOR(sku_dim.dw_batch_id) AS INTEGER)
  ELSE style_dim.dw_batch_id
  END AS dw_batch_id,
  CASE
  WHEN sku_dim.dw_batch_id > style_dim.dw_batch_id
  THEN sku_dim.dw_batch_date
  ELSE style_dim.dw_batch_date
  END AS dw_batch_date,
  CASE
  WHEN sku_dim.dw_batch_id > style_dim.dw_batch_id
  THEN sku_dim.dw_sys_load_tmstp
  ELSE style_dim.dw_sys_load_tmstp
  END AS dw_sys_load_tmstp
FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_STYLE_DIM AS STYLE_DIM
 INNER JOIN sku_dim ON style_dim.epm_style_num = sku_dim.epm_style_num AND  LOWER(sku_dim.channel_country) =  LOWER(style_dim
    .channel_country)
 LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.product_style_group_dim_vw AS style_group_vw ON  LOWER(style_group_vw.style_group_num) =
   LOWER(style_dim.style_group_num)
QUALIFY (ROW_NUMBER() OVER (PARTITION BY sku_dim.rms_style_num, RPAD(sku_dim.channel_country, 2, ' ') ORDER BY sku_dim.dw_sys_load_tmstp
     DESC)) = 1;

-- .IF ERRORCODE <> 0 THEN .QUIT 3


-- .IF ERRORCODE <> 0 THEN .QUIT 4
