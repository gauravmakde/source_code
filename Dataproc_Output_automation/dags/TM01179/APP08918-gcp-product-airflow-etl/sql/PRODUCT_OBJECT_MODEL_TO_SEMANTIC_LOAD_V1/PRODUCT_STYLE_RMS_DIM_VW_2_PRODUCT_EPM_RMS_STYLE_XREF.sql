
BEGIN TRANSACTION;


TRUNCATE TABLE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_epm_rms_style_xref;


INSERT INTO `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_epm_rms_style_xref
(
epm_style_num,
channel_country,
rms_seq_num,
rms_style_num,
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
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_lkup
WHERE LOWER(rms_style_num) <> LOWER('')
 AND rms_style_num IS NOT NULL
GROUP BY rms_style_num,
 epm_style_num,
 channel_country)


SELECT style_dim.epm_style_num,
 style_dim.channel_country,
 ROW_NUMBER() OVER (PARTITION BY sku_dim.epm_style_num, sku_dim.channel_country ORDER BY sku_dim.rms_style_num) AS
 rms_seq_num,
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
 REPLACE(REPLACE(TRIM(sku_dim.supp_part_num),  '\'00A000A0\'xc', ''), '\'00A0\'xc', '') AS supp_part_num,
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
  WHEN SKU_DIM.dw_batch_id IS NOT NULL AND SKU_DIM.dw_batch_id > style_dim.dw_batch_id
  THEN CAST(trunc(sku_dim.dw_batch_id )AS INTEGER)
  ELSE style_dim.dw_batch_id
  END AS dw_batch_id,
  CASE
  WHEN sku_dim.dw_batch_id is not NULL AND sku_dim.dw_batch_id > style_dim.dw_batch_id
  THEN sku_dim.dw_batch_date
  ELSE style_dim.dw_batch_date
  END AS dw_batch_date,
  CASE
  WHEN sku_dim.dw_batch_id is not NULL AND sku_dim.dw_batch_id > style_dim.dw_batch_id
  THEN sku_dim.dw_sys_load_tmstp
  ELSE style_dim.dw_sys_load_tmstp
  END AS dw_sys_load_tmstp
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_style_dim AS style_dim
 INNER JOIN sku_dim ON style_dim.epm_style_num = sku_dim.epm_style_num 
 AND LOWER(sku_dim.channel_country) = LOWER(style_dim.channel_country)
 LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_style_group_dim_vw AS style_group_vw 
 ON LOWER(style_group_vw.style_group_num) = LOWER(style_dim.style_group_num)
QUALIFY (ROW_NUMBER() OVER (PARTITION BY sku_dim.epm_style_num, sku_dim.channel_country ORDER BY sku_dim.rms_style_num)
  ) = 1;


COMMIT TRANSACTION;

--COLLECT STATISTICS                     -- default SYSTEM SAMPLE PERCENT                     -- default SYSTEM THRESHOLD PERCENT              COLUMN ( epm_style_num ) ,              COLUMN ( channel_country ) ,              COLUMN ( style_group_num ) ,              COLUMN ( sbclass_num,class_num,dept_num ) ,              COLUMN ( rms_style_num ) ,              COLUMN ( epm_style_num,channel_country ) ,              COLUMN ( rms_style_num,channel_country ) ,              COLUMN ( type_level_1_desc,type_level_2_desc,vendor_label_name ) ,              COLUMN ( vendor_label_name ) ,              COLUMN ( dw_sys_load_tmstp ) ,              COLUMN ( vendor_label_code ) ,              COLUMN ( style_group_num,style_group_desc ) ,              COLUMN ( style_desc ) ,              COLUMN ( epm_style_num,style_group_num ) ,              COLUMN ( style_group_num,style_group_short_desc ) ,              COLUMN ( prmy_supp_num ) ,              COLUMN ( class_num ) ,             COLUMN ( class_desc ) ,              COLUMN ( class_num, class_desc ) ,               COLUMN ( dept_num ) ,              COLUMN ( dept_desc ) ,              COLUMN ( dept_num, dept_desc ) ,              COLUMN ( sbclass_num ) ,              COLUMN ( sbclass_desc ) ,              COLUMN ( sbclass_num, sbclass_desc ) ,             COLUMN ( grp_num ) ,              COLUMN ( grp_desc ) ,              COLUMN ( grp_num, grp_desc ) ,             COLUMN ( div_desc ) ,              COLUMN ( div_num, div_desc ) ,              COLUMN ( style_desc,style_group_num ) ,              COLUMN ( sbclass_num,class_num ) ,              COLUMN ( class_num,dept_num ) ,              COLUMN ( style_group_num,type_level_1_desc,type_level_2_desc ) ,              COLUMN ( div_num )                   ON PRD_NAP_DIM.PRODUCT_EPM_RMS_STYLE_XREF;
