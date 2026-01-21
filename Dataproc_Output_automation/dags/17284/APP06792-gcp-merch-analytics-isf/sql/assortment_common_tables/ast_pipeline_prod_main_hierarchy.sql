
/*
Purpose:        Inserts data in t2dl_das_assortment_dim`assortment_hierarchy` for product hierarchy
Variable(s):    {{params.environment_schema}} T2DL_DAS_ASSORTMENT_DIM (prod) or {{params.environment_schema}}
                {{params.env_suffix}} '' or '{{params.env_suffix}}' tablesuffix for prod testing
Author(s):       Sara Riker & Christine Buckler
*/





CREATE TEMPORARY TABLE IF NOT EXISTS fanatics
CLUSTER BY sku_idnt
AS
SELECT DISTINCT psd.rms_sku_num AS sku_idnt
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS psd
 INNER JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.vendor_payto_relationship_dim AS payto 
 ON LOWER(psd.prmy_supp_num) = LOWER(payto.order_from_vendor_num
   )
WHERE LOWER(payto.payto_vendor_num) = LOWER('5179609');


--COLLECT STATS      PRIMARY INDEX(sku_idnt)      ON fanatics


CREATE TEMPORARY TABLE IF NOT EXISTS vendor_lkup
CLUSTER BY epm_style_num, channel_country
AS
SELECT DISTINCT s.epm_style_num,
 s.channel_country,
 s.vendor_label_code,
 s.vendor_label_name,
 vl.vendor_label_desc,
 vl.vendor_brand_name
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.product_style_dim AS s
 INNER JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.vendor_label_dim AS vl 
 ON CAST(s.vendor_label_code AS FLOAT64) = vl.vendor_label_code;


--COLLECT STATS      PRIMARY INDEX(epm_style_num, channel_country)      ON vendor_lkup


CREATE TEMPORARY TABLE IF NOT EXISTS selling_rights
CLUSTER BY rms_sku_num
AS
SELECT rms_sku_num,
 MAX(CASE
   WHEN LOWER(channel_brand) = LOWER('NORDSTROM') AND LOWER(selling_channel) = LOWER('STORE') AND LOWER(is_sellable_ind
       ) = LOWER('Y') AND LOWER(selling_status_code) = LOWER('UNBLOCKED')
   THEN 1
   ELSE 0
   END) AS nord_store_sellable_ind,
 MAX(CASE
   WHEN LOWER(channel_brand) = LOWER('NORDSTROM') AND LOWER(selling_channel) = LOWER('ONLINE') AND LOWER(is_sellable_ind
       ) = LOWER('Y') AND LOWER(selling_status_code) = LOWER('UNBLOCKED')
   THEN 1
   ELSE 0
   END) AS nord_web_sellable_ind,
 MAX(CASE
   WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK') AND LOWER(selling_channel) = LOWER('STORE') AND LOWER(is_sellable_ind
       ) = LOWER('Y') AND LOWER(selling_status_code) = LOWER('UNBLOCKED')
   THEN 1
   ELSE 0
   END) AS rack_store_sellable_ind,
 MAX(CASE
   WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK') AND LOWER(selling_channel) = LOWER('ONLINE') AND LOWER(is_sellable_ind
       ) = LOWER('Y') AND LOWER(selling_status_code) = LOWER('UNBLOCKED')
   THEN 1
   ELSE 0
   END) AS rack_web_sellable_ind
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.product_item_selling_rights_dim
WHERE LOWER(channel_country) = LOWER('US')
 AND  current_datetime('PST8PDT') BETWEEN eff_begin_tmstp
  AND eff_end_tmstp
GROUP BY rms_sku_num;


--COLLECT STATS      PRIMARY INDEX(rms_sku_num)      ON selling_rights




TRUNCATE TABLE {{params.gcp_project_id}}.{{params.environment_schema}}.assortment_hierarchy{{params.env_suffix}} --{env_suffix}
;



INSERT INTO {{params.gcp_project_id}}.{{params.environment_schema}}.assortment_hierarchy{{params.env_suffix}}
(SELECT DISTINCT TRIM(COALESCE(FORMAT('%11d', sku.dept_num), 'UNKNOWN') || '_' || COALESCE(sku.prmy_supp_num, 'UNKNOWN'
         ) || '_' || COALESCE(sku.supp_part_num, 'UNKNOWN') || '_' || COALESCE(sku.color_num, 'UNKNOWN')) AS
  customer_choice,
  sku.epm_choice_num,
    sku.rms_style_num || '-' || CASE
    WHEN LOWER(COALESCE(sku.color_num, '0')) IN (LOWER('0'), LOWER('00'), LOWER('000'))
    THEN '0'
    ELSE LTRIM(sku.color_num, '0')
    END AS cc_style_color,
  sku.rms_sku_num AS sku_idnt,
  sku.channel_country,
  sku.prmy_supp_num AS supplier_idnt,
  CAST(TRUNC(CAST(sku.div_num AS FLOAT64)) AS INTEGER) AS div_idnt,
  sku.div_desc,
  CAST(TRUNC(CAST(sku.grp_num AS FLOAT64)) AS INTEGER) AS grp_idnt,
  sku.grp_desc,
  CAST(TRUNC(CAST(sku.dept_num AS FLOAT64)) AS INTEGER) AS dept_idnt,
  sku.dept_desc,
  CAST(TRUNC(CAST(sku.class_num AS FLOAT64)) AS INTEGER) AS class_idnt,
  sku.class_desc,
  CAST(TRUNC(CAST(sku.sbclass_num AS FLOAT64)) AS INTEGER) AS sbclass_idnt,
  sku.sbclass_desc,
  COALESCE(ccs.quantrix_category, cat.category, cat1.category) AS quantrix_category,
  ccs.ccs_category,
  ccs.ccs_subcategory,
  sku.style_group_num,
  sku.web_style_num,
  CAST(sku.epm_style_num AS STRING) AS epm_style_num,
  sku.rms_style_num,
  sku.style_desc,
  sku.brand_name,
   CASE
   WHEN f.sku_idnt IS NOT NULL
   THEN 1
   ELSE 0
   END AS fanatics_ind,
   CASE
   WHEN LOWER(sku.partner_relationship_type_code) = LOWER('ECONCESSION')
   THEN 1
   ELSE 0
   END AS marketplace_ind,
  sku.npg_ind,
  v.vendor_name,
  CAST(TRUNC(CAST(vl.vendor_label_code AS FLOAT64)) AS INTEGER) AS vendor_label_code,
  vl.vendor_label_name,
  vl.vendor_label_desc,
  vl.vendor_brand_name,
  supp.fp_supplier_group,
  supp.op_supplier_group,
  sku.supp_part_num,
  sku.supp_color,
  sku.nrf_size_code,
  sku.supp_size,
  sku.size_1_num,
  sku.size_1_desc,
  sku.size_2_num,
  sku.size_2_desc,
  sku.color_num,
  sku.color_desc,
  sku.nord_display_color,
  ccs.nord_role,
  ccs.nord_role_desc,
  ccs.rack_role,
  ccs.rack_role_desc,
  ccs.parent_group,
  ccs.merch_themes,
  ccs.assortment_grouping,
  sku.smart_sample_ind,
  sku.gwp_ind,
  sku.fulfillment_type_code,
  itm.nord_store_sellable_ind,
  itm.nord_web_sellable_ind,
  itm.rack_store_sellable_ind,
  itm.rack_web_sellable_ind,
  sku.sku_type_code,
  sku.sku_type_desc,
  sku.return_disposition_code,
  sku.return_disposition_desc,
  sku.drop_ship_eligible_ind,
  sku.fp_replenishment_eligible_ind,
  sku.op_replenishment_eligible_ind,
  sku.dw_sys_load_tmstp,
  timestamp(current_datetime('PST8PDT')) AS update_timestamp,
  'PST8PDT' AS update_timestamp_tz
 FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS sku
  LEFT JOIN t2dl_das_ccs_categories.ccs_merch_themes AS ccs ON sku.dept_num = ccs.dept_idnt AND sku.class_num = ccs.class_idnt
      AND sku.sbclass_num = ccs.sbclass_idnt
  LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.catg_subclass_map_dim AS cat ON sku.dept_num = cat.dept_num AND sku.class_num = CAST(cat.class_num AS FLOAT64)
      AND sku.sbclass_num = CAST(cat.sbclass_num AS FLOAT64)
  LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.catg_subclass_map_dim AS cat1 ON sku.dept_num = cat1.dept_num AND sku.class_num = CAST(cat1.class_num AS FLOAT64)
      AND CAST(cat1.sbclass_num AS FLOAT64) = - 1
  LEFT JOIN fanatics AS f ON LOWER(sku.rms_sku_num) = LOWER(f.sku_idnt)
  LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.vendor_dim AS v ON LOWER(sku.prmy_supp_num) = LOWER(v.vendor_num)
  LEFT JOIN vendor_lkup AS vl ON sku.epm_style_num = vl.epm_style_num AND LOWER(sku.channel_country) = LOWER(vl.channel_country
     )
  LEFT JOIN selling_rights AS itm ON LOWER(sku.rms_sku_num) = LOWER(itm.rms_sku_num)
  LEFT JOIN (SELECT dept_num,
    supplier_num,
    MAX(CASE
      WHEN LOWER(banner) = LOWER('FP')
      THEN supplier_group
      ELSE NULL
      END) AS fp_supplier_group,
    MAX(CASE
      WHEN LOWER(banner) = LOWER('OP')
      THEN supplier_group
      ELSE NULL
      END) AS op_supplier_group
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.supp_dept_map_dim
   GROUP BY dept_num,
    supplier_num) AS supp ON sku.dept_num = CAST(supp.dept_num AS FLOAT64) AND LOWER(sku.prmy_supp_num) = LOWER(supp.supplier_num
     )
 WHERE LOWER(sku.channel_country) = LOWER('US')
  AND LOWER(sku.div_desc) NOT LIKE LOWER('INACTIVE%')
  AND LOWER(sku.dept_desc) NOT LIKE LOWER('INACTIVE%'));


--COLLECT STATS      PRIMARY INDEX (div_idnt, grp_idnt, dept_idnt, customer_choice)     ,COLUMN (dept_idnt)     ,COLUMN (dept_idnt, quantrix_category)     ,COLUMN (sku_idnt, channel_country)     ,COLUMN (div_idnt, grp_idnt, dept_idnt, quantrix_category)     ,COLUMN (customer_choice, sku_idnt, channel_country)     ,COLUMN (customer_choice, channel_country)     ,COLUMN (customer_choice, sku_idnt)     ,COLUMN (div_idnt, grp_idnt, dept_idnt)     ,COLUMN (channel_country)     ON {{params.gcp_project_id}}.{{params.environment_schema}}.assortment_hierarchy --{env_suffix} 