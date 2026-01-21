
/*
Purpose:        Inserts data in table `assortment_hierarchy` for product hierarchy
Variable(s):    {{environment_schema}} T2DL_DAS_ASSORTMENT_DIM (prod) or {environment_schema}
                {{env_suffix}} '' or '{env_suffix}' tablesuffix for prod testing
Author(s):       Sara Riker & Christine Buckler
*/


CREATE MULTISET VOLATILE TABLE fanatics AS (
SELECT DISTINCT
    rms_sku_num AS sku_idnt
FROM prd_nap_usr_vws.product_sku_dim_vw psd
JOIN prd_nap_usr_vws.vendor_payto_relationship_dim payto
  ON psd.prmy_supp_num = payto.order_from_vendor_num
WHERE payto.payto_vendor_num = '5179609'
)
WITH DATA
PRIMARY INDEX(sku_idnt)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(sku_idnt)
     ON fanatics;

CREATE MULTISET VOLATILE TABLE vendor_lkup AS (
SELECT DISTINCT
     epm_style_num
    ,channel_country
    ,s.vendor_label_code
    ,s.vendor_label_name
    ,vendor_label_desc
    ,vendor_brand_name
FROM prd_nap_usr_vws.product_style_dim s
JOIN prd_nap_usr_vws.vendor_label_dim vl
  ON s.vendor_label_code = vl.vendor_label_code
) WITH DATA
PRIMARY INDEX(epm_style_num, channel_country)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(epm_style_num, channel_country)
     ON vendor_lkup;

CREATE MULTISET VOLATILE TABLE selling_rights AS (
SELECT
     rms_sku_num
    ,MAX(CASE
        WHEN channel_brand = 'NORDSTROM' AND selling_channel = 'STORE' AND is_sellable_ind = 'Y' AND selling_status_code = 'UNBLOCKED'
        THEN 1 ELSE 0 END) AS nord_store_sellable_ind
    ,MAX(CASE
        WHEN channel_brand = 'NORDSTROM' AND selling_channel = 'ONLINE' AND is_sellable_ind = 'Y' AND selling_status_code = 'UNBLOCKED'
        THEN 1 ELSE 0 END) AS nord_web_sellable_ind
    ,MAX(CASE
        WHEN channel_brand = 'NORDSTROM_RACK' AND selling_channel = 'STORE' AND is_sellable_ind = 'Y' AND selling_status_code = 'UNBLOCKED'
        THEN 1 ELSE 0 END) AS rack_store_sellable_ind
    ,MAX(CASE
        WHEN channel_brand = 'NORDSTROM_RACK' AND selling_channel = 'ONLINE' AND is_sellable_ind = 'Y' AND selling_status_code = 'UNBLOCKED'
        THEN 1 ELSE 0 END) AS rack_web_sellable_ind
FROM prd_nap_usr_vws.product_item_selling_rights_dim
WHERE channel_country = 'US'
    AND CURRENT_TIMESTAMP BETWEEN eff_begin_tmstp AND eff_end_tmstp
GROUP BY rms_sku_num
) WITH DATA
PRIMARY INDEX(rms_sku_num)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(rms_sku_num)
     ON selling_rights;

DELETE FROM {environment_schema}.assortment_hierarchy{env_suffix} ALL;
INSERT INTO {environment_schema}.assortment_hierarchy{env_suffix}
SELECT DISTINCT
     TRIM(COALESCE(sku.dept_num,'UNKNOWN') || '_' || COALESCE(prmy_supp_num,'UNKNOWN') || '_' || COALESCE(supp_part_num,'UNKNOWN') || '_' || COALESCE(color_num,'UNKNOWN')) AS customer_choice
    ,sku.epm_choice_num
    ,sku.rms_style_num || '-' || (CASE WHEN COALESCE(sku.color_num, '0') IN ('0','00','000') THEN '0' ELSE LTRIM (sku.color_num,'0') END) AS cc_style_color
    ,sku.rms_sku_num AS sku_idnt
    ,sku.channel_country
    ,prmy_supp_num AS supplier_idnt
    ,CAST(div_num AS INTEGER) AS div_idnt
    ,sku.div_desc
    ,CAST(grp_num AS INTEGER) AS grp_idnt -- subdivision
    ,sku.grp_desc
    ,CAST(sku.dept_num  AS INTEGER) AS dept_idnt
    ,sku.dept_desc
    ,CAST(sku.class_num AS INTEGER) AS class_idnt
    ,sku.class_desc
    ,CAST(sku.sbclass_num AS INTEGER) AS sbclass_idnt
    ,sku.sbclass_desc
    ,COALESCE(ccs.quantrix_category, cat.category, cat1.category) AS quantrix_category
    ,ccs.ccs_category
    ,ccs.ccs_subcategory
    ,style_group_num
    ,web_style_num
    ,sku.epm_style_num
    ,sku.rms_style_num
    ,style_desc
    ,brand_name
    ,CASE WHEN f.sku_idnt IS NOT NULL THEN 1 ELSE 0 END AS fanatics_ind
    ,CASE WHEN partner_relationship_type_code = 'ECONCESSION' THEN 1 ELSE 0 END AS marketplace_ind
    ,npg_ind
    ,v.vendor_name
    ,vl.vendor_label_code
    ,vl.vendor_label_name
    ,vl.vendor_label_desc
    ,vl.vendor_brand_name
    ,supp.fp_supplier_group
    ,supp.op_supplier_group
    ,supp_part_num
    ,supp_color
    ,sku.nrf_size_code
    ,supp_size
    ,size_1_num
    ,size_1_desc
    ,size_2_num
    ,size_2_desc
    ,color_num
    ,color_desc
    ,nord_display_color
    ,ccs.nord_role
    ,ccs.nord_role_desc
    ,ccs.rack_role
    ,ccs.rack_role_desc
    ,ccs.parent_group
    ,ccs.merch_themes
    ,ccs.assortment_grouping
    ,sku.smart_sample_ind
    ,sku.gwp_ind
    ,sku.fulfillment_type_code
    ,nord_store_sellable_ind
    ,nord_web_sellable_ind
    ,rack_store_sellable_ind
    ,rack_web_sellable_ind
    ,sku.sku_type_code
    ,sku.sku_type_desc
    ,sku.return_disposition_code
    ,sku.return_disposition_desc
    ,sku.drop_ship_eligible_ind
    ,sku.fp_replenishment_eligible_ind
    ,sku.op_replenishment_eligible_ind
    ,sku.dw_sys_load_tmstp
    ,CURRENT_TIMESTAMP AS update_timestamp
FROM prd_nap_usr_vws.product_sku_dim_vw sku
LEFT JOIN t2dl_das_ccs_categories.ccs_merch_themes ccs
  ON sku.dept_num = ccs.dept_idnt
 AND sku.class_num = ccs.class_idnt
 AND sku.sbclass_num = ccs.sbclass_idnt
LEFT JOIN PRD_NAP_USR_VWS.CATG_SUBCLASS_MAP_DIM cat
  ON sku.dept_num = cat.dept_num
 AND sku.class_num = cat.class_num
 AND sku.sbclass_num = cat.sbclass_num
LEFT JOIN PRD_NAP_USR_VWS.CATG_SUBCLASS_MAP_DIM cat1
  ON sku.dept_num = cat1.dept_num
 AND sku.class_num = cat1.class_num
 AND cat1.sbclass_num = -1
LEFT JOIN fanatics f
  ON sku.rms_sku_num = f.sku_idnt
LEFT JOIN prd_nap_usr_vws.vendor_dim v
  ON sku.prmy_supp_num = v.vendor_num
LEFT JOIN vendor_lkup vl
  ON sku.epm_style_num = vl.epm_style_num
 AND sku.channel_country  = vl.channel_country
LEFT JOIN selling_rights itm
  ON sku.rms_sku_num = itm.rms_sku_num
LEFT JOIN (
            SELECT
                 dept_num
                ,supplier_num
                ,MAX(CASE WHEN banner = 'FP' THEN supplier_group ELSE NULL END) AS fp_supplier_group
                ,MAX(CASE WHEN banner = 'OP' THEN supplier_group ELSE NULL END) AS op_supplier_group
            FROM prd_nap_usr_vws.supp_dept_map_dim
            GROUP BY 1,2
        ) supp
  ON sku.dept_num = supp.dept_num
 AND sku.prmy_supp_num  = supp.supplier_num
WHERE sku.div_desc NOT LIKE 'INACTIVE%'
  AND sku.dept_desc NOT LIKE 'INACTIVE%'
  AND sku.channel_country = 'US'
-- QUALIFY dense_rank() OVER (PARTITION BY sku_idnt, customer_choice, channel_country ORDER BY sku.dw_sys_load_tmstp DESC) = 1
;

COLLECT STATS
     PRIMARY INDEX (div_idnt, grp_idnt, dept_idnt, customer_choice)
    ,COLUMN (dept_idnt)
    ,COLUMN (dept_idnt, quantrix_category)
    ,COLUMN (sku_idnt, channel_country)
    ,COLUMN (div_idnt, grp_idnt, dept_idnt, quantrix_category)
    ,COLUMN (customer_choice, sku_idnt, channel_country)
    ,COLUMN (customer_choice, channel_country)
    ,COLUMN (customer_choice, sku_idnt)
    ,COLUMN (div_idnt, grp_idnt, dept_idnt)
    ,COLUMN (channel_country)
    ON {environment_schema}.assortment_hierarchy{env_suffix};