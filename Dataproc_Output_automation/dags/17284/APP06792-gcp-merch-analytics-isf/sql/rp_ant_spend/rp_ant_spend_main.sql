/*
Name: RP Anticipated Spend 
Project: RP Anticipated Spend Report
Purpose: Query that supports the RP Ant Spend Report in Cost
Variable(s):    {{params.environment_schema}} T2DL_DAS_OPEN_TO_BUY
                {{params.env_suffix}} '' or '{{params.env_suffix}}' tablesuffix for prod testing
DAG: ast_rp_ant_spend_prod_main
Author(s): Sara Riker
Date Created: 12/29/22
Date Last Updated: 1/3/22
*/

-- begin
-- DROP TABLE fiscal_year;



CREATE TEMPORARY TABLE IF NOT EXISTS fiscal_year
CLUSTER BY fiscal_year_num
AS
SELECT fiscal_year_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE day_date = CURRENT_DATE
GROUP BY fiscal_year_num;


CREATE TEMPORARY TABLE IF NOT EXISTS vpns
CLUSTER BY vpn_clr, chnl
AS
SELECT DISTINCT vpn_clr,
 chnl
FROM `{{params.gcp_project_id}}`.t2dl_das_rp_anticipated_spend_reporting.rp_ant_spend_data
UNION DISTINCT
SELECT DISTINCT vpn_clr,
 chnl
FROM `{{params.gcp_project_id}}`.t2dl_das_rp_anticipated_spend_reporting.ty_ant_spend_data;


--COLLECT STATS      PRIMARY INDEX(vpn_clr, chnl)      ON vpns


-- DROP TABLE hierarchy;


CREATE TEMPORARY TABLE IF NOT EXISTS hierarchy
CLUSTER BY vpn_clr
AS
SELECT t20.vpn_clr,
 t20.channel_country,
 t20.nrf_color,
 t20.supp_part_num,
 t20.epm_style_num,
 t20.vpn,
 t20.customer_choice,
 t20.epm_choice_num,
 t20.color_desc,
 t20.style_desc,
 t20.division,
 t20.subdivision,
 t20.department,
 t20.class,
 t20.subclass,
 t20.category,
 t20.supplier_idnt0 AS supplier_idnt,
 t20.supplier,
 t20.vendor_label_code,
 t20.vendor_label_name,
 t20.vendor_label_desc,
 t20.vendor_brand_name,
 t20.fp_supplier_group,
 t20.op_supplier_group
FROM (SELECT DISTINCT a.vpn_clr,
   a.channel_country,
   a.nrf_color,
   COALESCE(b.supp_part_num, c.supp_part_num, a.supp_part_num) AS supp_part_num,
   COALESCE(b.epm_style_num, c.epm_style_num, a.epm_style_num) AS epm_style_num,
   COALESCE(b.vpn, c.vpn, d.vpn) AS vpn,
   COALESCE(b.customer_choice, c.customer_choice, d.customer_choice) AS customer_choice,
   COALESCE(b.epm_choice_num, c.epm_choice_num, d.epm_choice_num) AS epm_choice_num,
   COALESCE(b.color_desc, c.color_desc, d.color_desc) AS color_desc,
   COALESCE(b.style_desc, c.style_desc, d.style_desc) AS style_desc,
   COALESCE(b.division, c.division, d.division) AS division,
   COALESCE(b.subdivision, c.subdivision, d.subdivision) AS subdivision,
   COALESCE(b.department, c.department, d.department) AS department,
   COALESCE(b.clss, c.clss, d.clss) AS class,
   COALESCE(b.subclass, c.subclass, d.subclass) AS subclass,
   COALESCE(b.category, c.category, d.category) AS category,
   COALESCE(b.supplier_idnt, c.supplier_idnt, d.supplier_idnt) AS supplier_idnt0,
   COALESCE(b.vendor_name, c.vendor_name, d.vendor_name) AS supplier,
   COALESCE(b.vendor_label_code, c.vendor_label_code, d.vendor_label_code) AS vendor_label_code,
   COALESCE(b.vendor_label_name, c.vendor_label_name, d.vendor_label_name) AS vendor_label_name,
   COALESCE(b.vendor_label_desc, c.vendor_label_desc, d.vendor_label_desc) AS vendor_label_desc,
   COALESCE(b.vendor_brand_name, c.vendor_brand_name, d.vendor_brand_name) AS vendor_brand_name,
   COALESCE(b.fp_supplier_group, c.fp_supplier_group, d.fp_supplier_group) AS fp_supplier_group,
   COALESCE(b.op_supplier_group, c.op_supplier_group, d.op_supplier_group) AS op_supplier_group,
   c.supplier_idnt,
   b.supplier_idnt AS supplier_idnt1
  FROM (SELECT DISTINCT a.vpn_clr,
     TRIM(SUBSTR(a.vpn_clr, 3 * -1)) AS nrf_color,
     TRIM(REGEXP_EXTRACT_ALL(a.vpn_clr, '[^_]+')[SAFE_OFFSET(0)]) AS epm_style_num,
      CASE
      WHEN LENGTH(a.vpn_clr) - LENGTH(TRIM(REPLACE(a.vpn_clr, '_', ''))) = 2
      THEN UPPER(REGEXP_EXTRACT_ALL(a.vpn_clr, '[^_]+')[SAFE_OFFSET(1)])
      WHEN LENGTH(a.vpn_clr) - LENGTH(TRIM(REPLACE(a.vpn_clr, '_', ''))) = 3
      THEN UPPER(REGEXP_EXTRACT_ALL(a.vpn_clr, '[^_]+')[SAFE_OFFSET(1)]) || '' || UPPER(REGEXP_EXTRACT_ALL(a.vpn_clr,
         '[^_]+')[SAFE_OFFSET(2)])
      WHEN LENGTH(a.vpn_clr) - LENGTH(TRIM(REPLACE(a.vpn_clr, '_', ''))) = 4
      THEN UPPER(REGEXP_EXTRACT_ALL(a.vpn_clr, '[^_]+')[SAFE_OFFSET(1)]) || '' || UPPER(REGEXP_EXTRACT_ALL(a.vpn_clr,
           '[^_]+')[SAFE_OFFSET(2)]) || '' || UPPER(REGEXP_EXTRACT_ALL(a.vpn_clr, '[^_]+')[SAFE_OFFSET(3)])
      WHEN LENGTH(a.vpn_clr) - LENGTH(TRIM(REPLACE(a.vpn_clr, '_', ''))) = 5
      THEN UPPER(REGEXP_EXTRACT_ALL(a.vpn_clr, '[^_]+')[SAFE_OFFSET(1)]) || '' || UPPER(REGEXP_EXTRACT_ALL(a.vpn_clr,
             '[^_]+')[SAFE_OFFSET(2)]) || '' || UPPER(REGEXP_EXTRACT_ALL(a.vpn_clr, '[^_]+')[SAFE_OFFSET(3)]) || '' ||
       UPPER(REGEXP_EXTRACT_ALL(a.vpn_clr, '[^_]+')[SAFE_OFFSET(4)])
      WHEN LENGTH(a.vpn_clr) - LENGTH(TRIM(REPLACE(a.vpn_clr, '_', ''))) = 6
      THEN UPPER(REGEXP_EXTRACT_ALL(a.vpn_clr, '[^_]+')[SAFE_OFFSET(1)]) || '' || UPPER(REGEXP_EXTRACT_ALL(a.vpn_clr,
               '[^_]+')[SAFE_OFFSET(2)]) || '' || UPPER(REGEXP_EXTRACT_ALL(a.vpn_clr, '[^_]+')[SAFE_OFFSET(3)]) || '' ||
         UPPER(REGEXP_EXTRACT_ALL(a.vpn_clr, '[^_]+')[SAFE_OFFSET(4)]) || '' || UPPER(REGEXP_EXTRACT_ALL(a.vpn_clr,
         '[^_]+')[SAFE_OFFSET(5)])
      ELSE NULL
      END AS supp_part_num,
     c.channel_country
    FROM vpns AS a
     INNER JOIN (SELECT DISTINCT channel_num,
       channel_country
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw
      WHERE LOWER(selling_channel) <> LOWER('UNKNOWN')
       AND channel_num IS NOT NULL) AS c ON a.chnl = c.channel_num
    WHERE LOWER(c.channel_country) = LOWER('US')) AS a
   LEFT JOIN (SELECT DISTINCT TRIM(epm_style_num) AS epm_style_num,
     supp_part_num,
     style_desc,
     TRIM(color_num) AS color_num,
       supp_part_num || ', ' || style_desc AS vpn,
         supp_part_num || ', ' || style_desc || ': ' || color_desc AS customer_choice,
     epm_choice_num,
     color_desc,
     TRIM(FORMAT('%11d', div_idnt) || ' ' || div_desc) AS division,
     TRIM(FORMAT('%11d', grp_idnt) || ' ' || grp_desc) AS subdivision,
     TRIM(FORMAT('%11d', dept_idnt) || ' ' || dept_desc) AS department,
     TRIM(FORMAT('%11d', class_idnt) || ' ' || class_desc) AS clss,
     TRIM(FORMAT('%11d', sbclass_idnt) || ' ' || sbclass_desc) AS subclass,
     quantrix_category AS category,
     channel_country,
     quantrix_category,
     supplier_idnt,
     vendor_name,
     vendor_label_code,
     vendor_label_name,
     vendor_label_desc,
     vendor_brand_name,
     fp_supplier_group,
     op_supplier_group
    FROM `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.assortment_hierarchy) AS b ON LOWER(a.epm_style_num) = LOWER(b.epm_style_num) AND LOWER(a
        .supp_part_num) = LOWER(REPLACE(REGEXP_REPLACE(b.supp_part_num, '[-/":_]', ''), ' ', '')) AND LOWER(a.channel_country
       ) = LOWER(b.channel_country) AND LOWER(a.nrf_color) = LOWER(b.color_num)
   LEFT JOIN (SELECT DISTINCT TRIM(epm_style_num) AS epm_style_num,
     supp_part_num,
     style_desc,
     TRIM(color_num) AS color_num,
       supp_part_num || ', ' || style_desc AS vpn,
         supp_part_num || ', ' || style_desc || ': ' || color_desc AS customer_choice,
     epm_choice_num,
     color_desc,
     TRIM(FORMAT('%11d', div_idnt) || ' ' || div_desc) AS division,
     TRIM(FORMAT('%11d', grp_idnt) || ' ' || grp_desc) AS subdivision,
     TRIM(FORMAT('%11d', dept_idnt) || ' ' || dept_desc) AS department,
     TRIM(FORMAT('%11d', class_idnt) || ' ' || class_desc) AS clss,
     TRIM(FORMAT('%11d', sbclass_idnt) || ' ' || sbclass_desc) AS subclass,
     quantrix_category AS category,
     quantrix_category,
     supplier_idnt,
     vendor_name,
     vendor_label_code,
     vendor_label_name,
     vendor_label_desc,
     vendor_brand_name,
     fp_supplier_group,
     op_supplier_group
    FROM `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.assortment_hierarchy
    WHERE LOWER(channel_country) = LOWER('US')) AS c ON LOWER(a.epm_style_num) = LOWER(c.epm_style_num) AND LOWER(a.supp_part_num
       ) = LOWER(REPLACE(REGEXP_REPLACE(c.supp_part_num, '[-/":_]', ''), ' ', '')) AND LOWER(a.nrf_color) = LOWER(c.color_num
      )
   LEFT JOIN (SELECT DISTINCT TRIM(epm_style_num) AS epm_style_num,
     supp_part_num,
     style_desc,
     TRIM(color_num) AS color_num,
       supp_part_num || ', ' || style_desc AS vpn,
         supp_part_num || ', ' || style_desc || ': ' || color_desc AS customer_choice,
     epm_choice_num,
     color_desc,
     TRIM(FORMAT('%11d', div_idnt) || ' ' || div_desc) AS division,
     TRIM(FORMAT('%11d', grp_idnt) || ' ' || grp_desc) AS subdivision,
     TRIM(FORMAT('%11d', dept_idnt) || ' ' || dept_desc) AS department,
     TRIM(FORMAT('%11d', class_idnt) || ' ' || class_desc) AS clss,
     TRIM(FORMAT('%11d', sbclass_idnt) || ' ' || sbclass_desc) AS subclass,
     quantrix_category AS category,
     quantrix_category,
     supplier_idnt,
     vendor_name,
     vendor_label_code,
     vendor_label_name,
     vendor_label_desc,
     vendor_brand_name,
     fp_supplier_group,
     op_supplier_group
    FROM `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.assortment_hierarchy) AS d ON LOWER(a.epm_style_num) = LOWER(d.epm_style_num) AND LOWER(a
       .supp_part_num) = LOWER(REPLACE(REGEXP_REPLACE(d.supp_part_num, '[-/":_]', ''), ' ', '')) AND LOWER(a.nrf_color)
     = LOWER(d.color_num)
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY a.vpn_clr, a.channel_country ORDER BY supplier_idnt1, c.supplier_idnt DESC))
   = 1) AS t20;


--COLLECT STATS      PRIMARY INDEX(vpn_clr)      ON hierarchy


-- TY


-- CP 


--t2dl_das_rp_anticipated_spend_reporting.RP_ANT_SPEND_DATA rp 


-- TY


-- CP 


CREATE TEMPORARY TABLE IF NOT EXISTS rp_ant_spend_combine
CLUSTER BY week_idnt, chnl, vpn_clr
AS
SELECT week_idnt,
 chnl,
 vpn_clr,
 vpn,
 SUM(COALESCE(ty_rp_in_transit_u, 0)) AS ty_rp_in_transit_u,
 SUM(COALESCE(ty_rp_in_transit_c, 0)) AS ty_rp_in_transit_c,
 SUM(COALESCE(ty_rp_in_transit_r, 0)) AS ty_rp_in_transit_r,
 SUM(COALESCE(ty_rp_oo_u, 0)) AS ty_rp_oo_u,
 SUM(COALESCE(ty_rp_oo_c, 0)) AS ty_rp_oo_c,
 SUM(COALESCE(ty_rp_oo_r, 0)) AS ty_rp_oo_r,
 SUM(COALESCE(ty_rp_rcpt_u, 0)) AS ty_rp_rcpt_u,
 SUM(COALESCE(ty_rp_rcpt_c, 0)) AS ty_rp_rcpt_c,
 SUM(COALESCE(ty_rp_rcpt_r, 0)) AS ty_rp_rcpt_r,
 SUM(COALESCE(cp_cum_usable_rtns_rcpt_u, 0)) AS cp_cum_usable_rtns_rcpt_u,
 SUM(COALESCE(cp_cum_usable_rtns_rcpt_c, 0)) AS cp_cum_usable_rtns_rcpt_c,
 SUM(COALESCE(cp_cum_usable_rtns_rcpt_r, 0)) AS cp_cum_usable_rtns_rcpt_r,
 SUM(COALESCE(cp_wh_cum_usable_rtns_rcpt_u, 0)) AS cp_wh_cum_usable_rtns_rcpt_u,
 SUM(COALESCE(cp_wh_cum_usable_rtns_rcpt_c, 0)) AS cp_wh_cum_usable_rtns_rcpt_c,
 SUM(COALESCE(cp_wh_cum_usable_rtns_rcpt_r, 0)) AS cp_wh_cum_usable_rtns_rcpt_r,
 SUM(COALESCE(cp_rp_in_transit_u, 0)) AS cp_rp_in_transit_u,
 SUM(COALESCE(cp_rp_in_transit_c, 0)) AS cp_rp_in_transit_c,
 SUM(COALESCE(cp_rp_in_transit_r, 0)) AS cp_rp_in_transit_r,
 SUM(COALESCE(cp_rp_oo_u, 0)) AS cp_rp_oo_u,
 SUM(COALESCE(cp_rp_oo_c, 0)) AS cp_rp_oo_c,
 SUM(COALESCE(cp_rp_oo_r, 0)) AS cp_rp_oo_r,
 SUM(COALESCE(cp_rp_rcpt_u, 0)) AS cp_rp_rcpt_u,
 SUM(COALESCE(cp_rp_rcpt_c, 0)) AS cp_rp_rcpt_c,
 SUM(COALESCE(cp_rp_rcpt_r, 0)) AS cp_rp_rcpt_r,
 SUM(COALESCE(cp_aip_rcpt_plan_booked_u, 0)) AS cp_aip_rcpt_plan_booked_u,
 SUM(COALESCE(cp_aip_rcpt_plan_booked_c, 0)) AS cp_aip_rcpt_plan_booked_c,
 SUM(COALESCE(cp_aip_rcpt_plan_booked_r, 0)) AS cp_aip_rcpt_plan_booked_r,
 SUM(COALESCE(cp_rp_ant_spd_cum_usable_rtns_rcpt_u, 0)) AS cp_rp_ant_spd_cum_usable_rtns_rcpt_u,
 SUM(COALESCE(cp_rp_ant_spd_cum_usable_rtns_rcpt_c, 0)) AS cp_rp_ant_spd_cum_usable_rtns_rcpt_c,
 SUM(COALESCE(cp_rp_ant_spd_cum_usable_rtns_rcpt_r, 0)) AS cp_rp_ant_spd_cum_usable_rtns_rcpt_r,
 SUM(COALESCE(cp_rp_ant_spd_sys_extra_rcpt_u, 0)) AS cp_rp_ant_spd_sys_extra_rcpt_u,
 SUM(COALESCE(cp_rp_ant_spd_sys_extra_rcpt_c, 0)) AS cp_rp_ant_spd_sys_extra_rcpt_c,
 SUM(COALESCE(cp_rp_ant_spd_sys_extra_rcpt_r, 0)) AS cp_rp_ant_spd_sys_extra_rcpt_r,
 SUM(COALESCE(rp_ant_spd_u, 0)) AS rp_ant_spd_u,
 SUM(COALESCE(rp_ant_spd_c, 0)) AS rp_ant_spd_c,
 SUM(COALESCE(rp_ant_spd_r, 0)) AS rp_ant_spd_r,
 SUM(COALESCE(cp_uncons_rp_ant_spd_ttl_u, 0)) AS cp_uncons_rp_ant_spd_ttl_u,
 SUM(COALESCE(cp_uncons_rp_ant_spd_ttl_c, 0)) AS cp_uncons_rp_ant_spd_ttl_c,
 SUM(COALESCE(cp_uncons_rp_ant_spd_ttl_r, 0)) AS cp_uncons_rp_ant_spd_ttl_r,
 SUM(COALESCE(cp_flex_line_u, 0)) AS cp_flex_line_u,
 SUM(COALESCE(cp_flex_line_c, 0)) AS cp_flex_line_c,
 SUM(COALESCE(cp_flex_line_r, 0)) AS cp_flex_line_r,
 SUM(COALESCE(lcp_cons_rp_ant_spd_ttl_u, 0)) AS lcp_cons_rp_ant_spd_ttl_u,
 SUM(COALESCE(lcp_cons_rp_ant_spd_ttl_c, 0)) AS lcp_cons_rp_ant_spd_ttl_c,
 SUM(COALESCE(lcp_cons_rp_ant_spd_ttl_r, 0)) AS lcp_cons_rp_ant_spd_ttl_r,
 SUM(COALESCE(sp_cons_rp_ant_spd_ttl_u, 0)) AS sp_cons_rp_ant_spd_ttl_u,
 SUM(COALESCE(sp_cons_rp_ant_spd_ttl_c, 0)) AS sp_cons_rp_ant_spd_ttl_c,
 SUM(COALESCE(sp_cons_rp_ant_spd_ttl_r, 0)) AS sp_cons_rp_ant_spd_ttl_r
FROM (SELECT rp.week_idnt,
    rp.chnl,
    rp.vpn_clr,
    rp.vpn,
    0 AS ty_rp_in_transit_u,
    0 AS ty_rp_in_transit_c,
    0 AS ty_rp_in_transit_r,
    0 AS ty_rp_oo_u,
    0 AS ty_rp_oo_c,
    0 AS ty_rp_oo_r,
    0 AS ty_rp_rcpt_u,
    0 AS ty_rp_rcpt_c,
    0 AS ty_rp_rcpt_r,
    rp.cp_cum_usable_rtns_u AS cp_cum_usable_rtns_rcpt_u,
    rp.cp_cum_usable_rtns_c AS cp_cum_usable_rtns_rcpt_c,
    rp.cp_cum_usable_rtns_r AS cp_cum_usable_rtns_rcpt_r,
    rp.cp_wh_cum_usable_rtns_u AS cp_wh_cum_usable_rtns_rcpt_u,
    rp.cp_wh_cum_usable_rtns_c AS cp_wh_cum_usable_rtns_rcpt_c,
    rp.cp_wh_cum_usable_rtns_r AS cp_wh_cum_usable_rtns_rcpt_r,
    rp.cp_rp_in_tran_po_u AS cp_rp_in_transit_u,
    rp.cp_rp_in_tran_po_c AS cp_rp_in_transit_c,
    rp.cp_rp_in_tran_po_r AS cp_rp_in_transit_r,
    rp.cp_rp_oo_u,
    rp.cp_rp_oo_c,
    rp.cp_rp_oo_r,
    rp.cp_rp_rcpt_u,
    rp.cp_rp_rcpt_c,
    rp.cp_rp_rcpt_r,
    rp.cp_aip_rcpt_pl_bked_u AS cp_aip_rcpt_plan_booked_u,
    rp.cp_aip_rcpt_pl_bked_c AS cp_aip_rcpt_plan_booked_c,
    rp.cp_aip_rcpt_pl_bked_r AS cp_aip_rcpt_plan_booked_r,
    rp.cp_rp_ant_spd_cum_u_rtns_u AS cp_rp_ant_spd_cum_usable_rtns_rcpt_u,
    rp.cp_rp_ant_spd_cum_u_rtns_c AS cp_rp_ant_spd_cum_usable_rtns_rcpt_c,
    rp.cp_rp_ant_spd_cum_u_rtns_r AS cp_rp_ant_spd_cum_usable_rtns_rcpt_r,
    rp.cp_rp_ant_spd_sys_extr_rcpt_u AS cp_rp_ant_spd_sys_extra_rcpt_u,
    rp.cp_rp_ant_spd_sys_extr_rcpt_c AS cp_rp_ant_spd_sys_extra_rcpt_c,
    rp.cp_rp_ant_spd_sys_extr_rcpt_r AS cp_rp_ant_spd_sys_extra_rcpt_r,
    rp.cp_cons_rp_ant_spd_ttl_u AS rp_ant_spd_u,
    rp.cp_cons_rp_ant_spd_ttl_c AS rp_ant_spd_c,
    rp.cp_cons_rp_ant_spd_ttl_r AS rp_ant_spd_r,
    rp.cp_uncons_rp_ant_spd_ttl_u,
    rp.cp_uncons_rp_ant_spd_ttl_c,
    rp.cp_uncons_rp_ant_spd_ttl_r,
    rp.cp_flex_line_u,
    rp.cp_flex_line_c,
    rp.cp_flex_line_r,
    rp.lcp_cons_rp_ant_spd_ttl_u,
    rp.lcp_cons_rp_ant_spd_ttl_c,
    rp.lcp_cons_rp_ant_spd_ttl_r,
    rp.sp_cons_rp_ant_spd_ttl_u,
    rp.sp_cons_rp_ant_spd_ttl_c,
    rp.sp_cons_rp_ant_spd_ttl_r
   FROM `{{params.gcp_project_id}}`.t2dl_das_rp_anticipated_spend_reporting.rp_ant_spend_data AS rp
    INNER JOIN (SELECT DISTINCT channel_num
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw
     WHERE LOWER(selling_channel) <> LOWER('UNKNOWN')
      AND LOWER(channel_country) = LOWER('US')
      AND channel_num IS NOT NULL) AS c ON rp.chnl = c.channel_num
   UNION ALL
   SELECT rp0.week_idnt,
    rp0.chnl,
    rp0.vpn_clr,
    rp0.vpn,
    rp0.ty_rp_in_transit_u,
    rp0.ty_rp_in_transit_c,
    rp0.ty_rp_in_transit_r,
    rp0.ty_rp_oo_u,
    rp0.ty_rp_oo_c,
    rp0.ty_rp_oo_r,
    rp0.ty_rp_rcpt_u,
    rp0.ty_rp_rcpt_c,
    rp0.ty_rp_rcpt_r,
    0 AS cp_cum_usable_rtns_rcpt_u,
    0 AS cp_cum_usable_rtns_rcpt_c,
    0 AS cp_cum_usable_rtns_rcpt_r,
    0 AS cp_wh_cum_usable_rtns_rcpt_u,
    0 AS cp_wh_cum_usable_rtns_rcpt_c,
    0 AS cp_wh_cum_usable_rtns_rcpt_r,
    0 AS cp_rp_in_transit_u,
    0 AS cp_rp_in_transit_c,
    0 AS cp_rp_in_transit_r,
    0 AS cp_rp_oo_u,
    0 AS cp_rp_oo_c,
    0 AS cp_rp_oo_r,
    0 AS cp_rp_rcpt_u,
    0 AS cp_rp_rcpt_c,
    0 AS cp_rp_rcpt_r,
    0 AS cp_aip_rcpt_plan_booked_u,
    0 AS cp_aip_rcpt_plan_booked_c,
    0 AS cp_aip_rcpt_plan_booked_r,
    0 AS cp_rp_ant_spd_cum_usable_rtns_rcpt_u,
    0 AS cp_rp_ant_spd_cum_usable_rtns_rcpt_c,
    0 AS cp_rp_ant_spd_cum_usable_rtns_rcpt_r,
    0 AS cp_rp_ant_spd_sys_extra_rcpt_u,
    0 AS cp_rp_ant_spd_sys_extra_rcpt_c,
    0 AS cp_rp_ant_spd_sys_extra_rcpt_r,
    0 AS rp_ant_spd_u,
    0 AS rp_ant_spd_c,
    0 AS rp_ant_spd_r,
    0 AS cp_uncons_rp_ant_spd_ttl_u,
    0 AS cp_uncons_rp_ant_spd_ttl_c,
    0 AS cp_uncons_rp_ant_spd_ttl_r,
    0 AS cp_flex_line_u,
    0 AS cp_flex_line_c,
    0 AS cp_flex_line_r,
    0 AS lcp_cons_rp_ant_spd_ttl_u,
    0 AS lcp_cons_rp_ant_spd_ttl_c,
    0 AS lcp_cons_rp_ant_spd_ttl_r,
    0 AS sp_cons_rp_ant_spd_ttl_u,
    0 AS sp_cons_rp_ant_spd_ttl_c,
    0 AS sp_cons_rp_ant_spd_ttl_r
   FROM `{{params.gcp_project_id}}`.t2dl_das_rp_anticipated_spend_reporting.ty_ant_spend_data AS rp0
    INNER JOIN (SELECT DISTINCT channel_num
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw
     WHERE LOWER(selling_channel) <> LOWER('UNKNOWN')
      AND LOWER(channel_country) = LOWER('US')
      AND channel_num IS NOT NULL) AS c ON rp0.chnl = c.channel_num) AS rp
GROUP BY week_idnt,
 chnl,
 vpn_clr,
 vpn;


--select distinct chnl from rp_ant_spend_combine


-- DROP TABLE rp_ant_spend;


-- calendar


-- Channel


-- hierarchy


-- metrics 


-- TY


-- CP


-- update info


CREATE TEMPORARY TABLE IF NOT EXISTS rp_ant_spend
CLUSTER BY week_idnt, channel_number, vpn_clr
AS
SELECT t11.week_idnt,
 t11.week_label,
 t11.month_idnt,
 t11.month_label,
 t11.quarter_label,
 t11.quarter,
 t11.year,
 c.channel_number,
 c.channel_brand,
 c.channel_country,
 c.selling_channel,
 h.vpn_clr,
 h.supp_part_num,
 h.epm_style_num,
 h.vpn,
 h.customer_choice,
 h.epm_choice_num,
 h.nrf_color,
 h.color_desc,
 h.style_desc,
 h.division,
 h.subdivision,
 h.department,
 h.class,
 h.subclass,
 h.category,
 h.supplier_idnt,
 h.supplier,
 h.vendor_label_code,
 h.vendor_label_name,
 h.vendor_label_desc,
 h.vendor_brand_name,
  CASE
  WHEN LOWER(c.channel_brand) = LOWER('NORDSTROM')
  THEN COALESCE(h.fp_supplier_group, 'OTHER')
  WHEN LOWER(c.channel_brand) = LOWER('NORDSTROM_RACK')
  THEN COALESCE(h.op_supplier_group, 'OTHER')
  ELSE 'OTHER'
  END AS supplier_group,
 rp.ty_rp_in_transit_u,
 rp.ty_rp_in_transit_c,
 rp.ty_rp_in_transit_r,
 rp.ty_rp_oo_u,
 rp.ty_rp_oo_c,
 rp.ty_rp_oo_r,
 rp.ty_rp_rcpt_u,
 rp.ty_rp_rcpt_c,
 rp.ty_rp_rcpt_r,
 rp.cp_cum_usable_rtns_rcpt_u,
 rp.cp_cum_usable_rtns_rcpt_c,
 rp.cp_cum_usable_rtns_rcpt_r,
 rp.cp_wh_cum_usable_rtns_rcpt_u,
 rp.cp_wh_cum_usable_rtns_rcpt_c,
 rp.cp_wh_cum_usable_rtns_rcpt_r,
 rp.cp_rp_in_transit_u,
 rp.cp_rp_in_transit_c,
 rp.cp_rp_in_transit_r,
 rp.cp_rp_oo_u,
 rp.cp_rp_oo_c,
 rp.cp_rp_oo_r,
 rp.cp_rp_rcpt_u,
 rp.cp_rp_rcpt_c,
 rp.cp_rp_rcpt_r,
 rp.cp_aip_rcpt_plan_booked_u,
 rp.cp_aip_rcpt_plan_booked_c,
 rp.cp_aip_rcpt_plan_booked_r,
 rp.cp_rp_ant_spd_cum_usable_rtns_rcpt_u,
 rp.cp_rp_ant_spd_cum_usable_rtns_rcpt_c,
 rp.cp_rp_ant_spd_cum_usable_rtns_rcpt_r,
 rp.cp_rp_ant_spd_sys_extra_rcpt_u,
 rp.cp_rp_ant_spd_sys_extra_rcpt_c,
 rp.cp_rp_ant_spd_sys_extra_rcpt_r,
 rp.rp_ant_spd_u,
 rp.rp_ant_spd_c,
 rp.rp_ant_spd_r,
 rp.cp_uncons_rp_ant_spd_ttl_u,
 rp.cp_uncons_rp_ant_spd_ttl_c,
 rp.cp_uncons_rp_ant_spd_ttl_r,
 rp.cp_flex_line_u,
 rp.cp_flex_line_c,
 rp.cp_flex_line_r,
 rp.lcp_cons_rp_ant_spd_ttl_u,
 rp.lcp_cons_rp_ant_spd_ttl_c,
 rp.lcp_cons_rp_ant_spd_ttl_r,
 rp.sp_cons_rp_ant_spd_ttl_u,
 rp.sp_cons_rp_ant_spd_ttl_c,
 rp.sp_cons_rp_ant_spd_ttl_r,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS rcd_update_tmstmp
FROM rp_ant_spend_combine AS rp
 INNER JOIN (SELECT DISTINCT channel_num,
   TRIM(FORMAT('%11d', channel_num) || ' ' || channel_desc) AS channel_number,
   channel_brand,
   channel_country,
   selling_channel
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw
  WHERE LOWER(selling_channel) <> LOWER('UNKNOWN')
   AND LOWER(channel_country) = LOWER('US')
   AND FORMAT('%11d', channel_num) || ' ' || channel_desc IS NOT NULL) AS c ON rp.chnl = c.channel_num
 INNER JOIN hierarchy AS h ON LOWER(rp.vpn_clr) = LOWER(h.vpn_clr) AND LOWER(c.channel_country) = LOWER(h.channel_country
    )
 INNER JOIN (SELECT week_label,
   week_idnt,
   month_label,
   month_idnt,
   quarter_label,
   quarter_idnt,
      'FY' || SUBSTR(SUBSTR(CAST(fiscal_year_num AS STRING), 1, 5), 3, 2) || ' ' || quarter_abrv AS quarter,
   fiscal_year_num AS year
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS tme
  WHERE quarter_idnt <= (SELECT DISTINCT quarter_idnt + 10
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS a
     WHERE day_date = CURRENT_DATE)
  GROUP BY week_label,
   week_idnt,
   month_label,
   month_idnt,
   quarter_label,
   quarter_idnt,
   quarter,
   year
  QUALIFY (DENSE_RANK() OVER (ORDER BY quarter_idnt DESC)) < 9) AS t11 ON rp.week_idnt = t11.week_idnt;


--COLLECT STATS      PRIMARY INDEX(week_idnt, channel_number, vpn_clr)      ON rp_ant_spend


--{env_suffix}


TRUNCATE TABLE t2dl_das_open_to_buy.rp_ant_spend_report;


--{env_suffix}


INSERT INTO `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.rp_ant_spend_report
(SELECT CAST(week_idnt AS STRING) AS week_idnt,
  week_label,
  CAST(month_idnt AS STRING) AS month_idnt,
  month_label,
  quarter_label,
  quarter,
  CAST(year AS STRING) AS year,
  channel_number,
  channel_brand,
  channel_country,
  selling_channel,
  vpn_clr,
  supp_part_num,
  epm_style_num,
  vpn,
  customer_choice,
  epm_choice_num,
  nrf_color,
  color_desc,
  style_desc,
  division,
  subdivision,
  department,
  class,
  subclass,
  category,
  supplier_idnt,
  supplier,
  vendor_label_code,
  vendor_label_name,
  vendor_label_desc,
  vendor_brand_name,
  supplier_group,
  ty_rp_in_transit_u,
  ty_rp_in_transit_c,
  ty_rp_in_transit_r,
  ty_rp_oo_u,
  ty_rp_oo_c,
  ty_rp_oo_r,
  ty_rp_rcpt_u,
  ty_rp_rcpt_c,
  ty_rp_rcpt_r,
  cp_cum_usable_rtns_rcpt_u,
  cp_cum_usable_rtns_rcpt_c,
  cp_cum_usable_rtns_rcpt_r,
  cp_wh_cum_usable_rtns_rcpt_u,
  cp_wh_cum_usable_rtns_rcpt_c,
  cp_wh_cum_usable_rtns_rcpt_r,
  cp_rp_in_transit_u,
  cp_rp_in_transit_c,
  cp_rp_in_transit_r,
  cp_rp_oo_u,
  cp_rp_oo_c,
  cp_rp_oo_r,
  cp_rp_rcpt_u,
  cp_rp_rcpt_c,
  cp_rp_rcpt_r,
  cp_aip_rcpt_plan_booked_u,
  cp_aip_rcpt_plan_booked_c,
  cp_aip_rcpt_plan_booked_r,
  cp_rp_ant_spd_cum_usable_rtns_rcpt_u,
  cp_rp_ant_spd_cum_usable_rtns_rcpt_c,
  cp_rp_ant_spd_cum_usable_rtns_rcpt_r,
  cp_rp_ant_spd_sys_extra_rcpt_u,
  cp_rp_ant_spd_sys_extra_rcpt_c,
  cp_rp_ant_spd_sys_extra_rcpt_r,
  rp_ant_spd_u,
  rp_ant_spd_c,
  rp_ant_spd_r,
  cp_uncons_rp_ant_spd_ttl_u,
  cp_uncons_rp_ant_spd_ttl_c,
  cp_uncons_rp_ant_spd_ttl_r,
  cp_flex_line_u,
  cp_flex_line_c,
  cp_flex_line_r,
  lcp_cons_rp_ant_spd_ttl_u,
  lcp_cons_rp_ant_spd_ttl_c,
  lcp_cons_rp_ant_spd_ttl_r,
  sp_cons_rp_ant_spd_ttl_u,
  sp_cons_rp_ant_spd_ttl_c,
  sp_cons_rp_ant_spd_ttl_r,
  CAST(rcd_update_tmstmp AS TIMESTAMP) AS rcd_update_tmstmp,
  jwn_udf.default_tz_pst() as rcd_update_tmstmp_tz
 FROM rp_ant_spend);


-- Calendar


-- Channel


-- hierarchy


-- metrics     


-- Channel


-- hierarchy


CREATE TEMPORARY TABLE IF NOT EXISTS rp_ant_spend_hist
CLUSTER BY week_idnt, channel_number, division
AS
SELECT (SELECT DISTINCT week_idnt
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
  WHERE day_date = CURRENT_DATE) AS replan_week_idnt,
  (SELECT CASE
    WHEN week_num_of_fiscal_month IN (1, 3)
    THEN 1
    ELSE 0
    END AS replan_week
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
  WHERE day_date = CURRENT_DATE) AS replan_week,
 week_idnt,
 week_label,
 month_idnt,
 month_label,
 quarter_label,
 quarter,
 year,
 channel_number,
 channel_brand,
 channel_country,
 selling_channel,
 division,
 subdivision,
 department,
 class,
 subclass,
 category,
 supplier_idnt,
 supplier,
 vendor_label_code,
 vendor_label_name,
 vendor_label_desc,
 vendor_brand_name,
 supplier_group,
 SUM(cp_cum_usable_rtns_rcpt_u) AS cp_cum_usable_rtns_rcpt_u,
 SUM(cp_cum_usable_rtns_rcpt_c) AS cp_cum_usable_rtns_rcpt_c,
 SUM(cp_cum_usable_rtns_rcpt_r) AS cp_cum_usable_rtns_rcpt_r,
 SUM(cp_wh_cum_usable_rtns_rcpt_u) AS cp_wh_cum_usable_rtns_rcpt_u,
 SUM(cp_wh_cum_usable_rtns_rcpt_c) AS cp_wh_cum_usable_rtns_rcpt_c,
 SUM(cp_wh_cum_usable_rtns_rcpt_r) AS cp_wh_cum_usable_rtns_rcpt_r,
 SUM(cp_rp_in_transit_u) AS cp_rp_in_transit_u,
 SUM(cp_rp_in_transit_c) AS cp_rp_in_transit_c,
 SUM(cp_rp_in_transit_r) AS cp_rp_in_transit_r,
 SUM(cp_rp_oo_u) AS cp_rp_oo_u,
 SUM(cp_rp_oo_c) AS cp_rp_oo_c,
 SUM(cp_rp_oo_r) AS cp_rp_oo_r,
 SUM(cp_rp_rcpt_u) AS cp_ttl_rcpt_u,
 SUM(cp_rp_rcpt_c) AS cp_ttl_rcpt_c,
 SUM(cp_rp_rcpt_r) AS cp_ttl_rcpt_r,
 SUM(cp_aip_rcpt_plan_booked_u) AS cp_aip_rcpt_plan_booked_u,
 SUM(cp_aip_rcpt_plan_booked_c) AS cp_aip_rcpt_plan_booked_c,
 SUM(cp_aip_rcpt_plan_booked_r) AS cp_aip_rcpt_plan_booked_r,
 SUM(cp_rp_ant_spd_cum_usable_rtns_rcpt_u) AS cp_rp_ant_spd_cum_usable_rtns_rcpt_u,
 SUM(cp_rp_ant_spd_cum_usable_rtns_rcpt_c) AS cp_rp_ant_spd_cum_usable_rtns_rcpt_c,
 SUM(cp_rp_ant_spd_cum_usable_rtns_rcpt_r) AS cp_rp_ant_spd_cum_usable_rtns_rcpt_r,
 SUM(cp_rp_ant_spd_sys_extra_rcpt_u) AS cp_rp_ant_spd_sys_extra_rcpt_u,
 SUM(cp_rp_ant_spd_sys_extra_rcpt_c) AS cp_rp_ant_spd_sys_extra_rcpt_c,
 SUM(cp_rp_ant_spd_sys_extra_rcpt_r) AS cp_rp_ant_spd_sys_extra_rcpt_r,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS rcd_update_tmstmp
FROM rp_ant_spend
GROUP BY week_idnt,
 week_label,
 month_idnt,
 month_label,
 quarter_label,
 quarter,
 year,
 channel_number,
 channel_brand,
 channel_country,
 selling_channel,
 division,
 subdivision,
 department,
 class,
 subclass,
 category,
 supplier_idnt,
 supplier,
 vendor_label_code,
 vendor_label_name,
 vendor_label_desc,
 vendor_brand_name,
 supplier_group,
 replan_week_idnt,
 replan_week;


--{env_suffix}


DELETE FROM `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.rp_ant_spend_cp_history
WHERE CAST(replan_week_idnt AS FLOAT64) = (SELECT DISTINCT replan_week_idnt
  FROM rp_ant_spend_hist);


--{env_suffix}


INSERT INTO `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.rp_ant_spend_cp_history
(SELECT CAST(replan_week_idnt AS STRING) AS replan_week_idnt,
  CAST(replan_week AS STRING) AS replan_week,
  CAST(week_idnt AS STRING) AS week_idnt,
  week_label,
  CAST(month_idnt AS STRING) AS month_idnt,
  month_label,
  quarter_label,
  quarter,
  CAST(year AS STRING) AS year,
  channel_number,
  channel_brand,
  channel_country,
  selling_channel,
  division,
  subdivision,
  department,
  class,
  subclass,
  category,
  supplier_idnt,
  supplier,
  vendor_label_code,
  vendor_label_name,
  vendor_label_desc,
  vendor_brand_name,
  supplier_group,
  cp_cum_usable_rtns_rcpt_u,
  cp_cum_usable_rtns_rcpt_c,
  cp_cum_usable_rtns_rcpt_r,
  cp_wh_cum_usable_rtns_rcpt_u,
  cp_wh_cum_usable_rtns_rcpt_c,
  cp_wh_cum_usable_rtns_rcpt_r,
  cp_rp_in_transit_u,
  cp_rp_in_transit_c,
  cp_rp_in_transit_r,
  cp_rp_oo_u,
  cp_rp_oo_c,
  cp_rp_oo_r,
  cp_ttl_rcpt_u,
  cp_ttl_rcpt_c,
  cp_ttl_rcpt_r,
  cp_aip_rcpt_plan_booked_u,
  cp_aip_rcpt_plan_booked_c,
  cp_aip_rcpt_plan_booked_r,
  cp_rp_ant_spd_cum_usable_rtns_rcpt_u,
  cp_rp_ant_spd_cum_usable_rtns_rcpt_c,
  cp_rp_ant_spd_cum_usable_rtns_rcpt_r,
  cp_rp_ant_spd_sys_extra_rcpt_u,
  cp_rp_ant_spd_sys_extra_rcpt_c,
  cp_rp_ant_spd_sys_extra_rcpt_r,
  CAST(rcd_update_tmstmp AS TIMESTAMP) AS rcd_update_tmstmp,
  jwn_udf.default_tz_pst() as rcd_update_tmstmp_tz
 FROM rp_ant_spend_hist
 WHERE replan_week = 1);


-- end