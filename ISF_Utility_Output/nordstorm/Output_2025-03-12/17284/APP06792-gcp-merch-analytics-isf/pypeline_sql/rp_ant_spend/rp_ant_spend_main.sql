/*
Name: RP Anticipated Spend 
Project: RP Anticipated Spend Report
Purpose: Query that supports the RP Ant Spend Report in Cost
Variable(s):    {{environment_schema}} T2DL_DAS_OPEN_TO_BUY
                {{env_suffix}} '' or '{{env_suffix}}' tablesuffix for prod testing
DAG: ast_rp_ant_spend_prod_main
Author(s): Sara Riker
Date Created: 12/29/22
Date Last Updated: 1/3/22
*/

-- begin
-- DROP TABLE fiscal_year;
CREATE MULTISET VOLATILE TABLE fiscal_year AS (
SELECT
   fiscal_year_num
FROM prd_nap_usr_vws.day_cal_454_dim
WHERE day_date = CURRENT_DATE
GROUP BY 1
) WITH DATA
PRIMARY INDEX (fiscal_year_num)
ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE vpns AS (
SELECT DISTINCT 
     vpn_clr
    ,chnl
FROM T2DL_DAS_RP_ANTICIPATED_SPEND_REPORTING.RP_ANT_SPEND_DATA

UNION

SELECT DISTINCT 
     vpn_clr
    ,chnl
FROM T2DL_DAS_RP_ANTICIPATED_SPEND_REPORTING.TY_ANT_SPEND_DATA
) WITH DATA
PRIMARY INDEX(vpn_clr, chnl)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
     PRIMARY INDEX(vpn_clr, chnl)
     ON vpns;

-- DROP TABLE hierarchy;
CREATE MULTISET VOLATILE TABLE hierarchy AS (
SELECT DISTINCT
     a.vpn_clr 
    ,a.channel_country
    ,a.nrf_color
    ,COALESCE(b.supp_part_num, c.supp_part_num, a.supp_part_num) AS supp_part_num
    ,COALESCE(b.epm_style_num, c.epm_style_num, a.epm_style_num) AS epm_style_num
    ,COALESCE(b.VPN, c.VPN, d.VPN) AS VPN
    ,COALESCE(b.customer_choice, c.customer_choice, d.customer_choice) AS customer_choice
    ,COALESCE(b.epm_choice_num, c.epm_choice_num, d.epm_choice_num) AS epm_choice_num
    ,COALESCE(b.color_desc, c.color_desc, d.color_desc) AS color_desc
    ,COALESCE(b.style_desc, c.style_desc, d.style_desc) AS style_desc
    ,COALESCE(b.Division, c.Division, d.Division) AS Division
    ,COALESCE(b.Subdivision, c.Subdivision, d.Subdivision) AS Subdivision
    ,COALESCE(b.department, c.department, d.department) AS Department
    ,COALESCE(b.clss, c.clss, d.clss) AS "Class"
    ,COALESCE(b.subclass, c.subclass, d.subclass) AS Subclass
    ,COALESCE(b.Category, c.category, d.category) AS Category
    ,COALESCE(b.supplier_idnt, c.supplier_idnt, d.supplier_idnt) AS supplier_idnt
    ,COALESCE(b.vendor_name, c.vendor_name, d.vendor_name) AS Supplier
    ,COALESCE(b.vendor_label_code, c.vendor_label_code, d.vendor_label_code) AS vendor_label_code
    ,COALESCE(b.vendor_label_name, c.vendor_label_name, d.vendor_label_name) AS vendor_label_name
    ,COALESCE(b.vendor_label_desc, c.vendor_label_desc, d.vendor_label_desc) AS vendor_label_desc
    ,COALESCE(b.vendor_brand_name, c.vendor_brand_name, d.vendor_brand_name) AS vendor_brand_name
    ,COALESCE(b.fp_supplier_group, c.fp_supplier_group, d.fp_supplier_group) AS fp_supplier_group
    ,COALESCE(b.op_supplier_group, c.op_supplier_group, d.op_supplier_group) AS op_supplier_group

FROM (
        SELECT DISTINCT
             vpn_clr
            ,TRIM(RIGHT(vpn_clr, 3)) AS nrf_color
            ,TRIM(STRTOK(vpn_clr,'_',1)) AS epm_style_num
            ,CASE WHEN CHAR_LENGTH(vpn_clr) - CHAR_LENGTH(TRIM(OREPLACE(vpn_clr,'_',''))) = 2 THEN UPPER(STRTOK(vpn_clr,'_',2))
                  WHEN CHAR_LENGTH(vpn_clr) - CHAR_LENGTH(TRIM(OREPLACE(vpn_clr,'_',''))) = 3 THEN UPPER(STRTOK(vpn_clr,'_',2)) || '' || UPPER(STRTOK(vpn_clr,'_',3))
                  WHEN CHAR_LENGTH(vpn_clr) - CHAR_LENGTH(TRIM(OREPLACE(vpn_clr,'_',''))) = 4 THEN UPPER(STRTOK(vpn_clr,'_',2)) || '' || UPPER(STRTOK(vpn_clr,'_',3)) || '' || UPPER(STRTOK(vpn_clr,'_',4))
                  WHEN CHAR_LENGTH(vpn_clr) - CHAR_LENGTH(TRIM(OREPLACE(vpn_clr,'_',''))) = 5 THEN UPPER(STRTOK(vpn_clr,'_',2)) || '' || UPPER(STRTOK(vpn_clr,'_',3)) || '' || UPPER(STRTOK(vpn_clr,'_',4)) || '' || UPPER(STRTOK(vpn_clr,'_',5))
                  WHEN CHAR_LENGTH(vpn_clr) - CHAR_LENGTH(TRIM(OREPLACE(vpn_clr,'_',''))) = 6 THEN UPPER(STRTOK(vpn_clr,'_',2)) || '' || UPPER(STRTOK(vpn_clr,'_',3)) || '' || UPPER(STRTOK(vpn_clr,'_',4)) || '' || UPPER(STRTOK(vpn_clr,'_',5)) || '' || UPPER(STRTOK(vpn_clr,'_',6))
                  END AS supp_part_num
            ,c.channel_country
        FROM vpns a
        JOIN (
                SELECT DISTINCT 
                     channel_num
                    ,channel_country
                FROM PRD_NAP_USR_VWS.PRICE_STORE_DIM_VW
                WHERE channel_num IS NOT NULL
                  AND selling_channel <> 'UNKNOWN'
            ) c
          ON a.chnl = c.channel_num 
        WHERE c.channel_country = 'US'
    ) a
LEFT JOIN (
            SELECT DISTINCT
                 TRIM(epm_style_num) epm_style_num
                ,supp_part_num
                ,style_desc
                ,TRIM(color_num) AS color_num
                ,supp_part_num || ', ' || style_desc AS VPN
                ,supp_part_num || ', ' || style_desc || ': ' || color_desc AS customer_choice
                ,epm_choice_num
                ,color_desc
                ,TRIM(div_idnt || ' ' || div_desc) AS Division
                ,TRIM(grp_idnt || ' ' || grp_desc) AS Subdivision
                ,TRIM(dept_idnt || ' ' || dept_desc) AS Department
                ,TRIM(class_idnt || ' ' || class_desc) AS clss
                ,TRIM(sbclass_idnt || ' ' || sbclass_desc) AS Subclass
                ,quantrix_category AS Category
                ,channel_country
                ,quantrix_category
                ,supplier_idnt
                ,vendor_name
                ,vendor_label_code
                ,vendor_label_name
                ,vendor_label_desc
                ,vendor_brand_name
                ,fp_supplier_group
                ,op_supplier_group
            FROM t2dl_das_assortment_dim.assortment_hierarchy
        ) b
  ON a.epm_style_num = b.epm_style_num 
 AND a.supp_part_num = OREPLACE(REGEXP_REPLACE(b.supp_part_num, '[-/":_]', '',1,0), ' ', '')
 AND a.channel_country = b.channel_country
 AND a.nrf_color = b.color_num
 
LEFT JOIN (
            SELECT DISTINCT
                 TRIM(epm_style_num) epm_style_num
                ,supp_part_num
                ,style_desc
                ,TRIM(color_num) AS color_num
                ,supp_part_num || ', ' || style_desc AS VPN
                ,supp_part_num || ', ' || style_desc || ': ' || color_desc AS customer_choice
                ,epm_choice_num
                ,color_desc
                ,TRIM(div_idnt || ' ' || div_desc) AS Division
                ,TRIM(grp_idnt || ' ' || grp_desc) AS Subdivision
                ,TRIM(dept_idnt || ' ' || dept_desc) AS Department
                ,TRIM(class_idnt || ' ' || class_desc) AS clss
                ,TRIM(sbclass_idnt || ' ' || sbclass_desc) AS Subclass
                ,quantrix_category AS Category
                ,quantrix_category
                ,supplier_idnt
                ,vendor_name
                ,vendor_label_code
                ,vendor_label_name
                ,vendor_label_desc
                ,vendor_brand_name
                ,fp_supplier_group
                ,op_supplier_group
            FROM t2dl_das_assortment_dim.assortment_hierarchy
            WHERE channel_country = 'US'
        ) c
  ON a.epm_style_num = c.epm_style_num 
 AND a.supp_part_num = OREPLACE(REGEXP_REPLACE(c.supp_part_num, '[-/":_]', '',1,0), ' ', '')
 AND a.nrf_color = c.color_num
 
LEFT JOIN (
            SELECT DISTINCT
                 TRIM(epm_style_num) epm_style_num
                ,supp_part_num
                ,style_desc
                ,TRIM(color_num) AS color_num
                ,supp_part_num || ', ' || style_desc AS VPN
                ,supp_part_num || ', ' || style_desc || ': ' || color_desc AS customer_choice
                ,epm_choice_num
                ,color_desc
                ,TRIM(div_idnt || ' ' || div_desc) AS Division
                ,TRIM(grp_idnt || ' ' || grp_desc) AS Subdivision
                ,TRIM(dept_idnt || ' ' || dept_desc) AS Department
                ,TRIM(class_idnt || ' ' || class_desc) AS clss
                ,TRIM(sbclass_idnt || ' ' || sbclass_desc) AS Subclass
                ,quantrix_category AS Category
                ,quantrix_category
                ,supplier_idnt
                ,vendor_name
                ,vendor_label_code
                ,vendor_label_name
                ,vendor_label_desc
                ,vendor_brand_name
                ,fp_supplier_group
                ,op_supplier_group
            FROM t2dl_das_assortment_dim.assortment_hierarchy
        ) d
  ON a.epm_style_num = d.epm_style_num 
 AND a.supp_part_num = OREPLACE(REGEXP_REPLACE(d.supp_part_num, '[-/":_]', '',1,0), ' ', '')
 AND a.nrf_color = d.color_num
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.vpn_clr, a.channel_country ORDER BY b.supplier_idnt, c.supplier_idnt desc) = 1
) 
WITH DATA
PRIMARY INDEX(vpn_clr)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(vpn_clr)
     ON hierarchy;
     


CREATE MULTISET VOLATILE TABLE rp_ant_spend_combine AS (  
SELECT 
     week_idnt
    ,chnl
    ,vpn_clr
    ,vpn
    ,SUM(COALESCE(ty_rp_in_transit_u, 0)) AS ty_rp_in_transit_u
    ,SUM(COALESCE(ty_rp_in_transit_c, 0)) AS ty_rp_in_transit_c
    ,SUM(COALESCE(ty_rp_in_transit_r, 0)) AS ty_rp_in_transit_r
    ,SUM(COALESCE(ty_rp_oo_u, 0)) AS ty_rp_oo_u
    ,SUM(COALESCE(ty_rp_oo_c, 0)) AS ty_rp_oo_c
    ,SUM(COALESCE(ty_rp_oo_r, 0)) AS ty_rp_oo_r
    ,SUM(COALESCE(ty_rp_rcpt_u, 0)) AS ty_rp_rcpt_u
    ,SUM(COALESCE(ty_rp_rcpt_c, 0)) AS ty_rp_rcpt_c
    ,SUM(COALESCE(ty_rp_rcpt_r, 0)) AS ty_rp_rcpt_r
    ,SUM(COALESCE(cp_cum_usable_rtns_rcpt_u, 0)) AS cp_cum_usable_rtns_rcpt_u
    ,SUM(COALESCE(cp_cum_usable_rtns_rcpt_c, 0)) AS cp_cum_usable_rtns_rcpt_c
    ,SUM(COALESCE(cp_cum_usable_rtns_rcpt_r, 0)) AS cp_cum_usable_rtns_rcpt_r
    ,SUM(COALESCE(cp_wh_cum_usable_rtns_rcpt_u, 0)) AS cp_wh_cum_usable_rtns_rcpt_u
    ,SUM(COALESCE(cp_wh_cum_usable_rtns_rcpt_c, 0)) AS cp_wh_cum_usable_rtns_rcpt_c
    ,SUM(COALESCE(cp_wh_cum_usable_rtns_rcpt_r, 0)) AS cp_wh_cum_usable_rtns_rcpt_r
    ,SUM(COALESCE(cp_rp_in_transit_u, 0)) AS cp_rp_in_transit_u
    ,SUM(COALESCE(cp_rp_in_transit_c, 0)) AS cp_rp_in_transit_c
    ,SUM(COALESCE(cp_rp_in_transit_r, 0)) AS cp_rp_in_transit_r
    ,SUM(COALESCE(cp_rp_oo_u, 0)) AS cp_rp_oo_u
    ,SUM(COALESCE(cp_rp_oo_c, 0)) AS cp_rp_oo_c
    ,SUM(COALESCE(cp_rp_oo_r, 0)) AS cp_rp_oo_r
    ,SUM(COALESCE(cp_rp_rcpt_u, 0)) AS cp_rp_rcpt_u
    ,SUM(COALESCE(cp_rp_rcpt_c, 0)) AS cp_rp_rcpt_c
    ,SUM(COALESCE(cp_rp_rcpt_r, 0)) AS cp_rp_rcpt_r
    ,SUM(COALESCE(cp_aip_rcpt_plan_booked_u, 0)) AS cp_aip_rcpt_plan_booked_u
    ,SUM(COALESCE(cp_aip_rcpt_plan_booked_c, 0)) AS cp_aip_rcpt_plan_booked_c
    ,SUM(COALESCE(cp_aip_rcpt_plan_booked_r, 0)) AS cp_aip_rcpt_plan_booked_r
    ,SUM(COALESCE(cp_rp_ant_spd_cum_usable_rtns_rcpt_u, 0)) AS cp_rp_ant_spd_cum_usable_rtns_rcpt_u
    ,SUM(COALESCE(cp_rp_ant_spd_cum_usable_rtns_rcpt_c, 0)) AS cp_rp_ant_spd_cum_usable_rtns_rcpt_c
    ,SUM(COALESCE(cp_rp_ant_spd_cum_usable_rtns_rcpt_r, 0)) AS cp_rp_ant_spd_cum_usable_rtns_rcpt_r
    ,SUM(COALESCE(cp_rp_ant_spd_sys_extra_rcpt_u, 0)) AS cp_rp_ant_spd_sys_extra_rcpt_u
    ,SUM(COALESCE(cp_rp_ant_spd_sys_extra_rcpt_c, 0)) AS cp_rp_ant_spd_sys_extra_rcpt_c
    ,SUM(COALESCE(cp_rp_ant_spd_sys_extra_rcpt_r, 0)) AS cp_rp_ant_spd_sys_extra_rcpt_r
    ,SUM(COALESCE(rp_ant_spd_u, 0)) AS rp_ant_spd_u
    ,SUM(COALESCE(rp_ant_spd_c, 0)) AS rp_ant_spd_c
    ,SUM(COALESCE(rp_ant_spd_r, 0)) AS rp_ant_spd_r
    ,SUM(COALESCE(CP_UNCONS_RP_ANT_SPD_TTL_U, 0)) AS CP_UNCONS_RP_ANT_SPD_TTL_U
    ,SUM(COALESCE(CP_UNCONS_RP_ANT_SPD_TTL_C, 0)) AS CP_UNCONS_RP_ANT_SPD_TTL_C
    ,SUM(COALESCE(CP_UNCONS_RP_ANT_SPD_TTL_R, 0)) AS CP_UNCONS_RP_ANT_SPD_TTL_R
    ,SUM(COALESCE(CP_FLEX_LINE_U, 0)) AS CP_FLEX_LINE_U
    ,SUM(COALESCE(CP_FLEX_LINE_C, 0)) AS CP_FLEX_LINE_C
    ,SUM(COALESCE(CP_FLEX_LINE_R, 0)) AS CP_FLEX_LINE_R
    ,SUM(COALESCE(LCP_CONS_RP_ANT_SPD_TTL_U, 0)) AS LCP_CONS_RP_ANT_SPD_TTL_U
    ,SUM(COALESCE(LCP_CONS_RP_ANT_SPD_TTL_C, 0)) AS LCP_CONS_RP_ANT_SPD_TTL_C
    ,SUM(COALESCE(LCP_CONS_RP_ANT_SPD_TTL_R, 0)) AS LCP_CONS_RP_ANT_SPD_TTL_R
    ,SUM(COALESCE(SP_CONS_RP_ANT_SPD_TTL_U, 0)) AS SP_CONS_RP_ANT_SPD_TTL_U
    ,SUM(COALESCE(SP_CONS_RP_ANT_SPD_TTL_C, 0)) AS SP_CONS_RP_ANT_SPD_TTL_C
    ,SUM(COALESCE(SP_CONS_RP_ANT_SPD_TTL_R, 0)) AS SP_CONS_RP_ANT_SPD_TTL_R
FROM (
      SELECT
           week_idnt
          ,chnl
          ,vpn_clr
          ,vpn

          -- TY
          ,CAST(0 AS DECIMAL(20,4)) AS ty_rp_in_transit_u
          ,CAST(0 AS DECIMAL(20,4)) AS ty_rp_in_transit_c
          ,CAST(0 AS DECIMAL(20,4)) AS ty_rp_in_transit_r
          ,CAST(0 AS DECIMAL(20,4)) AS ty_rp_oo_u
          ,CAST(0 AS DECIMAL(20,4)) AS ty_rp_oo_c
          ,CAST(0 AS DECIMAL(20,4)) AS ty_rp_oo_r
          ,CAST(0 AS DECIMAL(20,4)) AS ty_rp_rcpt_u
          ,CAST(0 AS DECIMAL(20,4)) AS ty_rp_rcpt_c
          ,CAST(0 AS DECIMAL(20,4)) AS ty_rp_rcpt_r

          -- CP 
          ,CP_CUM_USABLE_RTNS_U as cp_cum_usable_rtns_rcpt_u
          ,CP_CUM_USABLE_RTNS_C as cp_cum_usable_rtns_rcpt_c
          ,CP_CUM_USABLE_RTNS_R as cp_cum_usable_rtns_rcpt_r
          ,CP_WH_CUM_USABLE_RTNS_U as cp_wh_cum_usable_rtns_rcpt_u
          ,CP_WH_CUM_USABLE_RTNS_C as cp_wh_cum_usable_rtns_rcpt_c
          ,CP_WH_CUM_USABLE_RTNS_R as cp_wh_cum_usable_rtns_rcpt_r
          ,CP_RP_IN_TRAN_PO_U as cp_rp_in_transit_u
          ,CP_RP_IN_TRAN_PO_C as cp_rp_in_transit_c
          ,CP_RP_IN_TRAN_PO_R as cp_rp_in_transit_r
          ,cp_rp_oo_u
          ,cp_rp_oo_c
          ,cp_rp_oo_r
          ,cp_rp_rcpt_u
          ,cp_rp_rcpt_c
          ,cp_rp_rcpt_r
          ,CP_AIP_RCPT_PL_BKED_U as cp_aip_rcpt_plan_booked_u
          ,CP_AIP_RCPT_PL_BKED_C as cp_aip_rcpt_plan_booked_c
          ,CP_AIP_RCPT_PL_BKED_R as cp_aip_rcpt_plan_booked_r
          ,CP_RP_ANT_SPD_CUM_U_RTNS_U as cp_rp_ant_spd_cum_usable_rtns_rcpt_u
          ,CP_RP_ANT_SPD_CUM_U_RTNS_C as cp_rp_ant_spd_cum_usable_rtns_rcpt_c
          ,CP_RP_ANT_SPD_CUM_U_RTNS_R as cp_rp_ant_spd_cum_usable_rtns_rcpt_r
          ,CP_RP_ANT_SPD_SYS_EXTR_RCPT_U as cp_rp_ant_spd_sys_extra_rcpt_u
          ,CP_RP_ANT_SPD_SYS_EXTR_RCPT_C as cp_rp_ant_spd_sys_extra_rcpt_c
          ,CP_RP_ANT_SPD_SYS_EXTR_RCPT_R as cp_rp_ant_spd_sys_extra_rcpt_r
          ,CP_CONS_RP_ANT_SPD_TTL_U as rp_ant_spd_u
          ,CP_CONS_RP_ANT_SPD_TTL_C as rp_ant_spd_c
          ,CP_CONS_RP_ANT_SPD_TTL_R as rp_ant_spd_r
          ,CP_UNCONS_RP_ANT_SPD_TTL_U
          ,CP_UNCONS_RP_ANT_SPD_TTL_C
          ,CP_UNCONS_RP_ANT_SPD_TTL_R
          ,CP_FLEX_LINE_U
          ,CP_FLEX_LINE_C
          ,CP_FLEX_LINE_R
          ,LCP_CONS_RP_ANT_SPD_TTL_U
          ,LCP_CONS_RP_ANT_SPD_TTL_C
          ,LCP_CONS_RP_ANT_SPD_TTL_R
          ,SP_CONS_RP_ANT_SPD_TTL_U
          ,SP_CONS_RP_ANT_SPD_TTL_C
          ,SP_CONS_RP_ANT_SPD_TTL_R
      FROM T2DL_DAS_RP_ANTICIPATED_SPEND_REPORTING.RP_ANT_SPEND_DATA rp
      --t2dl_das_rp_anticipated_spend_reporting.RP_ANT_SPEND_DATA rp 
      JOIN (
                SELECT DISTINCT 
                     channel_num
                FROM PRD_NAP_USR_VWS.PRICE_STORE_DIM_VW
                WHERE channel_num IS NOT NULL
                  AND selling_channel <> 'UNKNOWN'
                  AND channel_country = 'US'
            ) c
          ON rp.chnl = c.channel_num 

      UNION ALL

      SELECT
          week_idnt
          ,chnl
          ,vpn_clr
          ,vpn

          -- TY
          ,ty_rp_in_transit_u
          ,ty_rp_in_transit_c
          ,ty_rp_in_transit_r
          ,ty_rp_oo_u
          ,ty_rp_oo_c
          ,ty_rp_oo_r
          ,ty_rp_rcpt_u
          ,ty_rp_rcpt_c
          ,ty_rp_rcpt_r

          -- CP 
          ,CAST(0 AS DECIMAL(20,4)) AS cp_cum_usable_rtns_rcpt_u
          ,CAST(0 AS DECIMAL(20,4)) AS cp_cum_usable_rtns_rcpt_c
          ,CAST(0 AS DECIMAL(20,4)) AS cp_cum_usable_rtns_rcpt_r
          ,CAST(0 AS DECIMAL(20,4)) AS cp_wh_cum_usable_rtns_rcpt_u
          ,CAST(0 AS DECIMAL(20,4)) AS cp_wh_cum_usable_rtns_rcpt_c
          ,CAST(0 AS DECIMAL(20,4)) AS cp_wh_cum_usable_rtns_rcpt_r
          ,CAST(0 AS DECIMAL(20,4)) AS cp_rp_in_transit_u
          ,CAST(0 AS DECIMAL(20,4)) AS cp_rp_in_transit_c
          ,CAST(0 AS DECIMAL(20,4)) AS cp_rp_in_transit_r
          ,CAST(0 AS DECIMAL(20,4)) AS cp_rp_oo_u
          ,CAST(0 AS DECIMAL(20,4)) AS cp_rp_oo_c
          ,CAST(0 AS DECIMAL(20,4)) AS cp_rp_oo_r
          ,CAST(0 AS DECIMAL(20,4)) AS cp_rp_rcpt_u
          ,CAST(0 AS DECIMAL(20,4)) AS cp_rp_rcpt_c
          ,CAST(0 AS DECIMAL(20,4)) AS cp_rp_rcpt_r
          ,CAST(0 AS DECIMAL(20,4)) AS cp_aip_rcpt_plan_booked_u
          ,CAST(0 AS DECIMAL(20,4)) AS cp_aip_rcpt_plan_booked_c
          ,CAST(0 AS DECIMAL(20,4)) AS cp_aip_rcpt_plan_booked_r
          ,CAST(0 AS DECIMAL(20,4)) AS cp_rp_ant_spd_cum_usable_rtns_rcpt_u
          ,CAST(0 AS DECIMAL(20,4)) AS cp_rp_ant_spd_cum_usable_rtns_rcpt_c
          ,CAST(0 AS DECIMAL(20,4)) AS cp_rp_ant_spd_cum_usable_rtns_rcpt_r
          ,CAST(0 AS DECIMAL(20,4)) AS cp_rp_ant_spd_sys_extra_rcpt_u
          ,CAST(0 AS DECIMAL(20,4)) AS cp_rp_ant_spd_sys_extra_rcpt_c
          ,CAST(0 AS DECIMAL(20,4)) AS cp_rp_ant_spd_sys_extra_rcpt_r
          ,CAST(0 AS DECIMAL(20,4)) AS rp_ant_spd_u
          ,CAST(0 AS DECIMAL(20,4)) AS rp_ant_spd_c
          ,CAST(0 AS DECIMAL(20,4)) AS rp_ant_spd_r
          ,CAST(0 AS DECIMAL(20,4)) AS CP_UNCONS_RP_ANT_SPD_TTL_U
          ,CAST(0 AS DECIMAL(20,4)) AS CP_UNCONS_RP_ANT_SPD_TTL_C
          ,CAST(0 AS DECIMAL(20,4)) AS CP_UNCONS_RP_ANT_SPD_TTL_R
          ,CAST(0 AS DECIMAL(20,4)) AS CP_FLEX_LINE_U
          ,CAST(0 AS DECIMAL(20,4)) AS CP_FLEX_LINE_C
          ,CAST(0 AS DECIMAL(20,4)) AS CP_FLEX_LINE_R
          ,CAST(0 AS DECIMAL(20,4)) AS LCP_CONS_RP_ANT_SPD_TTL_U
          ,CAST(0 AS DECIMAL(20,4)) AS LCP_CONS_RP_ANT_SPD_TTL_C
          ,CAST(0 AS DECIMAL(20,4)) AS LCP_CONS_RP_ANT_SPD_TTL_R
          ,CAST(0 AS DECIMAL(20,4)) AS SP_CONS_RP_ANT_SPD_TTL_U
          ,CAST(0 AS DECIMAL(20,4)) AS SP_CONS_RP_ANT_SPD_TTL_C
          ,CAST(0 AS DECIMAL(20,4)) AS SP_CONS_RP_ANT_SPD_TTL_R
      FROM T2DL_DAS_RP_ANTICIPATED_SPEND_REPORTING.TY_ANT_SPEND_DATA rp 
      JOIN (
                SELECT DISTINCT 
                     channel_num
                FROM PRD_NAP_USR_VWS.PRICE_STORE_DIM_VW
                WHERE channel_num IS NOT NULL
                  AND selling_channel <> 'UNKNOWN'
                  AND channel_country = 'US'
            ) c
          ON rp.chnl = c.channel_num 
  ) rp
GROUP BY week_idnt
        ,chnl
        ,vpn_clr
        ,vpn
) WITH DATA
PRIMARY INDEX(week_idnt, chnl, vpn_clr)
ON COMMIT PRESERVE ROWS;

--select distinct chnl from rp_ant_spend_combine

-- DROP TABLE rp_ant_spend;
CREATE MULTISET VOLATILE TABLE rp_ant_spend AS (
SELECT 
    -- calendar
     cal.week_idnt
    ,cal.week_label
    ,cal.month_idnt
    ,cal.month_label
    ,cal.quarter_label
    ,cal.Quarter
    ,cal."Year"
	  -- Channel
    ,c.channel_number
    ,c.channel_brand
    ,c.channel_country
    ,c.selling_channel
    -- hierarchy
    ,h.vpn_clr
    ,h.supp_part_num
    ,h.epm_style_num
    ,h.VPN
    ,h.customer_choice
    ,h.epm_choice_num
    ,h.nrf_color
    ,h.color_desc
    ,h.style_desc
    ,h.Division
    ,h.Subdivision
    ,h.Department
    ,h."Class"
    ,h.Subclass
    ,h.Category
    ,h.supplier_idnt 
    ,h.Supplier
    ,vendor_label_code
    ,vendor_label_name
    ,vendor_label_desc
    ,vendor_brand_name
    ,CASE WHEN channel_brand = 'NORDSTROM' THEN COALESCE(h.fp_supplier_group, 'OTHER')
          WHEN channel_brand = 'NORDSTROM_RACK' THEN COALESCE(h.op_supplier_group, 'OTHER')
          ELSE 'OTHER' END AS supplier_group 
    -- metrics 
    -- TY
    ,rp.ty_rp_in_transit_u
    ,rp.ty_rp_in_transit_c
    ,rp.ty_rp_in_transit_r
    ,rp.ty_rp_oo_u
    ,rp.ty_rp_oo_c
    ,rp.ty_rp_oo_r
    ,rp.ty_rp_rcpt_u
    ,rp.ty_rp_rcpt_c
    ,rp.ty_rp_rcpt_r
    -- CP
    ,rp.cp_cum_usable_rtns_rcpt_u
    ,rp.cp_cum_usable_rtns_rcpt_c
    ,rp.cp_cum_usable_rtns_rcpt_r
    ,rp.cp_wh_cum_usable_rtns_rcpt_u
    ,rp.cp_wh_cum_usable_rtns_rcpt_c
    ,rp.cp_wh_cum_usable_rtns_rcpt_r
    ,rp.cp_rp_in_transit_u
    ,rp.cp_rp_in_transit_c
    ,rp.cp_rp_in_transit_r
    ,rp.cp_rp_oo_u
    ,rp.cp_rp_oo_c
    ,rp.cp_rp_oo_r
    ,rp.cp_rp_rcpt_u
    ,rp.cp_rp_rcpt_c
    ,rp.cp_rp_rcpt_r
    ,rp.cp_aip_rcpt_plan_booked_u
    ,rp.cp_aip_rcpt_plan_booked_c
    ,rp.cp_aip_rcpt_plan_booked_r
    ,rp.cp_rp_ant_spd_cum_usable_rtns_rcpt_u
    ,rp.cp_rp_ant_spd_cum_usable_rtns_rcpt_c
    ,rp.cp_rp_ant_spd_cum_usable_rtns_rcpt_r
    ,rp.cp_rp_ant_spd_sys_extra_rcpt_u
    ,rp.cp_rp_ant_spd_sys_extra_rcpt_c
    ,rp.cp_rp_ant_spd_sys_extra_rcpt_r
    ,rp.rp_ant_spd_u
    ,rp.rp_ant_spd_c
    ,rp.rp_ant_spd_r
    ,CP_UNCONS_RP_ANT_SPD_TTL_U
    ,CP_UNCONS_RP_ANT_SPD_TTL_C
    ,CP_UNCONS_RP_ANT_SPD_TTL_R
    ,CP_FLEX_LINE_U
    ,CP_FLEX_LINE_C
    ,CP_FLEX_LINE_R
    ,LCP_CONS_RP_ANT_SPD_TTL_U
    ,LCP_CONS_RP_ANT_SPD_TTL_C
    ,LCP_CONS_RP_ANT_SPD_TTL_R
    ,SP_CONS_RP_ANT_SPD_TTL_U
    ,SP_CONS_RP_ANT_SPD_TTL_C
    ,SP_CONS_RP_ANT_SPD_TTL_R
    -- update info
    ,current_timestamp AS rcd_update_tmstmp
FROM rp_ant_spend_combine rp
JOIN (
        SELECT DISTINCT 
        	   channel_num
            ,TRIM(channel_num||' '||channel_desc) AS channel_number
            ,channel_brand
            ,channel_country
            ,selling_channel
        FROM prd_nap_usr_vws.price_store_dim_vw
        WHERE channel_number IS NOT NULL
          AND selling_channel <> 'UNKNOWN'
          AND channel_country = 'US'
    ) c
  ON rp.chnl = c.channel_num
JOIN hierarchy h
  on rp.vpn_clr = h.vpn_clr
 AND c.channel_country = h.channel_country
JOIN (
        SELECT
			week_label
     		, week_idnt
			, month_label
			, month_idnt
			, quarter_label
			, quarter_idnt 
			,'FY'||substr(cast(fiscal_year_num as varchar(5)),3,2)||' '||quarter_abrv AS Quarter
    		, fiscal_year_num as "Year"
	FROM prd_nap_usr_vws.day_cal_454_dim tme
	WHERE quarter_idnt <= (SELECT DISTINCT quarter_idnt + 10 FROM prd_nap_usr_vws.day_cal_454_dim a WHERE day_date = current_date )
	QUALIFY DENSE_RANK() OVER (ORDER BY quarter_idnt desc) < 9
	group by 1,2,3,4,5,6,7,8
    ) cal
  ON rp.week_idnt = cal.week_idnt
) 
WITH DATA
PRIMARY INDEX(week_idnt, channel_number, vpn_clr)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(week_idnt, channel_number, vpn_clr)
     ON rp_ant_spend;
    
    

DELETE FROM {environment_schema}.rp_ant_spend_report{env_suffix} ALL;
INSERT INTO {environment_schema}.rp_ant_spend_report{env_suffix}
SELECT * 
FROM rp_ant_spend
;


CREATE MULTISET VOLATILE TABLE rp_ant_spend_hist AS (
SELECT 
     (SELECT DISTINCT week_idnt FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = current_date) AS replan_week_idnt
    ,(SELECT CASE WHEN week_num_of_fiscal_month IN (1,3) THEN 1 ELSE 0 END AS replan_week FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = current_date) AS replan_week
    -- Calendar
    ,week_idnt
    ,week_label
    ,month_idnt
    ,month_label
    ,quarter_label
    ,Quarter
  	,"Year"
	-- Channel
    ,channel_number
    ,channel_brand
    ,channel_country
    ,selling_channel
    -- hierarchy
    ,Division
    ,Subdivision
    ,Department
    ,"Class"
    ,Subclass
    ,Category
    ,supplier_idnt 
    ,Supplier
    ,vendor_label_code
    ,vendor_label_name
    ,vendor_label_desc
    ,vendor_brand_name
    ,supplier_group 
     -- metrics     
    ,SUM(cp_cum_usable_rtns_rcpt_u             ) AS cp_cum_usable_rtns_rcpt_u
    ,SUM(cp_cum_usable_rtns_rcpt_c             ) AS cp_cum_usable_rtns_rcpt_c
    ,SUM(cp_cum_usable_rtns_rcpt_r             ) AS cp_cum_usable_rtns_rcpt_r
    ,SUM(cp_wh_cum_usable_rtns_rcpt_u          ) AS cp_wh_cum_usable_rtns_rcpt_u
    ,SUM(cp_wh_cum_usable_rtns_rcpt_c          ) AS cp_wh_cum_usable_rtns_rcpt_c
    ,SUM(cp_wh_cum_usable_rtns_rcpt_r          ) AS cp_wh_cum_usable_rtns_rcpt_r
    ,SUM(cp_rp_in_transit_u                    ) AS cp_rp_in_transit_u
    ,SUM(cp_rp_in_transit_c                    ) AS cp_rp_in_transit_c
    ,SUM(cp_rp_in_transit_r                    ) AS cp_rp_in_transit_r
    ,SUM(cp_rp_oo_u                            ) AS cp_rp_oo_u
    ,SUM(cp_rp_oo_c                            ) AS cp_rp_oo_c
    ,SUM(cp_rp_oo_r                            ) AS cp_rp_oo_r
    ,SUM(cp_rp_rcpt_u                         ) AS cp_ttl_rcpt_u
    ,SUM(cp_rp_rcpt_c                         ) AS cp_ttl_rcpt_c
    ,SUM(cp_rp_rcpt_r                         ) AS cp_ttl_rcpt_r
    ,SUM(cp_aip_rcpt_plan_booked_u             ) AS cp_aip_rcpt_plan_booked_u
    ,SUM(cp_aip_rcpt_plan_booked_c             ) AS cp_aip_rcpt_plan_booked_c
    ,SUM(cp_aip_rcpt_plan_booked_r             ) AS cp_aip_rcpt_plan_booked_r
    ,SUM(cp_rp_ant_spd_cum_usable_rtns_rcpt_u  ) AS cp_rp_ant_spd_cum_usable_rtns_rcpt_u
    ,SUM(cp_rp_ant_spd_cum_usable_rtns_rcpt_c  ) AS cp_rp_ant_spd_cum_usable_rtns_rcpt_c
    ,SUM(cp_rp_ant_spd_cum_usable_rtns_rcpt_r  ) AS cp_rp_ant_spd_cum_usable_rtns_rcpt_r
    ,SUM(cp_rp_ant_spd_sys_extra_rcpt_u        ) AS cp_rp_ant_spd_sys_extra_rcpt_u
    ,SUM(cp_rp_ant_spd_sys_extra_rcpt_c        ) AS cp_rp_ant_spd_sys_extra_rcpt_c
    ,SUM(cp_rp_ant_spd_sys_extra_rcpt_r        ) AS cp_rp_ant_spd_sys_extra_rcpt_r
    ,CURRENT_TIMESTAMP AS rcd_update_tmstmp  
FROM rp_ant_spend
GROUP BY week_idnt
    ,week_label
    ,month_idnt
    ,month_label
    ,quarter_label
    ,Quarter
  	,"Year"
    -- Channel
    ,channel_number
    ,channel_brand
    ,channel_country
    ,selling_channel
    -- hierarchy
    ,Division
    ,Subdivision
    ,Department
    ,"Class"
    ,Subclass
    ,Category
    ,supplier_idnt 
    ,Supplier
    ,vendor_label_code
    ,vendor_label_name
    ,vendor_label_desc
    ,vendor_brand_name
    ,supplier_group 
    ,replan_week_idnt
    ,replan_week
) 
WITH DATA
PRIMARY INDEX(week_idnt, channel_number, division)
ON COMMIT PRESERVE ROWS;

DELETE FROM {environment_schema}.rp_ant_spend_cp_history{env_suffix} WHERE replan_week_idnt = (SELECT DISTINCT replan_week_idnt FROM rp_ant_spend_hist);
INSERT INTO {environment_schema}.rp_ant_spend_cp_history{env_suffix}
SELECT * 
FROM rp_ant_spend_hist
WHERE replan_week = 1
;

-- end