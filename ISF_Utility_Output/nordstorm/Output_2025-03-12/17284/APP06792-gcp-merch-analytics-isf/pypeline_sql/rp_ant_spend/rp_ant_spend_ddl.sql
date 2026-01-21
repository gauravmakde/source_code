/*
Name: RP Anticipated Spend DDL
Project: RP Anticipated Spend Report
Purpose: Query that creates DDL for the RP Ant Spend Report in Cost
Variable(s):    {{environment_schema}} T2DL_DAS_OPEN_TO_BUY
                {{env_suffix}} '' or '{{env_suffix}}' tablesuffix for prod testing
DAG: ast_rp_ant_spend_prod_ddl
Author(s): Sara Riker
Date Created: 12/29/22
Date Last Updated: 1/3/22
*/

-- begin

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'rp_ant_spend_report{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.rp_ant_spend_report{env_suffix}, FALLBACK,
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
     week_idnt VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,week_label VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,month_idnt VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,month_label VARCHAR(64) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,quarter_label VARCHAR(64) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,Quarter VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,"Year" VARCHAR(64) CHARACTER SET UNICODE NOT CASESPECIFIC 
	-- Channel
    ,channel_number VARCHAR(64) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,channel_brand VARCHAR(64) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,channel_country VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,selling_channel VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC
    -- hierarchy
    ,vpn_clr VARCHAR(250) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,supp_part_num VARCHAR(64) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,epm_style_num VARCHAR(64) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,VPN VARCHAR(64) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,customer_choice VARCHAR(250) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,epm_choice_num BIGINT 
    ,nrf_color VARCHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,color_desc VARCHAR(64) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,style_desc VARCHAR(250) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,Division VARCHAR(64) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,Subdivision VARCHAR(64) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,Department VARCHAR(64) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,"Class" VARCHAR(64) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,Subclass VARCHAR(64) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,Category VARCHAR(64) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,supplier_idnt VARCHAR(64) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,Supplier VARCHAR(64) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,vendor_label_code INTEGER
    ,vendor_label_name VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,vendor_label_desc VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,vendor_brand_name VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,supplier_group VARCHAR(64) CHARACTER SET UNICODE NOT CASESPECIFIC
    -- metrics 
    -- TY
    ,ty_rp_in_transit_u                    DECIMAL(20,4)
    ,ty_rp_in_transit_c                    DECIMAL(20,4)
    ,ty_rp_in_transit_r                    DECIMAL(20,4)
    ,ty_rp_oo_u                             DECIMAL(20,4)
    ,ty_rp_oo_c                             DECIMAL(20,4)
    ,ty_rp_oo_r                             DECIMAL(20,4)
    ,ty_rp_rcpt_u                           DECIMAL(20,4)
    ,ty_rp_rcpt_c                          DECIMAL(20,4)
    ,ty_rp_rcpt_r                          DECIMAL(20,4)
    -- CP
    ,cp_cum_usable_rtns_rcpt_u              DECIMAL(20,4)
    ,cp_cum_usable_rtns_rcpt_c              DECIMAL(20,4)
    ,cp_cum_usable_rtns_rcpt_r              DECIMAL(20,4)
    ,cp_wh_cum_usable_rtns_rcpt_u           DECIMAL(20,4)
    ,cp_wh_cum_usable_rtns_rcpt_c           DECIMAL(20,4)
    ,cp_wh_cum_usable_rtns_rcpt_r           DECIMAL(20,4)
    ,cp_rp_in_transit_u                  DECIMAL(20,4)
    ,cp_rp_in_transit_c                  DECIMAL(20,4)
    ,cp_rp_in_transit_r                  DECIMAL(20,4)
    ,cp_rp_oo_u                          DECIMAL(20,4)
    ,cp_rp_oo_c                          DECIMAL(20,4)
    ,cp_rp_oo_r                          DECIMAL(20,4)
    ,cp_rp_rcpt_u                       DECIMAL(20,4)
    ,cp_rp_rcpt_c                       DECIMAL(20,4)
    ,cp_rp_rcpt_r                       DECIMAL(20,4)
    ,cp_aip_rcpt_plan_booked_u              DECIMAL(20,4)
    ,cp_aip_rcpt_plan_booked_c              DECIMAL(20,4)
    ,cp_aip_rcpt_plan_booked_r              DECIMAL(20,4)
    ,cp_rp_ant_spd_cum_usable_rtns_rcpt_u   DECIMAL(20,4)
    ,cp_rp_ant_spd_cum_usable_rtns_rcpt_c   DECIMAL(20,4)
    ,cp_rp_ant_spd_cum_usable_rtns_rcpt_r   DECIMAL(20,4)
    ,cp_rp_ant_spd_sys_extra_rcpt_u         DECIMAL(20,4)
    ,cp_rp_ant_spd_sys_extra_rcpt_c         DECIMAL(20,4)
    ,cp_rp_ant_spd_sys_extra_rcpt_r         DECIMAL(20,4)
	,rp_ant_spd_u                           DECIMAL(20,4)
    ,rp_ant_spd_c                           DECIMAL(20,4)
    ,rp_ant_spd_r							DECIMAL(20,4)
    ,CP_UNCONS_RP_ANT_SPD_TTL_U				DECIMAL(20,4)
    ,CP_UNCONS_RP_ANT_SPD_TTL_C				DECIMAL(20,4)
    ,CP_UNCONS_RP_ANT_SPD_TTL_R				DECIMAL(20,4)
    ,CP_FLEX_LINE_U							DECIMAL(20,4)
    ,CP_FLEX_LINE_C							DECIMAL(20,4)
    ,CP_FLEX_LINE_R							DECIMAL(20,4)
    ,LCP_CONS_RP_ANT_SPD_TTL_U				DECIMAL(20,4)
    ,LCP_CONS_RP_ANT_SPD_TTL_C				DECIMAL(20,4)
    ,LCP_CONS_RP_ANT_SPD_TTL_R				DECIMAL(20,4)
    ,SP_CONS_RP_ANT_SPD_TTL_U				DECIMAL(20,4)
    ,SP_CONS_RP_ANT_SPD_TTL_C				DECIMAL(20,4)
    ,SP_CONS_RP_ANT_SPD_TTL_R				DECIMAL(20,4)
    -- update info
    ,rcd_update_tmstmp TIMESTAMP(6) WITH TIME ZONE
)
PRIMARY INDEX(week_idnt, channel_number, vpn_clr);
-- end