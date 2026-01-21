/* Creating a DDL specifically for historical rp ant spend data
   Created: Isaac Wallick
   Created on : 9/18/2023 */

 CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'rp_ant_spend_cp_history{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.rp_ant_spend_cp_history{env_suffix}, FALLBACK,
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
     replan_week_idnt VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,replan_week VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,week_idnt VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC 
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
    ,cp_cum_usable_rtns_rcpt_u              DECIMAL(20,4)
    ,cp_cum_usable_rtns_rcpt_c              DECIMAL(20,4)
    ,cp_cum_usable_rtns_rcpt_r              DECIMAL(20,4)
    ,cp_wh_cum_usable_rtns_rcpt_u           DECIMAL(20,4)
    ,cp_wh_cum_usable_rtns_rcpt_c           DECIMAL(20,4)
    ,cp_wh_cum_usable_rtns_rcpt_r           DECIMAL(20,4)
    ,cp_rp_in_transit_u                     DECIMAL(20,4)
    ,cp_rp_in_transit_c                     DECIMAL(20,4)
    ,cp_rp_in_transit_r                     DECIMAL(20,4)
    ,cp_rp_oo_u                             DECIMAL(20,4)
    ,cp_rp_oo_c                             DECIMAL(20,4)
    ,cp_rp_oo_r                             DECIMAL(20,4)
    ,cp_rp_rcpt_u                          DECIMAL(20,4)
    ,cp_rp_rcpt_c                          DECIMAL(20,4)
    ,cp_rp_rcpt_r                          DECIMAL(20,4)
    ,cp_aip_rcpt_plan_booked_u              DECIMAL(20,4)
    ,cp_aip_rcpt_plan_booked_c              DECIMAL(20,4)
    ,cp_aip_rcpt_plan_booked_r              DECIMAL(20,4)
    ,cp_rp_ant_spd_cum_usable_rtns_rcpt_u   DECIMAL(20,4)
    ,cp_rp_ant_spd_cum_usable_rtns_rcpt_c   DECIMAL(20,4)
    ,cp_rp_ant_spd_cum_usable_rtns_rcpt_r   DECIMAL(20,4)
    ,cp_rp_ant_spd_sys_extra_rcpt_u         DECIMAL(20,4)
    ,cp_rp_ant_spd_sys_extra_rcpt_c         DECIMAL(20,4)
    ,cp_rp_ant_spd_sys_extra_rcpt_r         DECIMAL(20,4)
    -- update info
    ,rcd_update_tmstmp TIMESTAMP(6) WITH TIME ZONE
)
PRIMARY INDEX(week_idnt, channel_number, division);