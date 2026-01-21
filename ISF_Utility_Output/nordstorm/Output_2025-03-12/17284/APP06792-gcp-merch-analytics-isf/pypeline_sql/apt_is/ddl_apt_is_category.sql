/*
APT In Season Category DDL
Author: Asiyah Fox
Date Created: 1/5/23
Date Last Updated: 5/31/23

Datalab: t2dl_das_apt_cost_reporting
Creates Table: apt_is_category
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'apt_is_category', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.apt_is_category
    ,FALLBACK
	,NO BEFORE JOURNAL
	,NO AFTER JOURNAL
	,CHECKSUM = DEFAULT
	,DEFAULT MERGEBLOCKRATIO
	,MAP = TD_MAP1
(
	channel_country                           CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC
	,banner                                   VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
	,date_ind                                 VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
	,month_idnt                               INTEGER
	,month_label                              VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
	,month_idnt_aligned                       INTEGER
	,month_label_aligned                      VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
	,month_start_day_date                     DATE
	,month_end_day_date                       DATE
	,active_store_ind                         CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('Y','N')
	,division_desc                            VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC
	,subdivision_desc                         VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC
	,department_desc                          VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC
	,department_num                           INTEGER
	,dropship_ind                             CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('Y','N')
	,category                                 VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC
	,category_planner_1                       VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
	,category_planner_2                       VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
	,category_group                           VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
	,seasonal_designation                     VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
	,rack_merch_zone                          VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
	,is_activewear                            VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
	,channel_category_roles_1                 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
	,channel_category_roles_2                 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
	,bargainista_dept_map                     VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
	,price_band                               VARCHAR(16) CHARACTER SET UNICODE NOT CASESPECIFIC
	,net_sls_r                                DECIMAL(38,4)
	,net_sls_c                                DECIMAL(38,4)
	,net_sls_units                            INTEGER
	,net_sls_reg_r                            DECIMAL(38,4)
	,net_sls_reg_c                            DECIMAL(38,4)
	,net_sls_reg_units                        INTEGER
	,returns_r                                DECIMAL(38,4)
	,returns_c                                DECIMAL(38,4)
	,returns_u                                INTEGER
	,demand_ttl_r                             DECIMAL(38,6)
	,demand_ttl_units                         INTEGER
	,eop_ttl_c                                DECIMAL(38,2)
	,eop_ttl_units                            INTEGER
	,bop_ttl_c                                DECIMAL(38,2)
	,bop_ttl_units                            INTEGER
	,ttl_porcpt_c                             DECIMAL(38,4)
	,ttl_porcpt_u                             INTEGER
	,ttl_porcpt_rp_c                          DECIMAL(38,4)
	,ttl_porcpt_rp_u                          INTEGER
	,ttl_porcpt_nrp_c                         DECIMAL(38,4)
	,ttl_porcpt_nrp_u                         INTEGER
	,pah_tsfr_in_c                            DECIMAL(38,4)
	,pah_tsfr_in_u                            INTEGER
	,pah_tsfr_out_c                           DECIMAL(38,4)
	,pah_tsfr_out_u                           INTEGER
	,rk_tsfr_in_c                             DECIMAL(38,4)
	,rk_tsfr_in_u                             INTEGER
	,rk_tsfr_out_c                            DECIMAL(38,4)
	,rk_tsfr_out_u                            INTEGER
	,rs_tsfr_in_c                             DECIMAL(38,4)
	,rs_tsfr_in_u                             INTEGER
	,rs_tsfr_out_c                            DECIMAL(38,4)
	,rs_tsfr_out_u                            INTEGER
    ,plan_bop_c                               DECIMAL(38,2)
    ,plan_bop_u                               INTEGER      
    ,plan_eop_c                               DECIMAL(38,2)
    ,plan_eop_u                               INTEGER      
    ,plan_receipts_c                          DECIMAL(38,4)
    ,plan_receipts_u                          INTEGER      
    ,plan_receipts_lr_c                       DECIMAL(38,4)
    ,plan_receipts_lr_u                       INTEGER      
    ,plan_sales_c                             DECIMAL(38,4)
    ,plan_sales_r                             DECIMAL(38,4)
    ,plan_sales_u                             INTEGER      
    ,plan_demand_r                            DECIMAL(38,4)
    ,plan_demand_u                            INTEGER      
	,update_timestamp	 	                  TIMESTAMP(6) WITH TIME ZONE
)
PRIMARY INDEX (date_ind, month_idnt, banner, channel_country, department_num, category, price_band);
GRANT SELECT ON {environment_schema}.apt_is_category TO PUBLIC;