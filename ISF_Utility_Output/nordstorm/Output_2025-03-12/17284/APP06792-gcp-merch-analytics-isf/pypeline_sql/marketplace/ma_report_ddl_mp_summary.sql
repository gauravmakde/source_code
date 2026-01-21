/*
Marketplace Summary DDL
Author: Asiyah Fox
Date Created: 4/25/24
Date Last Updated: 4/25/24

Datalab: t2dl_das_insights_delivery_secure
Service Account: T2DL_NAP_IDS_BATCH
Creates Table: mp_summary
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'mp_summary{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.mp_summary{env_suffix}
    ,FALLBACK
	,NO BEFORE JOURNAL
	,NO AFTER JOURNAL
	,CHECKSUM = DEFAULT
	,DEFAULT MERGEBLOCKRATIO
	,MAP = TD_MAP1
(	
	 week_end_day_date                      DATE
	,week_start_day_date                    DATE
	,week_idnt                              INTEGER
	,week_label                             VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	,month_idnt                             INTEGER
	,month_label                            VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	,quarter_idnt                           INTEGER
	,quarter_label                          VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
	,fiscal_year_num                        INTEGER
	,mp_ind                                 CHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('MP', 'NCOM')
	,style_group_num                        VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
	,style_group_desc                       VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,style_desc                             VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,color_num                              VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
	,color_desc                             VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,div_num                                INTEGER
	,division                               VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,sdiv_num                               INTEGER
	,subdivision                            VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,dept_num                               INTEGER
	,department                             VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,class_num                              INTEGER
	,"class"                                VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,sbclass_num                            INTEGER
	,subclass                               VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,category                               VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,category_group                         VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,supp_num                               INTEGER
	,seller                                 VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,los_ind                                CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('Y', 'N')
	,first_pub_date                         DATE
	,days_live                              INTEGER
	,order_sessions                         INTEGER
	,order_demand_r                         DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	,order_demand_u                         INTEGER
	,bag_add_u                              INTEGER
	,product_views                          DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	,instock_views                          DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	,scored_views                           DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	,op_gmv_u                               INTEGER
	,op_gmv_r                               DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	,ful_demand_u                           INTEGER
	,ful_demand_r                           DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	,rep_demand_u                           INTEGER
	,rep_demand_r                           DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	,returns_u                              INTEGER
	,returns_r                              DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
	,price_band_aur                         DECIMAL(10,2) DEFAULT 0.00 COMPRESS 0.00
	,update_timestamp    		            TIMESTAMP(6) WITH TIME ZONE	
)
PRIMARY INDEX (week_start_day_date, supp_num, category, subclass, department)
PARTITION BY RANGE_N (week_start_day_date BETWEEN DATE '2012-01-01' AND DATE '2035-12-31' EACH INTERVAL '1' DAY)
;
--GRANT SELECT ON {environment_schema}.mp_summary{env_suffix} TO PUBLIC; SECURE::DO NOT GRANT TO PUBLIC