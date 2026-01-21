/*
Name: Store Customer Counts DDL
APPID-Name: APP09268 Merch Analytics - Store Reporting
Purpose: DDL for Customer Counts Store Reporting T2 Source Data
Variable(s):    "environment_schema" - T2DL_DAS_<PROJECT DATALAB> 
DAG: merch_ddl_cust_counts_store
Author(s): Alli Moore
Date Created: 2024-02-27
Date Last Updated: 2024-03-15
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'STORE_CUST_COUNT{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.STORE_CUST_COUNT{env_suffix}
     , FALLBACK
     , NO BEFORE JOURNAL
     , NO AFTER JOURNAL
     , CHECKSUM = DEFAULT
     , DEFAULT MERGEBLOCKRATIO
(
    week_idnt						INTEGER NOT NULL
    , store_num						INTEGER NOT NULL
	, store_name					VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC
	, ty_ly_lly_ind					VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('TY', 'LY')
	, week_desc						VARCHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
	, week_label					VARCHAR(17) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
	, fiscal_week_num				INTEGER NOT NULL
	, week_start_day_date			DATE NOT NULL
	, week_end_day_date				DATE NOT NULL
	, week_num_of_fiscal_month		INTEGER NOT NULL
	, month_idnt					INTEGER NOT NULL
	, month_desc					VARCHAR(9) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
	, month_label					VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
	, month_abrv					VARCHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
	, fiscal_month_num				INTEGER NOT NULL
	, month_start_day_date			DATE NOT NULL
	, month_start_week_idnt			INTEGER NOT NULL
	, month_end_day_date			DATE NOT NULL
	, month_end_week_idnt			INTEGER NOT NULL
	, quarter_idnt					INTEGER NOT NULL
	, fiscal_year_num				INTEGER NOT NULL
	-- store dimensions
	, store_type_code				VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC
	, store_type_desc				VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC
	, selling_store_ind				CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC
	, region_num					INTEGER
	, region_desc					VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC
	, business_unit_num				INTEGER
	, business_unit_desc			VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC
	, subgroup_num					INTEGER
	, subgroup_desc					VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC
	, store_dma_code				VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
	, dma_desc						VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC
	, dma_shrt_desc					VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC
	, rack_district_num				INTEGER
	, rack_district_desc			VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC
	, channel_num					INTEGER
	, channel_desc					VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC
	, comp_status_code				VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
	, comp_status_desc				VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC
	, cluster_climate				VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC
	, cluster_price					VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC
	, cluster_designer				VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC
	, cluster_presidents_cup		VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC
	-- customer dimensions
	, nordy_level					VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	-- metrics
	, CUST_COUNT					INTEGER
	, NTN_CUST_COUNT				INTEGER
	, NET_SPEND						DECIMAL(38,2)
	, NET_UNITS						INTEGER
	, NET_TRAN_CT					INTEGER
	, NET_UPT						DECIMAL(5,2)
	, GROSS_UNITS					INTEGER
	, GROSS_TRAN_CT					INTEGER
	, GROSS_UPT						DECIMAL(5,2)
	, GROSS_SPEND					DECIMAL(38,2)
	, TRIPS							INTEGER
)
PRIMARY INDEX (week_idnt, store_num);
GRANT SELECT ON {environment_schema}.STORE_CUST_COUNT{env_suffix} TO PUBLIC;

COMMENT ON TABLE {environment_schema}.STORE_CUST_COUNT{env_suffix} AS 'Source Data for Weekly Store Customer Counts Reporting';
