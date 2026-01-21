/*
Name:Demand by Fulfillment Method script
Project:Demand by Fulfillment Method Dash
Purpose:  In the demand by fulfillment dashboard, there is a custom sql query and the query was using mdam data source, This script is replacing mdam data source to NAP data source(JWN_DEMAND_METRIC_VW)
Variable(s):    {{environment_schema}} T2DL_DAS_ACE_MFP
                {{env_suffix}} '' or '_dev' tablesuffix for prod testing
DAG:
Author(s):Xiao Tong
Date Created:1/23/23
Date Last Updated:4/19/24
*/
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'demand_by_fulfillment', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.demand_by_fulfillment, FALLBACK,
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
	fulfill_method VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
--  product dimensions
	, division VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
	, subdivision VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
	, department VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
	, "class" VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
	, subclass VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
	, supplier VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
	, price_type VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
	, npg_ind CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC
--  location dimensions
	, channel VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
	, location VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
--  fiscal dimensions
	, fiscal_week_number INTEGER
	, fiscal_week VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
	, fiscal_month VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
	, fiscal_quarter VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
	, fiscal_half VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
--  data source info
	, data_source VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('NAP')
--  Let users of the data source know which weeks are available
	, data_through_week VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
--ty metrics
	, demand_ty DECIMAL(38,2)
	, demand_units_ty INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, demand_shipped_ty DECIMAL(38,2)
	, demand_shipped_units_ty INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
-- LY metrics DECIMAL(38,9)
	, demand_ly DECIMAL(38,2)
	, demand_units_ly INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, demand_shipped_ly DECIMAL(38,2)
	, demand_shipped_units_ly INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
-- LLY metrics
	-- , demand_lly DECIMAL(38,2)
	-- , demand_units_lly INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	-- , demand_shipped_lly DECIMAL(38,2)
	-- , demand_shipped_units_lly INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, process_tmstp TIMESTAMP(6) WITH TIME ZONE
	, rp_ind CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC
)
PRIMARY INDEX(fulfill_method, department, "class", supplier, location, fiscal_week_number)
;

Grant SELECT ON {environment_schema}.demand_by_fulfillment TO PUBLIC;