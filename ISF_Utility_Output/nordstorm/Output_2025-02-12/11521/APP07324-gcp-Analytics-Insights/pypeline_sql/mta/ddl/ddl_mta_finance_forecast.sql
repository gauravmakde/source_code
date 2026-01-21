SET QUERY_BAND = 'App_ID=APP08743;
     DAG_ID=ddl_mta_finance_forecast_11521_ACE_ENG;
     Task_Name=ddl_mta_finance_forecast;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_MTA.FINANCE_FORECAST
Team/Owner: MOA/NHAN LE
Date Created/Modified: 2023-03-22
Note:
-- What is the the purpose of the table: IMPORT MARKETING FINANCE MTA VALUES INTO NAP FOR MARKETING PERFORMANCE SUITE DASHBOARDS
-- What is the update cadence/lookback window: TABLE SCHEDULED TO UPDATE QUARTERLY
*/
create multiset table {mta_t2_schema}.finance_forecast
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
     "date"               DATE
    ,"box"                VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,marketing_type       VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,cost                 DECIMAL(18,2)
    ,traffic_udv          DECIMAL(18,2)
    ,orders               DECIMAL(18,2)
    ,gross_sales          DECIMAL(18,2)
    ,net_sales            DECIMAL(18,2)
    ,"sessions"           DECIMAL(18,2)
    ,session_orders       DECIMAL(18,2)
    ,dw_sys_load_tmstp    TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index("date")
--partition by range_n("date" BETWEEN DATE'2021-01-01' AND DATE'2031-12-31' EACH INTERVAL '1' DAY, NO RANGE)
;
-- Table Comment (STANDARD)
COMMENT ON  {mta_t2_schema}.finance_forecast IS 'Marketing Finance Forecast values';
-- Column comments (OPTIONAL)
COMMENT ON  {mta_t2_schema}.finance_forecast."date" IS 'Marketing finance forecast date';
COMMENT ON  {mta_t2_schema}.finance_forecast.box IS 'Marketing finance forecast banner: Nordstrom/Nordstrom Rack';
COMMENT ON  {mta_t2_schema}.finance_forecast.marketing_type IS 'Marketing finance forecast marketing type: Paid/Unpaid/Base';
COMMENT ON  {mta_t2_schema}.finance_forecast.cost IS 'Marketing finance forecast value for marketing costs';
COMMENT ON  {mta_t2_schema}.finance_forecast.traffic_udv IS 'Marketing finance forecast value for unique daily visitors';
COMMENT ON  {mta_t2_schema}.finance_forecast.orders IS 'Marketing finance forecast value for orders submitted';
COMMENT ON  {mta_t2_schema}.finance_forecast.gross_sales IS 'Marketing finance forecast value for total gross sales';
COMMENT ON  {mta_t2_schema}.finance_forecast.net_sales IS 'Marketing finance forecast value for estimated net sales';
COMMENT ON  {mta_t2_schema}.finance_forecast.sessions IS 'Marketing finance forecast value for total digital sessions';
COMMENT ON  {mta_t2_schema}.finance_forecast.session_orders IS 'Marketing finance forecast value for total digital session orders submitted';
SET QUERY_BAND = NONE FOR SESSION;