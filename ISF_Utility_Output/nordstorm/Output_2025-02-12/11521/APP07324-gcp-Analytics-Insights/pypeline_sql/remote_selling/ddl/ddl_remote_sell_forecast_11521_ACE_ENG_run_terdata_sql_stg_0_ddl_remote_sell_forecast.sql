SET QUERY_BAND = 'App_ID=APP08154;
     DAG_ID=ddl_remote_sell_forecast_11521_ACE_ENG;
     Task_Name=ddl_remote_sell_forecast;'
     FOR SESSION VOLATILE;

/*
Table definitions for T2DL_DAS_REMOTE_SELLING.REMOTE_SELL_FORECAST
Analytics: Agatha Mak
Table: AE ace.eng@nordstrom.com
*/
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('T2DL_DAS_REMOTE_SELLING', 'remote_sell_forecast', OUT_RETURN_MSG);

CREATE MULTISET TABLE T2DL_DAS_REMOTE_SELLING.remote_sell_forecast
(
    fiscal_week         INTEGER NOT NULL,
    day_date            DATE FORMAT 'YYYY-MM-DD' NOT NULL,
    digital_sales       BIGINT,
    dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
)
;

SET QUERY_BAND = NONE FOR SESSION;