SET QUERY_BAND = 'App_ID=APP08154;
     DAG_ID=remote_sell_forecast_11521_ACE_ENG;
     Task_Name=ddl_remote_sell_forecast_ldg;'
     FOR SESSION VOLATILE;

-----Table definition for T2DL_DAS_REMOTE_SELLING.REMOTE_SELL_TRANSACTIONS_LDG
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{remote_selling_t2_schema}', 'remote_sell_forecast_ldg', OUT_RETURN_MSG);

CREATE MULTISET TABLE {remote_selling_t2_schema}.remote_sell_forecast_ldg
(
    fiscal_week         INTEGER NOT NULL,
    day_date            DATE FORMAT 'YYYY-MM-DD' NOT NULL,
    digital_sales       BIGINT,
    phone_text_virtual  BIGINT
)
;

SET QUERY_BAND = NONE FOR SESSION;