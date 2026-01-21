SET QUERY_BAND = 'App_ID=APP08743;
     DAG_ID=mta_finance_forecast_11521_ACE_ENG;
     Task_Name=ddl_mta_finance_forecast_ldg;'
     FOR SESSION VOLATILE;

/*

T2/Table Name:n TD2L_DAS_MTA.FINANCE_FORECAST
Team/Owner: MOA/Nhan Le
Date Created/Modified: 2023-03-22

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{mta_t2_schema}', 'finance_forecast_ldg', OUT_RETURN_MSG);

create multiset table {mta_t2_schema}.finance_forecast_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
     "date"               date
    ,"box"                varchar(10) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,marketing_type       varchar(10) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,cost                 decimal(18,2)
    ,traffic_udv          decimal(18,2)
    ,orders               decimal(18,2)
    ,gross_sales          decimal(18,2)
    ,net_sales            decimal(18,2)
    ,"sessions"           decimal(18,2)
    ,session_orders       decimal(18,2)
    )
primary index("date")
partition by range_n("date" BETWEEN DATE'2021-01-01' AND DATE'2031-12-31' EACH INTERVAL '1' DAY, NO RANGE)
;

SET QUERY_BAND = NONE FOR SESSION;