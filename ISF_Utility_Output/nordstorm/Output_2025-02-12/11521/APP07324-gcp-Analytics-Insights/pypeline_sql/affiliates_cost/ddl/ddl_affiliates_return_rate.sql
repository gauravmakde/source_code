SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=ddl_affiliates_return_rate_11521_ACE_ENG;
     Task_Name=ddl_affiliates_return_rate;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_FUNNEL_IO.affiliates_return_rate
Team/Owner: Marketing Optimization Analytics
Date Created/Modified: Migrated to ISF 12/22/2022 by Nate Eyre

Note:
Reads in presistent costs from a csv that the Affiliates team owns
Daily delete and re-insert from the csv
*/

CREATE MULTISET TABLE {funnel_io_t2_schema}.affiliates_return_rate
    , FALLBACK
    , NO BEFORE JOURNAL
    , NO AFTER JOURNAL
    , CHECKSUM = DEFAULT
    , DEFAULT MERGEBLOCKRATIO
    (
      banner            VARCHAR(16) compress
    , country           CHAR(3) compress
    , start_day_date    DATE
    , end_day_date      DATE
    , return_rate       FLOAT
    , dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
PRIMARY INDEX(start_day_date)
;

SET QUERY_BAND = NONE FOR SESSION;