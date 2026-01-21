SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=ddl_affiliates_cost_override_11521_ACE_ENG;
     Task_Name=ddl_affiliates_cost_override;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_FUNNEL_IO.affiliates_cost_override
Team/Owner: Marketing Optimization Analytics
Date Created/Modified: Migrated to ISF 12/22/2022 by Nate Eyre

Note:
Reads in presistent costs from a csv that the Affiliates team owns
Daily delete and re-insert from the csv
*/

CREATE MULTISET TABLE {funnel_io_t2_schema}.affiliates_cost_override
    , FALLBACK
    , NO BEFORE JOURNAL
    , NO AFTER JOURNAL
    , CHECKSUM = DEFAULT
    , DEFAULT MERGEBLOCKRATIO
    (
      banner            VARCHAR(16) compress
    , country           CHAR(3) compress
    , day_date          DATE
    , lf_cost           FLOAT
    , total_cost        FLOAT
    , dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
PRIMARY INDEX(day_date)
;

SET QUERY_BAND = NONE FOR SESSION;