SET QUERY_BAND = 'App_ID=APP08823;
     DAG_ID=affiliates_cost_11521_ACE_ENG;
     Task_Name=ddl_affiliates_cost_override_ldg;'
     FOR SESSION VOLATILE;


/*

T2/Table Name: T2DL_DAS_FUNNEL_IO.affiliates_cost_override_ldg
Team/Owner: Analytics Engineering
Date Created/Modified: 05/31/23

Note:
This landing table is created as part of the hive to td job that loads
data from hive to teradata.  The landing table is dropped when the job completes.

*/


CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{funnel_io_t2_schema}', 'affiliates_cost_override_ldg', OUT_RETURN_MSG);

CREATE MULTISET TABLE {funnel_io_t2_schema}.affiliates_cost_override_ldg
    , FALLBACK
    , NO BEFORE JOURNAL
    , NO AFTER JOURNAL
    , CHECKSUM = DEFAULT
    , DEFAULT MERGEBLOCKRATIO
    (
      banner            VARCHAR(16) CHARACTER SET Unicode NOT CaseSpecific compress
    , country           CHAR(3) CHARACTER SET Unicode NOT CaseSpecific compress
    , day_date          DATE
    , lf_cost           FLOAT
    , total_cost        FLOAT
    )
PRIMARY INDEX(day_date)
;

SET QUERY_BAND = NONE FOR SESSION;