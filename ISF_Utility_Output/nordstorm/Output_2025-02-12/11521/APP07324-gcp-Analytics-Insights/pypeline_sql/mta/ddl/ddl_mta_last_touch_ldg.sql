SET QUERY_BAND = 'App_ID=APP08047;
     DAG_ID=mta_11521_ACE_ENG;
     Task_Name=ddl_mta_last_touch_ldg;'
     FOR SESSION VOLATILE;


/*

T2/Table Name: T2DL_DAS_MTA.mta_last_touch_ldg
Team/Owner: Analytics Engineering
Date Created/Modified: 03/21/23

Note:
This landing table is created as part of the hive to td job that loads
data from hive to teradata.  The landing table is dropped when the job completes.


*/

call SYS_MGMT.DROP_IF_EXISTS_SP ('{mta_t2_schema}', 'mta_last_touch_ldg', OUT_RETURN_MSG);

CREATE MULTISET TABLE {mta_t2_schema}.mta_last_touch_ldg
    , FALLBACK
    , NO BEFORE JOURNAL
    , NO AFTER JOURNAL
    , CHECKSUM = DEFAULT
    , DEFAULT MERGEBLOCKRATIO
(
    order_number VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , mktg_type VARCHAR(32) COMPRESS CHARACTER SET UNICODE NOT CASESPECIFIC
    , utm_channel VARCHAR(400) COMPRESS CHARACTER SET UNICODE NOT CASESPECIFIC -- utm_channel VARCHAR(64) max 261
    , order_date_pacific date
    , sp_campaign VARCHAR(2000) CHARACTER SET Unicode NOT CaseSpecific --  sp_campaign VARCHAR(200) max 1448
    , utm_campaign VARCHAR(2000) CHARACTER SET Unicode NOT CaseSpecific
    , utm_content VARCHAR(4000) CHARACTER SET Unicode NOT CaseSpecific -- utm_content VARCHAR(3000) max 3378
    , utm_source VARCHAR(200) CHARACTER SET Unicode NOT CaseSpecific
    , utm_term VARCHAR(300) CHARACTER SET Unicode NOT CaseSpecific
)
PRIMARY INDEX(order_number)
PARTITION BY RANGE_N(order_date_pacific BETWEEN DATE '2022-01-31' AND DATE '2025-12-31' EACH INTERVAL '1' DAY)
;

SET QUERY_BAND = NONE FOR SESSION;