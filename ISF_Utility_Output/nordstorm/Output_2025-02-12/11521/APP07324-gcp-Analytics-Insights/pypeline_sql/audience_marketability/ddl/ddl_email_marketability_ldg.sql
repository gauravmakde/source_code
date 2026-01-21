/*
Table: T2DL_DAS_MKTG_AUDIENCE.email_marketability_ldg
Owner: Sophie Wang, Amanda Van Orsdale, Sushmitha Palleti, Nicole Miao
Modification Date: 2023-05-05
*/
SET QUERY_BAND = 'App_ID=APP08718;
DAG_ID=ddl_email_marketability_11521_ACE_ENG;
Task_Name=ddl_email_marketability;'
FOR SESSION VOLATILE;

--CALL SYS_MGMT.DROP_IF_EXISTS_SP('t2dl_das_bie_dev', 'email_marketability_ldg', OUT_RETURN_MSG);

create multiset table {mktg_audience_t2_schema}.email_marketability_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
              acp_id                 VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC
            , loyaltyid              VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC
            , iscardmember           INTEGER
            , nordylevel             VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC
            , channelcountry         VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
            , channel                VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC
            , platform               VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC
            , utm_medium             VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
            , finance_detail         VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
            , converted              VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
            , order_channelcountry   VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC
            , order_channel          VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC
            , order_platform         VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC
            , order_date_pacific     DATE FORMAT 'YYYY-MM-DD'
            , demand                 DECIMAL(38,2)
            , event_date_pacific     DATE FORMAT 'YYYY-MM-DD' NOT NULL
     )
primary index(acp_id, event_date_pacific, finance_detail)
partition by range_n(event_date_pacific BETWEEN DATE'2021-01-01' AND DATE'2031-12-31' EACH INTERVAL '1' DAY, NO RANGE)
;

SET QUERY_BAND = NONE FOR SESSION;
