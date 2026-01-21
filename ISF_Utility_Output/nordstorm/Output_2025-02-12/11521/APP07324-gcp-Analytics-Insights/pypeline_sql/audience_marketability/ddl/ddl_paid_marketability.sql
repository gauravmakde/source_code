/*
Table: T2DL_DAS_MKTG_AUDIENCE.paid_marketability
Owner: Sophie Wang, Amanda Van Orsdale, Sushmitha Palleti, Nicole Miao
Modification Date: 2023-05-0
Description: This table attributes daily touches to their respective conversion orders and oder details at an ACP ID level, this table will then be used to determine the % cuastomer from this data set as marketable through paid Platforms
*/
SET QUERY_BAND = 'App_ID=APP08718;
DAG_ID=ddl_paid_marketability_11521_ACE_ENG;
Task_Name=ddl_paid_marketability;'
FOR SESSION VOLATILE;


--CALL SYS_MGMT.DROP_IF_EXISTS_SP('t2dl_das_bie_dev', 'paid_marketability', OUT_RETURN_MSG);

create multiset table {mktg_audience_t2_schema}.paid_marketability
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
       , utm_source             VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
       , finance_detail         VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
       , converted              VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
       , order_channelcountry   VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC
       , order_channel          VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC
       , order_platform         VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC
       , order_date_pacific     DATE FORMAT 'YYYY-MM-DD'
       , demand                 DECIMAL(38,2)
       , event_date_pacific     DATE FORMAT 'YYYY-MM-DD' NOT NULL
       , dw_sys_load_tmstp      TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
       )
primary index(acp_id, event_date_pacific, finance_detail)
partition by range_n(event_date_pacific BETWEEN DATE'2021-01-01' AND DATE'2031-12-31' EACH INTERVAL '1' DAY, NO RANGE)
;

SET QUERY_BAND = NONE FOR SESSION;
