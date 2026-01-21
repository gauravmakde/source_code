SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=dior_sessions_11521_ACE_ENG;
     Task_Name=ddl_dior_session_fact_daily_ldg;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_SESSIONS.DIOR_SESSION_FACT_DAILY_LDG
Team/Owner: Data Foundations, Nate Eyre
Date Created/Modified: 1/31/2023     
Note:
DIOR SESSION FACT does common transformations and outputs basic metrics and dimensions at the session_id level
this table rolls them up to a daily grain. 

Included in first release:
- digital interactions and purchasing
- marketing infromation
- authentication information
- customer identifiers

This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{sessions_t2_schema}', 'dior_session_fact_daily_ldg', OUT_RETURN_MSG);

create multiset table {sessions_t2_schema}.dior_session_fact_daily_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    activity_date_pacific          date
    , sessions                      INTEGER
    , channelcountry                VARCHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC
    , channel                       VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC
    , experience                    VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC
    , mrkt_type                     VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , finance_rollup                VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , finance_detail                VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , recognized_flag               INTEGER
    , guest_flag                    INTEGER
    , authenticated_flag            INTEGER
    , shopper_ids                   INTEGER
    , acp_ids                       INTEGER
    , session_duration_seconds      BIGINT
    , bounce_flag                   INTEGER
    , product_views                 INTEGER
    , cart_adds                     INTEGER
    , web_orders                    INTEGER
    , web_ordered_units             INTEGER
    , web_demand_usd                DECIMAL(15, 2)
    , web_demand                    DECIMAL(15, 2)
    , web_demand_currency_code      CHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC
    , oms_orders                    INTEGER
    , oms_ordered_units             DECIMAL(15, 2)
    , oms_demand_usd                DECIMAL(15, 2)
    , oms_demand                    DECIMAL(15, 2)
    , oms_demand_currency_code      CHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC
    , bopus_orders                  INTEGER
    , bopus_ordered_units           INTEGER
    , bopus_demand_usd              DECIMAL(15, 2)
    , bopus_demand                  DECIMAL(15, 2)
    , bopus_demand_currency_code    CHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC
    , product_view_session          INTEGER
    , cart_add_session              INTEGER
    , web_order_session             INTEGER
    , oms_order_session             INTEGER
    , bopus_order_session           INTEGER
    , visited_homepage_session      INTEGER
    , visited_checkout_session      INTEGER 
    , searched_session              INTEGER
    , browsed_session               INTEGER
    , first_page_type               VARCHAR(16) CHARACTER SET UNICODE NOT CASESPECIFIC
    , last_page_type                VARCHAR(16) CHARACTER SET UNICODE NOT CASESPECIFIC
    , dw_batch_date                 DATE NOT NULL
    , dw_sys_load_tmstp             TIMESTAMP NOT NULL
    , active_session_flag           INTEGER
    , deterministic_bot_flag        INTEGER
    , sus_bot_flag                  INTEGER
    , bot_demand_flag               INTEGER
    )
primary index(activity_date_pacific)
partition by range_n(activity_date_pacific BETWEEN DATE'2021-01-01' AND DATE'2031-12-31' EACH INTERVAL '1' DAY, NO RANGE)
;


SET QUERY_BAND = NONE FOR SESSION;




