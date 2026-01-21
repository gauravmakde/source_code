SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=ddl_dior_session_fact_11521_ACE_ENG;
     Task_Name=ddl_dior_session_fact;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_SESSIONS.DIOR_SESSION_FACT
Team/Owner: Data Foundations, Nate Eyre
Date Created/Modified: 1/31/2023         

Note:
DIOR SESSION FACT does common transformations and outputs basic metrics and dimensions at the session_id level

Included in first release:
- digital interactions and purchasing
- marketing infromation
- authentication information
- customer identifiers

*/

create multiset table {sessions_t2_schema}.dior_session_fact
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    activity_date_pacific          date
    , session_id                    VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , channelcountry                VARCHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC
    , channel                       VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC
    , experience                    VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC
    , mrkt_type                     VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , finance_rollup                VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , finance_detail                VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , recognized_flag               INTEGER
    , guest_flag                    INTEGER
    , authenticated_flag            INTEGER
    , shopper_id                    VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC
    , acp_id                        VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC
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
    , first_page_instance_id        VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC
    , last_page_instance_id         VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC
    , first_page_type               VARCHAR(16) CHARACTER SET UNICODE NOT CASESPECIFIC
    , last_page_type                VARCHAR(16) CHARACTER SET UNICODE NOT CASESPECIFIC
    , dw_batch_date                 DATE DEFAULT CURRENT_DATE NOT NULL
    , dw_sys_load_tmstp             TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    , active_session_flag           INTEGER
    , deterministic_bot_flag        INTEGER
    , sus_bot_flag                  INTEGER
    , bot_demand_flag               INTEGER
    )
primary index(session_id)
partition by range_n(activity_date_pacific BETWEEN DATE'2021-01-01' AND DATE'2031-12-31' EACH INTERVAL '1' DAY, NO RANGE)
;


-- Table Comment (STANDARD)
COMMENT ON  {sessions_t2_schema}.dior_session_fact IS 'For each session ID, get dimensions and metrics across the following domains: digital experience, marketing, account, customer identifiers';
-- Column comments (OPTIONAL)
COMMENT ON  {sessions_t2_schema}.dior_session_fact.activity_date_pacific IS 'The date when the session occurred, converted to Pacific time';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.session_id IS 'Identifier of session, unique to each session';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.channelcountry IS 'Country where session was initiated, US or CA';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.channel IS 'Banner for the session, either NORDSTROM or NORDSTROM_RACK';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.experience IS 'Platform for the sessionDESKTOP_WEB, MOBILE_WEB, or IOS_APP';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.mrkt_type IS 'Highest view of marketing hierarchy, whether the session was BASE (non marketing driven), or driven by PAID or UNPAID marketing. Derived from referrer and utm_channel in the url';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.finance_rollup IS 'More granular view of which marketing channel drove a customer to their digital experience with Nordstrom';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.finance_detail IS 'Most granular view of which marketing channel drove a customer to their digital experience with Nordstrom';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.recognized_flag IS 'Was the customer recognized at any point during their session';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.guest_flag IS 'Was the customer a guest at any point during their session';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.authenticated_flag IS 'Did the customer authenticate at any point during their session';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.shopper_id IS 'Last shopper_id of the session, assumed to be most likely to be authenticated shopper id if they authenticated';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.acp_id IS 'acp_id associated with sessions where available, dedupe logic looks for the last shopper id of the session associated with an acp_id';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.session_duration_seconds IS 'Difference between session start and end times';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.bounce_flag IS 'Whether the session bounced (only had one page view/interaction upon site arrival)';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.product_views IS 'Count of product page views';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.cart_adds IS 'Count of items added to cart';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.web_orders IS 'Count of orders placed, using web events';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.web_ordered_units IS 'Count of units from orders placed, using web events';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.web_demand_usd IS 'Sum of demand from orders placed, using web events. Denominated in US dollars';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.web_demand IS 'Sum of demand from orders placed, using web events. Using original currency';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.web_demand_currency_code IS 'Original currency code for demand from orders placed, using web events, to be used in conjunction with web_demand column';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.oms_orders IS 'Count of orders placed, using OMS (Order Management System)';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.oms_ordered_units IS 'Count of units from orders placed, using OMS (Order Management System)';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.oms_demand_usd IS 'Sum of demand from orders placed, using OMS (Order Management System). Denominated in US dollars';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.oms_demand IS 'Sum of demand from orders placed, using OMS (Order Management System). Using original currency';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.oms_demand_currency_code IS 'Original currency code for demand from orders placed, using OMS (Order Management System), to be used in conjunction with web_demand column';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.bopus_orders IS 'Count of orders placed including at least one BOPUS item';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.bopus_ordered_units IS 'Count of BOPUS units ordered';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.bopus_demand_usd IS 'Sum of demand from BOPUS units, denominated in USD';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.bopus_demand IS 'Sum of demand from BOPUS units, using original currency';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.bopus_demand_currency_code IS 'Original currency code for BOPUS demand';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.product_view_session IS 'Did the session have at least one product view';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.cart_add_session IS 'Did the session have at least one add-to-cart';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.web_order_session IS 'Did the session have at least one order captured by web events';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.oms_order_session IS 'Did the session have at least one order captured by OMS (Order Management System)';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.bopus_order_session IS 'Did the session have at least one order with at least one BOPUS item';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.visited_homepage_session IS 'Did the session visit the homepage at least once, NULL until logic is populated';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.visited_checkout_session IS 'Did the session visit the checkout page at least once, NULL until logic is populated';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.searched_session IS 'Did the session use the search functionality at least once, NULL until logic is populated';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.browsed_session IS 'Did the session use the browse functionality at least once, NULL until logic is populated';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.first_page_instance_id IS 'Instance id for the first page view, aids in joining to session event table';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.last_page_instance_id IS 'Instance id for the exit page view, aids in joining to session event table';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.first_page_type IS 'Page type for first page view of session, aka entry page';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.last_page_type IS 'Page type for last page view of the session, aka exit page';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.dw_batch_date IS 'Date that the record was added to the table';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.dw_sys_load_tmstp IS 'Timestamp that the record was added to the table';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.active_session_flag IS 'Session Activity Status';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.deterministic_bot_flag IS 'deterministic_bot_flag is obtained from CATB_flag, useragent_bot_flag, useragent_crawl_flag';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.sus_bot_flag IS 'sus_bot_flag is obtained using the old analytical bot logic to identify similar sessions as suspected bot';
COMMENT ON  {sessions_t2_schema}.dior_session_fact.bot_demand_flag IS 'bot_demand_flag is True when a bot session either deterministic_bot or sus_bot contains demand';

SET QUERY_BAND = NONE FOR SESSION;

