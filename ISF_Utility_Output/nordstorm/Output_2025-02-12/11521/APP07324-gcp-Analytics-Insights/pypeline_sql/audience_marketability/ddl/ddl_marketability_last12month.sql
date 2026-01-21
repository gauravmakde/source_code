
/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08805;
     DAG_ID=ddl_marketability_last12month_11521_ACE_ENG;
     Task_Name=ddl_marketability_last12month;'
     FOR SESSION VOLATILE;



/*
T2/Table Name: T2DL_DAS_MKTG_AUDIENCE.CUSTOMER_MARKETABILITY_LTM
Team/Owner: Customer Analytics/Nicole Miao
Date Created/Modified: Dec 7, 2023



Note:
-- What is the the purpose of the table: The purpose of this table is to create an one-stop view, containing marketable/reachable information for four marketing channels (Paid, Email, DM, and Site), as well as customer attributes on the ACP_ID level. 
-- What is the update cadence/lookback window: Monthly update, 12 months look back window
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{mktg_audience_t2_schema}', 'CUSTOMER_MARKETABILITY_LTM', OUT_RETURN_MSG);
create multiset table {mktg_audience_t2_schema}.CUSTOMER_MARKETABILITY_LTM
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
     acp_id               varchar(50)
    ,l1y_purchase_idnt    integer
    ,l2y_purchase_idnt    integer
    ,l4y_purchase_idnt    integer
    ,directmail_idnt      integer
    ,site_visited_channel varchar(100)
    ,site_visited_experience varchar(100)
    ,site_visited_sessions integer
    ,site_visited_pageviews integer
    ,site_visited_atb       integer
    ,site_converted_orders  integer
    ,site_converted_demand  decimal(12,2) 
    ,paid_idnt            integer
    ,paid_marketable_idnt integer
    ,email_idnt           integer
    ,op_email_marketable_idnt  integer
    ,fp_email_marketable_idnt  integer
    ,email_reached_idnt   integer
    ,email_purchased_idnt integer
    ,is_loyalty_member    integer
    ,is_cardmember        integer
    ,loyalty_status       varchar(20)
    ,member_enroll_country_code  varchar(4)
    ,loyalty_level          varchar(20)
    ,anniversary_flag       varchar(40)
    ,engagement_cohort      varchar(40)
    ,historic_shopping_channel varchar(40)
    ,aare                   varchar(40)
    ,ltm_gross_spend        decimal(20,2)
    ,ltm_total_trips        integer
    ,lr_custom_column1      integer
    ,lr_custom_column2      integer
    ,lr_custom_column3      integer
    , dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(acp_id)
;

-- Table Comment (STANDARD)
COMMENT ON {mktg_audience_t2_schema}.CUSTOMER_MARKETABILITY_LTM IS 'Marketability by different cuts on customer level';


/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;

