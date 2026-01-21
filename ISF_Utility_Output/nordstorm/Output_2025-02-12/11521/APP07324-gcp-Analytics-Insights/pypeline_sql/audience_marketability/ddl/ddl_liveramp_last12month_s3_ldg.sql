/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08805;
     DAG_ID=ddl_liveramp_last12month_11521_ACE_ENG;
     Task_Name=ddl_liveramp_last12month_s3_ldg;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_MKTG_AUDIENCE.liverampe_table_last12month
Team/Owner: Nicole Miao
Date Created/Modified: Dec 6, 2023

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.
*/



CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{mktg_audience_t2_schema}', 'liveramp_table_last12month_ldg', OUT_RETURN_MSG);

create multiset table {mktg_audience_t2_schema}.liveramp_table_last12month_ldg
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
    ,op_email_marketable_idnt integer
    ,fp_email_marketable_idnt integer
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
    )
primary index(acp_id)
;





/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;

