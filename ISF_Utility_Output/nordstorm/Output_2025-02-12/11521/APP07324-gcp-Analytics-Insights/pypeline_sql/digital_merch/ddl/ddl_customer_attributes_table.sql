/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP09142;
     DAG_ID=ddl_customer_attributes_table_11521_ACE_ENG;
     Task_Name=ddl_customer_attributes_table;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: t2dl_das_digitalmerch_flash
Team/Owner: Nicole Miao, Rathin Deshpande, Rae Ann Boswell
Date Created/Modified: 08/19/2024
*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
-- CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'customer_attributes_table', OUT_RETURN_MSG);
create multiset table {digital_merch_t2_schema}.customer_attributes_table
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
        acp_id                        varchar(60) CHARACTER SET UNICODE NOT CASESPECIFIC  
        ,activity_date_pacific        date    
        ,session_id varchar(60) CHARACTER SET UNICODE NOT CASESPECIFIC
		,channel varchar(60) CHARACTER SET UNICODE NOT CASESPECIFIC
		,platform varchar(60) CHARACTER SET UNICODE NOT CASESPECIFIC
        ,loyalty_status               varchar(50) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,cardmember_flag              integer compress 
        ,member_flag                   integer compress
        ,engagement_cohort             varchar(30) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,billing_zipcode               varchar(10) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,us_dma_code                   integer compress
        ,dma_desc                      varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,CITY_NAME                     varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,STATE_NAME                    varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,predicted_ct_segment          varchar(15) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,clv_jwn                       decimal(14,4) compress 
        ,clv_op                        decimal(14,4) compress 
        ,clv_fp                        decimal(14,4) compress 
        ,aare_chnl_code                varchar(15) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,aare_status_date              date compress
        ,activated_chnl_code           varchar(15) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,activated_date                date compress
        ,ncom_first_digital_transaction       date compress
        ,ncom_second_digital_transaction      date compress
        ,ncom_last_digital_transaction          date compress
        ,rcom_first_digital_transaction         date compress
        ,rcom_second_digital_transaction        date compress
        ,rcom_last_digital_transaction          date compress
        ,first_digital_transaction              date compress
        ,second_digital_transaction             date compress
        ,last_digital_transaction               date compress
        ,pre_22_digital_order_count            integer compress
        ,pre_22_digital_demand                 decimal(32,4) compress 
        ,pre_22_digital_units                  integer compress
        ,pre_22_ncom_order_count               integer compress
        ,pre_22_ncom_demand                    decimal(32,4) compress 
        ,pre_22_ncom_units                     integer compress
        ,pre_22_rcom_order_count               integer compress
        ,pre_22_rcom_demand                    decimal(32,4) compress 
        ,pre_22_rcom_units                     integer compress
        ,age_group                              varchar(50) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,lifestage                              varchar(50) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        ,dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(acp_id, activity_date_pacific,session_id,channel,platform)
partition by range_n(activity_date_pacific BETWEEN DATE '2022-01-01' AND DATE '2055-12-31' EACH INTERVAL '1' DAY)
;

-- Table Comment (STANDARD)
COMMENT ON  {digital_merch_t2_schema}.customer_attributes_table IS 'A table containing all the customer attributes';


/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;

