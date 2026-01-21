SET QUERY_BAND = 'App_ID=APP08300;
     DAG_ID=ddl_mta_email_performance_11521_ACE_ENG;
     Task_Name=mta_email_performance;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: t2dl_das_mta.mta_email_performance
Team/Owner: MTA and MOA
Date Created/Modified: 6/2023

Note:
-- This table joins SalesForce email engagement data and MTA transactional data for the Email Performance Dashboard
-- The data rruns daily for all data existing since May 2022

*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.

--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'mta_email_performance', OUT_RETURN_MSG);



create multiset table {mta_t2_schema}.mta_email_performance
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
campaign_id_version             varchar(30) CHARACTER SET UNICODE NOT CASESPECIFIC 
,job_id                         varchar(30) CHARACTER SET UNICODE NOT CASESPECIFIC
,deploy_date                    date
,box_type                       varchar(30) CHARACTER SET UNICODE NOT CASESPECIFIC compress
,program_type                   varchar(30) CHARACTER SET UNICODE NOT CASESPECIFIC compress
,email_type                     varchar(30) CHARACTER SET UNICODE NOT CASESPECIFIC compress
,campaign_name                  varchar(2000) CHARACTER SET UNICODE NOT CASESPECIFIC compress
,arrived_channel                varchar(30) CHARACTER SET UNICODE NOT CASESPECIFIC compress
,campaign_category              varchar(500) CHARACTER SET UNICODE NOT CASESPECIFIC compress
,Measurement_days               smallint 
,impacted_flag                  smallint
,subject_line                   varchar(300) CHARACTER SET UNICODE NOT CASESPECIFIC 
,total_sent_count               integer
,total_delivered_count          integer 
,total_clicked_count            integer
,total_opened_count             integer
,total_bounced_count            integer
,total_unsubscribed_count       integer
,unique_clicked_count           integer
,unique_opened_count            integer
,unique_unsubscribed_count      integer
,gross_sales                    float 
,net_sales                      float
,orders                         float 
,units                          float
,"sessions"                     float
,session_orders                 float
,session_demand                 float
,bounced_sessions               float
,dw_sys_load_tmstp              TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(campaign_id_version, job_id, deploy_date, subject_line, measurement_days)
partition by RANGE_N(deploy_date BETWEEN DATE '2017-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY)
;

-- Table Comment (STANDARD)
COMMENT ON  {mta_t2_schema}.mta_email_performance IS 'MTA and Sendlog data for email performance dashboard';

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;



