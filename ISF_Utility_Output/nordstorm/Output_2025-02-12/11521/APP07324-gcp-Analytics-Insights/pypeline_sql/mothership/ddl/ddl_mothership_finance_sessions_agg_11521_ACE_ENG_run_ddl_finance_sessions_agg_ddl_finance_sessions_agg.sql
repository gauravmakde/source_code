SET QUERY_BAND = 'App_ID=APP08176;
     DAG_ID=ddl_mothership_finance_sessions_agg_11521_ACE_ENG;
     Task_Name=ddl_finance_finance_sessions_agg;'
     FOR SESSION VOLATILE;

/*
T2DL_DAS_MOTHERSHIP.FINANCE_SESSIONS_AGG
Description - This ddl creates a table of sessions split by platform (device), and customer categories
Full documenation: https://confluence.nordstrom.com/x/M4fITg
Contacts: Matthew Bond, Analytics
*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
-- CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'FINANCE_SESSIONS_AGG', OUT_RETURN_MSG);

 CREATE MULTISET TABLE T2DL_DAS_MOTHERSHIP.FINANCE_SESSIONS_AGG,
     FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
        activity_date                                       DATE FORMAT'YYYY-MM-DD' NOT NULL,
        business_unit_desc                                  VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
        platform_code                                       VARCHAR(30) CHARACTER SET UNICODE DEFAULT NULL,
        loyalty_status                                      VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC DEFAULT 'NOT_APPLICABLE' COMPRESS ('MEMBER','INSIDER','AMBASSADOR','INFLUENCER','ICON','UNKNOWN_VALUE','NOT_APPLICABLE'),
        buyerflow_code                                      VARCHAR(27) CHARACTER SET UNICODE compress ('1) New-to-JWN', '2) New-to-Channel (not JWN)', '3) Retained-to-Channel', '4) Reactivated-to-Channel'),
        AARE_acquired_ind                                   CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC DEFAULT NULL COMPRESS ('N','Y'),
        AARE_activated_ind                                  CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC DEFAULT NULL COMPRESS ('N','Y'),
        AARE_retained_ind                                   CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC DEFAULT NULL COMPRESS ('N','Y'),
        AARE_engaged_ind                                    CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC DEFAULT NULL COMPRESS ('N','Y'),
        engagement_cohort                                   VARCHAR(30) CHARACTER SET UNICODE compress ('Acquire & Activate', 'Acquired Mid-Qtr', 'Highly-Engaged', 'Lightly-Engaged', 'Moderately-Engaged'),
        mrtk_chnl_type_code                                 VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('BASE', 'PAID', 'UNATTRIBUTED', 'UNPAID'),
        mrtk_chnl_finance_rollup_code                       VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('AFFILIATES', 'BASE', 'DISPLAY', 'EMAIL', 'NOT_APPLICABLE', 'PAID_OTHER', 'PAID_SEARCH', 'SEO', 'SHOPPING', 'SOCIAL', 'UNATTRIBUTED', 'UNPAID_OTHER'),
        mrtk_chnl_finance_detail_code                       VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
        sessions                                            INTEGER DEFAULT 0  COMPRESS (0 ,1 ),
        viewing_sessions                                    INTEGER DEFAULT 0  COMPRESS (0 ,1 ),
        adding_sessions                                     INTEGER DEFAULT 0  COMPRESS (0 ,1 ),
        checkout_sessions                                   INTEGER DEFAULT 0  COMPRESS (0 ,1 ),
        ordering_sessions                                   INTEGER DEFAULT 0  COMPRESS (0 ,1 ),
        acp_count                                           INTEGER DEFAULT 0  COMPRESS (0 ,1 ),
        unique_cust_count                                   INTEGER DEFAULT 0  COMPRESS (0 ,1 ),
        total_pages_visited                                 INTEGER DEFAULT 0  COMPRESS (0 ,1 ),
        total_unique_pages_visited                          INTEGER DEFAULT 0  COMPRESS (0 ,1 ),
        bounced_sessions                                    INTEGER DEFAULT 0  COMPRESS (0 ,1 ),
        browse_sessions                                     INTEGER DEFAULT 0  COMPRESS (0 ,1 ),
        search_sessions                                     INTEGER DEFAULT 0  COMPRESS (0 ,1 ),
        wishlist_sessions                                   INTEGER DEFAULT 0  COMPRESS (0 ,1 ),
		    dw_sys_load_tmstp                                   TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
     )
     PRIMARY INDEX (activity_date,business_unit_desc,platform_code,mrtk_chnl_finance_detail_code)
     PARTITION BY RANGE_N(activity_date BETWEEN DATE '2021-01-31' AND DATE '2025-12-31' EACH INTERVAL '1' DAY, UNKNOWN);

SET QUERY_BAND = NONE FOR SESSION;