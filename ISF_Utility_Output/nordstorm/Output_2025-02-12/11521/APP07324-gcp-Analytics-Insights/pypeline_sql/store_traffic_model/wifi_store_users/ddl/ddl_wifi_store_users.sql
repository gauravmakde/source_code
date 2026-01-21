SET QUERY_BAND = 'App_ID=APP08158;
     DAG_ID=ddl_wifi_store_users_11521_ACE_ENG;
     Task_Name=ddl_wifi_store_users;'
     FOR SESSION VOLATILE;

/* 
Table definition for WIFI Store Users
t2dl_das_fls_traffic_model.wifi_store_users
*/

CREATE MULTISET TABLE {fls_traffic_model_t2_schema}.wifi_store_users
     , FALLBACK
     , NO BEFORE JOURNAL
     , NO AFTER JOURNAL
     , CHECKSUM = DEFAULT
     , DEFAULT MERGEBLOCKRATIO
 (
     store              CHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , business_date     DATE NOT NULL
    , wifi_users        DECIMAL(12,0) NOT NULL DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , wifi_source       VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('aruba_cleaned_wifi', 'aruba_raw_wifi', 'nsg_cleaned_wifi', 'nsg_raw_wifi', 'retailnext')
    , dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
 )
 PRIMARY INDEX (business_date)
 PARTITION BY RANGE_N(business_date BETWEEN DATE '2019-01-01' AND DATE '2040-01-01' EACH INTERVAL '1' DAY)
 ;
 
SET QUERY_BAND = NONE FOR SESSION;