SET QUERY_BAND = 'App_ID=APP08158;
     DAG_ID=wifi_store_users_11521_ACE_ENG;
     Task_Name=ddl_wifi_store_users_ldg;'
     FOR SESSION VOLATILE;

/*
LANDING TABLE 
Table definition for WIFI Store Users
t2dl_das_fls_traffic_model.wifi_store_users
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{fls_traffic_model_t2_schema}', 'wifi_store_users_ldg', OUT_RETURN_MSG);

CREATE MULTISET TABLE {fls_traffic_model_t2_schema}.wifi_store_users_ldg
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
 )
 PRIMARY INDEX (business_date)
 PARTITION BY RANGE_N(business_date BETWEEN DATE '2019-01-01' AND DATE '2040-01-01' EACH INTERVAL '1' DAY)
 ;
 
SET QUERY_BAND = NONE FOR SESSION;