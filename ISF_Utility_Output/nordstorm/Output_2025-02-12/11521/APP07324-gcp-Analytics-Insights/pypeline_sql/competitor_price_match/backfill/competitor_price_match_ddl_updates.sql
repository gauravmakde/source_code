-- DDL Updates to for problematic promo_name field - Historical data has higher length fields
-- So, bumping the size of the promo_name field in TD tables : ldg and final T2 tables

SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=competitor_price_matching_11521_ACE_ENG;
     Task_Name=competitor_price_match;'
     FOR SESSION VOLATILE;
 
ALTER TABLE {price_matching_t2_schema}.competitor_price_match_dump_ldg
ADD promo_name VARCHAR(3600) CHARACTER SET UNICODE NOT CASESPECIFIC ;

ALTER TABLE {price_matching_t2_schema}.competitor_price_match_dump
ADD promo_name VARCHAR(3600) CHARACTER SET UNICODE NOT CASESPECIFIC ;

SET QUERY_BAND = NONE FOR SESSION;