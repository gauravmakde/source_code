SET QUERY_BAND = 'App_ID=APP09442;
DAG_ID=ddl_nmn_head_brands_11521_ACE_ENG;
Task_Name=ddl_nmn_head_brands;' 
FOR SESSION VOLATILE;

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{nmn_t2_schema}', 'nmn_head_brands', OUT_RETURN_MSG);


CREATE MULTISET TABLE {nmn_t2_schema}.nmn_head_brands,
     FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      
      head_brands VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      brand_id INTEGER
      )
PRIMARY INDEX ( head_brands );

SET QUERY_BAND = NONE FOR SESSION;
