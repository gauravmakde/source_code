SET QUERY_BAND = 'App_ID=APP09442;
DAG_ID=ddl_nmn_torso_brands_11521_ACE_ENG;
Task_Name=ddl_nmn_torso_brands;' 
FOR SESSION VOLATILE;

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{nmn_t2_schema}', 'nmn_torso_brands', OUT_RETURN_MSG);


CREATE MULTISET TABLE {nmn_t2_schema}.nmn_torso_brands,
     FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      
      torso_brands VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      brand_id INTEGER
      )
PRIMARY INDEX ( torso_brands );

SET QUERY_BAND = NONE FOR SESSION;
