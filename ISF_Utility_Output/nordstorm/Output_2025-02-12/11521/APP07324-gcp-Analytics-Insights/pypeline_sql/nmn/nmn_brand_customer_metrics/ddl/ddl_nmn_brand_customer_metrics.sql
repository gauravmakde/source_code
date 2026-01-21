SET QUERY_BAND = 'App_ID=APP09442;
DAG_ID=ddl_nmn_brand_customer_metrics_11521_ACE_ENG;
Task_Name=ddl_nmn_brand_customer_metrics;' 
FOR SESSION VOLATILE;


CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{nmn_t2_schema}', 'nmn_brand_customer_metrics', OUT_RETURN_MSG);

CREATE MULTISET TABLE {nmn_t2_schema}.nmn_brand_customer_metrics,
     FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
     brand VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,month_idnt INTEGER 
    ,brand_tier VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,active_customers INTEGER
    ,new_customers INTEGER
    ,retained_customers INTEGER
    ,reactivated_customers INTEGER
    ,lapsed_customers INTEGER
    ,dw_sys_load_tmstp  	TIMESTAMP(6) DEFAULT Current_Timestamp(6) NOT NULL
      )
PRIMARY INDEX ( brand, month_idnt, brand_tier);

SET QUERY_BAND = NONE FOR SESSION;

