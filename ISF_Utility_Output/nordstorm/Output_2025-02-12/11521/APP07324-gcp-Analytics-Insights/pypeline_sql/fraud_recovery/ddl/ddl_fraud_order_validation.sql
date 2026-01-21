SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=ddl_fraud_order_validation_11521_ACE_ENG;
     Task_Name=ddl_fraud_order_validation;'
     FOR SESSION VOLATILE;

/*t2dl_das_fraud_recovery.fraud_order_validation*/

create multiset table {fraud_recovery_t2_schema}.fraud_order_validation ,fallback ,
     no before journal,
     no after journal,
     checksum = default,
     default mergeblockratio,
     map = td_map1
     (
      brand_name varchar(6) character set unicode not casespecific,
      shopper_id varchar(50) character set unicode not casespecific,
      order_num varchar(40) character set unicode not casespecific,
      order_date_pacific date format 'yyyy-mm-dd',
      amount decimal(12,2),
      report_date date format 'yyyy-mm-dd',
      report_tmstp timestamp(6),
      dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
      )
PRIMARY INDEX ( shopper_id )
PARTITION BY RANGE_N(report_date  BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY ,
 NO RANGE);
 

SET QUERY_BAND = NONE FOR SESSION;