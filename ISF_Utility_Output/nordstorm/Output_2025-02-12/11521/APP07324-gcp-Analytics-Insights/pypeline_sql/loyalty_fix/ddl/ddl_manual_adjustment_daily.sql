SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=ddl_manual_adjustment_daily_11521_ACE_ENG;
     Task_Name=ddl_manual_adjustment_daily;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_LOYALTY_FIX.MANUAL_ADJUSTMENT_DAILY
Team/Owner: Andrew Porter or Katie Hains
Date Created/Modified: 2023-02-22
*/


CREATE MULTISET TABLE {loyalty_fix_t2_schema}.manual_adjustment_daily ,FALLBACK ,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      loyalty_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      business_day_date DATE FORMAT 'YYYY-MM-DD',
      sum_points FLOAT,
      enroll_country VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC,
      dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
     )
PRIMARY INDEX ( loyalty_id );
 
-- Table Comment
COMMENT ON  {loyalty_fix_t2_schema}.manual_adjustment_daily IS 'Loyalty points manual adjustment';

SET QUERY_BAND = NONE FOR SESSION;