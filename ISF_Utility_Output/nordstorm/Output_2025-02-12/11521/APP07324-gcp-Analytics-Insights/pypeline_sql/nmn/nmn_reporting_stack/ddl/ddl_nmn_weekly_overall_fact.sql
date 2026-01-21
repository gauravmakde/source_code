SET QUERY_BAND = 'App_ID=APP08172;
     DAG_ID=ddl_nmn_weekly_overall_fact_11521_ACE_ENG;
     Task_Name=ddl_nmn_weekly_overall_fact;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2dl_DAS_NMN.nmn_weekly_overall_fact
Team/Owner: NMN
Date Created/Modified:

Note:
-- To Support the NMN Report view 12
-- Weekly Refresh

*/

CREATE multiset TABLE {nmn_t2_schema}.nmn_weekly_overall_fact ,FALLBACK ,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1,
     LOG
 (
      channel_num INTEGER,
      channel_country VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      week_idnt integer,
      last_year_week_idnt integer,
      month_idnt  integer,
      quarter_idnt  integer,
      year_idnt integer,
      div_desc VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC,
      div_num INTEGER,
      grp_desc VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC,
      grp_num INTEGER,
      dept_desc VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC,
      dept_num INTEGER,
      class_desc VARCHAR(73) CHARACTER SET UNICODE NOT CASESPECIFIC,
      class_num INTEGER,
      brand_name VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      npg_flag CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      supplier_name VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC,
      sales_dollars DECIMAL(38,2),
      sales_units DECIMAL(38,0),
      return_dollars DECIMAL(38,2),
      return_units DECIMAL(38,0),
      receipt_units DECIMAL(38,0),
      product_views DECIMAL(38,2),
      order_quantity INTEGER,
      eoh_units DECIMAL(38,0),
      dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL)
PRIMARY INDEX ( channel_num ,week_idnt ,channel_country ,div_desc ,
brand_name ,grp_desc ,dept_desc ,class_desc ,supplier_name )
PARTITION BY RANGE_N(week_idnt  BETWEEN 202101  AND 202153  EACH 1 ,
202201  AND 202253  EACH 1 ,
202301  AND 202353  EACH 1 ,
202401  AND 202453  EACH 1 ,
202501  AND 202553  EACH 1);

COMMENT ON  {nmn_t2_schema}.nmn_weekly_overall_fact IS 'Table to refresh NMN Reporting Stack Dashboard';
COMMENT ON  {nmn_t2_schema}.nmn_weekly_overall_fact.channel_num IS 'Channel Number to Indentify the category';
SET QUERY_BAND = NONE FOR SESSION;
