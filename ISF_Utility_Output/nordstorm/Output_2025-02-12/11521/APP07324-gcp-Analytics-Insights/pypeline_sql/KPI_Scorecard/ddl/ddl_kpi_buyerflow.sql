SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=ddl_kpi_buyerflow_11521_ACE_ENG;
     Task_Name=ddl_kpi_buyerflow;'
     FOR SESSION VOLATILE;



--DROP TABLE {kpi_scorecard_t2_schema}.kpi_buyerflow ;

CREATE MULTISET TABLE {kpi_scorecard_t2_schema}.kpi_buyerflow ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      report_period VARCHAR(18) CHARACTER SET UNICODE NOT CASESPECIFIC,
	  report_year VARCHAR(18) CHARACTER SET UNICODE NOT CASESPECIFIC,
      report_month VARCHAR(18) CHARACTER SET UNICODE NOT CASESPECIFIC,
      channel VARCHAR(18) CHARACTER SET UNICODE NOT CASESPECIFIC,
      country VARCHAR(7) CHARACTER SET UNICODE NOT CASESPECIFIC,
	  total_customers INTEGER,
      LY_total_customers INTEGER,
	  total_trips INTEGER,
	  LY_total_trips INTEGER,
	  retained INTEGER,
	  LY_retained INTEGER,
	  retained_trips INTEGER,
	  LY_retained_trips INTEGER,
	  acquired INTEGER,
	  LY_acquired INTEGER,
	  new_trips INTEGER,
	  LY_new_trips INTEGER,
	  reactivated INTEGER,
	  LY_reactivated INTEGER,
	  reactivated_trips INTEGER,
	  LY_reactivated_trips INTEGER,
	  activated INTEGER,
	  LY_activated INTEGER,
	  activated_trips INTEGER,
	  LY_activated_trips INTEGER,
	  engaged INTEGER,
	  LY_engaged INTEGER,
	  engaged_trips INTEGER,
	  LY_engaged_trips INTEGER,
	  total_net_spend INTEGER,
	  LY_total_net_spend INTEGER,
	  retained_net_spend INTEGER,
	  LY_retained_net_spend INTEGER,
	  new_net_spend INTEGER,
	  LY_new_net_spend INTEGER,
	  reactivated_net_spend INTEGER,
	  LY_reactivated_net_spend INTEGER,      
	 dw_sys_load_tmstp TIMESTAMP(6) DEFAULT Current_Timestamp(6) NOT NULL
     )
PRIMARY INDEX ( report_period );

-- Table Comment (STANDARD)
COMMENT ON  {kpi_scorecard_t2_schema}.kpi_buyerflow IS 'KPI BUYERFLOW CODE FOR CUSTOMERS DATA';

SET QUERY_BAND = NONE FOR SESSION;

