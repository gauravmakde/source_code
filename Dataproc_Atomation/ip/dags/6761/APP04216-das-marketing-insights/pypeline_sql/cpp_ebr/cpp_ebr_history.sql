SET QUERY_BAND = '
App_ID=app04216;
DAG_ID=event_producer_schema_v2_nonprod;
Task_Name=write_to_insight_event_stg_0;
LoginUser=SCH_NAP_MKTG_BATCH_DEV;
Job_Name=event_producer_schema_v2_prod;
Data_Plane=DAS_MARKETING;
Team_Email=yu-lin.chou@nordstrom.com;
PagerDuty=nap-marketing;
Conn_Type=JDBC;'
FOR SESSION VOLATILE;
ET;

DELETE FROM {teradata_env}_NAP_BASE_VWS.CPP_EBR_TRAN_HIST_FACT ALL;
ET;

--REFRESH THE HIST TABLE WITH TODAY'S DATA 
INSERT INTO {teradata_env}_NAP_BASE_VWS.CPP_EBR_TRAN_HIST_FACT
SELECT 
INSIGHT_ID,
LAST_PURCHASE_DATE,
DW_SYS_UPDT_TMSTP
FROM {teradata_env}_NAP_BASE_VWS.CPP_EBR_TRAN_FACT;
ET;

--COLLECT STATISTICS COLUMN(INSIGHT_ID) ON {teradata_env}_NAP_BASE_VWS.CPP_EBR_TRAN_HIST_FACT;
SET QUERY_BAND = NONE FOR SESSION;
ET;
