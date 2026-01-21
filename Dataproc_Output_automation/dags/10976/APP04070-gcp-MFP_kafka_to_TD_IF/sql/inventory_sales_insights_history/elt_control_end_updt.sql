/*
 *   Close current batch
 */

SET QUERY_BAND = 'App_ID=app04070;DAG_ID=inventory_sales_insights_history_10976_tech_nap_merch;Task_Name=execute_elt_control_end_update;'
FOR SESSION VOLATILE;

ET;

UPDATE PRD_NAP_UTL.ELT_CONTROL
   SET BATCH_END_TMSTP = CURRENT_TIMESTAMP,
       ACTIVE_LOAD_IND = 'N'
 WHERE SUBJECT_AREA_NM = 'METRICS_DATA_SERVICE_HIST';

ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;
