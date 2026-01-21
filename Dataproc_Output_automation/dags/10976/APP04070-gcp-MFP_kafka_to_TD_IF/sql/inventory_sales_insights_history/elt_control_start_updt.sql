/*
 *   Open current batch
 */

SET QUERY_BAND = 'App_ID=app04070;DAG_ID=inventory_sales_insights_history_10976_tech_nap_merch;Task_Name=execute_elt_control_start_update;'
FOR SESSION VOLATILE;

ET;

UPDATE PRD_NAP_UTL.ELT_CONTROL
   SET BATCH_ID           = BATCH_ID + 1,
       CURR_BATCH_DATE    = CURRENT_DATE,
       BATCH_START_TMSTP  = CURRENT_TIMESTAMP,
       EXTRACT_FROM_TMSTP = CURRENT_DATE - 1,
       EXTRACT_TO_TMSTP   = CURRENT_DATE - 1,
       ACTIVE_LOAD_IND    = 'Y'
 WHERE SUBJECT_AREA_NM = 'METRICS_DATA_SERVICE_HIST'
;

ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;
