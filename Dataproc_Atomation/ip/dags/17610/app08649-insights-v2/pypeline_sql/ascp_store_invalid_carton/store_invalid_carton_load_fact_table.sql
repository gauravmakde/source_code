/***********************************************************************************
--DELETE OLD EVENTS
************************************************************************************/
LOCK TABLE {db_env}_NAP_FCT.STORE_INVALID_CARTON_FACT FOR ACCESS
DELETE FROM {db_env}_NAP_FCT.STORE_INVALID_CARTON_FACT fact
WHERE fact.carton_num = {db_env}_NAP_STG.STORE_INVALID_CARTON_LDG.carton_num
      and fact.location_num = {db_env}_NAP_STG.STORE_INVALID_CARTON_LDG.location_num
	  and fact.carton_sts = {db_env}_NAP_STG.STORE_INVALID_CARTON_LDG.carton_sts
	  and fact.user_id = {db_env}_NAP_STG.STORE_INVALID_CARTON_LDG.user_id;

/***********************************************************************************
-- INSERT into STORE_INVALID_CARTON_FACT fact table
************************************************************************************/
LOCK TABLE {db_env}_NAP_FCT.STORE_INVALID_CARTON_FACT FOR ACCESS
INSERT INTO {db_env}_NAP_FCT.STORE_INVALID_CARTON_FACT
(
        carton_num,
        object_created_sys_tmstp,
        last_triggered_event_tmstp,
        last_triggered_event_type,
        event_id,
        event_tmstp,
        location_num,
        carton_sts,
        user_id,
        user_type,
        dw_batch_id,
        dw_batch_date,
        dw_sys_load_tmstp,
        dw_sys_updt_tmstp
)
SELECT 
  carton_num,
  CAST(concat(object_created_sys_tmstp, '+00:00') as timestamp with time zone format 'YYYY-MM-DDBHH:MI:SS.S(F)Z') as object_created_sys_tmstp,
  CAST(concat(last_triggered_event_tmstp, '+00:00') as timestamp with time zone format 'YYYY-MM-DDBHH:MI:SS.S(F)Z') as last_triggered_event_tmstp,
  last_triggered_event_type,
  event_id,
  CAST(concat(event_tmstp, '+00:00') as timestamp with time zone format 'YYYY-MM-DDBHH:MI:SS.S(F)Z') as event_tmstp,
  location_num,
  carton_sts,
  user_id,
  user_type,
  (SELECT BATCH_ID FROM {db_env}_NAP_BASE_VWS.ELT_CONTROL WHERE SUBJECT_AREA_NM ='NAP_ASCP_STORE_INVALID_CARTON') as dw_batch_id,
  (SELECT CURR_BATCH_DATE FROM {db_env}_NAP_BASE_VWS.ELT_CONTROL WHERE SUBJECT_AREA_NM ='NAP_ASCP_STORE_INVALID_CARTON') as dw_batch_date,
  current_timestamp as dw_sys_load_tmstp,
  current_timestamp as dw_sys_updt_tmstp
FROM {db_env}_NAP_STG.STORE_INVALID_CARTON_LDG
QUALIFY ROW_NUMBER() OVER(PARTITION BY carton_num, event_id  ORDER BY event_tmstp desc) = 1;

ET;

/***********************************************************************************
-- Collect stats on fact tables
************************************************************************************/
COLLECT STATS ON {db_env}_NAP_FCT.STORE_INVALID_CARTON_FACT
INDEX (carton_num, carton_sts, location_num);

ET;

/***********************************************************************************
-- Perform audit between stg and fct tables
************************************************************************************/
CALL {db_env}_NAP_UTL.DQ_PIPELINE_AUDIT_V1
(
  'STORE_INVALID_CARTON_TO_BASE',
  'NAP_ASCP_STORE_INVALID_CARTON',
  '{db_env}_NAP_STG',
  'STORE_INVALID_CARTON_LDG',
  '{db_env}_NAP_FCT',
  'STORE_INVALID_CARTON_FACT',
  'Count_Distinct',
  0,
  'T-S',
  'concat(carton_num, ''-'', event_id)',
  'concat(carton_num, ''-'', event_id)',
  null,
  null,
  'N'
);
ET;
