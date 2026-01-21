/***********************************************************************************
-- DELETE failed records from staging table
************************************************************************************/
LOCK TABLE {db_env}_NAP_STG.STORE_INVENTORY_TRANSFER_EVENTS_LDG FOR ACCESS
DELETE FROM {db_env}_NAP_STG.STORE_INVENTORY_TRANSFER_EVENTS_LDG
WHERE (
  -- Primary key null check
  transfer_num is null
  or transfer_type is null
  or event_id is null
  or sku_num is null
  -- Primary key empty string check
  or transfer_num = ''
  or transfer_type = ''
  or event_id = ''
  or sku_num = ''
  -- Primary key empty quotes check
  or transfer_num = '""'
  or transfer_type = '""'
  or event_id = '""'
  or sku_num  = '""'
  -- Timestamp cast check
  or (event_tmstp is not null and TRYCAST(concat(event_tmstp, '+00:00') as timestamp with time zone) is null)
);

/***********************************************************************************
-- Merge into STORE_INVENTORY_TRANSFER_INITIAL_PLANNING_FACT fact table
************************************************************************************/
LOCK TABLE {db_env}_NAP_FCT.STORE_INVENTORY_TRANSFER_INITIAL_PLANNING_FACT FOR ACCESS
MERGE INTO {db_env}_NAP_FCT.STORE_INVENTORY_TRANSFER_INITIAL_PLANNING_FACT fact
USING (
SELECT
    coalesce(created.transfer_num, canceled.transfer_num) as transfer_num,
    coalesce(created.transfer_type, canceled.transfer_type) as transfer_type,
	created.event_id as created_event_id,
    canceled.event_id as canceled_event_id,
	CAST(concat(created.event_tmstp, '+00:00') as timestamp with time zone format 'YYYY-MM-DDBHH:MI:SS.S(F)Z') as created_event_tmstp,
	CAST(concat(canceled.event_tmstp, '+00:00') as timestamp with time zone format 'YYYY-MM-DDBHH:MI:SS.S(F)Z') as canceled_event_tmstp,
	coalesce(created.event_type, canceled.event_type) as latest_event_type,
    created.user_id as created_user_id,
	canceled.user_id as canceled_user_id,
    created.external_refence_num,
    created.system_id as created_system_id,
	canceled.system_id as canceled_system_id,
    coalesce(created.location_num, canceled.location_num) as location_num,
    created.to_location_num,
	created.to_logical_location_num,
    created.transfer_type_code,
    created.transfer_context_type,
    coalesce(created.sku_num, canceled.sku_num) as sku_num,
    coalesce(created.sku_type, canceled.sku_type) as sku_type,
    created.disposition_code as created_disposition_code, 
	canceled.disposition_code as canceled_disposition_code,
    created.sku_qty as created_sku_qty,
	canceled.sku_qty as canceled_sku_qty,
    (SELECT BATCH_ID FROM {db_env}_NAP_BASE_VWS.ELT_CONTROL WHERE SUBJECT_AREA_NM ='NAP_ASCP_STORE_INVENTORY_TRANSFER_INITIAL_PLANNING') as dw_batch_id,
    (SELECT CURR_BATCH_DATE FROM {db_env}_NAP_BASE_VWS.ELT_CONTROL WHERE SUBJECT_AREA_NM ='NAP_ASCP_STORE_INVENTORY_TRANSFER_INITIAL_PLANNING') as dw_batch_date
FROM (select * from {db_env}_NAP_BASE_VWS.STORE_INVENTORY_TRANSFER_EVENTS_LDG
      where event_type = 'STORE_TRANSFER_CREATED'
      QUALIFY ROW_NUMBER() OVER(PARTITION BY transfer_num, sku_num  ORDER BY event_tmstp desc) = 1
      ) created full join
	  (select * from {db_env}_NAP_BASE_VWS.STORE_INVENTORY_TRANSFER_EVENTS_LDG
      where event_type = 'STORE_TRANSFER_CANCELED'
      QUALIFY ROW_NUMBER() OVER(PARTITION BY transfer_num, sku_num  ORDER BY event_tmstp desc) = 1
      ) canceled ON created.transfer_num = canceled.transfer_num
                 AND created.sku_num = canceled.sku_num
) ldg
    ON fact.transfer_num = ldg.transfer_num
    AND fact.sku_num = ldg.sku_num
WHEN MATCHED THEN
UPDATE SET
    transfer_type = ldg.transfer_type,
	created_event_id = ldg.created_event_id,
	canceled_event_id = ldg.canceled_event_id,
	latest_event_type = ldg.latest_event_type,
    created_event_tmstp = ldg.created_event_tmstp,
	canceled_event_tmstp = ldg.canceled_event_tmstp,
    created_user_id = ldg.created_user_id,
	canceled_user_id = ldg.canceled_user_id,
    external_refence_num = ldg.external_refence_num,
    created_system_id = ldg.created_system_id,
	canceled_system_id = ldg.canceled_system_id,
    location_num = ldg.location_num,
    to_location_num = ldg.to_location_num,
    to_logical_location_num = ldg.to_logical_location_num,
    transfer_type_code = ldg.transfer_type_code,
    transfer_context_type = ldg.transfer_context_type,
    sku_type = ldg.sku_type,
    created_disposition_code = ldg.created_disposition_code,
	canceled_disposition_code = ldg.canceled_disposition_code,
    created_sku_qty = ldg.created_sku_qty,
	canceled_sku_qty = ldg.canceled_sku_qty,
    dw_batch_id = ldg.dw_batch_id,
    dw_batch_date = ldg.dw_batch_date,
    dw_sys_updt_tmstp = current_timestamp(0)
WHEN NOT MATCHED THEN
    INSERT (
        transfer_num,
        transfer_type,
		created_event_id,
		canceled_event_id,
        created_event_tmstp,
		canceled_event_tmstp,
        latest_event_type,
        created_user_id,
		canceled_user_id,
        external_refence_num,
        created_system_id,
		canceled_system_id,
        location_num,
        to_location_num,
        to_logical_location_num,
        transfer_type_code,
        transfer_context_type,
        sku_num,
        sku_type,
        created_disposition_code,
		canceled_disposition_code,
        created_sku_qty,
		canceled_sku_qty,
        dw_batch_id,
        dw_batch_date,
        dw_sys_load_tmstp,
        dw_sys_updt_tmstp
    )
    VALUES (
	    ldg.transfer_num,
        ldg.transfer_type,
		ldg.created_event_id,
		ldg.canceled_event_id,
        ldg.created_event_tmstp,
		ldg.canceled_event_tmstp,
        ldg.latest_event_type,
        ldg.created_user_id,
		ldg.canceled_user_id,
        ldg.external_refence_num,
        ldg.created_system_id,
		ldg.canceled_system_id,
        ldg.location_num,
        ldg.to_location_num,
        ldg.to_logical_location_num,
        ldg.transfer_type_code,
        ldg.transfer_context_type,
        ldg.sku_num,
        ldg.sku_type,
        ldg.created_disposition_code,
		ldg.canceled_disposition_code,
        ldg.created_sku_qty,
		ldg.canceled_sku_qty,
        ldg.dw_batch_id,
        ldg.dw_batch_date,
        current_timestamp(0),
        current_timestamp(0)
    );

ET;

/***********************************************************************************
-- Collect stats on fact tables
************************************************************************************/
COLLECT STATS ON {db_env}_NAP_FCT.STORE_INVENTORY_TRANSFER_INITIAL_PLANNING_FACT
INDEX (transfer_num, sku_num);

ET;

/***********************************************************************************
-- Perform audit between stg and fct tables
************************************************************************************/
CALL {db_env}_NAP_UTL.DQ_PIPELINE_AUDIT_V1
(
  'STORE_INVENTORY_TRANSFER_INITIAL_PLANNING_TO_BASE',
  'NAP_ASCP_STORE_INVENTORY_TRANSFER_INITIAL_PLANNING',
  '{db_env}_NAP_BASE_VWS',
  'STORE_INVENTORY_TRANSFER_EVENTS_LDG',
  '{db_env}_NAP_FCT',
  'STORE_INVENTORY_TRANSFER_INITIAL_PLANNING_FACT',
  'Count_Distinct',
  0,
  'T-S',
  'concat(transfer_num, ''-'', sku_num)',
  'concat(transfer_num, ''-'', sku_num)',
  'event_type in (''STORE_TRANSFER_CREATED'', ''STORE_TRANSFER_CANCELED'')',
  null,
  'N'
);

ET;
