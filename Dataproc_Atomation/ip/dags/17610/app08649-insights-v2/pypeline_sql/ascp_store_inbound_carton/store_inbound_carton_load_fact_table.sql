
/***********************************************************************************
-- Merge into STORE_INBOUND_CARTON_EVENTS_FACT fact table
************************************************************************************/
LOCK TABLE {db_env}_NAP_FCT.STORE_INBOUND_CARTON_EVENTS_FACT FOR ACCESS
MERGE INTO {db_env}_NAP_FCT.STORE_INBOUND_CARTON_EVENTS_FACT fact
USING (
SELECT
    purchase_order_num,
    carton_num,
	vendor_carton_num,
	advance_shipment_num,
    location_num,
    event_id,
	CAST(concat(event_tmstp, '+00:00') as timestamp with time zone format 'YYYY-MM-DDBHH:MI:SS.S(F)Z') as event_tmstp,
	CAST(SUBSTRING(event_tmstp,1, 10) as DATE ) as event_date,
	event_type,
    sku_num,
    sku_type,
    disposition_code,
    sku_qty,
    (SELECT BATCH_ID FROM {db_env}_NAP_BASE_VWS.ELT_CONTROL WHERE SUBJECT_AREA_NM ='NAP_ASCP_STORE_INBOUND_CARTON') as dw_batch_id,
    (SELECT CURR_BATCH_DATE FROM {db_env}_NAP_BASE_VWS.ELT_CONTROL WHERE SUBJECT_AREA_NM ='NAP_ASCP_STORE_INBOUND_CARTON') as dw_batch_date
FROM {db_env}_NAP_STG.STORE_INBOUND_CARTON_EVENTS_LDG
QUALIFY ROW_NUMBER() OVER(PARTITION BY purchase_order_num, carton_num, event_id, sku_num  ORDER BY event_tmstp desc) = 1
) ldg
    ON fact.purchase_order_num = ldg.purchase_order_num
	AND fact.carton_num = ldg.carton_num
	AND fact.event_id = ldg.event_id
    AND fact.sku_num = ldg.sku_num
	AND fact.event_date = ldg.event_date
WHEN MATCHED THEN
UPDATE SET
    advance_shipment_num = ldg.advance_shipment_num,
	vendor_carton_num = ldg.vendor_carton_num,
    location_num = ldg.location_num,
    event_tmstp = ldg.event_tmstp,
	event_type = ldg.event_type,
    sku_type = ldg.sku_type,
    disposition_code = ldg.disposition_code,
    sku_qty = ldg.sku_qty,
    dw_batch_id = ldg.dw_batch_id,
    dw_batch_date = ldg.dw_batch_date,
    dw_sys_updt_tmstp = current_timestamp
WHEN NOT MATCHED THEN
    INSERT (
        purchase_order_num,
        carton_num,
		vendor_carton_num,
		advance_shipment_num,
        location_num,
        event_id,
        event_tmstp,
		event_date,
		event_type,
        sku_num,
        sku_type,
        disposition_code,
        sku_qty,
        dw_batch_id,
        dw_batch_date,
        dw_sys_load_tmstp,
        dw_sys_updt_tmstp
    )
    VALUES (
	    ldg.purchase_order_num,
        ldg.carton_num,
		ldg.vendor_carton_num,
		ldg.advance_shipment_num,
        ldg.location_num,
        ldg.event_id,
        ldg.event_tmstp,
		ldg.event_date,
		ldg.event_type,
        ldg.sku_num,
        ldg.sku_type,
        ldg.disposition_code,
        ldg.sku_qty,
        ldg.dw_batch_id,
        ldg.dw_batch_date,
        current_timestamp,
        current_timestamp
    );

ET;

/***********************************************************************************
-- Collect stats on fact tables
************************************************************************************/
COLLECT STATS ON {db_env}_NAP_FCT.STORE_INBOUND_CARTON_EVENTS_FACT
INDEX (purchase_order_num, carton_num, event_id, sku_num);

ET;

/***********************************************************************************
-- Perform audit between stg and fct tables
************************************************************************************/
CALL {db_env}_NAP_UTL.DQ_PIPELINE_AUDIT_V1
(
  'STORE_INBOUND_CARTON_EVENTS_TO_BASE',
  'NAP_ASCP_STORE_INBOUND_CARTON',
  '{db_env}_NAP_STG',
  'STORE_INBOUND_CARTON_EVENTS_LDG',
  '{db_env}_NAP_FCT',
  'STORE_INBOUND_CARTON_EVENTS_FACT',
  'Count_Distinct',
  0,
  'T-S',
  'concat(purchase_order_num, ''-'', carton_num, ''-'', event_id, ''-'', sku_num)',
  'concat(purchase_order_num, ''-'', carton_num, ''-'', event_id, ''-'', sku_num)',
  null,
  null,
  'N'
);

ET;

/***********************************************************************************
-- Merge into STORE_INBOUND_CARTON_FACT fact table
************************************************************************************/
LOCK TABLE {db_env}_NAP_FCT.STORE_INBOUND_CARTON_FACT FOR ACCESS
MERGE INTO {db_env}_NAP_FCT.STORE_INBOUND_CARTON_FACT fact
USING (
SELECT 
  purchase_order_num,
  carton_num,
  vendor_carton_num,
  advance_shipment_num,
  location_num,
  sku_num,
  sku_type,
  dw_batch_id,
  dw_batch_date,
  min(event_tmstp) as received_tmstp,
  min(event_date) as received_date,
  sum(sku_qty) as sku_qty
FROM 
  (SELECT 
     purchase_order_num,
     carton_num,
	 vendor_carton_num,
     advance_shipment_num,
     location_num,
     CAST(concat(event_tmstp, '+00:00') as timestamp with time zone format 'YYYY-MM-DDBHH:MI:SS.S(F)Z') as event_tmstp,
	 CAST(SUBSTRING(event_tmstp,1, 10) as DATE ) as event_date,
     sku_num,
     sku_type,
     sku_qty,
     (SELECT BATCH_ID FROM {db_env}_NAP_BASE_VWS.ELT_CONTROL WHERE SUBJECT_AREA_NM ='NAP_ASCP_STORE_INBOUND_CARTON') as dw_batch_id,
     (SELECT CURR_BATCH_DATE FROM {db_env}_NAP_BASE_VWS.ELT_CONTROL WHERE SUBJECT_AREA_NM ='NAP_ASCP_STORE_INBOUND_CARTON') as dw_batch_date
  FROM {db_env}_NAP_STG.STORE_INBOUND_CARTON_EVENTS_LDG
  QUALIFY ROW_NUMBER() OVER(PARTITION BY purchase_order_num, carton_num, event_id, sku_num  ORDER BY event_tmstp desc) = 1
  ) src
GROUP BY
  purchase_order_num,
  carton_num,
  vendor_carton_num,
  advance_shipment_num,
  location_num,
  sku_num,
  sku_type,
  dw_batch_id,
  dw_batch_date
) ldg
    ON fact.purchase_order_num = ldg.purchase_order_num
	AND fact.carton_num = ldg.carton_num
    AND fact.sku_num = ldg.sku_num
	AND fact.received_date = ldg.received_date
WHEN MATCHED THEN
UPDATE SET
    vendor_carton_num = ldg.vendor_carton_num,
	advance_shipment_num = ldg.advance_shipment_num,
    location_num = ldg.location_num,
    received_tmstp = ldg.received_tmstp,
    sku_type = ldg.sku_type,
    sku_qty = ldg.sku_qty,
    dw_batch_id = ldg.dw_batch_id,
    dw_batch_date = ldg.dw_batch_date,
    dw_sys_updt_tmstp = current_timestamp
WHEN NOT MATCHED THEN
    INSERT (
        purchase_order_num,
        carton_num,
		vendor_carton_num,
		advance_shipment_num,
        location_num,
        received_tmstp,
		received_date,
        sku_num,
        sku_type,
        sku_qty,
        dw_batch_id,
        dw_batch_date,
        dw_sys_load_tmstp,
        dw_sys_updt_tmstp
    )
    VALUES (
	    ldg.purchase_order_num,
        ldg.carton_num,
		ldg.vendor_carton_num,
		ldg.advance_shipment_num,
        ldg.location_num,
        ldg.received_tmstp,
		ldg.received_date,
        ldg.sku_num,
        ldg.sku_type,
        ldg.sku_qty,
        ldg.dw_batch_id,
        ldg.dw_batch_date,
        current_timestamp,
        current_timestamp
    );

ET;

/***********************************************************************************
-- Collect stats on fact tables
************************************************************************************/
COLLECT STATS ON {db_env}_NAP_FCT.STORE_INBOUND_CARTON_FACT
INDEX (purchase_order_num, carton_num, sku_num);

ET;

/***********************************************************************************
-- Perform audit between stg and fct tables
************************************************************************************/
CALL {db_env}_NAP_UTL.DQ_PIPELINE_AUDIT_V1
(
  'STORE_INBOUND_CARTON_TO_BASE',
  'NAP_ASCP_STORE_INBOUND_CARTON',
  '{db_env}_NAP_STG',
  'STORE_INBOUND_CARTON_EVENTS_LDG',
  '{db_env}_NAP_FCT',
  'STORE_INBOUND_CARTON_FACT',
  'Count_Distinct',
  0,
  'T-S',
  'concat(purchase_order_num, ''-'', carton_num, ''-'', sku_num)',
  'concat(purchase_order_num, ''-'', carton_num, ''-'', sku_num)',
  null,
  null,
  'N'
);

ET;
