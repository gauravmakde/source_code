/***********************************************************************************
-- Merge into STORE_INVENTORY_TRANSFER_SHIPMENT_RECEIPT_FACT fact table
************************************************************************************/
LOCK TABLE {db_env}_NAP_FCT.STORE_INVENTORY_TRANSFER_SHIPMENT_RECEIPT_FACT FOR ACCESS
MERGE INTO {db_env}_NAP_FCT.STORE_INVENTORY_TRANSFER_SHIPMENT_RECEIPT_FACT fact
USING (
SELECT
    coalesce(rec.transfer_num, ship.transfer_num) as transfer_num, 
    coalesce(rec.transfer_type, ship.transfer_type) as transfer_type,
	ship.event_id as shipped_event_id,
    rec.event_id as receipt_event_id,
	CAST(concat(ship.event_tmstp, '+00:00') as timestamp with time zone format 'YYYY-MM-DDBHH:MI:SS.S(F)Z') as shipped_tmstp,
	CAST(concat(rec.event_tmstp, '+00:00') as timestamp with time zone format 'YYYY-MM-DDBHH:MI:SS.S(F)Z') as receipt_tmstp,
	coalesce(rec.event_type, ship.event_type) as latest_event_type,
    ship.location_num,
	coalesce(rec.location_num, ship.to_location_num) as to_location_num,
	ship.to_logical_location_num,
    coalesce(rec.bill_of_lading, ship.bill_of_lading) as bill_of_lading,
    coalesce(rec.carton_num, ship.carton_num) as carton_num,
    coalesce(rec.sku_num, ship.sku_num) as sku_num,
    coalesce(rec.sku_type, ship.sku_type) as sku_type,
    ship.sku_qty as shipped_sku_qty,
	rec.sku_qty as receipt_sku_qty,
	ship.disposition_code as shipped_disposition_code, 
	rec.disposition_code as receipt_disposition_code,
    (SELECT BATCH_ID FROM {db_env}_NAP_BASE_VWS.ELT_CONTROL WHERE SUBJECT_AREA_NM ='NAP_ASCP_STORE_INVENTORY_TRANSFER_SHIPMENT_RECEIPT') as dw_batch_id,
    (SELECT CURR_BATCH_DATE FROM {db_env}_NAP_BASE_VWS.ELT_CONTROL WHERE SUBJECT_AREA_NM ='NAP_ASCP_STORE_INVENTORY_TRANSFER_SHIPMENT_RECEIPT') as dw_batch_date
FROM (select * from {db_env}_NAP_BASE_VWS.STORE_INVENTORY_TRANSFER_EVENTS_LDG
      where event_type = 'STORE_TRANSFER_SHIPPED'
      QUALIFY ROW_NUMBER() OVER(PARTITION BY transfer_num, transfer_type, carton_num, sku_num  ORDER BY event_tmstp desc) = 1
      )	ship full join
     (select 
	    transfer_num,
        transfer_type,
        event_id,
        event_tmstp,
	    event_type,
        shipment_num,
        location_num,
        bill_of_lading,
        carton_num,
        sku_num,
        sku_type,
        disposition_code,
        sku_qty 
	  from {db_env}_NAP_BASE_VWS.STORE_INVENTORY_TRANSFER_EVENTS_LDG
	  where event_type = 'STORE_TRANSFER_RECEIVED'
      QUALIFY ROW_NUMBER() OVER(PARTITION BY transfer_num, transfer_type, carton_num, sku_num  ORDER BY event_tmstp desc) = 1
	  union all
	  select * from {db_env}_NAP_BASE_VWS.STORE_INVENTORY_TRANSFER_ALLOCATION_RECEIPT_LDG
      QUALIFY ROW_NUMBER() OVER(PARTITION BY transfer_num, transfer_type, carton_num, sku_num  ORDER BY event_tmstp desc) = 1
	  ) rec ON ship.transfer_num = rec.transfer_num
	    AND ship.transfer_type = rec.transfer_type
	    AND ship.carton_num = rec.carton_num
        AND ship.sku_num = rec.sku_num
) ldg
    ON fact.transfer_num = ldg.transfer_num
	AND fact.transfer_type = ldg.transfer_type
	AND fact.carton_num = ldg.carton_num
    AND fact.sku_num = ldg.sku_num
WHEN MATCHED THEN
UPDATE SET
    shipped_event_id = ldg.shipped_event_id,
	receipt_event_id = ldg.receipt_event_id,
    shipped_tmstp = ldg.shipped_tmstp,
	receipt_tmstp = ldg.receipt_tmstp,
	latest_event_type = ldg.latest_event_type,
    location_num = ldg.location_num,
	to_location_num = ldg.to_location_num,
	to_logical_location_num = ldg.to_logical_location_num,
    bill_of_lading = ldg.bill_of_lading,
    sku_type = ldg.sku_type,
    shipped_sku_qty = ldg.shipped_sku_qty,
	receipt_sku_qty = ldg.receipt_sku_qty,
	shipped_disposition_code = ldg.shipped_disposition_code,
	receipt_disposition_code = ldg.receipt_disposition_code,
    dw_sys_updt_tmstp = current_timestamp(0)
WHEN NOT MATCHED THEN
    INSERT (
        transfer_num,
        transfer_type,
		shipped_event_id,
		receipt_event_id,
        shipped_tmstp,
		receipt_tmstp,
	    latest_event_type,
        location_num,
	    to_location_num,
	    to_logical_location_num,
        bill_of_lading,
        carton_num,
        sku_num,
        sku_type,
        shipped_sku_qty,
		receipt_sku_qty,
		shipped_disposition_code,
		receipt_disposition_code,
        dw_batch_id,
        dw_batch_date,
        dw_sys_load_tmstp,
        dw_sys_updt_tmstp
    )
    VALUES (
	    ldg.transfer_num,
        ldg.transfer_type,
		ldg.shipped_event_id,
		ldg.receipt_event_id,
        ldg.shipped_tmstp,
		ldg.receipt_tmstp,
	    ldg.latest_event_type,
        ldg.location_num,
	    ldg.to_location_num,
	    ldg.to_logical_location_num,
        ldg.bill_of_lading,
        ldg.carton_num,
        ldg.sku_num,
        ldg.sku_type,
        ldg.shipped_sku_qty,
		ldg.receipt_sku_qty,
		ldg.shipped_disposition_code,
		ldg.receipt_disposition_code,
        ldg.dw_batch_id,
        ldg.dw_batch_date,
        current_timestamp(0),
        current_timestamp(0)
    );
	
/***********************************************************************************
-- Collect stats on fact tables
************************************************************************************/
COLLECT STATS ON {db_env}_NAP_FCT.STORE_INVENTORY_TRANSFER_SHIPMENT_RECEIPT_FACT
INDEX (transfer_num, transfer_type, carton_num, sku_num);
