--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : pypeline_sql/inventory_dextr_navi_store_return_to_vendor_request_completed/td_stg_to_fact.sql
-- Author                  : Bohdan Sapozhnikov
-- Description             : ETL to write from INVENTORY_DEXTR_NAVI_STORE_RETURN_TO_VENDOR_EXPIRED_ITEM_LDG to INVENTORY_DEXTR_NAVI_STORE_RETURN_TO_VENDOR_EXPIRED_ITEM_FACT
--                             and from INVENTORY_DEXTR_NAVI_STORE_RETURN_TO_VENDOR_COMPLETED_LDG to INVENTORY_DEXTR_NAVI_STORE_RETURN_TO_VENDOR_COMPLETED_FACT tables
-- Data Source             : inventory-store-rtv-avro Kafka topic
-- ETL Run Frequency       : Daily Hourly
-- Reference Documentation :  
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2024-04-26  Bohdan Sapozhnikov   FA-12529: Store RTV new events: StoreReturnToVendorCanceled, StoreReturnToVendorUpdated, StoreReturnToVendorRequestCompleted
-- 2024-08-07  Bohdan Sapozhnikov   FA-13535: Remapping returnToVendorNumber and externalReferenceNumber columns
--*************************************************************************************************************************************

MERGE INTO {db_env}_NAP_BASE_VWS.INVENTORY_DEXTR_NAVI_STORE_RETURN_TO_VENDOR_COMPLETED_FACT AS tgt
USING (
        SELECT 
                event_id as request_completed_event_id,
                CAST(event_tmstp as timestamp with time zone) as request_completed_tmstp,
                CAST(event_tmstp as timestamp(6) with time zone) at 'america pacific' as request_completed_tmstp_pacific,
                CAST((CAST(event_tmstp as timestamp(6) with time zone) at 'america pacific') AS date format 'YYYY-MM-DD') as request_completed_date_pacific,
                request_id,
                return_to_vendor_num,
                sku_num,
                sku_type_code,
                disposition_code,
                sku_qty,
                batch_id as dw_batch_id,
                batch_date as dw_batch_date
        FROM (SELECT * 
                FROM {db_env}_NAP_BASE_VWS.INVENTORY_DEXTR_NAVI_STORE_RETURN_TO_VENDOR_COMPLETED_LDG
             QUALIFY ROW_NUMBER() OVER(PARTITION BY request_id, return_to_vendor_num, sku_num ORDER BY CAST(event_tmstp as timestamp with time zone) desc) = 1) as l
        LEFT JOIN (SELECT batch_id, batch_date 
                     FROM {db_env}_NAP_BASE_VWS.ETL_BATCH_INFO
                    WHERE interface_code = 'INVENTORY_DEXTR_NAVI_STORE_RETURN_TO_VENDOR_REQUEST_COMPLETED' 
                      AND dw_sys_end_tmstp IS NULL) bi
          ON 1=1
      ) AS src
   ON tgt.request_id = src.request_id
  AND tgt.return_to_vendor_num = src.return_to_vendor_num
  AND tgt.sku_num = src.sku_num
WHEN MATCHED THEN
    UPDATE SET
        request_completed_event_id = src.request_completed_event_id,
        request_completed_tmstp = src.request_completed_tmstp,
        request_completed_tmstp_pacific = src.request_completed_tmstp_pacific,
        request_completed_date_pacific = src.request_completed_date_pacific,
        sku_type_code = src.sku_type_code,
        disposition_code = src.disposition_code,
        sku_qty = src.sku_qty,
        dw_batch_id = src.dw_batch_id,
        dw_batch_date = src.dw_batch_date,
        dw_sys_updt_tmstp = current_timestamp(0)
WHEN NOT MATCHED THEN
    INSERT
        (request_completed_event_id,
        request_completed_tmstp,
        request_completed_tmstp_pacific,
        request_completed_date_pacific,
        request_id,
        return_to_vendor_num,
        sku_num,
        sku_type_code,
        disposition_code,
        sku_qty,
        dw_batch_id,
        dw_batch_date,
        dw_sys_load_tmstp,
        dw_sys_updt_tmstp)
    VALUES 
        (src.request_completed_event_id,
        src.request_completed_tmstp,
        src.request_completed_tmstp_pacific,
        src.request_completed_date_pacific,
        src.request_id,
        src.return_to_vendor_num,
        src.sku_num,
        src.sku_type_code,
        src.disposition_code,
        src.sku_qty,
        src.dw_batch_id,
        src.dw_batch_date,
        current_timestamp(0),
        current_timestamp(0));

ET;

CREATE VOLATILE MULTISET TABLE #INVENTORY_DEXTR_NAVI_STORE_RETURN_TO_VENDOR_EXPIRED_ITEM_LDG AS
( SELECT 
        event_id as request_completed_event_id,
        CAST(event_tmstp as timestamp with time zone) as request_completed_tmstp,
        CAST(event_tmstp as timestamp(6) with time zone) at 'america pacific' as request_completed_tmstp_pacific,
        CAST((CAST(event_tmstp as timestamp(6) with time zone) at 'america pacific') AS date format 'YYYY-MM-DD') as request_completed_date_pacific,
        request_id,
        sku_num,
        sku_type_code,
        sku_qty,
        electronic_product_code,
        batch_id as dw_batch_id,
        batch_date as dw_batch_date
   FROM (SELECT * 
           FROM {db_env}_NAP_BASE_VWS.INVENTORY_DEXTR_NAVI_STORE_RETURN_TO_VENDOR_EXPIRED_ITEM_LDG
        QUALIFY ROW_NUMBER() OVER(PARTITION BY request_id, sku_num, electronic_product_code ORDER BY CAST(event_tmstp as timestamp with time zone) desc) = 1) as l
    LEFT JOIN (SELECT batch_id, batch_date 
                FROM {db_env}_NAP_BASE_VWS.ETL_BATCH_INFO
                WHERE interface_code = 'INVENTORY_DEXTR_NAVI_STORE_RETURN_TO_VENDOR_REQUEST_COMPLETED' 
                AND dw_sys_end_tmstp IS NULL) bi
      ON 1=1
)
WITH DATA
PRIMARY INDEX(request_id, sku_num)
ON COMMIT PRESERVE ROWS;
ET;

DELETE  
  FROM {db_env}_NAP_BASE_VWS.INVENTORY_DEXTR_NAVI_STORE_RETURN_TO_VENDOR_EXPIRED_ITEM_FACT tgt,
       #INVENTORY_DEXTR_NAVI_STORE_RETURN_TO_VENDOR_EXPIRED_ITEM_LDG as src
 WHERE tgt.request_id = src.request_id
   AND tgt.sku_num = src.sku_num
   AND tgt.electronic_product_code IS NULL;


MERGE INTO {db_env}_NAP_BASE_VWS.INVENTORY_DEXTR_NAVI_STORE_RETURN_TO_VENDOR_EXPIRED_ITEM_FACT AS tgt
USING #INVENTORY_DEXTR_NAVI_STORE_RETURN_TO_VENDOR_EXPIRED_ITEM_LDG AS src
   ON tgt.request_id = src.request_id
  AND tgt.sku_num = src.sku_num
  AND tgt.electronic_product_code = src.electronic_product_code
WHEN MATCHED THEN
    UPDATE SET
        request_completed_event_id = src.request_completed_event_id,
        request_completed_tmstp = src.request_completed_tmstp,
        request_completed_tmstp_pacific = src.request_completed_tmstp_pacific,
        request_completed_date_pacific = src.request_completed_date_pacific,
        sku_type_code = src.sku_type_code,
        sku_qty = src.sku_qty,
        dw_batch_id = src.dw_batch_id,
        dw_batch_date = src.dw_batch_date,
        dw_sys_updt_tmstp = current_timestamp(0)
WHEN NOT MATCHED THEN
    INSERT
        (request_completed_event_id,
        request_completed_tmstp,
        request_completed_tmstp_pacific,
        request_completed_date_pacific,
        request_id,
        sku_num,
        sku_type_code,
        sku_qty,
        electronic_product_code,
        dw_batch_id,
        dw_batch_date,
        dw_sys_load_tmstp,
        dw_sys_updt_tmstp)
    VALUES 
        (src.request_completed_event_id,
        src.request_completed_tmstp,
        src.request_completed_tmstp_pacific,
        src.request_completed_date_pacific,
        src.request_id,
        src.sku_num,
        src.sku_type_code,
        src.sku_qty,
        src.electronic_product_code,
        src.dw_batch_id,
        src.dw_batch_date,
        current_timestamp(0),
        current_timestamp(0));
ET;
