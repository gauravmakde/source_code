
--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : store_return_to_vendor_detail_load_fact_table.sql
-- Description             : write data from 
--                           1. INVENTORY_NAVI_STORE_TRANSFER_LDG to  INVENTORY_NAVI_STORE_TRANSFER_PLANNING_FACT and INVENTORY_NAVI_STORE_TRANSFER_DELIVERY_FACT
--                           2. INVENTORY_NAVI_STORE_TRANSFER_COMPLETED_LDG to INVENTORY_NAVI_STORE_TRANSFER_COMPLETED_FACT
--                           3. INVENTORY_NAVI_STORE_TRANSFER_EXPIRED_ITEM_LDG to INVENTORY_NAVI_STORE_TRANSFER_EXPIRED_ITEM_FACT
-- Reference Documentation : https://confluence.nordstrom.com/display/TDS/Store+Transfer+%28NAVI%29+-+Semantic+Layer
--/***********************************************************************************
---- Merge into final INVENTORY_NAVI_STORE_TRANSFER_PLANNING_FACT fact table
---- Merge into final INVENTORY_NAVI_STORE_TRANSFER_DELIVERY_FACT fact table
---- Merge into final INVENTORY_NAVI_STORE_TRANSFER_COMPLETED_FACT table
---- Merge into final INVENTORY_NAVI_STORE_TRANSFER_EXPIRED_ITEM_FACT table
--************************************************************************************/


/* Merge into INVENTORY_NAVI_STORE_TRANSFER_PLANNING_FACT fact table */

MERGE INTO {db_env}_NAP_BASE_VWS.INVENTORY_NAVI_STORE_TRANSFER_PLANNING_FACT AS tgt
USING (SELECT ldg.*,                 
              bi.batch_id as dw_batch_id,
              bi.batch_date as dw_batch_date
         FROM (SELECT COALESCE(created.transfer_num, canceled.transfer_num) as transfer_num,
                      created.external_refence_num,
                      created.transfer_type_code,
                      created.transfer_context_type,
                      COALESCE(created.sku_num, canceled.sku_num) as sku_num,
                      COALESCE(created.sku_type_code, canceled.sku_type_code) as sku_type_code,
                      COALESCE(created.location_num, canceled.location_num) as location_num,
                      created.to_location_num,
                      created.to_logical_location_num,
                      created.event_id as created_event_id,
                      CAST(created.event_tmstp as timestamp with time zone format 'YYYY-MM-DDBHH:MI:SS.S(F)Z') as created_tmstp,
                      CAST(created.event_tmstp as timestamp(6) with time zone) at 'america pacific' as created_tmstp_pacific,
                      CAST((CAST(created.event_tmstp as timestamp(6) with time zone) at 'america pacific') AS date format 'YYYY-MM-DD') as created_date_pacific,
                      created.user_id as created_user_id,
                      created.system_id as created_system_id,
                      created.disposition_code as created_disposition_code,
                      created.sku_qty as created_sku_qty,
                      canceled.event_id as canceled_event_id,
                      CAST(canceled.event_tmstp as timestamp with time zone format 'YYYY-MM-DDBHH:MI:SS.S(F)Z') as canceled_tmstp,
                      CAST(canceled.event_tmstp as timestamp(6) with time zone) at 'america pacific' as canceled_tmstp_pacific,
                      CAST((CAST(canceled.event_tmstp as timestamp(6) with time zone) at 'america pacific') AS date format 'YYYY-MM-DD') as canceled_date_pacific,
                      canceled.user_id as canceled_user_id,
                      canceled.system_id as canceled_system_id,
                      canceled.disposition_code as canceled_disposition_code,
                      canceled.sku_qty as canceled_sku_qty,
                      COALESCE(created.event_type, canceled.event_type) as latest_event_type
                 FROM (SELECT * 
                         FROM {db_env}_NAP_BASE_VWS.INVENTORY_NAVI_STORE_TRANSFER_LDG
                        WHERE event_type = 'StoreTransferCreated' 
                      QUALIFY ROW_NUMBER() OVER(PARTITION BY transfer_num,  sku_num, location_num  ORDER BY CAST(event_tmstp as timestamp)  desc) = 1) as created
                 FULL JOIN
                      (SELECT * 
                         FROM {db_env}_NAP_BASE_VWS.INVENTORY_NAVI_STORE_TRANSFER_LDG
                        WHERE event_type = 'StoreTransferCanceled' 
                      QUALIFY ROW_NUMBER() OVER(PARTITION BY transfer_num, sku_num, location_num  ORDER BY CAST(event_tmstp as timestamp)  desc) = 1) as canceled 
                   ON created.transfer_num = canceled.transfer_num   
                  AND created.sku_num = canceled.sku_num
                  AND created.location_num = canceled.location_num) ldg
         LEFT JOIN (SELECT batch_id, batch_date 
                      FROM {db_env}_NAP_BASE_VWS.ETL_BATCH_INFO
                     WHERE interface_code = 'INVENTORY_NAVI_STORE_TRANSFER_LIFECYCLE' 
                       AND dw_sys_end_tmstp IS NULL) bi
           ON 1=1
      ) AS src
   ON tgt.transfer_num = src.transfer_num
  AND tgt.sku_num = src.sku_num 
  AND tgt.location_num = src.location_num
WHEN MATCHED THEN
	UPDATE
		SET
			external_refence_num = COALESCE(src.external_refence_num, tgt.external_refence_num),
			transfer_type_code = COALESCE(src.transfer_type_code, tgt.transfer_type_code),
			transfer_context_type = COALESCE(src.transfer_context_type, tgt.transfer_context_type),
			sku_type_code = COALESCE(src.sku_type_code, tgt.sku_type_code),
			to_location_num = COALESCE(src.to_location_num, tgt.to_location_num),
			to_logical_location_num = COALESCE(src.to_logical_location_num, tgt.to_logical_location_num),
			created_event_id = COALESCE(src.created_event_id, tgt.created_event_id),
			created_tmstp = COALESCE(src.created_tmstp, tgt.created_tmstp),
			created_tmstp_pacific = COALESCE(src.created_tmstp_pacific, tgt.created_tmstp_pacific),
			created_date_pacific = COALESCE(src.created_date_pacific, tgt.created_date_pacific),
			created_user_id = COALESCE(src.created_user_id, tgt.created_user_id),
			created_system_id = COALESCE(src.created_system_id, tgt.created_system_id),
			created_disposition_code = COALESCE(src.created_disposition_code, tgt.created_disposition_code),
			created_sku_qty = COALESCE(src.created_sku_qty, tgt.created_sku_qty),
			canceled_event_id = COALESCE(src.canceled_event_id, tgt.canceled_event_id),
			canceled_tmstp = COALESCE(src.canceled_tmstp, tgt.canceled_tmstp),
			canceled_tmstp_pacific = COALESCE(src.canceled_tmstp_pacific, tgt.canceled_tmstp_pacific),
			canceled_date_pacific = COALESCE(src.canceled_date_pacific, tgt.canceled_date_pacific),
			canceled_user_id = COALESCE(src.canceled_user_id, tgt.canceled_user_id),
			canceled_system_id = COALESCE(src.canceled_system_id, tgt.canceled_system_id),
			canceled_disposition_code = COALESCE(src.canceled_disposition_code, tgt.canceled_disposition_code),
			canceled_sku_qty = COALESCE(src.canceled_sku_qty, tgt.canceled_sku_qty),
			latest_event_type = COALESCE(src.latest_event_type, tgt.latest_event_type),
			dw_batch_id = src.dw_batch_id,
			dw_batch_date = src.dw_batch_date,
			dw_sys_updt_tmstp = current_timestamp(0)
WHEN NOT MATCHED THEN
	INSERT (
	        transfer_num,
			external_refence_num,
			transfer_type_code,
			transfer_context_type,
			sku_num,
			sku_type_code,
			location_num,
			to_location_num,
			to_logical_location_num,
			created_event_id,
			created_tmstp,
			created_tmstp_pacific,
			created_date_pacific,
			created_user_id,
			created_system_id,
			created_disposition_code,
			created_sku_qty,
			canceled_event_id,
			canceled_tmstp,
			canceled_tmstp_pacific,
			canceled_date_pacific,
			canceled_user_id,
			canceled_system_id,
			canceled_disposition_code,
			canceled_sku_qty,
			latest_event_type,
			dw_batch_id,
			dw_batch_date,
			dw_sys_load_tmstp,
			dw_sys_updt_tmstp
	)
	VALUES (
	        src.transfer_num,
			src.external_refence_num,
			src.transfer_type_code,
			src.transfer_context_type,
			src.sku_num,
			src.sku_type_code,
			src.location_num,
			src.to_location_num,
			src.to_logical_location_num,
			src.created_event_id,
			src.created_tmstp,
			src.created_tmstp_pacific,
			src.created_date_pacific,
			src.created_user_id,
			src.created_system_id,
			src.created_disposition_code,
			src.created_sku_qty,
			src.canceled_event_id,
			src.canceled_tmstp,
			src.canceled_tmstp_pacific,
			src.canceled_date_pacific,
			src.canceled_user_id,
			src.canceled_system_id,
			src.canceled_disposition_code,
			src.canceled_sku_qty,
			src.latest_event_type,
			src.dw_batch_id,
			src.dw_batch_date,
			current_timestamp(0),
			current_timestamp(0)
	);

ET;

/***********************************************************************************
-- Collect stats on fact tables
************************************************************************************/
COLLECT STATS ON {db_env}_NAP_FCT.INVENTORY_NAVI_STORE_TRANSFER_PLANNING_FACT
INDEX (transfer_num, sku_num, location_num);

ET;

/* Merge into INVENTORY_NAVI_STORE_TRANSFER_DELIVERY_FACT fact table */

MERGE INTO  {db_env}_NAP_BASE_VWS.INVENTORY_NAVI_STORE_TRANSFER_DELIVERY_FACT AS tgt
USING (SELECT ldg.*,                 
              bi.batch_id as dw_batch_id,
              bi.batch_date as dw_batch_date
         FROM (SELECT COALESCE(rec_adj.transfer_num, shipped.transfer_num) as transfer_num,
		              COALESCE(rec_adj.carton_num, shipped.carton_num) as carton_num,
					  COALESCE(rec_adj.shipment_num, shipped.shipment_num) as shipment_num, 
                      COALESCE(rec_adj.sku_num, shipped.sku_num) as sku_num,
                      COALESCE(rec_adj.sku_type_code, shipped.sku_type_code) as sku_type_code,
					  COALESCE(rec_adj.bill_of_lading, shipped.bill_of_lading) as bill_of_lading,
                      COALESCE(rec_adj.location_num, shipped.location_num) as location_num,
					  shipped.to_location_num,
                      shipped.to_logical_location_num,
                      shipped.event_id as shipped_event_id,
                      CAST(shipped.event_tmstp as timestamp with time zone format 'YYYY-MM-DDBHH:MI:SS.S(F)Z') as shipped_tmstp,
                      CAST(shipped.event_tmstp as timestamp(6) with time zone) at 'america pacific' as shipped_tmstp_pacific,
                      CAST((CAST(shipped.event_tmstp as timestamp(6) with time zone) at 'america pacific') AS date format 'YYYY-MM-DD') as shipped_date_pacific,
                      shipped.disposition_code as shipped_disposition_code,
                      shipped.sku_qty as shipped_sku_qty,
                      rec_adj.received_event_id,
                      CAST(rec_adj.received_tmstp as timestamp with time zone format 'YYYY-MM-DDBHH:MI:SS.S(F)Z') as received_tmstp,
                      CAST(rec_adj.received_tmstp as timestamp(6) with time zone) at 'america pacific' as received_tmstp_pacific,
                      CAST((CAST(rec_adj.received_tmstp as timestamp(6) with time zone) at 'america pacific') AS date format 'YYYY-MM-DD') as received_date_pacific,
                      CAST(rec_adj.received_adjusted_tmstp as timestamp with time zone format 'YYYY-MM-DDBHH:MI:SS.S(F)Z') as received_adjusted_tmstp,
                      CAST(rec_adj.received_adjusted_tmstp as timestamp(6) with time zone) at 'america pacific' as received_adjusted_tmstp_pacific,
                      CAST((CAST(rec_adj.received_adjusted_tmstp as timestamp(6) with time zone) at 'america pacific') AS date format 'YYYY-MM-DD') as received_adjusted_date_pacific,
                      rec_adj.received_disposition_code,
                      rec_adj.received_sku_qty,
                      COALESCE(rec_adj.latest_event_type, shipped.event_type) as latest_event_type
                 FROM (SELECT * 
                         FROM {db_env}_NAP_BASE_VWS.INVENTORY_NAVI_STORE_TRANSFER_LDG
                        WHERE event_type = 'StoreTransferShipped' 
                      QUALIFY ROW_NUMBER() OVER(PARTITION BY transfer_num,  carton_num,  sku_num, location_num  ORDER BY CAST(event_tmstp as timestamp)  desc) = 1) as shipped
                 FULL JOIN
                      (SELECT COALESCE(received_adjusted.transfer_num, received.transfer_num) as transfer_num,
							  COALESCE(received_adjusted.carton_num, received.carton_num) as carton_num,
							  COALESCE(received_adjusted.shipment_num, received.shipment_num) as shipment_num, 
							  COALESCE(received_adjusted.sku_num, received.sku_num) as sku_num,
							  COALESCE(received_adjusted.sku_type_code, received.sku_type_code) as sku_type_code,
							  COALESCE(received_adjusted.bill_of_lading, received.bill_of_lading) as bill_of_lading,
							  COALESCE(received_adjusted.location_num, received.location_num) as location_num,
							  received.event_id as received_event_id,
							  received.event_tmstp as received_tmstp,
							  received_adjusted.event_tmstp as received_adjusted_tmstp,
							  COALESCE(received_adjusted.disposition_code, received.disposition_code) as received_disposition_code,
							  COALESCE(received_adjusted.sku_qty, received.sku_qty) as received_sku_qty,
							  COALESCE(received_adjusted.event_type, received.event_type) as latest_event_type 
						FROM (SELECT *
                                FROM {db_env}_NAP_BASE_VWS.INVENTORY_NAVI_STORE_TRANSFER_LDG  received
                               WHERE event_type = 'StoreTransferReceived' 
                             QUALIFY ROW_NUMBER() OVER(PARTITION BY transfer_num, carton_num, sku_num, location_num  ORDER BY CAST(event_tmstp as timestamp)  desc) = 1) as received 
					    FULL JOIN 
							 (SELECT *
                                FROM {db_env}_NAP_BASE_VWS.INVENTORY_NAVI_STORE_TRANSFER_LDG  received
                               WHERE event_type = 'StoreTransferReceivedAdjusted' 
                             QUALIFY ROW_NUMBER() OVER(PARTITION BY transfer_num, carton_num, sku_num, location_num  ORDER BY CAST(event_tmstp as timestamp)  desc) = 1) as received_adjusted 	 
						  ON received.transfer_num = received_adjusted.transfer_num
						 AND received.carton_num = received_adjusted.carton_num   
						 AND received.sku_num = received_adjusted.sku_num
						 AND received.location_num = received_adjusted.location_num) rec_adj
				   ON shipped.transfer_num = rec_adj.transfer_num
				  AND shipped.carton_num = rec_adj.carton_num   
				  AND shipped.sku_num = rec_adj.sku_num
				  AND shipped.location_num = rec_adj.location_num) ldg	 
         LEFT JOIN (SELECT batch_id, batch_date 
                      FROM {db_env}_NAP_BASE_VWS.ETL_BATCH_INFO
                     WHERE interface_code = 'INVENTORY_NAVI_STORE_TRANSFER_LIFECYCLE' 
                       AND dw_sys_end_tmstp IS NULL) bi
           ON 1=1)  AS src
       ON tgt.transfer_num = src.transfer_num
	  AND tgt.carton_num = src.carton_num 
	  AND tgt.sku_num = src.sku_num 
	  AND tgt.location_num = src.location_num	
WHEN MATCHED  THEN
	UPDATE
		SET
		    shipment_num = COALESCE(src.shipment_num, tgt.shipment_num), 
			sku_type_code = COALESCE(src.sku_type_code, tgt.sku_type_code),
			bill_of_lading = COALESCE(src.bill_of_lading, tgt.bill_of_lading),
			to_location_num = COALESCE(src.to_location_num, tgt.to_location_num),
			to_logical_location_num = COALESCE(src.to_logical_location_num, tgt.to_logical_location_num),
			shipped_event_id = COALESCE(src.shipped_event_id, tgt.shipped_event_id),
			shipped_tmstp = COALESCE(src.shipped_tmstp, tgt.shipped_tmstp),
			shipped_tmstp_pacific = COALESCE(src.shipped_tmstp_pacific, tgt.shipped_tmstp_pacific),
			shipped_date_pacific = COALESCE(src.shipped_date_pacific, tgt.shipped_date_pacific),
			shipped_disposition_code = COALESCE(src.shipped_disposition_code, tgt.shipped_disposition_code),
			shipped_sku_qty = COALESCE(src.shipped_sku_qty, tgt.shipped_sku_qty),
			received_event_id = COALESCE(src.received_event_id, tgt.received_event_id),
			received_tmstp = COALESCE(src.received_tmstp, tgt.received_tmstp),
			received_tmstp_pacific = COALESCE(src.received_tmstp_pacific, tgt.received_tmstp_pacific),
			received_date_pacific = COALESCE(src.received_date_pacific, tgt.received_date_pacific),
			received_adjusted_tmstp = COALESCE(src.received_adjusted_tmstp, tgt.received_adjusted_tmstp),
			received_adjusted_tmstp_pacific = COALESCE(src.received_adjusted_tmstp_pacific, tgt.received_adjusted_tmstp_pacific),
			received_adjusted_date_pacific = COALESCE(src.received_adjusted_date_pacific, tgt.received_adjusted_date_pacific),
			received_disposition_code = COALESCE(src.received_disposition_code, tgt.received_disposition_code),
			received_sku_qty = COALESCE(src.received_sku_qty, tgt.received_sku_qty),
			latest_event_type = COALESCE(src.latest_event_type, tgt.latest_event_type),
			dw_batch_id = src.dw_batch_id,
			dw_batch_date = src.dw_batch_date,
			dw_sys_updt_tmstp = current_timestamp(0)
WHEN NOT MATCHED THEN
	INSERT (
			transfer_num,
			carton_num,
			shipment_num,
			sku_num,
			sku_type_code,
			bill_of_lading,
			location_num,
			to_location_num,
			to_logical_location_num,
			shipped_event_id,
			shipped_tmstp,
			shipped_tmstp_pacific,
			shipped_date_pacific,
			shipped_disposition_code,
			shipped_sku_qty,
			received_event_id,
			received_tmstp,
			received_tmstp_pacific,
			received_date_pacific,
			received_adjusted_tmstp,
			received_adjusted_tmstp_pacific,
			received_adjusted_date_pacific,
			received_disposition_code,
			received_sku_qty,
			latest_event_type,
			dw_batch_id,
			dw_batch_date,
			dw_sys_load_tmstp,
			dw_sys_updt_tmstp
	)
	VALUES (
			src.transfer_num,
			src.carton_num,
			src.shipment_num,
			src.sku_num,
			src.sku_type_code,
			src.bill_of_lading,
			src.location_num,
			src.to_location_num,
			src.to_logical_location_num,
			src.shipped_event_id,
			src.shipped_tmstp,
			src.shipped_tmstp_pacific,
			src.shipped_date_pacific,
			src.shipped_disposition_code,
			src.shipped_sku_qty,
			src.received_event_id,
			src.received_tmstp,
			src.received_tmstp_pacific,
			src.received_date_pacific,
			src.received_adjusted_tmstp,
			src.received_adjusted_tmstp_pacific,
			src.received_adjusted_date_pacific,
			src.received_disposition_code,
			src.received_sku_qty,
			src.latest_event_type,
			src.dw_batch_id,
			src.dw_batch_date,
			current_timestamp(0),
			current_timestamp(0)
	);

ET;

/***********************************************************************************
-- Collect stats on fact tables
************************************************************************************/
COLLECT STATS ON {db_env}_NAP_FCT.INVENTORY_NAVI_STORE_TRANSFER_DELIVERY_FACT
INDEX (transfer_num, carton_num, sku_num, location_num);

ET;

/* Merge into INVENTORY_NAVI_STORE_TRANSFER_COMPLETED_FACT */

MERGE INTO {db_env}_NAP_BASE_VWS.INVENTORY_NAVI_STORE_TRANSFER_COMPLETED_FACT AS tgt
USING (
        SELECT 
                event_id as request_completed_event_id,
                CAST(event_tmstp as timestamp with time zone) as request_completed_tmstp,
                CAST(event_tmstp as timestamp(6) with time zone) at 'america pacific' as request_completed_tmstp_pacific,
                CAST((CAST(event_tmstp as timestamp(6) with time zone) at 'america pacific') AS date format 'YYYY-MM-DD') as request_completed_date_pacific,
                request_id,
                transfer_num,
                sku_num,
                sku_type_code,
                disposition_code,
                sku_qty,
                batch_id as dw_batch_id,
                batch_date as dw_batch_date
        FROM (SELECT * 
                FROM {db_env}_NAP_BASE_VWS.INVENTORY_NAVI_STORE_TRANSFER_COMPLETED_LDG
             QUALIFY ROW_NUMBER() OVER(PARTITION BY request_id, transfer_num, sku_num ORDER BY CAST(event_tmstp as timestamp with time zone) desc) = 1) as l
        LEFT JOIN (SELECT batch_id, batch_date 
                     FROM {db_env}_NAP_BASE_VWS.ETL_BATCH_INFO
                    WHERE interface_code = 'INVENTORY_NAVI_STORE_TRANSFER_LIFECYCLE' 
                      AND dw_sys_end_tmstp IS NULL) bi
          ON 1=1
      ) AS src
   ON tgt.request_id = src.request_id
  AND tgt.transfer_num = src.transfer_num
  AND tgt.sku_num = src.sku_num
WHEN MATCHED THEN
UPDATE
	SET
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
	INSERT (
	        request_completed_event_id,
			request_completed_tmstp,
			request_completed_tmstp_pacific,
			request_completed_date_pacific,
			request_id,
			transfer_num,
			sku_num,
			sku_type_code,
			disposition_code,
			sku_qty,
			dw_batch_id,
			dw_batch_date,
			dw_sys_load_tmstp,
			dw_sys_updt_tmstp
	)
	VALUES (
			src.request_completed_event_id,
			src.request_completed_tmstp,
			src.request_completed_tmstp_pacific,
			src.request_completed_date_pacific,
			src.request_id,
			src.transfer_num,
			src.sku_num,
			src.sku_type_code,
			src.disposition_code,
			src.sku_qty,
			src.dw_batch_id,
			src.dw_batch_date,
			current_timestamp(0),
			current_timestamp(0)
	);

ET;

/* Merge into INVENTORY_NAVI_STORE_TRANSFER_EXPIRED_ITEM_FACT */

CREATE VOLATILE MULTISET TABLE #INVENTORY_NAVI_STORE_TRANSFER_EXPIRED_ITEM_LDG AS
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
           FROM {db_env}_NAP_BASE_VWS.INVENTORY_NAVI_STORE_TRANSFER_EXPIRED_ITEM_LDG
        QUALIFY ROW_NUMBER() OVER(PARTITION BY request_id, sku_num, electronic_product_code ORDER BY CAST(event_tmstp as timestamp with time zone) desc) = 1) as l
    LEFT JOIN (SELECT batch_id, batch_date 
                FROM {db_env}_NAP_BASE_VWS.ETL_BATCH_INFO
                WHERE interface_code = 'INVENTORY_NAVI_STORE_TRANSFER_LIFECYCLE' 
                AND dw_sys_end_tmstp IS NULL) bi
      ON 1=1
)
WITH DATA
PRIMARY INDEX(request_id ,sku_num)
ON COMMIT PRESERVE ROWS;
ET;




DELETE  
  FROM {db_env}_NAP_BASE_VWS.INVENTORY_NAVI_STORE_TRANSFER_EXPIRED_ITEM_FACT tgt,
       #INVENTORY_NAVI_STORE_TRANSFER_EXPIRED_ITEM_LDG as src
 WHERE tgt.request_id = src.request_id
   AND tgt.sku_num = src.sku_num
   AND tgt.electronic_product_code IS NULL;


MERGE INTO {db_env}_NAP_BASE_VWS.INVENTORY_NAVI_STORE_TRANSFER_EXPIRED_ITEM_FACT AS tgt
USING #INVENTORY_NAVI_STORE_TRANSFER_EXPIRED_ITEM_LDG AS src
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
