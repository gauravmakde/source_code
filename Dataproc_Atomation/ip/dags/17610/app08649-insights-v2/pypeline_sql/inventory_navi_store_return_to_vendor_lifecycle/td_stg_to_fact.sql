/***********************************************************************************
-- DELETE failed records from staging table
************************************************************************************/
LOCK TABLE {db_env}_NAP_STG.INVENTORY_NAVI_STORE_RETURN_TO_VENDOR_LIFECYCLE_LDG FOR ACCESS
DELETE FROM {db_env}_NAP_STG.INVENTORY_NAVI_STORE_RETURN_TO_VENDOR_LIFECYCLE_LDG
WHERE (
  -- Primary key null check
  return_to_vendor_num is null
  or sku_num is null
  or sku_type is null
  or location_num is null
  -- Primary key empty string check
  or return_to_vendor_num = ''
  or sku_num = ''
  or sku_type = ''
  or location_num = ''
  -- Primary key empty quotes check
  or return_to_vendor_num = '""'
  or sku_num = '""'
  or sku_type = '""'
  or location_num  = '""'
  -- Timestamp cast check
  or (last_updated_sys_tmstp is null)
);

--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : store_return_to_vendor_detail_load_fact_table.sql
-- Description             : write data from INVENTORY_NAVI_STORE_RETURN_TO_VENDOR_LIFECYCLE_LDG to INVENTORY_NAVI_STORE_RETURN_TO_VENDOR_LIFECYCLE_FACT table
-- Reference Documentation : https://confluence.nordstrom.com/pages/viewpage.action?pageId=1391364079
--/***********************************************************************************
---- Merge into final INVENTORY_NAVI_STORE_RETURN_TO_VENDOR_LIFECYCLE_FACT fact table
--************************************************************************************/

LOCK TABLE {db_env}_NAP_FCT.INVENTORY_NAVI_STORE_RETURN_TO_VENDOR_LIFECYCLE_FACT FOR ACCESS
MERGE INTO {db_env}_NAP_FCT.INVENTORY_NAVI_STORE_RETURN_TO_VENDOR_LIFECYCLE_FACT fact
USING (
        SELECT
                return_to_vendor_num,
                sku_num,
                sku_type,
                location_num,
                created_return_authorization_num,
                created_event_id,
                CAST(concat(created_event_tmstp, '+00:00') as timestamp with time zone format 'YYYY-MM-DDBHH:MI:SS.S(F)Z') as created_event_tmstp,
                CAST(concat(created_event_tmstp, '+00:00') as timestamp(6) with time zone) at 'america pacific' as created_event_tmstp_pacific,
                CAST((CAST(concat(created_event_tmstp, '+00:00') as timestamp(6) with time zone) at 'america pacific') AS date format 'YYYY-MM-DD') as created_event_date_pacific,
                created_vendor_num,
                created_qty,
                created_disposition_code,
                created_reason_code,
                canceled_event_id,
                CAST(concat(canceled_event_tmstp, '+00:00') as timestamp with time zone format 'YYYY-MM-DDBHH:MI:SS.S(F)Z') as canceled_event_tmstp,
                CAST(concat(canceled_event_tmstp, '+00:00') as timestamp(6) with time zone) at 'america pacific' as canceled_event_tmstp_pacific,
                CAST((CAST(concat(canceled_event_tmstp, '+00:00') as timestamp(6) with time zone) at 'america pacific') AS date format 'YYYY-MM-DD') as canceled_event_date_pacific,
                canceled_qty,
                canceled_disposition_code,
                canceled_reason_code,
                shipped_event_id,
                CAST(concat(shipped_event_tmstp, '+00:00') as timestamp with time zone format 'YYYY-MM-DDBHH:MI:SS.S(F)Z') as shipped_event_tmstp,
                CAST(concat(shipped_event_tmstp, '+00:00') as timestamp(6) with time zone) at 'america pacific' as shipped_event_tmstp_pacific,
                CAST((CAST(concat(shipped_event_tmstp, '+00:00') as timestamp(6) with time zone) at 'america pacific') AS date format 'YYYY-MM-DD') as shipped_event_date_pacific,
                shipped_qty,
                shipped_disposition_code,
                CAST(concat(created_sys_tmstp, '+00:00') as timestamp with time zone format 'YYYY-MM-DDBHH:MI:SS.S(F)Z') as created_sys_tmstp,
                CAST(concat(last_updated_sys_tmstp, '+00:00') as timestamp with time zone format 'YYYY-MM-DDBHH:MI:SS.S(F)Z') as last_updated_sys_tmstp,
                last_triggering_event_type,
                partnerRelationship_id as partner_relationship_id,
                partnerRelationship_type as partner_relationship_type_code,
                batch_id as dw_batch_id,
                batch_date as dw_batch_date
        FROM (SELECT * 
                FROM {db_env}_NAP_BASE_VWS.INVENTORY_NAVI_STORE_RETURN_TO_VENDOR_LIFECYCLE_LDG
            QUALIFY ROW_NUMBER() OVER(PARTITION BY return_to_vendor_num, sku_num, sku_type, location_num ORDER BY last_updated_sys_tmstp desc) = 1) as l
        LEFT JOIN (SELECT batch_id, batch_date 
                    FROM {db_env}_NAP_BASE_VWS.ETL_BATCH_INFO
                    WHERE interface_code = 'INVENTORY_NAVI_STORE_RETURN_TO_VENDOR_LIFECYCLE_FACT' 
                    AND dw_sys_end_tmstp IS NULL) bi
            ON 1=1
            ) as ldg
     ON fact.return_to_vendor_num = ldg.return_to_vendor_num
    AND fact.sku_num = ldg.sku_num
    AND fact.sku_type = ldg.sku_type
    AND fact.location_num = ldg.location_num
WHEN MATCHED THEN
    UPDATE SET
        created_return_authorization_num = ldg.created_return_authorization_num,
        created_sys_tmstp = ldg.created_sys_tmstp,
        last_updated_sys_tmstp = ldg.last_updated_sys_tmstp,
        last_triggering_event_type = ldg.last_triggering_event_type,
        created_event_id = ldg.created_event_id,
        canceled_event_id = ldg.canceled_event_id,
        shipped_event_id = ldg.shipped_event_id,
        created_event_tmstp = ldg.created_event_tmstp,
        created_event_tmstp_pacific = ldg.created_event_tmstp_pacific,
        created_event_date_pacific = ldg.created_event_date_pacific,
        canceled_event_tmstp = ldg.canceled_event_tmstp,
        canceled_event_tmstp_pacific = ldg.canceled_event_tmstp_pacific,
        canceled_event_date_pacific = ldg.canceled_event_date_pacific,
        shipped_event_tmstp = ldg.shipped_event_tmstp,
        shipped_event_tmstp_pacific = ldg.shipped_event_tmstp_pacific,
        shipped_event_date_pacific = ldg.shipped_event_date_pacific,
        created_vendor_num = ldg.created_vendor_num,
        created_qty = ldg.created_qty,
        canceled_qty = ldg.canceled_qty,
        shipped_qty = ldg.shipped_qty,
        created_disposition_code = ldg.created_disposition_code,
        canceled_disposition_code = ldg.canceled_disposition_code,
        shipped_disposition_code = ldg.shipped_disposition_code,
        created_reason_code = ldg.created_reason_code,
        canceled_reason_code = ldg.canceled_reason_code,
        partner_relationship_id = ldg.partner_relationship_id,
        partner_relationship_type_code = ldg.partner_relationship_type_code,
        dw_batch_id = ldg.dw_batch_id,
        dw_batch_date = ldg.dw_batch_date,
        dw_sys_updt_tmstp = current_timestamp(0)
WHEN NOT MATCHED THEN
    INSERT
        (
            return_to_vendor_num,
            sku_num,
            sku_type,
            location_num,
            created_return_authorization_num,
            created_event_id,
            created_event_tmstp,
            created_event_tmstp_pacific,
            created_event_date_pacific,
            created_vendor_num,
            created_qty,
            created_disposition_code,
            created_reason_code,
            canceled_event_id,
            canceled_event_tmstp,
            canceled_event_tmstp_pacific,
            canceled_event_date_pacific,
            canceled_qty,
            canceled_disposition_code,
            canceled_reason_code,
            shipped_event_id,
            shipped_event_tmstp,
            shipped_event_tmstp_pacific,
            shipped_event_date_pacific,
            shipped_qty,
            shipped_disposition_code,
            created_sys_tmstp,
            last_updated_sys_tmstp,
            last_triggering_event_type,
            partner_relationship_id,
            partner_relationship_type_code,
            dw_batch_id,
            dw_batch_date,
            dw_sys_load_tmstp,
            dw_sys_updt_tmstp
        )
    VALUES
        (
            ldg.return_to_vendor_num,
            ldg.sku_num,
            ldg.sku_type,
            ldg.location_num,
            ldg.created_return_authorization_num,
            ldg.created_event_id,
            ldg.created_event_tmstp,
            ldg.created_event_tmstp_pacific,
            ldg.created_event_date_pacific,
            ldg.created_vendor_num,
            ldg.created_qty,
            ldg.created_disposition_code,
            ldg.created_reason_code,
            ldg.canceled_event_id,
            ldg.canceled_event_tmstp,
            ldg.canceled_event_tmstp_pacific,
            ldg.canceled_event_date_pacific,
            ldg.canceled_qty,
            ldg.canceled_disposition_code,
            ldg.canceled_reason_code,
            ldg.shipped_event_id,
            ldg.shipped_event_tmstp,
            ldg.shipped_event_tmstp_pacific,
            ldg.shipped_event_date_pacific,
            ldg.shipped_qty,
            ldg.shipped_disposition_code,
            ldg.created_sys_tmstp,
            ldg.last_updated_sys_tmstp,
            ldg.last_triggering_event_type,
            ldg.partner_relationship_id,
            ldg.partner_relationship_type_code,
            ldg.dw_batch_id,
            ldg.dw_batch_date,
            current_timestamp(0),
            current_timestamp(0)
        );

ET;

/***********************************************************************************
-- Collect stats on fact tables
************************************************************************************/
COLLECT STATS ON {db_env}_NAP_FCT.INVENTORY_NAVI_STORE_RETURN_TO_VENDOR_LIFECYCLE_FACT
INDEX (return_to_vendor_num, sku_num, location_num);

ET;
