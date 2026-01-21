/***********************************************************************************
-- DELETE failed records from staging table
************************************************************************************/
LOCK TABLE {db_env}_NAP_STG.STORE_RETURN_TO_VENDOR_DETAIL_LDG FOR ACCESS
DELETE FROM {db_env}_NAP_STG.STORE_RETURN_TO_VENDOR_DETAIL_LDG
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
-- Description             : write data from STORE_RETURN_TO_VENDOR_DETAIL_LDG to STORE_RETURN_TO_VENDOR_DETAIL_FACT table
-- Reference Documentation : https://confluence.nordstrom.com/display/SCNAP/SIM+16+Store+Return+To+Vendor+Design+and+Events+Mapping
--/***********************************************************************************
---- Merge into final NAP_ASCP_STORE_RETURN_TO_VENDOR_DETAIL_FACT fact table
--************************************************************************************/
LOCK TABLE {db_env}_NAP_FCT.STORE_RETURN_TO_VENDOR_DETAIL_FACT FOR ACCESS
MERGE INTO {db_env}_NAP_FCT.STORE_RETURN_TO_VENDOR_DETAIL_FACT fact
USING (
SELECT
        return_to_vendor_num,
        sku_num,
        sku_type,
        location_num,
        created_return_authorization_num,
        created_event_id,
        CAST(concat(created_event_tmstp, '+00:00') as timestamp with time zone format 'YYYY-MM-DDBHH:MI:SS.S(F)Z') as created_event_tmstp,
        created_vendor_num,
        created_qty,
        created_disposition_code,
        created_reason_code,
        canceled_event_id,
        CAST(concat(canceled_event_tmstp, '+00:00') as timestamp with time zone format 'YYYY-MM-DDBHH:MI:SS.S(F)Z') as canceled_event_tmstp,
        canceled_qty,
        canceled_disposition_code,
        canceled_reason_code,
        shipped_event_id,
        CAST(concat(shipped_event_tmstp, '+00:00') as timestamp with time zone format 'YYYY-MM-DDBHH:MI:SS.S(F)Z') as shipped_event_tmstp,
        shipped_qty,
        shipped_disposition_code,
        CAST(concat(created_sys_tmstp, '+00:00') as timestamp with time zone format 'YYYY-MM-DDBHH:MI:SS.S(F)Z') as created_sys_tmstp,
        CAST(concat(last_updated_sys_tmstp, '+00:00') as timestamp with time zone format 'YYYY-MM-DDBHH:MI:SS.S(F)Z') as last_updated_sys_tmstp,
        last_triggering_event_type,
        (SELECT BATCH_ID FROM {db_env}_NAP_BASE_VWS.ELT_CONTROL WHERE SUBJECT_AREA_NM ='NAP_ASCP_STORE_RETURN_TO_VENDOR_DETAIL_FACT') as dw_batch_id,
        (SELECT CURR_BATCH_DATE FROM {db_env}_NAP_BASE_VWS.ELT_CONTROL WHERE SUBJECT_AREA_NM ='NAP_ASCP_STORE_RETURN_TO_VENDOR_DETAIL_FACT') as dw_batch_date
FROM (select * from {db_env}_NAP_BASE_VWS.STORE_RETURN_TO_VENDOR_DETAIL_LDG
        QUALIFY ROW_NUMBER() OVER(PARTITION BY return_to_vendor_num, sku_num, sku_type, location_num ORDER BY last_updated_sys_tmstp desc) = 1
) ldg
        ) ON fact.return_to_vendor_num = ldg.return_to_vendor_num
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
        canceled_event_tmstp = ldg.canceled_event_tmstp,
        shipped_event_tmstp = ldg.shipped_event_tmstp,
        created_vendor_num = ldg.created_vendor_num,
        created_qty = ldg.created_qty,
        canceled_qty = ldg.canceled_qty,
        shipped_qty = ldg.shipped_qty,
        created_disposition_code = ldg.created_disposition_code,
        canceled_disposition_code = ldg.canceled_disposition_code,
        shipped_disposition_code = ldg.shipped_disposition_code,
        created_reason_code = ldg.created_reason_code,
        canceled_reason_code = ldg.canceled_reason_code,
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
            created_vendor_num,
            created_qty,
            created_disposition_code,
            created_reason_code,
            canceled_event_id,
            canceled_event_tmstp,
            canceled_qty,
            canceled_disposition_code,
            canceled_reason_code,
            shipped_event_id,
            shipped_event_tmstp,
            shipped_qty,
            shipped_disposition_code,
            created_sys_tmstp,
            last_updated_sys_tmstp,
            last_triggering_event_type,
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
            ldg.created_vendor_num,
            ldg.created_qty,
            ldg.created_disposition_code,
            ldg.created_reason_code,
            ldg.canceled_event_id,
            ldg.canceled_event_tmstp,
            ldg.canceled_qty,
            ldg.canceled_disposition_code,
            ldg.canceled_reason_code,
            ldg.shipped_event_id,
            ldg.shipped_event_tmstp,
            ldg.shipped_qty,
            ldg.shipped_disposition_code,
            ldg.created_sys_tmstp,
            ldg.last_updated_sys_tmstp,
            ldg.last_triggering_event_type,
            ldg.dw_batch_id,
            ldg.dw_batch_date,
            current_timestamp(0),
            current_timestamp(0)
        );

ET;

/***********************************************************************************
-- Collect stats on fact tables
************************************************************************************/
COLLECT STATS ON {db_env}_NAP_FCT.STORE_RETURN_TO_VENDOR_DETAIL_FACT
INDEX (return_to_vendor_num, sku_num, location_num);

ET;

/***********************************************************************************
-- Perform audit between stg and fct tables
************************************************************************************/
CALL {db_env}_NAP_UTL.DQ_PIPELINE_AUDIT_V1
(
  'STORE_RETURN_TO_VENDOR_DETAIL_FACT_TO_BASE',
  'NAP_ASCP_STORE_RETURN_TO_VENDOR_DETAIL_FACT',
  '{db_env}_NAP_BASE_VWS',
  'STORE_RETURN_TO_VENDOR_DETAIL_LDG',
  '{db_env}_NAP_FCT',
  'STORE_RETURN_TO_VENDOR_DETAIL_FACT',
  'Count_Distinct',
  0,
  'T-S',
  'concat(return_to_vendor_num, ''-'',sku_num , ''-'',location_num)',
  'concat(return_to_vendor_num, ''-'',sku_num , ''-'',location_num)',
  null,
  null,
  'N'
);

ET;
