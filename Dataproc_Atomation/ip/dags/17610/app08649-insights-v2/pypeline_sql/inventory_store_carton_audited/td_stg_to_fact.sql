
--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : pypeline_sql/inventory_store_carton_audited/td_stg_to_fact.sql
-- Author                  : Spektor Stanislav
-- Description             : ETL to write Dropship shipped event data from kafka topic "customer-orderlinelifecycle-v1-avro-value" to CUSTOMER_DSCO_SHIPPED_EVENT_LDG table
-- Data Source             : INVENTORY_STORE_CARTON_AUDITED_LDG
-- ETL Run Frequency       : 
-- Reference Documentation : 
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
--*************************************************************************************************************************************

LOCK TABLE {db_env}_NAP_STG.INVENTORY_STORE_CARTON_AUDITED_LDG FOR ACCESS
-- Create temporary table to get latest data from staging table
CREATE VOLATILE MULTISET TABLE  #INVENTORY_STORE_CARTON_AUDITED_LDG
AS (SELECT * 
      FROM (
            SELECT id as audited_event_id
                , CAST(eventTime AS TIMESTAMP(6) WITH TIME ZONE) AT 'America Pacific' AS latest_inventory_store_carton_audited_event_tmstp_pacific
                , locationId as location_num
                , cartonNumber as carton_num
                , auditUserId_idType as audit_user_type_code
                , auditUserId_id as audit_user_id
                , productDetails_product_idType as product_id_type_code
                , productDetails_product_id as sku_num
                , productDetails_quantity as quantity
                , productDetails_disposition as disposition
                , auditType as audit_type_code
                , batch_id AS dw_batch_id
                , batch_date AS dw_batch_date
                , current_timestamp(0) AS dw_sys_load_tmstp
                , current_timestamp(0) AS dw_sys_updt_tmstp
              FROM {db_env}_NAP_STG.INVENTORY_STORE_CARTON_AUDITED_LDG isca
              LEFT JOIN (SELECT batch_id, batch_date 
                          FROM {db_env}_NAP_BASE_VWS.ETL_BATCH_INFO
                          WHERE interface_code = 'INVENTORY_STORE_CARTON_AUDITED_FACT' 
                            AND dw_sys_end_tmstp IS NULL) bi
                ON 1=1) stg
      WHERE NOT EXISTS (SELECT NULL 
                          FROM {db_env}_NAP_BASE_VWS.INVENTORY_STORE_CARTON_AUDITED_FACT iscaf
                         WHERE iscaf.carton_num = stg.carton_num
                           AND iscaf.sku_num = stg.sku_num
                           AND iscaf.location_num = stg.location_num
                           AND iscaf.latest_inventory_store_carton_audited_event_tmstp_pacific >= stg.latest_inventory_store_carton_audited_event_tmstp_pacific)
)
WITH DATA
PRIMARY INDEX( carton_num, sku_num, location_num )
ON COMMIT PRESERVE ROWS;
ET;

-- Collect stats for tables
COLLECT STATISTICS INDEX ( carton_num, sku_num, location_num ) ON #INVENTORY_STORE_CARTON_AUDITED_LDG;
ET;

--Merge data based
MERGE INTO {db_env}_NAP_BASE_VWS.INVENTORY_STORE_CARTON_AUDITED_FACT AS fct
USING #INVENTORY_STORE_CARTON_AUDITED_LDG AS stg
ON fct.carton_num = stg.carton_num
  AND fct.sku_num = stg.sku_num
  AND fct.location_num = stg.location_num
WHEN MATCHED
THEN
UPDATE
   SET  audited_event_id = stg.audited_event_id
      , latest_inventory_store_carton_audited_event_tmstp_pacific = stg.latest_inventory_store_carton_audited_event_tmstp_pacific
      , audit_user_type_code = stg.audit_user_type_code
      , audit_user_id = stg.audit_user_id
      , product_id_type_code = stg.product_id_type_code
      , quantity = stg.quantity
      , disposition = stg.disposition
      , audit_type_code = stg.audit_type_code
      , dw_batch_id = stg.dw_batch_id
      , dw_batch_date = stg.dw_batch_date
      , dw_sys_updt_tmstp = stg.dw_sys_updt_tmstp
WHEN NOT MATCHED 
THEN
  INSERT (audited_event_id
        ,latest_inventory_store_carton_audited_event_tmstp_pacific
        ,location_num
        ,carton_num
        ,audit_user_type_code
        ,audit_user_id
        ,product_id_type_code
        ,sku_num
        ,quantity
        ,disposition
        ,audit_type_code
        ,dw_batch_id
        ,dw_batch_date
        ,dw_sys_load_tmstp
        ,dw_sys_updt_tmstp)
  VALUES (stg.audited_event_id
        ,stg.latest_inventory_store_carton_audited_event_tmstp_pacific
        ,stg.location_num
        ,stg.carton_num
        ,stg.audit_user_type_code
        ,stg.audit_user_id
        ,stg.product_id_type_code
        ,stg.sku_num
        ,stg.quantity
        ,stg.disposition
        ,stg.audit_type_code
        ,stg.dw_batch_id
        ,stg.dw_batch_date
        ,stg.dw_sys_load_tmstp
        ,stg.dw_sys_updt_tmstp);
