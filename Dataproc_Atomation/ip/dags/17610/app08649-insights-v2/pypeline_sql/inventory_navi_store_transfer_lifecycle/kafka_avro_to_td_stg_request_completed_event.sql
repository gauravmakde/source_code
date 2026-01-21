--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : inventory_navi_store_transfer_lifecycle/kafka_avro_to_td_stg.sql.sql
-- Description             : Reading Data from Source Kafka Topic and writing to
--                           INVENTORY_NAVI_STORE_TRANSFER_COMPLETED_LDG,
--                           INVENTORY_NAVI_STORE_TRANSFER_EXPIRED_ITEM_LDG tables
-- Data Source             : Business events Kafka source topic "inventory-store-transfer-avro"
-- Reference Documentation : https://confluence.nordstrom.com/pages/viewpage.action?pageId=1411943258
--*************************************************************************************************************************************
-- 2024-04-16  Bohdan Sapozhnikov   FA-12213: ISF/SL - Store Transfer Lifecycle data
--*************************************************************************************************************************************

-- Source
-- Reading required Data from Source Kafka Topic Name=inventory-store-transfer-avro using SQL API CODE

----------------------------------------------------------------------------------------------------
-- Reading StoreTransferRequestCompleted data
----------------------------------------------------------------------------------------------------

create temporary view inventory_store_transfer_request_completed_src as
select *
  from (select
              element_at(src.headers, 'EventType') as eventtype,
              src.*,
              row_number() over (partition by requestid order by src.eventtime desc) as rn
         from kafka_store_transfer_request_completed_event_avro src
        where element_at(src.headers, 'Nord-Test') is null
          and element_at(src.headers, 'Nord-Load') is null
          and element_at(src.headers, 'EventType') = 'StoreTransferRequestCompleted') t
where rn = 1;

create temporary view inventory_store_transfer_request_completed as
select
      eventtype,
      eventtime,
      id,
      requestid,
      explode(shippedtransfers) as shippedtransfers
 from inventory_store_transfer_request_completed_src;

create temporary view inventory_store_transfer_request_completed_shipped_transfers_details as
select
      eventtype,
      eventtime,
      id,
      requestid,
      shippedtransfers.transfernumber,
      explode(shippedtransfers.productdetails) as productdetails
 from inventory_store_transfer_request_completed;

create temporary view inventory_store_transfer_request_completed_product_details as
select 
      eventtype,
      (cast(eventtime as string) || '+00:00') as event_tmstp,
      id                                      as event_id,
      requestid                               as request_id,
      transfernumber                          as transfer_num,
      productdetails.product.id               as sku_num,
      productdetails.product.idtype           as sku_type_code,
      productdetails.quantity                 as sku_qty,
      productdetails.disposition              as disposition_code
 from inventory_store_transfer_request_completed_shipped_transfers_details;

create temporary view inventory_store_transfer_request_completed_expired_items as
select
      eventtype,
      eventtime,
      id,
      requestid,
      explode(expireditemquantity) as expireditems
 from inventory_store_transfer_request_completed_src;

create temporary view inventory_store_transfer_request_completed_expired_items_details as
select
      (cast(eventtime as string) || '+00:00') as event_tmstp,
      id                                      as event_id,
      requestid                               as request_id,
      expireditems.item.id                    as sku_num,
      expireditems.item.idtype                as sku_type_code,
      expireditems.quantity                   as sku_qty,
      explode_outer(expireditems.epcs)        as electronic_product_code
 from inventory_store_transfer_request_completed_expired_items;

----------------------------------------------------------------------------------------------------
-- Writing Kafka Data to Teradata staging table: INVENTORY_NAVI_STORE_TRANSFER_COMPLETED_LDG
----------------------------------------------------------------------------------------------------
insert overwrite table store_inventory_transfer_completed_ldg_table
      (event_tmstp,
      event_id,
      request_id,
      transfer_num,
      sku_num,
      sku_type_code,
      sku_qty,
      disposition_code,
      dw_sys_load_tmstp)
select
      event_tmstp,
      event_id,
      request_id,
      transfer_num,
      sku_num,
      sku_type_code,
      sku_qty,
      disposition_code,
      current_timestamp as dw_sys_load_tmstp
 from inventory_store_transfer_request_completed_product_details;

----------------------------------------------------------------------------------------------------
-- Writing Kafka Data to Teradata staging table: INVENTORY_NAVI_STORE_TRANSFER_EXPIRED_ITEM_LDG
----------------------------------------------------------------------------------------------------
insert overwrite table store_inventory_transfer_expired_item_ldg_table
      (event_tmstp,
      event_id,
      request_id,
      sku_num,
      sku_type_code,
      sku_qty,
      electronic_product_code,
      dw_sys_load_tmstp)
select
      event_tmstp,
      event_id,
      request_id,
      sku_num,
      sku_type_code,
      sku_qty,
      electronic_product_code,
      current_timestamp as dw_sys_load_tmstp
 from inventory_store_transfer_request_completed_expired_items_details;
