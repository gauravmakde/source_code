--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : inventory_navi_store_transfer_lifecycle/kafka_avro_to_td_stg.sql.sql
-- Description             : Reading Data from Source Kafka Topic and writing to INVENTORY_NAVI_STORE_TRANSFER_LDG table
-- Data Source             : Business events Kafka source topic "inventory-store-transfer-avro"
-- Reference Documentation : https://confluence.nordstrom.com/pages/viewpage.action?pageId=1411943258
--*************************************************************************************************************************************
-- 2024-04-16  Bohdan Sapozhnikov   FA-12213: ISF/SL - Store Transfer Lifecycle data
--*************************************************************************************************************************************

-- Source
-- Reading required Data from Source Kafka Topic Name=inventory-store-transfer-avro using SQL API CODE

----------------------------------------------------------------------------------------------------
-- Reading StoreTransferCreated data
----------------------------------------------------------------------------------------------------
create temporary view inventory_store_transfer_created as
select
      eventtype,
      eventtime,
      locationid,
      id,
      transfernumber,
      externalreferencenumber,
      systemid,
      userid,
      tolocation.facility,
      tolocation.logical,
      transfertype,
      transfercontexttype,
      comments,
      explode(transferdetails) as transferdetails
 from (select element_at(src.headers, 'EventType') as eventtype,
              src.*,
              row_number() over (partition by transfernumber, externalreferencenumber, locationid order by src.eventtime desc) as rn 
         from kafkay_store_transfer_created_event_avro src 
        where element_at(src.headers, 'Nord-Test') is null
          and element_at(src.headers, 'Nord-Load') is null
          and element_at(src.headers, 'EventType') = 'StoreTransferCreated') t
where rn = 1;

create temporary view inventory_store_transfer_created_transfer_details as
select 
      eventtype                               as event_type,
      (cast(eventtime as string) || '+00:00') as event_tmstp,
      locationid                              as location_num,
      id                                      as event_id,
      transfernumber                          as transfer_num,
      externalreferencenumber                 as external_refence_num,
      systemid                                as system_id,
      userid                                  as user_id,
      facility                                as to_location_num,
      logical                                 as to_logical_location_num,
      transfertype                            as transfer_type_code,
      transfercontexttype                     as transfer_context_type,
      comments,
      transferdetails.product.id              as sku_num,
      transferdetails.product.idtype          as sku_type_code,
      transferdetails.quantity                as sku_qty,
      transferdetails.disposition             as disposition_code
 from inventory_store_transfer_created;

----------------------------------------------------------------------------------------------------
---Writing Kafka Data to Teradata staging table: INVENTORY_NAVI_STORE_TRANSFER_LDG
----------------------------------------------------------------------------------------------------
insert into store_inventory_transfer_ldg_table (
      event_type,
      event_tmstp,
      location_num,
      event_id,
      transfer_num,
      external_refence_num,
      system_id,
      user_id,
      to_location_num,
      to_logical_location_num,
      transfer_type_code,
      transfer_context_type,
      shipment_num,
      bill_of_lading,
      carton_num,
      sku_num,
      sku_type_code,
      sku_qty,
      disposition_code,
      dw_sys_load_tmstp
)
select
      event_type,
      event_tmstp,
      location_num,
      event_id,
      transfer_num,
      external_refence_num,
      system_id,
      user_id,
      to_location_num,
      to_logical_location_num,
      transfer_type_code,
      transfer_context_type,
      cast(null AS string) as shipment_num,
      cast(null AS string) as bill_of_lading,
      cast(null AS string) as carton_num,
      sku_num,
      sku_type_code,
      sku_qty,
      disposition_code,
      current_timestamp as dw_sys_load_tmstp
 from inventory_store_transfer_created_transfer_details;
