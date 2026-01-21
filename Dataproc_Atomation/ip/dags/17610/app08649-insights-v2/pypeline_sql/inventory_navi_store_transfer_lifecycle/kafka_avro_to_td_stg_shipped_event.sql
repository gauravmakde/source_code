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
-- Reading StoreTransferShipped data
----------------------------------------------------------------------------------------------------
create temporary view inventory_store_transfer_shipped as
select
      eventtype,
      eventtime,
      locationid,
      id,
      shipmentnumber,
      tolocation.facility,
      tolocation.logical,
      billoflading,
      cartonnumber,
      explode(transfershippeddetails) as transfershippeddetails
 from (select element_at(src.headers, 'EventType') as eventtype,
              src.*,
              row_number() over (partition by shipmentnumber, cartonnumber, locationid order by src.eventtime desc) as rn 
         from kafka_store_transfer_shipped_event_avro src 
        where element_at(src.headers, 'Nord-Test') is null
          and element_at(src.headers, 'Nord-Load') is null
          and element_at(src.headers, 'EventType') = 'StoreTransferShipped') t
where rn = 1;

create temporary view inventory_store_transfer_shipped_transfer_shipped_details as
select
      eventtype,
      eventtime,
      locationid,
      id,
      shipmentnumber,
      facility,
      logical,
      billoflading,
      cartonnumber,
      transfershippeddetails.transfernumber,
      explode(transfershippeddetails.productdetails)  as productdetails
from inventory_store_transfer_shipped;

create temporary view inventory_store_transfer_shipped_product_details as
select 
      eventtype                               as event_type,
      (cast(eventtime as string) || '+00:00') as event_tmstp,
      locationid                              as location_num,
      id                                      as event_id,
      shipmentnumber                          as shipment_num,
      facility                                as to_location_num,
      logical                                 as to_logical_location_num,
      billoflading                            as bill_of_lading,
      cartonnumber                            as carton_num,
      transfernumber                          as transfer_num,
      productdetails.product.id               as sku_num,
      productdetails.product.idtype           as sku_type_code,
      productdetails.quantity                 as sku_qty,
      productdetails.disposition              as disposition_code
 from inventory_store_transfer_shipped_transfer_shipped_details;


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
      cast(null AS string) as external_refence_num,
      cast(null AS string) as system_id,
      cast(null AS string) as user_id,
      to_location_num,
      to_logical_location_num,
      cast(null AS string) as transfer_type_code,
      cast(null AS string) as transfer_context_type,
      shipment_num,
      bill_of_lading,
      carton_num,
      sku_num,
      sku_type_code,
      sku_qty,
      disposition_code,
      current_timestamp as dw_sys_load_tmstp
 from inventory_store_transfer_shipped_product_details;
