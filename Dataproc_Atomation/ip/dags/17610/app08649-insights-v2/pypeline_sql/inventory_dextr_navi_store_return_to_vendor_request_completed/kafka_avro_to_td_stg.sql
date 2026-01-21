--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : pypeline_sql/inventory_dextr_navi_store_return_to_vendor_request_completed/kafka_avro_to_td_stg.sql
-- Author                  : Bohdan Sapozhnikov
-- Description             : ETL to write NAVI RTV request completed event data from kafka topic "inventory-store-rtv-avro" to 
--                           INVENTORY_DEXTR_NAVI_STORE_RETURN_TO_VENDOR_EXPIRED_ITEM_LDG  and INVENTORY_DEXTR_NAVI_STORE_RETURN_TO_VENDOR_COMPLETED_LDG tables
-- Data Source             : inventory-store-rtv-avro Kafka topic
-- ETL Run Frequency       : Daily Hourly
-- Reference Documentation : 
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2024-04-26  Bohdan Sapozhnikov   FA-12529: Store RTV new events: StoreReturnToVendorCanceled, StoreReturnToVendorUpdated, StoreReturnToVendorRequestCompleted
-- 2024-08-07  Bohdan Sapozhnikov   FA-13535: Remapping returnToVendorNumber and externalReferenceNumber columns
--*************************************************************************************************************************************
/*
ROW_NUMBER() in the preceding CREATE TABLE logic is used to identify duplicate records and select only the most recent record
*/

SET QUERY_BAND = '
App_ID=app0864960;
LoginUser={td_login_user};
Job_Name=inventory_dextr_navi_store_return_to_vendor_request_completed;
Data_Plane=StoreInventory;
Team_Email=TECH_NAP_SUPPLYCHAIN_OUTBOUND@nordstrom.com;
PagerDuty=NAP_Supply_Chain_Outbound;
Conn_Type=JDBC;'
FOR SESSION VOLATILE;


--Reading Data from Source Kafka Topic Name=inventory-store-rtv-avro
create temporary view navi_rtv_request_completed_dtl as
select t.*
  from (select 
              (cast(src.eventTime  as string) ||'+00:00')  as event_tmstp,
               src.id as event_id,
               src.requestId as request_id,
               src.shippedRtvs,
               src.expiredItemQuantity,
               row_number() over (partition by src.requestId order by  cast(src.eventTime as timestamp) desc) as rn
          from kafka_inventory_store_return_to_vendor_request_completed_avro src
         where element_at(src.headers, 'Nord-Test') is null
           and element_at(src.headers, 'Nord-Load') is null) t
 where rn = 1;


----------------------------------------------------------------
-- all dispatched RTVs within the Merch Initiated request
----------------------------------------------------------------
create temporary view navi_store_return_to_vendor_shipped_rtv_details as
select
       event_tmstp,
       event_id,
       request_id,
       explode(shippedRtvs) as shipped_rtv_detail
  from navi_rtv_request_completed_dtl;

create temporary view navi_store_return_to_vendor_details as
select
       event_tmstp,
       event_id,
       request_id,
       shipped_rtv_detail.rtvId as return_to_vendor_num,
       explode(shipped_rtv_detail.returnToVendorDetails) as return_to_vendor_detail
  from navi_store_return_to_vendor_shipped_rtv_details;

create temporary view navi_store_return_to_vendor_sku_details as
select
       event_tmstp,
       event_id,
       request_id,
       return_to_vendor_num,
       return_to_vendor_detail.product.id as sku_num,
       return_to_vendor_detail.product.idType as sku_type_code,
       return_to_vendor_detail.quantity as sku_qty, 
       return_to_vendor_detail.disposition as disposition_code
  from navi_store_return_to_vendor_details;


--------------------------------------------------------------------------------------------------
-- List of items, that were not dispatched of a part of any RTV within the Merch Initiated request
--------------------------------------------------------------------------------------------------
create temporary view navi_store_return_to_vendor_expired_item as
select
       event_tmstp,
       event_id,
       request_id,
       explode(expiredItemQuantity) as expired_item
  from navi_rtv_request_completed_dtl;

create temporary view navi_store_return_to_vendor_expired_item_dtl as
select
       event_tmstp,
       event_id,
       request_id,
       expired_item.item.id as sku_num,
       expired_item.item.idType as sku_type_code,
       expired_item.quantity as sku_qty, 
       explode_outer(expired_item.epcs) as electronic_product_code
  from navi_store_return_to_vendor_expired_item;


----------------------------------------------------------------------------------------------------
---Writing Kafka Data to Teradata staging table: INVENTORY_DEXTR_NAVI_STORE_RETURN_TO_VENDOR_COMPLETED_LDG
----------------------------------------------------------------------------------------------------
insert overwrite table inventory_dextr_navi_store_return_to_vendor_completed_ldg_table
     (event_id,
      event_tmstp,
      request_id,
      return_to_vendor_num,
      sku_num,
      sku_type_code,
      disposition_code,
      sku_qty,
      dw_sys_load_tmstp)
select 
      event_id,
      event_tmstp,
      request_id,
      return_to_vendor_num,
      sku_num,
      sku_type_code,
      disposition_code,
      sku_qty, 
      current_timestamp as dw_sys_load_tmstp
 from navi_store_return_to_vendor_sku_details;

-------------------------------------------------------------------------------------------------------
---Writing Kafka Data to Teradata staging table: INVENTORY_DEXTR_NAVI_STORE_RETURN_TO_VENDOR_EXPIRED_ITEM_LDG
-------------------------------------------------------------------------------------------------------
insert overwrite table inventory_dextr_navi_store_return_to_vendor_expired_item_ldg_table
     (event_id,
      event_tmstp,
      request_id,
      sku_num,
      sku_type_code,
      sku_qty,
      electronic_product_code,
      dw_sys_load_tmstp)
select 
      event_id,
      event_tmstp,
      request_id,
      sku_num,
      sku_type_code,
      sku_qty, 
      electronic_product_code,
      current_timestamp as dw_sys_load_tmstp
 from navi_store_return_to_vendor_expired_item_dtl;
