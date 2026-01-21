
--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : pypeline_sql/customer_dsco_shipped_event/kafka_avro_to_td_stg.sql
-- Author                  : Bohdan Sapozhnikov
-- Description             : ETL to write Dropship shipped event data from kafka topic "customer-orderlinelifecycle-v1-avro-value" to CUSTOMER_DSCO_SHIPPED_EVENT_LDG table
-- Data Source             : OLL Object model kafka topic "customer-orderlinelifecycle-v1-avro-value
-- ETL Run Frequency       : Near Realtime (Hourly or Bi-hourly)
-- Reference Documentation : https://confluence.nordstrom.com/display/TDS/Dropship+Dynamic+Promise+Semantic+Layer
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2023-11-01 Bohdan Sapozhnikov     FA-10221: To create DSCOShipped table
--*************************************************************************************************************************************
-- SET QUERY_BAND = '
-- App_ID=app02560;
-- LoginUser={td_login_user};
-- Job_Name=sco_customer_dsco_shipped_event;
-- Data_Plane=Customer;
-- Team_Email=TECH_NAP_SUPPLYCHAIN_OUTBOUND@nordstrom.com;
-- PagerDuty=NAP_Supply_Chain_Outbound;
-- Conn_Type=JDBC;'
-- FOR SESSION VOLATILE;


--Reading Data from Source Kafka Topic Name=customer-orderlinelifecycle-v1-avro
create temporary view shipping_dtl as
select t.*
  from (select src.orderNumber as orderNumber,
               src.orderLineId as orderLineId,
               src.orderLineNumber as orderLineNumber,
               src.shippedDetails.shippingDetails.carrierManifestId as shippingDetails_carrierManifestId, 
               src.shippedDetails.shippingDetails.packageDetails.dimensions.length as shippingDetails_packageDetails_length, 
               src.shippedDetails.shippingDetails.packageDetails.dimensions.width as shippingDetails_packageDetails_width, 
               src.shippedDetails.shippingDetails.packageDetails.dimensions.height as shippingDetails_packageDetails_height, 
               src.shippedDetails.shippingDetails.packageDetails.dimensions.unit as shippingDetails_packageDetails_lengthUnit, 
               src.shippedDetails.shippingDetails.packageDetails.estimatedDeliveryDate as shippingDetails_packageDetails_estimatedDeliveryDate, 
               cast(cast(src.shippedDetails.shippingDetails.packageDetails.shipCost.units + src.shippedDetails.shippingDetails.packageDetails.shipCost.nanos/1000000000 as decimal(36,6)) as string) as shippingDetails_packageDetails_shipCost,
               src.shippedDetails.shippingDetails.packageDetails.shipCost.currencyCode as shippingDetails_packageDetails_shipCost_currencyCode, 
               src.shippedDetails.shippingDetails.packageDetails.weight as shippingDetails_packageDetails_weight, 
               src.shippedDetails.shippingDetails.packageDetails.units as shippingDetails_packageDetails_weightUnit, 
               src.shippedDetails.shippingDetails.shipCarrier as shippingDetails_shipCarrier, 
               src.shippedDetails.shippingDetails.shipFromDetails.address.line1 as shippingDetails_shipFrom_address_line1, 
               src.shippedDetails.shippingDetails.shipFromDetails.address.line2 as shippingDetails_shipFrom_address_line2, 
               src.shippedDetails.shippingDetails.shipFromDetails.address.line3 as shippingDetails_shipFrom_address_line3, 
               src.shippedDetails.shippingDetails.shipFromDetails.address.city as shippingDetails_shipFrom_address_city, 
               src.shippedDetails.shippingDetails.shipFromDetails.address.state as shippingDetails_shipFrom_address_state, 
               src.shippedDetails.shippingDetails.shipFromDetails.address.postalCode as shippingDetails_shipFrom_address_postalCode, 
               src.shippedDetails.shippingDetails.shipFromDetails.address.countryCode as shippingDetails_shipFrom_address_countryCode, 
               src.shippedDetails.shippingDetails.shipFromDetails.company as shippingDetails_shipFrom_company, 
               src.shippedDetails.shippingDetails.shipFromDetails.locationCode as shippingDetails_shipFrom_locationCode, 
               src.shippedDetails.shippingDetails.transportationMethodCode as shippingDetails_transportationMethodCode, 
               src.shippedDetails.shippingDetails.supplierId as shippingDetails_supplierId, 
              cast(src.shippedDetails.shippingDetails.shipDate as string) as shippingDetails_shipDate, 
               src.shippedDetails.shippingDetails.ssccBarcode as shippingDetails_ssccBarcode, 
               src.shippedDetails.shippingDetails.warehouseCode as shippingDetails_warehouseCode, 
               src.shippedDetails.shippingDetails.warehouseId as shippingDetails_warehouseId,
              (cast(src.shippedDetails.eventTime  as string) ||'+00:00')  as shippedDetails_eventTime,
               row_number() over (partition by orderNumber, orderLineId order by  cast(src.shippedDetails.eventTime as timestamp) desc) as rn
          from kafka_orderline_lifecycle src
         where src.shippedDetails.shippingDetails is not null) t
 where rn = 1 and orderNumber is not null;

insert overwrite table customer_dsco_shipped_event_ldg    
      ( 
        orderNumber
      , orderLineId
      , orderLineNumber
      , shippingDetails_carrierManifestId
      , shippingDetails_packageDetails_length
      , shippingDetails_packageDetails_width
      , shippingDetails_packageDetails_height
      , shippingDetails_packageDetails_lengthUnit
      , shippingDetails_packageDetails_estimatedDeliveryDate
      , shippingDetails_packageDetails_shipCost
      , shippingDetails_packageDetails_shipCost_currencyCode
      , shippingDetails_packageDetails_weight
      , shippingDetails_packageDetails_weightUnit
      , shippingDetails_shipCarrier
      , shippingDetails_shipFrom_address_line1
      , shippingDetails_shipFrom_address_line2
      , shippingDetails_shipFrom_address_line3
      , shippingDetails_shipFrom_address_city
      , shippingDetails_shipFrom_address_state
      , shippingDetails_shipFrom_address_postalCode
      , shippingDetails_shipFrom_address_countryCode
      , shippingDetails_shipFrom_company
      , shippingDetails_shipFrom_locationCode
      , shippingDetails_transportationMethodCode
      , shippingDetails_supplierId
      , shippingDetails_shipDate
      , shippingDetails_ssccBarcode
      , shippingDetails_warehouseCode
      , shippingDetails_warehouseId
      , shippedDetails_eventTime
      , dw_sys_load_tmstp
      )
SELECT 
CAST ( orderlineid AS STRING ) AS orderlineid,
CAST ( ordernumber AS STRING ) AS ordernumber,
CAST ( orderlinenumber AS STRING ) AS orderlinenumber,
CAST ( shippingdetails_carriermanifestid AS STRING ) AS shippingdetails_carriermanifestid,
CAST ( shippingdetails_packagedetails_length AS STRING ) AS shippingdetails_packagedetails_length,
CAST ( shippingdetails_packagedetails_width AS STRING ) AS shippingdetails_packagedetails_width,
CAST ( shippingdetails_packagedetails_height AS STRING ) AS shippingdetails_packagedetails_height,
CAST ( shippingdetails_packagedetails_lengthunit AS STRING ) AS shippingdetails_packagedetails_lengthunit,
CAST ( shippingdetails_packagedetails_estimateddeliverydate AS STRING ) AS shippingdetails_packagedetails_estimateddeliverydate,
CAST ( shippingdetails_packagedetails_shipcost AS STRING ) AS shippingdetails_packagedetails_shipcost,
CAST ( shippingdetails_packagedetails_shipcost_currencycode AS STRING ) AS shippingdetails_packagedetails_shipcost_currencycode,
CAST ( shippingdetails_packagedetails_weight AS STRING ) AS shippingdetails_packagedetails_weight,
CAST ( shippingdetails_packagedetails_weightunit AS STRING ) AS shippingdetails_packagedetails_weightunit,
CAST ( shippingdetails_shipcarrier AS STRING ) AS shippingdetails_shipcarrier,
CAST ( shippingdetails_shipfrom_address_line1 AS STRING ) AS shippingdetails_shipfrom_address_line1,
CAST ( shippingdetails_shipfrom_address_line2 AS STRING ) AS shippingdetails_shipfrom_address_line2,
CAST ( shippingdetails_shipfrom_address_line3 AS STRING ) AS shippingdetails_shipfrom_address_line3,
CAST ( shippingdetails_shipfrom_address_city AS STRING ) AS shippingdetails_shipfrom_address_city,
CAST ( shippingdetails_shipfrom_address_state AS STRING ) AS shippingdetails_shipfrom_address_state,
CAST ( shippingdetails_shipfrom_address_postalcode AS STRING ) AS shippingdetails_shipfrom_address_postalcode,
CAST ( shippingdetails_shipfrom_address_countrycode AS STRING ) AS shippingdetails_shipfrom_address_countrycode,
CAST ( shippingdetails_shipfrom_company AS STRING ) AS shippingdetails_shipfrom_company,
CAST ( shippingdetails_shipfrom_locationcode AS STRING ) AS shippingdetails_shipfrom_locationcode,
CAST ( shippingdetails_transportationmethodcode AS STRING ) AS shippingdetails_transportationmethodcode,
CAST ( shippingdetails_supplierid AS STRING ) AS shippingdetails_supplierid,
CAST ( shippingdetails_shipdate AS STRING ) AS shippingdetails_shipdate,
CAST ( shippingdetails_ssccbarcode AS STRING ) AS shippingdetails_ssccbarcode,
CAST ( shippingdetails_warehousecode AS STRING ) AS shippingdetails_warehousecode,
CAST ( shippingdetails_warehouseid AS STRING ) AS shippingdetails_warehouseid,
CAST ( shippeddetails_eventtime AS STRING ) AS shippeddetails_eventtime,
current_timestamp()  as dw_sys_load_tmstp 
FROM shipping_dtl;