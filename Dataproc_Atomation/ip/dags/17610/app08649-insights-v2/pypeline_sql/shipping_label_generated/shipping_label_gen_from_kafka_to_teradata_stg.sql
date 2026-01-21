--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : shipping_label_gen_from_kafka_to_teradata_stg.sql
-- Description             : Reading Data from Source Kafka Topic and writing to INVENTORY_STORE_NON_CUSTOMER_ORDER_SHIPPING_LABEL_GENERATED_LDG table
-- Data Source             : Order Object model kafka topic "inventory-supply-chain-delivery-non-customer-order-shipping-label-generated-avro"
--*************************************************************************************************************************************
SET QUERY_BAND = '
App_ID=app08649;
DAG_ID=non_customer_order_shipping_label_gen_event_13569_DAS_SC_OUTBOUND_sc_outbound_insights_framework;
Task_Name=main_job_2_load_avro_td_stg;
LoginUser={td_login_user};
Job_Name=non_customer_order_shipping_label_gen_event;
Data_Plane=Inventory;
Team_Email=TECH_NAP_SUPPLYCHAIN_OUTBOUND@nordstrom.com;
PagerDuty=NAP_Supply_Chain_Outbound;
Conn_Type=JDBC;'
FOR SESSION VOLATILE;

create
temporary view order_shipping_label_gen_input AS
select *
from kafka_order_shipping_label_gen_input;

-- Writing Kafka to Semantic Layer:

create temporary view order_shipping_label_gen_vw as
select eventTime,
        timestamp_millis(cast(element_at(headers, 'SystemTime') as BIGINT)) as kafkaTimestamp,
        shippingNode.locationType as shippingNode_locationType,
        shippingNode.id as shippingNode_id,
        user.type as user_type,
        user.systemId as user_systemId,
        user.employee.idType as user_employee_idType,
        user.employee.id as user_employee_id,
        user.externalUser.type as user_externalUser_type,
        user.externalUser.id.value as user_externalUser_id_value,
        user.externalUser.id.authority as user_externalUser_id_authority,
        user.externalUser.id.strategy as user_externalUser_id_strategy,
        user.externalUser.id.dataClassification as user_externalUser_id_dataClassification,
        shipmentType,
        vendorNumber,
        partnerRelationship.id as partnerRelationship_id,
        partnerRelationship.type as partnerRelationship_type,
        cartonNumber,
        plannedShipDate,
        recipientFullName,
        shipToAddress.line1 as shipToAddress_line1,
        shipToAddress.line2 as shipToAddress_line2,
        shipToAddress.line3 as shipToAddress_line3,
        shipToAddress.city as shipToAddress_city,
        shipToAddress.state as shipToAddress_state,
        shipToAddress.postalCode as shipToAddress_postalCode,
        shipToAddress.countryCode as shipToAddress_countryCode,
        shippingCharge.currencyCode as shippingCharge_currencyCode,
        shippingCharge.units as shippingCharge_units,
        shippingCharge.nanos as shippingCharge_nanos,
        estimatedDeliveryDate,
        shippingLabelDetails.scac as shippingLabelDetails_scac,
        shippingLabelDetails.trackingNumber as shippingLabelDetails_trackingNumber,
        sourceSystem,
        explode(itemDetails) as itemDetails
from order_shipping_label_gen_input;

create temporary view order_shipping_label_gen_item_details_vw as
select cast(eventTime as string) || '+00:00' as eventTime,
               kafkaTimestamp,
               shippingNode_locationType,
               shippingNode_id,
               user_type,
               user_systemId,
               user_employee_idType,
               user_employee_id,
               user_externalUser_type,
               user_externalUser_id_value,
               user_externalUser_id_authority,
               user_externalUser_id_strategy,
               user_externalUser_id_dataClassification,
               shipmentType,
               vendorNumber,
               partnerRelationship_id,
               partnerRelationship_type,
               cartonNumber,
               plannedShipDate,
               recipientFullName,
               shipToAddress_line1,
               shipToAddress_line2,
               shipToAddress_line3,
               shipToAddress_city,
               shipToAddress_state,
               shipToAddress_postalCode,
               shipToAddress_countryCode,
               shippingCharge_currencyCode,
               shippingCharge_units,
               shippingCharge_nanos,
               estimatedDeliveryDate,
               shippingLabelDetails_scac,
               shippingLabelDetails_trackingNumber,
               sourceSystem,
               itemDetails.productSku.id as itemDetails_productSku_id,
               itemDetails.productSku.idType as itemDetails_productSku_idType,
               itemDetails.quantity as itemDetails_quantity,
               itemDetails.multiplier as itemDetails_multiplier
from order_shipping_label_gen_vw;

insert
overwrite table order_shipping_label_gen_stg_table
select eventTime,
               kafkaTimestamp,
               shippingNode_locationType,
               shippingNode_id,
               user_type,
               user_systemId,
               user_employee_idType,
               user_employee_id,
               user_externalUser_type,
               user_externalUser_id_value,
               user_externalUser_id_authority,
               user_externalUser_id_strategy,
               user_externalUser_id_dataClassification,
               shipmentType,
               vendorNumber,
               partnerRelationship_id,
               partnerRelationship_type,
               cartonNumber,
               plannedShipDate,
               recipientFullName,
               shipToAddress_line1,
               shipToAddress_line2,
               shipToAddress_line3,
               shipToAddress_city,
               shipToAddress_state,
               shipToAddress_postalCode,
               shipToAddress_countryCode,
               shippingCharge_currencyCode,
               CAST(CAST(shippingCharge_units AS INTEGER) + (CAST(shippingCharge_nanos AS DECIMAL(38, 9)) / 1000000000) AS DECIMAL(38, 9)) as shippingCharge_amount,
               shippingCharge_units,
               shippingCharge_nanos,
               estimatedDeliveryDate,
               shippingLabelDetails_scac,
               shippingLabelDetails_trackingNumber,
               sourceSystem,
               itemDetails_productSku_id,
               itemDetails_productSku_idType,
               itemDetails_quantity,
               itemDetails_multiplier
from order_shipping_label_gen_item_details_vw;

