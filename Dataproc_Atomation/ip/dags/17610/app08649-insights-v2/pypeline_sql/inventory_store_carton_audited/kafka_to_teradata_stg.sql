--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- Description             : Reading Data from Source Kafka Topic and writing to the inventory_store_carton_audited_ldg table
-- Data Source             : Order Object model kafka topic "inventory-store-carton-audited-avro"
--*************************************************************************************************************************************
SET QUERY_BAND = '
App_ID=app08649;
DAG_ID=inventory_store_carton_audited_event_13569_DAS_SC_OUTBOUND_sc_outbound_insights_framework;
Task_Name=main_job_2_load_avro_td_stg;
LoginUser={td_login_user};
Job_Name=inventory_store_carton_audited_event;
Data_Plane=Inventory;
Team_Email=TECH_NAP_SUPPLYCHAIN_OUTBOUND@nordstrom.com;
PagerDuty=NAP_Supply_Chain_Outbound;
Conn_Type=JDBC;'
FOR SESSION VOLATILE;

create or replace temporary view inventorystorecartonaudited_final as
 select id,
        eventTime,
        locationId,
        cartonNumber,
        auditUserId_idType,
        auditUserId_id,
        productDetails_product_idType,
        productDetails_product_id,
        productDetails_quantity,
        productDetails_disposition,
        auditType
   from (select 
                src.id as id
                ,(cast(src.eventTime  as string) ||'+00:00')  as eventTime
                ,src.locationId as locationId
                ,src.cartonNumber as cartonNumber
                ,src.auditUserId_idType as auditUserId_idType
                ,src.auditUserId_id as auditUserId_id
                ,src.productDetails.product.idType as productDetails_product_idType
                ,src.productDetails.product.id as productDetails_product_id
                ,src.productDetails.quantity as productDetails_quantity
                ,src.productDetails.disposition as productDetails_disposition
                ,src.auditType as auditType
                ,row_number() over (partition by src.cartonNumber, src.productDetails.product.id, src.locationId order by cast(src.eventTime as timestamp) desc) as rn
        from (select 
                        id as id
                        ,eventTime as eventTime
                        ,locationId as locationId
                        ,cartonNumber as cartonNumber
                        ,auditUserId.idType as auditUserId_idType
                        ,auditUserId.id as auditUserId_id
                        ,explode_outer (productDetails) as productDetails
                        ,auditType
                from inventory_store_carton_audited_avro ) src
        ) main_src
  where main_src.rn=1;

 -- Writing Kafka Data  to Semantic Layer LDG 2
 insert overwrite table inventory_store_carton_audited_ldg
     (  id
        ,eventTime
        ,locationId
        ,cartonNumber
        ,auditUserId_idType
        ,auditUserId_id
        ,productDetails_product_idType
        ,productDetails_product_id
        ,productDetails_quantity
        ,productDetails_disposition
        ,auditType
        ,dw_sys_load_tmstp
      )
 select id,
        eventTime,
        locationId,
        cartonNumber,
        auditUserId_idType,
        auditUserId_id,
        productDetails_product_idType,
        productDetails_product_id,
        productDetails_quantity,
        productDetails_disposition,
        auditType,
        current_timestamp() as dw_sys_load_tmstp
 from inventorystorecartonaudited_final;
