-- Query Band part without required DAG_ID
SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=alterations_load_2656_napstore_insights;
Task_Name=alterations_load_stg;'
FOR SESSION VOLATILE;

-- Reading from kafka
-- SOURCE TOPIC NAME: customer-alterationdetail-analytical-avro
create
temporary view alterations_input AS
select *
from kafka_alterations_input;

-- Writing Kafka Data to S3 Path
insert into table alterations_orc_output partition(year, month, day)
select *, current_date() as process_date, year(current_date()) as year,month(current_date()) as month,day(current_date()) as day
from alterations_input;


-- Writing Kafka to Semantic Layer:
insert
overwrite table alterations_stg_table
select
    alterationId                                    as TICKET_ID,
    alterationDetails.creationTime                  as CREATED_TS,
    eventTime                                       as LAST_MODIFIED,
    customer.id                                     as CUSTOMER_ID,
    alterationStatus                                as TICKET_STATUS,
    alterationDetails.salesPerson.id                as SALESPERSON_NUMBER,
    alterationDetails.fitSpecialist.id              as FIT_SPECIALIST_NUMBER,
    alterationDetails.fittingDateTime               as FITTING_DT,
    concat_ws('.', STRING(alterationTotalFee.units),
    STRING(alterationTotalFee.nanos))               as SUB_TOTAL,
    alterationDetails.originStore.number            as STORE_ORIGIN,
    alterationDetails.destinationStore.number       as STORE_DESTINATION,
    alterationDetails.deliveryMethod                as DELIVERY_OPTION,
    cast(decode(encode(alterationDetails.storeDeliveryDepartmentName, 'US-ASCII'), 'US-ASCII') as string)  as DELIVERY_DESTINATION
from alterations_input;


create temporary view ticket_garment_input AS
select alterationId, eventTime,
       explode_outer(garmentDetails) AS garmentDetailsExploded
from alterations_input;


insert
overwrite table ticket_garment_stg_table
Select
    alterationId                                                                    as TICKET_ID,
    garmentDetailsExploded.garmentId                                                as GARMENT_ID,
    concat_ws('.', STRING(garmentDetailsExploded.extraFee.units),
    STRING(garmentDetailsExploded.extraFee.nanos))                                  as EXTRA_FEE,
    cast(decode(encode(garmentDetailsExploded.color, 'US-ASCII'), 'US-ASCII') as string)                                                    as GARMENT_COLOR,
    cast(decode(encode(garmentDetailsExploded.description, 'US-ASCII'), 'US-ASCII') as string) as GARMENT_DESCRIPTION,
    garmentDetailsExploded.garmentSequence                                          as GARMENT_SEQUENCE,
    garmentDetailsExploded.garmentStatus                                            as GARMENT_STATUS,
    garmentDetailsExploded.garmentType                                              as GARMENT_TYPE,
    eventTime                                                                       as LAST_MODIFIED,
    garmentDetailsExploded.promiseDate                                              as PROMISE_DATE,
    garmentDetailsExploded.upc.id                                                   as GARMENT_UPC,
    garmentDetailsExploded.qualityAssurancefailureReason                            as QA_FAILED_REASON,
    garmentDetailsExploded.garmentOrigin                                            as GARMENT_ORIGIN,
    concat_ws('.', STRING(garmentDetailsExploded.garmentAlterationFee.units),
    STRING(garmentDetailsExploded.garmentAlterationFee.nanos))                      as GARMENT_ALTERATION_PRICE,
    garmentDetailsExploded.size                                                     as GARMENT_SIZE,
    cast(decode(encode(garmentDetailsExploded.brand, 'US-ASCII'), 'US-ASCII') as string) as GARMENT_BRAND,
    garmentDetailsExploded.garmentCondition                                         as GARMENT_CONDITION,
    garmentDetailsExploded.garmentPriceType                                         as PRICE_TYPE,
    concat_ws('.', STRING(garmentDetailsExploded.expeditedFee.units),
    STRING(garmentDetailsExploded.expeditedFee.nanos))                              as EXPEDITE_FEE,
    garmentDetailsExploded.promiseTime                                              as PROMISE_TIME,
    garmentDetailsExploded.expressHem                                               as EXPRESS_HEM
from ticket_garment_input
where garmentDetailsExploded.garmentId is not null;


create temporary view garment_work_details_input AS
select  alterationId as TICKET_ID,
        garmentDetailsExploded.garmentId as GARMENT_ID,
        explode_outer(garmentDetailsExploded.garmentWorkDetails) garmentWorkDetailsExploded
from ticket_garment_input;


insert
overwrite table garment_work_details_stg_table
select
    TICKET_ID,
    GARMENT_ID,
    garmentWorkDetailsExploded.garmentWorkItemId        as GARMENT_WORK_ITEM_ID,
    garmentWorkDetailsExploded.alterationCategory       as ALTERATION_CATEGORY,
    garmentWorkDetailsExploded.alterationName           as ALTERATION_NAME,
    garmentWorkDetailsExploded.minutes                  as MINUTES
from garment_work_details_input
where garmentWorkDetailsExploded.garmentWorkItemId is not null;
