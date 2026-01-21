
--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File              : read_rms_cost_rtv_v2_from_kafka_to_teradata.sql
-- Author            : Kunal Lalwani
-- Description       : Read from Source Kakfa Topic to RMS_COST_RTV_V2_LDG tables
-- Object model      : inventory-merchandise-return-to-vendor-v2-analytical-avro
-- ETL Run Frequency : Every day
-- Version :         : 0.2
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2024-03-06 Kunal Lalwani  FA-11695   NSK Migration
-- 2024-03-11 Andrew Ivchuk  FA-11147   Adding step sending NewRelic metrics
--************************************************************************************************************************************

-- read from kafka
create temporary view top_frame as
select 
    *,
    row_number() over (partition by returnToVendorNumber order by element_at(headers, 'SystemTime') desc) as row_num
from kafka_rms_cost_rtv_v2_object_model_avro;

--dedupe
create temporary view deduped_top_frame as
select
  *
from top_frame
where row_num = 1;

-- Exploding rtvCreateDetails array
create temporary view rtv_explode as 
select
    returnToVendorNumber,
    externalReferenceNumber,
    fromLocation,
    vendorNumber,
    vendorAddress,
    returnAuthorizationNumber,
    createdSystemTime,
    lastUpdatedSystemTime,
    lastTriggeringEventType,
    explode_outer(returnToVendorCreateDetails) as detail 
from deduped_top_frame
where cardinality(returnToVendorCreateDetails) > 0;


-- Inserting into Teradata staging table
insert overwrite table rtv_v2_event_ldg_table
select
    returnToVendorNumber as return_to_vendor_number,
    externalReferenceNumber as external_reference_number,
    detail.correlationId as correlation_id,
    cast(fromLocation.facility as integer) as from_location_facility,
    cast(fromLocation.logical as integer) as from_location_logical,
    vendorNumber as vendor_number,
    vendorAddress.id as vendor_address_id,
    vendorAddress.firstName.value as vendor_address_first_name_tokenized,
    vendorAddress.lastName.value as vendor_address_last_name_tokenized,
    vendorAddress.middleName.value as vendor_address_middle_name_tokenized,
    vendorAddress.line1.value as vendor_address_line1_tokenized,
    vendorAddress.line2.value as vendor_address_line2_tokenized,
    vendorAddress.line3.value as vendor_address_line3_tokenized,
    vendorAddress.city as vendor_address_city,
    vendorAddress.state as vendor_address_state,
    vendorAddress.postalCode.value as vendor_address_postal_code_tokenized,
    vendorAddress.coarsePostalCode as vendor_address_coarse_postal_code,
    vendorAddress.countryCode as vendor_address_country_code,
    regexp_replace(returnAuthorizationNumber, '[\\r\\n]', ' ') as returns_authorization_number,
    cast(createdSystemTime as timestamp) as create_system_time,
    '-0'||extract(hours from to_utc_timestamp(current_timestamp,'UTC' )-from_utc_timestamp(current_timestamp,'America/Los_Angeles' ))||":00" as create_system_time_tz,
    cast(lastUpdatedSystemTime as timestamp) as latest_updated_system_time,
    '-0'||extract(hours from to_utc_timestamp(current_timestamp,'UTC' )-from_utc_timestamp(current_timestamp,'America/Los_Angeles' ))||":00" as latest_updated_system_time_tz,
    lastTriggeringEventType as last_triggering_event_type,
    detail.returnToVendorProductDetail.product.id as create_detail_product_id,
    cast(detail.returnToVendorProductDetail.quantity as integer) as create_detail_quantity,
    detail.returnToVendorProductDetail.disposition as create_detail_product_disposition,
    regexp_replace(detail.returnToVendorProductDetail.reasonCode, '[\\r\\n]', ' ') as create_detail_product_reason_code,
    regexp_replace(detail.comments, '[\\r\\n]', ' ') as create_comments,
    cast(detail.detailTime as timestamp) as create_detail_time,
    '-0'||extract(hours from to_utc_timestamp(current_timestamp,'UTC' )-from_utc_timestamp(current_timestamp,'America/Los_Angeles' ))||":00" as create_detail_time_tz
from rtv_explode
where (
        returnToVendorNumber is not null
        and returnToVendorNumber <> ''
        and returnToVendorNumber <> '""'
        )
    or (
        detail.correlationId is not null
        and detail.correlationId <> ''
        and detail.correlationId <> '""'
        )
    or (
        detail.returnToVendorProductDetail.product.id is not null
        and detail.returnToVendorProductDetail.product.id <> ''
        and detail.returnToVendorProductDetail.product.id <> '""'
        );


-- Exploding rtvCanceledDetails array
create temporary view rtv_canceled_details_explode as 
select
    returnToVendorNumber,
    explode_outer(returnToVendorCanceledDetails) as canceledDetail   
from deduped_top_frame 
where cardinality(returnToVendorCanceledDetails) > 0;


-- Inserting into Teradata staging table
insert overwrite table rtv_canceled_details_v2_ldg_table
select
    returnToVendorNumber as return_to_vendor_number,
    canceledDetail.correlationId as correlation_id,
    canceledDetail.returnToVendorProductDetail.product.id as canceled_detail_product_id,
    cast(canceledDetail.returnToVendorProductDetail.quantity as integer) as canceled_detail_quantity,
    canceledDetail.returnToVendorProductDetail.disposition as canceled_detail_product_disposition,
    regexp_replace(canceledDetail.returnToVendorProductDetail.reasonCode, '[\\r\\n]', ' ') as canceled_detail_product_reason_code,
    regexp_replace(canceledDetail.comments, '[\\r\\n]', ' ') as canceled_comments,
    cast(canceledDetail.detailTime as timestamp) as canceled_detail_time,
    '-0'||extract(hours from to_utc_timestamp(current_timestamp,'UTC' )-from_utc_timestamp(current_timestamp,'America/Los_Angeles' ))||":00" as canceled_detail_time_tz
from rtv_canceled_details_explode
where (
        returnToVendorNumber is not null
        and returnToVendorNumber <> ''
        and returnToVendorNumber <> '""'
        )
    or (
        canceledDetail.correlationId is not null
        and canceledDetail.correlationId <> ''
        and canceledDetail.correlationId <> '""'
        )
    or (
        canceledDetail.returnToVendorProductDetail.product.id is not null
        and canceledDetail.returnToVendorProductDetail.product.id <> ''
        and canceledDetail.returnToVendorProductDetail.product.id <> '""'
        );



-- Exploding rtvShipmentDetails array
create temporary view rtv_shipment_details_explode as 
select
    returnToVendorNumber,
    fromLocation,
    explode_outer(returnToVendorShippedDetails) shipmentDetail 
from deduped_top_frame 
where cardinality(returnToVendorShippedDetails) > 0;


-- Inserting into Teradata staging table
insert overwrite table rtv_shipment_details_v2_ldg_table
select
    returnToVendorNumber as return_to_vendor_number,
    shipmentDetail.correlationId as correlation_id,
    shipmentDetail.returnToVendorProductDetail.product.id as shipment_detail_product_id,
    cast(shipmentDetail.returnToVendorProductDetail.quantity as integer) as shipment_detail_quantity,
    shipmentDetail.returnToVendorProductDetail.disposition as shipment_detail_product_disposition,
    cast(fromLocation.facility as integer) as from_location_facility,
    cast(fromLocation.logical as integer) as from_location_logical,
    cast(shipmentDetail.detailTime as timestamp) as shipment_detail_time,
    '-0'||extract(hours from to_utc_timestamp(current_timestamp,'UTC' )-from_utc_timestamp(current_timestamp,'America/Los_Angeles' ))||":00" as shipment_detail_time_tz
from rtv_shipment_details_explode
where (
        returnToVendorNumber is not null
        and returnToVendorNumber <> ''
        and returnToVendorNumber <> '""'
        )
    or (
        shipmentDetail.correlationId is not null
        and shipmentDetail.correlationId <> ''
        and shipmentDetail.correlationId <> '""'
        )
    or (
        shipmentDetail.returnToVendorProductDetail.product.id is not null
        and shipmentDetail.returnToVendorProductDetail.product.id <> ''
        and shipmentDetail.returnToVendorProductDetail.product.id <> '""'
        );

insert into table processed_data_spark_log
select
    isf_dag_nm,
	step_nm,
	tbl_nm,
	metric_nm,
	metric_value,
	metric_tmstp
from (
       select
          '{dag_name}' as isf_dag_nm,
	      'Loading to csv' as step_nm,
	      'RMS_COST_RTV_V2_LDG' as tbl_nm,
	      'PROCESSED_ROWS_CNT' as metric_nm,
	      cast(count(*) as string) as metric_value,
		  cast(current_timestamp() as string) as metric_tmstp
        from rtv_explode
        where (
                returnToVendorNumber is not null
                and returnToVendorNumber <> ''
                and returnToVendorNumber <> '""'
                )
            or (
                detail.correlationId is not null
                and detail.correlationId <> ''
                and detail.correlationId <> '""'
                )
            or (
                detail.returnToVendorProductDetail.product.id is not null
                and detail.returnToVendorProductDetail.product.id <> ''
                and detail.returnToVendorProductDetail.product.id <> '""'
                )
	   union all
       select
          '{dag_name}' as isf_dag_nm,
	      'Loading to csv' as step_nm,
	      'RMS_COST_RTV_CANCELED_DETAILS_V2_LDG' as tbl_nm,
	      'PROCESSED_ROWS_CNT' as metric_nm,
	      cast(count(*) as string) as metric_value,
		  cast(current_timestamp() as string) as metric_tmstp
        from rtv_canceled_details_explode
        where (
                returnToVendorNumber is not null
                and returnToVendorNumber <> ''
                and returnToVendorNumber <> '""'
                )
            or (
                canceledDetail.correlationId is not null
                and canceledDetail.correlationId <> ''
                and canceledDetail.correlationId <> '""'
                )
            or (
                canceledDetail.returnToVendorProductDetail.product.id is not null
                and canceledDetail.returnToVendorProductDetail.product.id <> ''
                and canceledDetail.returnToVendorProductDetail.product.id <> '""'
                )
	   union all
       select
          '{dag_name}' as isf_dag_nm,
	      'Loading to csv' as step_nm,
	      'RMS_COST_RTV_SHIPMENT_DETAILS_V2_LDG' as tbl_nm,
	      'PROCESSED_ROWS_CNT' as metric_nm,
	      cast(count(*) as string) as metric_value,
		  cast(current_timestamp() as string) as metric_tmstp
        from rtv_shipment_details_explode
        where (
                returnToVendorNumber is not null
                and returnToVendorNumber <> ''
                and returnToVendorNumber <> '""'
                )
            or (
                shipmentDetail.correlationId is not null
                and shipmentDetail.correlationId <> ''
                and shipmentDetail.correlationId <> '""'
                )
            or (
                shipmentDetail.returnToVendorProductDetail.product.id is not null
                and shipmentDetail.returnToVendorProductDetail.product.id <> ''
                and shipmentDetail.returnToVendorProductDetail.product.id <> '""'
                )
	   union all
	   select
	      '{dag_name}' as isf_dag_nm,
	      'Reading from kafka' as step_nm,
	      cast (null as string) as tbl_nm,
	      'SPARK_PROCESSED_MESSAGE_CNT' as metric_nm,
	      cast(count(*) as string) as metric_value,
	      cast(current_timestamp() as string) as metric_tmstp
	   from kafka_rms_cost_rtv_v2_object_model_avro
	 ) a
;
