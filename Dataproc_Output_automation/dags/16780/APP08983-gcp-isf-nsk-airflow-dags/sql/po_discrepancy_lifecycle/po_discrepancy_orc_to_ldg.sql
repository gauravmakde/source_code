--  read from ORC
CREATE TEMPORARY TABLE po_discrepancy
  AS
    SELECT
        *
      FROM
        scoi.po_discrepancy_lifecycle
      WHERE po_discrepancy_lifecycle.batch_id = 'SQL.PARAM.AIRFLOW_RUN_ID'
;
--  purchaseOrderShipmentDiscrepancyAcceptedDetails
CREATE TEMPORARY TABLE accepteddetails_exploded_base
  AS
    SELECT
        po_discrepancy.exceptionid AS exception_id,
        'PurchaseOrderShipmentDiscrepancyAccepted' AS event_name,
        explode(po_discrepancy.purchaseordershipmentdiscrepancyaccepteddetails) AS accepteddetails,
        po_discrepancy.batch_id
      FROM
        po_discrepancy
;
--  purchaseOrderShipmentDiscrepancyIdentifiedClosedPurchaseOrderDetails
CREATE TEMPORARY TABLE identifiedclosedpurchaseorderdetails_exploded_base
  AS
    SELECT
        po_discrepancy.exceptionid AS exception_id,
        'PurchaseOrderShipmentDiscrepancyIdentifiedClosedPurchaseOrder' AS event_name,
        explode(po_discrepancy.purchaseordershipmentdiscrepancyidentifiedclosedpurchaseorderdetails) AS identifiedclosedpurchaseorderdetails,
        po_discrepancy.batch_id
      FROM
        po_discrepancy
;
--  purchaseOrderShipmentDiscrepancyIdentifiedItemNotOrderedDetails
CREATE TEMPORARY TABLE identifieditemnotordereddetails_exploded_base
  AS
    SELECT
        po_discrepancy.exceptionid AS exception_id,
        'PurchaseOrderShipmentDiscrepancyIdentifiedItemNotOrdered' AS event_name,
        explode(po_discrepancy.purchaseordershipmentdiscrepancyidentifieditemnotordereddetails) AS identifieditemnotordereddetails,
        po_discrepancy.batch_id
      FROM
        po_discrepancy
;
--  purchaseOrderShipmentDiscrepancyIdentifiedShipWindowViolationDetails
CREATE TEMPORARY TABLE identifiedshipwindowviolationdetails_exploded_base
  AS
    SELECT
        po_discrepancy.exceptionid AS exception_id,
        'PurchaseOrderShipmentDiscrepancyIdentifiedShipWindowViolation' AS event_name,
        explode(po_discrepancy.purchaseordershipmentdiscrepancyidentifiedshipwindowviolationdetails) AS identifiedshipwindowviolationdetails,
        po_discrepancy.batch_id
      FROM
        po_discrepancy
;
--  purchaseOrderShipmentDiscrepancyIdentifiedStoreNotOnPurchaseOrderDetails
CREATE TEMPORARY TABLE identifiedstorenotonpurchaseorderdetails_exploded_base
  AS
    SELECT
        po_discrepancy.exceptionid AS exception_id,
        'PurchaseOrderShipmentDiscrepancyIdentifiedStoreNotOnPurchaseOrder' AS event_name,
        explode(po_discrepancy.purchaseordershipmentdiscrepancyidentifiedstorenotonpurchaseorderdetails) AS identifiedstorenotonpurchaseorderdetails,
        po_discrepancy.batch_id
      FROM
        po_discrepancy
;
-- 
CREATE TEMPORARY TABLE identifiedstorenotonpurchaseorderdetails_exploded_base_details
  AS
    SELECT
        identifiedstorenotonpurchaseorderdetails_exploded_base.exception_id,
        identifiedstorenotonpurchaseorderdetails_exploded_base.event_name,
        identifiedstorenotonpurchaseorderdetails_exploded_base.identifiedstorenotonpurchaseorderdetails,
        explode_outer(identifiedstorenotonpurchaseorderdetails.actualdistributelocations) AS actualdistributelocations,
        identifiedstorenotonpurchaseorderdetails_exploded_base.batch_id
      FROM
        identifiedstorenotonpurchaseorderdetails_exploded_base
;
--  purchaseOrderShipmentDiscrepancyRefusedDetails
CREATE TEMPORARY TABLE refuseddetails_exploded_base
  AS
    SELECT
        po_discrepancy.exceptionid AS exception_id,
        'PurchaseOrderShipmentDiscrepancyRefused' AS event_name,
        explode(po_discrepancy.purchaseordershipmentdiscrepancyrefuseddetails) AS refuseddetails,
        po_discrepancy.batch_id
      FROM
        po_discrepancy
;
--  PurchaseOrderShipmentDiscrepancyIdentifiedMisShipDetails
CREATE TEMPORARY TABLE misshipdetails_exploded_base
  AS
    SELECT
        po_discrepancy.exceptionid AS exception_id,
        'PurchaseOrderShipmentDiscrepancyIdentifiedMisShip' AS event_name,
        explode(po_discrepancy.purchaseordershipmentdiscrepancyidentifiedmisshipdetails) AS misshipdetails,
        po_discrepancy.batch_id
      FROM
        po_discrepancy
;
-- 
CREATE TEMPORARY TABLE misshipdetails_exploded_base_details
  AS
    SELECT
        misshipdetails_exploded_base.exception_id,
        misshipdetails_exploded_base.event_name,
        misshipdetails_exploded_base.misshipdetails,
        explode_outer(misshipdetails.expectedshiplocations) AS expectedshiplocations,
        misshipdetails_exploded_base.batch_id
      FROM
        misshipdetails_exploded_base
;
--  PurchaseOrderShipmentDiscrepancyIdentifiedMissingPurchaseOrderDetails
CREATE TEMPORARY TABLE missingpurchaseorderdetails_exploded_base
  AS
    SELECT
        po_discrepancy.exceptionid AS exception_id,
        'PurchaseOrderShipmentDiscrepancyIdentifiedMissingPurchaseOrder' AS event_name,
        explode(po_discrepancy.purchaseordershipmentdiscrepancyidentifiedmissingpurchaseorderdetails) AS missingpurchaseorderdetails,
        po_discrepancy.batch_id
      FROM
        po_discrepancy
;
--  PurchaseOrderShipmentDiscrepancyIdentifiedOverageDetails
CREATE TEMPORARY TABLE overagedetails_exploded_base
  AS
    SELECT
        po_discrepancy.exceptionid AS exception_id,
        'PurchaseOrderShipmentDiscrepancyIdentifiedOverage' AS event_name,
        explode(po_discrepancy.purchaseordershipmentdiscrepancyidentifiedoveragedetails) AS overagedetails,
        po_discrepancy.batch_id
      FROM
        po_discrepancy
;
--  PurchaseOrderShipmentDiscrepancyIdentifiedBulkStoreMismatchDetails
CREATE TEMPORARY TABLE bulkstoremismatchdetails_exploded_base
  AS
    SELECT
        po_discrepancy.exceptionid AS exception_id,
        'PurchaseOrderShipmentDiscrepancyIdentifiedBulkStoreMismatch' AS event_name,
        explode(po_discrepancy.purchaseordershipmentdiscrepancyidentifiedbulkstoremismatchdetails) AS bulkstoremismatchdetails,
        po_discrepancy.batch_id
      FROM
        po_discrepancy
;
-- 
CREATE TEMPORARY TABLE bulkstoremismatchdetails_exploded_base_details
  AS
    SELECT
        bulkstoremismatchdetails_exploded_base.exception_id,
        bulkstoremismatchdetails_exploded_base.event_name,
        bulkstoremismatchdetails_exploded_base.bulkstoremismatchdetails,
        explode_outer(bulkstoremismatchdetails.actualdistributelocations) AS actualdistributelocations,
        bulkstoremismatchdetails_exploded_base.batch_id
      FROM
        bulkstoremismatchdetails_exploded_base
;
--  PurchaseOrderShipmentDiscrepancyIdentifiedSupplierDepartmentMismatchDetails
CREATE TEMPORARY TABLE supplierdepartmentmismatchdetails_exploded_base
  AS
    SELECT
        po_discrepancy.exceptionid AS exception_id,
        'PurchaseOrderShipmentDiscrepancyIdentifiedSupplierDepartmentMismatch' AS event_name,
        explode(po_discrepancy.purchaseordershipmentdiscrepancyidentifiedsupplierdepartmentmismatchdetails) AS supplierdepartmentmismatchdetails,
        po_discrepancy.batch_id
      FROM
        po_discrepancy
;
--  PurchaseOrderShipmentDiscrepancyIdentifiedVirtualStoreMismatchDetails
CREATE TEMPORARY TABLE virtualstoremismatchdetails_exploded_base
  AS
    SELECT
        po_discrepancy.exceptionid AS exception_id,
        'PurchaseOrderShipmentDiscrepancyIdentifiedVirtualStoreMismatch' AS event_name,
        explode(po_discrepancy.purchaseordershipmentdiscrepancyidentifiedvirtualstoremismatchdetails) AS virtualstoremismatchdetails,
        po_discrepancy.batch_id
      FROM
        po_discrepancy
;
-- 
CREATE TEMPORARY TABLE virtualstoremismatchdetails_exploded_base_details
  AS
    SELECT
        virtualstoremismatchdetails_exploded_base.exception_id,
        virtualstoremismatchdetails_exploded_base.event_name,
        virtualstoremismatchdetails_exploded_base.virtualstoremismatchdetails,
        explode_outer(virtualstoremismatchdetails.actualdistributelocations) AS actualdistributelocations,
        virtualstoremismatchdetails_exploded_base.batch_id
      FROM
        virtualstoremismatchdetails_exploded_base
;
CREATE TEMPORARY TABLE po_discrepancy_temp
  AS
    SELECT
        -- purchaseOrderShipmentDiscrepancyAcceptedDetails
        accepteddetails_exploded_base.exception_id,
        accepteddetails.vendornumber AS vendor_number,
        accepteddetails_exploded_base.event_name,
        format_datetime('%Y-%m-%d %H:%M:%E3Sxxx', accepteddetails.eventtime) AS event_time,
        substr(CAST(datetime(timestamp_millis(accepteddetails.eventtime), 'America/Los_Angeles') as STRING), 0, 10) AS event_date_pacific,
        datetime(timestamp_millis(accepteddetails.eventtime), 'America/Los_Angeles') AS event_datetime_pacific,
        --  common to all except PurchaseOrderShipmentDiscrepancyIdentifiedMissingPurchaseOrderDetails
        accepteddetails.externalpurchaseorderid AS external_purchase_order_id,
        accepteddetails.purchaseorderid AS purchase_order_id,
        --  item details required for some events
        CAST(NULL as STRING) AS item_detail_rms_sku_id,
        CAST(NULL as STRING) AS item_detail_upc_number,
        CAST(NULL as INT64) AS item_detail_units_in_discrepancy,
        --  additional columns in one or more events
        accepteddetails.crossreferencermspurchaseorderid AS cross_reference_rms_purchase_order_id,
        accepteddetails.unitsaccepted AS units_accepted,
        CAST(NULL as STRING) AS point_of_identification,
        CAST(NULL as STRING) AS discrepancy_identification_source_system,
        CAST(NULL as STRING) AS advanced_shipment_notice_id,
        CAST(NULL as STRING) AS vendor_bill_of_landing,
        CAST(NULL as STRING) AS freight_bill_id,
        CAST(NULL as STRING) AS receiving_location_id,
        CAST(NULL as STRING) AS actual_ship_location_id,
        CAST(NULL as STRING) AS expected_ship_locations,
        CAST(NULL as STRING) AS actual_purchase_order_id,
        CAST(NULL as DATE) AS actual_vendor_ship_date,
        CAST(NULL as STRING) AS actual_distribute_locations,
        CAST(NULL as INT64) AS units_refused,
        CAST(NULL as STRING) AS actual_pack_type,
        CAST(NULL as STRING) AS actual_vendor_number,
        --  audit fields
        accepteddetails_exploded_base.batch_id AS dw_batch_id
      FROM
        accepteddetails_exploded_base
      WHERE accepteddetails_exploded_base.accepteddetails IS NOT NULL
    UNION ALL
    SELECT
        -- purchaseOrderShipmentDiscrepancyIdentifiedClosedPurchaseOrderDetails
        identifiedclosedpurchaseorderdetails_exploded_base.exception_id,
        identifiedclosedpurchaseorderdetails.vendornumber AS vendor_number,
        identifiedclosedpurchaseorderdetails_exploded_base.event_name,
        format_datetime('%Y-%m-%d %H:%M:%E3Sxxx', identifiedclosedpurchaseorderdetails.eventtime) AS event_time,
        substr(CAST(datetime(timestamp_millis(identifiedclosedpurchaseorderdetails.eventtime), 'America/Los_Angeles') as STRING), 0, 10) AS event_date_pacific,
        datetime(timestamp_millis(identifiedclosedpurchaseorderdetails.eventtime), 'America/Los_Angeles') AS event_datetime_pacific,
        --  common to all except PurchaseOrderShipmentDiscrepancyIdentifiedMissingPurchaseOrderDetails
        identifiedclosedpurchaseorderdetails.externalpurchaseorderid AS external_purchase_order_id,
        identifiedclosedpurchaseorderdetails.purchaseorderid AS purchase_order_id,
        --  item details required for some events
        itemdetail.rmsskuid AS item_detail_rms_sku_id,
        itemdetail.upc AS item_detail_upc_number,
        itemdetail.unitsindiscrepancy AS item_detail_units_in_discrepancy,
        --  additional columns in one or more events
        CAST(NULL as STRING) AS cross_reference_rms_purchase_order_id,
        CAST(NULL as INT64) AS units_accepted,
        identifiedclosedpurchaseorderdetails.pointofidentification AS point_of_identification,
        identifiedclosedpurchaseorderdetails.discrepancyidentificationsourcesystem AS discrepancy_identification_source_system,
        identifiedclosedpurchaseorderdetails.advancedshipmentnoticeid AS advanced_shipment_notice_id,
        identifiedclosedpurchaseorderdetails.vendorbilloflading AS vendor_bill_of_landing,
        identifiedclosedpurchaseorderdetails.freightbillid AS freight_bill_id,
        identifiedclosedpurchaseorderdetails.receivinglocationid AS receiving_location_id,
        CAST(NULL as STRING) AS actual_ship_location_id,
        CAST(NULL as STRING) AS expected_ship_locations,
        CAST(NULL as STRING) AS actual_purchase_order_id,
        CAST(NULL as DATE) AS actual_vendor_ship_date,
        CAST(NULL as STRING) AS actual_distribute_locations,
        CAST(NULL as INT64) AS units_refused,
        CAST(NULL as STRING) AS actual_pack_type,
        CAST(NULL as STRING) AS actual_vendor_number,
        --  audit fields
        identifiedclosedpurchaseorderdetails_exploded_base.batch_id AS dw_batch_id
      FROM
        identifiedclosedpurchaseorderdetails_exploded_base
      WHERE identifiedclosedpurchaseorderdetails_exploded_base.identifiedclosedpurchaseorderdetails IS NOT NULL
    UNION ALL
    SELECT
        --  purchaseOrderShipmentDiscrepancyIdentifiedItemNotOrderedDetails
        identifieditemnotordereddetails_exploded_base.exception_id,
        identifieditemnotordereddetails.vendornumber AS vendor_number,
        identifieditemnotordereddetails_exploded_base.event_name,
        format_datetime('%Y-%m-%d %H:%M:%E3Sxxx', identifieditemnotordereddetails.eventtime) AS event_time,
        substr(CAST(datetime(timestamp_millis(identifieditemnotordereddetails.eventtime), 'America/Los_Angeles') as STRING), 0, 10) AS event_date_pacific,
        datetime(timestamp_millis(identifieditemnotordereddetails.eventtime), 'America/Los_Angeles') AS event_datetime_pacific,
        --  common to all except PurchaseOrderShipmentDiscrepancyIdentifiedMissingPurchaseOrderDetails
        identifieditemnotordereddetails.externalpurchaseorderid AS external_purchase_order_id,
        identifieditemnotordereddetails.purchaseorderid AS purchase_order_id,
        --  item details required for some events
        itemdetail.rmsskuid AS item_detail_rms_sku_id,
        itemdetail.upc AS item_detail_upc_number,
        itemdetail.unitsindiscrepancy AS item_detail_units_in_discrepancy,
        --  additional columns in one or more events
        CAST(NULL as STRING) AS cross_reference_rms_purchase_order_id,
        CAST(NULL as INT64) AS units_accepted,
        identifieditemnotordereddetails.pointofidentification AS point_of_identification,
        identifieditemnotordereddetails.discrepancyidentificationsourcesystem AS discrepancy_identification_source_system,
        identifieditemnotordereddetails.advancedshipmentnoticeid AS advanced_shipment_notice_id,
        CAST(NULL as STRING) AS vendor_bill_of_landing,
        CAST(NULL as STRING) AS freight_bill_id,
        identifieditemnotordereddetails.receivinglocationid AS receiving_location_id,
        CAST(NULL as STRING) AS actual_ship_location_id,
        CAST(NULL as STRING) AS expected_ship_locations,
        CAST(NULL as STRING) AS actual_purchase_order_id,
        CAST(NULL as DATE) AS actual_vendor_ship_date,
        CAST(NULL as STRING) AS actual_distribute_locations,
        CAST(NULL as INT64) AS units_refused,
        CAST(NULL as STRING) AS actual_pack_type,
        CAST(NULL as STRING) AS actual_vendor_number,
        --  audit fields
        identifieditemnotordereddetails_exploded_base.batch_id AS dw_batch_id
      FROM
        identifieditemnotordereddetails_exploded_base
      WHERE identifieditemnotordereddetails_exploded_base.identifieditemnotordereddetails IS NOT NULL
    UNION ALL
    SELECT
        --  purchaseOrderShipmentDiscrepancyIdentifiedShipWindowViolationDetails
        identifiedshipwindowviolationdetails_exploded_base.exception_id,
        identifiedshipwindowviolationdetails.vendornumber AS vendor_number,
        identifiedshipwindowviolationdetails_exploded_base.event_name,
        format_datetime('%Y-%m-%d %H:%M:%E3Sxxx', identifiedshipwindowviolationdetails.eventtime) AS event_time,
        substr(CAST(datetime(timestamp_millis(identifiedshipwindowviolationdetails.eventtime), 'America/Los_Angeles') as STRING), 0, 10) AS event_date_pacific,
        datetime(timestamp_millis(identifiedshipwindowviolationdetails.eventtime), 'America/Los_Angeles') AS event_datetime_pacific,
        --  common to all except PurchaseOrderShipmentDiscrepancyIdentifiedMissingPurchaseOrderDetails
        identifiedshipwindowviolationdetails.externalpurchaseorderid AS external_purchase_order_id,
        identifiedshipwindowviolationdetails.purchaseorderid AS purchase_order_id,
        --  item details required for some events
        itemdetail.rmsskuid AS item_detail_rms_sku_id,
        itemdetail.upc AS item_detail_upc_number,
        itemdetail.unitsindiscrepancy AS item_detail_units_in_discrepancy,
        --  additional columns in one or more events
        CAST(NULL as STRING) AS cross_reference_rms_purchase_order_id,
        CAST(NULL as INT64) AS units_accepted,
        identifiedshipwindowviolationdetails.pointofidentification AS point_of_identification,
        identifiedshipwindowviolationdetails.discrepancyidentificationsourcesystem AS discrepancy_identification_source_system,
        identifiedshipwindowviolationdetails.advancedshipmentnoticeid AS advanced_shipment_notice_id,
        CAST(NULL as STRING) AS vendor_bill_of_landing,
        CAST(NULL as STRING) AS freight_bill_id,
        identifiedshipwindowviolationdetails.receivinglocationid AS receiving_location_id,
        CAST(NULL as STRING) AS actual_ship_location_id,
        CAST(NULL as STRING) AS expected_ship_locations,
        CAST(NULL as STRING) AS actual_purchase_order_id,
        identifiedshipwindowviolationdetails.actualvendorshipdate AS actual_vendor_ship_date,
        CAST(NULL as STRING) AS actual_distribute_locations,
        CAST(NULL as INT64) AS units_refused,
        CAST(NULL as STRING) AS actual_pack_type,
        CAST(NULL as STRING) AS actual_vendor_number,
        --  audit fields
        identifiedshipwindowviolationdetails_exploded_base.batch_id AS dw_batch_id
      FROM
        identifiedshipwindowviolationdetails_exploded_base
      WHERE identifiedshipwindowviolationdetails_exploded_base.identifiedshipwindowviolationdetails IS NOT NULL
    UNION ALL
    SELECT
        --  purchaseOrderShipmentDiscrepancyIdentifiedStoreNotOnPurchaseOrderDetails
        identifiedstorenotonpurchaseorderdetails_exploded_base_details.exception_id,
        identifiedstorenotonpurchaseorderdetails.vendornumber AS vendor_number,
        identifiedstorenotonpurchaseorderdetails_exploded_base_details.event_name,
        format_datetime('%Y-%m-%d %H:%M:%E3Sxxx', identifiedstorenotonpurchaseorderdetails.eventtime) AS event_time,
        substr(CAST(datetime(timestamp_millis(identifiedstorenotonpurchaseorderdetails.eventtime), 'America/Los_Angeles') as STRING), 0, 10) AS event_date_pacific,
        datetime(timestamp_millis(identifiedstorenotonpurchaseorderdetails.eventtime), 'America/Los_Angeles') AS event_datetime_pacific,
        --  common to all except PurchaseOrderShipmentDiscrepancyIdentifiedMissingPurchaseOrderDetails
        identifiedstorenotonpurchaseorderdetails.externalpurchaseorderid AS external_purchase_order_id,
        identifiedstorenotonpurchaseorderdetails.purchaseorderid AS purchase_order_id,
        --  item details required for some events
        itemdetail.rmsskuid AS item_detail_rms_sku_id,
        itemdetail.upc AS item_detail_upc_number,
        itemdetail.unitsindiscrepancy AS item_detail_units_in_discrepancy,
        --  additional columns in one or more events
        CAST(NULL as STRING) AS cross_reference_rms_purchase_order_id,
        CAST(NULL as INT64) AS units_accepted,
        identifiedstorenotonpurchaseorderdetails.pointofidentification AS point_of_identification,
        identifiedstorenotonpurchaseorderdetails.discrepancyidentificationsourcesystem AS discrepancy_identification_source_system,
        identifiedstorenotonpurchaseorderdetails.advancedshipmentnoticeid AS advanced_shipment_notice_id,
        CAST(NULL as STRING) AS vendor_bill_of_landing,
        CAST(NULL as STRING) AS freight_bill_id,
        identifiedstorenotonpurchaseorderdetails.receivinglocationid AS receiving_location_id,
        CAST(NULL as STRING) AS actual_ship_location_id,
        CAST(NULL as STRING) AS expected_ship_locations,
        CAST(NULL as STRING) AS actual_purchase_order_id,
        CAST(NULL as DATE) AS actual_vendor_ship_date,
        identifiedstorenotonpurchaseorderdetails_exploded_base_details.actualdistributelocations AS actual_distribute_locations,
        CAST(NULL as INT64) AS units_refused,
        CAST(NULL as STRING) AS actual_pack_type,
        CAST(NULL as STRING) AS actual_vendor_number,
        --  audit fields
        identifiedstorenotonpurchaseorderdetails_exploded_base_details.batch_id AS dw_batch_id
      FROM
        identifiedstorenotonpurchaseorderdetails_exploded_base_details
      WHERE identifiedstorenotonpurchaseorderdetails_exploded_base_details.identifiedstorenotonpurchaseorderdetails IS NOT NULL
    UNION ALL
    SELECT
        --  purchaseOrderShipmentDiscrepancyRefusedDetails
        refuseddetails_exploded_base.exception_id,
        refuseddetails.vendornumber AS vendor_number,
        refuseddetails_exploded_base.event_name,
        format_datetime('%Y-%m-%d %H:%M:%E3Sxxx', refuseddetails.eventtime) AS event_time,
        substr(CAST(datetime(timestamp_millis(refuseddetails.eventtime), 'America/Los_Angeles') as STRING), 0, 10) AS event_date_pacific,
        datetime(timestamp_millis(refuseddetails.eventtime), 'America/Los_Angeles') AS event_datetime_pacific,
        --  common to all except PurchaseOrderShipmentDiscrepancyIdentifiedMissingPurchaseOrderDetails
        refuseddetails.externalpurchaseorderid AS external_purchase_order_id,
        refuseddetails.purchaseorderid AS purchase_order_id,
        --  item details required for some events
        CAST(NULL as STRING) AS item_detail_rms_sku_id,
        CAST(NULL as STRING) AS item_detail_upc_number,
        CAST(NULL as INT64) AS item_detail_units_in_discrepancy,
        --  additional columns in one or more events
        CAST(NULL as STRING) AS cross_reference_rms_purchase_order_id,
        CAST(NULL as INT64) AS units_accepted,
        CAST(NULL as STRING) AS point_of_identification,
        CAST(NULL as STRING) AS discrepancy_identification_source_system,
        CAST(NULL as STRING) AS advanced_shipment_notice_id,
        CAST(NULL as STRING) AS vendor_bill_of_landing,
        CAST(NULL as STRING) AS freight_bill_id,
        CAST(NULL as STRING) AS receiving_location_id,
        CAST(NULL as STRING) AS actual_ship_location_id,
        CAST(NULL as STRING) AS expected_ship_locations,
        CAST(NULL as STRING) AS actual_purchase_order_id,
        CAST(NULL as DATE) AS actual_vendor_ship_date,
        CAST(NULL as STRING) AS actual_distribute_locations,
        refuseddetails.unitsrefused AS units_refused,
        CAST(NULL as STRING) AS actual_pack_type,
        CAST(NULL as STRING) AS actual_vendor_number,
        --  audit fields
        refuseddetails_exploded_base.batch_id AS dw_batch_id
      FROM
        refuseddetails_exploded_base
      WHERE refuseddetails_exploded_base.refuseddetails IS NOT NULL
    UNION ALL
    SELECT
        --  PurchaseOrderShipmentDiscrepancyIdentifiedMisShipDetails
        misshipdetails_exploded_base_details.exception_id,
        misshipdetails.vendornumber AS vendor_number,
        misshipdetails_exploded_base_details.event_name,
        format_datetime('%Y-%m-%d %H:%M:%E3Sxxx', misshipdetails.eventtime) AS event_time,
        substr(CAST(datetime(timestamp_millis(misshipdetails.eventtime), 'America/Los_Angeles') as STRING), 0, 10) AS event_date_pacific,
        datetime(timestamp_millis(misshipdetails.eventtime), 'America/Los_Angeles') AS event_datetime_pacific,
        --  common to all except PurchaseOrderShipmentDiscrepancyIdentifiedMissingPurchaseOrderDetails
        misshipdetails.externalpurchaseorderid AS external_purchase_order_id,
        misshipdetails.purchaseorderid AS purchase_order_id,
        --  item details required for some events
        itemdetail.rmsskuid AS item_detail_rms_sku_id,
        itemdetail.upc AS item_detail_upc_number,
        itemdetail.unitsindiscrepancy AS item_detail_units_in_discrepancy,
        --  additional columns in one or more events
        CAST(NULL as STRING) AS cross_reference_rms_purchase_order_id,
        CAST(NULL as INT64) AS units_accepted,
        misshipdetails.pointofidentification AS point_of_identification,
        misshipdetails.discrepancyidentificationsourcesystem AS discrepancy_identification_source_system,
        misshipdetails.advancedshipmentnoticeid AS advanced_shipment_notice_id,
        CAST(NULL as STRING) AS vendor_bill_of_landing,
        CAST(NULL as STRING) AS freight_bill_id,
        misshipdetails.receivinglocationid AS receiving_location_id,
        CAST(NULL as STRING) AS actual_ship_location_id,
        misshipdetails_exploded_base_details.expectedshiplocations AS expected_ship_locations,
        CAST(NULL as STRING) AS actual_purchase_order_id,
        CAST(NULL as DATE) AS actual_vendor_ship_date,
        CAST(NULL as STRING) AS actual_distribute_locations,
        CAST(NULL as INT64) AS units_refused,
        CAST(NULL as STRING) AS actual_pack_type,
        CAST(NULL as STRING) AS actual_vendor_number,
        --  audit fields
        misshipdetails_exploded_base_details.batch_id AS dw_batch_id
      FROM
        misshipdetails_exploded_base_details
      WHERE misshipdetails_exploded_base_details.misshipdetails IS NOT NULL
    UNION ALL
    SELECT
        --  PurchaseOrderShipmentDiscrepancyIdentifiedMissingPurchaseOrderDetails
        missingpurchaseorderdetails_exploded_base.exception_id,
        missingpurchaseorderdetails.vendornumber AS vendor_number,
        missingpurchaseorderdetails_exploded_base.event_name,
        format_datetime('%Y-%m-%d %H:%M:%E3Sxxx', missingpurchaseorderdetails.eventtime) AS event_time,
        substr(CAST(datetime(timestamp_millis(missingpurchaseorderdetails.eventtime), 'America/Los_Angeles') as STRING), 0, 10) AS event_date_pacific,
        datetime(timestamp_millis(missingpurchaseorderdetails.eventtime), 'America/Los_Angeles') AS event_datetime_pacific,
        --  common to all except PurchaseOrderShipmentDiscrepancyIdentifiedMissingPurchaseOrderDetails
        CAST(NULL as STRING) AS external_purchase_order_id,
        CAST(NULL as STRING) AS purchase_order_id,
        --  item details required for some events
        itemdetail.rmsskuid AS item_detail_rms_sku_id,
        itemdetail.upc AS item_detail_upc_number,
        itemdetail.unitsindiscrepancy AS item_detail_units_in_discrepancy,
        --  additional columns in one or more events
        CAST(NULL as STRING) AS cross_reference_rms_purchase_order_id,
        CAST(NULL as INT64) AS units_accepted,
        missingpurchaseorderdetails.pointofidentification AS point_of_identification,
        missingpurchaseorderdetails.discrepancyidentificationsourcesystem AS discrepancy_identification_source_system,
        missingpurchaseorderdetails.advancedshipmentnoticeid AS advanced_shipment_notice_id,
        CAST(NULL as STRING) AS vendor_bill_of_landing,
        CAST(NULL as STRING) AS freight_bill_id,
        missingpurchaseorderdetails.receivinglocationid AS receiving_location_id,
        CAST(NULL as STRING) AS actual_ship_location_id,
        CAST(NULL as STRING) AS expected_ship_locations,
        missingpurchaseorderdetails.actualpurchaseorderid AS actual_purchase_order_id,
        CAST(NULL as DATE) AS actual_vendor_ship_date,
        CAST(NULL as STRING) AS actual_distribute_locations,
        CAST(NULL as INT64) AS units_refused,
        CAST(NULL as STRING) AS actual_pack_type,
        CAST(NULL as STRING) AS actual_vendor_number,
        --  audit fields
        missingpurchaseorderdetails_exploded_base.batch_id AS dw_batch_id
      FROM
        missingpurchaseorderdetails_exploded_base
      WHERE missingpurchaseorderdetails_exploded_base.missingpurchaseorderdetails IS NOT NULL
    UNION ALL
    SELECT
        --  PurchaseOrderShipmentDiscrepancyIdentifiedOverageDetails
        overagedetails_exploded_base.exception_id,
        overagedetails.vendornumber AS vendor_number,
        overagedetails_exploded_base.event_name,
        format_datetime('%Y-%m-%d %H:%M:%E3Sxxx', overagedetails.eventtime) AS event_time,
        substr(CAST(datetime(timestamp_millis(overagedetails.eventtime), 'America/Los_Angeles') as STRING), 0, 10) AS event_date_pacific,
        datetime(timestamp_millis(overagedetails.eventtime), 'America/Los_Angeles') AS event_datetime_pacific,
        --  common to all except PurchaseOrderShipmentDiscrepancyIdentifiedMissingPurchaseOrderDetails
        overagedetails.externalpurchaseorderid AS external_purchase_order_id,
        overagedetails.purchaseorderid AS purchase_order_id,
        --  item details required for some events
        itemdetail.rmsskuid AS item_detail_rms_sku_id,
        itemdetail.upc AS item_detail_upc_number,
        itemdetail.unitsindiscrepancy AS item_detail_units_in_discrepancy,
        --  additional columns in one or more events
        CAST(NULL as STRING) AS cross_reference_rms_purchase_order_id,
        CAST(NULL as INT64) AS units_accepted,
        overagedetails.pointofidentification AS point_of_identification,
        overagedetails.discrepancyidentificationsourcesystem AS discrepancy_identification_source_system,
        overagedetails.advancedshipmentnoticeid AS advanced_shipment_notice_id,
        overagedetails.vendorbilloflading AS vendor_bill_of_landing,
        CAST(NULL as STRING) AS freight_bill_id,
        overagedetails.receivinglocationid AS receiving_location_id,
        CAST(NULL as STRING) AS actual_ship_location_id,
        CAST(NULL as STRING) AS expected_ship_locations,
        CAST(NULL as STRING) AS actual_purchase_order_id,
        CAST(NULL as DATE) AS actual_vendor_ship_date,
        CAST(NULL as STRING) AS actual_distribute_locations,
        CAST(NULL as INT64) AS units_refused,
        CAST(NULL as STRING) AS actual_pack_type,
        CAST(NULL as STRING) AS actual_vendor_number,
        --  audit fields
        overagedetails_exploded_base.batch_id AS dw_batch_id
      FROM
        overagedetails_exploded_base
      WHERE overagedetails_exploded_base.overagedetails IS NOT NULL
    UNION ALL
    SELECT
        --  PurchaseOrderShipmentDiscrepancyIdentifiedBulkStoreMismatchDetails
        bulkstoremismatchdetails_exploded_base_details.exception_id,
        bulkstoremismatchdetails.vendornumber AS vendor_number,
        bulkstoremismatchdetails_exploded_base_details.event_name,
        format_datetime('%Y-%m-%d %H:%M:%E3Sxxx', bulkstoremismatchdetails.eventtime) AS event_time,
        substr(CAST(datetime(timestamp_millis(bulkstoremismatchdetails.eventtime), 'America/Los_Angeles') as STRING), 0, 10) AS event_date_pacific,
        datetime(timestamp_millis(bulkstoremismatchdetails.eventtime), 'America/Los_Angeles') AS event_datetime_pacific,
        --  common to all except PurchaseOrderShipmentDiscrepancyIdentifiedMissingPurchaseOrderDetails
        bulkstoremismatchdetails.externalpurchaseorderid AS external_purchase_order_id,
        bulkstoremismatchdetails.purchaseorderid AS purchase_order_id,
        --  item details required for some events
        itemdetail.rmsskuid AS item_detail_rms_sku_id,
        itemdetail.upc AS item_detail_upc_number,
        itemdetail.unitsindiscrepancy AS item_detail_units_in_discrepancy,
        --  additional columns in one or more events
        CAST(NULL as STRING) AS cross_reference_rms_purchase_order_id,
        CAST(NULL as INT64) AS units_accepted,
        bulkstoremismatchdetails.pointofidentification AS point_of_identification,
        bulkstoremismatchdetails.discrepancyidentificationsourcesystem AS discrepancy_identification_source_system,
        bulkstoremismatchdetails.advancedshipmentnoticeid AS advanced_shipment_notice_id,
        CAST(NULL as STRING) AS vendor_bill_of_landing,
        CAST(NULL as STRING) AS freight_bill_id,
        bulkstoremismatchdetails.receivinglocationid AS receiving_location_id,
        CAST(NULL as STRING) AS actual_ship_location_id,
        CAST(NULL as STRING) AS expected_ship_locations,
        CAST(NULL as STRING) AS actual_purchase_order_id,
        CAST(NULL as DATE) AS actual_vendor_ship_date,
        bulkstoremismatchdetails_exploded_base_details.actualdistributelocations AS actual_distribute_locations,
        CAST(NULL as INT64) AS units_refused,
        bulkstoremismatchdetails.actualpacktype AS actual_pack_type,
        CAST(NULL as STRING) AS actual_vendor_number,
        --  audit fields
        bulkstoremismatchdetails_exploded_base_details.batch_id AS dw_batch_id
      FROM
        bulkstoremismatchdetails_exploded_base_details
      WHERE bulkstoremismatchdetails_exploded_base_details.bulkstoremismatchdetails IS NOT NULL
    UNION ALL
    SELECT
        --  PurchaseOrderShipmentDiscrepancyIdentifiedSupplierDepartmentMismatchDetails
        supplierdepartmentmismatchdetails_exploded_base.exception_id,
        supplierdepartmentmismatchdetails.vendornumber AS vendor_number,
        supplierdepartmentmismatchdetails_exploded_base.event_name,
        format_datetime('%Y-%m-%d %H:%M:%E3Sxxx', supplierdepartmentmismatchdetails.eventtime) AS event_time,
        substr(CAST(datetime(timestamp_millis(supplierdepartmentmismatchdetails.eventtime), 'America/Los_Angeles') as STRING), 0, 10) AS event_date_pacific,
        datetime(timestamp_millis(supplierdepartmentmismatchdetails.eventtime), 'America/Los_Angeles') AS event_datetime_pacific,
        --  common to all except PurchaseOrderShipmentDiscrepancyIdentifiedMissingPurchaseOrderDetails
        supplierdepartmentmismatchdetails.externalpurchaseorderid AS external_purchase_order_id,
        supplierdepartmentmismatchdetails.purchaseorderid AS purchase_order_id,
        --  item details required for some events
        itemdetail.rmsskuid AS item_detail_rms_sku_id,
        itemdetail.upc AS item_detail_upc_number,
        itemdetail.unitsindiscrepancy AS item_detail_units_in_discrepancy,
        --  additional columns in one or more events
        CAST(NULL as STRING) AS cross_reference_rms_purchase_order_id,
        CAST(NULL as INT64) AS units_accepted,
        supplierdepartmentmismatchdetails.pointofidentification AS point_of_identification,
        supplierdepartmentmismatchdetails.discrepancyidentificationsourcesystem AS discrepancy_identification_source_system,
        supplierdepartmentmismatchdetails.advancedshipmentnoticeid AS advanced_shipment_notice_id,
        CAST(NULL as STRING) AS vendor_bill_of_landing,
        CAST(NULL as STRING) AS freight_bill_id,
        supplierdepartmentmismatchdetails.receivinglocationid AS receiving_location_id,
        CAST(NULL as STRING) AS actual_ship_location_id,
        CAST(NULL as STRING) AS expected_ship_locations,
        CAST(NULL as STRING) AS actual_purchase_order_id,
        CAST(NULL as DATE) AS actual_vendor_ship_date,
        CAST(NULL as STRING) AS actual_distribute_locations,
        CAST(NULL as INT64) AS units_refused,
        CAST(NULL as STRING) AS actual_pack_type,
        supplierdepartmentmismatchdetails.actualvendornumber AS actual_vendor_number,
        --  audit fields
        supplierdepartmentmismatchdetails_exploded_base.batch_id AS dw_batch_id
      FROM
        supplierdepartmentmismatchdetails_exploded_base
      WHERE supplierdepartmentmismatchdetails_exploded_base.supplierdepartmentmismatchdetails IS NOT NULL
    UNION ALL
    SELECT
        --  purchaseOrderShipmentDiscrepancyIdentifiedVirtualStoreMismatchDetails
        virtualstoremismatchdetails_exploded_base_details.exception_id,
        virtualstoremismatchdetails.vendornumber AS vendor_number,
        virtualstoremismatchdetails_exploded_base_details.event_name,
        format_datetime('%Y-%m-%d %H:%M:%E3Sxxx', virtualstoremismatchdetails.eventtime) AS event_time,
        substr(CAST(datetime(timestamp_millis(virtualstoremismatchdetails.eventtime), 'America/Los_Angeles') as STRING), 0, 10) AS event_date_pacific,
        datetime(timestamp_millis(virtualstoremismatchdetails.eventtime), 'America/Los_Angeles') AS event_datetime_pacific,
        --  common to all except PurchaseOrderShipmentDiscrepancyIdentifiedMissingPurchaseOrderDetails
        virtualstoremismatchdetails.externalpurchaseorderid AS external_purchase_order_id,
        virtualstoremismatchdetails.purchaseorderid AS purchase_order_id,
        --  item details required for some events
        itemdetail.rmsskuid AS item_detail_rms_sku_id,
        itemdetail.upc AS item_detail_upc_number,
        itemdetail.unitsindiscrepancy AS item_detail_units_in_discrepancy,
        --  additional columns in one or more events
        CAST(NULL as STRING) AS cross_reference_rms_purchase_order_id,
        CAST(NULL as INT64) AS units_accepted,
        virtualstoremismatchdetails.pointofidentification AS point_of_identification,
        virtualstoremismatchdetails.discrepancyidentificationsourcesystem AS discrepancy_identification_source_system,
        virtualstoremismatchdetails.advancedshipmentnoticeid AS advanced_shipment_notice_id,
        CAST(NULL as STRING) AS vendor_bill_of_landing,
        CAST(NULL as STRING) AS freight_bill_id,
        virtualstoremismatchdetails.receivinglocationid AS receiving_location_id,
        CAST(NULL as STRING) AS actual_ship_location_id,
        CAST(NULL as STRING) AS expected_ship_locations,
        CAST(NULL as STRING) AS actual_purchase_order_id,
        CAST(NULL as DATE) AS actual_vendor_ship_date,
        virtualstoremismatchdetails_exploded_base_details.actualdistributelocations AS actual_distribute_locations,
        CAST(NULL as INT64) AS units_refused,
        CAST(NULL as STRING) AS actual_pack_type,
        CAST(NULL as STRING) AS actual_vendor_number,
        --  audit fields
        virtualstoremismatchdetails_exploded_base_details.batch_id AS dw_batch_id
      FROM
        virtualstoremismatchdetails_exploded_base_details
      WHERE virtualstoremismatchdetails_exploded_base_details.virtualstoremismatchdetails IS NOT NULL
;
TRUNCATE TABLE purchase_order_shipment_discrepancy_ldg;
INSERT INTO purchase_order_shipment_discrepancy_ldg 
  SELECT
      po_discrepancy_temp.exception_id,
      po_discrepancy_temp.vendor_number,
      po_discrepancy_temp.event_name,
      po_discrepancy_temp.event_time,
      po_discrepancy_temp.event_datetime_pacific,
      po_discrepancy_temp.event_date_pacific,
      po_discrepancy_temp.external_purchase_order_id,
      po_discrepancy_temp.purchase_order_id,
      po_discrepancy_temp.item_detail_rms_sku_id,
      po_discrepancy_temp.item_detail_upc_number,
      po_discrepancy_temp.item_detail_units_in_discrepancy,
      po_discrepancy_temp.cross_reference_rms_purchase_order_id,
      po_discrepancy_temp.units_accepted,
      po_discrepancy_temp.point_of_identification,
      po_discrepancy_temp.discrepancy_identification_source_system,
      po_discrepancy_temp.advanced_shipment_notice_id,
      po_discrepancy_temp.vendor_bill_of_landing,
      po_discrepancy_temp.freight_bill_id,
      po_discrepancy_temp.receiving_location_id,
      po_discrepancy_temp.actual_ship_location_id,
      po_discrepancy_temp.expected_ship_locations,
      po_discrepancy_temp.actual_purchase_order_id,
      po_discrepancy_temp.actual_vendor_ship_date,
      po_discrepancy_temp.actual_distribute_locations,
      po_discrepancy_temp.units_refused,
      po_discrepancy_temp.actual_pack_type,
      po_discrepancy_temp.actual_vendor_number,
      po_discrepancy_temp.dw_batch_id
    FROM
      po_discrepancy_temp
;
