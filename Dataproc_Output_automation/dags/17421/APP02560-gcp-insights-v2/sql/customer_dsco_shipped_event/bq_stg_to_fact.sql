CREATE TEMPORARY TABLE IF NOT EXISTS CUSTOMER_DSCO_SHIPPED_EVENT_LDG
-- CLUSTER BY order_num, order_line_id
AS
SELECT *
FROM (SELECT cdsel.ordernumber AS order_num,
   cdsel.orderlineid AS order_line_id,
   cdsel.orderlinenumber AS order_line_num,
   cdsel.shippingdetails_carriermanifestid AS carrier_manifest_id,
   cdsel.shippingdetails_packagedetails_length AS package_length,
   cdsel.shippingdetails_packagedetails_width AS package_width,
   cdsel.shippingdetails_packagedetails_height AS package_height,
   cdsel.shippingdetails_packagedetails_lengthunit AS package_length_unit,
   cdsel.shippingdetails_packagedetails_estimateddeliverydate AS package_estimated_delivery_date,
   cdsel.shippingdetails_packagedetails_shipcost AS package_ship_cost,
   cdsel.shippingdetails_packagedetails_shipcost_currencycode AS package_ship_cost_currency_code,
   cdsel.shippingdetails_packagedetails_weight AS package_weight,
   cdsel.shippingdetails_packagedetails_weightunit AS package_weight_unit,
   cdsel.shippingdetails_shipcarrier AS ship_carrier,
   cdsel.shippingdetails_shipfrom_address_line1 AS ship_from_address_line_1,
   cdsel.shippingdetails_shipfrom_address_line2 AS ship_from_address_line_2,
   cdsel.shippingdetails_shipfrom_address_line3 AS ship_from_address_line_3,
   cdsel.shippingdetails_shipfrom_address_city AS ship_from_address_city,
   cdsel.shippingdetails_shipfrom_address_state AS ship_from_address_state,
   cdsel.shippingdetails_shipfrom_address_postalcode AS ship_from_address_postal_code,
   cdsel.shippingdetails_shipfrom_address_countrycode AS ship_from_address_country_code,
   cdsel.shippingdetails_shipfrom_company AS ship_from_company,
   cdsel.shippingdetails_shipfrom_locationcode AS ship_from_location_code,
   cdsel.shippingdetails_transportationmethodcode AS transportation_method_code,
   cdsel.shippingdetails_supplierid AS supplier_id,
    CASE
    WHEN cdsel.shippingdetails_shipdate IS NULL
    THEN DATE '4444-04-04'
    ELSE PARSE_DATE('%F', cdsel.shippingdetails_shipdate)
    END AS supplier_ship_date,
   cdsel.shippingdetails_ssccbarcode AS sscc_barcode,
   cdsel.shippingdetails_warehousecode AS warehouse_code,
   cdsel.shippingdetails_warehouseid AS warehouse_id,

   CAST(shippedDetails_eventTime as TIMESTAMP) as latest_shipped_event_tmstp_pacific,
  'PST8PDT' as latest_shipped_event_tmstp_pacific_tz,

   t0.batch_id AS dw_batch_id,
   t0.batch_date AS dw_batch_date,
   current_datetime('PST8PDT') AS dw_sys_load_tmstp,
   current_datetime('PST8PDT') AS dw_sys_updt_tmstp
  FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.CUSTOMER_DSCO_SHIPPED_EVENT_LDG AS cdsel
   LEFT JOIN (SELECT batch_id,
     batch_date
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.ETL_BATCH_INFO
    WHERE LOWER(interface_code) = LOWER('CUSTOMER_DSCO_SHIPPED_EVENT_FACT')
     AND dw_sys_end_tmstp IS NULL) AS t0 ON TRUE) AS t1
WHERE NOT EXISTS (SELECT NULL
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.CUSTOMER_DSCO_SHIPPED_EVENT_FACT AS cdsel
  WHERE LOWER(order_num) = LOWER(cdsel.order_num)
   AND LOWER(order_line_id) = LOWER(cdsel.order_line_id)
   AND latest_shipped_event_tmstp_pacific >= cdsel.latest_shipped_event_tmstp_pacific);



DELETE FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_FCT.CUSTOMER_DSCO_SHIPPED_EVENT_FACT AS tgt
WHERE EXISTS (SELECT *
    FROM `CUSTOMER_DSCO_SHIPPED_EVENT_LDG` AS src
    WHERE LOWER(order_num) = LOWER(tgt.order_num) 
    AND LOWER(order_line_id) = LOWER(tgt.order_line_id) 
    AND supplier_ship_date <> DATE '4444-04-04' 
    AND tgt.supplier_ship_date = DATE '4444-04-04');



MERGE INTO {{params.gcp_project_id}}.{{params.dbenv}}_NAP_FCT.CUSTOMER_DSCO_SHIPPED_EVENT_FACT AS fct
USING `CUSTOMER_DSCO_SHIPPED_EVENT_LDG` AS stg
ON LOWER(fct.order_num) = LOWER(stg.order_num) 
AND LOWER(fct.order_line_id) = LOWER(stg.order_line_id) 
AND fct.supplier_ship_date = stg.supplier_ship_date
WHEN MATCHED THEN 
UPDATE SET
    order_line_num = CAST(trunc(CAST(stg.order_line_num AS FLOAT64)) AS INTEGER),
    carrier_manifest_id = stg.carrier_manifest_id,
    package_length = CAST(trunc(CAST(stg.package_length AS FLOAT64)) AS INTEGER),
    package_width = CAST(trunc(CAST(stg.package_width AS FLOAT64)) AS INTEGER),
    package_height = CAST(trunc(CAST(stg.package_height AS FLOAT64)) AS INTEGER),
    package_length_unit = stg.package_length,
    package_estimated_delivery_date = CAST(stg.package_estimated_delivery_date AS DATE),
    package_ship_cost = CAST(ROUND(CAST(stg.package_ship_cost AS BIGNUMERIC), 0) AS NUMERIC),
    package_ship_cost_currency_code = stg.package_ship_cost,
    package_weight = CAST(ROUND(CAST(stg.package_weight AS BIGNUMERIC), 0) AS NUMERIC),
    package_weight_unit = stg.package_weight,
    ship_carrier = stg.ship_carrier,
    ship_from_address_line_1 = stg.ship_from_address_line_1,
    ship_from_address_line_2 = stg.ship_from_address_line_2,
    ship_from_address_line_3 = stg.ship_from_address_line_3,
    ship_from_address_city = stg.ship_from_address_city,
    ship_from_address_state = stg.ship_from_address_state,
    ship_from_address_postal_code = stg.ship_from_address_postal_code,
    ship_from_address_country_code = stg.ship_from_address_country_code,
    ship_from_company = stg.ship_from_company,
    ship_from_location_code = stg.ship_from_location_code,
    transportation_method_code = stg.transportation_method_code,
    supplier_id = stg.supplier_id,
    sscc_barcode = stg.sscc_barcode,
    warehouse_code = stg.warehouse_code,
    warehouse_id = stg.warehouse_id,
    latest_shipped_event_tmstp_pacific = CAST(stg.latest_shipped_event_tmstp_pacific AS DATETIME),
    dw_batch_id = stg.dw_batch_id,
    dw_batch_date = stg.dw_batch_date,
    dw_sys_updt_tmstp = stg.dw_sys_updt_tmstp
WHEN NOT MATCHED THEN INSERT 
(
order_num
,order_line_id
,order_line_num
,carrier_manifest_id
,package_length
,package_width
,package_height
,package_length_unit
,package_estimated_delivery_date
,package_ship_cost
,package_ship_cost_currency_code
,package_weight
,package_weight_unit
,ship_carrier
,ship_from_address_line_1
,ship_from_address_line_2
,ship_from_address_line_3
,ship_from_address_city
,ship_from_address_state
,ship_from_address_postal_code
,ship_from_address_country_code
,ship_from_company
,ship_from_location_code
,transportation_method_code
,supplier_id
,sscc_barcode
,warehouse_code
,warehouse_id
,latest_shipped_event_tmstp_pacific
,dw_batch_id
,dw_batch_date,
supplier_ship_date
,dw_sys_updt_tmstp,
dw_sys_load_tmstp
)
VALUES(
  stg.order_num, 
  stg.order_line_id,
  CAST(trunc(CAST(stg.order_line_num AS FLOAT64)) AS INTEGER),
  stg.carrier_manifest_id, 
  CAST(trunc(CAST(stg.package_length AS FLOAT64)) AS INTEGER), 
  CAST(trunc(CAST(stg.package_width AS FLOAT64)) AS INTEGER), 
  CAST(trunc(CAST(stg.package_height AS FLOAT64)) AS INTEGER),
  stg.package_length_unit, 
  CAST(stg.package_estimated_delivery_date AS DATE),
  CAST(CAST(stg.package_ship_cost AS BIGNUMERIC) AS NUMERIC) , 
  stg.package_ship_cost_currency_code,
  CAST(CAST(stg.package_weight AS BIGNUMERIC) AS NUMERIC), 
  stg.package_weight_unit, 
  stg.ship_carrier, 
  stg.ship_from_address_line_1, 
  stg.ship_from_address_line_2, 
  stg.ship_from_address_line_3, 
  stg.ship_from_address_city, 
  stg.ship_from_address_state, 
  stg.ship_from_address_postal_code, 
  stg.ship_from_address_country_code, 
  stg.ship_from_company, 
  stg.ship_from_location_code, 
  stg.transportation_method_code, 
  stg.supplier_id,
  stg.sscc_barcode, 
  stg.warehouse_code,
  stg.warehouse_id,
  CAST(stg.latest_shipped_event_tmstp_pacific AS DATETIME),
  stg.dw_batch_id, 
  stg.dw_batch_date,
  stg.supplier_ship_date,
  stg.dw_sys_updt_tmstp, 
  stg.dw_sys_load_tmstp
);

