/* ***********************************************************************************************************
* Kafka to Landing query.
* Version: 1.00
*     - Pulls Version from header
* Desc: Reads from the topic product-item-supplier-size-analytical-avro, and writes to ITEM_SUPPLIER_SIZE_LDG
* ************************************************************************************************************
*/

SET QUERY_BAND = 'App_ID=app09248;DAG_ID=supplier_size_17216_app06742_DASCLM;Task_Name=load_kafka_to_landing;'
FOR SESSION VOLATILE;

--Source
--Reading Data from Source Kafka Topic using SQL API CODE
CREATE TEMPORARY VIEW temp_item_supplier_size_object_model AS SELECT * FROM kafka_product_item_supplier_size_object_model_avro;

--Transform Logic
CREATE TEMPORARY VIEW kafka_product_item_supplier_size_object_model_avro_extract_columns AS
SELECT
    npin AS npin_num,
    rmsSkuId AS rms_sku_id,
    supplierSize1 AS supplier_size_1,
    supplierSize1Description AS supplier_size_1_desc,
    supplierSize2 AS supplier_size_2,
    supplierSize2Description AS supplier_size_2_desc,
    lastUpdatedTime AS last_updated_tmstp,
    createdTime AS created_tmstp,
    element_at(headers,'Version') AS header_version
FROM temp_item_supplier_size_object_model;

--- Writing into landing table
INSERT INTO ITEM_SUPPLIER_SIZE_LDG
(
    npin_num,
    rms_sku_id,
    supplier_size_1,
    supplier_size_1_desc,
    supplier_size_2,
    supplier_size_2_desc,
    last_updated_tmstp,
    created_tmstp,
    header_version
)
SELECT DISTINCT
    CAST(npin_num AS string),
    CAST(rms_sku_id AS string),
    CAST(supplier_size_1 AS string),
    CAST(supplier_size_1_desc AS string),
    CAST(supplier_size_2 AS string),
    CAST(supplier_size_2_desc AS string),
    CAST(last_updated_tmstp AS string),
    CAST(created_tmstp AS string),
    CAST(header_version AS string)
FROM kafka_product_item_supplier_size_object_model_avro_extract_columns;
