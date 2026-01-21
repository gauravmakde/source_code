
CREATE TEMPORARY TABLE TMP_ETL_BATCH_INFO AS
SELECT batch_id,
 batch_date,
 dw_sys_start_tmstp
FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.ETL_BATCH_INFO
WHERE LOWER(interface_code) = LOWER('CUSTOMER_DSCO_SHIPPED_EVENT_FACT')
 AND dw_sys_end_tmstp IS NULL;

-- COMMIT TRANSACTION;

INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_NAP_UTL.ETL_BATCH_DQC_LOG (error_id, batch_id, batch_date, source_table, error_status, error_message)
(SELECT FARM_FINGERPRINT(GENERATE_UUID()) AS error_id,
  `TMP_ETL_BATCH_INFO0`.batch_id,
  `TMP_ETL_BATCH_INFO0`.batch_date,
  '{{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.CUSTOMER_DSCO_SHIPPED_EVENT_FACT' AS source_table,
   CASE WHEN t0.fct_cnt = t2.ldg_cnt AND t2.ldg_cnt > 0 THEN 'INFO' ELSE 'WARNING' END AS error_status, 
   CASE WHEN t0.fct_cnt = t2.ldg_cnt THEN CONCAT('Loaded ', SUBSTR(CAST(t2.ldg_cnt AS STRING), 1, 20), ' records') ELSE CONCAT('STAGE count=', SUBSTR(CAST(t2.ldg_cnt AS STRING), 1, 20), ' is not equal to FACT count=', RPAD(CAST(t0.fct_cnt AS STRING) , 20, ' ')) END AS error_message
 FROM (SELECT COUNT(DISTINCT customer_dsco_shipped_event_fact.order_num || customer_dsco_shipped_event_fact.order_line_id ) AS fct_cnt
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.CUSTOMER_DSCO_SHIPPED_EVENT_FACT
   INNER JOIN `TMP_ETL_BATCH_INFO` 
   ON customer_dsco_shipped_event_fact.dw_batch_id = `TMP_ETL_BATCH_INFO`.batch_id) AS t0
  INNER JOIN (SELECT COUNT(DISTINCT ordernumber || orderlineid) AS ldg_cnt
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.CUSTOMER_DSCO_SHIPPED_EVENT_LDG) AS t2 ON TRUE
  INNER JOIN `TMP_ETL_BATCH_INFO` AS `TMP_ETL_BATCH_INFO0` ON TRUE);

-- COMMIT TRANSACTION;

INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_NAP_UTL.ETL_BATCH_DQC_LOG (error_id, batch_id, batch_date, source_table, error_status, error_message)
(SELECT FARM_FINGERPRINT(GENERATE_UUID()) AS error_id,
  `TMP_ETL_BATCH_INFO0`.batch_id,
  `TMP_ETL_BATCH_INFO0`.batch_date,
  '{{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.CUSTOMER_DSCO_SHIPPED_EVENT_FACT' AS source_table,
  'WARNING' AS error_status,
  CONCAT('FACT has ', SUBSTR(CAST(COUNT(1) AS STRING), 1, 20), ' duplicates by order_num, order_line_id') AS error_message
 FROM (SELECT customer_dsco_shipped_event_fact.order_num,
              customer_dsco_shipped_event_fact.order_line_id
     FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.CUSTOMER_DSCO_SHIPPED_EVENT_FACT
     INNER JOIN `TMP_ETL_BATCH_INFO` 
     ON customer_dsco_shipped_event_fact.dw_batch_id = `TMP_ETL_BATCH_INFO`.batch_id
     GROUP BY customer_dsco_shipped_event_fact.order_num, customer_dsco_shipped_event_fact.order_line_id
     HAVING COUNT(1) > 1) AS t2
  INNER JOIN `TMP_ETL_BATCH_INFO` AS `TMP_ETL_BATCH_INFO0` ON TRUE
 GROUP BY `TMP_ETL_BATCH_INFO0`.batch_id, `TMP_ETL_BATCH_INFO0`.batch_date);

-- COMMIT TRANSACTION;

INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_NAP_UTL.ETL_BATCH_DQC_LOG (error_id, batch_id, batch_date, source_table, error_status, error_message)
(SELECT FARM_FINGERPRINT(GENERATE_UUID()) AS error_id,
  `TMP_ETL_BATCH_INFO`.batch_id,
  `TMP_ETL_BATCH_INFO`.batch_date,
  '{{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.CUSTOMER_DSCO_SHIPPED_EVENT_FACT' AS source_table,
  'WARNING' AS error_status,
  t3.error_message
 FROM (SELECT CONCAT(
    CASE WHEN COUNT(CASE WHEN f.warehouse_id IS NULL THEN 1 ELSE NULL END) > 0 THEN 'warehouse_id is null, ' ELSE '' END, 
    CASE WHEN COUNT(CASE WHEN f.sscc_barcode IS NULL THEN 1 ELSE NULL END) > 0 THEN 'sscc_barcode is null, ' ELSE '' END, 
    CASE WHEN COUNT(CASE WHEN f.supplier_ship_date IS NULL THEN 1 ELSE NULL END) > 0 THEN 'supplier_ship_date is null, ' ELSE '' END, 
    CASE WHEN COUNT(CASE WHEN f.supplier_id IS NULL THEN 1 ELSE NULL END) > 0 THEN 'supplier_id is null, ' ELSE '' END, 
    CASE WHEN COUNT(CASE WHEN f.ship_carrier IS NULL THEN 1 ELSE NULL END) > 0 THEN 'ship_carrier is null, ' ELSE '' END) AS error_message
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.CUSTOMER_DSCO_SHIPPED_EVENT_FACT AS f
   INNER JOIN `TMP_ETL_BATCH_INFO` AS bi 
   ON f.dw_batch_id = bi.batch_id
   WHERE f.warehouse_id IS NULL
    OR f.sscc_barcode IS NULL
    OR f.supplier_ship_date IS NULL
    OR f.supplier_id IS NULL
    OR f.ship_carrier IS NULL
   HAVING COUNT(1) > 0) AS t3
   INNER JOIN `TMP_ETL_BATCH_INFO` ON TRUE);

-- COMMIT TRANSACTION;

INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_NAP_UTL.ETL_BATCH_DQC_LOG (error_id, batch_id, batch_date, source_table, error_status, error_message)
(SELECT FARM_FINGERPRINT(GENERATE_UUID()) AS error_id,
  t1.batch_id,
  t1.batch_date,
  '{{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.CUSTOMER_DSCO_SHIPPED_EVENT_FACT' AS source_table,
  'WARNING' AS error_status,
  CONCAT('package_length_unit not in  (INCH, FOOT, METER, CENTIMETER, MILLIMETER, UNKNOWN): ', t1.package_length_unit)
  AS error_message
 FROM (SELECT f.package_length_unit,
    `TMP_ETL_BATCH_INFO`.batch_id,
    `TMP_ETL_BATCH_INFO`.batch_date
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.CUSTOMER_DSCO_SHIPPED_EVENT_FACT AS f
    INNER JOIN `TMP_ETL_BATCH_INFO` ON f.dw_batch_id = `TMP_ETL_BATCH_INFO`.batch_id
   WHERE LOWER(f.package_length_unit) NOT IN (LOWER('INCH'), LOWER('FOOT'), LOWER('METER'), LOWER('CENTIMETER'), LOWER('MILLIMETER'
       ), LOWER('UNKNOWN'))
   LIMIT 1) AS t1);

-- COMMIT TRANSACTION;

INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_NAP_UTL.ETL_BATCH_DQC_LOG (error_id, batch_id, batch_date, source_table, error_status, error_message)
(SELECT FARM_FINGERPRINT(GENERATE_UUID()) AS error_id,
  t1.batch_id,
  t1.batch_date,
  '{{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.CUSTOMER_DSCO_SHIPPED_EVENT_FACT' AS source_table,
  'WARNING' AS error_status,
  CONCAT('package_weight_unit not in (OUNCE, POUND, MILLIGRAM, GRAM, KILOGRAM, UNKNOWN): ', t1.package_weight_unit) AS
  error_message
 FROM (SELECT f.package_weight_unit,
    `TMP_ETL_BATCH_INFO`.batch_id,
    `TMP_ETL_BATCH_INFO`.batch_date
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.CUSTOMER_DSCO_SHIPPED_EVENT_FACT AS f
    INNER JOIN `TMP_ETL_BATCH_INFO` ON f.dw_batch_id = `TMP_ETL_BATCH_INFO`.batch_id
   WHERE LOWER(f.package_weight_unit) NOT IN (LOWER('OUNCE'), LOWER('POUND'), LOWER('MILLIGRAM'), LOWER('GRAM'), LOWER('KILOGRAM'
       ), LOWER('UNKNOWN'))
   LIMIT 1) AS t1);

-- COMMIT TRANSACTION;

SELECT CAST(FLOOR(1 / 0) AS INT64) AS err,COUNT(1)
FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_UTL.ETL_BATCH_DQC_LOG AS di
    INNER JOIN `TMP_ETL_BATCH_INFO` AS bi ON di.batch_id = bi.batch_id
WHERE LOWER(di.error_status) = LOWER('ERROR')
HAVING COUNT(1) > 0;

