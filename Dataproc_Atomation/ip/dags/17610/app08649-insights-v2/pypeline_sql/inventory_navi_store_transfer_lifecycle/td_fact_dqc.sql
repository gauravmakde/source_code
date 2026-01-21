/*To avoid duplicates of the same error messages due to Teradata architecture 
 Massively Parallel Processing (MPP) qnd Access Module Processors (AMPs)*/
DELETE 
  FROM {db_env}_NAP_UTL.ETL_BATCH_DQC_LOG t,
       {db_env}_NAP_BASE_VWS.ETL_BATCH_INFO s
 where t.batch_id = s.batch_id
   and s.interface_code = 'INVENTORY_NAVI_STORE_TRANSFER_LIFECYCLE'
   AND s.dw_sys_end_tmstp IS NULL; 

CREATE VOLATILE MULTISET TABLE  TMP_ETL_BATCH_INFO AS
(
  SELECT batch_id, batch_date, dw_sys_start_tmstp
  FROM {db_env}_NAP_BASE_VWS.ETL_BATCH_INFO
  WHERE interface_code = 'INVENTORY_NAVI_STORE_TRANSFER_LIFECYCLE'
AND dw_sys_end_tmstp IS NULL
)
WITH DATA ON COMMIT PRESERVE ROWS
;

ET;

LOCK {db_env}_NAP_UTL.ETL_BATCH_DQC_LOG FOR ACCESS
CREATE VOLATILE MULTISET TABLE TMP_ETL_BATCH_DQC_LOG AS 
(SELECT BATCH_ID,
  BATCH_DATE,
  SOURCE_TABLE,
  ERROR_STATUS,
  ERROR_MESSAGE
  FROM {db_env}_NAP_UTL.ETL_BATCH_DQC_LOG)
WITH NO DATA 
ON COMMIT PRESERVE ROWS
;
ET;


/*
Check that at least one record loaded and that the number of records loaded into stage table is equal to the number of records got into target
*/
INSERT INTO TMP_ETL_BATCH_DQC_LOG (batch_id, batch_date, source_table, error_status, error_message)
SELECT batch_id
     , batch_date
     , '{db_env}_NAP_BASE_VWS.INVENTORY_NAVI_STORE_TRANSFER_PLANNING_FACT' as source_table
     , CASE WHEN fct_cnt = ldg_cnt AND ldg_cnt > 0 THEN 'INFO'
            ELSE 'ERROR'
       END as error_status
     , CASE WHEN fct_cnt = ldg_cnt THEN CONCAT('Loaded ', cast(ldg_cnt as varchar(20)), ' records')
            ELSE CONCAT('STAGE count=', cast(ldg_cnt as varchar(20)), ' of StoreTransferCreated events is not equal to FACT count=', cast(FCT_CNT as char(20)))
       END as error_message
FROM (
  SELECT COUNT(DISTINCT transfer_num || sku_num || location_num) as fct_cnt
    FROM {db_env}_NAP_BASE_VWS.INVENTORY_NAVI_STORE_TRANSFER_PLANNING_FACT
    JOIN TMP_ETL_BATCH_INFO
      ON dw_batch_id = batch_id
   WHERE created_date_pacific IS NOT NULL
 ) as F
CROSS JOIN (
   SELECT COUNT(DISTINCT transfer_num || sku_num || location_num) as ldg_cnt
     FROM {db_env}_NAP_BASE_VWS.INVENTORY_NAVI_STORE_TRANSFER_LDG
    WHERE event_type = 'StoreTransferCreated' 
 ) as S
CROSS JOIN TMP_ETL_BATCH_INFO
;
ET;


/*
Check that at least one record loaded and that the number of records loaded into stage table is equal to the number of records got into target
*/
INSERT INTO TMP_ETL_BATCH_DQC_LOG (batch_id, batch_date, source_table, error_status, error_message)
SELECT batch_id
     , batch_date
     , '{db_env}_NAP_BASE_VWS.INVENTORY_NAVI_STORE_TRANSFER_DELIVERY_FACT' as source_table
     , CASE WHEN fct_cnt = ldg_cnt AND ldg_cnt > 0 THEN 'INFO'
            ELSE 'ERROR'
       END as error_status
     , CASE WHEN fct_cnt = ldg_cnt THEN CONCAT('Loaded ', cast(ldg_cnt as varchar(20)), ' records')
            ELSE CONCAT('STAGE count=', cast(ldg_cnt as varchar(20)), ' of StoreTransferShipped events is not equal to FACT count=', cast(FCT_CNT as char(20)))
       END as error_message
FROM (
  SELECT COUNT(DISTINCT transfer_num || carton_num || sku_num || location_num) as fct_cnt
   FROM {db_env}_NAP_BASE_VWS.INVENTORY_NAVI_STORE_TRANSFER_DELIVERY_FACT
   JOIN TMP_ETL_BATCH_INFO
     ON dw_batch_id = batch_id
  WHERE shipped_date_pacific IS NOT NULL
 ) as F
CROSS JOIN (
   SELECT COUNT(DISTINCT transfer_num || carton_num || sku_num || location_num) as ldg_cnt
     FROM {db_env}_NAP_BASE_VWS.INVENTORY_NAVI_STORE_TRANSFER_LDG
    WHERE event_type = 'StoreTransferShipped'
 ) as S
CROSS JOIN TMP_ETL_BATCH_INFO
;
ET;



/*
Check that at least one record loaded and that the number of records loaded into stage table is equal to the number of records got into target
*/
INSERT INTO TMP_ETL_BATCH_DQC_LOG (batch_id, batch_date, source_table, error_status, error_message)
SELECT batch_id
     , batch_date
     , '{db_env}_NAP_BASE_VWS.INVENTORY_NAVI_STORE_TRANSFER_COMPLETED_FACT' as source_table
     , CASE WHEN fct_cnt = ldg_cnt THEN 'INFO'
            ELSE 'ERROR'
       END as error_status
     , CASE WHEN fct_cnt = ldg_cnt THEN CONCAT('Loaded ', cast(ldg_cnt as varchar(20)), ' records')
            ELSE CONCAT('STAGE count=', cast(ldg_cnt as varchar(20)), ' is not equal to FACT count=', cast(FCT_CNT as char(20)))
       END as error_message
FROM (
  SELECT COUNT(DISTINCT request_id || transfer_num || sku_num) as fct_cnt
   FROM {db_env}_NAP_BASE_VWS.INVENTORY_NAVI_STORE_TRANSFER_COMPLETED_FACT
   JOIN TMP_ETL_BATCH_INFO
     ON dw_batch_id = batch_id
 ) as F
CROSS JOIN (
   SELECT COUNT(DISTINCT request_id || transfer_num || sku_num) as ldg_cnt
   FROM {db_env}_NAP_BASE_VWS.INVENTORY_NAVI_STORE_TRANSFER_COMPLETED_LDG
 ) as S
CROSS JOIN TMP_ETL_BATCH_INFO
;
ET;

/*
Check that the number of records loaded into stage table is equal to the number of records got into target
*/
INSERT INTO TMP_ETL_BATCH_DQC_LOG (batch_id, batch_date, source_table, error_status, error_message)
SELECT batch_id
     , batch_date
     , '{db_env}_NAP_BASE_VWS.INVENTORY_NAVI_STORE_TRANSFER_EXPIRED_ITEM_FACT' as source_table
     , CASE WHEN fct_cnt = ldg_cnt THEN 'INFO'
            ELSE 'ERROR'
       END as error_status
     , CASE WHEN fct_cnt = ldg_cnt THEN CONCAT('Loaded ', cast(ldg_cnt as varchar(20)), ' records')
            ELSE CONCAT('STAGE count=', cast(ldg_cnt as varchar(20)), ' is not equal to FACT count=', cast(FCT_CNT as char(20)))
       END as error_message
FROM (
  SELECT COUNT(DISTINCT request_id || sku_num || electronic_product_code) as fct_cnt
   FROM {db_env}_NAP_BASE_VWS.INVENTORY_NAVI_STORE_TRANSFER_EXPIRED_ITEM_FACT
   JOIN TMP_ETL_BATCH_INFO
     ON dw_batch_id = batch_id
 ) as F
CROSS JOIN (
   SELECT COUNT(DISTINCT request_id || sku_num || electronic_product_code) as ldg_cnt
   FROM {db_env}_NAP_BASE_VWS.INVENTORY_NAVI_STORE_TRANSFER_EXPIRED_ITEM_LDG
 ) as S
CROSS JOIN TMP_ETL_BATCH_INFO
;
ET;

INSERT INTO {db_env}_NAP_UTL.ETL_BATCH_DQC_LOG (batch_id, batch_date, source_table, error_status, error_message)
SELECT batch_id
     , batch_date
     , source_table
     , error_status
     , error_message
FROM TMP_ETL_BATCH_DQC_LOG
;
ET;

SELECT 1/0 as ERR
FROM {db_env}_NAP_UTL.ETL_BATCH_DQC_LOG di
JOIN TMP_ETL_BATCH_INFO bi
ON di.BATCH_ID = bi.BATCH_ID
WHERE di.ERROR_STATUS = 'ERROR'
HAVING COUNT(1) > 0
;
