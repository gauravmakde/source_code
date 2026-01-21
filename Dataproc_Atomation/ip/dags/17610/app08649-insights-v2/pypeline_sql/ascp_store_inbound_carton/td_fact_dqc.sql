CREATE VOLATILE MULTISET TABLE TMP_ELT_CONTROL AS
(
  SELECT batch_id
       , curr_batch_date AS batch_date
       , batch_start_tmstp AS dw_sys_start_tmstp
  FROM {db_env}_NAP_BASE_VWS.ELT_CONTROL
  WHERE subject_area_nm = 'NAP_ASCP_STORE_INBOUND_CARTON'
)
WITH DATA ON COMMIT PRESERVE ROWS
;

ET;
CREATE VOLATILE MULTISET TABLE TMP_ETL_BATCH_DQC_LOG
(
  BATCH_ID INTEGER NOT NULL,
  BATCH_DATE DATE FORMAT 'yyyy-mm-dd' NOT NULL,
  SOURCE_TABLE VARCHAR(250) CHARACTER SET UNICODE NOT CASESPECIFIC,
  ERROR_STATUS VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
  ERROR_MESSAGE VARCHAR(1000) CHARACTER SET UNICODE NOT CASESPECIFIC,
  DW_SYS_TMSTP TIMESTAMP(6) WITH TIME ZONE
)
ON COMMIT PRESERVE ROWS
;
ET;

/*
Check that at least one record loaded and that the number of records loaded into stage table is equal to the number of records got into target
*/
LOCK {db_env}_NAP_BASE_VWS.STORE_INBOUND_CARTON_EVENTS_FACT FOR ACCESS
INSERT INTO TMP_ETL_BATCH_DQC_LOG (batch_id, batch_date, source_table, error_status, error_message, dw_sys_tmstp)
SELECT batch_id
     , batch_date
     , '{db_env}_NAP_BASE_VWS.STORE_INBOUND_CARTON_EVENTS_FACT' as source_table
     , CASE WHEN fct_cnt = ldg_cnt AND ldg_cnt > 0 THEN 'INFO'
            ELSE 'ERROR'
       END as error_status
     , CASE WHEN fct_cnt = ldg_cnt THEN CONCAT('Loaded ', cast(ldg_cnt as varchar(20)), ' records')
            ELSE CONCAT('STAGE count=', cast(ldg_cnt as varchar(20)), ' is not equal to FACT count=', cast(FCT_CNT as char(20)))
       END as error_message
     , CURRENT_TIMESTAMP(6)
FROM (
  SELECT COUNT(DISTINCT purchase_order_num || carton_num || event_id) as fct_cnt
   FROM {db_env}_NAP_BASE_VWS.STORE_INBOUND_CARTON_EVENTS_FACT
   JOIN TMP_ELT_CONTROL
     ON dw_batch_id = batch_id
 ) as F
CROSS JOIN (
   SELECT COUNT(distinct purchase_order_num || carton_num || event_id) as ldg_cnt
   FROM {db_env}_NAP_BASE_VWS.STORE_INBOUND_CARTON_EVENTS_LDG
 ) as S
CROSS JOIN TMP_ELT_CONTROL
;
ET;

LOCK {db_env}_NAP_BASE_VWS.STORE_INBOUND_CARTON_FACT FOR ACCESS
INSERT INTO TMP_ETL_BATCH_DQC_LOG (batch_id, batch_date, source_table, error_status, error_message, dw_sys_tmstp)
SELECT batch_id
     , batch_date
     , '{db_env}_NAP_BASE_VWS.STORE_INBOUND_CARTON_FACT' as source_table
     , CASE WHEN fct_cnt = ldg_cnt AND ldg_cnt > 0 THEN 'INFO'
            ELSE 'ERROR'
       END as error_status
     , CASE WHEN fct_cnt = ldg_cnt THEN CONCAT('Loaded ', cast(ldg_cnt as varchar(20)), ' records')
            ELSE CONCAT('STAGE count=', cast(ldg_cnt as varchar(20)), ' is not equal to FACT count=', cast(FCT_CNT as char(20)))
       END as error_message
     , CURRENT_TIMESTAMP(6)
FROM (
  SELECT COUNT(DISTINCT purchase_order_num || carton_num || sku_num || CAST(received_date AS VARCHAR(23))) as fct_cnt
   FROM {db_env}_NAP_BASE_VWS.STORE_INBOUND_CARTON_FACT
   JOIN TMP_ELT_CONTROL
     ON dw_batch_id = batch_id
 ) as F
CROSS JOIN (
   SELECT COUNT(distinct purchase_order_num || carton_num || sku_num || CAST(CAST(SUBSTRING(event_tmstp, 1, 10) as DATE) AS VARCHAR(23))) as ldg_cnt
   FROM {db_env}_NAP_BASE_VWS.STORE_INBOUND_CARTON_EVENTS_LDG
 ) as S
CROSS JOIN TMP_ELT_CONTROL
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
JOIN TMP_ELT_CONTROL bi
ON di.BATCH_ID = bi.BATCH_ID
WHERE di.ERROR_STATUS = 'ERROR'
HAVING COUNT(1) > 0
;
