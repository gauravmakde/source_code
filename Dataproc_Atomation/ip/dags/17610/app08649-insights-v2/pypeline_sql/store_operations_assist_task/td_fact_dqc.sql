CREATE VOLATILE MULTISET TABLE TMP_ETL_BATCH_INFO AS
(
  SELECT batch_id, batch_date, dw_sys_start_tmstp
  FROM {db_env}_NAP_BASE_VWS.ETL_BATCH_INFO
  WHERE interface_code = 'STORE_OPS_TASK_FACT'
    AND dw_sys_end_tmstp IS NULL
)
WITH DATA ON COMMIT PRESERVE ROWS
;
ET;
DELETE FROM {db_env}_NAP_UTL.ETL_BATCH_DQC_LOG WHERE BATCH_ID IN (SELECT BATCH_ID FROM TMP_ETL_BATCH_INFO);
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
LOCK {db_env}_NAP_BASE_VWS.STORE_OPS_TASK_LDG FOR ACCESS
INSERT INTO TMP_ETL_BATCH_DQC_LOG (batch_id, batch_date, source_table, error_status, error_message, dw_sys_tmstp)
SELECT batch_id
     , batch_date
     , '{db_env}_NAP_BASE_VWS.STORE_OPS_TASK_FACT' as source_table
     , CASE WHEN fct_cnt = ldg_cnt AND ldg_cnt = 0 THEN 'WARNING'
            WHEN fct_cnt = ldg_cnt THEN 'INFO'
            ELSE 'ERROR'
              END as error_status
     , CASE WHEN fct_cnt = ldg_cnt AND ldg_cnt = 0 THEN 'Loaded 0 records, staging table is empty'
            WHEN fct_cnt = ldg_cnt THEN CONCAT('Loaded ', cast(ldg_cnt as varchar(20)), ' records')
            ELSE CONCAT('STAGE count=', cast(ldg_cnt as varchar(20)), ' is not equal to FACT count=', cast(FCT_CNT as char(20)))
       END as error_message
     , CURRENT_TIMESTAMP(6)
FROM (
  SELECT COUNT(DISTINCT task_type_id || subject || owning_business_unit_id || outlet_zone_id || CAST(scheduled_end AS VARCHAR(23)))
  as fct_cnt
   FROM {db_env}_NAP_BASE_VWS.STORE_OPS_TASK_FACT
   JOIN TMP_ETL_BATCH_INFO
     ON dw_batch_id = batch_id
 ) as F
CROSS JOIN (
   SELECT COUNT(distinct taskType_id || subject || owningBusinessUnit_id || outletzone_id || scheduledEnd)
   as ldg_cnt
   FROM {db_env}_NAP_BASE_VWS.STORE_OPS_TASK_LDG
 ) as S
CROSS JOIN TMP_ETL_BATCH_INFO;
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
