SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=comp_store_lifecycle_dim_load_2656_napstore_insights;
Task_Name=comp_store_lifecycle_dim_load;'
FOR SESSION VOLATILE;

ET;

EXEC {db_env}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD
('COMP_STORE_STATUS_LIFECYCLE_DIM','{db_env}_NAP_DIM','comp_store_lifecycle_dim_load_2656_napstore_insights','comp_store_status_dim_table',1,'LOAD_START','',current_timestamp,'ORG');

ET;

DELETE FROM {db_env}_NAP_DIM.COMP_STORE_STATUS_LIFECYCLE_DIM TGT
WHERE EXISTS ( SELECT 1 FROM {db_env}_NAP_STG.COMP_STORE_STATUS_LDG SRC
WHERE TGT.store_num = SRC.currentdata_storenumber);

ET;

INSERT INTO {db_env}_NAP_DIM.COMP_STORE_STATUS_LIFECYCLE_DIM 
(store_num,
comp_status_code,
comp_status_desc,
comp_store_status_begin_date,
comp_store_status_end_date,
current_data_updated_timestamp,
dw_batch_date,
dw_sys_load_tmstp,
dw_sys_updt_tmstp)
SELECT
currentdata_storenumber as store_num,
compStoreStatusSet_compStatusCode as comp_status_code,
compStoreStatusSet_compStatusDesc as comp_status_desc,
CAST(compStoreStatusSet_compStoreAssocBeginDate  AS TIMESTAMP WITH TIME ZONE) AS comp_store_status_begin_date,
CAST(compStoreStatusSet_compStoreAssocEndDate AS TIMESTAMP WITH TIME ZONE)  as comp_store_status_end_date,
 NORD_UDF.ISO8601_TMSTP(currentdataupdatedtimestamp) as current_data_updated_timestamp,
CURRENT_TIMESTAMP(0) as dw_sys_load_tmstp,
CURRENT_TIMESTAMP(0) as dw_sys_updt_tmstp,
CURRENT_DATE AS dw_batch_date
FROM {db_env}_NAP_STG.COMP_STORE_STATUS_LDG SRC
QUALIFY ROW_NUMBER() OVER(PARTITION BY currentData_storeNumber,compStoreStatusSet_compStatusCode,compStoreStatusSet_compStoreAssocBeginDate ORDER BY current_data_updated_timestamp DESC)=1;


ET;

EXEC {db_env}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD
('COMP_STORE_STATUS_LIFECYCLE_DIM','{db_env}_NAP_DIM','comp_store_lifecycle_dim_load_2656_napstore_insights','comp_store_status_dim_table',2,'LOAD_END','',current_timestamp,'ORG');

ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;