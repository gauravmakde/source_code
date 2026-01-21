SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=org_load_v2_2656_napstore_insights;
Task_Name=org_store_dim_load_0_s3_to_stg_table;'
FOR SESSION VOLATILE;

set `mapreduce.input.fileinputformat.input.dir.recursive` = true;

MSCK REPAIR TABLE acp_etl_landing.store_json;

create
temporary view org_store_dim_input AS
SELECT *,
       ROW_NUMBER() OVER (PARTITION BY currentData.storeNumber ORDER BY currentDataUpdatedTimeStamp DESC) AS rn
FROM acp_etl_landing.store_json; 


create
temporary view org_store_comp_status_dim_input AS
select
       currentData.storeNumber as currentdata_storenumber,
       explode_outer(currentData.compStoreStatusSet) currentData_compStoreStatusSet,
       currentDataUpdatedTimeStamp
from org_store_dim_input
WHERE rn = 1;

insert
overwrite table org_store_comp_status_dim_stg_table
select
       currentdata_storenumber,
       currentData_compStoreStatusSet.compStatusCode as compStoreStatusSet_compStatusCode,
       currentData_compStoreStatusSet.compStatusDesc as compStoreStatusSet_compStatusDesc,
       currentData_compStoreStatusSet.compStoreAssocBeginDate as compStoreStatusSet_compStoreAssocBeginDate,
       currentData_compStoreStatusSet.compStoreAssocEndDate as compStoreStatusSet_compStoreAssocEndDate,
       currentDataUpdatedTimeStamp as currentdataupdatedtimestamp
from org_store_comp_status_dim_input
where currentData_compStoreStatusSet.compStatusCode IS NOT NULL;
