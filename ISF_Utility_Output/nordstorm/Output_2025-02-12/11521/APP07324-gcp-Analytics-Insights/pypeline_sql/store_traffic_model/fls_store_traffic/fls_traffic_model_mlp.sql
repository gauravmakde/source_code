SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=fls_store_traffic_11521_ACE_ENG;
     Task_Name=fls_traffic_model_mlp;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_fls_traffic_model.fls_traffic_model_mlp
Team/Owner: TECH_FFP_ANALYTICS/Agnes Bao
Date Created/Modified: 02/10/2023

Note:
FLS traffic model v2 deployed on MLP and output result to s3
Update cadence: daily
Lookback window: 15 days

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/

delete
from {fls_traffic_model_t2_schema}.fls_traffic_model_mlp
where day_date between {start_date} and {end_date}
;

insert into {fls_traffic_model_t2_schema}.fls_traffic_model_mlp
select store_number
     , day_date
     , thanksgiving
     , christmas
     , easter
     , estimated_traffic
     , estimate_tmstp
     , model_version
     , CURRENT_TIMESTAMP as dw_sys_load_tmstp
from {fls_traffic_model_t2_schema}.fls_traffic_model_mlp_ldg
where day_date between {start_date} and {end_date}
;

COLLECT STATISTICS column (store_number) ,column (day_date) ON {fls_traffic_model_t2_schema}.fls_traffic_model_mlp;
COLLECT STATISTICS COLUMN (PARTITION , day_date) ON {fls_traffic_model_t2_schema}.fls_traffic_model_mlp;
COLLECT STATISTICS COLUMN (PARTITION) ON {fls_traffic_model_t2_schema}.fls_traffic_model_mlp;

-- drop staging table
drop table {fls_traffic_model_t2_schema}.fls_traffic_model_mlp_ldg
;

SET QUERY_BAND = NONE FOR SESSION;