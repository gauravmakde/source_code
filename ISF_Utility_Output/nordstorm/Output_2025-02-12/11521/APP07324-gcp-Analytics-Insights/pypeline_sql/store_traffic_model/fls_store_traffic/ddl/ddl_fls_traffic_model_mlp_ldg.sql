SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=fls_store_traffic_11521_ACE_ENG;
     Task_Name=ddl_fls_traffic_model_mlp_ldg;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_fls_traffic_model.fls_traffic_model_mlp_ldg
Team/Owner: TECH_FFP_ANALYTICS/Agnes Bao
Date Created/Modified: 02/10/2023

Note:
This landing table is created as part of the fls_store_traffic job that loads
data from S3 to teradata. The landing table is dropped when the job completes.

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{fls_traffic_model_t2_schema}', 'fls_traffic_model_mlp_ldg', OUT_RETURN_MSG);

create multiset table {fls_traffic_model_t2_schema}.fls_traffic_model_mlp_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
(
     store_number       INTEGER NOT NULL
    , day_date          DATE
    , thanksgiving      INTEGER COMPRESS
    , christmas         INTEGER COMPRESS
    , easter            INTEGER COMPRESS
    , estimated_traffic DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    , estimate_tmstp    INTEGER
    , model_version     VARCHAR(50) COMPRESS
)
primary index(store_number, day_date)
partition by range_n(day_date BETWEEN DATE'2019-01-01' AND DATE'2031-12-31' EACH INTERVAL '1' DAY, NO RANGE)
;

SET QUERY_BAND = NONE FOR SESSION;