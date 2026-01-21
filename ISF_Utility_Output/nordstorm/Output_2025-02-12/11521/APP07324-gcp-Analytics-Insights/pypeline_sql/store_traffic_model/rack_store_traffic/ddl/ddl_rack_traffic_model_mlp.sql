SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=ddl_rack_traffic_model_mlp_11521_ACE_ENG;
     Task_Name=ddl_rack_traffic_model_mlp;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_fls_traffic_model.rack_traffic_model_mlp
Team/Owner: TECH_FFP_ANALYTICS/Agnes Bao
Date Created/Modified: 07/25/2023

Note:
RACK traffic model v2 deployed on MLP and output result to s3
Update cadence: daily
Lookback window: 15 days

*/

create multiset table {fls_traffic_model_t2_schema}.rack_traffic_model_mlp
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
    , model_version     VARCHAR(50) CHARACTER SET UNICODE COMPRESS
    , dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
UNIQUE PRIMARY INDEX(store_number, day_date)
partition by range_n(day_date BETWEEN DATE'2019-01-01' AND DATE'2031-12-31' EACH INTERVAL '1' DAY, NO RANGE)
;

-- Table Comment (STANDARD)
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_model_mlp IS 'RACK traffic model estimation results from MLP';
-- Column comments (OPTIONAL)
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_model_mlp.store_number IS 'A unique number that was assigned to a store';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_model_mlp.day_date IS 'Date';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_model_mlp.thanksgiving IS 'Flag for Thanksgiving closure';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_model_mlp.christmas IS 'Flag for Christmas closure';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_model_mlp.easter IS 'Flag for Easter closure';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_model_mlp.estimated_traffic IS 'Estimated traffic from model v2';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_model_mlp.estimate_tmstp IS 'Timestamp when estimated traffic was made';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_model_mlp.model_version IS 'Model version of estimated traffic';



SET QUERY_BAND = NONE FOR SESSION;