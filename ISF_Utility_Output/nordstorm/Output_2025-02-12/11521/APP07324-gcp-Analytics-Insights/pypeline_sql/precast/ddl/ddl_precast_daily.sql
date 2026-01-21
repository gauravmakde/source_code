SET QUERY_BAND = 'App_ID=APP08219;
     DAG_ID=ddl_precast_daily_11521_ACE_ENG;
     Task_Name=ddl_precast_daily;'
     FOR SESSION VOLATILE;

/*
Table Name: T2DL_DAS_FORECASTING_SOLUTIONS.precast_daily
Owner: Selina Song, Sierra Broussard 
Date Modified: 05/19/2023

Notes: 
-- PreCast is a collection of highly aggregated KPI time series. It is formatted in a way to allow flexibility 
in the number and type of KPIs while also providing a structure for automated forecasting.
-- Update cadence: daily
-- Lookback window: 60 days, to match up with mothership

1. Incremental functionality available in ISF - features are updated on schedule. DELETE FROM criteria to be set for all available features.
2. New feature integration - Backfill/History dag specifically created to load historical data for new feature. SQL script would persist in repo. DAG would be created as needed.
*/ 

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{forecasting_solutions_t2_schema}', 'precast_daily', OUT_RETURN_MSG);

CREATE MULTISET TABLE {forecasting_solutions_t2_schema}.precast_daily
    , FALLBACK
    , NO BEFORE JOURNAL
    , NO AFTER JOURNAL
    , CHECKSUM = DEFAULT
    , DEFAULT MERGEBLOCKRATIO
(
    day_date                DATE NOT NULL
    , business_unit_desc    VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , dimension_type        VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , feature_dimension     VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , feature_name          VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , feature_value         DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , feature_id            INTEGER NOT NULL
    , dw_sys_load_tmstp     TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
)
PRIMARY INDEX(day_date, business_unit_desc, dimension_type, feature_dimension, feature_id)
PARTITION BY RANGE_N(day_date BETWEEN DATE '2019-02-03' AND DATE '2025-12-31' EACH INTERVAL '1' DAY, UNKNOWN);


-- Table Comment
COMMENT ON  {forecasting_solutions_t2_schema}.precast_daily IS 'Data source table for automated forecasting';

SET QUERY_BAND = NONE FOR SESSION;