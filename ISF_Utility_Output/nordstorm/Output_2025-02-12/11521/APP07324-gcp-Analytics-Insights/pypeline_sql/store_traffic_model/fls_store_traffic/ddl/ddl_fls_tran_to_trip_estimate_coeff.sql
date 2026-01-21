SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=ddl_fls_tran_to_trip_estimate_coeff_11521_ACE_ENG;
     Task_Name=ddl_fls_tran_to_trip_estimate_coeff;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_fls_traffic_model.fls_tran_to_trip_estimate_coeff
Team/Owner: tech_ffp_analytics/Agnes Bao
Date Created/Modified: 03/03/2023

Note:
-- Given a 3 day delay in assigning customer identity to transaction, this tables has details on coefficents for
-- estimating purchase trip counts from transaction counts

*/

create multiset table {fls_traffic_model_t2_schema}.fls_tran_to_trip_estimate_coeff
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
     store_number           integer
    ,time_period_type       varchar(10) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    ,trans_to_trips_mltplr  DECIMAL(20,15) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    ,dw_sys_load_tmstp      TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
UNIQUE PRIMARY INDEX (store_number, time_period_type)
;

-- Table Comment (STANDARD)
COMMENT ON {fls_traffic_model_t2_schema}.fls_tran_to_trip_estimate_coeff IS 'details on coefficents for estimating purchase trip counts from transaction counts';
SET QUERY_BAND = NONE FOR SESSION;