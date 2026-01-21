SET QUERY_BAND = 'App_ID=APP08818;
     DAG_ID=ddl_customer_acquisition_dim_11521_ACE_ENG;
     Task_Name=ddl_customer_acquisition_dim;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: t2dl_das_usl.customer_acquisition_dim
Team/Owner: Customer Analytics - Styling & Strategy
Date Created/Modified: May 10th 2023

Note:
-- Purpose of the table: Dim table to directly get acquisition related info (date, year, month, box) of the customers
-- Update Cadence: Daily

*/


create multiset table {usl_t2_schema}.customer_acquisition_dim
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
     acp_id varchar(50)
    ,customer_acquired_date date
    ,customer_acquired_year integer compress
    ,customer_acquired_month integer compress
    ,customer_acquired_box varchar(10) compress
    ,customer_life_years integer compress
    ,customer_life_months integer compress
    ,dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(acp_id);

-- Table Comment (STANDARD)
COMMENT ON {usl_t2_schema}.customer_acquisition_dim IS 'Dim table to directly get acquisition related info (date, year, month, box) of the customers';

SET QUERY_BAND = NONE FOR SESSION;