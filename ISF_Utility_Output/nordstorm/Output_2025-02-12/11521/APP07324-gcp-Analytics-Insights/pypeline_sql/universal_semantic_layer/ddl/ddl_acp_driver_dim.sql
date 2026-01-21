SET QUERY_BAND = 'App_ID=APP08818;
     DAG_ID=ddl_acp_driver_dim_11521_ACE_ENG;
     Task_Name=ddl_acp_driver_dim;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: t2dl_das_usl.acp_driver_dim
Team/Owner: Customer Analytics - Styling & Strategy
Date Created/Modified: May 9th 2023

Note:
-- Purpose of the table: Acp ID driver table for all customer focused dimension tables for the Universal Semantic Layer - Acp IDs since FY 2021
-- Update Cadence: Daily

*/

create multiset table {usl_t2_schema}.acp_driver_dim
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
     acp_id varchar(50)
    ,dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(acp_id);

-- Table Comment (STANDARD)
COMMENT ON {usl_t2_schema}.acp_driver_dim IS 'Acp ID driver table for all customer focused dimension tables for the Universal Semantic Layer - Acp IDs since FY 2021';

SET QUERY_BAND = NONE FOR SESSION;