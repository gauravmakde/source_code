SET QUERY_BAND = 'App_ID=APP08818;
     DAG_ID=ddl_ship_method_dim_11521_ACE_ENG;
     Task_Name=ddl_ship_method_dim;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_USL.ship_method_dim
Team/Owner: Customer Analytics/Irene Ma
Date Created/Modified: 05/10/2023

Note:
-- What is the the purpose of the table: Lookup table creating a unique identifier for each promise_type_code value, which represents shipping method (and time in days) selected by the customer at time of online purchase.
-- What is the update cadence/lookback window: daily refreshment, run at 8am UTC
*/

CREATE SET TABLE {usl_t2_schema}.ship_method_dim ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      ship_method_id INTEGER GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
      promise_type_code VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
      ship_method_name VARCHAR(8000) CHARACTER SET UNICODE NOT CASESPECIFIC,
      dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL)
PRIMARY INDEX ( PROMISE_TYPE_CODE );
COMMENT ON {usl_t2_schema}.ship_method_dim IS 'Lookup table creating a unique identifier for each promise_type_code value, which represents shipping method (and time in days) selected by the customer at time of online purchase.';
-- NOTE: Identity columns are not intended to provide an accurate, sequential order for which the rows are loaded into a table.

SET QUERY_BAND = NONE FOR SESSION;