SET QUERY_BAND = 'App_ID=APP08818;
     DAG_ID=ddl_price_type_dim_11521_ACE_ENG;
     Task_Name=ddl_price_type_dim;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_USL.price_type_dim
Team/Owner: Customer Analytics/Irene Ma
Date Created/Modified: 05/10/2023

Note:
-- What is the the purpose of the table: Lookup table creating a unique identifier for each price_type value, which represents whether an item was purchased: at regular price, on clearance, or on promotion.
-- What is the update cadence/lookback window: daily refreshment, run at 8am UTC
*/

CREATE SET TABLE {usl_t2_schema}.price_type_dim ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      price_type_id INTEGER GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
      price_type CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      price_type_name VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL)
PRIMARY INDEX ( price_type );
COMMENT ON {usl_t2_schema}.price_type_dim IS 'Lookup table creating a unique identifier for each price_type value, which represents whether an item was purchased: at regular price, on clearance, or on promotion.';
-- NOTE: Identity columns are not intended to provide an accurate, sequential order for which the rows are loaded into a table.

SET QUERY_BAND = NONE FOR SESSION;