SET QUERY_BAND = 'App_ID=APP08818;
     DAG_ID=ddl_device_dim_11521_ACE_ENG;
     Task_Name=ddl_device_dim;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_USL.device_dim
Team/Owner: Customer Analytics/Irene Ma
Date Created/Modified: 05/10/2023

Note:
-- What is the the purpose of the table: Lookup table creating a unique identifier for each source_platform_code value, which represent the device on which a transaction occurred.
-- What is the update cadence/lookback window: daily refreshment, run at 8am UTC
*/

CREATE SET TABLE {usl_t2_schema}.device_dim ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      device_id INTEGER GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
      source_platform_code VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      device_name VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL)
PRIMARY INDEX ( SOURCE_PLATFORM_CODE );
COMMENT ON {usl_t2_schema}.device_dim IS 'Lookup table creating a unique identifier for each source_platform_code value, which represent the device on which a transaction occurred.';
-- NOTE: Identity columns are not intended to provide an accurate, sequential order for which the rows are loaded into a table.

SET QUERY_BAND = NONE FOR SESSION;