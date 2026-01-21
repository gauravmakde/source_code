SET QUERY_BAND = 'App_ID=APP08818;
     DAG_ID=device_pricetype_shipmethod_transtype_dim_11521_ACE_ENG;
     Task_Name=device_dim;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_USL.device_dim
Team/Owner: Customer Analytics/Irene Ma
Date Created/Modified: 05/10/2023

Note:
-- What is the the purpose of the table: Lookup table creating a unique identifier for each source_platform_code value, which represent the device on which a transaction occurred.
-- What is the update cadence/lookback window: daily refreshment, run at 8am UTC
*/

MERGE INTO {usl_t2_schema}.device_dim as tgt
USING (SELECT source_platform_code
             ,OREPLACE(LOWER(COALESCE(NULLIF(source_platform_code, 'unknown'), 'unknown device')), 'csr_', '') AS device_name
        FROM (select distinct source_platform_code from prd_nap_usr_vws.order_line_detail_fact) as oldf) as src
 ON tgt.source_platform_code = src.source_platform_code

WHEN MATCHED THEN
 UPDATE SET device_name = src.device_name
           ,dw_sys_load_tmstp = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
 INSERT (source_platform_code, device_name, dw_sys_load_tmstp)
 VALUES (src.source_platform_code, src.device_name, CURRENT_TIMESTAMP);

-- NOTE: SQL treats each NULL as unique value!
DELETE FROM {usl_t2_schema}.device_dim
 WHERE source_platform_code IS NULL
   AND device_id <> (SELECT MIN(device_id) FROM {usl_t2_schema}.device_dim WHERE source_platform_code IS NULL);

COLLECT STATISTICS COLUMN (DEVICE_ID, SOURCE_PLATFORM_CODE) on {usl_t2_schema}.device_dim;


SET QUERY_BAND = NONE FOR SESSION;