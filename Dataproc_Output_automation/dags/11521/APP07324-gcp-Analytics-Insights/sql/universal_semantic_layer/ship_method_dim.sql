/* SET QUERY_BAND = 'App_ID=APP08818;
     DAG_ID=device_pricetype_shipmethod_transtype_dim_11521_ACE_ENG;
     Task_Name=ship_method_dim;'
     FOR SESSION VOLATILE;*/

/*
T2/Table Name: T2DL_DAS_USL.ship_method_dim
Team/Owner: Customer Analytics/Irene Ma
Date Created/Modified: 05/10/2023

Note:
-- What is the the purpose of the table: Lookup table creating a unique identifier for each promise_type_code value, which represents shipping method (and time in days) selected by the customer at time of online purchase.
-- What is the update cadence/lookback window: daily refreshment, run at 8am UTC
*/


MERGE INTO {{params.gcp_project_id}}.{{params.usl_t2_schema}}.ship_method_dim AS tgt
USING (SELECT promise_type_code,
  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LOWER(COALESCE(promise_type_code, 'unknown ship method')), '_', ' '),
       'standardshipping', 'standard shipping'), 'nextbus', 'next bus'), 'ssday', 'ss day'), 'twobus', 'two bus'), '',
   'unknown ship method') AS ship_method_name
 FROM (SELECT DISTINCT promise_type_code
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.order_line_detail_fact AS oldf) AS t1) AS src
ON LOWER(tgt.promise_type_code) = LOWER(src.promise_type_code)
WHEN MATCHED THEN UPDATE SET
 ship_method_name = src.ship_method_name,
 dw_sys_load_tmstp = CURRENT_DATETIME('PST8PDT')
WHEN NOT MATCHED THEN INSERT (promise_type_code, ship_method_name, dw_sys_load_tmstp) 
VALUES(src.promise_type_code, src.ship_method_name, CURRENT_DATETIME('PST8PDT'));


DELETE FROM {{params.gcp_project_id}}.{{params.usl_t2_schema}}.ship_method_dim
WHERE promise_type_code IS NULL AND ship_method_id <> (SELECT MIN(ship_method_id)
   FROM {{params.gcp_project_id}}.{{params.usl_t2_schema}}.ship_method_dim
   WHERE promise_type_code IS NULL);


--COLLECT   STATISTICS COLUMN (ship_method_id, promise_type_code) on {{params.gcp_project_id}}.{{params.usl_t2_schema}}.ship_method_dim