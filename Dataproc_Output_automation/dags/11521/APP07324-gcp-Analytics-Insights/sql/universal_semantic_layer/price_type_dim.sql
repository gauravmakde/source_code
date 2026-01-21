/* SET QUERY_BAND = 'App_ID=APP08818;
     DAG_ID=device_pricetype_shipmethod_transtype_dim_11521_ACE_ENG;
     Task_Name=price_type_dim;'
     FOR SESSION VOLATILE;*/

/*
T2/Table Name: {{params.gcp_project_id}}.{{params.usl_t2_schema}}.price_type_dim
Team/Owner: Customer Analytics/Irene Ma
Date Created/Modified: 05/10/2023

Note:
-- What is the the purpose of the table: Lookup table creating a unique identifier for each price_type value, which represents whether an item was purchased: at regular price, on clearance, or on promotion.
-- What is the update cadence/lookback window: daily refreshment, run at 8am UTC
*/






MERGE INTO {{params.gcp_project_id}}.{{params.usl_t2_schema}}.price_type_dim AS tgt
USING (
  SELECT
    price_type,
    CASE
      WHEN price_type = 'C' THEN 'closeout price'
      WHEN price_type = 'P' THEN 'promo price'
      WHEN price_type = 'R' THEN 'regular price'
      ELSE 'unknown price type'
    END AS price_type_name
  FROM (
    SELECT DISTINCT price_type
    FROM {{params.gcp_project_id}}.t2dl_das_sales_returns.sales_and_returns_fact_base) AS srdb) AS src
ON tgt.price_type = src.price_type

WHEN MATCHED THEN
  UPDATE SET
    tgt.price_type_name = src.price_type_name,
    tgt.dw_sys_load_tmstp = CURRENT_DATETIME('PST8PDT')
WHEN NOT MATCHED THEN
  INSERT (price_type, price_type_name, dw_sys_load_tmstp)
  VALUES (src.price_type, src.price_type_name, CURRENT_DATETIME('PST8PDT'));



DELETE FROM {{params.gcp_project_id}}.{{params.usl_t2_schema}}.price_type_dim
WHERE price_type IS NULL AND price_type_id <> (SELECT MIN(price_type_id)
   FROM {{params.gcp_project_id}}.{{params.usl_t2_schema}}.price_type_dim
   WHERE price_type IS NULL);


--COLLECT   STATISTICS COLUMN (price_type_id, price_type) on {{params.gcp_project_id}}.{{params.usl_t2_schema}}.price_type_dim