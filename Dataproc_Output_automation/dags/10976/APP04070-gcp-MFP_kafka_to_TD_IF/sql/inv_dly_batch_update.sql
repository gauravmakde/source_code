--SET QUERY_BAND = '
--App_ID=APP04070;
--DAG_ID=inventory_day_fact_load_dly;
--Task_Name=inv_dw_batch_update;'
--FOR SESSION VOLATILE;

--ET;

--SET QUERY_BAND = NONE FOR SESSION;

--ET;

UPDATE 
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup 
SET 
  dw_batch_dt = DATE_ADD(
    etl_batch_dt_lkup.dw_batch_dt, INTERVAL etl_batch_dt_lkup.interface_freq DAY
  ), 
  extract_start_dt = DATE_ADD(
    etl_batch_dt_lkup.extract_start_dt, 
    INTERVAL etl_batch_dt_lkup.interface_freq DAY
  ), 
  extract_end_dt = DATE_ADD(
    etl_batch_dt_lkup.extract_end_dt, 
    INTERVAL etl_batch_dt_lkup.interface_freq DAY
  ) 
WHERE 
  lower(interface_code) = lower('MERCH_NAP_INVDLY')
  AND dw_batch_dt < CURRENT_DATE('PST8PDT');
  

