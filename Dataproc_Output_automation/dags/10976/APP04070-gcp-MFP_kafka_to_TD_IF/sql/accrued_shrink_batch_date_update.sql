-- CREATE OR REPLACE PROCEDURE prd_nap_utl.raven_source_bteq_100()
BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=accrued_shrink_fact_load;
---Task_Name=accr_shrink_batch_date_update;'*/
-- COMMIT TRANSACTION;

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup SET
    dw_batch_dt = DATE_ADD(etl_batch_dt_lkup.dw_batch_dt, INTERVAL etl_batch_dt_lkup.interface_freq DAY)
WHERE LOWER(interface_code) = LOWER('ACCRUED_SHRINK') AND dw_batch_dt < CURRENT_DATE('PST8PDT');


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_ACCRUED_SHRINK_SUBCLASS_STORE_WEEK_FACT',  'prd_NAP_FCT',  'accrued_shrink_fact_load',  'accr_shrink_load',  1,  'LOAD_END',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME(('PST8PDT'))) AS DATETIME),  'ACCRUED_SHRINK');

/*SET QUERY_BAND = NONE FOR SESSION;*/

END;
