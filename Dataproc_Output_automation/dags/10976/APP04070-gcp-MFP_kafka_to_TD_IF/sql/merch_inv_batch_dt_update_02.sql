-- SET QUERY_BAND = '
-- App_ID=APP04070;
-- DAG_ID=inventory_apt_week_fact_load_wkly;
-- Task_Name=inv_week_fct_updt_batch_dt_03;'
-- FOR SESSION VOLATILE;

-- COMMIT TRANSACTION;

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup SET
    extract_start_dt = (SELECT DATE_ADD(t10.start_rebuild_date, INTERVAL CAST(trunc(CAST(cl0.config_value AS FLOAT64)) * 7 / 2 AS INTEGER) DAY)
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS t10
            INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.config_lkup AS cl0 ON LOWER(t10.interface_code) = LOWER(cl0.interface_code)
        WHERE LOWER(t10.interface_code) = LOWER('MERCH_NAP_INV_DLY') AND LOWER(cl0.config_key) = LOWER('REBUILD_WEEKS')),
    extract_end_dt = (SELECT end_rebuild_date
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw
        WHERE LOWER(interface_code) = LOWER('MERCH_NAP_INV_DLY'))
WHERE LOWER(interface_code) = LOWER('MERCH_NAP_INV_DLY');
-- COMMIT TRANSACTION;

/*SET QUERY_BAND = NONE FOR SESSION;*/

-- COMMIT TRANSACTION;