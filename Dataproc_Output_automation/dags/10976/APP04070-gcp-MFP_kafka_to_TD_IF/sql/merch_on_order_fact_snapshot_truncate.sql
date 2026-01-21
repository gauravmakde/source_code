
BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
BEGIN
SET _ERROR_CODE  =  0;

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_on_order_snapshot_fact AS tgt
WHERE snapshot_week_date <= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL CAST(TRUNC(CAST((SELECT config_value
                FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup AS chnl
                WHERE LOWER(interface_code) = LOWER('MERCH_NAP_ONORDR_DLY') AND LOWER(config_key) = LOWER('RETAIN_YEARS')) AS FLOAT64)) AS INTEGER) YEAR);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;