UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.config_lkup SET
    config_value = 'N'
WHERE LOWER(interface_code) = LOWER('MRIc_DAY_AGG_DLY') AND LOWER(config_key) = LOWER('MRCH_NAP_DAY_FACT_LOAD_FL');