UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup SET
 dw_batch_dt = DATE_ADD(etl_batch_dt_lkup.dw_batch_dt, INTERVAL etl_batch_dt_lkup.interface_freq DAY)
WHERE LOWER(interface_code) = LOWER('MRIc_DAY_AGG_DLY');


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup SET
 dw_batch_dt = (SELECT MAX(cal0.day_date)
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim AS cal0
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl0 ON cal0.day_date <= etl0.dw_batch_dt
  WHERE LOWER(etl0.interface_code) = LOWER('MRIc_DAY_AGG_DLY')
   AND cal0.day_num_of_fiscal_week = 7)
WHERE LOWER(interface_code) = LOWER('MRIc_DAY_AGG_WKLY');