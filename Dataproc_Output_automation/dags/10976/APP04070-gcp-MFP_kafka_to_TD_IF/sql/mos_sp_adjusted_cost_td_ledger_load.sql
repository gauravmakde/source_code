

BEGIN
DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=merch_mos_sp_adjusted_cost_td_ledger_load;
---Task_Name=mos_sp_adjusted_cost_td_ledger_stage_ledger_load;'*/


--COLLECT STATISTICS COLUMN ( TRANSACTION_DATE ) , COLUMN ( PARTITION , TRANSACTION_DATE, SKU_NUM, LOCATION_NUM  ), COLUMN ( GENERAL_LEDGER_REFERENCE_NUMBER, REASON_CODE, TRANSACTION_DATE, SKU_NUM, LOCATION_NUM ), COLUMN ( PARTITION ) ON `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.MERCH_SP_MOS_ADJUSTED_EXPENSE_TRANSFER_LEDGER_FACT;

BEGIN TRANSACTION;

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_sp_mos_adjusted_expense_transfer_ledger_fact AS mosadj
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_expense_transfer_ledger_fact AS metlf
    WHERE LOWER(mosadj.sku_num) = LOWER(sku_num) 
	AND mosadj.location_num = location_num 
	AND mosadj.transaction_date = transaction_date 
	AND LOWER(mosadj.reason_code) = LOWER(reason_code) 
	AND LOWER(mosadj.general_ledger_reference_number) = LOWER(general_ledger_reference_number) 
	AND CAST(dw_sys_load_tmstp AS DATE) > DATE_SUB(CAST((SELECT dw_batch_dt
                        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup
                        WHERE LOWER(interface_code) = LOWER('MRCH_NAP_MOS_SP_ADJ')) AS DATE), INTERVAL (SELECT interface_freq
                    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup
                    WHERE LOWER(interface_code) = LOWER('MRCH_NAP_MOS_SP_ADJ')) DAY) AND CAST(dw_sys_load_tmstp AS DATE) <= CAST((SELECT dw_batch_dt
                    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup
                    WHERE LOWER(interface_code) = LOWER('MRCH_NAP_MOS_SP_ADJ')) AS DATE) AND CAST(mosadj.dw_sys_load_tmstp AS DATE) <= CAST((SELECT dw_batch_dt
                    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup
                    WHERE LOWER(interface_code) = LOWER('MRCH_NAP_MOS_SP_ADJ')) AS DATE));

					

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_sp_mos_adjusted_expense_transfer_ledger_fact (general_ledger_reference_number, reason_code
 , last_updated_time_in_millis, last_updated_time, last_updated_time_tz, event_time, event_time_tz, event_id, location_num, department_num, class_num,
 subclass_num, sku_type, sku_num, transaction_date, transaction_code, quantity, total_cost_currency_code,
 total_cost_amount, total_retail_currency_code, total_retail_amount, dw_sys_load_tmstp, dw_sys_load_tmstp_tz, dw_sys_updt_tmstp, dw_sys_updt_tmstp_tz)
(SELECT *
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_expense_transfer_ledger_fact AS metlf
 WHERE CAST(dw_sys_load_tmstp AS DATE) > DATE_SUB(CAST((SELECT dw_batch_dt
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('MRCH_NAP_MOS_SP_ADJ')) AS DATE), INTERVAL (SELECT interface_freq
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup
     WHERE LOWER(interface_code) = LOWER('MRCH_NAP_MOS_SP_ADJ')) DAY)
  AND CAST(dw_sys_load_tmstp AS DATE) <= CAST((SELECT dw_batch_dt
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup
     WHERE LOWER(interface_code) = LOWER('MRCH_NAP_MOS_SP_ADJ')) AS DATE));


	 
DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_sp_mos_adjusted_expense_transfer_ledger_fact AS mosadj
WHERE transaction_date = {{params.mos_sp_adj_tran_date}} 
	AND location_num = {{params.mos_sp_adj_loc}} AND general_ledger_reference_number IN {{params.moas_sp_adj_ref_num}} AND CAST(dw_sys_load_tmstp AS DATE) > DATE_SUB(CAST((SELECT dw_batch_dt
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('MRCH_NAP_MOS_SP_ADJ')) AS DATE), INTERVAL (SELECT interface_freq
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup
     WHERE LOWER(interface_code) = LOWER('MRCH_NAP_MOS_SP_ADJ')) DAY) AND CAST(dw_sys_load_tmstp AS DATE) <= CAST((SELECT dw_batch_dt
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup
    WHERE LOWER(interface_code) = LOWER('MRCH_NAP_MOS_SP_ADJ')) AS DATE);



	
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup 
SET dw_batch_dt = DATE_ADD(etl_batch_dt_lkup.dw_batch_dt, INTERVAL 1 DAY),
    extract_start_dt = DATE_ADD(etl_batch_dt_lkup.extract_start_dt, INTERVAL 1 DAY),
    extract_end_dt = DATE_ADD(etl_batch_dt_lkup.extract_end_dt, INTERVAL 1 DAY)
WHERE LOWER(interface_code) = LOWER('MRCH_NAP_MOS_SP_ADJ');



/*SET QUERY_BAND = NONE FOR SESSION;*/
COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
ROLLBACK TRANSACTION;
RAISE USING MESSAGE = @@error.message;
END;
