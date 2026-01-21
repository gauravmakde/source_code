--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : retail_tran_tender_type_code.td_fact_load.sql
-- Author                  : Dogukan Ulu
-- Description             : Script to write data from `cf-nordstrom`.PRD_NAP_FCT.RETAIL_TRAN_TENDER_FACT, modify and insert into `cf-nordstrom`.PRD_NAP_FCT.RETAIL_TRAN_TENDER_TYPE_CODE_FACT
-- Data Source             : Fact view `cf-nordstrom`.PRD_NAP_FCT.RETAIL_TRAN_TENDER_FACT
-- ETL Run Frequency       : Daily
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2024-08-16  Dogukan Ulu   FA-13597: Load the created FACT table.
-- 2024-10-01  Dogukan Ulu   FA-13620: Add Tender Entry Method Code as requested.
--*************************************************************************************************************************************


CREATE TEMPORARY TABLE IF NOT EXISTS retail_tran_tender_fact

AS
SELECT global_tran_id,
 tender_type_code,
 tender_item_entry_method_code,
 MAX(tender_item_usd_amt) AS max_tender_item_usd_amt,
 business_day_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_tender_fact
WHERE dw_batch_date = (SELECT MAX(dw_batch_date)
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_tender_fact
   WHERE business_day_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 15 DAY))
 AND business_day_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 15 DAY)
GROUP BY global_tran_id,
 business_day_date,
 tender_type_code,
 tender_item_entry_method_code
QUALIFY (RANK() OVER (PARTITION BY global_tran_id ORDER BY business_day_date DESC)) = 1;


CREATE TEMPORARY TABLE IF NOT EXISTS retail_tran_tender_fact_final

AS
SELECT retail_tran_tender_fact.global_tran_id,
 SUBSTR(STRING_AGG(retail_tran_tender_fact.tender_type_code || ' |'), 1, 300) AS tender_type_code_list,
 SUBSTR(STRING_AGG(retail_tran_tender_fact.tender_item_entry_method_code || ' |'), 1, 300) AS
 tender_item_entry_method_code_list,
 MAX(retail_tran_tender_fact.business_day_date) AS business_day_date,
 MAX(bi.batch_id) AS dw_batch_id,
 MAX(bi.batch_date) AS dw_batch_date,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_updt_tmstp
FROM retail_tran_tender_fact
 LEFT JOIN (SELECT batch_id,
   batch_date
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_info
  WHERE LOWER(interface_code) = LOWER('RETAIL_TRAN_TENDER_TYPE_CODE_FACT')
   AND dw_sys_end_tmstp IS NULL) AS bi ON TRUE
GROUP BY retail_tran_tender_fact.global_tran_id;


MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.retail_tran_tender_type_code_fact AS fct_daily
USING retail_tran_tender_fact_final AS fct_main
ON fct_daily.global_tran_id = fct_main.global_tran_id AND fct_daily.business_day_date = fct_main.business_day_date
WHEN MATCHED THEN UPDATE SET
 tender_type_code_list = fct_main.tender_type_code_list,
 dw_batch_id = fct_main.dw_batch_id,
 dw_batch_date = fct_main.dw_batch_date,
 dw_sys_updt_tmstp = fct_main.dw_sys_updt_tmstp,
 tender_item_entry_method_code = fct_main.tender_item_entry_method_code_list
WHEN NOT MATCHED THEN INSERT VALUES(fct_main.global_tran_id, fct_main.tender_type_code_list, fct_main.business_day_date
 , fct_main.dw_batch_id, fct_main.dw_batch_date, fct_main.dw_sys_load_tmstp, fct_main.dw_sys_updt_tmstp, fct_main.tender_item_entry_method_code_list
 );