BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID={app_id};
DAG_ID=isf_order_cancel_17393_customer_das_customer;
---Task_Name=order_cancel_fact_load;'*/
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS order_acp_stg
AS
SELECT order_num,
 acp_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.retail_tran_hdr_fact AS rthf
WHERE dw_batch_date = (SELECT curr_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('SALES_FACT'))
 AND business_day_date >= DATE '2015-01-01'
 AND acp_id IS NOT NULL
 AND order_num IS NOT NULL
QUALIFY (ROW_NUMBER() OVER (PARTITION BY order_num ORDER BY dw_batch_date DESC, tran_time DESC, global_tran_id DESC)) =
  1;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS order_shopper_acp_stg
AS
SELECT odr.order_num,
 apx.acp_id,
 apx.dw_sys_updt_tmstp,
 apx.dw_batch_date,
 apx.dw_sys_load_tmstp
FROM (SELECT DISTINCT olcf.order_num,
   olcf.shopper_id
  FROM (SELECT DISTINCT order_num,
     shopper_id
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.order_line_cancel_fact
    WHERE dw_batch_date >= (SELECT DATE_SUB(curr_batch_date, INTERVAL 5 DAY)
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
       WHERE LOWER(subject_area_nm) = LOWER('ORDER_CANCEL'))
     AND acp_id IS NULL) AS olcf
   LEFT JOIN order_acp_stg AS stg ON LOWER(olcf.order_num) = LOWER(stg.order_num)
  WHERE stg.order_num IS NULL) AS odr
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.acp_analytical_program_xref AS apx ON LOWER(odr.shopper_id) = LOWER(apx.program_index_id)
WHERE LOWER(apx.program_name) = LOWER('WEB');


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS order_shopper_acp_chg_stg
AS
SELECT odr.order_num,
 apx.acp_id,
 apx.dw_sys_updt_tmstp,
 apx.dw_batch_date,
 apx.dw_sys_load_tmstp
FROM (SELECT DISTINCT olcf.order_num,
   olcf.shopper_id
  FROM (SELECT DISTINCT order_num,
     shopper_id
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.order_line_cancel_fact) AS olcf
   LEFT JOIN order_acp_stg AS stg ON LOWER(olcf.order_num) = LOWER(stg.order_num)
  WHERE stg.order_num IS NULL) AS odr
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.acp_analytical_program_xref AS apx ON LOWER(odr.shopper_id) = LOWER(apx.program_index_id)
WHERE LOWER(apx.program_name) IN (LOWER('WEB'), LOWER('NRHL'))
 AND apx.dw_batch_date = (SELECT curr_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('CUSTOMER_DIM'));
   
   
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

INSERT INTO order_acp_stg
(SELECT order_num,
  acp_id
 FROM (SELECT order_num,
     acp_id,
     dw_batch_date,
     dw_sys_load_tmstp,
     dw_sys_updt_tmstp
    FROM order_shopper_acp_stg
    UNION DISTINCT
    SELECT order_num,
     acp_id,
     dw_batch_date,
     dw_sys_load_tmstp,
     dw_sys_updt_tmstp
    FROM order_shopper_acp_chg_stg) AS tmp
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY order_num ORDER BY dw_sys_updt_tmstp DESC, dw_batch_date DESC,
       dw_sys_load_tmstp DESC)) = 1);
       
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.order_line_cancel_fact AS trg
SET 
    trg.acp_id = src.acp_id,
    trg.acp_id_updt_date = CURRENT_DATE('PST8PDT'),
    trg.dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
FROM order_acp_stg AS src
WHERE LOWER(trg.order_num) = LOWER(src.order_num);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
