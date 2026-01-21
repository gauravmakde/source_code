BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP07324;
DAG_ID=nordstrom_on_digital_styling_11521_ACE_ENG;
---     Task_Name=nordstrom_on_digital_daily;'*/
---     FOR SESSION VOLATILE;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS styleboards_all
--CLUSTER BY board_created_pst
AS
SELECT MIN(activity_date) AS activity_date,
 board_id,
 MIN(CASE
   WHEN board_type IS NULL
   THEN 'UNKNOWN'
   ELSE board_type
   END) AS board_type,
 MIN(COALESCE(board_created_pst, board_sent_pst)) AS board_created_pst,
 MIN(board_sent_pst) AS board_sent_pst,
 LPAD(board_curator_id, 15, '0') AS board_curator_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.curation_fact AS a
WHERE activity_date >= DATE '2021-09-21'
 AND COALESCE(board_created_pst, board_sent_pst) IS NOT NULL
 AND board_curator_id IS NOT NULL
GROUP BY board_id,
 board_curator_id;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS styleboards
--CLUSTER BY board_created_pst
AS
SELECT activity_date,
 board_id,
 board_type,
 board_created_pst,
 board_sent_pst,
 board_curator_id
FROM styleboards_all
WHERE CAST(board_created_pst AS DATE) BETWEEN DATE_SUB(current_date('PST8PDT'), INTERVAL 100 DAY) AND (DATE_SUB(current_date('PST8PDT'),
    INTERVAL 1 DAY));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS styleboard_orders AS WITH solf AS (SELECT order_line_id,
   order_number,
   MIN(board_id) AS board_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.curation_order_line_fact
  GROUP BY order_line_id,
   order_number) (SELECT f.order_date_pacific AS order_date_pst,
   f.order_tmstp_pacific AS order_tmstp_pst,
   f.order_num,
   f.order_line_id,
   f.rms_sku_num,
   f.source_channel_country_code,
   f.order_line_amount_usd AS demand,
   f.order_line_quantity AS units,
   LPAD(TRIM(f.upc_code), 15, '0') AS padded_upc,
   ROW_NUMBER() OVER (PARTITION BY f.order_num, f.upc_code ORDER BY f.upc_code) AS upc_seq_num,
    CASE
    WHEN LOWER(f.requested_level_of_service_code) = LOWER('07')
    THEN 1
    WHEN LOWER(UPPER(f.delivery_method_code)) = LOWER('PICK')
    THEN 1
    ELSE NULL
    END AS order_pickup_ind,
    CASE
    WHEN LOWER(f.requested_level_of_service_code) = LOWER('11')
    THEN 1
    WHEN f.destination_node_num > 0 AND LOWER(UPPER(f.delivery_method_code)) <> LOWER('PICK') AND LOWER(f.requested_level_of_service_code
       ) = LOWER('42')
    THEN 1
    WHEN f.destination_node_num BETWEEN 0 AND 799 AND LOWER(UPPER(f.delivery_method_code)) <> LOWER('PICK')
    THEN 1
    ELSE NULL
    END AS ship_to_store_ind,
    CASE
    WHEN a.board_id IS NOT NULL
    THEN 1
    ELSE NULL
    END AS attributed_item_ind,
   a.board_id,
   MAX(a.board_id) OVER (PARTITION BY f.order_num ORDER BY f.order_num RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS adj_board_id,
   MAX(f.source_platform_code) OVER (PARTITION BY f.order_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS platform
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.order_line_detail_fact AS f
   LEFT JOIN solf AS a ON LOWER(a.order_number) = LOWER(f.order_num) AND LOWER(a.order_line_id) = LOWER(f.order_line_id
      )
  WHERE f.order_date_pacific >= DATE '2021-09-21'
   AND f.order_date_pacific >= DATE_SUB(current_date('PST8PDT'), INTERVAL 100 DAY)
  QUALIFY (MAX(a.board_id) OVER (PARTITION BY f.order_num ORDER BY f.order_num RANGE BETWEEN UNBOUNDED PRECEDING AND
     UNBOUNDED FOLLOWING)) IS NOT NULL);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM `{{params.gcp_project_id}}`.t2dl_das_remote_selling.nordstrom_on_digital_daily
WHERE board_created_date >= DATE_SUB(current_date('PST8PDT'), INTERVAL 100 DAY) AND board_created_date <= DATE_SUB(current_date('PST8PDT'), INTERVAL 1 DAY);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.t2dl_das_remote_selling.nordstrom_on_digital_daily
(SELECT COALESCE(COALESCE(b.board_id, b.adj_board_id), a.board_id) AS board_id,
  a.board_curator_id,
  hr.employee_name,
  SUBSTR(hr.payroll_store, 1, 5) AS payroll_store,
  hr.payroll_store_description,
  hr.payroll_department_description,
  hr.region_num AS employee_payroll_store_region_num,
  hr.region_desc AS employee_payroll_region_desc,
  CAST(a.board_created_pst AS DATE) AS board_created_date,
  CAST(a.board_sent_pst AS DATE) AS board_sent_date,
  b.order_date_pst AS board_order_date,
  b.order_tmstp_pst,
  b.platform,
  a.board_type,
   CASE
   WHEN a.board_sent_pst IS NOT NULL
   THEN a.board_id
   ELSE NULL
   END AS board_id_sent,
  b.attributed_item_ind,
  b.order_num AS order_number,
  b.order_line_id,
  b.rms_sku_num,
  b.padded_upc,
  b.upc_seq_num,
  b.source_channel_country_code,
  ROUND(CAST(b.demand AS NUMERIC), 2) AS demand,
  CAST(b.units AS NUMERIC) AS units,
  b.order_pickup_ind,
  b.ship_to_store_ind,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM styleboards AS a
  LEFT JOIN styleboard_orders AS b ON LOWER(a.board_id) = LOWER(COALESCE(b.board_id, b.adj_board_id))
  LEFT JOIN (SELECT LPAD(a0.worker_number, 15, '0') AS worker_number,
    CONCAT(nme.last_name, ', ', nme.first_name) AS employee_name,
    LTRIM(a0.payroll_store, '0') AS payroll_store,
    stor.store_name AS payroll_store_description,
    dpt.organization_description AS payroll_department_description,
    stor.region_num,
    stor.region_desc,
    a0.eff_begin_date AS eff_begin_tmstp,
    DATETIME_SUB(a0.eff_end_date, INTERVAL 1 SECOND) AS eff_end_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_hr_usr_vws.hr_org_details_dim_eff_date_vw AS a0
    LEFT JOIN (SELECT a1.worker_number,
      a1.last_name,
      a1.first_name
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_hr_usr_vws.hr_worker_v1_dim AS a1
      INNER JOIN (SELECT worker_number,
        MAX(last_updated) AS max_date
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_hr_usr_vws.hr_worker_v1_dim
       GROUP BY worker_number) AS b ON LOWER(a1.worker_number) = LOWER(b.worker_number) AND a1.last_updated = b.max_date
        ) AS nme ON LOWER(a0.worker_number) = LOWER(nme.worker_number)
    LEFT JOIN (SELECT store_num,
      store_name,
      store_type_code,
      region_num,
      region_desc
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim
     WHERE LOWER(store_type_code) IN (LOWER('FL'), LOWER('RK'), LOWER('NL'))) AS stor ON LOWER(SUBSTR(LTRIM(a0.payroll_store
        , '0'), 1, 10)) = LOWER(SUBSTR(CAST(stor.store_num AS STRING), 1, 10))
    LEFT JOIN (SELECT organization_code,
      organization_description
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_hr_usr_vws.hr_worker_org_dim
     WHERE LOWER(organization_type) = LOWER('DEPARTMENT')
      AND CAST(is_inactive AS FLOAT64) = 0) AS dpt ON LOWER(a0.payroll_department) = LOWER(dpt.organization_code)) AS hr
  ON LOWER(a.board_curator_id) = LOWER(hr.worker_number) AND a.board_created_pst >= hr.eff_begin_tmstp AND a.board_created_pst
     < hr.eff_end_tmstp
 WHERE CAST(a.board_created_pst AS DATE) >= DATE_SUB(current_date('PST8PDT'), INTERVAL 100 DAY)
  AND CAST(a.board_created_pst AS DATE) <= DATE_SUB(current_date('PST8PDT'), INTERVAL 1 DAY));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS  COLUMN (board_id), COLUMN (PARTITION), COLUMN (board_created_date) ON T2DL_DAS_REMOTE_SELLING.NORDSTROM_ON_DIGITAL_DAILY;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
