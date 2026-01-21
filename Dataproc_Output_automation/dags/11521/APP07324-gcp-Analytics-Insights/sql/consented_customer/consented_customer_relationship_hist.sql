BEGIN DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP09267;
DAG_ID=ddl_consented_customer_relationship_hist_11521_ACE_ENG;
---     Task_Name=ddl_consented_customer_relationship_hist;'*/
---     FOR SESSION VOLATILE;
BEGIN 
SET 
  ERROR_CODE = 0;


CREATE TEMPORARY TABLE IF NOT EXISTS dat as 
SELECT 
  DISTINCT consented_action_request_date_pst, 
  consented_action_request_timestamp_pst, 
  customer_id, 
  customer_id_type, 
  seller_id, 
  consented_action_request_type, 
  'NEW UAT' AS tabletype 
FROM 
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.consented_clienteling_relationship_fact AS a;
EXCEPTION WHEN ERROR THEN 
SET 
  ERROR_CODE = 1;
SET 
  ERROR_MESSAGE= @@error.message;
END;
BEGIN 
SET 
  ERROR_CODE = 0;


CREATE TEMPORARY TABLE IF NOT EXISTS ocp1  AS 
SELECT * FROM (SELECT 
  DISTINCT a.icon_id AS provided_icon_id, 
  c.acp_id, 
  c.icon_id, 
  COALESCE(ocp.ocp_id, c.icon_id) AS ocp_id, 

  COALESCE(
    CAST(ocp.event_timestamp AS DATE), 
    DATE '1901-01-01'
  ) AS event_timestamp,

  ocp.event_timestamp_tz,

  RANK() OVER (
    PARTITION BY c.acp_id 
    ORDER BY 
      ocp.event_timestamp DESC
  ) AS rank_num 
FROM 
  (
    SELECT 
      DISTINCT customer_id AS icon_id 
    FROM 
      dat
  ) AS a 
  LEFT JOIN (
    SELECT 
      acp_id, 
      cust_id AS icon_id 
    FROM 
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.acp_analytical_cust_xref 
    WHERE 
      LOWER(cust_source) = LOWER('icon')
  ) AS b ON LOWER(a.icon_id) = LOWER(b.icon_id) 
  LEFT JOIN (
    SELECT 
      acp_id, 
      cust_id AS icon_id 
    FROM 
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.acp_analytical_cust_xref 
    WHERE 
      LOWER(cust_source) = LOWER('icon')
  ) AS c ON LOWER(b.acp_id) = LOWER(c.acp_id) 
  LEFT JOIN (
    SELECT 
      unique_source_id AS customer_id, 
      program_index_id AS ocp_id, 
      object_event_tmstp AS event_timestamp ,
      object_event_tmstp_tz as event_timestamp_tz
    FROM 
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_obj_program_dim 
    WHERE 
      LOWER(customer_source) = LOWER('icon') 
      AND LOWER(program_index_name) = LOWER('OCP')
  ) AS ocp ON LOWER('icon::' || c.icon_id) = LOWER(ocp.customer_id) )
WHERE 
  rank_num = 1;



EXCEPTION WHEN ERROR THEN 
SET 
  ERROR_CODE = 1;
SET 
  ERROR_MESSAGE= @@error.message;
END;
--COLLECT STATISTICS COLUMN (ocp_id) ON ocp1;
BEGIN 
SET 
  ERROR_CODE = 0;



CREATE TEMPORARY TABLE IF NOT EXISTS current_loyalty AS 
SELECT 
  a.provided_icon_id, 
  a.acp_id, 
  a.ocp_id, 
  copd.loyalty_id, 
  lty.rewards_level 
FROM 
  ocp1 AS a 
  LEFT JOIN (
    SELECT 
      unique_source_id, 
      program_index_id AS loyalty_id 
    FROM 
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_obj_program_dim 
    WHERE 
      LOWER(program_index_name) = LOWER('MTLYLTY')
  ) AS copd ON LOWER('icon::' || a.ocp_id) = LOWER(copd.unique_source_id) 
  LEFT JOIN (
    SELECT 
      loyalty_id, 
      rewards_level 
    FROM 
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.loyalty_member_dim_vw 
    WHERE 
      CURRENT_DATE('PST8PDT') BETWEEN rewards_level_start_date 
      AND rewards_level_end_date
  ) AS lty ON LOWER(copd.loyalty_id) = LOWER(lty.loyalty_id);


EXCEPTION WHEN ERROR THEN 
SET 
  ERROR_CODE = 1;
SET 
  ERROR_MESSAGE= @@error.message;
END;
BEGIN 
SET 
  ERROR_CODE = 0;

CREATE TEMPORARY TABLE IF NOT EXISTS distinct_xref  AS 
SELECT 
  DISTINCT unique_source_id, 
  tokenized_mobile, 
  tokenized_email_id 
FROM 
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_wholesession_dim;
EXCEPTION WHEN ERROR THEN 
SET 
  ERROR_CODE = 1;
SET 
  ERROR_MESSAGE= @@error.message;
END;
--COLLECT STATISTICS COLUMN (unique_source_id) ON distinct_xref;
BEGIN 
SET 
  ERROR_CODE = 0;
CREATE TEMPORARY TABLE IF NOT EXISTS gc_list AS 
SELECT 
  * 
FROM 
  current_loyalty 
  LEFT JOIN distinct_xref ON LOWER(
    'icon::' || current_loyalty.ocp_id
  ) = LOWER(distinct_xref.unique_source_id);

EXCEPTION WHEN ERROR THEN 
SET 
  ERROR_CODE = 1;
SET 
  ERROR_MESSAGE= @@error.message;
END;
--COLLECT STATISTICS COLUMN (tokenized_email_id, tokenized_mobile) ON gc_list;
BEGIN 
SET 
  ERROR_CODE = 0;
CREATE TEMPORARY TABLE IF NOT EXISTS tokenized_phone  AS 
SELECT 
  t1.tokenized_value, 
  t2.preference_value 
FROM 
  (
    SELECT 
      * 
    FROM 
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.cpp_preference_master_dim 
    WHERE 
      LOWER(preference_type) = LOWER('TELEPHONE')
  ) AS t1 
  INNER JOIN (
    SELECT 
      * 
    FROM 
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.cpp_preference_contact_dim 
    WHERE 
      LOWER(preference_name) = LOWER('MANUAL')
  ) AS t2 ON LOWER(t1.enterprise_id) = LOWER(t2.enterprise_id) 
WHERE 
  t1.tokenized_value IN (
    SELECT 
      tokenized_mobile 
    FROM 
      gc_list
  );
EXCEPTION WHEN ERROR THEN 
SET 
  ERROR_CODE = 1;
SET 
  ERROR_MESSAGE= @@error.message;
END;
BEGIN 
SET 
  ERROR_CODE = 0;
CREATE TEMPORARY TABLE IF NOT EXISTS tokenized_email AS 
SELECT 
  t1.tokenized_value, 
  t2.preference_value 
FROM 
  (
    SELECT 
      * 
    FROM 
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.cpp_preference_master_dim 
    WHERE 
      LOWER(preference_type) = LOWER('EMAIL')
  ) AS t1 
  INNER JOIN (
    SELECT 
      * 
    FROM 
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.cpp_preference_contact_dim 
    WHERE 
      LOWER(preference_name) = LOWER('CONTACT')
  ) AS t2 ON LOWER(t1.enterprise_id) = LOWER(t2.enterprise_id) 
WHERE 
  t1.tokenized_value IN (
    SELECT 
      tokenized_email_id 
    FROM 
      gc_list
  );
EXCEPTION WHEN ERROR THEN 
SET 
  ERROR_CODE = 1;
SET 
  ERROR_MESSAGE= @@error.message;
END;
--COLLECT STATISTICS COLUMN (tokenized_value) ON tokenized_phone;
--COLLECT STATISTICS COLUMN (tokenized_value) ON tokenized_email;
BEGIN 
SET 
  ERROR_CODE = 0;
CREATE TEMPORARY TABLE IF NOT EXISTS market_table  AS 
SELECT 
  gc.provided_icon_id, 
  gc.acp_id, 
  gc.ocp_id, 
  gc.loyalty_id, 
  gc.rewards_level, 
  gc.unique_source_id, 
  gc.tokenized_mobile, 
  gc.tokenized_email_id, 
  MAX(em.preference_value) AS em_preference_value, 
  MAX(ph.preference_value) AS ph_preference_value 
FROM 
  gc_list AS gc 
  LEFT JOIN tokenized_email AS em ON LOWER(gc.tokenized_email_id) = LOWER(em.tokenized_value) 
  LEFT JOIN tokenized_phone AS ph ON LOWER(gc.tokenized_mobile) = LOWER(ph.tokenized_value) 
GROUP BY 
  gc.provided_icon_id, 
  gc.acp_id, 
  gc.ocp_id, 
  gc.loyalty_id, 
  gc.rewards_level, 
  gc.unique_source_id, 
  gc.tokenized_mobile, 
  gc.tokenized_email_id;
EXCEPTION WHEN ERROR THEN 
SET 
  ERROR_CODE = 1;
SET 
  ERROR_MESSAGE= @@error.message;
END;
BEGIN 
SET 
  ERROR_CODE = 0;

CREATE TEMPORARY TABLE IF NOT EXISTS pre_attributes  AS 
select * from (SELECT 
  DISTINCT provided_icon_id, 
  acp_id, 
  rewards_level, 
  em_preference_value, 
  ph_preference_value, 
  RANK() OVER (
    PARTITION BY provided_icon_id 
    ORDER BY 
      CASE WHEN LOWER(ph_preference_value) = LOWER('Y') THEN 3 WHEN LOWER(ph_preference_value) = LOWER('N') THEN 2 WHEN LOWER(ph_preference_value) = LOWER('NA') THEN 1 ELSE NULL END DESC, 
      em_preference_value DESC, 
      rewards_level DESC, 
      ocp_id DESC
  ) AS rank_num 
FROM 
  market_table AS a )
WHERE 
  rank_num = 1;


EXCEPTION WHEN ERROR THEN 
SET 
  ERROR_CODE = 1;
SET 
  ERROR_MESSAGE= @@error.message;
END;
--COLLECT STATISTICS COLUMN (provided_icon_id, acp_id) ON pre_attributes;
BEGIN 
SET 
  ERROR_CODE = 0;
CREATE TEMPORARY TABLE IF NOT EXISTS consented_acp  AS 
SELECT 
  a.consented_action_request_date_pst, 
  a.consented_action_request_timestamp_pst, 
  a.customer_id, 
  a.customer_id_type, 
  a.seller_id, 
  a.consented_action_request_type, 
  MAX(b.acp_id) AS acp_id 
FROM 
  dat AS a 
  INNER JOIN pre_attributes AS b ON LOWER(a.customer_id) = LOWER(b.provided_icon_id) 
GROUP BY 
  a.consented_action_request_date_pst, 
  a.consented_action_request_timestamp_pst, 
  a.customer_id, 
  a.customer_id_type, 
  a.seller_id, 
  a.consented_action_request_type;
EXCEPTION WHEN ERROR THEN 
SET 
  ERROR_CODE = 1;
SET 
  ERROR_MESSAGE= @@error.message;
END;
--COLLECT STATISTICS COLUMN (acp_id) ON consented_acp;
BEGIN 
SET 
  ERROR_CODE = 0;
CREATE TEMPORARY TABLE IF NOT EXISTS consented_acp_clean  AS 
SELECT 
  consented_action_request_date_pst, 
  consented_action_request_timestamp_pst, 
  consented_action_request_type, 
  acp_id, 
  seller_id 
FROM 
  (
    SELECT 
      * 
    FROM 
      consented_acp 
    EXCEPT 
      DISTINCT 
    SELECT 
      consented_action_request_date_pst, 
      consented_action_request_timestamp_pst, 
      customer_id, 
      customer_id_type, 
      seller_id, 
      consented_action_request_type, 
      acp_id 
    FROM 
      (
        SELECT 
          * 
        FROM 
          consented_acp QUALIFY (
            ROW_NUMBER() OVER (
              PARTITION BY acp_id, 
              seller_id 
              ORDER BY 
                consented_action_request_timestamp_pst
            )
          ) = 1
      ) AS b 
    WHERE 
      LOWER(consented_action_request_type) = LOWER('OPT_OUT')
  ) AS t3;
EXCEPTION WHEN ERROR THEN 
SET 
  ERROR_CODE = 1;
SET 
  ERROR_MESSAGE= @@error.message;
END;
--COLLECT STATISTICS COLUMN (acp_id) ON consented_acp_clean;
BEGIN 
SET 
  ERROR_CODE = 0;


 
  
CREATE TEMPORARY TABLE IF NOT EXISTS consented_acp_clean2  AS 
SELECT 
  DISTINCT acp_id, 
  seller_id, 
  consented_action_request_date_pst, 
  consented_action_request_type, 
  next_action_date_pst, 
  next_action_type, 
  ROW_NUMBER() OVER (
    PARTITION BY acp_id, 
    seller_id 
    ORDER BY 
      consented_action_request_date_pst
  ) AS acp_seller_action_row_num 
FROM 
  (
    select * from (
	SELECT 
      DISTINCT acp_id, 
      seller_id, 
      consented_action_request_date_pst, 
      consented_action_request_timestamp_pst, 
      consented_action_request_type, 
      LEAD(
        consented_action_request_date_pst, 
        1, DATE '9999-12-31'
      ) OVER (
        PARTITION BY acp_id, 
        seller_id 
        ORDER BY 
          consented_action_request_timestamp_pst
      ) AS next_action_date_pst, 
      LEAD(
        consented_action_request_timestamp_pst, 
        1, DATE '9999-12-31'
      ) OVER (
        PARTITION BY acp_id, 
        seller_id 
        ORDER BY 
          consented_action_request_timestamp_pst
      ) AS next_action_timestamp_pst, 
      LEAD(
        consented_action_request_type, 1, 
        'tbd'
      ) OVER (
        PARTITION BY acp_id, 
        seller_id 
        ORDER BY 
          consented_action_request_timestamp_pst
      ) AS next_action_type 
    FROM 
      (
        SELECT 
          acp_id, 
          seller_id, 
          consented_action_request_date_pst, 
          consented_action_request_timestamp_pst, 
          consented_action_request_type, 
          LAG(
            consented_action_request_type, 1, 
            'unkn'
          ) OVER (
            PARTITION BY acp_id, 
            seller_id 
            ORDER BY 
              consented_action_request_timestamp_pst
          ) AS prev_action_type, 
          CASE WHEN LOWER(consented_action_request_type) = LOWER(
            LAG(
              consented_action_request_type, 1, 
              'unkn'
            ) OVER (
              PARTITION BY acp_id, 
              seller_id 
              ORDER BY 
                consented_action_request_timestamp_pst
            )
          ) THEN 1 ELSE 0 END AS mltpl_optinout 
        FROM 
          consented_acp_clean QUALIFY CASE WHEN LOWER(consented_action_request_type) = LOWER(
            LAG(
              consented_action_request_type, 1, 
              'unkn'
            ) OVER (
              PARTITION BY acp_id, 
              seller_id 
              ORDER BY 
                consented_action_request_timestamp_pst
            )
          ) THEN 1 ELSE 0 END = 0
      ) AS t0 )
    WHERE 
      LOWER(consented_action_request_type) = LOWER('OPT_IN') 
      AND LOWER(next_action_type) IN (
        LOWER('OPT_OUT'), 
        LOWER('tbd')
      )
  ) AS t3;



EXCEPTION WHEN ERROR THEN 
SET 
  ERROR_CODE = 1;
SET 
  ERROR_MESSAGE= @@error.message;
END;
--COLLECT STATISTICS COLUMN (acp_id) ON consented_acp_clean2;
BEGIN 
SET 
  ERROR_CODE = 0;
  
CREATE TEMPORARY TABLE IF NOT EXISTS consented_numbered  AS 
SELECT 
  acp_id, 
  seller_id, 
  consented_action_request_type, 
  consented_action_request_date_pst, 
  next_action_type, 
  next_action_date_pst, 
  acp_seller_action_row_num, 
  MIN(
    consented_action_request_date_pst
  ) OVER (
    PARTITION BY acp_id, 
    seller_id RANGE BETWEEN UNBOUNDED PRECEDING 
    AND UNBOUNDED FOLLOWING
  ) AS first_seller_consent_dt, 
  MIN(
    consented_action_request_date_pst
  ) OVER (
    PARTITION BY acp_id RANGE BETWEEN UNBOUNDED PRECEDING 
    AND UNBOUNDED FOLLOWING
  ) AS first_clienteling_consent_dt, 
  ROW_NUMBER() OVER (
    PARTITION BY acp_id 
    ORDER BY 
      consented_action_request_date_pst, 
      seller_id, 
      next_action_date_pst
  ) AS row_num 
FROM 
  consented_acp_clean2;
EXCEPTION WHEN ERROR THEN 
SET 
  ERROR_CODE = 1;
SET 
  ERROR_MESSAGE= @@error.message;
END;
--COLLECT STATISTICS COLUMN (acp_id) ON consented_numbered;
BEGIN 
SET 
  ERROR_CODE = 0;
  
CREATE TEMPORARY TABLE IF NOT EXISTS consented_history  AS 
SELECT 
  a.acp_id, 
  a.seller_id, 
  a.first_clienteling_consent_dt, 
  a.first_seller_consent_dt, 
  a.seller_num, 
  a.opt_in_date_pst, 
  a.opt_out_date_pst, 
  a.next_action_type, 
  a.acp_seller_action_row_num, 
  a.row_num, 
  a.clienteling_history, 
  a.seller_history, 
  MAX(b.rewards_level) AS rewards_level, 
  MAX(b.em_preference_value) AS em_preference_value, 
  MAX(b.ph_preference_value) AS ph_preference_value 
FROM 
  (
    SELECT 
      acp_id, 
      seller_id, 
      first_clienteling_consent_dt, 
      first_seller_consent_dt, 
      DENSE_RANK() OVER (
        PARTITION BY acp_id 
        ORDER BY 
          first_seller_consent_dt, 
          seller_id
      ) AS seller_num, 
      consented_action_request_date_pst AS opt_in_date_pst, 
      next_action_date_pst AS opt_out_date_pst, 
      next_action_type, 
      acp_seller_action_row_num, 
      row_num, 
      CASE WHEN row_num = 1 THEN 'NEW TO CLIENTELING' ELSE 'EXISTING RELN W CLIENTELING' END AS clienteling_history, 
      CASE WHEN acp_seller_action_row_num = 1 THEN 'NEW TO SELLER' ELSE 'EXISTING RELN W SELLER' END AS seller_history 
    FROM 
      consented_numbered
  ) AS a 
  LEFT JOIN pre_attributes AS b ON LOWER(a.acp_id) = LOWER(b.acp_id) 
GROUP BY 
  a.acp_id, 
  a.seller_id, 
  a.first_clienteling_consent_dt, 
  a.first_seller_consent_dt, 
  a.seller_num, 
  a.opt_in_date_pst, 
  a.opt_out_date_pst, 
  a.next_action_type, 
  a.acp_seller_action_row_num, 
  a.row_num, 
  a.clienteling_history, 
  a.seller_history;
EXCEPTION WHEN ERROR THEN 
SET 
  ERROR_CODE = 1;
SET 
  ERROR_MESSAGE= @@error.message;
END;
--COLLECT STATISTICS COLUMN (acp_id) ON consented_history;
BEGIN 
SET 
  ERROR_CODE = 0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.cli_t2_schema}}.consented_customer_relationship_hist;
EXCEPTION WHEN ERROR THEN 
SET 
  ERROR_CODE = 1;
SET 
  ERROR_MESSAGE= @@error.message;
END;
BEGIN 
SET 
  ERROR_CODE = 0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.cli_t2_schema}}.consented_customer_relationship_hist (
  SELECT 
    acp_id, 
    seller_id, 
    first_clienteling_consent_dt, 
    first_seller_consent_dt, 
    seller_num, 
    opt_in_date_pst, 
    opt_out_date_pst, 
    clienteling_history, 
    seller_history, 
    rewards_level, 
    em_preference_value AS email_pref_cur, 
    ph_preference_value AS phone_pref_cur, 
    CAST(
      FORMAT_TIMESTAMP(
        '%F %H:%M:%E6S', 
        CURRENT_DATETIME('PST8PDT')
      ) AS DATETIME
    ) AS dw_sys_load_tmstp 
  FROM 
    consented_history
);

EXCEPTION WHEN ERROR THEN 
SET 
  ERROR_CODE = 1;
SET 
  ERROR_MESSAGE= @@error.message;
END;
--COLLECT STATISTICS COLUMN (ACP_ID) ON t2dl_das_clienteling.consented_customer_relationship_hist;

/*grant select on t2dl_das_clienteling.consented_customer_relationship_hist to public;*/

/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
