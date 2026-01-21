SELECT timestamp(current_datetime('PST8PDT'));

CREATE temp TABLE deterministic_customer_profile_association_max{{params.tbl_sfx}}
 AS
(
  SELECT 
    deterministicprofileid,
    MAX(header_eventTime_utc) AS m_header_eventTime,
    ANY_VALUE(header_eventTime_tz HAVING MIN header_eventTime_utc)  m_header_eventTime_tz
  FROM 
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.deterministic_customer_profile_association_wrk{{params.tbl_sfx}} -- Replace with the correct table path
  GROUP BY 
    deterministicprofileid
);



CREATE temp TABLE deterministic_customer_profile_tombstone{{params.tbl_sfx}}
AS
(
  -- Profile tombstone records:
  SELECT DISTINCT sum_wrk.deterministicprofileid
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.deterministic_customer_summary_wrk{{params.tbl_sfx}} AS sum_wrk
  INNER JOIN deterministic_customer_profile_association_max{{params.tbl_sfx}} AS asso_wrk_max
    ON sum_wrk.deterministicprofileid = asso_wrk_max.deterministicprofileid
    AND sum_wrk.header_eventTime_utc = asso_wrk_max.m_header_eventTime
  WHERE sum_wrk.uniquesourceid IS NULL
    AND sum_wrk.enterpriseretailtransaction_count = 0
    AND sum_wrk.financialretailtransaction_count = 0
)
;





DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.deterministic_customer_classification_dim{{params.tbl_sfx}} AS dim WHERE EXISTS (
  SELECT
      1
    FROM
      --  {tbl_sfx}
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.deterministic_customer_classification_wrk{{params.tbl_sfx}} AS wrk
    WHERE dim.deterministic_profile_id = wrk.deterministicprofileid
);
--  {tbl_sfx}
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.deterministic_customer_classification_dim{{params.tbl_sfx}} (deterministic_profile_id, classifications_verification_status, classifications_partner_pool, profile_event_tmstp,profile_event_tmstp_tz, dw_batch_date, dw_sys_load_tmstp, dw_sys_updt_tmstp, dw_batch_id)
  SELECT
      -- {tbl_sfx}
      wrk.deterministicprofileid AS deterministic_profile_id,
      wrk.classifications_verificationstatus AS classifications_verification_status,
      wrk.classifications_partnerpool AS classifications_partner_pool,
      wrk.header_eventtime_utc AS profile_event_tmstp,
      wrk.header_eventtime_tz as profile_event_tmstp_tz,
      ctrl.curr_batch_date AS dw_batch_date,
      coalesce(wrk.dw_sys_load_tmstp, current_datetime('PST8PDT')) AS dw_sys_load_tmstp,
      current_datetime('PST8PDT') AS dw_sys_updt_tmstp,
      ctrl.batch_id AS dw_batch_id
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.deterministic_customer_classification_wrk{{params.tbl_sfx}} AS wrk
      CROSS JOIN --  {tbl_sfx}
      (
        SELECT
            elt_control.curr_batch_date,
            elt_control.batch_id
          FROM
           `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
          WHERE lower(elt_control.subject_area_nm) = lower('DETERMINISTIC_CUSTOMER_PROFILE{{params.tbl_sfx}}')
      ) AS ctrl
    WHERE lower(coalesce(wrk.classifications_verificationstatus, 'null')) <> lower('NULL')
     OR lower(coalesce(wrk.classifications_partnerpool, 'null')) <> lower('NULL')
;







DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.deterministic_customer_profile_association_dim{{params.tbl_sfx}} AS dim WHERE EXISTS (
  SELECT
      1
    FROM
      --  {tbl_sfx}
      deterministic_customer_profile_association_max{{params.tbl_sfx}} AS wrk_max
    WHERE lower(dim.deterministic_profile_id) = lower(wrk_max.deterministicprofileid)
);
--  {tbl_sfx}
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.deterministic_customer_profile_association_dim{{params.tbl_sfx}} (deterministic_profile_id, unique_source_id, business_day_date, profile_event_tmstp,profile_event_tmstp_tz,
 dw_batch_date, dw_sys_load_tmstp, dw_sys_updt_tmstp, dw_batch_id)
  SELECT DISTINCT
      -- {tbl_sfx}
      wrk.deterministicprofileid AS deterministic_profile_id,
      -- Set uniquesourceid as deterministicprofileid if it's NULL
      coalesce(wrk.uniquesourceid, wrk.deterministicprofileid) AS unique_source_id,
      wrk.businessdaydate AS business_day_date,
      wrk.header_eventtime_utc AS profile_event_tmstp,
      wrk.header_eventtime_tz as profile_event_tmstp_tz,
      ctrl.curr_batch_date AS dw_batch_date,
      coalesce(wrk.dw_sys_load_tmstp,current_datetime('PST8PDT')) AS dw_sys_load_tmstp,
      current_datetime('PST8PDT') AS dw_sys_updt_tmstp,
      ctrl.batch_id AS dw_batch_id
    FROM
     `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.deterministic_customer_profile_association_wrk{{params.tbl_sfx}} AS wrk
      LEFT OUTER JOIN --  {tbl_sfx}
      deterministic_customer_profile_tombstone{{params.tbl_sfx}} AS ts_pr ON wrk.deterministicprofileid = ts_pr.deterministicprofileid
      CROSS JOIN --  {tbl_sfx}
      (
        SELECT
            elt_control.curr_batch_date,
            elt_control.batch_id
          FROM
           `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
          WHERE lower(elt_control.subject_area_nm) = lower('DETERMINISTIC_CUSTOMER_PROFILE{tbl_sfx}')
      ) AS ctrl
    WHERE ts_pr.deterministicprofileid IS NULL
;




DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.pending_deterministic_customer_dim{{params.tbl_sfx}} AS dim_pending WHERE EXISTS (
  SELECT
      1
    FROM
      --  {tbl_sfx}
      deterministic_customer_profile_association_max AS wrk_max
    WHERE lower(dim_pending.deterministic_profile_id) = lower(wrk_max.deterministicprofileid)
);
--  {tbl_sfx}
-- Insert Profile ids which are refreshed in Association DIM along with the once which needs to be deleted
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.pending_deterministic_customer_dim{{params.tbl_sfx}} 
  SELECT
      -- {tbl_sfx}
      wrk_max.deterministicprofileid AS deterministic_profile_id,
      wrk_max.m_header_eventtime AS profile_event_tmstp,
      wrk_max.m_header_eventtime_tz as profile_event_tmstp_tz,
      CASE
        WHEN ts_pr.deterministicprofileid IS NOT NULL THEN 'DEL'
        ELSE 'DML'
      END AS change_flag,
      ctrl.curr_batch_date AS dw_batch_date,
      current_datetime('PST8PDT') AS dw_sys_load_tmstp,
      current_datetime('PST8PDT') AS dw_sys_updt_tmstp,
      ctrl.batch_id AS dw_batch_id
    FROM
      deterministic_customer_profile_association_max{{params.tbl_sfx}} AS wrk_max
      LEFT OUTER JOIN --  {tbl_sfx}
      deterministic_customer_profile_tombstone{{params.tbl_sfx}} AS ts_pr ON wrk_max.deterministicprofileid = ts_pr.deterministicprofileid
      CROSS JOIN --  {tbl_sfx}
      (
        SELECT
            elt_control.curr_batch_date,
            elt_control.batch_id
          FROM
           `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
          WHERE lower(elt_control.subject_area_nm) = lower('DETERMINISTIC_CUSTOMER_PROFILE{tbl_sfx}')
      ) AS ctrl
;
-- Insert Profile ids which are only refreshed in Classification DIM and not association DIM
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.pending_deterministic_customer_dim{{params.tbl_sfx}} 
  SELECT
      -- {tbl_sfx}
      wrk.deterministicprofileid AS deterministic_profile_id,
      wrk.header_eventtime_utc AS profile_event_tmstp,
      wrk.header_eventtime_tz as profile_event_tmstp_tz,
      'DML' AS change_flag,
      ctrl.curr_batch_date AS dw_batch_date,
      current_datetime('PST8PDT') AS dw_sys_load_tmstp,
      current_datetime('PST8PDT') AS dw_sys_updt_tmstp,
      ctrl.batch_id AS dw_batch_id
    FROM
     `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.deterministic_customer_classification_wrk{{params.tbl_sfx}} AS wrk
      LEFT OUTER JOIN --  {tbl_sfx}
     `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.pending_deterministic_customer_dim{{params.tbl_sfx}} AS pending ON wrk.deterministicprofileid = pending.deterministic_profile_id
      CROSS JOIN --  {tbl_sfx}
      (
        SELECT
            elt_control.curr_batch_date,
            elt_control.batch_id
          FROM
           `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
          WHERE lower(elt_control.subject_area_nm) = lower('DETERMINISTIC_CUSTOMER_PROFILE{TBL_SFX}')
      ) AS ctrl
    WHERE pending.deterministic_profile_id IS NULL
;

SELECT timestamp(current_datetime('PST8PDT'));