/*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File              : yms_lifecycle_agg_tables_load.sql
-- Author            : David Yuan
-- Description       : Merge YARD_VISIT_LIFECYCLE_FACT table with data from YARD_TRAILER_VISIT_EVENT_FACT table
-- Version :         : 0.1
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2024-09-03  David Yuan  FA-13642: Initial release of lifecycle table
--************************************************************************************************************************************/
-- this VOLATILE table return first/last non-null value for the column
CREATE TEMPORARY TABLE IF NOT EXISTS yard_trailer_first_last_known AS
SELECT DISTINCT
  visit_id,
  FIRST_VALUE (NULLIF(TRIM(trailer_num), '')) OVER (
    PARTITION BY
      visit_id
    ORDER BY
      event_time RANGE BETWEEN UNBOUNDED PRECEDING
      AND UNBOUNDED FOLLOWING
  ) AS first_known_trailer_num,
  FIRST_VALUE (NULLIF(TRIM(trailer_id), '')) OVER (
    PARTITION BY
      visit_id
    ORDER BY
      event_time RANGE BETWEEN UNBOUNDED PRECEDING
      AND UNBOUNDED FOLLOWING
  ) AS first_known_trailer_id,
  LAST_VALUE (NULLIF(TRIM(trailer_num), '')) OVER (
    PARTITION BY
      visit_id
    ORDER BY
      event_time RANGE BETWEEN UNBOUNDED PRECEDING
      AND UNBOUNDED FOLLOWING
  ) AS last_known_trailer_num,
  LAST_VALUE (NULLIF(TRIM(trailer_id), '')) OVER (
    PARTITION BY
      visit_id
    ORDER BY
      event_time RANGE BETWEEN UNBOUNDED PRECEDING
      AND UNBOUNDED FOLLOWING
  ) AS last_known_trailer_id,
  LAST_VALUE (NULLIF(TRIM(event_name), '')) OVER (
    PARTITION BY
      visit_id
    ORDER BY
      event_time RANGE BETWEEN UNBOUNDED PRECEDING
      AND UNBOUNDED FOLLOWING
  ) AS last_known_event_name,
  LAST_VALUE (NULLIF(TRIM(carrier_name), '')) OVER (
    PARTITION BY
      visit_id
    ORDER BY
      event_time RANGE BETWEEN UNBOUNDED PRECEDING
      AND UNBOUNDED FOLLOWING
  ) AS last_known_carrier_name,
  LAST_VALUE (NULLIF(TRIM(subcarrier_name), '')) OVER (
    PARTITION BY
      visit_id
    ORDER BY
      event_time RANGE BETWEEN UNBOUNDED PRECEDING
      AND UNBOUNDED FOLLOWING
  ) AS last_known_subcarrier_name,
  LAST_VALUE (NULLIF(TRIM(load_num), '')) OVER (
    PARTITION BY
      visit_id
    ORDER BY
      event_time RANGE BETWEEN UNBOUNDED PRECEDING
      AND UNBOUNDED FOLLOWING
  ) AS last_known_load_num,
  LAST_VALUE (NULLIF(TRIM(seal_num), '')) OVER (
    PARTITION BY
      visit_id
    ORDER BY
      event_time RANGE BETWEEN UNBOUNDED PRECEDING
      AND UNBOUNDED FOLLOWING
  ) AS last_known_seal_num,
  LAST_VALUE (NULLIF(TRIM(scac_code), '')) OVER (
    PARTITION BY
      visit_id
    ORDER BY
      event_time RANGE BETWEEN UNBOUNDED PRECEDING
      AND UNBOUNDED FOLLOWING
  ) AS last_known_scac_code,
  LAST_VALUE (NULLIF(TRIM(trailer_type), '')) OVER (
    PARTITION BY
      visit_id
    ORDER BY
      event_time RANGE BETWEEN UNBOUNDED PRECEDING
      AND UNBOUNDED FOLLOWING
  ) AS last_known_trailer_type,
  LAST_VALUE (NULLIF(TRIM(trailer_function), '')) OVER (
    PARTITION BY
      visit_id
    ORDER BY
      event_time RANGE BETWEEN UNBOUNDED PRECEDING
      AND UNBOUNDED FOLLOWING
  ) AS last_known_trailer_function,
  LAST_VALUE (NULLIF(TRIM(yard_name), '')) OVER (
    PARTITION BY
      visit_id
    ORDER BY
      event_time RANGE BETWEEN UNBOUNDED PRECEDING
      AND UNBOUNDED FOLLOWING
  ) AS last_known_yard_name,
  LAST_VALUE (NULLIF(TRIM(yard_trailer_location_name), '')) OVER (
    PARTITION BY
      visit_id
    ORDER BY
      event_time RANGE BETWEEN UNBOUNDED PRECEDING
      AND UNBOUNDED FOLLOWING
  ) AS last_known_yard_location,
  LAST_VALUE (NULLIF(TRIM(yard_trailer_location_zone), '')) OVER (
    PARTITION BY
      visit_id
    ORDER BY
      event_time RANGE BETWEEN UNBOUNDED PRECEDING
      AND UNBOUNDED FOLLOWING
  ) AS last_known_yard_zone,
  LAST_VALUE (NULLIF(TRIM(carrier_bill_of_lading), '')) OVER (
    PARTITION BY
      visit_id
    ORDER BY
      event_time RANGE BETWEEN UNBOUNDED PRECEDING
      AND UNBOUNDED FOLLOWING
  ) AS last_known_carrier_bill_of_lading
FROM
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.yard_trailer_visit_event_fact;

CREATE TEMPORARY TABLE IF NOT EXISTS yard_trailer_inbound_last_at_door_unload AS
SELECT
  LAST_UNLOAD.visit_id,
  LAST_AT_DOCK.appointment_id AS at_door_appointment_id,
  LAST_AT_DOCK.yard_trailer_location_name AS inbound_last_at_door_num,
  LAST_AT_DOCK.last_at_door_time,
  LAST_AT_DOCK.last_at_door_time_tz,
  LAST_UNLOAD.appointment_id AS unload_appointment_id,
  LAST_UNLOAD.last_unload_time,
  LAST_UNLOAD.last_unload_time_tz
FROM
  (
    SELECT
      visit_id,
      appointment_id,
      event_time_utc AS last_unload_time,
      event_time_tz AS last_unload_time_tz
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.yard_trailer_visit_event_fact
    WHERE
      LOWER(event_name) = LOWER('TrailerMarkedEmpty') QUALIFY (
        ROW_NUMBER() OVER (
          PARTITION BY
            visit_id
          ORDER BY
            event_time DESC
        )
      ) = 1
  ) AS LAST_UNLOAD
  LEFT JOIN (
    SELECT
      visit_id,
      appointment_id,
      yard_trailer_location_name,
      event_time_utc AS last_at_door_time,
      event_time_tz AS last_at_door_time_tz
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.yard_trailer_visit_event_fact
    WHERE
      LOWER(yard_trailer_location_type) = LOWER('DOCK')
  ) AS LAST_AT_DOCK ON LOWER(LAST_UNLOAD.visit_id) = LOWER(LAST_AT_DOCK.visit_id)
  AND LAST_UNLOAD.last_unload_time > LAST_AT_DOCK.last_at_door_time
   QUALIFY (
    ROW_NUMBER() OVER (
      PARTITION BY
        LAST_UNLOAD.visit_id
      ORDER BY
        LAST_AT_DOCK.last_at_door_time DESC
    )
  ) = 1;

CREATE TEMPORARY TABLE IF NOT EXISTS yard_trailer_inbound_first_at_door_unload AS
SELECT
  FIRST_UNLOAD.visit_id,
  FIRST_AT_DOCK.appointment_id AS at_door_appointment_id,
  FIRST_AT_DOCK.yard_trailer_location_name AS inbound_first_at_door_num,
  FIRST_AT_DOCK.first_at_door_time,
  FIRST_AT_DOCK.first_at_door_time_tz,
  FIRST_UNLOAD.appointment_id AS unload_appointment_id,
  FIRST_UNLOAD.first_unload_time,
  FIRST_UNLOAD.first_unload_time_tz
FROM
  (
    SELECT
      visit_id,
      appointment_id,
      event_time_utc AS first_unload_time,
      event_time_tz AS first_unload_time_tz
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.yard_trailer_visit_event_fact
    WHERE
      LOWER(event_name) = LOWER('TrailerMarkedEmpty') QUALIFY (
        ROW_NUMBER() OVER (
          PARTITION BY
            visit_id
          ORDER BY
            event_time
        )
      ) = 1
  ) AS FIRST_UNLOAD
  LEFT JOIN (
    SELECT
      visit_id,
      appointment_id,
      yard_trailer_location_name,
      event_time_utc AS first_at_door_time,
      event_time_tz AS first_at_door_time_tz,
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.yard_trailer_visit_event_fact
    WHERE
      LOWER(yard_trailer_location_type) = LOWER('DOCK')
  ) AS FIRST_AT_DOCK ON LOWER(FIRST_UNLOAD.visit_id) = LOWER(FIRST_AT_DOCK.visit_id)
  AND FIRST_UNLOAD.first_unload_time > FIRST_AT_DOCK.first_at_door_time QUALIFY (
    ROW_NUMBER() OVER (
      PARTITION BY
        FIRST_UNLOAD.visit_id
      ORDER BY
        FIRST_AT_DOCK.first_at_door_time
    )
  ) = 1;

CREATE TEMPORARY TABLE IF NOT EXISTS yard_trailer_outbound_last_at_door_load AS
SELECT
  LAST_LOAD.visit_id,
  LAST_AT_DOCK.appointment_id AS at_door_appointment_id,
  LAST_AT_DOCK.yard_trailer_location_name AS outbound_last_at_door_num,
  LAST_AT_DOCK.last_at_door_time,
  LAST_AT_DOCK.last_at_door_time_tz,
  LAST_LOAD.appointment_id AS load_appointment_id,
  LAST_LOAD.last_load_time,
  LAST_LOAD.last_load_time_tz
FROM
  (
    SELECT
      visit_id,
      appointment_id,
      event_time_utc AS last_load_time,
      event_time_tz AS last_load_time_tz
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.yard_trailer_visit_event_fact
    WHERE
      LOWER(event_name) = LOWER('TrailerMarkedFull') QUALIFY (
        ROW_NUMBER() OVER (
          PARTITION BY
            visit_id
          ORDER BY
            event_time DESC
        )
      ) = 1
  ) AS LAST_LOAD
  LEFT JOIN (
    SELECT
      visit_id,
      appointment_id,
      yard_trailer_location_name,
      event_time_utc AS last_at_door_time,
      event_time_tz AS last_at_door_time_tz
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.yard_trailer_visit_event_fact
    WHERE
      LOWER(yard_trailer_location_type) = LOWER('DOCK')
  ) AS LAST_AT_DOCK ON LOWER(LAST_LOAD.visit_id) = LOWER(LAST_AT_DOCK.visit_id)
  AND LAST_LOAD.last_load_time > LAST_AT_DOCK.last_at_door_time QUALIFY (
    ROW_NUMBER() OVER (
      PARTITION BY
        LAST_LOAD.visit_id
      ORDER BY
        LAST_AT_DOCK.last_at_door_time DESC
    )
  ) = 1;

CREATE TEMPORARY TABLE IF NOT EXISTS yard_trailer_outbound_first_at_door_load AS
SELECT
  FIRST_LOAD.visit_id,
  FIRST_AT_DOCK.appointment_id AS at_door_appointment_id,
  FIRST_AT_DOCK.yard_trailer_location_name AS outbound_first_at_door_num,
  FIRST_AT_DOCK.first_at_door_time,
  FIRST_AT_DOCK.first_at_door_time_tz,
  FIRST_LOAD.appointment_id AS load_appointment_id,
  FIRST_LOAD.first_load_time,
  FIRST_LOAD.first_load_time_tz
FROM
  (
    SELECT
      visit_id,
      appointment_id,
      event_time_utc AS first_load_time,
      event_time_tz AS first_load_time_tz
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.yard_trailer_visit_event_fact
    WHERE
      LOWER(event_name) = LOWER('TrailerMarkedFull') QUALIFY (
        ROW_NUMBER() OVER (
          PARTITION BY
            visit_id
          ORDER BY
            event_time
        )
      ) = 1
  ) AS FIRST_LOAD
  LEFT JOIN (
    SELECT
      visit_id,
      appointment_id,
      yard_trailer_location_name,
      event_time_utc AS first_at_door_time,
      event_time_tz AS first_at_door_time_tz
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.yard_trailer_visit_event_fact
    WHERE
      LOWER(yard_trailer_location_type) = LOWER('DOCK')
  ) AS FIRST_AT_DOCK ON LOWER(FIRST_LOAD.visit_id) = LOWER(FIRST_AT_DOCK.visit_id)
  AND FIRST_LOAD.first_load_time > FIRST_AT_DOCK.first_at_door_time QUALIFY (
    ROW_NUMBER() OVER (
      PARTITION BY
        FIRST_LOAD.visit_id
      ORDER BY
        FIRST_AT_DOCK.first_at_door_time
    )
  ) = 1;

CREATE TEMPORARY TABLE IF NOT EXISTS yard_trailer_td_tz AS
SELECT
  store_num,
  CASE
    WHEN LOWER(store_time_zone) = LOWER('MST')
    AND LOWER(store_address_state) <> LOWER('AZ') THEN 'America Mountain'
    WHEN LOWER(store_time_zone) IN (LOWER('PDT'), LOWER('PST')) THEN 'America Pacific'
    WHEN LOWER(store_time_zone) = LOWER('EST') THEN 'America Eastern'
    WHEN LOWER(store_time_zone) = LOWER('CST') THEN 'America Central'
    WHEN LOWER(store_time_zone) = LOWER('AKST') THEN 'America Alaska'
    ELSE store_time_zone_offset
  END AS td_timezone_offset
FROM
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim
GROUP BY
  store_num,
  td_timezone_offset;

CREATE TEMPORARY TABLE IF NOT EXISTS yard_trailer_invalid_visit_union AS (
  SELECT
    visit_id
  FROM
    (
      SELECT visit_id, event_time_utc,
        CASE WHEN prev_event_time IS NULL THEN 0 ELSE
        CAST(date_diff(event_time_utc, prev_event_time, HOUR) AS INT) END AS time_gap_hour
      FROM
        (SELECT visit_id, event_time_utc,
        LAG(event_time_utc) OVER (PARTITION BY visit_id ORDER BY event_time_utc ASC) as prev_event_time
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_USR_VWS.YARD_TRAILER_VISIT_EVENT_FACT
      WHERE
        LOWER(event_name) = LOWER('TrailerCheckedIntoYard'))as EG) AS TG
  GROUP BY
    visit_id
  HAVING
    SUM(time_gap_hour) > 0
  UNION DISTINCT
  SELECT
    *
  FROM
    (
      SELECT DISTINCT
        visit_id
      FROM
        `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.yard_trailer_visit_event_fact
      EXCEPT DISTINCT
      SELECT DISTINCT
        visit_id
      FROM
        `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.yard_trailer_visit_event_fact
      WHERE
        LOWER(event_name) = LOWER('TrailerCheckedIntoYard')
    )
)
UNION DISTINCT
SELECT DISTINCT
  visit_id
FROM
  (
    SELECT
      *
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.yard_trailer_visit_event_fact QUALIFY (
        ROW_NUMBER() OVER (
          PARTITION BY
            visit_id
          ORDER BY
            event_time
        )
      ) = 1
  ) AS first_event
WHERE
  LOWER(event_name) <> LOWER('TrailerCheckedIntoYard');

CREATE TEMPORARY TABLE IF NOT EXISTS yard_trailer_liftcycle_stg AS
SELECT
  ci.visit_id,
  ci.location_num,
  COALESCE(
    yard_trailer_first_last_known.first_known_trailer_num,
    ci.trailer_num,
    d.trailer_num
  ) AS first_known_trailer_num,
  COALESCE(
    yard_trailer_first_last_known.first_known_trailer_id,
    ci.trailer_id,
    d.trailer_id
  ) AS first_known_trailer_id,
  yard_trailer_first_last_known.last_known_trailer_num,
  yard_trailer_first_last_known.last_known_trailer_id,
  yard_trailer_first_last_known.last_known_event_name,
  yard_trailer_first_last_known.last_known_carrier_name,
  yard_trailer_first_last_known.last_known_subcarrier_name,
  yard_trailer_first_last_known.last_known_load_num,
  yard_trailer_first_last_known.last_known_seal_num,
  yard_trailer_first_last_known.last_known_scac_code,
  CASE
    WHEN LOWER(
      yard_trailer_first_last_known.last_known_trailer_type
    ) = LOWER('OUTBOUND_DC_TO_DCFC') THEN 'OUTBOUND_DISTRIBUTION_CENTER_TO_DISTRIBUTION_CENTER_OR_FULFILLMENT_CENTER'
    WHEN LOWER(
      yard_trailer_first_last_known.last_known_trailer_type
    ) = LOWER('OUTBOUND_DC_TO_STORE') THEN 'OUTBOUND_DISTRIBUTION_CENTER_TO_STORE'
    ELSE yard_trailer_first_last_known.last_known_trailer_type
  END AS last_known_trailer_type,
  yard_trailer_first_last_known.last_known_trailer_function,
  yard_trailer_first_last_known.last_known_yard_name,
  yard_trailer_first_last_known.last_known_yard_location,
  yard_trailer_first_last_known.last_known_yard_zone,
  ci.carrier_bill_of_lading AS carrier_bill_of_lading_at_check_in,
  yard_trailer_first_last_known.last_known_carrier_bill_of_lading,
  ci.latest_trailer_checked_in_timestamp_utc,
  ci.latest_trailer_checked_in_timestamp_utc_tz,
  d.latest_trailer_released_timestamp_utc,
  d.latest_trailer_released_timestamp_utc_tz,
  COALESCE(
    yard_trailer_inbound_last_at_door_unload.at_door_appointment_id,
    yard_trailer_inbound_first_at_door_unload.at_door_appointment_id
  ) AS inbound_at_door_appointment_id,
  yard_trailer_inbound_last_at_door_unload.last_at_door_time AS inbound_last_at_door_time_utc,
  yard_trailer_inbound_last_at_door_unload.last_at_door_time_tz AS inbound_last_at_door_time_utc_tz,
  yard_trailer_inbound_first_at_door_unload.first_at_door_time AS inbound_first_at_door_time_utc,
  yard_trailer_inbound_first_at_door_unload.first_at_door_time_tz AS inbound_first_at_door_time_utc_tz,
  COALESCE(
    yard_trailer_inbound_last_at_door_unload.unload_appointment_id,
    yard_trailer_inbound_first_at_door_unload.unload_appointment_id
  ) AS inbound_unload_appointment_id,
  yard_trailer_inbound_first_at_door_unload.first_unload_time AS inbound_first_unload_time_utc,
  yard_trailer_inbound_first_at_door_unload.first_unload_time_tz AS inbound_first_unload_time_utc_tz,
  yard_trailer_inbound_last_at_door_unload.last_unload_time AS inbound_last_unload_time_utc,
  yard_trailer_inbound_last_at_door_unload.last_unload_time_tz AS inbound_last_unload_time_utc_tz,
  yard_trailer_inbound_first_at_door_unload.inbound_first_at_door_num,
  yard_trailer_inbound_last_at_door_unload.inbound_last_at_door_num,
  COALESCE(
    yard_trailer_outbound_last_at_door_load.at_door_appointment_id,
    yard_trailer_outbound_first_at_door_load.at_door_appointment_id
  ) AS outbound_at_door_appointment_id,
  yard_trailer_outbound_last_at_door_load.last_at_door_time AS outbound_last_at_door_time_utc,
  yard_trailer_outbound_last_at_door_load.last_at_door_time_tz AS outbound_last_at_door_time_utc_tz,
  yard_trailer_outbound_first_at_door_load.first_at_door_time AS outbound_first_at_door_time_utc,
  yard_trailer_outbound_first_at_door_load.first_at_door_time_tz AS outbound_first_at_door_time_utc_tz,
  COALESCE(
    yard_trailer_outbound_last_at_door_load.load_appointment_id,
    yard_trailer_outbound_first_at_door_load.at_door_appointment_id
  ) AS outbound_load_appointment_id,
  yard_trailer_outbound_first_at_door_load.first_load_time AS outbound_first_load_time_utc,
  yard_trailer_outbound_first_at_door_load.first_load_time_tz AS outbound_first_load_time_utc_tz,
  yard_trailer_outbound_last_at_door_load.last_load_time AS outbound_last_load_time_utc,
  yard_trailer_outbound_last_at_door_load.last_load_time_tz AS outbound_last_load_time_utc_tz,
  yard_trailer_outbound_first_at_door_load.outbound_first_at_door_num,
  yard_trailer_outbound_last_at_door_load.outbound_last_at_door_num,
  ci.latest_trailer_checked_in_timestamp_utc AS latest_trailer_checked_in_timestamp_local,
  ci.latest_trailer_checked_in_timestamp_utc_tz AS latest_trailer_checked_in_timestamp_local_tz,
  cast(DATETIME(d.latest_trailer_released_timestamp_utc, sd.TD_timezone_offset) as timestamp) AS latest_trailer_released_timestamp_local,
  sd.TD_timezone_offset as latest_trailer_released_timestamp_local_tz,
  cast(DATETIME(yard_trailer_inbound_last_at_door_unload.last_at_door_time, sd.TD_timezone_offset)as timestamp) AS inbound_last_at_door_time_local,
  sd.TD_timezone_offset as inbound_last_at_door_time_local_tz,
  cast(DATETIME(yard_trailer_inbound_last_at_door_unload.last_unload_time, sd.TD_timezone_offset)as timestamp) AS inbound_last_unload_time_local,
  sd.TD_timezone_offset as inbound_last_unload_time_local_tz,
  cast(DATETIME(yard_trailer_inbound_first_at_door_unload.first_at_door_time, sd.TD_timezone_offset) as timestamp) AS inbound_first_at_door_time_local,
  sd.TD_timezone_offset as inbound_first_at_door_time_local_tz,
  cast(DATETIME(yard_trailer_inbound_first_at_door_unload.first_unload_time, sd.TD_timezone_offset)as timestamp) AS inbound_first_unload_time_local,
  sd.TD_timezone_offset as inbound_first_unload_time_local_tz,
  cast(DATETIME(yard_trailer_outbound_last_at_door_load.last_at_door_time, sd.TD_timezone_offset)as timestamp) AS outbound_last_at_door_time_local,
  sd.TD_timezone_offset as outbound_last_at_door_time_local_tz,
  cast(DATETIME(yard_trailer_outbound_last_at_door_load.last_load_time, sd.TD_timezone_offset)as timestamp) AS outbound_last_load_time_local,
  sd.TD_timezone_offset as outbound_last_load_time_local_tz,
  cast(DATETIME(yard_trailer_outbound_first_at_door_load.first_at_door_time, sd.TD_timezone_offset)as timestamp) AS outbound_first_at_door_time_local,
  sd.TD_timezone_offset as outbound_first_at_door_time_local_tz,
 cast(DATETIME(yard_trailer_outbound_first_at_door_load.first_load_time, sd.TD_timezone_offset)as timestamp) AS outbound_first_load_time_local,
 sd.TD_timezone_offset as outbound_first_load_time_local_tz,
  CASE
    WHEN yard_trailer_inbound_last_at_door_unload.last_unload_time IS NOT NULL THEN 'Y'
    ELSE 'N'
  END AS has_been_unloaded_flag,
  CASE
    WHEN yard_trailer_outbound_last_at_door_load.last_load_time IS NOT NULL THEN 'Y'
    ELSE 'N'
  END AS has_been_loaded_flag,
  CASE
    WHEN LOWER(
      yard_trailer_first_last_known.last_known_event_name
    ) = LOWER('TrailerReleased') THEN 'Finished'
    ELSE 'Active'
  END AS visit_status,
  CAST(
    date_diff(COALESCE(
      d.latest_trailer_released_timestamp_utc,CAST( CURRENT_DATETIME ('PST8PDT') AS timestamp)
    ),Latest_Trailer_Checked_In_Timestamp_UTC,DAY) AS INTEGER
  ) AS days_in_yard,
  CAST(date_diff(COALESCE(
      yard_trailer_inbound_last_at_door_unload.last_unload_time,
      COALESCE(d.latest_trailer_released_timestamp_utc,CAST(CURRENT_DATETIME ('PST8PDT') AS timestamp
        )
      )
    ),yard_trailer_outbound_last_at_door_load.last_at_door_time,HOUR) AS INTEGER
  ) AS hours_to_unload_from_door_arrival,
  CASE
    WHEN yard_trailer_inbound_last_at_door_unload.last_unload_time IS NULL THEN NULL
    ELSE CAST(
      date_diff(COALESCE(
        yard_trailer_inbound_last_at_door_unload.last_unload_time,
        COALESCE(d.latest_trailer_released_timestamp_utc,CAST( CURRENT_DATETIME ('PST8PDT') AS timestamp))),Latest_Trailer_Checked_In_Timestamp_UTC,DAY) AS INTEGER)
  END AS days_to_unload_from_checkin,
  CAST(date_diff(COALESCE(yard_trailer_outbound_last_at_door_load.last_load_time,COALESCE(d.latest_trailer_released_timestamp_utc,CAST(CURRENT_DATETIME ('PST8PDT') AS timestamp ))),yard_trailer_outbound_last_at_door_load.last_at_door_time,HOUR) AS INTEGER
  ) AS hours_to_load_from_door_arrival,

  CASE
    WHEN yard_trailer_outbound_last_at_door_load.last_load_time IS NULL THEN NULL
    ELSE CAST(date_diff(COALESCE(yard_trailer_outbound_last_at_door_load.last_load_time,COALESCE(
          d.latest_trailer_released_timestamp_utc,CAST(CURRENT_DATETIME ('PST8PDT') AS timestamp))),Latest_Trailer_Checked_In_Timestamp_UTC,DAY) AS INTEGER
    )
  END AS days_to_load_from_checkin
FROM
  (
    SELECT
      visit_id,
      appointment_id,
      trailer_num,
      trailer_id,
      location_num,
      NULLIF(TRIM(carrier_bill_of_lading), '') AS carrier_bill_of_lading,
      event_time_utc AS latest_trailer_checked_in_timestamp_utc,
      event_time_tz AS latest_trailer_checked_in_timestamp_utc_tz
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.yard_trailer_visit_event_fact
    WHERE
      LOWER(event_name) = LOWER('TrailerCheckedIntoYard') QUALIFY (
        ROW_NUMBER() OVER (
          PARTITION BY
            visit_id
          ORDER BY
            event_time DESC
        )
      ) = 1
  ) AS ci
  LEFT JOIN (
    SELECT
      visit_id,
      appointment_id,
      trailer_num,
      trailer_id,
      location_num,
      NULLIF(TRIM(carrier_bill_of_lading), '') AS carrier_bill_of_lading,
      event_time_utc AS latest_trailer_released_timestamp_utc,
      event_time_tz AS latest_trailer_released_timestamp_utc_tz
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.yard_trailer_visit_event_fact
    WHERE
      LOWER(event_name) = LOWER('TrailerReleased') QUALIFY (
        ROW_NUMBER() OVER (
          PARTITION BY
            visit_id
          ORDER BY
            event_time DESC
        )
      ) = 1
  ) AS d ON LOWER(ci.visit_id) = LOWER(d.visit_id)
  LEFT JOIN yard_trailer_inbound_last_at_door_unload ON LOWER(yard_trailer_inbound_last_at_door_unload.visit_id) = LOWER(ci.visit_id)
  LEFT JOIN yard_trailer_outbound_last_at_door_load ON LOWER(yard_trailer_outbound_last_at_door_load.visit_id) = LOWER(ci.visit_id)
  LEFT JOIN yard_trailer_inbound_first_at_door_unload ON LOWER(
    yard_trailer_inbound_first_at_door_unload.visit_id
  ) = LOWER(ci.visit_id)
  LEFT JOIN yard_trailer_outbound_first_at_door_load ON LOWER(yard_trailer_outbound_first_at_door_load.visit_id) = LOWER(ci.visit_id)
  LEFT JOIN yard_trailer_first_last_known ON LOWER(yard_trailer_first_last_known.visit_id) = LOWER(ci.visit_id)
  INNER JOIN yard_trailer_td_tz AS sd ON sd.store_num = CAST(ci.location_num AS FLOAT64)
WHERE
  ci.visit_id NOT IN (
    SELECT
      visit_id
    FROM
      yard_trailer_invalid_visit_union
  );

MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.yard_visit_lifecycle_fact AS w USING (
  SELECT
    visit_id,
    location_num,
    first_known_trailer_num,
    first_known_trailer_id,
    last_known_trailer_num,
    last_known_trailer_id,
    last_known_event_name,
    last_known_carrier_name,
    last_known_subcarrier_name,
    last_known_load_num,
    last_known_seal_num,
    last_known_scac_code,
    last_known_trailer_type,
    last_known_trailer_function,
    last_known_yard_name,
    last_known_yard_location,
    last_known_yard_zone,
    carrier_bill_of_lading_at_check_in,
    last_known_carrier_bill_of_lading,
    latest_trailer_checked_in_timestamp_utc,
    latest_trailer_checked_in_timestamp_utc_tz,
    latest_trailer_released_timestamp_utc,
    latest_trailer_released_timestamp_utc_tz,
    inbound_at_door_appointment_id,
    inbound_last_at_door_time_utc,
    inbound_last_at_door_time_utc_tz,
    inbound_first_at_door_time_utc,
    inbound_first_at_door_time_utc_tz,
    inbound_unload_appointment_id,
    inbound_first_unload_time_utc,
    inbound_first_unload_time_utc_tz,
    inbound_last_unload_time_utc,
    inbound_last_unload_time_utc_tz,
    inbound_first_at_door_num,
    inbound_last_at_door_num,
    outbound_at_door_appointment_id,
    outbound_last_at_door_time_utc,
    outbound_last_at_door_time_utc_tz,
    outbound_first_at_door_time_utc,
    outbound_first_at_door_time_utc_tz,
    outbound_load_appointment_id,
    outbound_first_load_time_utc,
    outbound_first_load_time_utc_tz,
    outbound_last_load_time_utc,
    outbound_last_load_time_utc_tz,
    outbound_first_at_door_num,
    outbound_last_at_door_num,
    latest_trailer_checked_in_timestamp_local,
    latest_trailer_checked_in_timestamp_local_tz,
    latest_trailer_released_timestamp_local,
    latest_trailer_released_timestamp_local_tz,
    inbound_last_at_door_time_local,
    inbound_last_at_door_time_local_tz,
    inbound_last_unload_time_local,
    inbound_last_unload_time_local_tz,
    inbound_first_at_door_time_local,
    inbound_first_at_door_time_local_tz,
    inbound_first_unload_time_local,
    inbound_first_unload_time_local_tz,
    outbound_last_at_door_time_local,
    outbound_last_at_door_time_local_tz,
    outbound_last_load_time_local,
    outbound_last_load_time_local_tz,
    outbound_first_at_door_time_local,
    outbound_first_at_door_time_local_tz,
    outbound_first_load_time_local,
    outbound_first_load_time_local_tz,
    has_been_unloaded_flag,
    has_been_loaded_flag,
    visit_status,
    days_in_yard,
    hours_to_unload_from_door_arrival,
    days_to_unload_from_checkin,
    hours_to_load_from_door_arrival,
    days_to_load_from_checkin,
    (
      SELECT
        batch_id
      FROM
        `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
      WHERE
        LOWER(subject_area_nm) = LOWER('NAP_ASCP_YMS_LIFECYCLE')
    ) AS dw_batch_id,
    (
      SELECT
        curr_batch_date
      FROM
        `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
      WHERE
        LOWER(subject_area_nm) = LOWER('NAP_ASCP_YMS_LIFECYCLE')
    ) AS dw_batch_date
  FROM
    yard_trailer_liftcycle_stg
) AS s ON LOWER(w.visit_id) = LOWER(s.visit_id) WHEN MATCHED THEN
UPDATE
SET
  location_num = s.location_num,
  first_known_trailer_num = s.first_known_trailer_num,
  first_known_trailer_id = s.first_known_trailer_id,
  last_known_trailer_num = s.last_known_trailer_num,
  last_known_trailer_id = s.last_known_trailer_id,
  last_known_event_name = s.last_known_event_name,
  last_known_carrier_name = s.last_known_carrier_name,
  last_known_subcarrier_name = s.last_known_subcarrier_name,
  last_known_load_num = s.last_known_load_num,
  last_known_seal_num = s.last_known_seal_num,
  last_known_scac_code = s.last_known_scac_code,
  last_known_trailer_type = s.last_known_trailer_type,
  last_known_trailer_function = s.last_known_trailer_function,
  last_known_yard_name = s.last_known_yard_name,
  last_known_yard_location = s.last_known_yard_location,
  last_known_yard_zone = s.last_known_yard_zone,
  carrier_bill_of_lading_at_check_in = s.carrier_bill_of_lading_at_check_in,
  last_known_carrier_bill_of_lading = s.last_known_carrier_bill_of_lading,
  latest_trailer_checked_in_timestamp_utc = s.latest_trailer_checked_in_timestamp_utc,
  latest_trailer_checked_in_timestamp_utc_tz = s.latest_trailer_checked_in_timestamp_utc_tz,
  latest_trailer_released_timestamp_utc = s.latest_trailer_released_timestamp_utc,
  latest_trailer_released_timestamp_utc_tz = s.latest_trailer_released_timestamp_utc_tz,
  inbound_at_door_appointment_id = s.inbound_at_door_appointment_id,
  inbound_last_at_door_time_utc = s.inbound_last_at_door_time_utc,
  inbound_last_at_door_time_utc_tz = s.inbound_last_at_door_time_utc_tz,
  inbound_first_at_door_time_utc = s.inbound_first_at_door_time_utc,
  inbound_first_at_door_time_utc_tz = s.inbound_first_at_door_time_utc_tz,
  inbound_unload_appointment_id = s.inbound_unload_appointment_id,
  inbound_first_unload_time_utc = s.inbound_first_unload_time_utc,
  inbound_first_unload_time_utc_tz = s.inbound_first_unload_time_utc_tz,
  inbound_last_unload_time_utc = s.inbound_last_unload_time_utc,
  inbound_last_unload_time_utc_tz = s.inbound_last_unload_time_utc_tz,
  inbound_first_at_door_num = s.inbound_first_at_door_num,
  inbound_last_at_door_num = s.inbound_last_at_door_num,
  outbound_at_door_appointment_id = s.outbound_at_door_appointment_id,
  outbound_last_at_door_time_utc = s.outbound_last_at_door_time_utc,
  outbound_last_at_door_time_utc_tz = s.outbound_last_at_door_time_utc_tz,
  outbound_first_at_door_time_utc = s.outbound_first_at_door_time_utc,
  outbound_first_at_door_time_utc_tz = s.outbound_first_at_door_time_utc_tz,
  outbound_load_appointment_id = s.outbound_load_appointment_id,
  outbound_first_load_time_utc = s.outbound_first_load_time_utc,
  outbound_first_load_time_utc_tz = s.outbound_first_load_time_utc_tz,
  outbound_last_load_time_utc = s.outbound_last_load_time_utc,
  outbound_last_load_time_utc_tz = s.outbound_last_load_time_utc_tz,
  outbound_first_at_door_num = s.outbound_first_at_door_num,
  outbound_last_at_door_num = s.outbound_last_at_door_num,
  latest_trailer_checked_in_timestamp_local = s.latest_trailer_checked_in_timestamp_local,
  latest_trailer_checked_in_timestamp_local_tz = s.latest_trailer_checked_in_timestamp_local_tz,
  latest_trailer_released_timestamp_local = s.latest_trailer_released_timestamp_local,
  latest_trailer_released_timestamp_local_tz = s.latest_trailer_released_timestamp_local_tz,
  inbound_last_at_door_time_local = s.inbound_last_at_door_time_local,
  inbound_last_at_door_time_local_tz = s.inbound_last_at_door_time_local_tz,
  inbound_last_unload_time_local = s.inbound_last_unload_time_local,
  inbound_last_unload_time_local_tz = s.inbound_last_unload_time_local_tz,
  inbound_first_at_door_time_local = s.inbound_first_at_door_time_local,
  inbound_first_at_door_time_local_tz = s.inbound_first_at_door_time_local_tz,
  inbound_first_unload_time_local = s.inbound_first_unload_time_local,
   inbound_first_unload_time_local_tz = s.inbound_first_unload_time_local_tz,
  outbound_last_at_door_time_local = s.outbound_last_at_door_time_local,
  outbound_last_at_door_time_local_tz = s.outbound_last_at_door_time_local_tz,
  outbound_last_load_time_local = s.outbound_last_load_time_local,
  outbound_last_load_time_local_tz = s.outbound_last_load_time_local_tz,
  outbound_first_at_door_time_local = s.outbound_first_at_door_time_local,
  outbound_first_at_door_time_local_tz = s.outbound_first_at_door_time_local_tz,
  outbound_first_load_time_local = s.outbound_first_load_time_local,
  outbound_first_load_time_local_tz = s.outbound_first_load_time_local_tz,
  has_been_unloaded_flag = s.has_been_unloaded_flag,
  has_been_loaded_flag = s.has_been_loaded_flag,
  visit_status = s.visit_status,
  days_in_yard = s.days_in_yard,
  hours_to_unload_from_door_arrival = s.hours_to_unload_from_door_arrival,
  days_to_unload_from_checkin = s.days_to_unload_from_checkin,
  hours_to_load_from_door_arrival = s.hours_to_load_from_door_arrival,
  days_to_load_from_checkin = s.days_to_load_from_checkin,
  dw_batch_id = CAST(s.dw_batch_id AS STRING),
  dw_batch_date = s.dw_batch_date,
  dw_sys_updt_tmstp = CAST( CURRENT_DATETIME ('PST8PDT') AS timestamp),
  dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.jwn_UDF.DEFAULT_TZ_PST()
   WHEN NOT MATCHED THEN INSERT
(
		  visit_id,
	      location_num,
	      first_known_trailer_num,
	      first_known_trailer_id,
	      last_known_trailer_num,
	      last_known_trailer_id,
	      last_known_event_name,
	      last_known_carrier_name,
	      last_known_subcarrier_name,
	      last_known_load_num,
	      last_known_seal_num,
	      last_known_scac_code,
	      last_known_trailer_type,
	      last_known_trailer_function,
	      last_known_yard_name,
	      last_known_yard_location,
	      last_known_yard_zone,
	      carrier_bill_of_lading_at_check_in,
	      last_known_carrier_bill_of_lading,
	      Latest_Trailer_Checked_In_Timestamp_UTC,
        Latest_Trailer_Checked_In_Timestamp_UTC_tz,
	      Latest_Trailer_Released_Timestamp_UTC,
        Latest_Trailer_Released_Timestamp_UTC_tz,
	      inbound_at_door_appointment_id,
	      inbound_last_at_door_time_UTC,
        inbound_last_at_door_time_UTC_tz,
	      inbound_first_at_door_time_UTC,
        inbound_first_at_door_time_UTC_tz,
	      inbound_unload_appointment_id,
	      inbound_first_unload_time_UTC,
        inbound_first_unload_time_UTC_tz,
	      inbound_last_unload_time_UTC,
        inbound_last_unload_time_UTC_tz,
	 	  inbound_first_at_door_num,
		  inbound_last_at_door_num,
	      outbound_at_door_appointment_id,
	      outbound_last_at_door_time_UTC,
        outbound_last_at_door_time_UTC_tz,
	      outbound_first_at_door_time_UTC,
        outbound_first_at_door_time_UTC_tz,
	      outbound_load_appointment_id,
	      outbound_first_load_time_UTC,
        outbound_first_load_time_UTC_tz,
	      outbound_last_load_time_UTC,
        outbound_last_load_time_UTC_tz,
	      outbound_first_at_door_num,
		  outbound_last_at_door_num,
	      latest_trailer_checked_in_timestamp_local,
    latest_trailer_checked_in_timestamp_local_tz,
    latest_trailer_released_timestamp_local,
    latest_trailer_released_timestamp_local_tz,
    inbound_last_at_door_time_local,
    inbound_last_at_door_time_local_tz,
    inbound_last_unload_time_local,
    inbound_last_unload_time_local_tz,
    inbound_first_at_door_time_local,
    inbound_first_at_door_time_local_tz,
    inbound_first_unload_time_local,
    inbound_first_unload_time_local_tz,
    outbound_last_at_door_time_local,
    outbound_last_at_door_time_local_tz,
    outbound_last_load_time_local,
    outbound_last_load_time_local_tz,
    outbound_first_at_door_time_local,
    outbound_first_at_door_time_local_tz,
    outbound_first_load_time_local,
    outbound_first_load_time_local_tz,
          has_been_unloaded_flag,
          has_been_loaded_flag,
	      visit_status,
	      days_in_yard,
	      hours_to_unload_from_door_arrival,
	      days_to_unload_from_checkin,
	      hours_to_load_from_door_arrival,
	      days_to_load_from_checkin,
          dw_batch_id,
          dw_batch_date,
          dw_sys_load_tmstp,
          dw_sys_load_tmstp_tz,
          dw_sys_updt_tmstp,
          dw_sys_updt_tmstp_tz
	)

VALUES
  (
    s.visit_id,
    s.location_num,
    s.first_known_trailer_num,
    s.first_known_trailer_id,
    s.last_known_trailer_num,
    s.last_known_trailer_id,
    s.last_known_event_name,
    s.last_known_carrier_name,
    s.last_known_subcarrier_name,
    s.last_known_load_num,
    s.last_known_seal_num,
    s.last_known_scac_code,
    s.last_known_trailer_type,
    s.last_known_trailer_function,
    s.last_known_yard_name,
    s.last_known_yard_location,
    s.last_known_yard_zone,
    s.carrier_bill_of_lading_at_check_in,
    s.last_known_carrier_bill_of_lading,
    s.Latest_Trailer_Checked_In_Timestamp_UTC,
    s.Latest_Trailer_Checked_In_Timestamp_UTC_tz,
	  s.Latest_Trailer_Released_Timestamp_UTC,
    s.Latest_Trailer_Released_Timestamp_UTC_tz,
	  s.inbound_at_door_appointment_id,
	  s.inbound_last_at_door_time_UTC,
    s.inbound_last_at_door_time_UTC_tz,
	  s.inbound_first_at_door_time_UTC,
    s.inbound_first_at_door_time_UTC_tz,
	  s.inbound_unload_appointment_id,
	  s.inbound_first_unload_time_UTC,
    s.inbound_first_unload_time_UTC_tz,
	  s.inbound_last_unload_time_UTC,
    s.inbound_last_unload_time_UTC_tz,
	 	s.inbound_first_at_door_num,
		s.inbound_last_at_door_num,
	  s.outbound_at_door_appointment_id,
	  s.outbound_last_at_door_time_UTC,
    s.outbound_last_at_door_time_UTC_tz,
	  s.outbound_first_at_door_time_UTC,
    s.outbound_first_at_door_time_UTC_tz,
	  s.outbound_load_appointment_id,
	  s.outbound_first_load_time_UTC,
    s.outbound_first_load_time_UTC_tz,
	  s.outbound_last_load_time_UTC,
    s.outbound_last_load_time_UTC_tz,
    s.outbound_first_at_door_num,
    s.outbound_last_at_door_num,
    s.latest_trailer_checked_in_timestamp_local,
    s.latest_trailer_checked_in_timestamp_local_tz,
    s.latest_trailer_released_timestamp_local,
    s.latest_trailer_released_timestamp_local_tz,
    s.inbound_last_at_door_time_local,
    s.inbound_last_at_door_time_local_tz,
    s.inbound_last_unload_time_local,
    s.inbound_last_unload_time_local_tz,
    s.inbound_first_at_door_time_local,
    s.inbound_first_at_door_time_local_tz,
    s.inbound_first_unload_time_local,
    s.inbound_first_unload_time_local_tz,
    s.outbound_last_at_door_time_local,
    s.outbound_last_at_door_time_local_tz,
    s.outbound_last_load_time_local,
    s.outbound_last_load_time_local_tz,
    s.outbound_first_at_door_time_local,
    s.outbound_first_at_door_time_local_tz,
    s.outbound_first_load_time_local,
    s.outbound_first_load_time_local_tz,
    s.has_been_unloaded_flag,
    s.has_been_loaded_flag,
    s.visit_status,
    s.days_in_yard,
    s.hours_to_unload_from_door_arrival,
    s.days_to_unload_from_checkin,
    s.hours_to_load_from_door_arrival,
    s.days_to_load_from_checkin,
    CAST(s.dw_batch_id AS STRING),
    s.dw_batch_date,
    CAST( CURRENT_DATETIME ('PST8PDT') AS timestamp),
    `{{params.gcp_project_id}}`.jwn_UDF.DEFAULT_TZ_PST(),
    CAST( CURRENT_DATETIME ('PST8PDT') AS timestamp),
    `{{params.gcp_project_id}}`.jwn_UDF.DEFAULT_TZ_PST()
  );