
--SET QUERY_BAND = 'App_ID=app05039;LoginUser=SCH_NAP_STYL_BATCH_DEV;Job_Name=curation_stg_to_curation_styling_interaction_fact;Data_Plane=Customer;Team_Email=TECH_ISF_DAS_STYLING@nordstrom.com;PagerDuty=DAS Digital Styling Services;Obj_Name=CURATION_STYLING_INTERACTION_FACT;Conn_Type=JDBC;' FOR SESSION VOLATILE;
--ET;

MERGE INTO `{{params.gcp_project_id}}`.{{params.fact_db}}.curation_styling_interaction_fact AS destination USING (
  SELECT
      customer_id,
      customer_id_type,
      curating_employee_id AS curation_curator_id,
      curation_requested_time AS eventtime_pst,
      activity_date,
      'REQUESTED_CURATION' AS styling_event,
      curation_id,
      channel_brand AS channel,
      NULL AS platform
    FROM
      `{{params.gcp_project_id}}`.{{params.stg_db}}.curation_stg
    WHERE curation_requested_time IS NOT NULL
    QUALIFY row_number() OVER (PARTITION BY curation_id ORDER BY activity_tmstp DESC) = 1
) AS source
ON source.customer_id = destination.customer_id
 AND source.curation_curator_id = destination.curation_curator_id
 AND source.eventtime_pst = destination.eventtime_pst
 AND source.styling_event = destination.styling_event
 AND source.curation_id = destination.curation_id
 AND source.activity_date = destination.activity_date
   WHEN MATCHED THEN UPDATE SET
    dw_sys_update_tmstp_pst = datetime(CURRENT_TIMESTAMP(),'America/Los_Angeles')
   WHEN NOT MATCHED BY TARGET THEN
    INSERT (customer_id, curation_curator_id, eventtime_pst, styling_event, curation_id, activity_date, customer_id_type, channel, platform, dw_batch_date, dw_sys_load_tmstp_pst, dw_sys_update_tmstp_pst)
    VALUES (source.customer_id, source.curation_curator_id, source.eventtime_pst, source.styling_event, source.curation_id, source.activity_date, source.customer_id_type, source.channel, cast(source.platform as STRING), current_date('PST8PDT'), datetime(CURRENT_TIMESTAMP(),'America/Los_Angeles'), datetime(CURRENT_TIMESTAMP(),'America/Los_Angeles'))
;
MERGE INTO `{{params.gcp_project_id}}`.{{params.fact_db}}.curation_styling_interaction_fact AS destination USING (
  SELECT
      customer_id,
      customer_id_type,
      curating_employee_id AS curation_curator_id,
      curation_started_time AS eventtime_pst,
      activity_date,
      'STARTED_CURATION' AS styling_event,
      curation_id,
      channel_brand AS channel,
      NULL AS platform
    FROM
      `{{params.gcp_project_id}}`.{{params.stg_db}}.curation_stg
    WHERE curation_started_time IS NOT NULL
    QUALIFY row_number() OVER (PARTITION BY curation_id ORDER BY activity_tmstp DESC) = 1
) AS source
ON source.customer_id = destination.customer_id
 AND source.curation_curator_id = destination.curation_curator_id
 AND source.eventtime_pst = destination.eventtime_pst
 AND source.styling_event = destination.styling_event
 AND source.curation_id = destination.curation_id
 AND source.activity_date = destination.activity_date
   WHEN MATCHED THEN UPDATE SET
    dw_sys_update_tmstp_pst = datetime(CURRENT_TIMESTAMP(),'America/Los_Angeles')
   WHEN NOT MATCHED BY TARGET THEN
    INSERT (customer_id, curation_curator_id, eventtime_pst, styling_event, curation_id, activity_date, customer_id_type, channel, platform, dw_batch_date, dw_sys_load_tmstp_pst, dw_sys_update_tmstp_pst)
    VALUES (source.customer_id, source.curation_curator_id, source.eventtime_pst, source.styling_event, source.curation_id, source.activity_date, source.customer_id_type, source.channel, cast(source.platform as string), current_date('PST8PDT'), datetime(CURRENT_TIMESTAMP(),'America/Los_Angeles'), datetime(CURRENT_TIMESTAMP(),'America/Los_Angeles'))
;
MERGE INTO `{{params.gcp_project_id}}`.{{params.fact_db}}.curation_styling_interaction_fact AS destination USING (
  SELECT
      customer_id,
      customer_id_type,
      curating_employee_id AS curation_curator_id,
      curation_completed_time AS eventtime_pst,
      activity_date,
      'SENT_CURATION' AS styling_event,
      curation_id,
      channel_brand AS channel,
      NULL AS platform
    FROM
      `{{params.gcp_project_id}}`.{{params.stg_db}}.curation_stg
    WHERE curation_completed_time IS NOT NULL
    QUALIFY row_number() OVER (PARTITION BY curation_id ORDER BY activity_tmstp DESC) = 1
) AS source
ON source.customer_id = destination.customer_id
 AND source.curation_curator_id = destination.curation_curator_id
 AND source.eventtime_pst = destination.eventtime_pst
 AND source.styling_event = destination.styling_event
 AND source.curation_id = destination.curation_id
 AND source.activity_date = destination.activity_date
   WHEN MATCHED THEN UPDATE SET
    dw_sys_update_tmstp_pst = datetime(CURRENT_TIMESTAMP(),'America/Los_Angeles')
   WHEN NOT MATCHED BY TARGET THEN
    INSERT (customer_id, curation_curator_id, eventtime_pst, styling_event, curation_id, activity_date, customer_id_type, channel, platform, dw_batch_date, dw_sys_load_tmstp_pst, dw_sys_update_tmstp_pst)
    VALUES (source.customer_id, source.curation_curator_id, source.eventtime_pst, source.styling_event, source.curation_id, source.activity_date, source.customer_id_type, source.channel, cast(source.platform as string), current_date('PST8PDT'), datetime(CURRENT_TIMESTAMP(),'America/Los_Angeles'), datetime(CURRENT_TIMESTAMP(),'America/Los_Angeles'))
;
MERGE INTO `{{params.gcp_project_id}}`.{{params.fact_db}}.curation_styling_interaction_fact AS destination USING (
  SELECT
      customer_id,
      customer_id_type,
      curating_employee_id AS curation_curator_id,
      activity_tmstp AS eventtime_pst,
      activity_date,
      'CURATION_LED_PURCHASE' AS styling_event,
      curation_id,
      channel_brand AS channel,
      NULL AS platform
    FROM
      `{{params.gcp_project_id}}`.{{params.stg_db}}.curation_stg
    WHERE order_number IS NOT NULL
    QUALIFY row_number() OVER (PARTITION BY curation_id ORDER BY activity_tmstp DESC) = 1
) AS source
ON source.customer_id = destination.customer_id
 AND source.curation_curator_id = destination.curation_curator_id
 AND source.eventtime_pst = destination.eventtime_pst
 AND source.styling_event = destination.styling_event
 AND source.curation_id = destination.curation_id
 AND source.activity_date = destination.activity_date
   WHEN MATCHED THEN UPDATE SET
    dw_sys_update_tmstp_pst = datetime(CURRENT_TIMESTAMP(),'America/Los_Angeles')
   WHEN NOT MATCHED BY TARGET THEN
    INSERT (customer_id, curation_curator_id, eventtime_pst, styling_event, curation_id, activity_date, customer_id_type, channel, platform, dw_batch_date, dw_sys_load_tmstp_pst, dw_sys_update_tmstp_pst)
    VALUES (source.customer_id, source.curation_curator_id, source.eventtime_pst, source.styling_event, source.curation_id, source.activity_date, source.customer_id_type, source.channel, cast(source.platform as string), current_date('PST8PDT'), datetime(CURRENT_TIMESTAMP(),'America/Los_Angeles'), datetime(CURRENT_TIMESTAMP(),'America/Los_Angeles'));
	--SET QUERY_BAND = NONE FOR SESSION;