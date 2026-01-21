--{autocommit_on};

--SET QUERY_BAND = 'App_ID=app05039;LoginUser=SCH_NAP_STYL_BATCH_DEV;Job_Name=curation_stg_to_curation_fact;Data_Plane=Customer;--Team_Email=TECH_ISF_DAS_STYLING@nordstrom.com;PagerDuty=DAS Digital Styling Services;Obj_Name=CURATION_FACT;Conn_Type=JDBC;' FOR --SESSION VOLATILE;
--ET;

CALL `{{params.gcp_project_id}}`.{{params.utl_db}}.DATA_TIMELINESS_METRIC_FACT_LD
('CURATION_FACT','PRD_NAP_FCT' ,'curation_ongoing_7847_styling_styling_insights_kpi','read_kafka_to_teradata_job_1',1,'LOAD_START','',current_datetime('PST8PDT'),'CURATION');


--Purge and reprocess data to account for updates and late arriving data.
DELETE FROM `{{params.gcp_project_id}}`.{{params.fact_db}}.CURATION_FACT 
WHERE curation_fact.board_id IN(
  SELECT DISTINCT
      curation_id
    FROM
      `{{params.gcp_project_id}}`.{{params.stg_db}}.CURATION_STG);

      
INSERT INTO `{{params.gcp_project_id}}`.{{params.fact_db}}.CURATION_FACT (board_id, board_type, activity_date,activity_tmstp_pst, look_id, board_curator_id, board_curator_id_type, customer_id, customer_id_type, board_created_pst, board_sent_pst, curation_requested_pst, selling_channel, channel_brand, channel_country, number_of_items_sent, dw_batch_date, dw_sys_load_tmstp_pst, dw_sys_update_tmstp_pst)
  SELECT
      curation_stg.curation_id AS board_id,
      CASE
        WHEN curation_stg.curation_board_type IS NOT NULL THEN curation_stg.curation_board_type
        ELSE 'UNKNOWN'
      END AS board_type,
      DATE(activity_tmstp - INTERVAL 7 HOUR) AS activity_date,
      DATE(curation_stg.activity_tmstp - INTERVAL 7 HOUR) AS activity_tmstp,
      curation_stg.look_id,
      curation_stg.curating_employee_id AS board_curator_id,
      curation_stg.curating_employee_id_type AS board_curator_id_type,
      curation_stg.customer_id,
      curation_stg.customer_id_type,
      DATE(curation_stg.curation_started_time - INTERVAL 7 HOUR) AS board_created,
      DATE(curation_stg.curation_completed_time - INTERVAL 7 HOUR) AS board_sent,
      DATE(curation_stg.curation_requested_time - INTERVAL 7 HOUR) AS curation_requested_pst,
      curation_stg.selling_channel,
      curation_stg.channel_brand,
      curation_stg.channel_country,
      cast(floor(curation_stg.number_of_items) as int64),
      current_date('PST8PDT') AS dw_batch_date,
      datetime(CURRENT_TIMESTAMP(),'America/Los_Angeles') dw_sys_load_tmstp,
      datetime(CURRENT_TIMESTAMP(),'America/Los_Angeles') AS dw_sys_updt_tmstp
    FROM
      `{{params.gcp_project_id}}`.{{params.stg_db}}.CURATION_STG
    QUALIFY row_number() OVER (PARTITION BY board_id ORDER BY curation_stg.systemtime DESC) = 1;
	
	CALL `{{params.gcp_project_id}}`.{{params.utl_db}}.DATA_TIMELINESS_METRIC_FACT_LD
('CURATION_FACT','PRD_NAP_FCT' ,'curation_ongoing_7847_styling_styling_insights_kpi','read_kafka_to_teradata_job_1',2,'LOAD_END','',current_datetime('PST8PDT'),'CURATION');
 

--SET QUERY_BAND = NONE FOR SESSION;
