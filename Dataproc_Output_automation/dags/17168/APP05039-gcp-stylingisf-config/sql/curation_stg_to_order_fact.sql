
-- SET QUERY_BAND = 'App_ID=app05039;LoginUser=SCH_NAP_STYL_BATCH_DEV;Job_Name=curation_stg_to_order_fact;Data_Plane=Customer;Team_Email=TECH_ISF_DAS_STYLING@nordstrom.com;PagerDuty=DAS Digital Styling Services;Obj_Name=CURATION_ORDER_LINE_FACT;Conn_Type=JDBC;' FOR SESSION VOLATILE;
-- ET;

CALL `{{params.gcp_project_id}}`.{{params.utl_db}}.DATA_TIMELINESS_METRIC_FACT_LD
('CURATION_ORDER_LINE_FACT','{{params.fact_db}}' ,'curation_ongoing_7847_styling_styling_insights_kpi','read_kafka_to_teradata_job_2',1,'LOAD_START','',current_datetime,'CURATION_ORDER_LINE');
				 
DELETE FROM `{{params.gcp_project_id}}`.{{params.fact_db}}.curation_order_line_fact 
WHERE curation_order_line_fact.order_line_id IN(
  SELECT DISTINCT order_line_id FROM {{params.gcp_project_id}}.{{params.stg_db}}.curation_stg);

INSERT INTO `{{params.gcp_project_id}}`.{{params.fact_db}}.curation_order_line_fact 
(board_id, 
order_line_id, 
order_number, 
activity_date,
activity_tmstp_pst,
dw_batch_date, 
dw_sys_load_tmstp_pst, 
dw_sys_update_tmstp_pst)
  SELECT curation_stg.curation_id AS board_id,
      curation_stg.order_line_id,
      curation_stg.order_number,
      DATE(curation_stg.employee_commission_eventtime - INTERVAL 7 HOUR) AS activity_date,
      DATE(curation_stg.employee_commission_eventtime - INTERVAL 7 HOUR) AS activity_tmstp,
      current_date('PST8PDT'),
      datetime(CURRENT_TIMESTAMP(),'America/Los_Angeles'),
	    datetime(CURRENT_TIMESTAMP(),'America/Los_Angeles')
    FROM
      `{{params.gcp_project_id}}`.{{params.stg_db}}.curation_stg
    WHERE curation_stg.order_line_id IS NOT NULL
    QUALIFY row_number() OVER (PARTITION BY order_line_id ORDER BY curation_stg.systemtime DESC) = 1
;
																																																																						 
CALL `{{params.gcp_project_id}}`.{{params.utl_db}}.DATA_TIMELINESS_METRIC_FACT_LD
('CURATION_ORDER_LINE_FACT','{{params.fact_db}}' ,'curation_ongoing_7847_styling_styling_insights_kpi','read_kafka_to_teradata_job_2',2,'LOAD_END','',current_datetime,'CURATION_ORDER_LINE');

--SET QUERY_BAND = NONE FOR SESSION;