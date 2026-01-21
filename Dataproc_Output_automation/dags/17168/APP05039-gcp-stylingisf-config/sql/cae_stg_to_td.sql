--{autocommit_on};

-- SET QUERY_BAND = 'App_ID=app05039;LoginUser=SCH_NAP_STYL_BATCH_DEV;Job_Name=cae_stg_to_td;Data_Plane=Customer;Team_Email=TECH_ISF_DAS_STYLING@nordstrom.com;PagerDuty=DAS Digital Styling Services;Obj_Name=CURATION_STYLING_INTERACTION_FACT;Conn_Type=JDBC;DAG_ID=customer_activity_engaged_curation_ongoing_7847_styling_styling_insights_kpi;Task_Name=job_1;' FOR SESSION VOLATILE;


INSERT INTO {{params.gcp_project_id}}.{{params.fact_db}}.curation_styling_interaction_stg
(customer_id, customer_id_type, curation_curator_id, eventtime_pst, activity_date, styling_event, curation_id, channel, platform)
SELECT customer_id, customer_id_type, curation_curator_id, eventtime_pst, CAST(eventtime_pst AS DATE) as activity_date, styling_event, curation_id, channel, platform
FROM {{params.gcp_project_id}}.{{params.stg_db}}.curation_styling_interaction_stg;

-- SET QUERY_BAND = NONE FOR SESSION;
