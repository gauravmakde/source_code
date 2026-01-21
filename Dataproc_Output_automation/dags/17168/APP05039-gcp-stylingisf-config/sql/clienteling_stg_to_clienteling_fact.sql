--{autocommit_on};
-- SET QUERY_BAND = 'App_ID=app05039;LoginUser=SCH_NAP_STYL_BATCH_DEV;Job_Name=clienteling_stg_to_clienteling_fact;Data_Plane=Customer;Team_Email=TECH_ISF_DAS_STYLING@nordstrom.com;PagerDuty=DAS Digital Styling Services;Obj_Name=CONSENTED_CLIENTELING_FACT;Conn_Type=JDBC;DAG_ID=clienteling_ongoing_17168_styling_styling_insights_kpi;Task_Name=job_2;' FOR SESSION VOLATILE;


MERGE INTO `{{params.gcp_project_id}}`.{{params.fact_db}}.consented_clienteling_relationship_fact AS destination USING (
    SELECT
        `{{params.gcp_project_id}}.NORD_UDF.EPOCH_TMSTP`(CAST(TRUNC(CAST(eventtime AS FLOAT64)) AS BIGINT)) AS eventtime_utc,
        DATE((`{{params.gcp_project_id}}.NORD_UDF.EPOCH_TMSTP`(CAST(TRUNC(CAST(eventtime AS FLOAT64)) AS BIGINT))),'PST8PDT') AS eventdate_pacific,
        DATETIME(`{{params.gcp_project_id}}.NORD_UDF.EPOCH_TMSTP`(CAST(TRUNC(CAST(eventtime AS FLOAT64)) AS BIGINT)),'PST8PDT') AS event_tmstp_pacific,
        customer_id,
        customer_id_type,
        employee_id,
        employee_id_type,
        'OPT_IN' AS request_type
    FROM
        `{{params.gcp_project_id}}`.{{params.stg_db}}.clienteling_relationship_started_ldg
) AS t ON CAST(t.event_tmstp_pacific AS DATETIME) = destination.consented_action_request_timestamp_pst
AND CAST(t.eventdate_pacific AS DATE) = destination.consented_action_request_date_pst
AND LOWER(t.customer_id) = LOWER(destination.customer_id)
AND LOWER(t.request_type) = LOWER(destination.consented_action_request_type)
AND LOWER(t.employee_id) = LOWER(destination.seller_id) WHEN MATCHED THEN
UPDATE
SET
    seller_id_type = t.employee_id WHEN NOT MATCHED THEN INSERT (
        consented_action_request_date_pst,
        consented_action_request_timestamp_pst,
        customer_id,
        customer_id_type,
        seller_id,
        seller_id_type,
        consented_action_request_type
    )
VALUES
    (
        CAST(t.eventdate_pacific AS DATE),
        CAST(t.event_tmstp_pacific AS DATETIME),
        t.customer_id,
        t.customer_id_type,
        t.employee_id,
        t.employee_id_type,
        t.request_type
    );

MERGE INTO `{{params.gcp_project_id}}`.{{params.fact_db}}.consented_clienteling_relationship_fact AS destination USING (
    SELECT
        `{{params.gcp_project_id}}.NORD_UDF.EPOCH_TMSTP`(CAST(TRUNC(CAST(eventtime AS FLOAT64)) AS BIGINT)) AS eventtime_utc,
        DATE((`{{params.gcp_project_id}}.NORD_UDF.EPOCH_TMSTP`(CAST(TRUNC(CAST(eventtime AS FLOAT64)) AS BIGINT))),'PST8PDT') AS eventdate_pacific,
        DATETIME(`{{params.gcp_project_id}}.NORD_UDF.EPOCH_TMSTP`(CAST(TRUNC(CAST(eventtime AS FLOAT64)) AS BIGINT)) ,'PST8PDT') AS event_tmstp_pacific,
        customer_id,
        customer_id_type,
        employee_id,
        employee_id_type,
        'OPT_OUT' AS request_type
    FROM
        `{{params.gcp_project_id}}`.{{params.stg_db}}.clienteling_relationship_ended_ldg
) AS t ON CAST(t.event_tmstp_pacific AS DATETIME) = destination.consented_action_request_timestamp_pst
AND CAST(t.eventdate_pacific AS DATE) = destination.consented_action_request_date_pst
AND LOWER(t.customer_id) = LOWER(destination.customer_id)
AND LOWER(t.request_type) = LOWER(destination.consented_action_request_type)
AND LOWER(t.employee_id) = LOWER(destination.seller_id) WHEN MATCHED THEN
UPDATE
SET
    seller_id_type = t.employee_id WHEN NOT MATCHED THEN INSERT (
        consented_action_request_date_pst,
        consented_action_request_timestamp_pst,
        customer_id,
        customer_id_type,
        seller_id,
        seller_id_type,
        consented_action_request_type
    )
VALUES
    (
        CAST(t.eventdate_pacific AS DATE),
        CAST(t.event_tmstp_pacific AS DATETIME),
        t.customer_id,
        t.customer_id_type,
        t.employee_id,
        t.employee_id_type,
        t.request_type
    );

-- SET QUERY_BAND = NONE FOR SESSION;
