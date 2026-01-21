
DELETE FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.adoption_metrics{{params.env_suffix}} WHERE fiscal_month IN (SELECT DISTINCT fiscal_month FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.adoption_staging{{params.env_suffix}});

INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.adoption_metrics{{params.env_suffix}}
SELECT 
     a.*
    ,CAST(current_datetime('PST8PDT') AS TIMESTAMP) AS update_timestamp,
    `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as update_timestamp_tz

FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.adoption_staging{{params.env_suffix}} a
;
