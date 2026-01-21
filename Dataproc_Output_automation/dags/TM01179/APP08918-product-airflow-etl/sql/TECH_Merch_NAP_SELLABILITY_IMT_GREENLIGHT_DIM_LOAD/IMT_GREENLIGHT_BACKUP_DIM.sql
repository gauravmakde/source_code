BEGIN
DECLARE activityCount_var INT64 DEFAULT 0;


SELECT COUNT(*)
FROM {{params.gcp_project_id}}.T2DL_DAS_ETE_INSTRUMENTATION.INFORMATION_SCHEMA.TABLES
WHERE LOWER(table_name) = LOWER('greenLight_dim_bkp') AND LOWER(table_schema) = LOWER('T2DL_DAS_ETE_INSTRUMENTATION')
HAVING COUNT(*) > 0;

SET activityCount_var  =  (SELECT COUNT(*) AS `__count__`
FROM (SELECT COUNT(*)
        FROM {{params.gcp_project_id}}.T2DL_DAS_ETE_INSTRUMENTATION.INFORMATION_SCHEMA.TABLES
        WHERE LOWER(table_name) = LOWER('greenLight_dim_bkp') AND LOWER(table_schema) = LOWER('T2DL_DAS_ETE_INSTRUMENTATION')
        HAVING COUNT(*) > 0) AS t1);
SET activityCount_var =  CASE WHEN @@row_count IS NOT NULL THEN @@row_count ELSE activityCount_var END;

IF activityCount_var <> 0 THEN

    DROP TABLE IF EXISTS {{params.gcp_project_id}}.t2dl_das_ete_instrumentation.greenlight_dim_bkp;

END IF;


CREATE TABLE IF NOT EXISTS  {{params.gcp_project_id}}.t2dl_das_ete_instrumentation.greenlight_dim_bkp 
CLONE  {{params.gcp_project_id}}.t2dl_das_ete_instrumentation.greenlight_dim;


END;

