-- /*
-- Size Curves Adoption s3 TPT
-- Author: Sara Riker
-- 12/11/23: Create Script

--     {environment_schema}: t2dl_das_size
--     pulls adoption csv from s3 into staging table
-- */
DECLARE OUT_MESSAGE STRING;

 drop table if exists `{{params.gcp_project_id}}`.{{params.environment_schema}}.adoption_staging;
--CALL sys_mgmt.drop_if_exists_sp ('{{params.environment_schema}}', 'adoption_staging{{params.env_suffix}}', OUT_RETURN_MSG);


CREATE OR REPLACE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.adoption_staging{{params.env_suffix}}
 (
fiscal_month INTEGER,
plan_seasonid INTEGER,
channel_id INTEGER,
banner STRING,
dept_id INTEGER,
size_profile STRING,
rcpt_dollars NUMERIC,
rcpt_units INTEGER,
plan_keys INTEGER,
po_keys INTEGER,
extract_date DATE
) ;

-- DECLARE OUT_MESSAGE STRING;

CALL `{{params.gcp_project_id}}`.SYS_MGMT.S3_TPT_LOAD ('{{params.environment_schema}}','adoption_staging{{params.env_suffix}}','us-west-2','size-curve','adoption/','adoption_extract.csv','2C',OUT_MESSAGE);
