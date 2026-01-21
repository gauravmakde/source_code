/*
RP Anticipated Spend for OTB
Author: Sara Riker
Date Created: 8/25/22
Date Last Updated:  9/8/22

Purpose: Pulls RP anticipated spend from s3 bucket into a staging table

Builds table {environment_schema}.rp_anticipated_spend_staging
*/


--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'rp_anticipated_spend_staging', OUT_RETURN_MSG);
DECLARE OUT_MESSAGE string;


Drop  table if exists  `{{params.gcp_project_id}}`.{{params.environment_schema}}.rp_anticipated_spend_staging ;


CREATE or REPLACE TABLE  `{{params.gcp_project_id}}`.{{params.environment_schema}}.rp_anticipated_spend_staging (
week_idnt STRING(40),
dept STRING(40),
dept_desc STRING(40),
cls_idnt STRING(40),
scls_idnt STRING(40),
supp_idnt STRING(40),
supp_name STRING(40),
banner_id STRING(40),
ft_id STRING(40),
rp_antspnd_u STRING(40),
rp_antspnd_c STRING(40),
rp_antspnd_r STRING(40)
);

CALL SYS_MGMT.S3_TPT_LOAD ('{{params.environment_schema}}','rp_anticipated_spend_staging','us-west-2','rpds-qtrx-extract-prod','prod/apt/','rpspendRetail.csv','7C',OUT_MESSAGE); 


