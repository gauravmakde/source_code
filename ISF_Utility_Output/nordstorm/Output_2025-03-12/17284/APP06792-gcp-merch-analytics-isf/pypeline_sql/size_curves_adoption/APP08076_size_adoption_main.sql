/*
Size Curves Adoption Metrics Insert
Author: Sara Riker
12/11/23: Create Script

    {{environment_schema}}: t2dl_das_size
    Inserts adoption metrics from s3 staging table
*/


DELETE FROM {environment_schema}.adoption_metrics{env_suffix} WHERE fiscal_month IN (SELECT DISTINCT fiscal_month FROM {environment_schema}.adoption_staging{env_suffix});

INSERT INTO {environment_schema}.adoption_metrics{env_suffix}
SELECT 
     a.*
    ,CURRENT_TIMESTAMP AS update_timestamp
FROM {environment_schema}.adoption_staging{env_suffix} a
;