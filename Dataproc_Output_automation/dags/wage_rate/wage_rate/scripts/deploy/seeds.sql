
DELETE FROM <bq_project_id>.<DBENV>_sca_prf.dl_interface_dt_lkup
WHERE LOWER(interface_id) = LOWER('WAGE') AND LOWER(subject_id) = LOWER('AVERAGE_RATE');

INSERT INTO <bq_project_id>.<DBENV>_sca_prf.dl_interface_dt_lkup
( INTERFACE_ID
	,SUBJECT_ID
  ,BTCH_ID
	,CURR_LOAD_DT
	,START_TMSTP
	,END_TMSTP)
(SELECT *
    FROM (SELECT 'WAGE' AS interface_id, 'AVERAGE_RATE' AS subject_id, 0 AS btch_id, DATE '2022-11-14' AS curr_load_dt, CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS start_tmstp, CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS end_tmstp) AS t0
    WHERE NOT EXISTS (SELECT 1 AS `A12180`
            FROM <bq_project_id>.<DBENV>_sca_prf.dl_interface_dt_lkup
            WHERE interface_id = t0.interface_id AND subject_id = t0.subject_id AND btch_id = t0.btch_id));

DELETE FROM <bq_project_id>.<DBENV>_sca_prf.tpt_control_tbl
WHERE LOWER(job_name) = LOWER('WAGE_RATE_S3_TO_STG');




INSERT INTO <bq_project_id>.<DBENV>_sca_prf.tpt_control_tbl
(job_name,
database_name,
object_name,
object_type,
batchusername,
truncateindicator,
errorlimit,
textdelimiter_hexa_flag,
textdelimiter,
acceptexcesscolumns,
acceptmissingcolumns,
quoteddata,
openquotemark,	
s3bucket,	
s3prefix,	
s3singlepartfile,	
s3connectioncount,	
collist_indicator_flag,
collist,	
rcd_load_tmstp,	
rcd_update_tmstp,	
headerrcdindicator,
allfilesheaderrcdindicator )
(SELECT *
 FROM (SELECT 'WAGE_RATE_S3_TO_STG' AS job_name,
    '<DBENV>_SCA_PRF' AS database_name,
    'WAGE_RATE_STG' AS object_name,
    'T' AS object_type,
    '<DBENV>_NAP_SCA_BATCH' AS batchusername,
    'Y' AS truncateindicator,
    '1' AS errorlimit,
    'Y' AS textdelimiter_hexa_flag,
    '2C' AS textdelimiter,
    'N' AS acceptexcesscolumns,
    'N' AS acceptmissingcolumns,
    'N' AS quoteddata,
    cast(NULL as string) AS openquotemark,
    cast(NULL as string) AS s3bucket,
    cast(NULL as string) AS s3prefix,
    cast(NULL as string) AS s3singlepartfile,
    '20' AS s3connectioncount,
    'N' AS collist_indicator_flag,
    cast(NULL as string) AS collist,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS rcd_load_tmstp,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS rcd_update_tmstp,
    'Y' AS headerrcdindicator,
    'Y' AS allfilesheaderrcdindicator) AS t0
 WHERE NOT EXISTS (SELECT 1 AS `A12180`
   FROM <bq_project_id>.<DBENV>_sca_prf.tpt_control_tbl
   WHERE job_name = t0.job_name));
