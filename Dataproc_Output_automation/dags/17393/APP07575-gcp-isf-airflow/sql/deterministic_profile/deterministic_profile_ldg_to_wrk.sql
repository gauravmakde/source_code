/* SET QUERY_BAND = '
App_ID={app_id};
DAG_ID=isf_deterministic_profile_ldg_to_dim_17393_customer_das_customer;
Task_Name=deterministic_profile_ldg_to_wrk;'
FOR SESSION VOLATILE;*/


-----------------------------------------------------------------------------
----------------- DETERMINISTIC_CUSTOMER_CLASSIFICATION_WRK -----------------
-----------------------------------------------------------------------------



TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.deterministic_customer_classification_wrk{{params.tbl_sfx}};



INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.deterministic_customer_classification_wrk{{params.tbl_sfx}} --{tbl_sfx}

(
  deterministicprofileid
  , classifications_verificationstatus
  , classifications_partnerpool
  , header_transponderid
  , header_eventTime
  ,header_eventtime_tz
  , dw_sys_load_tmstp
)
SELECT ldg.deterministicprofileid
  , ldg.classifications_verificationstatus
  , ldg.classifications_partnerpool
  , ldg.header_transponderid
  , ldg.header_eventTime
  , ldg.header_eventtime_tz
  , dim.m_dw_sys_load_tmstp
FROM (
  SELECT DISTINCT trim(ldg.deterministicprofileid) AS deterministicprofileid
    , trim(ldg.classifications_verificationstatus) AS classifications_verificationstatus
    , trim(ldg.classifications_partnerpool) AS classifications_partnerpool
    , trim(ldg.header_transponderid) AS header_transponderid
    ,cast(concat(substr(replace(trim(ldg.header_eventtime, ' '), 'T', ' '), 1, 19), '+00:00')as TIMESTAMP) AS header_eventtime
    ,'+00:00' as header_eventtime_tz

    /*
      --With NULL handling
      , cast(Substr(OReplace(coalesce(ldg.header_eventTime, '1900-01-01 00:00:00+00:00' ) , 'T',' ') , 1, 19) || '+00:00'
          AS timestamp(0) with time zone FORMAT 'Y4-MM-DDBHH:MI:SS') AS header_eventTime
      --To convert transponderid into timestamp
      , cast(Substr(header_transponderid , 1 , 14) || '.' || Substr(header_transponderid ,15,3) || '+00:00'
          AS timestamp(3) with time zone FORMAT 'YYYYMMDDHHMISSDs(F)Z') AS header_transponderid_ts
    */
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.deterministic_customer_classification_ldg{{params.tbl_sfx}}-- {tbl_sfx}
  ldg


 WHERE LOWER(TRIM(COALESCE(ldg.deterministicprofileid, 'null'))) <> 'null'
  AND LOWER(TRIM(COALESCE(ldg.header_transponderid, 'null'))) <> 'null'
  AND LOWER(TRIM(COALESCE(ldg.header_eventTime, 'null'))) <> 'null'
  --Take the latest record for each profile based on transponderid
  QUALIFY rank() over(PARTITION BY trim(ldg.deterministicprofileid) ORDER BY trim(ldg.header_transponderid) DESC ) = 1
  ) ldg
  LEFT OUTER JOIN
  ( SELECT deterministic_profile_id
    , max(profile_event_tmstp) AS m_profile_event_tmstp
    , min(dw_sys_load_tmstp) AS m_dw_sys_load_tmstp
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.deterministic_customer_classification_dim{{params.tbl_sfx}} --{tbl_sfx}

    GROUP BY 1
  ) dim
  ON ldg.deterministicprofileid = dim.deterministic_profile_id
  --LDG records which has header_eventTime greater then DIM or new records
  WHERE ( ldg.header_eventTime > CAST(dim.m_profile_event_tmstp AS TIMESTAMP) OR dim.deterministic_profile_id IS NULL)
;






--COLLECT STATS ON `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_STG.DETERMINISTIC_CUSTOMER_CLASSIFICATION_WRK --{tbl_sfx} 


CREATE TEMPORARY TABLE IF NOT EXISTS vt_deterministic_customer_transaction_max
AS
SELECT TRIM(deterministicprofileid) AS deterministicprofileid,
 MAX(TRIM(header_transponderid)) AS m_header_transponderid
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.deterministic_customer_transaction_ldg{{params.tbl_sfx}} AS ldg
WHERE LOWER(TRIM(COALESCE(deterministicprofileid, 'null'))) <> LOWER('null')
 AND LOWER(TRIM(COALESCE(header_transponderid, 'null'))) <> LOWER('null')
 AND LOWER(TRIM(COALESCE(globaltransactionid, 'null'))) <> LOWER('null')
 AND LOWER(TRIM(COALESCE(businessdaydate, 'null'))) <> LOWER('null')
GROUP BY deterministicprofileid;


--COLLECT STATS COLUMN(deterministicprofileid) ON VT_DETERMINISTIC_CUSTOMER_TRANSACTION_MAX


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.deterministic_customer_transaction_wrk{{params.tbl_sfx}};




INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.deterministic_customer_transaction_wrk{{params.tbl_sfx}}
(deterministicprofileid, globaltransactionid, businessdaydate, header_transponderid, header_eventtime,header_eventtime_tz)
  SELECT DISTINCT
      -- {tbl_sfx}
      trim(ldg.deterministicprofileid, ' '),
      cast(trunc(cast(trim(ldg.globaltransactionid, ' ') as float64)) as int64),
      CAST(trim(substr(trim(ldg.businessdaydate, ' '), 1, 10), ' ') as DATE) AS businessdaydate,
      trim(ldg.header_transponderid, ' '),
      cast(concat(substr(replace(trim(ldg.header_eventtime, ' '), 'T', ' '), 1, 19), '+00:00')as TIMESTAMP) AS header_eventtime,
     '+00:00' AS header_eventtime_tz

    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.deterministic_customer_transaction_ldg{{params.tbl_sfx}} AS ldg
      INNER JOIN --  {tbl_sfx}
      vt_deterministic_customer_transaction_max AS m ON 
       LOWER(TRIM(ldg.deterministicprofileid, ' ')) = LOWER(RTRIM(m.deterministicprofileid, ' '))
AND LOWER(TRIM(ldg.header_transponderid, ' ')) = LOWER(RTRIM(m.m_header_transponderid, ' '))
    WHERE upper(trim(coalesce(ldg.deterministicprofileid, 'null'), ' ')) <> 'NULL'
     AND upper(trim(coalesce(ldg.header_transponderid, 'null'), ' ')) <> 'NULL'
     AND upper(trim(coalesce(ldg.globaltransactionid, 'null'), ' ')) <> 'NULL'
     AND upper(trim(coalesce(ldg.businessdaydate, 'null'), ' ')) <> 'NULL'
;
--Use Max table instead of rank() and qualify to have better performance
--QUALIFY rank() over(PARTITION BY ldg.deterministicprofileid ORDER BY ldg.header_transponderid DESC ) = 1






--COLLECT STATS ON `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_STG.DETERMINISTIC_CUSTOMER_TRANSACTION_WRK --{tbl_sfx} 


CREATE TEMPORARY TABLE IF NOT EXISTS vt_deterministic_customer_frt_max
AS
SELECT TRIM(deterministicprofileid) AS deterministicprofileid,
 MAX(TRIM(header_transponderid)) AS m_header_transponderid
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.deterministic_customer_frt_ldg{{params.tbl_sfx}} AS ldg
WHERE LOWER(TRIM(COALESCE(deterministicprofileid, 'null'))) <> LOWER('null')
 AND LOWER(TRIM(COALESCE(header_transponderid, 'null'))) <> LOWER('null')
 AND LOWER(TRIM(COALESCE(financialretailtransactionrecordid, 'null'))) <> LOWER('null')
 AND LOWER(TRIM(COALESCE(FORMAT('%20d', enterpriseretailtransactionlegacyid), 'null'))) <> LOWER('null')
 AND LOWER(TRIM(COALESCE(businessdaydate, 'null'))) <> LOWER('null')
GROUP BY deterministicprofileid;


--COLLECT STATS COLUMN(deterministicprofileid) ON VT_DETERMINISTIC_CUSTOMER_FRT_MAX


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.deterministic_customer_frt_wrk{{params.tbl_sfx}};


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.deterministic_customer_frt_wrk{{params.tbl_sfx}} (deterministicprofileid, financialretailtransactionrecordid, enterpriseretailtransactionlegacyid, businessdaydate, header_transponderid, header_eventtime,header_eventtime_tz)
  SELECT DISTINCT
      -- {tbl_sfx}
      trim(ldg.deterministicprofileid, ' '),
      trim(ldg.financialretailtransactionrecordid, ' '),
      ldg.enterpriseretailtransactionlegacyid,
      CAST(trim(substr(trim(ldg.businessdaydate, ' '), 1, 10), ' ') as DATE) AS businessdaydate,
      trim(ldg.header_transponderid, ' '),
      cast(concat(substr(replace(trim(ldg.header_eventtime, ' '), 'T', ' '), 1, 19), '+00:00')as TIMESTAMP) AS header_eventtime,
     '+00:00' AS header_eventtime_tz

    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.deterministic_customer_frt_ldg{{params.tbl_sfx}} AS ldg
      INNER JOIN --  {tbl_sfx}
      vt_deterministic_customer_frt_max AS m ON
      LOWER(TRIM(ldg.deterministicprofileid, ' ')) = LOWER(RTRIM(m.deterministicprofileid, ' '))
AND LOWER(TRIM(ldg.header_transponderid, ' ')) = LOWER(RTRIM(m.m_header_transponderid, ' '))
    WHERE upper(trim(coalesce(ldg.deterministicprofileid, 'null'), ' ')) <> 'NULL'
     AND upper(trim(coalesce(ldg.header_transponderid, 'null'), ' ')) <> 'NULL'
     AND upper(trim(coalesce(ldg.financialretailtransactionrecordid, 'null'), ' ')) <> 'NULL'
     AND upper(trim(coalesce(cast(ldg.enterpriseretailtransactionlegacyid as string), 'null'), ' ')) <> 'NULL'
     AND upper(trim(coalesce(ldg.businessdaydate, 'null'), ' ')) <> 'NULL'
;
-- Use Max table instead of rank() and qualify to have better performance
-- QUALIFY rank() over(PARTITION BY ldg.deterministicprofileid ORDER BY ldg.header_transponderid DESC ) = 1






--COLLECT STATS ON `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_STG.DETERMINISTIC_CUSTOMER_FRT_WRK --{tbl_sfx} 


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.deterministic_customer_summary_wrk{{params.tbl_sfx}};




INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.deterministic_customer_summary_wrk{{params.tbl_sfx}}
(deterministicprofileid, uniquesourceid, enterpriseretailtransaction_count, financialretailtransaction_count, header_transponderid, header_eventtime,header_eventtime_tz)
  SELECT DISTINCT
      -- {tbl_sfx}
      trim(ldg.deterministicprofileid, ' '),
      trim(ldg.uniquesourceid, ' '),
      CAST(trunc(cast(coalesce(trim(ldg.enterpriseretailtransaction_count),'0') as float64)) as INT64) AS enterpriseretailtransaction_count,
      CAST(trunc(cast(coalesce(trim(ldg.financialretailtransaction_count),'0')as float64)) as INT64) AS financialretailtransaction_count,
      cast(trunc(cast(trim(ldg.header_transponderid, ' ') as float64))as int64),
      cast(concat(substr(replace(trim(ldg.header_eventtime, ' '), 'T', ' '), 1, 19), '+00:00')as TIMESTAMP) AS header_eventtime,
      '+00:00' AS header_eventtime_tz
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.deterministic_customer_summary_ldg{{params.tbl_sfx}} AS ldg
    WHERE upper(trim(coalesce(ldg.deterministicprofileid, 'null'), ' ')) <> 'NULL'
     AND upper(trim(coalesce(ldg.header_transponderid, 'null'), ' ')) <> 'NULL'
    QUALIFY rank() OVER (PARTITION BY trim(ldg.deterministicprofileid, ' ') ORDER BY trim(ldg.header_transponderid, ' ') DESC) = 1
;
--  {tbl_sfx}







--COLLECT STATS ON `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_STG.DETERMINISTIC_CUSTOMER_SUMMARY_WRK --{tbl_sfx} 


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.deterministic_customer_profile_association_wrk{{params.tbl_sfx}};




INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.deterministic_customer_profile_association_wrk{{params.tbl_sfx}}

 --{tbl_sfx}

(
  deterministicprofileid
  , uniquesourceid
  , businessDayDate
  , header_eventTime
  ,header_eventtime_tz
  , dw_sys_load_tmstp
)
SELECT wrk.deterministicprofileid
  , wrk.uniquesourceid
  , wrk.businessDayDate
  , cast(wrk.header_eventTime as timestamp)
  , wrk.header_eventtime_tz
  , dim.m_dw_sys_load_tmstp
FROM (
  SELECT sum_wrk.deterministicprofileid
    , sum_wrk.uniquesourceid 
    , cast(NULL AS DATE) AS businessDayDate
    , sum_wrk.header_eventTime
    ,sum_wrk.header_eventTime_tz
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.deterministic_customer_summary_wrk{{params.tbl_sfx}}-- {tbl_sfx}
sum_wrk
  UNION ALL
  SELECT tran_wrk.deterministicprofileid
    , cast(tran_wrk.globaltransactionid AS STRING) AS uniquesourceid
    , tran_wrk.businessDayDate
    , tran_wrk.header_eventTime 
    , tran_wrk.header_eventtime_tz
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.deterministic_customer_transaction_wrk{{params.tbl_sfx}}-- {tbl_sfx}
tran_wrk
  UNION ALL
  SELECT frt_wrk.deterministicprofileid
    , cast(frt_wrk.enterpriseRetailTransactionLegacyId AS STRING) AS uniquesourceid
    , frt_wrk.businessDayDate
    , frt_wrk.header_eventTime
    ,frt_wrk.header_eventTime_tz
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.deterministic_customer_frt_wrk{{params.tbl_sfx}}-- {tbl_sfx}
frt_wrk
  ) wrk
  LEFT OUTER JOIN
  ( SELECT deterministic_profile_id
      , max(profile_event_tmstp) AS m_profile_event_tmstp
      , min(dw_sys_load_tmstp) AS m_dw_sys_load_tmstp
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.deterministic_customer_profile_association_dim{{params.tbl_sfx}}-- {tbl_sfx}
dim
    GROUP BY 1
  ) dim
  ON  LOWER(wrk.deterministicprofileid) = LOWER(dim.deterministic_profile_id)
  --WRK records which has Max(profile_event_tmstp) greater then DIM or new records
  WHERE ( wrk.header_eventTime > dim.m_profile_event_tmstp OR dim.deterministic_profile_id IS NULL)
;




--COLLECT STATS ON `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_STG.DETERMINISTIC_CUSTOMER_PROFILE_ASSOCIATION_WRK --{tbl_sfx} 