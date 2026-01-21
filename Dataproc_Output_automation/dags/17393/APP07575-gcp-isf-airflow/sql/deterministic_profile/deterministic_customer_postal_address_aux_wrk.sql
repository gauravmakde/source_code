/* SET QUERY_BAND = '
App_ID={app_id};
DAG_ID=isf_deterministic_customer_postal_address_aux_dim_17393_customer_das_customer;
Task_Name=customer_postal_address_aux_wrk;'
FOR SESSION VOLATILE;*/


--Collect stats and close the transaction as Stats and DDL can only be the last statement of the transaction
--COLLECT STATS ON PRD_NAP_DIM.PENDING_ICON_CUST_ADDR_LDG_DIM --{tbl_sfx};

--Build the Work table for the records to be refreshed

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.customer_obj_postal_address_aux_wrk{{params.tbl_sfx}};

-- Insert new records into the work table
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.customer_obj_postal_address_aux_wrk{{params.tbl_sfx}}  (
  unique_source_id,
  customer_source,
  address_id,
  country_code,
  postal_code,
  address_types,
  object_event_tmstp,
  object_event_tmstp_tz,
  source_audit_update_tmstp,
  source_audit_update_tmstp_tz,
  dw_sys_load_tmstp
)
WITH ldg AS (
    select 
    unique_source_id,
    customer_source,
    address_id,
    country_code,
    postal_code,
    address_types,
    object_event_tmstp,
    cast(COALESCE(`{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(object_event_tmstp AS STRING)),'') as timestamp) as object_event_tmstp_tz,
    source_audit_update_tmstp,
    cast(COALESCE(`{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(source_audit_update_tmstp AS STRING)),'') as timestamp) as source_audit_update_tmstp_tz
    from(
        SELECT DISTINCT 
            TRIM(uniqueSourceId) AS unique_source_id,
            TRIM(SUBSTR(uniquesourceid, 1, GREATEST(0, STRPOS(uniquesourceid, '::') - 1))) AS customer_source,
            TRIM(addressid) AS address_id,
            TRIM(COALESCE(country, '')) AS country_code,
            TRIM(COALESCE(postalCode, '')) AS postal_code,
            TRIM(COALESCE(addressTypes, '')) AS address_types,
            CASE
            WHEN SAFE_CAST(eventTimestamp AS INT64) IS NOT NULL THEN 
                TIMESTAMP_SECONDS(cast(SAFE_CAST(eventTimestamp AS int64) / 1000 as int64)) + 
                MOD(SAFE_CAST(eventTimestamp AS INT64), 1000) * INTERVAL 1 SECOND 
            ELSE 
                TIMESTAMP( CONCAT(
                    CASE
                        WHEN LENGTH(COALESCE(trim(eventTimestamp), '')) > 0 THEN
                        SUBSTR(TRIM(COALESCE(eventTimestamp, '')), 1, LENGTH(TRIM(COALESCE(eventTimestamp, ''))) - 1)
                        ELSE
                            NULL  -- or handle the empty case as needed
                    END,
                'Z'
                )
)
            END AS object_event_tmstp,
            SAFE_CAST(
            CASE
                WHEN REGEXP_CONTAINS(addresses_audit_updatedauditDate, r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$') THEN 
                REPLACE(REPLACE(addresses_audit_updatedauditDate, 'T', ' '), 'Z', '+00:00')
                WHEN REGEXP_CONTAINS(addresses_audit_updatedauditDate, r'^\w{3} \w{3} \d{2} \d{2}:\d{2}:\d{2} \w{3} \d{4}$') THEN 
                FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S.%f %z', PARSE_TIMESTAMP('MON dd mm:ss tzh:tzm yyyy', REPLACE( REPLACE( REPLACE(  SUBSTR(addresses_audit_updatedauditDate, 5),'UTC', '+00:00'),'PDT', '-07:00'),'PST', '-08:00') ))
                ELSE '1990-01-01 00:00:00.000000+00:00'
            END AS TIMESTAMP
            ) AS source_audit_update_tmstp,
            dw_sys_load_tmstp
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.pending_icon_cust_addr_ldg_dim{{params.tbl_sfx}}
        WHERE uniquesourceid IS NOT NULL
            AND addressid IS NOT NULL
            AND eventTimestamp IS NOT NULL
            AND eventTimestamp <> '""'
    )
)
, dim AS (
  SELECT unique_source_id,
         MAX(object_event_tmstp) AS object_event_tmstp,
         MIN(dw_sys_load_tmstp) AS dw_sys_load_tmstp
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_postal_address_aux_dim{{params.tbl_sfx}}
  GROUP BY unique_source_id
)
SELECT ldg.unique_source_id,
       ldg.customer_source,
       ldg.address_id,
       ldg.country_code,
       ldg.postal_code,
       ldg.address_types,
       ldg.object_event_tmstp,
       cast(ldg.object_event_tmstp_tz as string),
       ldg.source_audit_update_tmstp,
       cast(ldg.source_audit_update_tmstp_tz as string),
       dim.dw_sys_load_tmstp
FROM ldg
LEFT JOIN dim ON ldg.unique_source_id = dim.unique_source_id
WHERE ldg.object_event_tmstp > CAST(dim.object_event_tmstp  AS TIMESTAMP)
OR dim.object_event_tmstp IS NULL;

-- Note: Collecting statistics is not directly supported in BigQuery