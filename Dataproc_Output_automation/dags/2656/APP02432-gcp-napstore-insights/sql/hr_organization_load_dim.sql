

CREATE TEMPORARY TABLE hr_worker_org_dim_unique_temp                                 
AS
(SELECT DISTINCT organizationid,
 effectivedate,
  CASE
  WHEN LOWER(isinactive) = LOWER('')
  THEN NULL
  ELSE isinactive
  END AS isinactive,
 lastupdated,
 lastupdated_tz,
  CASE
  WHEN LOWER(organizationcode) = LOWER('')
  THEN NULL
  ELSE organizationcode
  END AS organizationcode,
  CASE
  WHEN LOWER(organizationdescription) = LOWER('')
  THEN NULL
  ELSE organizationdescription
  END AS organizationdescription,
  CASE
  WHEN LOWER(organizationname) = LOWER('')
  THEN NULL
  ELSE organizationname
  END AS organizationname,
  CASE
  WHEN LOWER(organizationtype) = LOWER('')
  THEN NULL
  ELSE organizationtype
  END AS organizationtype
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_hr_stg.hr_worker_org_ldg
QUALIFY RANK() OVER (PARTITION BY organizationid ORDER BY lastupdated DESC) = 1);



MERGE INTO {{params.gcp_project_id}}.{{params.dbenv}}_NAP_HR_DIM.HR_WORKER_ORG_DIM AS tgt
USING hr_worker_org_dim_unique_temp AS src
ON LOWER(src.organizationid) = LOWER(tgt.organization_id)
WHEN MATCHED THEN UPDATE SET
    effective_date = CAST(src.effectivedate AS DATE),
    is_inactive = src.isinactive,
    last_updated = src.lastupdated,
    last_updated_tz = src.lastupdated_tz,
    organization_code = src.organizationcode,
    organization_description = src.organizationdescription,
    organization_name = src.organizationname,
    organization_type = src.organizationtype,
    dw_batch_date = CURRENT_DATE('PST8PDT'),
    dw_sys_load_tmstp = current_datetime('PST8PDT')
WHEN NOT MATCHED THEN 
INSERT
(
effective_date
,is_inactive
,last_updated
,organization_code
,organization_description
,organization_id
,organization_name
,organization_type
,dw_batch_date
,dw_sys_load_tmstp
,last_updated_tz
) 
VALUES(CAST(src.effectivedate AS DATE), src.isinactive, src.lastupdated, src.organizationcode, src.organizationdescription, src.organizationid, src.organizationname, src.organizationtype, CURRENT_DATE('PST8PDT'), current_datetime('PST8PDT'),src.lastupdated_tz);


