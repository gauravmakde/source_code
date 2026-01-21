--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=stg-dim;AppSubArea=PRODUCT_PRTNR_RELTN_CUST_RETURN_POLICY_DIM;' UPDATE FOR SESSION;

--NONSEQUENCED VALIDTIME --Purge work table for staging temporal rows
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_prtnr_reltn_cust_return_policy_dim_vtw;
--.IF ERRORCODE <> 0 THEN .QUIT 1

BEGIN TRANSACTION;
---.IF ERRORCODE <> 0 THEN .QUIT 2

-- Update eff_end_tmstp of whichever partner_relationship_num has default value and is getting expired in this batch.
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_prtnr_reltn_cust_return_policy_dim AS target 
SET
 eff_end_tmstp = CAST(SOURCE.effective_end_date_time AS TIMESTAMP) ,
 eff_end_tmstp_tz = `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(CAST(SOURCE.effective_end_date_time AS TIMESTAMP) AS STRING))
 FROM (SELECT dim.cust_return_policy_num,
   dim.eff_begin_tmstp,
   LDG.effective_end_date_time
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_prtnr_reltn_cust_return_policy_dim AS dim
   INNER JOIN (SELECT DISTINCT customerreturnpolicyid AS cust_return_policy_num,
     `{{params.gcp_project_id}}.JWN_UDF.ISO8601_TMSTP`(effectivestartdatetime) AS effective_start_date_time,
     `{{params.gcp_project_id}}.JWN_UDF.ISO8601_TMSTP`(effectiveenddatetime) AS effective_end_date_time,
     `{{params.gcp_project_id}}.JWN_UDF.ISO8601_TMSTP`(lastupdatedtime) AS last_updated_time,
     recsequencenum AS rec_sequence_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_prtnr_reltn_cust_return_policy_ldg
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY cust_return_policy_num ORDER BY rec_sequence_num DESC)) = 1) AS LDG ON
     LOWER(dim.cust_return_policy_num) = LOWER(LDG.cust_return_policy_num) AND CAST(dim.eff_end_tmstp AS DATETIME) = CAST('9999-12-31 23:59:59.999999+00:00' AS DATETIME)
     
  WHERE CAST(LDG.effective_end_date_time AS TIMESTAMP) >= dim.eff_begin_tmstp) AS SOURCE
WHERE LOWER(SOURCE.cust_return_policy_num) = LOWER(target.cust_return_policy_num) AND SOURCE.eff_begin_tmstp = target.eff_begin_tmstp;
--.IF ERRORCODE <> 0 THEN .QUIT 3

COMMIT TRANSACTION;
--.IF ERRORCODE <> 0 THEN .QUIT 4

--Insert Add, Updates into VTW

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_prtnr_reltn_cust_return_policy_dim_vtw
(
partner_relationship_num,
cust_return_policy_num,
cust_return_policy_desc,
days_to_return,
is_in_store_return_eligible_flag,
is_mail_in_return_eligible_flag,
eff_begin_tmstp,
eff_begin_tmstp_tz,
eff_end_tmstp,
eff_end_tmstp_tz
)
SELECT
NRML.partner_relationship_num,
NRML.cust_return_policy_num,
NRML.cust_return_policy_desc,
NRML.days_to_return,
NRML.is_in_store_return_eligible_flag,
NRML.is_mail_in_return_eligible_flag,
RANGE_START(NRML.eff_period) AS eff_begin,
`{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(RANGE_START(NRML.eff_period) as string)) as eff_begin_tmstp_tz,
RANGE_END(NRML.eff_period)   AS eff_end,
`{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(RANGE_END(NRML.eff_period) AS STRING)) as eff_end_tmstp_tz
FROM (
    SELECT DISTINCT --NORMALIZE
    SRC.partner_relationship_num,
    SRC.cust_return_policy_num,
    SRC.cust_return_policy_desc,
    SRC.days_to_return,
    SRC.is_in_store_return_eligible_flag,
    SRC.is_mail_in_return_eligible_flag,
    COALESCE( RANGE_INTERSECT(SRC.eff_period , RANGE(TGT.eff_begin_tmstp,TGT.eff_end_tmstp)), SRC.eff_period ) AS eff_period
    FROM (
        SELECT DISTINCT --NORMALIZE
        SRC2.partner_relationship_num,
        SRC2.cust_return_policy_num,
        SRC2.cust_return_policy_desc,
        SRC2.days_to_return,
        SRC2.is_in_store_return_eligible_flag,
        SRC2.is_mail_in_return_eligible_flag,
        RANGE( eff_begin_tmstp, eff_end_tmstp ) AS eff_period
        FROM (
            SELECT
            SRC1.partner_relationship_num,
            SRC1.cust_return_policy_num,
            SRC1.cust_return_policy_desc,
            SRC1.days_to_return,
            SRC1.is_in_store_return_eligible_flag,
            SRC1.is_mail_in_return_eligible_flag,
            SRC1.eff_begin_tmstp,
            CASE WHEN effective_end_date_time IS NOT NULL THEN effective_end_date_time
            ELSE (COALESCE(MAX(SRC1.eff_begin_tmstp) OVER(PARTITION BY SRC1.cust_return_policy_num, SRC1.partner_relationship_num ORDER BY SRC1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING),
            TIMESTAMP'9999-12-31 23:59:59.999999+00:00')) END AS max_eff_end_tmstp,
            COALESCE((CASE WHEN effective_end_date_time IS NOT NULL THEN effective_end_date_time
            ELSE (COALESCE(MAX(SRC1.eff_begin_tmstp) OVER(PARTITION BY SRC1.cust_return_policy_num, SRC1.partner_relationship_num ORDER BY SRC1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING),
            TIMESTAMP'9999-12-31 23:59:59.999999+00:00')) END) ,TIMESTAMP'9999-12-31 23:59:59.999999+00:00') AS eff_end_tmstp
            FROM (
                SELECT DISTINCT partner_relationship_num,
                cust_return_policy_num,
                cust_return_policy_desc,
                days_to_return,
                is_in_store_return_eligible_flag,
                is_mail_in_return_eligible_flag,
                effective_start_tmstp AS eff_begin_tmstp,
                cust_return_policy_cancelled_event_tmstp as effective_end_date_time,
                cust_return_policy_cancelled_ind,
                src_event_tmstp,
                rec_sequence_num
                FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.product_prtnr_reltn_cust_return_policy_opr OPR
                JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_prtnr_reltn_cust_return_policy_ldg LDG
                ON  LOWER(OPR.partner_relationship_num)  = LOWER(LDG.partnerRelationshipId)
                AND LOWER(OPR.cust_return_policy_num)  = LOWER(LDG.customerReturnPolicyId)
                WHERE effective_start_tmstp IS NOT NULL
                AND cust_return_policy_num IS NOT NULL
                AND LOWER(cust_return_policy_cancelled_ind) <> LOWER('Y')
                QUALIFY ROW_NUMBER() OVER (PARTITION BY partner_relationship_num, cust_return_policy_num, effective_start_tmstp ORDER BY src_event_tmstp DESC,rec_sequence_num DESC)= 1
            ) SRC1
        ) SRC2
        WHERE eff_begin_tmstp < eff_end_tmstp
    ) SRC
    LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_prtnr_reltn_cust_return_policy_dim TGT
    ON SRC.cust_return_policy_num = TGT.cust_return_policy_num
    AND LOWER(SRC.partner_relationship_num) = LOWER(TGT.partner_relationship_num)
    AND RANGE_OVERLAPS(SRC.eff_period , RANGE(TGT.eff_begin_tmstp, TGT.eff_end_tmstp))
    WHERE ( TGT.cust_return_policy_num IS NULL OR
    ((LOWER(SRC.partner_relationship_num) <> LOWER(TGT.partner_relationship_num) OR (SRC.partner_relationship_num IS NOT NULL AND TGT.partner_relationship_num IS NULL) OR (SRC.partner_relationship_num IS NULL AND TGT.partner_relationship_num IS NOT NULL))
    OR (LOWER(SRC.cust_return_policy_desc) <> LOWER(TGT.cust_return_policy_desc) OR (SRC.cust_return_policy_desc IS NOT NULL AND TGT.cust_return_policy_desc IS NULL) OR (SRC.cust_return_policy_desc IS NULL AND TGT.cust_return_policy_desc IS NOT NULL))
    OR (SRC.days_to_return <> TGT.days_to_return OR (SRC.days_to_return IS NOT NULL AND TGT.days_to_return IS NULL) OR (SRC.days_to_return IS NULL AND TGT.days_to_return IS NOT NULL))
    OR (LOWER(SRC.is_in_store_return_eligible_flag) <> LOWER(TGT.is_in_store_return_eligible_flag) OR (SRC.is_in_store_return_eligible_flag IS NOT NULL AND TGT.is_in_store_return_eligible_flag IS NULL) OR (SRC.is_in_store_return_eligible_flag IS NULL AND TGT.is_in_store_return_eligible_flag IS NOT NULL))
    OR (LOWER(SRC.is_mail_in_return_eligible_flag) <> LOWER(TGT.is_mail_in_return_eligible_flag) OR (SRC.is_mail_in_return_eligible_flag IS NOT NULL AND TGT.is_mail_in_return_eligible_flag IS NULL) OR (SRC.is_mail_in_return_eligible_flag IS NULL AND TGT.is_mail_in_return_eligible_flag IS NOT NULL))
    ))
) NRML
;
--.IF ERRORCODE <> 0 THEN .QUIT 5

BEGIN TRANSACTION;
--.IF ERRORCODE <> 0 THEN .QUIT 6

--SEQUENCED VALIDTIME
--SEQUENCED VALIDTIME
DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_prtnr_reltn_cust_return_policy_dim AS tgt
WHERE EXISTS (SELECT 1
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_prtnr_reltn_cust_return_policy_dim_vtw AS src
  WHERE LOWER(src.cust_return_policy_num) = LOWER(tgt.cust_return_policy_num)
  AND LOWER(src.partner_relationship_num) = LOWER(tgt.partner_relationship_num)
  AND src.eff_begin_tmstp <= tgt.eff_begin_tmstp
  AND src.eff_end_tmstp >= tgt.eff_end_tmstp);

UPDATE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_prtnr_reltn_cust_return_policy_dim AS tgt
SET TGT.eff_end_tmstp = SRC.eff_begin_tmstp,
TGT.eff_end_tmstp_tz = SRC.eff_begin_tmstp_tz
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_prtnr_reltn_cust_return_policy_dim_vtw AS src
  WHERE LOWER(src.cust_return_policy_num) = LOWER(tgt.cust_return_policy_num)
   AND LOWER(src.partner_relationship_num) = LOWER(tgt.partner_relationship_num)
    AND SRC.eff_begin_tmstp >= TGT.eff_begin_tmstp
    AND SRC.eff_begin_tmstp < TGT.eff_end_tmstp 
    AND SRC.eff_end_tmstp > TGT.eff_end_tmstp ;


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_prtnr_reltn_cust_return_policy_dim AS tgt
SET TGT.eff_begin_tmstp = SRC.eff_end_tmstp,
 TGT.eff_begin_tmstp_tz = SRC.eff_end_tmstp_tz
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_prtnr_reltn_cust_return_policy_dim_vtw AS src
WHERE LOWER(src.cust_return_policy_num) = LOWER(tgt.cust_return_policy_num)
  AND LOWER(src.partner_relationship_num) = LOWER(tgt.partner_relationship_num)
    AND SRC.eff_end_tmstp > TGT.eff_begin_tmstp
    AND SRC.eff_end_tmstp <= TGT.eff_end_tmstp;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_prtnr_reltn_cust_return_policy_dim (partner_relationship_num, cust_return_policy_num,
 cust_return_policy_desc, days_to_return, is_in_store_return_eligible_flag, is_mail_in_return_eligible_flag,
 eff_begin_tmstp,eff_begin_tmstp_tz, eff_end_tmstp,eff_end_tmstp_tz, dw_batch_id, dw_batch_date, dw_sys_load_date, dw_sys_load_tmstp)
(SELECT DISTINCT partner_relationship_num,
  cust_return_policy_num,
  cust_return_policy_desc,
  days_to_return,
  is_in_store_return_eligible_flag,
  is_mail_in_return_eligible_flag,
  eff_begin_tmstp,
  `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(eff_begin_tmstp AS STRING)) as eff_begin_tmstp_tz,
  eff_end_tmstp,
  `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(eff_end_tmstp AS STRING)) as eff_begin_tmstp_tz,
  dw_batch_id,
  dw_batch_date,
  dw_sys_load_date,
  dw_sys_load_tmstp
 FROM (SELECT partner_relationship_num,
    cust_return_policy_num,
    cust_return_policy_desc,
    days_to_return,
    is_in_store_return_eligible_flag,
    is_mail_in_return_eligible_flag,
    eff_begin_tmstp,
    eff_end_tmstp,
    cast(trunc(cast(RPAD(FORMAT_TIMESTAMP('%Y%m%d%I%M%S', CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)), 14, ' ') as float64))  AS BIGINT) AS dw_batch_id,
    CURRENT_DATE('PST8PDT') AS dw_batch_date,
    CURRENT_DATE('PST8PDT') AS dw_sys_load_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_prtnr_reltn_cust_return_policy_dim_vtw) AS t
 WHERE NOT EXISTS (SELECT 1 AS `A12180`
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_prtnr_reltn_cust_return_policy_dim
   WHERE cust_return_policy_num = t.cust_return_policy_num)
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY cust_return_policy_num)) = 1);


--.IF ERRORCODE <> 0 THEN .QUIT 8

COMMIT TRANSACTION;
--.IF ERRORCODE <> 0 THEN .QUIT 9

-- COLLECT STATISTICS
--                    -- default SYSTEM SAMPLE PERCENT
--                    -- default SYSTEM THRESHOLD PERCENT
--             COLUMN ( partner_relationship_num ) ,
--             COLUMN( cust_return_policy_num ),
--             COLUMN ( eff_begin_tmstp ) ,
--             COLUMN ( eff_end_tmstp )
--                 ON prd_nap_dim.product_prtnr_reltn_cust_return_policy_dim;
