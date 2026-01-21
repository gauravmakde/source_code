BEGIN
DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;

BEGIN TRANSACTION;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.product_prtnr_reltn_cust_return_policy_opr
(
partner_relationship_num,
cust_return_policy_num,
cust_return_policy_desc,
days_to_return,
is_in_store_return_eligible_flag,
is_mail_in_return_eligible_flag,
effective_start_tmstp,
effective_start_tmstp_tz,
effective_end_tmstp,
effective_end_tmstp_tz,
cust_return_policy_cancelled_ind,
cust_return_policy_cancelled_desc,
cust_return_policy_cancelled_event_tmstp,
cust_return_policy_cancelled_event_tmstp_tz,
src_event_tmstp,
src_event_tmstp_tz,
rec_sequence_num,
dw_batch_id,
dw_batch_date,
dw_sys_load_date,
dw_sys_load_tmstp
)
SELECT LDG.partner_relationship_num, 
LDG.cust_return_policy_num, 
LDG.cust_return_policy_desc, 
LDG.days_to_return, 
LDG.is_in_store_return_eligible_flag, 
LDG.is_mail_in_return_eligible_flag, 
CAST(LDG.conv_effective_start_tmstp AS TIMESTAMP),
`{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(CAST(LDG.conv_effective_start_tmstp AS TIMESTAMP) AS STRING)) AS effective_start_tmstp_tz,
CAST(LDG.conv_effective_end_tmstp AS TIMESTAMP),
`{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(CAST(LDG.conv_effective_end_tmstp AS TIMESTAMP) AS STRING)) AS effective_end_tmstp_tz,
'N' AS cust_return_policy_cancelled_ind, 
'' AS cust_return_policy_cancelled_desc, 
NULL AS cust_return_policy_cancelled_event_tmstp,
`{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as cust_return_policy_cancelled_event_tmstp_tz,
CAST(LDG.conv_src_event_tmstp AS TIMESTAMP),
`{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(CAST(LDG.conv_src_event_tmstp AS TIMESTAMP) AS STRING)) AS src_event_tmstp_tz,
LDG.rec_sequence_num, 
CAST(CASE WHEN RPAD(FORMAT_TIMESTAMP('%Y%m%d%I%M%S', CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)), 14, ' ') = '' THEN '0' ELSE RPAD(FORMAT_TIMESTAMP('%Y%m%d%I%M%S', CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)), 14, ' ') END AS BIGINT) AS dw_batch_id, 
CURRENT_DATE('PST8PDT') AS dw_batch_date, 
DATE(LDG.conv_src_event_tmstp) AS dw_sys_load_date, 
CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp

FROM (SELECT DISTINCT 
`{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(lastupdatedtime) AS conv_src_event_tmstp, 
`{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(effectivestartdatetime) AS conv_effective_start_tmstp, 
`{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(effectiveenddatetime) AS conv_effective_end_tmstp, 
partnerrelationshipid AS partner_relationship_num, 
customerreturnpolicyid AS cust_return_policy_num, 
description AS cust_return_policy_desc, 
daystoreturn AS days_to_return, 
CASE WHEN LOWER(TRIM(isinstorereturneligible)) = LOWER('true') THEN 'Y' ELSE 'N' END AS is_in_store_return_eligible_flag, CASE WHEN LOWER(TRIM(ismailinreturneligible)) = LOWER('true') THEN 'Y' ELSE 'N' END AS is_mail_in_return_eligible_flag, recsequencenum AS rec_sequence_num
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_prtnr_reltn_cust_return_policy_ldg
        QUALIFY (ROW_NUMBER() OVER (PARTITION BY partner_relationship_num, partnerrelationshipid, effectivestartdatetime, conv_effective_end_tmstp, lastupdatedtime ORDER BY recsequencenum DESC)) = 1) AS LDG
    LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.product_prtnr_reltn_cust_return_policy_opr AS opr ON LOWER(LDG.partner_relationship_num) = LOWER(opr.partner_relationship_num) AND LOWER(LDG.cust_return_policy_num) = LOWER(opr.cust_return_policy_num) AND CAST(LDG.conv_effective_start_tmstp AS TIMESTAMP) = opr.effective_start_tmstp AND CAST(LDG.conv_src_event_tmstp AS TIMESTAMP) = opr.src_event_tmstp
WHERE opr.partner_relationship_num IS NULL AND LDG.conv_effective_end_tmstp IS NULL;

IF ERROR_CODE <> 0 THEN
    SELECT ERROR('RC = 2') AS `A12180`;
END IF;

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.product_prtnr_reltn_cust_return_policy_opr AS tgt SET
    cust_return_policy_cancelled_ind = 'Y',
    cust_return_policy_cancelled_desc = 'FUTURE_CLEAN_UP_CUSTOMER_RETURN_POLICY',
    cust_return_policy_cancelled_event_tmstp = CAST(SRC.src_event_tmstp AS TIMESTAMP),
    cust_return_policy_cancelled_event_tmstp_tz = SRC.cust_return_policy_cancelled_event_tmstp_tz
    FROM (SELECT opr.partner_relationship_num, 
    opr.cust_return_policy_num, 
    LDG.effective_start_tmstp, 
    LDG.src_event_tmstp, 
    LDG.rec_sequence_num,
    `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(LDG.src_event_tmstp AS STRING)) AS cust_return_policy_cancelled_event_tmstp_tz,
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.product_prtnr_reltn_cust_return_policy_opr AS opr
            INNER JOIN (SELECT DISTINCT ldg1.partnerrelationshipid AS partner_relationship_num, ldg1.customerreturnpolicyid AS cust_return_policy_num, `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(ldg1.effectivestartdatetime) AS effective_start_tmstp, `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(ldg1.effectiveenddatetime) AS effective_end_tmstp, `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(ldg1.lastupdatedtime) AS src_event_tmstp, ldg1.recsequencenum AS rec_sequence_num
                FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_prtnr_reltn_cust_return_policy_ldg AS ldg1
                    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_prtnr_reltn_cust_return_policy_ldg AS ldg2 ON LOWER(ldg1.partnerrelationshipid) = LOWER(ldg2.partnerrelationshipid) AND LOWER(ldg1.customerreturnpolicyid) = LOWER(ldg2.customerreturnpolicyid)
                WHERE ldg1.effectiveenddatetime IS NULL AND LOWER(ldg1.effectivestartdatetime) < LOWER(ldg2.effectivestartdatetime)
                QUALIFY (ROW_NUMBER() OVER (PARTITION BY partner_relationship_num, cust_return_policy_num, effective_start_tmstp ORDER BY src_event_tmstp DESC, rec_sequence_num DESC)) = 1) AS LDG ON LOWER(opr.partner_relationship_num) = LOWER(LDG.partner_relationship_num) AND LOWER(opr.cust_return_policy_num) = LOWER(LDG.cust_return_policy_num)
        QUALIFY (ROW_NUMBER() OVER (PARTITION BY opr.partner_relationship_num, opr.cust_return_policy_num ORDER BY LDG.effective_end_tmstp DESC, LDG.rec_sequence_num DESC)) = 1) AS SRC
WHERE LOWER(SRC.partner_relationship_num) = LOWER(tgt.partner_relationship_num) AND LOWER(SRC.cust_return_policy_num) = LOWER(tgt.cust_return_policy_num) AND CAST(SRC.effective_start_tmstp AS TIMESTAMP) = tgt.effective_start_tmstp;

IF ERROR_CODE <> 0 THEN
    SELECT ERROR('RC = 3') AS `A12180`;
END IF;

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.product_prtnr_reltn_cust_return_policy_opr AS tgt SET
    cust_return_policy_desc = t6.cust_return_policy_desc,
    days_to_return = t6.days_to_return,
    is_in_store_return_eligible_flag = t6.is_in_store_return_eligible_flag,
    is_mail_in_return_eligible_flag = t6.is_mail_in_return_eligible_flag,
    cust_return_policy_cancelled_ind = 'N',
    cust_return_policy_cancelled_desc = 'END_CURRENT_CUSTOMER_RETURN_POLICY',
    cust_return_policy_cancelled_event_tmstp = CAST(t6.effective_end_tmstp AS TIMESTAMP),
    cust_return_policy_cancelled_event_tmstp_tz = t6.cust_return_policy_cancelled_event_tmstp_tz
    FROM (SELECT partner_relationship_num, 
    cust_return_policy_num, 
    effective_start_tmstp, 
    src_event_tmstp, 
    cust_return_policy_desc0 AS cust_return_policy_desc, 
    days_to_return0 AS days_to_return, 
    is_in_store_return_eligible_flag0 AS is_in_store_return_eligible_flag, 
    is_mail_in_return_eligible_flag0 AS is_mail_in_return_eligible_flag, 
    effective_end_tmstp0 AS effective_end_tmstp, 
    `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(effective_end_tmstp0 AS STRING)) AS cust_return_policy_cancelled_event_tmstp_tz
        FROM (SELECT DISTINCT opr.partner_relationship_num, opr.cust_return_policy_num, opr.effective_start_tmstp, opr.src_event_tmstp, LDG.cust_return_policy_desc AS cust_return_policy_desc0, LDG.days_to_return AS days_to_return0, LDG.is_in_store_return_eligible_flag AS is_in_store_return_eligible_flag0, LDG.is_mail_in_return_eligible_flag AS is_mail_in_return_eligible_flag0, LDG.effective_end_tmstp AS effective_end_tmstp0, 
                FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.product_prtnr_reltn_cust_return_policy_opr AS opr
                    INNER JOIN (SELECT DISTINCT partnerrelationshipid AS partner_relationship_num, customerreturnpolicyid AS cust_return_policy_num, description AS cust_return_policy_desc, daystoreturn AS days_to_return, CASE WHEN LOWER(TRIM(isinstorereturneligible)) = LOWER('true') THEN 'Y' ELSE 'N' END AS is_in_store_return_eligible_flag, CASE WHEN LOWER(TRIM(ismailinreturneligible)) = LOWER('true') THEN 'Y' ELSE 'N' END AS is_mail_in_return_eligible_flag, `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(effectivestartdatetime) AS effective_start_tmstp, `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(effectiveenddatetime) AS effective_end_tmstp, `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(lastupdatedtime) AS src_event_tmstp, recsequencenum AS rec_sequence_num
                        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_prtnr_reltn_cust_return_policy_ldg
                        WHERE LOWER(`{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(effectivestartdatetime)) <= LOWER(`{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(effectiveenddatetime)) AND effectiveenddatetime IS NOT NULL
                        QUALIFY (ROW_NUMBER() OVER (PARTITION BY partner_relationship_num, cust_return_policy_num, effective_start_tmstp ORDER BY src_event_tmstp DESC, rec_sequence_num DESC)) = 1) AS LDG ON LOWER(opr.partner_relationship_num) = LOWER(LDG.partner_relationship_num) AND LOWER(opr.cust_return_policy_num) = LOWER(LDG.cust_return_policy_num) AND opr.effective_start_tmstp = CAST(LDG.effective_start_tmstp AS TIMESTAMP)) AS SRC) AS t6
WHERE LOWER(t6.partner_relationship_num) = LOWER(tgt.partner_relationship_num) AND LOWER(t6.cust_return_policy_num) = LOWER(tgt.cust_return_policy_num) AND t6.effective_start_tmstp = tgt.effective_start_tmstp AND t6.src_event_tmstp = tgt.src_event_tmstp AND LOWER(tgt.cust_return_policy_cancelled_ind) <> LOWER('Y');

IF ERROR_CODE <> 0 THEN
    SELECT ERROR('RC = 4') AS `A12180`;
END IF;

COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
ROLLBACK TRANSACTION;
RAISE USING MESSAGE = @@error.message;

END;
