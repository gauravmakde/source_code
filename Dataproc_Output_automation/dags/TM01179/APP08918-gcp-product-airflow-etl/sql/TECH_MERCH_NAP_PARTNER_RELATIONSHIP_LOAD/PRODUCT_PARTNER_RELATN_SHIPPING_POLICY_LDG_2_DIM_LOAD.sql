BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND='AppName=NAP-Merch-Dimension;
AppRelease=1;AppFreq=Daily;AppPhase=ldg-dim;
AppSubArea=PRODUCT_PARTNER_SHIP_POLICY_DIM;' UPDATE FOR SESSION;*/
---NONSEQUENCED VALIDTIME
BEGIN
SET _ERROR_CODE  =  0;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_partner_ship_policy_dim_vtw;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

IF _ERROR_CODE <> 0 THEN
    SELECT ERROR('RC = 1') AS `A12180`;
END IF;

BEGIN TRANSACTION;

IF _ERROR_CODE <> 0 THEN
    SELECT ERROR('RC = 2') AS `A12180`;
END IF;

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_partner_ship_policy_dim AS target 
SET
    eff_end_tmstp = CAST(SOURCE.effective_end_date_time AS TIMESTAMP) 
FROM (SELECT dim.partner_relationship_num, dim.eff_begin_tmstp, LDG.effective_end_date_time
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_partner_ship_policy_dim AS dim
      INNER JOIN (SELECT DISTINCT partnerrelationshipid AS partner_relationship_num, 
          `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(effectivestartdatetime) AS effective_start_date_time, 
          `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(effectiveenddatetime) AS effective_end_date_time, 
          `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(lastupdatedtime) AS last_updated_time, 
          recsequencenum AS rec_sequence_num
          FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_partner_ship_policy_ldg
        QUALIFY (ROW_NUMBER() OVER (PARTITION BY partner_relationship_num ORDER BY rec_sequence_num DESC)) = 1) AS LDG 
        ON LOWER(dim.partner_relationship_num) = LOWER(LDG.partner_relationship_num) 
        AND CAST(dim.eff_end_tmstp AS DATETIME) = CAST('0000-01-01 00:00:00' AS DATETIME)
        WHERE CAST(LDG.effective_end_date_time AS TIMESTAMP) >= dim.eff_begin_tmstp) AS SOURCE
WHERE LOWER(SOURCE.partner_relationship_num) = LOWER(target.partner_relationship_num) 
AND SOURCE.eff_begin_tmstp = target.eff_begin_tmstp;

IF _ERROR_CODE <> 0 THEN
    SELECT ERROR('RC = 3') AS `A12180`;
END IF;

COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
ROLLBACK TRANSACTION;
RAISE USING MESSAGE = @@error.message;


IF _ERROR_CODE <> 0 THEN
    SELECT ERROR('RC = 4') AS `A12180`;
END IF;

END;

BEGIN

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_partner_ship_policy_dim_vtw
(
partner_relationship_num
, partner_relationship_type_code
, vendor_num
, is_expedited_shipping_allowed_flag
, do_not_ship_to_states
, eff_begin_tmstp
,eff_begin_tmstp_tz
, eff_end_tmstp
,eff_end_tmstp_tz
)

with SRC_1 as (
    SELECT partner_relationship_num,partner_relationship_type_code,vendor_num,is_expedited_shipping_allowed_flag,do_not_ship_to_states_varchar,
        eff_begin_tmstp
        eff_end_tmstp,eff_period
    FROM (
        -- Inner normalize
        SELECT partner_relationship_num,partner_relationship_type_code,vendor_num,is_expedited_shipping_allowed_flag,do_not_ship_to_states_varchar,eff_period,
            MIN(eff_begin_tmstp) AS eff_begin_tmstp,
            MAX(eff_end_tmstp) AS eff_end_tmstp
        FROM (
            SELECT *,
                SUM(discontinuity_flag) OVER (
                    PARTITION BY partner_relationship_num,partner_relationship_type_code,vendor_num,is_expedited_shipping_allowed_flag,do_not_ship_to_states_varchar
                    ORDER BY eff_begin_tmstp 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS range_group
            FROM (
                SELECT *,
                    CASE 
                        WHEN LAG(eff_end_tmstp) OVER (
                            PARTITION BY partner_relationship_num,partner_relationship_type_code,vendor_num,is_expedited_shipping_allowed_flag,do_not_ship_to_states_varchar
                            ORDER BY eff_begin_tmstp
                        ) >= DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                        THEN 0
                        ELSE 1
                    END AS discontinuity_flag
                FROM
    (
        
            SELECT DISTINCT --NORMALIZE 
			SRC2.partner_relationship_num, SRC2.partner_relationship_type_code, SRC2.vendor_num, SRC2.is_expedited_shipping_allowed_flag,
            SRC2.do_not_ship_to_states_varchar,CAST(eff_begin_tmstp AS TIMESTAMP) as eff_begin_tmstp,CAST(eff_end_tmstp AS TIMESTAMP) as eff_end_tmstp,
            RANGE(CAST(eff_begin_tmstp as TIMESTAMP), eff_end_tmstp) AS eff_period
            FROM (
                SELECT SRC1.partner_relationship_num
                , SRC1.partner_relationship_type_code
                , SRC1.vendor_num
                , SRC1.is_expedited_shipping_allowed_flag
                , SRC1.do_not_ship_to_states_varchar
                , SRC1.eff_begin_tmstp
                , MAX(SRC1.eff_begin_tmstp) OVER(PARTITION BY SRC1.partner_relationship_num, SRC1.vendor_num ORDER BY
				  SRC1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS max_rec_eff_end_tmstp
				 
                  , CASE WHEN SRC1.effective_end_date_time IS NOT NULL AND MAX(SRC1.eff_begin_tmstp) OVER(PARTITION BY SRC1.partner_relationship_num, SRC1.vendor_num ORDER BY
				  SRC1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) IS NULL THEN SRC1.effective_end_date_time
						 ELSE MAX(SRC1.eff_begin_tmstp) OVER(PARTITION BY SRC1.partner_relationship_num, SRC1.vendor_num ORDER BY
				  SRC1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) END AS max_eff_end_tmstp

                , COALESCE(CAST(MAX(SRC1.eff_begin_tmstp) OVER(PARTITION BY SRC1.partner_relationship_num, SRC1.vendor_num ORDER BY
				  SRC1.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS TIMESTAMP),CURRENT_TIMESTAMP) AS eff_end_tmstp

                , SRC1.last_updated_time
                FROM (SELECT
                    SRC0.partner_relationship_num
                    , SRC0.partner_relationship_type_code
                    , SRC0.vendor_num
                    , SRC0.is_expedited_shipping_allowed_flag
                    , ARRAY_TO_STRING(ARRAY_AGG(SRC0.do_not_ship_to_states), ',') AS do_not_ship_to_states_varchar
                    , CASE WHEN SRC0.last_updated_time > SRC0.effective_start_date_time THEN SRC0.last_updated_time
                           ELSE SRC0.effective_start_date_time END AS eff_begin_tmstp
                    , SRC0.effective_end_date_time
                    , SRC0.last_updated_time
                    , SRC0.rec_sequence_num
                    , ROW_NUMBER() OVER(PARTITION BY partner_relationship_num, vendor_num, last_updated_time ORDER BY rec_sequence_num DESC) AS row_num
                        FROM (SELECT DISTINCT
                         partnerRelationshipId as partner_relationship_num
                         , partnerRelationshipType AS partner_relationship_type_code
                         , vendorNumber AS vendor_num
                         , isExpeditedShippingAllowed AS is_expedited_shipping_allowed_flag
                         , doNotShipToStates AS do_not_ship_to_states
                         , `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(effectiveStartDateTime) AS effective_start_date_time
                         , `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(effectiveEndDateTime) AS effective_end_date_time
                         , `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(lastUpdatedTime) AS last_updated_time
                         , recSequenceNum AS rec_sequence_num
                         FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_partner_ship_policy_ldg
                     ) SRC0
                     group by 1,2,3,4,6,7,8,9
                     QUALIFY row_num = 1
                ) SRC1
                QUALIFY eff_begin_tmstp < CAST(eff_end_tmstp as STRING)
            ) SRC2
    )
    ) AS ordered_data
            ) AS grouped_data
            GROUP BY 
               partner_relationship_num,partner_relationship_type_code,vendor_num,is_expedited_shipping_allowed_flag,do_not_ship_to_states_varchar,eff_period, range_group
            ORDER BY  
                partner_relationship_num,partner_relationship_type_code,vendor_num,is_expedited_shipping_allowed_flag,do_not_ship_to_states_varchar,
                eff_begin_tmstp
        )
    )

    SELECT partner_relationship_num,partner_relationship_type_code,vendor_num,is_expedited_shipping_allowed_flag,do_not_ship_to_states_varchar
        ,timestamp(current_datetime('PST8PDT')) as eff_begin_tmstp
        ,`{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(timestamp(current_datetime('PST8PDT')) as string)) as eff_begin_tmstp_tz
        ,timestamp(current_datetime('PST8PDT')) as eff_end_tmstp
        ,`{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(timestamp(current_datetime('PST8PDT')) as string)) as eff_end_tmstp_tz
    FROM (
        -- Inner normalize
        SELECT partner_relationship_num,partner_relationship_type_code,vendor_num,is_expedited_shipping_allowed_flag,do_not_ship_to_states_varchar,
            MIN(eff_begin_tmstp_utc) AS eff_begin_tmstp,
            MAX(eff_end_tmstp_utc) AS eff_end_tmstp
        FROM (
            SELECT *,
                SUM(discontinuity_flag) OVER (
                    PARTITION BY partner_relationship_num,partner_relationship_type_code,vendor_num,is_expedited_shipping_allowed_flag,do_not_ship_to_states_varchar
                    ORDER BY eff_begin_tmstp_utc 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS range_group
            FROM (
                SELECT *,
                    CASE 
                        WHEN LAG(eff_end_tmstp_utc) OVER (
                            PARTITION BY partner_relationship_num,partner_relationship_type_code,vendor_num,is_expedited_shipping_allowed_flag,do_not_ship_to_states_varchar
                            ORDER BY eff_begin_tmstp_utc
                        ) >= DATE_SUB(eff_begin_tmstp_utc, INTERVAL 1 DAY) 
                        THEN 0
                        ELSE 1
                    END AS discontinuity_flag
                FROM
    (
    SELECT DISTINCT
    SRC.partner_relationship_num
    , SRC.partner_relationship_type_code
    , SRC.vendor_num
    , SRC.is_expedited_shipping_allowed_flag
    , SRC.do_not_ship_to_states_varchar
    ,COALESCE( RANGE_INTERSECT(SRC.eff_period, RANGE(TGT.eff_begin_tmstp_utc,TGT.eff_end_tmstp_utc)), SRC.eff_period ) AS eff_period,TGT.eff_begin_tmstp_utc,TGT.eff_end_tmstp_utc
        FROM (
            select * from SRC_1 
        ) SRC
    LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_partner_ship_policy_dim TGT
    ON SRC.partner_relationship_num = TGT.partner_relationship_num
    AND RANGE_OVERLAPS (RANGE(tgt.eff_begin_tmstp_utc, tgt.eff_end_tmstp_utc),(RANGE(tgt.eff_begin_tmstp_utc, tgt.eff_end_tmstp_utc)))
    WHERE ( TGT.partner_relationship_num IS NULL OR
        ( (SRC.partner_relationship_type_code <> TGT.partner_relationship_type_code OR (SRC.partner_relationship_type_code IS NOT NULL AND TGT.partner_relationship_type_code IS NULL) OR (SRC.partner_relationship_type_code IS NULL AND TGT.partner_relationship_type_code IS NOT NULL))
        OR (SRC.vendor_num <> TGT.vendor_num OR (SRC.vendor_num IS NOT NULL AND TGT.vendor_num IS NULL) OR (SRC.vendor_num IS NULL AND TGT.vendor_num IS NOT NULL))
        OR (SRC.is_expedited_shipping_allowed_flag <> TGT.is_expedited_shipping_allowed_flag OR (SRC.is_expedited_shipping_allowed_flag IS NOT NULL AND TGT.is_expedited_shipping_allowed_flag IS NULL) OR (SRC.is_expedited_shipping_allowed_flag IS NULL AND TGT.is_expedited_shipping_allowed_flag IS NOT NULL))
        OR (SRC.do_not_ship_to_states_varchar <> cast(TGT.do_not_ship_to_states as STRING) OR (SRC.do_not_ship_to_states_varchar IS NOT NULL AND cast(TGT.do_not_ship_to_states as STRING) IS NULL) OR (SRC.do_not_ship_to_states_varchar IS NULL AND cast(TGT.do_not_ship_to_states as STRING) IS NOT NULL))
        )
))) AS ordered_data
            ) AS grouped_data
            GROUP BY 
               partner_relationship_num,partner_relationship_type_code,vendor_num,is_expedited_shipping_allowed_flag,do_not_ship_to_states_varchar, range_group
            ORDER BY  
                partner_relationship_num,partner_relationship_type_code,vendor_num,is_expedited_shipping_allowed_flag,do_not_ship_to_states_varchar,
                eff_begin_tmstp
        );

END;

BEGIN

DELETE FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_partner_ship_policy_dim AS TGT
WHERE EXISTS (SELECT 1
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_partner_ship_policy_dim_vtw  AS SRC
 WHERE LOWER(SRC.partner_relationship_num) = LOWER(tgt.partner_relationship_num)
  AND SRC.eff_begin_tmstp <= TGT.eff_begin_tmstp
    AND SRC.eff_end_tmstp >= TGT.eff_end_tmstp);

UPDATE   `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_partner_ship_policy_dim AS TGT
SET TGT.eff_end_tmstp = SRC.eff_begin_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_partner_ship_policy_dim_vtw  AS SRC
WHERE LOWER(SRC.partner_relationship_num) = LOWER(tgt.partner_relationship_num)
    AND SRC.eff_begin_tmstp >= TGT.eff_begin_tmstp
    AND SRC.eff_begin_tmstp < TGT.eff_end_tmstp 
    AND SRC.eff_end_tmstp > TGT.eff_end_tmstp ;


UPDATE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_partner_ship_policy_dim AS TGT
SET TGT.eff_begin_tmstp = SRC.eff_end_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_partner_ship_policy_dim_vtw  AS SRC
WHERE LOWER(SRC.partner_relationship_num) = LOWER(tgt.partner_relationship_num)
    AND SRC.eff_end_tmstp > TGT.eff_begin_tmstp
    AND SRC.eff_end_tmstp <= TGT.eff_end_tmstp;

END;



BEGIN

BEGIN TRANSACTION; 

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_partner_ship_policy_dim (partner_relationship_num, partner_relationship_type_code,
 vendor_num, is_expedited_shipping_allowed_flag, do_not_ship_to_states, eff_begin_tmstp, eff_begin_tmstp_tz, eff_end_tmstp, eff_end_tmstp_tz, dw_batch_id,
 dw_batch_date, dw_sys_load_date, dw_sys_load_tmstp)
(SELECT partner_relationship_num,
  partner_relationship_type_code,
  vendor_num,
  is_expedited_shipping_allowed_flag,
  do_not_ship_to_states,
  eff_begin_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST( eff_begin_tmstp AS STRING)) AS eff_begin_tmstp_tz,
  eff_end_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST( eff_end_tmstp AS STRING)) as eff_end_tmstp_tz,
  CAST(CASE
    WHEN RPAD(FORMAT_TIMESTAMP('%Y%m%d%I%M%S', CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)), 14, ' ') = ''
    THEN '0'
    ELSE RPAD(FORMAT_TIMESTAMP('%Y%m%d%I%M%S', CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)), 14, ' ')
    END AS BIGINT) AS dw_batch_id,
  CURRENT_DATE AS dw_batch_date,
  CURRENT_DATE AS dw_sys_load_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_partner_ship_policy_dim_vtw);


END;