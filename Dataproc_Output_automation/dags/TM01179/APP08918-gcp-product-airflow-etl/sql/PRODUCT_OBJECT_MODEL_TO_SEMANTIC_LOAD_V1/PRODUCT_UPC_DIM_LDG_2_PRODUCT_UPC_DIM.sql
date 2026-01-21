
--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=stg-dim;AppSubArea=NAP_PRODUCT;' UPDATE FOR SESSION;


--NONSEQUENCED VALIDTIME --Purge work table for staging temporal rows
TRUNCATE TABLE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_upc_dim_vtw;
--.IF ERRORCODE <> 0 THEN .QUIT 1

INSERT INTO `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_upc_dim_vtw
(
    upc_num,
    channel_country,
    upc_desc,
    rms_sku_num,
    epm_sku_num,
    prmy_upc_ind,
    upc_type_code,
    eff_begin_tmstp,
    eff_end_tmstp,
    eff_begin_tmstp_tz,
    eff_end_tmstp_tz
)

WITH SRC1 AS (
    -- Inner normalize
    SELECT 
        upc_num, 
        channel_country, 
        upc_desc, 
        rms_sku_num, 
        epm_sku_num, 
        prmy_upc_ind, 
        upc_type_code, 
        eff_begin_tmstp, 
        eff_end_tmstp
    FROM (
        SELECT 
            upc_num, 
            channel_country, 
            upc_desc, 
            rms_sku_num, 
            epm_sku_num, 
            prmy_upc_ind, 
            upc_type_code,
            MIN(eff_begin_tmstp) AS eff_begin_tmstp, 
            MAX(eff_end_tmstp) AS eff_end_tmstp
        FROM (
            SELECT *,
                SUM(discontinuity_flag) OVER (
                    PARTITION BY upc_num, channel_country, upc_desc, rms_sku_num, epm_sku_num, prmy_upc_ind, upc_type_code 
                    ORDER BY eff_begin_tmstp 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS range_group
            FROM (
                SELECT *,
                    CASE 
                        WHEN LAG(eff_end_tmstp) OVER (
                            PARTITION BY upc_num, channel_country, upc_desc, rms_sku_num, epm_sku_num, prmy_upc_ind, upc_type_code 
                            ORDER BY eff_begin_tmstp
                        ) >= DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                        THEN 0
                        ELSE 1
                    END AS discontinuity_flag
                FROM (
                    SELECT 
                        SRC_2.*,
                        COALESCE(MAX(eff_begin_tmstp) OVER(PARTITION BY upc_num, channel_country ORDER BY eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING),
                        TIMESTAMP '9999-12-31 23:59:59.999999+00:00') AS eff_end_tmstp
                    FROM (
                        SELECT 
                            vendorupcs_upcs_code AS upc_num,
                            marketcode AS channel_country,
                            vendorupcs_upcs_description AS upc_desc,
                            legacyrmsskuid AS rms_sku_num,
                            id AS epm_sku_num,
                            (CASE WHEN LOWER(vendorupcs_upcs_isprimaryupc) = LOWER('true') THEN 'Y' ELSE 'N' END) AS prmy_upc_ind,
                            (CASE WHEN LOWER(vendorupcs_upcs_upctype) <> LOWER('null') THEN vendorupcs_upcs_upctype ELSE 'No UPC Type' END) AS upc_type_code,
                            TIMESTAMP(`{{params.dataplex_project_id}}.JWN_UDF.ISO8601_TMSTP`(COLLATE(sourcepublishtimestamp, ''))) AS eff_begin_tmstp
                        FROM 
                            `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_upc_dim_ldg
                    ) SRC_2
                    QUALIFY eff_begin_tmstp < eff_end_tmstp
                ) AS SRC_1)
            ) AS ordered_data
            GROUP BY 
                upc_num, 
                channel_country, 
                upc_desc, 
                rms_sku_num, 
                epm_sku_num, 
                prmy_upc_ind, 
                upc_type_code, 
                range_group
        ) AS grouped_data
        ORDER BY 
            upc_num, 
            channel_country, 
            upc_desc, 
            rms_sku_num, 
            epm_sku_num, 
            prmy_upc_ind, 
            upc_type_code, 
            eff_begin_tmstp 
    ) -- CTE closing


select 
upc_num,
    channel_country,
    upc_desc,
    rms_sku_num,
    epm_sku_num,
    prmy_upc_ind,
    upc_type_code,
    eff_begin_tmstp,
    eff_end_tmstp,
    `{{params.dataplex_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(eff_begin_tmstp as string)) as eff_begin_tmstp_tz,
    `{{params.dataplex_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(eff_end_tmstp as string)) as eff_end_tmstp_tz
    from (
      SELECT
    upc_num,
    channel_country,
    upc_desc,
    rms_sku_num,
    epm_sku_num,
    prmy_upc_ind,
    upc_type_code,
    MIN(RANGE_START(eff_period)) AS eff_begin_tmstp,
    MAX(RANGE_END(eff_period)) AS eff_end_tmstp
FROM (
    SELECT 
        upc_num,
        channel_country,
        upc_desc,
        rms_sku_num,
        epm_sku_num,
        prmy_upc_ind,
        upc_type_code,
        RANGE(eff_begin_tmstp, eff_end_tmstp) AS eff_period,
        SUM(discontinuity_flag) OVER (
            PARTITION BY upc_num, channel_country, upc_desc, rms_sku_num, epm_sku_num, prmy_upc_ind, upc_type_code 
            ORDER BY eff_begin_tmstp 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS range_group
    FROM (
        SELECT DISTINCT
            SRC.upc_num,
            SRC.channel_country,
            SRC.upc_desc,
            SRC.rms_sku_num,
            SRC.epm_sku_num,
            SRC.prmy_upc_ind,
            SRC.upc_type_code,
            SRC.eff_begin_tmstp,
            SRC.eff_end_tmstp,
            CASE 
                WHEN LAG(TGT.eff_end_tmstp_utc) OVER (
                    PARTITION BY SRC.upc_num, SRC.channel_country, SRC.upc_desc, SRC.rms_sku_num, SRC.epm_sku_num, SRC.prmy_upc_ind, SRC.upc_type_code 
                    ORDER BY SRC.eff_begin_tmstp
                ) >= DATE_SUB(SRC.eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag,
            COALESCE(SAFE.RANGE_INTERSECT(
                SRC.eff_period,
                RANGE(TGT.eff_begin_tmstp_utc, TGT.eff_end_tmstp_utc)
            ), SRC.eff_period) AS eff_period
        FROM (
            SELECT
                upc_num,
                channel_country,
                upc_desc,
                rms_sku_num,
                epm_sku_num,
                prmy_upc_ind,
                upc_type_code,
                eff_begin_tmstp,
                eff_end_tmstp,RANGE(eff_begin_tmstp, eff_end_tmstp) AS eff_period
            FROM SRC1
        ) SRC
        LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_upc_dim_hist TGT ON 
            LOWER(SRC.upc_num) = LOWER(TGT.upc_num) AND
            LOWER(SRC.channel_country) = LOWER(TGT.channel_country) AND 
            RANGE_OVERLAPS(
                RANGE(SRC.eff_begin_tmstp, SRC.eff_end_tmstp),
                RANGE(TGT.eff_begin_tmstp_utc, TGT.eff_end_tmstp_utc)
            )
        WHERE 
            TGT.upc_num IS NULL OR 
            (
                (LOWER(SRC.upc_desc) <> LOWER(TGT.upc_desc) OR (SRC.upc_desc IS NOT NULL AND TGT.upc_desc IS NULL) OR (SRC.upc_desc IS NULL AND TGT.upc_desc IS NOT NULL))
                OR (LOWER(SRC.rms_sku_num) <> LOWER(TGT.rms_sku_num) OR (SRC.rms_sku_num IS NOT NULL AND TGT.rms_sku_num IS NULL) OR (SRC.rms_sku_num IS NULL AND TGT.rms_sku_num IS NOT NULL))
                OR (SRC.epm_sku_num <> TGT.epm_sku_num OR (SRC.epm_sku_num IS NOT NULL AND TGT.epm_sku_num IS NULL) OR (SRC.epm_sku_num IS NULL AND TGT.epm_sku_num IS NOT NULL))
                OR (LOWER(SRC.prmy_upc_ind) <> LOWER(TGT.prmy_upc_ind) OR (SRC.prmy_upc_ind IS NOT NULL AND TGT.prmy_upc_ind IS NULL) OR (SRC.prmy_upc_ind IS NULL AND TGT.prmy_upc_ind IS NOT NULL))
                OR (LOWER(SRC.upc_type_code) <> LOWER(TGT.upc_type_code) OR (SRC.upc_type_code IS NOT NULL AND TGT.upc_type_code IS NULL) OR (SRC.upc_type_code IS NULL AND TGT.upc_type_code IS NOT NULL))
            )
    ) AS ordered_data
) AS grouped_data
GROUP BY 
    upc_num,
    channel_country,
    upc_desc,
    rms_sku_num,
    epm_sku_num,
    prmy_upc_ind,
    upc_type_code,
    range_group
ORDER BY  
    upc_num,
    channel_country,
    upc_desc,
    rms_sku_num,
    epm_sku_num,
    prmy_upc_ind,
    upc_type_code);

--.IF ERRORCODE <> 0 THEN .QUIT 2


--Explicit transaction on Teradata ensures that we do not corrupt target table if failure in middle of updating target table
BEGIN TRANSACTION;
--.IF ERRORCODE <> 0 THEN .QUIT 3

--SEQUENCED VALIDTIME
DELETE FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_upc_dim AS TGT
WHERE EXISTS (SELECT 1
 FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_upc_dim_vtw AS SRC
 WHERE LOWER(SRC.upc_num) = LOWER(TGT.upc_num)
  AND LOWER(SRC.channel_country) = LOWER(TGT.channel_country)
  AND SRC.eff_begin_tmstp <= TGT.eff_begin_tmstp
    AND SRC.eff_end_tmstp >= TGT.eff_end_tmstp);
--.IF ERRORCODE <> 0 THEN .QUIT 4


UPDATE  `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_upc_dim AS TGT
SET TGT.eff_end_tmstp = SRC.eff_begin_tmstp
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_upc_dim_vtw AS SRC
WHERE LOWER(SRC.upc_num) = LOWER(TGT.upc_num)
  AND LOWER(SRC.channel_country) = LOWER(TGT.channel_country)
    AND SRC.eff_begin_tmstp > TGT.eff_begin_tmstp
    AND SRC.eff_begin_tmstp <= TGT.eff_end_tmstp
    AND SRC.eff_end_tmstp >= TGT.eff_end_tmstp;


UPDATE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_upc_dim AS TGT
SET TGT.eff_begin_tmstp = SRC.eff_end_tmstp
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_upc_dim_vtw AS SRC
WHERE LOWER(SRC.upc_num) = LOWER(TGT.upc_num)
  AND LOWER(SRC.channel_country) = LOWER(TGT.channel_country)
    AND SRC.eff_end_tmstp >= TGT.eff_begin_tmstp
    AND SRC.eff_begin_tmstp < TGT.eff_begin_tmstp
    AND SRC.eff_end_tmstp <= TGT.eff_end_tmstp;


INSERT INTO `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_upc_dim(upc_num,channel_country,upc_desc,rms_sku_num,epm_sku_num,prmy_upc_ind,upc_type_code,eff_begin_tmstp_tz,eff_end_tmstp_tz,dw_batch_id,dw_batch_date,dw_sys_load_tmstp,eff_begin_tmstp,eff_end_tmstp)
WITH tbl AS 
(SELECT tgt.*, 
src.eff_begin_tmstp AS src_eff_begin_tmstp, 
src.eff_end_tmstp AS src_eff_end_tmstp, 
tgt.eff_begin_tmstp AS tgt_eff_begin_tmstp, 
tgt.eff_end_tmstp AS tgt_eff_end_tmstp 
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_upc_dim_vtw AS src 
INNER JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_upc_dim AS tgt ON src.upc_num = tgt.upc_num 
AND tgt.eff_begin_tmstp < src.eff_begin_tmstp 
AND tgt.eff_end_tmstp > src.eff_end_tmstp)

SELECT * except(eff_begin_tmstp, eff_end_tmstp, src_eff_begin_tmstp, src_eff_end_tmstp, tgt_eff_begin_tmstp,tgt_eff_end_tmstp), tgt_eff_begin_tmstp, src_eff_begin_tmstp FROM tbl
UNION ALL
SELECT * except(eff_begin_tmstp, eff_end_tmstp, src_eff_begin_tmstp, src_eff_end_tmstp, tgt_eff_begin_tmstp,tgt_eff_end_tmstp), src_eff_end_tmstp, tgt_eff_end_tmstp FROM tbl;

DELETE FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_upc_dim AS tgt 
WHERE EXISTS (SELECT 1 FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_upc_dim_vtw AS src 
WHERE src.upc_num = tgt.upc_num AND tgt.eff_begin_tmstp < src.eff_begin_tmstp 
and tgt.eff_end_tmstp > src.eff_end_tmstp);



INSERT INTO `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_upc_dim
( upc_num
, channel_country
, upc_desc
, rms_sku_num
, epm_sku_num
, prmy_upc_ind
, upc_type_code
, eff_begin_tmstp
, eff_end_tmstp
, dw_batch_id
, dw_batch_date
, dw_sys_load_tmstp
, eff_begin_tmstp_tz
, eff_end_tmstp_tz
)
SELECT
  upc_num
, channel_country
, upc_desc
, rms_sku_num
, epm_sku_num
, prmy_upc_ind
, upc_type_code
, eff_begin_tmstp
, eff_end_tmstp
,(SELECT BATCH_ID FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(Subject_Area_Nm) =LOWER('NAP_PRODUCT')) AS dw_batch_id
,(SELECT CURR_BATCH_DATE FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(Subject_Area_Nm) =LOWER('NAP_PRODUCT')) AS dw_batch_date
, CURRENT_DATETIME('PST8PDT') AS dw_sys_load_tmstp
, eff_begin_tmstp_tz
, eff_end_tmstp_tz
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_upc_dim_vtw;

--.IF ERRORCODE <> 0 THEN .QUIT 5


COMMIT TRANSACTION;
--.IF ERRORCODE <> 0 THEN .QUIT 6

-- COLLECT STATISTICS COLUMN(upc_num), COLUMN(channel_country), COLUMN(rms_sku_num), COLUMN(eff_end_tmstp)
-- ON {{params.dbenv}}_NAP_DIM.PRODUCT_UPC_DIM;


