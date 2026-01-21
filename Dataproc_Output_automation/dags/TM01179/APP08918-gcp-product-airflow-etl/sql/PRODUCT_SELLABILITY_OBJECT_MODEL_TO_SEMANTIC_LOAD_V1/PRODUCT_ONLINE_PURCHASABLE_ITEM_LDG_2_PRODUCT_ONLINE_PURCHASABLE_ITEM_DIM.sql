
--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=stg-dim;AppSubArea=NAP_PRODUCT;' UPDATE FOR SESSION;



TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_online_purchasable_item_dim_vtw;
--.IF ERRORCODE <> 0 THEN .QUIT 1



INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_online_purchasable_item_dim_vtw
( rms_sku_num
, channel_country
, channel_brand
, selling_channel
, is_online_purchasable
, eff_begin_tmstp
,eff_begin_tmstp_tz
, eff_end_tmstp
,eff_end_tmstp_tz
 
)

--with cte 
WITH SRC1 AS (
    SELECT 
        rms_sku_num,
        channel_country,
        channel_brand,
        selling_channel,
        is_online_purchasable,
        eff_begin_tmstp,
        eff_end_tmstp
    FROM (
        -- Inner normalize
        SELECT 
            rms_sku_num,
            channel_country,
            channel_brand,
            selling_channel,
            is_online_purchasable,
            MIN(eff_begin_tmstp) AS eff_begin_tmstp,
            MAX(eff_end_tmstp) AS eff_end_tmstp
        FROM (
            SELECT *,
                SUM(discontinuity_flag) OVER (
                    PARTITION BY 
                        rms_sku_num, 
                        channel_country, 
                        channel_brand, 
                        selling_channel, 
                        is_online_purchasable
                    ORDER BY eff_begin_tmstp 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS range_group
            FROM (
                SELECT *,
                    CASE 
                        WHEN LAG(eff_end_tmstp) OVER (
                            PARTITION BY 
                                rms_sku_num, 
                                channel_country, 
                                channel_brand, 
                                selling_channel, 
                                is_online_purchasable 
                            ORDER BY eff_begin_tmstp
                        ) >= DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                        THEN 0
                        ELSE 1
                    END AS discontinuity_flag
                FROM (
                    SELECT DISTINCT 
                        rms_sku_num,
                        channel_country,
                        channel_brand,
                        selling_channel,
                        is_online_purchasable,
                        RANGE(CAST(eff_begin_tmstp AS TIMESTAMP), eff_end_tmstp) AS eff_period,eff_begin_tmstp,eff_end_tmstp
                    FROM (
                        SELECT 
                            SRC_2.*,
                            --jwn_udf.udf_time_zone(jwn_udf.iso8601_tmstp(eff_begin_tmstp)) AS eff_begin_tmstp_tz,
                            CAST(COALESCE(
                                CAST(MAX(eff_begin_tmstp) OVER (
                                    PARTITION BY 
                                        rms_sku_num, 
                                        channel_country, 
                                        channel_brand, 
                                        selling_channel, 
                                        is_online_purchasable
                                    ORDER BY eff_begin_tmstp 
                                    ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING
                                ) AS TIMESTAMP),
                                TIMESTAMP '9999-12-31 23:59:59.999999+00:00'
                            ) AS TIMESTAMP )AS eff_end_tmstp
                            -- jwn_udf.udf_time_zone(
                            --     CAST(
                            --         COALESCE(
                            --             CAST(MAX(eff_begin_tmstp) OVER (
                            --                 PARTITION BY 
                            --                     rms_sku_num, 
                            --                     channel_country, 
                            --                     channel_brand, 
                            --                     selling_channel, 
                            --                     is_online_purchasable
                            --                 ORDER BY eff_begin_tmstp 
                            --                 ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING
                            --             ) AS TIMESTAMP), 
                            --             TIMESTAMP '9999-12-31 23:59:59.999999+00:00'
                            --         ) AS STRING
                            --     )
                            -- ) AS eff_end_tmstp_tz
                        FROM (
                            SELECT
                                rmssku AS rms_sku_num,
                                channelcountry AS channel_country,
                                channelbrand AS channel_brand,
                                sellingchannel AS selling_channel,
                                (CASE WHEN isonlinepurchasable = 'true' THEN 'Y' ELSE 'N' END) AS is_online_purchasable,
                                CAST(`{{params.gcp_project_id}}.JWN_UDF.ISO8601_TMSTP`(lastupdatedat) AS TIMESTAMP) AS eff_begin_tmstp
                            FROM 
                                `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_online_purchasable_item_ldg
                        ) SRC_2
                        QUALIFY 
                            CAST(eff_begin_tmstp AS TIMESTAMP) < eff_end_tmstp
                    ) SRC_1
                )
            ) AS ordered_data
        ) AS grouped_data
        GROUP BY 
            rms_sku_num,
            channel_country,
            channel_brand,
            selling_channel,
            is_online_purchasable,
            range_group
        ORDER BY  
            rms_sku_num,
            channel_country,
            channel_brand,
            selling_channel,
            is_online_purchasable,
            eff_begin_tmstp
    )
)


SELECT
    NRML.rms_sku_num,
    NRML.channel_country,
    NRML.channel_brand,
    NRML.selling_channel,
    NRML.is_online_purchasable,
    eff_begin_tmstp_utc ,
    `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(eff_begin as string)) as eff_begin_tmstp_tz,
    eff_end,
    `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(eff_end as string)) as eff_end_tmstp_tz
FROM (
    -- Second normalize 
    SELECT
        rms_sku_num,channel_country,channel_brand,selling_channel,is_online_purchasable,eff_begin_tmstp_utc,
        MIN(RANGE_START(eff_period)) AS eff_begin,
        MAX(RANGE_END(eff_period)) AS eff_end
    FROM (
        -- Inner normalize
        SELECT 
            rms_sku_num,channel_country,channel_brand,selling_channel,is_online_purchasable,eff_period,range_group,eff_begin_tmstp_utc
        FROM (
            SELECT *,
                SUM(discontinuity_flag) OVER (
                    PARTITION BY rms_sku_num,channel_country,channel_brand,selling_channel,is_online_purchasable
                    ORDER BY eff_begin_tmstp_utc 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS range_group
            FROM (
                -- Non-sequenced valid time
                SELECT
                    rms_sku_num,channel_country,channel_brand,selling_channel,is_online_purchasable,
                    eff_begin_tmstp_utc,
                    eff_end_tmstp_utc,
                    eff_period,
                    CASE 
                        WHEN LAG(eff_end_tmstp_utc) OVER (
                            PARTITION BY rms_sku_num,channel_country,channel_brand,selling_channel,is_online_purchasable
                            ORDER BY eff_begin_tmstp_utc
                        ) >= DATE_SUB(eff_begin_tmstp_utc, INTERVAL 1 DAY) 
                        THEN 0
                        ELSE 1
                    END AS discontinuity_flag
                FROM (
                    SELECT DISTINCT
                        SRC.rms_sku_num,
                        SRC.channel_country,
                        SRC.channel_brand,
                        SRC.selling_channel,
                        SRC.is_online_purchasable,
                        COALESCE(
                            RANGE_INTERSECT(SRC.eff_period, RANGE(TGT.eff_begin_tmstp_utc,TGT.eff_end_tmstp_utc)),
                            SRC.eff_period
                        ) AS eff_period,
                        tgt.eff_end_tmstp_utc,
                        tgt.eff_begin_tmstp_utc
                    FROM (
                        -- From CTE 
                        SELECT *,RANGE( eff_begin_tmstp, eff_end_tmstp ) AS eff_period FROM SRC1
                    ) SRC
                    LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_online_purchasable_item_dim TGT ON 
                        SRC.rms_sku_num = TGT.rms_sku_num AND
                        SRC.channel_country = TGT.channel_country AND
                        SRC.channel_brand = TGT.channel_brand AND
                        SRC.selling_channel = TGT.selling_channel AND
                        RANGE_OVERLAPS(SRC.eff_period, RANGE(TGT.eff_begin_tmstp_utc,TGT.eff_end_tmstp_utc))
                    WHERE (
                        TGT.rms_sku_num IS NULL OR
                        (SRC.is_online_purchasable <> TGT.is_online_purchasable OR
                         (SRC.is_online_purchasable IS NOT NULL AND TGT.is_online_purchasable IS NULL) OR
                         (SRC.is_online_purchasable IS NULL AND TGT.is_online_purchasable IS NOT NULL))
                        )
                    )
                ) AS ordered_data
            ) AS grouped_data)
            GROUP BY 
                rms_sku_num,channel_country,channel_brand,selling_channel,is_online_purchasable,range_group,eff_begin_tmstp_utc
            ORDER BY  
                rms_sku_num,channel_country,channel_brand,selling_channel,is_online_purchasable
        )AS NRML;
		

--.IF ERRORCODE <> 0 THEN .QUIT 2


--Explicit transaction on Teradata ensures that we do not corrupt target table if failure in middle of updating target table
Begin Transaction;
--.IF ERRORCODE <> 0 THEN .QUIT 3

--SEQUENCED VALIDTIME
DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_online_purchasable_item_dim AS TGT
WHERE EXISTS (
  SELECT 1
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_online_purchasable_item_dim_vtw AS SRC
 WHERE LOWER(src.rms_sku_num) = LOWER(tgt.rms_sku_num)
  AND LOWER(src.channel_country) = LOWER(tgt.channel_country)
  AND LOWER(src.channel_brand) = LOWER(tgt.channel_brand)
  AND LOWER(src.selling_channel) = LOWER(tgt.selling_channel)
  AND SRC.eff_begin_tmstp <= TGT.eff_begin_tmstp
  AND SRC.eff_end_tmstp >= TGT.eff_end_tmstp
);

UPDATE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_online_purchasable_item_dim AS TGT
SET TGT.eff_end_tmstp = SRC.eff_begin_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_online_purchasable_item_dim_vtw AS SRC
 WHERE LOWER(src.rms_sku_num) = LOWER(tgt.rms_sku_num)
  AND LOWER(src.channel_country) = LOWER(tgt.channel_country)
  AND LOWER(src.channel_brand) = LOWER(tgt.channel_brand)
  AND LOWER(src.selling_channel) = LOWER(tgt.selling_channel)
  AND SRC.eff_begin_tmstp >= TGT.eff_begin_tmstp
  AND SRC.eff_begin_tmstp < TGT.eff_end_tmstp 
  AND SRC.eff_end_tmstp > TGT.eff_end_tmstp ;


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_online_purchasable_item_dim AS TGT
SET TGT.eff_begin_tmstp = SRC.eff_end_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_online_purchasable_item_dim_vtw AS SRC
 WHERE LOWER(rms_sku_num) = LOWER(tgt.rms_sku_num)
  AND LOWER(src.channel_country) = LOWER(tgt.channel_country)
  AND LOWER(src.channel_brand) = LOWER(tgt.channel_brand)
  AND LOWER(src.selling_channel) = LOWER(tgt.selling_channel)
  AND SRC.eff_end_tmstp > TGT.eff_begin_tmstp
  AND SRC.eff_end_tmstp <= TGT.eff_end_tmstp;
	

--.IF ERRORCODE <> 0 THEN .QUIT 4

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_online_purchasable_item_dim
( rms_sku_num
, channel_country
, channel_brand
, selling_channel
, is_online_purchasable
, eff_begin_tmstp
, eff_end_tmstp
, dw_batch_id
, dw_batch_date
, dw_sys_load_tmstp
)
SELECT
  rms_sku_num
, channel_country
, channel_brand
, selling_channel
, is_online_purchasable
, eff_begin_tmstp
, eff_end_tmstp
,(SELECT BATCH_ID FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE Subject_Area_Nm ='NAP_PRODUCT_SELLABILITY') AS dw_batch_id
,(SELECT CURR_BATCH_DATE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE Subject_Area_Nm ='NAP_PRODUCT_SELLABILITY') AS dw_batch_date
, current_datetime('PST8PDT') AS dw_sys_load_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_online_purchasable_item_dim_vtw
;
--.IF ERRORCODE <> 0 THEN .QUIT 5

COMMIT TRANSACTION;
--.IF ERRORCODE <> 0 THEN .QUIT 6

-- COLLECT STATISTICS COLUMN(rms_sku_num), COLUMN(channel_country), COLUMN(channel_brand), COLUMN(selling_channel), COLUMN(eff_end_tmstp)
-- ON PRD_NAP_DIM.PRODUCT_ONLINE_PURCHASABLE_ITEM_DIM;


