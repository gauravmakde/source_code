
--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=stg-dim;AppSubArea=ITEM_SELLING_RIGHTS;' UPDATE FOR SESSION;
--Purge the XREF work table for staging temporal rows
-- NONSEQUENCED VALIDTIME

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_xref_vtw;

--.IF ERRORCODE <> 0 THEN .QUIT 1
--Purge work table for keys

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_xref_keys_stg;

--.IF ERRORCODE <> 0 THEN .QUIT 2
--Purge work table for future clean up from XREF

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_sell_rights_xref_del_stg;

--.IF ERRORCODE <> 0 THEN .QUIT 3
--Insert all the records that changed

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_xref_keys_stg (rms_sku_num, channel_country, eff_begin_tmstp,eff_begin_tmstp_tz)
(SELECT rms_sku_num,
  channel_country,
  eff_begin_tmstp,
  eff_begin_tmstp_tz
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_dim
 WHERE (rms_sku_num, channel_country) IN (SELECT (rms_sku_num, channel_country)
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_dim
    WHERE dw_batch_date BETWEEN (SELECT extract_start_dt
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
       WHERE LOWER(interface_code) = LOWER('NAP_SCP_XREF_DLY')) AND (SELECT extract_end_dt
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
       WHERE LOWER(interface_code) = LOWER('NAP_SCP_XREF_DLY'))
    GROUP BY rms_sku_num,
     channel_country)
 GROUP BY rms_sku_num,
  channel_country,
  eff_begin_tmstp,
  eff_begin_tmstp_tz);

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_sell_rights_xref_del_stg (rms_sku_num, channel_country, eff_begin_tmstp,eff_begin_tmstp_tz)
(SELECT xref.rms_sku_num,
  xref.channel_country,
  xref.eff_begin_tmstp,
  xref.eff_begin_tmstp_tz
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref
  INNER JOIN (SELECT opr.rms_sku_num,
    opr.channel_country,
    opr.selling_rights_start_tmstp AS min_begin_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.product_item_selling_rights_opr AS opr
    INNER JOIN (SELECT DISTINCT rmsskunum AS rms_sku_num,
      sellingcountry,
      sellingchannel,
      sellingbrand,
      `{{params.gcp_project_id}}.jwn_udf.ISO8601_TMSTP`(lastupdatedtimestamp) AS src_event_tmstp,
      `{{params.gcp_project_id}}.jwn_udf.ISO8601_TMSTP`(starttime) AS selling_rights_start_tmstp
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_item_selling_rights_ldg
     QUALIFY (ROW_NUMBER() OVER (PARTITION BY rmsskunum, sellingcountry, sellingbrand, sellingchannel ORDER BY
          lastupdatedtimestamp, selling_rights_start_tmstp)) = 1) AS oldest_ldg ON LOWER(oldest_ldg.rms_sku_num) = LOWER(opr
          .rms_sku_num) AND LOWER(oldest_ldg.sellingcountry) = LOWER(opr.channel_country) AND LOWER(oldest_ldg.sellingbrand
         ) = LOWER(opr.channel_brand) AND LOWER(oldest_ldg.sellingchannel) = LOWER(opr.selling_channel) AND CAST(oldest_ldg.selling_rights_start_tmstp AS TIMESTAMP)
      = opr.selling_rights_start_tmstp
   GROUP BY opr.rms_sku_num,
    opr.channel_country,
    opr.channel_brand,
    opr.selling_channel,
    opr.selling_rights_start_tmstp
   QUALIFY (ROW_NUMBER() OVER (PARTITION BY opr.rms_sku_num, opr.channel_country ORDER BY opr.selling_rights_start_tmstp
        )) = 1) AS STG ON LOWER(xref.rms_sku_num) = LOWER(STG.rms_sku_num) AND LOWER(xref.channel_country) = LOWER(STG.channel_country
     )
 WHERE xref.eff_begin_tmstp >= STG.min_begin_tmstp);


BEGIN TRANSACTION;

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS tgt
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_sell_rights_xref_del_stg AS src
    WHERE LOWER(rms_sku_num) = LOWER(tgt.rms_sku_num) AND LOWER(channel_country) = LOWER(tgt.channel_country) AND eff_begin_tmstp = tgt.eff_begin_tmstp);

-- Reset the EFF_END_TMSTP of the deleted records to default date.

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS target 
SET
    eff_end_tmstp = CAST(CAST('9999-12-31 23:59:59.999999+00:' AS DATETIME) AS TIMESTAMP) ,
    eff_end_tmstp_tz = '+00'
    FROM (SELECT xref.rms_sku_num, xref.channel_country, MAX(xref.eff_begin_tmstp) AS max_eff_begin_tmstp, MAX(xref.eff_end_tmstp) AS max_eff_end_tmstp
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref
            INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_sell_rights_xref_del_stg AS pdel ON LOWER(xref.rms_sku_num) = LOWER(pdel.rms_sku_num) AND LOWER(xref.channel_country) = LOWER(pdel.channel_country)
        GROUP BY xref.rms_sku_num, xref.channel_country) AS SOURCE
WHERE LOWER(target.rms_sku_num) = LOWER(SOURCE.rms_sku_num) AND LOWER(target.channel_country) = LOWER(SOURCE.channel_country) AND target.eff_begin_tmstp = SOURCE.max_eff_begin_tmstp AND target.eff_end_tmstp = SOURCE.max_eff_end_tmstp;
--.IF ERRORCODE <> 0 THEN .QUIT 8
-- Load the VTW tables for the incremental changes

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_xref_vtw 
(rms_sku_num, 
channel_country, 
selling_status_code, 
selling_channel_eligibility_list, 
live_date, 
eff_begin_tmstp, 
eff_begin_tmstp_tz,
eff_end_tmstp,
eff_end_tmstp_tz
)

with SRC1 AS 
(select 
rms_sku_num, 
channel_country, 
selling_status_code,
selling_channel_eligibility_list,
live_date,
eff_begin_tmstp,
eff_end_tmstp
 from  (
    --inner normalize
            SELECT rms_sku_num, channel_country, selling_status_code,selling_channel_eligibility_list,live_date, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num, channel_country, selling_status_code,selling_channel_eligibility_list,live_date ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM

(
        SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY rms_sku_num, channel_country, selling_status_code,selling_channel_eligibility_list,live_date ORDER BY eff_begin_tmstp) >= 
                DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
         from
(

                          SELECT
                              eligibility.rms_sku_num,
                              eligibility.channel_country,
                              status.selling_status_code,
                              eligibility.selling_channel_eligibility_list,
                              eligibility.eff_begin_tmstp,
coalesce(min(eligibility.eff_begin_tmstp) OVER (PARTITION BY eligibility.rms_sku_num, eligibility.channel_country ORDER BY eligibility.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING), TIMESTAMP '9999-12-31 23:59:59.999') AS eff_end_tmstp,
                            live_date.live_date
                            FROM
                              (
                                SELECT
                                    keys.rms_sku_num,
                                    keys.channel_country,
                                    keys.eff_begin_tmstp,
                                    -- sell_eligibility.eff_begin_tmstp_tz,
                                    -- sell_eligibility.eff_end_tmstp_tz,
                                    rtrim(substr(string_agg(CASE
                                      WHEN upper(rtrim(sell_eligibility.channel_brand, ' ')) = 'NORDSTROM'
                                       AND upper(rtrim(sell_eligibility.selling_channel, ' ')) = 'ONLINE'
                                       AND upper(rtrim(sell_eligibility.is_sellable_ind, ' ')) = 'Y' THEN 'Web,'
                                      WHEN upper(rtrim(sell_eligibility.channel_brand, ' ')) = 'NORDSTROM'
                                       AND upper(rtrim(sell_eligibility.selling_channel, ' ')) = 'STORE'
                                       AND upper(rtrim(sell_eligibility.is_sellable_ind, ' ')) = 'Y' THEN 'FLS,'
                                      WHEN upper(rtrim(sell_eligibility.channel_brand, ' ')) = 'NORDSTROM_RACK'
                                       AND upper(rtrim(sell_eligibility.selling_channel, ' ')) = 'ONLINE'
                                       AND upper(rtrim(sell_eligibility.is_sellable_ind, ' ')) = 'Y' THEN 'Rack Web,'
                                      WHEN upper(rtrim(sell_eligibility.channel_brand, ' ')) = 'NORDSTROM_RACK'
                                       AND upper(rtrim(sell_eligibility.selling_channel, ' ')) = 'STORE'
                                       AND upper(rtrim(sell_eligibility.is_sellable_ind, ' ')) = 'Y' THEN 'Rack,'
                                      ELSE NULL
                                    END, ' '), 1, 90), ',') AS selling_channel_eligibility_list
                                  FROM
                                    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_xref_keys_stg AS keys
                                    LEFT OUTER JOIN (
                                      SELECT
                                          product_item_selling_rights_dim.rms_sku_num,
                                          product_item_selling_rights_dim.channel_country,
                                          product_item_selling_rights_dim.channel_brand,
                                          product_item_selling_rights_dim.selling_channel,
                                          product_item_selling_rights_dim.selling_rights_start_tmstp,
                                          product_item_selling_rights_dim.is_sellable_ind,
                                          product_item_selling_rights_dim.eff_begin_tmstp,
                                          product_item_selling_rights_dim.eff_begin_tmstp_tz,
                                          product_item_selling_rights_dim.eff_end_tmstp,
                                          product_item_selling_rights_dim.eff_end_tmstp_tz


                                      
                                        FROM
                                          `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_dim
                                    ) AS sell_eligibility ON keys.rms_sku_num = sell_eligibility.rms_sku_num
                                     AND keys.channel_country = sell_eligibility.channel_country
                                    AND RANGE_CONTAINS(range (sell_eligibility.eff_begin_tmstp, sell_eligibility.eff_end_tmstp ) ,keys.eff_begin_tmstp)

                                  GROUP BY 1, 2, 3
                              ) AS eligibility
                              LEFT OUTER JOIN
                              (
                                SELECT
                                    keys.rms_sku_num,
                                    keys.channel_country,
                                    keys.eff_begin_tmstp,
                                    CASE
                                      WHEN sell_status.rms_sku_num IS NULL THEN 'UNBLOCKED'
                                      ELSE sell_status.selling_status_code
                                    END AS selling_status_code
                                  FROM
                                    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_xref_keys_stg AS keys
                                    LEFT OUTER JOIN (
                                      SELECT
                                          sell_status_online.rms_sku_num,
                                          sell_status_online.channel_country,
                                          sell_status_online.selling_status_code,
                                          sell_status_online.eff_begin_tmstp,
                                          coalesce(min(sell_status_online.eff_begin_tmstp) OVER (PARTITION BY sell_status_online.rms_sku_num, sell_status_online.channel_country ORDER BY sell_status_online.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING), TIMESTAMP '9999-12-31 23:59:59.999') AS eff_end_tmstp
                                        FROM
                                          (
                                            SELECT
                                                rights_dim.rms_sku_num,
                                                rights_dim.channel_country,
                                                rights_dim.eff_begin_tmstp,
                                                rights_dim.selling_status_code
                                              FROM
                                                `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_dim AS rights_dim
                                                INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_xref_keys_stg AS recs ON upper(rtrim(rights_dim.selling_channel, ' ')) = 'ONLINE'
                                                 AND rights_dim.rms_sku_num = recs.rms_sku_num
                                                 AND rights_dim.channel_country = recs.channel_country
                                              GROUP BY 1, 2, 3, 4
                                              QUALIFY row_number() OVER (PARTITION BY rights_dim.rms_sku_num, rights_dim.channel_country, rights_dim.eff_begin_tmstp ORDER BY rights_dim.selling_status_code) = 1
                                          ) AS sell_status_online
                                    ) AS sell_status ON keys.rms_sku_num = sell_status.rms_sku_num
                                     AND keys.channel_country = sell_status.channel_country
                          AND RANGE_CONTAINS(range(sell_status.eff_begin_tmstp,sell_status.eff_end_tmstp),keys.eff_begin_tmstp)
                              ) AS status 
                              ON status.rms_sku_num = eligibility.rms_sku_num
                               AND status.channel_country = eligibility.channel_country
                               AND status.eff_begin_tmstp = eligibility.eff_begin_tmstp
                              LEFT OUTER JOIN 
                              (
                                SELECT
                                    derived_date.rms_sku_num,
                                    derived_date.channel_country,
                                    derived_date.eff_begin_tmstp,
                                    derived_date.live_date
                                  FROM
                                    (
                                      SELECT
                                          keys.rms_sku_num,
                                          keys.channel_country,
                                          keys.eff_begin_tmstp,
                                          sell_eligibility.pref_ind,
                                          CASE
                                            WHEN sell_eligibility.pref_ind = 1
                                             OR sell_eligibility.pref_ind = 2 THEN min(sell_eligibility.live_date) OVER (PARTITION BY keys.rms_sku_num, keys.channel_country, keys.eff_begin_tmstp, sell_eligibility.pref_ind ORDER BY sell_eligibility.live_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                                            WHEN sell_eligibility.pref_ind >= 3 THEN max(sell_eligibility.live_date) OVER (PARTITION BY keys.rms_sku_num, keys.channel_country, keys.eff_begin_tmstp, sell_eligibility.pref_ind ORDER BY sell_eligibility.live_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                                            ELSE max(sell_eligibility.live_date) OVER (PARTITION BY keys.rms_sku_num, keys.channel_country, keys.eff_begin_tmstp, sell_eligibility.pref_ind ORDER BY sell_eligibility.live_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                                          END AS live_date
                                        FROM
                                          `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_xref_keys_stg AS keys
                                          LEFT OUTER JOIN (
                                            SELECT
                                                dim.rms_sku_num,
                                                dim.channel_country,
                                                dim.channel_brand,
                                                dim.selling_channel,
                                                CAST(/* expression of unknown or erroneous type */ dim.selling_rights_start_tmstp as DATE) AS live_date,
                                                CASE
                                                  WHEN upper(rtrim(dim.channel_brand, ' ')) = 'NORDSTROM'
                                                   AND upper(rtrim(dim.selling_channel, ' ')) = 'ONLINE'
                                                   AND upper(rtrim(dim.is_sellable_ind, ' ')) = 'Y' THEN 1
                                                  WHEN upper(rtrim(dim.channel_brand, ' ')) = 'NORDSTROM_RACK'
                                                   AND upper(rtrim(dim.selling_channel, ' ')) = 'ONLINE'
                                                   AND upper(rtrim(dim.is_sellable_ind, ' ')) = 'Y' THEN 1
                                                  WHEN upper(rtrim(dim.channel_brand, ' ')) = 'NORDSTROM'
                                                   AND upper(rtrim(dim.selling_channel, ' ')) = 'STORE'
                                                   AND upper(rtrim(dim.is_sellable_ind, ' ')) = 'Y' THEN 2
                                                  WHEN upper(rtrim(dim.channel_brand, ' ')) = 'NORDSTROM_RACK'
                                                   AND upper(rtrim(dim.selling_channel, ' ')) = 'STORE'
                                                   AND upper(rtrim(dim.is_sellable_ind, ' ')) = 'Y' THEN 2
                                                  ELSE 3
                                                END AS pref_ind,
                                                dim.eff_begin_tmstp,
                                                dim.eff_end_tmstp
                                              FROM
                                                `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_dim AS dim
                                                INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_xref_keys_stg AS recs ON dim.rms_sku_num = recs.rms_sku_num
                                                 AND dim.channel_country = recs.channel_country
                                          ) AS sell_eligibility ON keys.rms_sku_num = sell_eligibility.rms_sku_num
                                           AND keys.channel_country = sell_eligibility.channel_country
                                           and RANGE_CONTAINS(range(sell_eligibility.eff_begin_tmstp, sell_eligibility.eff_end_tmstp), keys.eff_begin_tmstp)
                                    ) AS derived_date
                                  QUALIFY row_number() OVER (PARTITION BY derived_date.rms_sku_num, derived_date.channel_country, derived_date.eff_begin_tmstp ORDER BY derived_date.pref_ind) = 1
                              ) AS live_date 
                              ON live_date.rms_sku_num = eligibility.rms_sku_num
                               AND live_date.channel_country = eligibility.channel_country
                               AND live_date.eff_begin_tmstp = eligibility.eff_begin_tmstp
                        )     ) AS ordered_data
) AS grouped_data
GROUP BY rms_sku_num, channel_country, selling_status_code,selling_channel_eligibility_list,live_date, range_group
ORDER BY  rms_sku_num, channel_country, selling_status_code,selling_channel_eligibility_list,live_date, eff_begin_tmstp))


SELECT 
      rms_sku_num,
      channel_country,
      selling_status_code,
      selling_channel_eligibility_list,
      live_date,
      eff_begin,
      `{{params.gcp_project_id}}.jwn_udf.UDF_TIME_ZONE`(cast(eff_begin as string)) as eff_begin_tmstp_tz,
      eff_end,
      `{{params.gcp_project_id}}.jwn_udf.UDF_TIME_ZONE`(cast(eff_end as string)) as eff_end_tmstp_tz
FROM (
  SELECT
      rms_sku_num,
      channel_country,
      selling_status_code,
      selling_channel_eligibility_list,
      live_date,
      MIN(RANGE_START(eff_period)) AS eff_begin,
      MAX(RANGE_END(eff_period))   AS eff_end
       from  (
    --inner normalize
            SELECT rms_sku_num, channel_country, selling_status_code,selling_channel_eligibility_list,live_date,eff_period,range_group
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num, channel_country, selling_status_code,selling_channel_eligibility_list,live_date ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM
      --  NONSEQUENCED VALIDTIME
      (
        
            --  NORMALIZE
         SELECT  src.rms_sku_num, src.channel_country, src.selling_status_code,src.selling_channel_eligibility_list,src.live_date,TGT.eff_begin_tmstp,TGT.eff_end_tmstp,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY src.rms_sku_num, src.channel_country, src.selling_status_code,src.selling_channel_eligibility_list,src.live_date ORDER BY eff_begin_tmstp) >= 
                DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag,
             COALESCE(RANGE_INTERSECT(SRC.eff_period , RANGE(TGT.eff_begin_tmstp,TGT.eff_end_tmstp)), SRC.eff_period ) AS eff_period ,
             FROM
            (
              SELECT
                  --  NORMALIZE
                  src2.rms_sku_num,
                  src2.channel_country,
                  src2.selling_status_code,
                  src2.selling_channel_eligibility_list,
                  src2.live_date,
                  RANGE( eff_begin_tmstp, eff_end_tmstp ) AS eff_period,

                FROM
                  (
                    SELECT
                        rms_sku_num,
                        channel_country,
                        selling_status_code,
                        selling_channel_eligibility_list,
                        live_date,
                        eff_begin_tmstp,
                        eff_end_tmstp FROM SRC1
                        --  Get incremental changes
                       
                  ) AS src2
                WHERE src2.eff_begin_tmstp < src2.eff_end_tmstp
            ) AS src
            LEFT OUTER JOIN 
            `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS tgt ON src.rms_sku_num = tgt.rms_sku_num
             AND src.channel_country = tgt.channel_country
             and range_overlaps(SRC.eff_period,  range(TGT.eff_begin_tmstp, TGT.eff_end_tmstp))
          WHERE tgt.rms_sku_num IS NULL
           OR upper(rtrim(src.selling_channel_eligibility_list, ' ')) <> upper(rtrim(tgt.selling_channel_eligibility_list, ' '))
           OR src.selling_channel_eligibility_list IS NOT NULL
           AND tgt.selling_channel_eligibility_list IS NULL
           OR src.selling_channel_eligibility_list IS NULL
           AND tgt.selling_channel_eligibility_list IS NOT NULL
           OR upper(rtrim(src.selling_status_code, ' ')) <> upper(rtrim(tgt.selling_status_code, ' '))
           OR src.selling_status_code IS NOT NULL
           AND tgt.selling_status_code IS NULL
           OR src.selling_status_code IS NULL
           AND tgt.selling_status_code IS NOT NULL
           OR src.live_date <> tgt.live_date
           OR src.live_date IS NOT NULL
           AND tgt.live_date IS NULL
           OR src.live_date IS NULL
           AND tgt.live_date IS NOT NULL
      )  AS ordered_data
) AS grouped_data)
GROUP BY rms_sku_num, channel_country, selling_status_code,selling_channel_eligibility_list,live_date,range_group
ORDER BY  rms_sku_num, channel_country, selling_status_code,selling_channel_eligibility_list,live_date);


--.IF ERRORCODE <> 0 THEN .QUIT 9
-- SEQUENCED VALIDTIME
DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS tgt
WHERE EXISTS (SELECT 1
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_xref_vtw AS src
    WHERE LOWER(rms_sku_num) = LOWER(tgt.rms_sku_num) AND LOWER(channel_country) = LOWER(tgt.channel_country)
    AND  src.eff_begin_tmstp <= tgt.eff_begin_tmstp
    AND src.eff_end_tmstp >= tgt.eff_end_tmstp);

UPDATE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS TGT
SET TGT.eff_end_tmstp = SRC.eff_begin_tmstp,
TGT.eff_end_tmstp_tz = SRC.eff_begin_tmstp_tz
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_xref_vtw AS SRC
WHERE LOWER(SRC.rms_sku_num) = LOWER(TGT.rms_sku_num) AND LOWER(SRC.channel_country) = LOWER(TGT.channel_country)
    AND SRC.eff_begin_tmstp >= TGT.eff_begin_tmstp
    AND SRC.eff_begin_tmstp < TGT.eff_end_tmstp 
    AND SRC.eff_end_tmstp > TGT.eff_end_tmstp ;

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS TGT
SET TGT.eff_begin_tmstp = SRC.eff_end_tmstp,
TGT.eff_begin_tmstp_tz = SRC.eff_end_tmstp_tz
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_xref_vtw AS SRC
WHERE LOWER(SRC.rms_sku_num) = LOWER(TGT.rms_sku_num) AND LOWER(SRC.channel_country) = LOWER(TGT.channel_country)
    AND SRC.eff_end_tmstp > TGT.eff_begin_tmstp
    AND SRC.eff_end_tmstp <= TGT.eff_end_tmstp;

--.IF ERRORCODE <> 0 THEN .QUIT 10
-- Override the selling_status_code of FLS and Rack if they are blocked in any ONLINE channels.-- AND period (ovr.eff_begin_tmstp, ovr.eff_end_tmstp ) OVERLAPS PERIOD(vtw.eff_begin_tmstp, vtw.eff_end_tmstp )


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_xref_vtw AS target SET
 selling_status_code = SOURCE.correct_selling_status_code FROM (SELECT vtw.rms_sku_num,
   vtw.channel_country,
   ovr.selling_status_code AS correct_selling_status_code,
   vtw.eff_begin_tmstp
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_xref_vtw AS vtw
   INNER JOIN (SELECT t1.rms_sku_num,
     t1.channel_country,
     t1.selling_status_code,
     t1.eff_begin_tmstp_utc as eff_begin_tmstp,
     COALESCE(MIN(t1.eff_begin_tmstp_utc) OVER (PARTITION BY t1.rms_sku_num, t1.channel_country ORDER BY t1.eff_begin_tmstp_utc
       ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING), CAST('0000-01-01 00:00:00' AS timestamp)) as eff_end_tmstp
    FROM (SELECT status_dim.rms_sku_num,
       status_dim.channel_country,
       status_dim.eff_begin_tmstp_utc,
       status_dim.selling_status_code
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_item_selling_status_dim AS status_dim
       INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_xref_vtw AS recs ON LOWER(status_dim.rms_sku_num) = LOWER(recs
          .rms_sku_num) AND LOWER(status_dim.channel_country) = LOWER(recs.channel_country)
      WHERE LOWER(status_dim.selling_channel) = LOWER('ONLINE')
       AND (LOWER(recs.selling_channel_eligibility_list) = LOWER('FLS') OR LOWER(recs.selling_channel_eligibility_list)
            = LOWER('Rack') OR LOWER(recs.selling_channel_eligibility_list) = LOWER('FLS, Rack') OR LOWER(recs.selling_channel_eligibility_list
           ) = LOWER('Rack, FLS'))
      GROUP BY status_dim.rms_sku_num,
       status_dim.channel_country,
       status_dim.selling_status_code,
       status_dim.eff_begin_tmstp_utc) AS t1) AS ovr ON LOWER(vtw.rms_sku_num) = LOWER(ovr.rms_sku_num) AND LOWER(vtw.channel_country
       ) = LOWER(ovr.channel_country) 
       and range_overlaps(range(ovr.eff_begin_tmstp, ovr.eff_end_tmstp ), range(vtw.eff_begin_tmstp, vtw.eff_end_tmstp))
       AND LOWER(vtw.selling_status_code) <> LOWER(ovr.selling_status_code)
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY vtw.rms_sku_num, vtw.channel_country, vtw.eff_begin_tmstp ORDER BY ovr.selling_status_code
       )) = 1) AS SOURCE
WHERE LOWER(target.rms_sku_num) = LOWER(SOURCE.rms_sku_num) AND LOWER(target.channel_country) = LOWER(SOURCE.channel_country
    ) AND target.eff_begin_tmstp = SOURCE.eff_begin_tmstp;
--.IF ERRORCODE <> 0 THEN .QUIT 11


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref (rms_sku_num, channel_country, selling_status_code,
 selling_channel_eligibility_list, live_date, eff_begin_tmstp,eff_begin_tmstp_tz,eff_end_tmstp,eff_end_tmstp_tz, dw_batch_id, dw_batch_date,
 dw_sys_load_tmstp)
(SELECT DISTINCT rms_sku_num,
  channel_country,
  selling_status_code,
  selling_channel_eligibility_list,
  live_date,
  eff_begin_tmstp,
  eff_begin_tmstp_tz,
  eff_end_tmstp,
  eff_end_tmstp_tz,
  dw_batch_id,
  dw_batch_date,
  dw_sys_load_tmstp
 FROM (SELECT rms_sku_num,
    channel_country,
    selling_status_code,
    selling_channel_eligibility_list,
    live_date,
    eff_begin_tmstp,
    eff_begin_tmstp_tz,
    eff_end_tmstp,
    eff_end_tmstp_tz,
    CAST(RPAD(FORMAT_TIMESTAMP('%Y%m%d%I%M%S', CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)), 14, ' ') AS BIGINT)
    AS dw_batch_id,
    CURRENT_DATE('PST8PDT') AS dw_batch_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_xref_vtw) AS t
 WHERE NOT EXISTS (SELECT 1 AS `A12180`
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref
   WHERE LOWER(rms_sku_num) = LOWER(t.rms_sku_num)
    AND LOWER(channel_country) = LOWER(t.channel_country))
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, channel_country)) = 1);

COMMIT TRANSACTION;