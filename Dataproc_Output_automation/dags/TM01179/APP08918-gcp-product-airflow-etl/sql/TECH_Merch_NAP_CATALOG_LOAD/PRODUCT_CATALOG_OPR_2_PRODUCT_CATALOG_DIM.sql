--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=opr-dim;AppSubArea=PRODUCT_CATALOG;' UPDATE FOR SESSION;

-- NONSEQUENCED VALIDTIME --Purge work table for staging temporal rows
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_catalog_dim_vtw;
--.IF ERRORCODE <> 0 THEN .QUIT 1




INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_catalog_dim_vtw
(
catalog_num,
catalog_name,
catalog_desc,
npin,
rms_sku_num,
catalog_start_tmstp,
catalog_start_tmstp_tz,
eff_begin_tmstp,
eff_begin_tmstp_tz,
eff_end_tmstp,
eff_end_tmstp_tz
)


WITH cat_opr AS (SELECT 
 catalog_num,
 catalog_name,
 catalog_desc,
 catalog_start_tmstp,
 catalog_start_tmstp_tz,
 is_deleted,
 del_reason,
 del_tmstp,
 event_time,
 eff_begin_tmstp,
 eff_begin_tmstp_tz,
 eff_end_tmstp,
 eff_end_tmstp_tz,
 RANGE(eff_begin_tmstp,eff_end_tmstp) AS eff_period
FROM 

(
SELECT catalog_num,catalog_name,catalog_desc,catalog_start_tmstp,catalog_start_tmstp_tz,is_deleted,del_reason,del_tmstp,event_time,
MIN(eff_begin_tmstp) AS eff_begin_tmstp,MIN(eff_begin_tmstp_tz) AS eff_begin_tmstp_tz,MAX(eff_end_tmstp) AS eff_end_tmstp,MAX(eff_end_tmstp_tz) AS eff_end_tmstp_tz
            FROM     
 (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY catalog_num,catalog_name,catalog_desc,catalog_start_tmstp,catalog_start_tmstp_tz,is_deleted,del_reason,del_tmstp,event_time ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM
( SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY catalog_num,catalog_name,catalog_desc,catalog_start_tmstp,catalog_start_tmstp_tz,is_deleted,del_reason,del_tmstp,event_time 
				ORDER BY eff_begin_tmstp) >= DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
    FROM


(
SELECT  
 catalog_num,
 catalog_name,
 catalog_desc,
 catalog_start_tmstp,
 catalog_start_tmstp_tz,
 is_deleted,
 del_reason,
 del_tmstp,
 event_time,
 event_time as eff_begin_tmstp,
 `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(event_time as string)) as eff_begin_tmstp_tz, 
 COALESCE(MIN(event_time)OVER(PARTITION BY catalog_num  ORDER BY event_time ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING),TIMESTAMP '9999-12-31 23:59:59.999999+00:00') AS eff_end_tmstp,
`{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(COALESCE(MIN(event_time)OVER(PARTITION BY catalog_num  ORDER BY event_time ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING),TIMESTAMP '9999-12-31 23:59:59.999999+00:00') as string)) AS eff_end_tmstp_tz

FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.product_catalog_opr
WHERE LOWER(npin) = LOWER('-1')
QUALIFY (ROW_NUMBER() OVER (PARTITION BY catalog_num, catalog_name, event_time ORDER BY catalog_desc DESC)) = 1)
) AS ordered_data
 ) AS grouped_data

GROUP BY catalog_num,catalog_name,catalog_desc,catalog_start_tmstp,catalog_start_tmstp_tz,is_deleted,del_reason,del_tmstp,event_time, range_group
ORDER BY catalog_num,catalog_name,catalog_desc,catalog_start_tmstp,catalog_start_tmstp_tz,is_deleted,del_reason,del_tmstp,event_time
)
)
,


delta AS (SELECT DISTINCT catalogid AS catalog_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_catalog_ldg
WHERE LOWER(isremoved) = LOWER('false')
UNION DISTINCT
SELECT DISTINCT a.catalogid AS catalog_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_catalog_ldg AS a
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_catalog_dim AS b ON LOWER(a.catalogid) = LOWER(b.catalog_num) AND CAST(b.eff_end_tmstp AS DATE)
   >= CURRENT_DATE('PST8PDT')
WHERE LOWER(a.isdeleted) = LOWER('false'))
,


SRC1 AS (
SELECT catalog_num,npin,rms_sku_num,eff_begin_tmstp,eff_end_tmstp FROM
(
SELECT catalog_num,npin,rms_sku_num
,MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
FROM 
  (
SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY catalog_num,npin,rms_sku_num ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM
(
SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY catalog_num,npin,rms_sku_num
				ORDER BY eff_begin_tmstp) >= DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag FROM
  
  (SELECT  --normalize
    catalog_num,
     npin,
     rms_sku_num,
     RANGE(SRC2.eff_begin_tmstp,SRC2.eff_end_tmstp) AS EFF_PERIOD,
     eff_begin_tmstp,
     COALESCE(MAX(SRC2.eff_begin_tmstp) OVER(PARTITION BY SRC2.catalog_num, SRC2.npin ORDER BY SRC2.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING), TIMESTAMP '9999-12-31 23:59:59.999999+00:00') AS eff_end_tmstp
    FROM (SELECT DISTINCT  --NORMALIZE
       item_opr.catalog_num,
       item_opr.npin,
       item_opr.rms_sku_num,
       item_opr.event_time AS eff_begin_tmstp,                   -- this was cast as TIMESTAMP WITH TIME ZONE
       COALESCE(MAX(item_opr.event_time) OVER(PARTITION BY item_opr.catalog_num, item_opr.npin ORDER BY item_opr.event_time ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING), TIMESTAMP '9999-12-31 23:59:59.999999+00:00') AS eff_end_tmstp
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.product_catalog_opr AS item_opr
       INNER JOIN delta ON LOWER(item_opr.catalog_num) = LOWER(delta.catalog_num)
      WHERE LOWER(item_opr.is_deleted) = LOWER('N')
       AND LOWER(item_opr.npin) <> LOWER('-1')) AS SRC2
    QUALIFY eff_begin_tmstp < eff_end_tmstp)     
)  AS ordered_data
) AS grouped_data
GROUP BY catalog_num,npin,rms_sku_num, range_group
ORDER BY catalog_num,npin,rms_sku_num, eff_begin_tmstp
))
,
SRC AS
(
SELECT  catalog_num, npin, rms_sku_num, catalog_name, catalog_desc, catalog_start_tmstp, catalog_start_tmstp_tz, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp,eff_period1
            FROM 
(
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY catalog_num, npin, rms_sku_num, catalog_name, catalog_desc, catalog_start_tmstp, catalog_start_tmstp_tz ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
    FROM
(
SELECT *,
CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY   catalog_num, npin, rms_sku_num, catalog_name, catalog_desc, catalog_start_tmstp, catalog_start_tmstp_tz ORDER BY eff_begin_tmstp) >= 
                DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag,
            COALESCE(RANGE_INTERSECT(SRC.eff_period,eff_period2),src.eff_period ) AS eff_period1
             FROM
 (SELECT --normalize
   SRC0.catalog_num,
   SRC0.npin,
   SRC0.rms_sku_num,
   cat_opr.catalog_name,
   cat_opr.catalog_desc,
   cat_opr.catalog_start_tmstp,
   cat_opr.catalog_start_tmstp_tz,
   SRC0.eff_begin_tmstp,
   SRC0.eff_end_tmstp,
   RANGE(SRC0.eff_begin_tmstp, SRC0.eff_end_tmstp) AS eff_period,
   RANGE(cat_opr.eff_begin_tmstp, cat_opr.eff_end_tmstp) AS eff_period2,
  FROM 
  SRC1
  AS SRC0

   INNER JOIN cat_opr ON LOWER(SRC0.catalog_num) = LOWER(cat_opr.catalog_num)
   AND RANGE_OVERLAPS(EFF_PERIOD,cat_opr.EFF_PERIOD)
  WHERE LOWER(cat_opr.is_deleted) = LOWER('N')
   AND cat_opr.catalog_name IS NOT NULL) AS SRC

) AS ordered_data
) AS grouped_data
GROUP BY catalog_num, npin, rms_sku_num, catalog_name, catalog_desc, catalog_start_tmstp, catalog_start_tmstp_tz,eff_period1,range_group
ORDER BY catalog_num, npin, rms_sku_num, catalog_name, catalog_desc, catalog_start_tmstp, catalog_start_tmstp_tz
)


SELECT 
 catalog_num,
 catalog_name,
 catalog_desc,
 npin,
 rms_sku_num,
 catalog_start_tmstp,
 catalog_start_tmstp_tz,
 MIN(RANGE_START(EFF_PERIOD)) AS eff_begin_tmstp,
 `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(MIN(RANGE_START(EFF_PERIOD)) AS STRING)) as eff_begin_tmstp_tz,
 MAX(RANGE_END(EFF_PERIOD)) AS eff_end_tmstp,
 `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(MAX(RANGE_END(EFF_PERIOD)) AS STRING)) as eff_end_tmstp_tz

FROM

(
SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY catalog_num,catalog_name,catalog_desc,npin,rms_sku_num,catalog_start_tmstp,catalog_start_tmstp_tz
				ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group FROM

(
SELECT *,
CASE WHEN LAG(eff_end_tmstp) OVER (PARTITION BY catalog_num,catalog_name,catalog_desc,npin,rms_sku_num,catalog_start_tmstp,catalog_start_tmstp_tz
 ORDER BY eff_begin_tmstp) >= DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
THEN 0 ELSE 1
END AS discontinuity_flag
FROM
(
SELECT 
SRC.catalog_num,
SRC.catalog_name,
SRC.catalog_desc,
SRC.npin,
SRC.rms_sku_num,
SRC.catalog_start_tmstp,
SRC.catalog_start_tmstp_tz,
TGT.eff_begin_tmstp,
TGT.eff_end_tmstp,
COALESCE(RANGE_INTERSECT(SRC.eff_period1,RANGE(cast(TGT.eff_begin_tmstp as timestamp),cast(TGT.eff_end_tmstp as timestamp))), SRC.eff_period1 ) AS eff_period, 
TGT.eff_begin_tmstp_tz,
TGT.eff_end_tmstp_tz
FROM SRC
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_catalog_dim AS tgt ON LOWER(SRC.catalog_num) = LOWER(tgt.catalog_num) AND LOWER(SRC.npin
    ) = LOWER(tgt.npin)
    AND RANGE_OVERLAPS(RANGE(SRC.eff_begin_tmstp, SRC.eff_end_tmstp),RANGE(tgt.eff_begin_tmstp, tgt.eff_end_tmstp))
WHERE (tgt.catalog_num IS NULL
 OR ((LOWER(SRC.catalog_num) <> LOWER(tgt.catalog_num)
 OR (tgt.catalog_num IS NULL AND SRC.catalog_num IS NOT NULL)
 OR (SRC.catalog_num IS NULL AND tgt.catalog_num IS NOT NULL))

 OR (LOWER(SRC.catalog_name) <> LOWER(tgt.catalog_name)
 OR (tgt.catalog_name IS NULL AND SRC.catalog_name IS NOT NULL)
 OR (SRC.catalog_name IS NULL AND tgt.catalog_name IS NOT NULL))

 OR (LOWER(SRC.catalog_desc) <> LOWER(tgt.catalog_desc)
 OR (tgt.catalog_desc IS NULL AND SRC.catalog_desc IS NOT NULL)
 OR (SRC.catalog_desc IS NULL AND tgt.catalog_desc IS NOT NULL))

 OR (LOWER(SRC.npin) <> LOWER(tgt.npin)
 OR (tgt.npin IS NULL AND SRC.npin IS NOT NULL)
 OR (SRC.npin IS NULL AND tgt.npin IS NOT NULL))

 OR (SRC.catalog_start_tmstp <> tgt.catalog_start_tmstp
 OR (tgt.catalog_start_tmstp IS NULL AND SRC.catalog_start_tmstp IS NOT NULL)
 OR (SRC.catalog_start_tmstp IS NULL AND tgt.catalog_start_tmstp IS NOT NULL))

 OR (LOWER(SRC.rms_sku_num) <> LOWER(tgt.rms_sku_num)
 OR (tgt.rms_sku_num IS NULL AND SRC.rms_sku_num IS NOT NULL)
 OR (SRC.rms_sku_num IS NULL AND tgt.rms_sku_num IS NOT NULL))))    

) AS ordered_data
) AS grouped_data
) GROUP BY catalog_num,catalog_name,catalog_desc,npin,rms_sku_num,catalog_start_tmstp,catalog_start_tmstp_tz,range_group
  ORDER BY catalog_num,catalog_name,catalog_desc,npin,rms_sku_num,catalog_start_tmstp,catalog_start_tmstp_tz,eff_begin_tmstp
;




--.IF ERRORCODE <> 0 THEN .QUIT 2

--Explicit transaction on Teradata ensures that we do not corrupt target table if failure in middle of updating target table
BEGIN TRANSACTION;
--.IF ERRORCODE <> 0 THEN .QUIT 3

--SEQUENCED VALIDTIME handled
DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_catalog_dim AS TGT
WHERE EXISTS (
  SELECT 1
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_catalog_dim_vtw AS SRC
  WHERE LOWER(SRC.catalog_num) = LOWER(TGT.catalog_num)
    AND LOWER(SRC.npin) = LOWER(TGT.npin)
    AND SRC.eff_begin_tmstp <= TGT.eff_begin_tmstp
    AND SRC.eff_end_tmstp >= TGT.eff_end_tmstp
);

UPDATE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_catalog_dim AS TGT
SET TGT.eff_end_tmstp = SRC.eff_begin_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_catalog_dim_vtw AS SRC
WHERE LOWER(SRC.catalog_num) = LOWER(TGT.catalog_num)
    AND LOWER(SRC.npin) = LOWER(TGT.npin)
    AND SRC.eff_begin_tmstp >= TGT.eff_begin_tmstp
    AND SRC.eff_begin_tmstp < TGT.eff_end_tmstp 
    AND SRC.eff_end_tmstp > TGT.eff_end_tmstp ;


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_catalog_dim AS TGT
SET TGT.eff_begin_tmstp = SRC.eff_end_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_catalog_dim_vtw AS SRC
WHERE LOWER(SRC.catalog_num) = (TGT.catalog_num)
    AND LOWER(SRC.npin) = LOWER(TGT.npin)
    AND SRC.eff_end_tmstp > TGT.eff_begin_tmstp
    AND SRC.eff_end_tmstp <= TGT.eff_end_tmstp;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_catalog_dim (catalog_num, catalog_name, catalog_desc, npin, rms_sku_num,
 catalog_start_tmstp,catalog_start_tmstp_tz, eff_begin_tmstp,eff_begin_tmstp_tz, eff_end_tmstp,eff_end_tmstp_tz, dw_batch_id, dw_batch_date, dw_sys_load_tmstp)
(SELECT DISTINCT catalog_num,
  catalog_name,
  catalog_desc,
  npin,
  rms_sku_num,
  catalog_start_tmstp,
  catalog_start_tmstp_tz,
  eff_begin_tmstp,
  eff_begin_tmstp_tz,
  eff_end_tmstp,
  eff_end_tmstp_tz,
  dw_batch_id,
  dw_batch_date,
  dw_sys_load_tmstp
 FROM (SELECT catalog_num,
    catalog_name,
    catalog_desc,
    npin,
    rms_sku_num,
    catalog_start_tmstp,
    catalog_start_tmstp_tz,
    eff_begin_tmstp,
    eff_begin_tmstp_tz,
    eff_end_tmstp,
    eff_end_tmstp_tz,
    CAST(RPAD(FORMAT_TIMESTAMP('%Y%m%d%I%M%S', CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)), 14, ' ') AS BIGINT)
    AS dw_batch_id,
    CURRENT_DATE('PST8PDT') AS dw_batch_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_catalog_dim_vtw AS src) AS t
 WHERE NOT EXISTS (SELECT 1 AS `A12180`
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_catalog_dim
   WHERE catalog_num = t.catalog_num
    AND npin = t.npin)
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY catalog_num, npin)) = 1);

--.IF ERRORCODE <> 0 THEN .QUIT 5
COMMIT TRANSACTION;
--.IF ERRORCODE <> 0 THEN .QUIT 6

-- Update the eff_end_tmstp in PRODUCT_CATALOG_DIM for entries corresponding to deleted events
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_catalog_dim AS target SET
 eff_end_tmstp = SOURCE.max_delete_event_time,
 eff_end_tmstp_tz = SOURCE.max_delete_event_time_tz
FROM (SELECT opr.catalog_num,
   opr.npin,
   MAX(opr.event_time) AS max_delete_event_time,   
   `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(MAX(opr.event_time) AS STRING)) AS max_delete_event_time_tz,                     
   MAX(dim.eff_begin_tmstp) AS max_event_time          
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.product_catalog_opr AS opr
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_catalog_dim AS dim ON LOWER(opr.catalog_num) = LOWER(dim.catalog_num) AND LOWER(opr.npin
      ) = LOWER(dim.npin)
  WHERE LOWER(opr.is_deleted) = LOWER('Y')
   AND opr.dw_sys_load_date BETWEEN DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 24 MONTH) AND (DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL 24
      MONTH))
   AND dim.eff_end_tmstp = TIMESTAMP '9999-12-31 23:59:59.999999+00:00'
   AND (opr.catalog_num, opr.npin) IN (SELECT (catalogid, npin)
     FROM (SELECT DISTINCT catalogid,
         SUBSTR(npin, 1, 100) AS npin
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_catalog_ldg
        WHERE LOWER(isremoved) = LOWER('true')
        UNION DISTINCT
        SELECT DISTINCT catalogid,
         SUBSTR('-1', 1, 100) AS npin
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_catalog_ldg
        WHERE LOWER(isdeleted) = LOWER('true')) AS t7)
  GROUP BY opr.catalog_num,
   opr.npin
  HAVING max_delete_event_time > max_event_time
  ) AS SOURCE
WHERE LOWER(target.catalog_num) = LOWER(SOURCE.catalog_num) AND LOWER(target.npin) = LOWER(SOURCE.npin) AND target.eff_begin_tmstp
    = SOURCE.max_event_time AND target.eff_end_tmstp  = TIMESTAMP '9999-12-31 23:59:59.999999+00:00';
-- Only update if the current eff_end_tmstp is set to the 'open' value

--.IF ERRORCODE <> 0 THEN .QUIT 7

