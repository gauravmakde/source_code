--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=stg-dim;AppSubArea=NAP_PRODUCT;' UPDATE FOR SESSION;


--NONSEQUENCED VALIDTIME --Purge work table for staging temporal rows
TRUNCATE TABLE  `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.casepack_sku_xref_vtw;
--.IF ERRORCODE<> 0 THEN .QUIT 1



INSERT INTO `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.casepack_sku_xref_vtw
( rms_casepack_num
, epm_casepack_num
, channel_country
, rms_sku_num
, epm_sku_num
, pack_component_seq_num
, items_per_pack_qty
, eff_begin_tmstp
, eff_end_tmstp
, eff_begin_tmstp_tz
, eff_end_tmstp_tz
)

with src_1 as
 (select rms_casepack_num, epm_casepack_num, channel_country, rms_sku_num, epm_sku_num, pack_component_seq_num, items_per_pack_qty
	, eff_begin_tmstp, eff_end_tmstp
--  range(eff_begin_tmstp,eff_end_tmstp) as eff_period 
 from  (
    --inner normalize
            SELECT rms_casepack_num, epm_casepack_num, channel_country, rms_sku_num, epm_sku_num, pack_component_seq_num, items_per_pack_qty, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_casepack_num, epm_casepack_num, channel_country, rms_sku_num, epm_sku_num, pack_component_seq_num, items_per_pack_qty ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
    FROM (
        SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY rms_casepack_num, epm_casepack_num, channel_country, rms_sku_num, epm_sku_num, pack_component_seq_num, items_per_pack_qty ORDER BY eff_begin_tmstp) >= 
                DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
         from(SELECT SRC_2.*
	, COALESCE(MAX(eff_begin_tmstp) OVER(PARTITION BY epm_casepack_num, channel_country, epm_sku_num
	ORDER BY eff_begin_tmstp
	           ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
	          ,TIMESTAMP'9999-12-31 23:59:59.999999+00:00') AS eff_end_tmstp
	FROM (
SELECT COALESCE(TRIM(referencekeyfg_legacyrmscasepackid), '0') AS rms_casepack_num,
 id AS epm_casepack_num,
 marketcode AS channel_country,
 COALESCE(TRIM(skus_referencekeyfg_legacyrmsskuid), '0') AS rms_sku_num,
 skus_id AS epm_sku_num,
 skus_componentseqnum AS pack_component_seq_num,
 skus_itemperpackquantity AS items_per_pack_qty,
cast(`{{params.dataplex_project_id}}.JWN_UDF.ISO8601_TMSTP`( COLLATE(sourcepublishtimestamp,'')) as timestamp) AS eff_begin_tmstp
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.casepack_sku_xref_ldg
	) SRC_2
	QUALIFY eff_begin_tmstp  < eff_end_tmstp
	)) AS ordered_data
) AS grouped_data
GROUP BY rms_casepack_num, epm_casepack_num, channel_country, rms_sku_num, epm_sku_num, pack_component_seq_num, items_per_pack_qty,range_group
ORDER BY rms_casepack_num, epm_casepack_num, channel_country, rms_sku_num, epm_sku_num, pack_component_seq_num, items_per_pack_qty, eff_begin_tmstp) ) 

SELECT
  rms_casepack_num
, epm_casepack_num
, channel_country
, rms_sku_num
, epm_sku_num
, pack_component_seq_num
, items_per_pack_qty
, min(RANGE_START(eff_period)) AS eff_begin
, max(RANGE_END(eff_period))   AS eff_end
,`{{params.dataplex_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(min(RANGE_START(eff_period)) as string)) as eff_begin_tmstp_tz
,`{{params.dataplex_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(max(RANGE_END(eff_period)) as string)) as eff_end_tmstp_tz

       from  (
    --inner normalize
            SELECT rms_casepack_num, epm_casepack_num, channel_country, rms_sku_num, epm_sku_num, pack_component_seq_num, items_per_pack_qty,range_group,eff_period
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_casepack_num, epm_casepack_num, channel_country, rms_sku_num, epm_sku_num, pack_component_seq_num, items_per_pack_qty ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM
      --  NONSEQUENCED VALIDTIME
      (
        
            --  NORMALIZE
         SELECT  src.rms_casepack_num, src.epm_casepack_num, src.channel_country, src.rms_sku_num, src.epm_sku_num, src.pack_component_seq_num, src.items_per_pack_qty,TGT.eff_begin_tmstp,TGT.eff_end_tmstp,
            CASE 
                WHEN LAG(eff_end_tmstp_utc) OVER (PARTITION BY src.rms_casepack_num, src.epm_casepack_num, src.channel_country, src.rms_sku_num, src.epm_sku_num, src.pack_component_seq_num, src.items_per_pack_qty ORDER BY eff_begin_tmstp_utc) >= 
                DATE_SUB(eff_begin_tmstp_utc, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag,
             COALESCE( SAFE.RANGE_INTERSECT (SRC.eff_period,range(TGT.eff_begin_tmstp_utc,TGT.eff_end_tmstp_utc))
          , SRC.eff_period ) AS eff_period ,
             FROM(
	SELECT 
  --normalize 
  rms_casepack_num, epm_casepack_num, channel_country, rms_sku_num, epm_sku_num
	, pack_component_seq_num, items_per_pack_qty
	, RANGE(eff_begin_tmstp, eff_end_tmstp) AS eff_period
	FROM SRC_1
)  as SRC
LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.casepack_sku_xref_hist tgt
ON  LOWER(SRC.rms_casepack_num) = LOWER(TGT.rms_casepack_num)
AND LOWER(SRC.channel_country) = LOWER(TGT.channel_country)
AND LOWER(SRC.rms_sku_num) = LOWER(TGT.rms_sku_num)
AND RANGE_OVERLAPS(RANGE(TGT.eff_begin_tmstp_utc, TGT.eff_end_tmstp_utc), SRC.eff_period )
WHERE ( TGT.rms_sku_num IS NULL 
   OR ((SRC.epm_casepack_num <> TGT.epm_casepack_num OR (SRC.epm_casepack_num IS NOT NULL AND TGT.epm_casepack_num IS NULL) OR (SRC.epm_casepack_num IS NULL AND TGT.epm_casepack_num IS NOT NULL))
   OR (SRC.epm_sku_num <> TGT.epm_sku_num OR (SRC.epm_sku_num IS NOT NULL AND TGT.epm_sku_num IS NULL) OR (SRC.epm_sku_num IS NULL AND TGT.epm_sku_num IS NOT NULL))
   OR (SRC.pack_component_seq_num <> TGT.pack_component_seq_num OR (SRC.pack_component_seq_num IS NOT NULL AND TGT.pack_component_seq_num IS NULL) OR (SRC.pack_component_seq_num IS NULL AND TGT.pack_component_seq_num IS NOT NULL))
   OR (SRC.items_per_pack_qty <> TGT.items_per_pack_qty OR (SRC.items_per_pack_qty IS NOT NULL AND TGT.items_per_pack_qty IS NULL) OR (SRC.items_per_pack_qty IS NULL AND TGT.items_per_pack_qty IS NOT NULL))
   )  )
	  )as   ordered_data
) AS grouped_data)
GROUP BY   rms_casepack_num, epm_casepack_num, channel_country, rms_sku_num, epm_sku_num, pack_component_seq_num, items_per_pack_qty,range_group,eff_period
ORDER BY  rms_casepack_num, epm_casepack_num, channel_country, rms_sku_num, epm_sku_num, pack_component_seq_num, items_per_pack_qty;
BEGIN TRANSACTION;


DELETE FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.casepack_sku_xref AS tgt
WHERE EXISTS (SELECT 1
    FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.casepack_sku_xref_vtw AS src
    WHERE LOWER(rms_casepack_num) = LOWER(tgt.rms_casepack_num) 
    AND   LOWER(channel_country) = LOWER(tgt.channel_country)
    AND   LOWER(rms_sku_num) = LOWER(tgt.rms_sku_num)
    AND SRC.eff_begin_tmstp <= TGT.eff_begin_tmstp
    AND SRC.eff_end_tmstp >= TGT.eff_end_tmstp);

UPDATE  `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.casepack_sku_xref AS tgt
SET TGT.eff_end_tmstp = SRC.eff_begin_tmstp
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.casepack_sku_xref_vtw AS src
WHERE  LOWER(src.rms_casepack_num) = LOWER(tgt.rms_casepack_num) 
    AND   LOWER(src.channel_country) = LOWER(tgt.channel_country)
    AND   LOWER(src.rms_sku_num) = LOWER(tgt.rms_sku_num)
    AND SRC.eff_begin_tmstp > TGT.eff_begin_tmstp
    AND SRC.eff_begin_tmstp <= TGT.eff_end_tmstp
AND SRC.eff_end_tmstp >= TGT.eff_end_tmstp;


UPDATE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.casepack_sku_xref AS tgt
SET TGT.eff_begin_tmstp = SRC.eff_end_tmstp
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.casepack_sku_xref_vtw AS src
WHERE  LOWER(src.rms_casepack_num) = LOWER(tgt.rms_casepack_num) 
    AND   LOWER(src.channel_country) = LOWER(tgt.channel_country)
    AND   LOWER(src.rms_sku_num) = LOWER(tgt.rms_sku_num)
    AND SRC.eff_end_tmstp >= TGT.eff_begin_tmstp
    AND SRC.eff_begin_tmstp < TGT.eff_begin_tmstp
AND SRC.eff_end_tmstp <= TGT.eff_end_tmstp;

insert into `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.casepack_sku_xref(
rms_casepack_num			
,epm_casepack_num			
,channel_country			
,rms_sku_num			
,epm_sku_num			
,pack_component_seq_num			
,items_per_pack_qty			
,eff_begin_tmstp_tz			
,eff_end_tmstp_tz			
,dw_batch_id			
,dw_batch_date	
,dw_sys_load_tmstp
,eff_begin_tmstp
,eff_end_tmstp	
)

with tbl as 
(select 
tgt.*,
cast(src.eff_begin_tmstp as timestamp) AS src_eff_begin_tmstp, 
cast(src.eff_end_tmstp as timestamp) AS src_eff_end_tmstp, 
tgt.eff_begin_tmstp AS tgt_eff_begin_tmstp, 
tgt.eff_end_tmstp AS tgt_eff_end_tmstp 
 from `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.casepack_sku_xref_vtw as src 
 inner join `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.casepack_sku_xref as tgt 
 on src.rms_casepack_num = tgt.rms_casepack_num 
 and tgt.eff_begin_tmstp < src.eff_begin_tmstp 
 and tgt.eff_end_tmstp > src.eff_end_tmstp)

select * except (eff_begin_tmstp, eff_end_tmstp, src_eff_begin_tmstp, src_eff_end_tmstp, tgt_eff_begin_tmstp,tgt_eff_end_tmstp ),
tgt_eff_begin_tmstp,cast(src_eff_begin_tmstp as timestamp)
-- src_rms_casepack_num,src_epm_casepack_num,src_channel_country,src_rms_sku_num, src_epm_sku_num,src_pack_component_seq_num,src_items_per_pack_qty,
-- --tgt_id,tgt_rms_casepack_num,tgt_epm_casepack_num,tgt_channel_country,tgt_rms_sku_num,tgt_epm_sku_num,tgt_pack_component_seq_num,tgt_items_per_pack_qty,
-- tgt_begin_tmstp,tgt_begin_tmstp_tz, src_begin_tmstp,src_begin_tmstp_tz,tgt_dw_batch_id,tgt_dw_batch_date,tgt_dw_sys_load_tmstp 
from tbl
union all
select * except(eff_begin_tmstp, eff_end_tmstp, src_eff_begin_tmstp, src_eff_end_tmstp, tgt_eff_begin_tmstp,tgt_eff_end_tmstp), cast(src_eff_end_tmstp as timestamp), tgt_eff_end_tmstp
-- src_rms_casepack_num,src_epm_casepack_num,src_channel_country,src_rms_sku_num, src_epm_sku_num,src_pack_component_seq_num,src_items_per_pack_qty,
-- --tgt_id,tgt_rms_casepack_num,tgt_epm_casepack_num,tgt_channel_country,tgt_rms_sku_num,tgt_epm_sku_num,tgt_pack_component_seq_num,tgt_items_per_pack_qty,
-- src_end_tmstp, src_end_tmstp_tz, tgt_end_tmstp,tgt_end_tmstp_tz,tgt_dw_batch_id,tgt_dw_batch_date,tgt_dw_sys_load_tmstp,  
from tbl;

delete from `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.casepack_sku_xref as tgt 
where exists (select 1 from `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.casepack_sku_xref_vtw AS src 
where src.rms_casepack_num = tgt.rms_casepack_num and tgt.eff_begin_tmstp < src.eff_begin_tmstp 
and tgt.eff_end_tmstp > src.eff_end_tmstp);


INSERT INTO `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.casepack_sku_xref (rms_casepack_num, epm_casepack_num, channel_country, rms_sku_num, epm_sku_num
 , pack_component_seq_num, items_per_pack_qty, eff_begin_tmstp, eff_end_tmstp, dw_batch_id, dw_batch_date,
 dw_sys_load_tmstp,eff_begin_tmstp_tz, eff_end_tmstp_tz)
(SELECT DISTINCT rms_casepack_num,
  epm_casepack_num,
  channel_country,
  rms_sku_num,
  epm_sku_num,
  pack_component_seq_num,
  items_per_pack_qty,
  eff_begin_tmstp,
  eff_end_tmstp,
  dw_batch_date,
  dw_batch_date0,
  dw_sys_load_tmstp,
  eff_begin_tmstp_tz, eff_end_tmstp_tz
 FROM (SELECT rms_casepack_num,
    epm_casepack_num,
    channel_country,
    rms_sku_num,
    epm_sku_num,
    pack_component_seq_num,
    items_per_pack_qty,
    eff_begin_tmstp,
    eff_end_tmstp,
    (SELECT batch_id
     FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
     WHERE LOWER(subject_area_nm) = LOWER('NAP_Product'))  AS dw_batch_date,
    (SELECT CURR_BATCH_DATE
      FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
      WHERE LOWER(subject_area_nm) = LOWER('NAP_Product')) AS dw_batch_date0,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
    eff_begin_tmstp_tz, eff_end_tmstp_tz
   FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.casepack_sku_xref_vtw)AS t3
 WHERE NOT EXISTS (SELECT 1 AS `A12180`
   FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.casepack_sku_xref
   WHERE (epm_casepack_num) = (t3.epm_casepack_num)
    AND LOWER(channel_country) = LOWER(t3.channel_country)
    AND (epm_sku_num) = (t3.epm_sku_num))
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY epm_casepack_num, channel_country, epm_sku_num)) = 1); 


COMMIT TRANSACTION;